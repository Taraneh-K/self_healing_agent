"""
File: main.py
Description: Autonomous Self-Healing Data Pipeline Orchestrator.
"""
import asyncio
import ast
import polars as pl
import logfire
import re
import json
import httpx
from datetime import datetime
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from src.schema import DataFix
from src.agent import data_fix_agent, AgentDeps
from scripts.check_audit import generate_audit_report

# 1. Setup Logging and Instrumentation
load_dotenv()
logfire.instrument_httpx() 
logfire.configure()

# --- STEP 4a: RETRY LOGIC ---
@retry(
    # Increase to 5 attempts to survive longer rate-limit windows
    stop=stop_after_attempt(5),
    # Start at 5s, double it each time (5, 10, 20...)
    wait=wait_exponential(multiplier=2, min=5, max=30),
    # Explicitly catch the 429 rate limit error
    retry=retry_if_exception_type((httpx.HTTPStatusError, httpx.HTTPError, asyncio.TimeoutError)),
    before_sleep=lambda retry_state: print(f"Rate Limit Hit. Waiting {retry_state.next_action.sleep}s before attempt {retry_state.attempt_number}...")
)
async def get_fix_from_ai(error_context, deps):
    return await data_fix_agent.run(error_context, deps=deps)
async def get_fix_from_ai(error_context, deps):
    """Calls the AI agent with automatic retry logic for network/API failures."""
    return await data_fix_agent.run(error_context, deps=deps)

def get_diverse_samples(series: pl.Series, n_samples: int = 20) -> list[str]:
    """Groups unique strings by their 'shape' and picks representatives."""
    if series.len() == 0:
        return []
    df_unique = series.unique().to_frame("raw")
    df_patterns = df_unique.with_columns(
        pattern=pl.col("raw")
        .str.replace_all(r"\d", "0")
        .str.replace_all(r"[a-zA-Z]", "X")
    )
    return (
        df_patterns.group_by("pattern")
        .agg(pl.col("raw").first())
        .head(n_samples)
        .get_column("raw")
        .to_list()
    )

def parse_ai_response(response_text: str):
    """Extracts JSON with high resilience for malformed or truncated AI outputs."""
    # Find anything between the first { and the last }
    match = re.search(r"(\{.*\})", response_text, re.DOTALL)
    if not match:
        raise ValueError("No JSON block found. AI might have timed out.")
    
    json_str = match.group(1)
    
    # Clean common formatting issues
    json_str = json_str.replace("'", '"') # LLMs sometimes use single quotes
    
    try:
        data = json.loads(json_str)
    except json.JSONDecodeError:
        # If it's still broken, try to close the JSON manually (common for truncation)
        try:
            data = json.loads(json_str + '"}')
        except:
            raise ValueError("AI response was truncated and could not be recovered.")
    
    return {
        "explanation": data.get("explanation", "No explanation."),
        "python_code": data.get("python_code") or data.get("expression") or "pl.col('unknown')"
    }
def get_pipeline_health(df: pl.DataFrame):
    """Generates a health status based on the fix_status column."""
    total = len(df)
    if total == 0: return {"status": "INACTIVE"}
    
    degraded_count = 0
    if "fix_status" in df.columns:
        degraded_count = df.filter(pl.col("fix_status") == "degraded").height
    
    success_rate = (total - degraded_count) / total
    status = "HEALTHY"
    if degraded_count > (total * 0.1): status = "CRITICAL"
    elif degraded_count > 0: status = "DEGRADED"
        
    return {"status": status, "success_rate": round(success_rate, 4)}

async def main():
    """Main pipeline function with resilient health tracking and retries."""
    with logfire.span("Self-Healing Data Pipeline"):
        
        # --- STEP 1: LOAD DATA ---
        try:
            with logfire.span("Loading Data Source"):
                df = pl.read_csv("data/raw_inventory.csv", infer_schema_length=0)
                current_df = df.with_columns([
                    pl.lit("pending").alias("fix_status"),
                    pl.lit(None).cast(pl.Utf8).alias("fix_error")
                ])
                logfire.info("Data loaded", row_count=len(df))
        except FileNotFoundError:
            logfire.error("Data source missing")
            return

        target_schema = {"cost": pl.Float64, "timestamp": pl.Date}
        print(f"Starting Pipeline on {len(df)} rows...")

        for col, dtype in target_schema.items():
            with logfire.span(f"Processing Column: {col}"):
                try:
                    # 1. Try the easy way
                    current_df = current_df.with_columns(pl.col(col).cast(dtype))
                    current_df = current_df.with_columns(pl.lit("success").alias("fix_status"))
                    logfire.info(f"Column {col} cast successfully")
                
                except Exception as e:
                    logfire.warn(f"Healing required for {col}", error=str(e))
                    
                    # ... [Sampling and AI code here] ...

                    try:
                        # 2. Try the AI way
                        result = await get_fix_from_ai(error_context, deps)
                        fix = parse_ai_response(result.output)
                        healed_expr = eval(fix["python_code"], eval_scope)

                        # IMPORTANT: Re-assign the result to current_df
                        # We apply the fix AND cast it to the final type in one go
                        current_df = current_df.with_columns(healed_expr).with_columns(pl.col(col).cast(dtype))
                        
                        current_df = current_df.with_columns(pl.lit("success").alias("fix_status"))
                        logfire.info(f"Column {col} healed successfully")

                    except Exception as repair_err:
                        # 3. If everything fails, mark as degraded
                        current_df = current_df.with_columns([
                            pl.lit("degraded").alias("fix_status"),
                            pl.lit(str(repair_err)[:50]).alias("fix_error")
                        ])
                    # --- STEP 3: SAMPLING ---
                    failed_series = current_df.filter(
                        pl.col(col).cast(dtype, strict=False).is_null()
                    ).select(col).to_series()
                    broken_values = get_diverse_samples(failed_series, n_samples=15)

                    # # --- STEP 4: AI REPAIR WITH RETRIES ---
                    # intent = "INTENT: Numeric Safe-Cast" if col == "cost" else "INTENT: High-Effort Healing"
                    # error_context = f"Column: {col}\nTarget Type: {dtype}\n{intent}\nSample Errors: {broken_values}"

                    # try:
                    #     with logfire.span("AI Diagnosis and Repair") as ai_span:
                    #         deps = AgentDeps(column_name=col, sample_values=broken_values)
                    #         # Wrapped call with Tenacity retries
                    #         result = await get_fix_from_ai(error_context, deps)
                    #         fix = parse_ai_response(result.output)
                    #         clean_code = fix["python_code"]
                    #         ai_span.set_attribute("explanation", fix["explanation"])

                    #     # --- STEP 5: AST SANITIZATION ---
                    #     try:
                    #         tree = ast.parse(clean_code.strip())
                    #         if isinstance(tree.body[0], ast.Assign):
                    #             clean_code = ast.unparse(tree.body[0].value)
                    #         elif isinstance(tree.body[0], (ast.Expr, ast.Call)):
                    #             clean_code = ast.unparse(tree.body[0])
                    #     except SyntaxError:
                    #         pass 

                    #     # --- STEP 6: DRY RUN ---
                    #     eval_scope = {"pl": pl, "col": pl.col, "Float64": pl.Float64, "Date": pl.Date, "re": re}
                    #     healed_expr = eval(clean_code, eval_scope)
                        
                    #     current_df = current_df.with_columns(healed_expr)
                    #     current_df = current_df.with_columns(pl.lit("success").alias("fix_status"))
                    #     status = "success"
                        
                    # except Exception as err:
                    #     logfire.error(f"Healing failed for {col}", error=str(err))
                    #     current_df = current_df.with_columns([
                    #         pl.lit("degraded").alias("fix_status"),
                    #         pl.lit(str(err)[:100]).alias("fix_error")
                    #     ])
                    #     status = "failed"
                    #     clean_code = "FAILED_TO_GENERATE"
                    # --- STEP 4: AI REPAIR WITH RETRIES ---
                    intent = "INTENT: Numeric Safe-Cast" if col == "cost" else "INTENT: High-Effort Healing"
                    error_context = f"Column: {col}\nTarget Type: {dtype}\n{intent}\nSample Errors: {broken_values}"

                    try:
                        with logfire.span("AI Diagnosis and Repair") as ai_span:
                            deps = AgentDeps(column_name=col, sample_values=broken_values)
                            # This call now handles Groq 429 retries internally via Tenacity
                            result = await get_fix_from_ai(error_context, deps)
                            
                            # Using the robust parser (see helper below)
                            fix = parse_ai_response(result.output)
                            clean_code = fix["python_code"]
                            ai_span.set_attribute("explanation", fix["explanation"])

                        # --- STEP 5: AST SANITIZATION ---
                        try:
                            tree = ast.parse(clean_code.strip())
                            if isinstance(tree.body[0], ast.Assign):
                                clean_code = ast.unparse(tree.body[0].value)
                            elif isinstance(tree.body[0], (ast.Expr, ast.Call)):
                                clean_code = ast.unparse(tree.body[0])
                        except SyntaxError:
                            pass 

                        # --- STEP 6: DRY RUN & COMMIT ---
                        with logfire.span("Executing Heal"):
                            eval_scope = {"pl": pl, "col": pl.col, "Float64": pl.Float64, "Date": pl.Date, "re": re}
                            healed_expr = eval(clean_code, eval_scope)
                            
                            # CRITICAL FIX: We apply the fix AND force the cast to the target dtype
                            # This ensures the values in the CSV actually change from String to Float/Date
                            current_df = current_df.with_columns(
                                healed_expr.cast(dtype).alias(col)
                            )
                            
                            # Mark metadata as success
                            current_df = current_df.with_columns([
                                pl.lit("success").alias("fix_status"),
                                pl.lit(None).cast(pl.Utf8).alias("fix_error")
                            ])
                            status = "success"
                            logfire.info(f"Successfully committed heal for {col}")

                    except Exception as err:
                        # This block runs if AI fails, JSON is broken, or Polars crashes
                        logfire.error(f"Healing failed for {col}", error=str(err))
                        current_df = current_df.with_columns([
                            pl.lit("degraded").alias("fix_status"),
                            pl.lit(str(err)[:100]).alias("fix_error")
                        ])
                        status = "failed"
                        clean_code = "FAILED_TO_GENERATE"

                    # --- STEP 7: RATE LIMIT COOLDOWN ---
                    # Added a 3-second pause to prevent hitting the 429 rate limit between columns
                    await asyncio.sleep(3)

                    # --- STEP 7: LOG AUDIT ---
                    audit_entry = {
                        "timestamp": datetime.now().isoformat(),
                        "column": col,
                        "status": status,
                        "fix": clean_code,
                        "tokens": result.usage().total_tokens if 'result' in locals() else 0
                    }
                    with open("audit_trail.json", "a") as f:
                        f.write(json.dumps(audit_entry) + "\n")

        # --- FINAL EXPORT & HEALTH CHECK ---
        health = get_pipeline_health(current_df)
        logfire.info("Pipeline Health Report", status=health["status"], rate=health["success_rate"])
        
        current_df.write_csv("healed_data.csv")
        print(f"\nPipeline Complete. Status: {health['status']} ({health['success_rate']*100}%)")

    generate_audit_report()

if __name__ == "__main__":
    asyncio.run(main())