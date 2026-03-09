"""
Self-Healing Data Pipeline
--------------------------
This module automates the cleaning of messy CSV data by identifying structural 
patterns in 'broken' rows and utilizing an LLM-powered agent to generate 
normalization regex rules. It specifically targets type-casting failures 
for numeric and date fields.
"""
import asyncio
import polars as pl
import logfire
from src.agent import data_repair_agent

logfire.configure()

async def main():
    """
    Executes the multi-format healing loop.
    1. Loads data as strings to prevent early truncation.
    2. Identifies 'messy' rows that fail standard type casting.
    3. Groups messy data into structural 'patterns' (e.g., '00-00-0000').
    4. Requests regex normalization rules from the AI agent per pattern.
    5. Applies fixes scoped to specific patterns and performs a final cast.
    """
    with logfire.span("Multi-Format Rule-Based Healing"):
        # Load raw data as strings to preserve all formatting until healed
        df = pl.read_csv("data/raw_inventory.csv", infer_schema_length=0)
        
        target_schema = {
            "cost": pl.Float64,
            "timestamp": pl.Date
        }

        for col, dtype in target_schema.items():
            print(f"\n--- PROCESSING COLUMN: {col} ---")
            # Create a span for the column processing
            with logfire.span("Processing column {column}", column=col):            
                # 1. Pre-clean literal junk to actual nulls
                df = df.with_columns(pl.col(col).replace({"None": None, "null": None}))

                # 2. Identify messy rows that fail to cast
                is_messy_mask = df[col].cast(dtype, strict=False).is_null() & df[col].is_not_null()
                
                if is_messy_mask.any():
                    messy_df = df.filter(is_messy_mask)
                    # 3. Conditional Clustering: Use logic appropriate for the data type
                    if col == "timestamp":
                        # Preserve digit sequence lengths to differentiate format structures
                        pattern_logic = (
                            pl.col(col).cast(pl.String)
                            .str.replace_all(r"\d{4}", "YYYY")
                            .str.replace_all(r"\d{1,2}", "DD")
                        )
                    else:
                        # Cluster based on separators and digit presence
                        pattern_logic = (
                            pl.col(col).cast(pl.String)
                            .str.replace_all(r"\d+", "0")
                            .str.replace_all(r"[a-zA-Z]", "X")
                        )
                    # 3. Cluster patterns to group similar "junk"
                    clusters = (
                        messy_df.with_columns(
                            pattern=pattern_logic
                        )
                        .group_by("pattern")
                        .agg(
                            # 1. Get the count of rows for this pattern
                            count=pl.len(), 
                            # 2. Get the unique samples
                            samples=pl.col(col).unique()
                        )
                        .sort("count", descending=True) # Optional: Fix the biggest problems first
                    )                    
                    for row in clusters.iter_rows(named=True):
                        samples = row['samples']
                        pattern = row['pattern']
                        count = row['count']

                        # Create a span for this specific pattern
                        with logfire.span("Healing pattern {pattern}", pattern=pattern) as span:                        
                            # 4. Get the AI's regex rule for this pattern
                            result = await data_repair_agent.run(
                                f"Target Type: {dtype}. Samples: {list(samples[:3])}. "
                                "Return a regex with capture groups and a replacement format (e.g., '$1.$2' or '$3-$1-$2') to normalize the data."
                            )
                            
                            ai_rule = result.output
                            print(f"PATTERN: {pattern} -> {ai_rule.explanation}")
                            print(f"RULE: Match '{ai_rule.regex_pattern}' -> Replace with '{ai_rule.replacement_format}'")
                            
                            
                            # Add metadata to logfire
                            span.set_attribute("regex", ai_rule.regex_pattern)
                            span.set_attribute("replacement_format", ai_rule.replacement_format)
                            span.set_attribute("rows_pattern", count)
                            
                            with logfire.span("regex pattern {pattern}", pattern=ai_rule.regex_pattern) as span:                            
                                # 5. Apply the rule globally to the entire column
                                # 1. Define the mask (a boolean series of where the change actually happens)

                                mask = pl.col(col).str.contains(ai_rule.regex_pattern)

                                # 2. Get the count of rows that will be affected
                                rows_affected = df.select(mask.sum()).item()

                                # 3. Apply the transformation
                                df = df.with_columns(
                                    pl.when(mask)
                                    .then(pl.col(col).str.replace(ai_rule.regex_pattern, ai_rule.replacement_format))
                                    .otherwise(pl.col(col))
                                    .alias(col)
                                )

                                print(f"Pattern {pattern} affected {rows_affected} rows.")
                                span.set_attribute("rows_affected", rows_affected)
                                df = df.with_columns(
                                    pl.when(pl.col(col).is_in(["None", "NaN", "nan"]))
                                    .then(None)
                                    .otherwise(pl.col(col))
                                    .alias(col)
                                )
                
            # 6. Final cast to the target type
            df = df.with_columns(pl.col(col).cast(dtype, strict=False))

        # Final cleanup: Ensure impossible values are now truly null
        df.write_csv("healed_data.csv")
        print("\nPipeline Complete. Healed data saved to healed_data.csv.")

if __name__ == "__main__":
    asyncio.run(main())