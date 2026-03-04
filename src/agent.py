"""
File: agent.py
Description: Pydantic-AI Agent configuration for data healing.
Uses a Sense-Think-Act loop with a validation tool to ensure code accuracy.
"""
from dotenv import load_dotenv
from pydantic_ai import Agent, RunContext
import polars as pl
from dataclasses import dataclass

load_dotenv()

@dataclass
class AgentDeps:
    column_name: str
    sample_values: list[str]

# --- TOOLS ---

def test_polars_expression(ctx: RunContext[AgentDeps], expression: str) -> str:
    """
    Validation tool that executes the AI's suggested Polars expression 
    against the identified broken samples. Returns a detailed failure 
    report if nulls are created in 'Healable' columns.
    """
    try:
        # Create a test series from the sample values provided in dependencies
        test_series = pl.Series("test_col", ctx.deps.sample_values)
        
        # Replace the AI's guessed column name with our internal 'test_col'
        clean_expr_str = expression.replace(ctx.deps.column_name, "test_col")
        
        # Evaluate the string as a Polars expression
        test_expr = eval(clean_expr_str, {"pl": pl})
        
        # Apply the expression to a temporary DataFrame
        result = test_series.to_frame().with_columns(test_expr)
        
        # Identify how many values failed to convert (became null)
        null_count = result.select(pl.all().is_null().sum()).to_series()[0]
        
        if null_count > 0:
            return (f"FAILURE: {null_count}/{len(ctx.deps.sample_values)} rows became null. "
                    "This means your format string or regex is likely incorrect. "
                    "Try using pl.coalesce to handle multiple formats.")
            
        return "SUCCESS: All samples transformed correctly."
    except Exception as e:
        return f"FAILURE: Python error during execution: {str(e)}"

# --- AGENT CONFIGURATION ---

data_fix_agent = Agent(
    'groq:llama-3.3-70b-versatile',
    deps_type=AgentDeps,
    output_type=str,  # Set to str to handle potential LLM conversational noise
    retries=3,
    system_prompt=(
        "You are an elite Data Engineering Agent specializing in Polars."
        "\n\n--- YOUR OBJECTIVE ---"
        "Generate a Polars expression that repairs data types for a specific column. "
        "Use the 'test_polars_expression' tool to verify your code before providing a final answer."
        "Return ONLY a JSON object. No conversation. No markdown blocks."
        "Required Keys:"
        "- explanation: Max 10 words."
        "- python_code: A single Polars expression using pl.col()."

        "Example: {'explanation': 'Cleaned currency', 'python_code': \"pl.col('cost').str.replace('$', '')\"}"

        "\n\n--- STRATEGY BY INTENT ---"
        "1. NUMERIC SAFE-CAST: Goal is to convert to Float64. Use .cast(pl.Float64, strict=False). "
        "   If the samples contain currency symbols ($, €) or text (e.g. 'Price: '), "
        "   use .str.replace or .str.strip_chars first. Unfixable text should become null."
        
        "2. HIGH-EFFORT HEALING (Dates): Goal is to avoid nulls at all costs. "
        "   Mixed delimiters (/, -, .) require 'pl.coalesce' with multiple 'str.to_date' format strings. "
        "   - Example: pl.coalesce([pl.col('c').str.to_date('%Y-%m-%d', strict=False), "
        "                          pl.col('c').str.to_date('%m/%d/%Y', strict=False)])"

        "\n\n--- OUTPUT FORMAT ---"
        "You must return ONLY a JSON object. Do not include tool tags like <function>."
        "{"
        "  'explanation': 'Briefly describe why the previous cast failed and how you fixed it.',"
        "  'python_code': 'The valid Polars expression (e.g., pl.col(...)...)'"
        "}"
    )
)

# Register the validation tool
data_fix_agent.tool(test_polars_expression)