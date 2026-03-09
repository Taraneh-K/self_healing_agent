"""
Agent Module for Data Repair
----------------------------
This module utilizes the PydanticAI Agent to interpret structural irregularities
in raw data and generate robust regex-based normalization rules.
"""
import os
from dotenv import load_dotenv
from pydantic import BaseModel, Field
from pydantic_ai import Agent

# Load environment variables
load_dotenv()

# Ensure API Key is present
if not os.getenv("GROQ_API_KEY"):
    raise EnvironmentError("GROQ_API_KEY is not set in your .env file.")

class DataRepair(BaseModel):
    """
    Defines the structured output for the repair agent.
    The agent must return an instance of this model for every repair request.
    """
    explanation: str = Field(description="Description of the detected format (e.g., 'US Date' or 'EU Currency').")
    # repairs: Dict[str, str] = Field(description="Mapping of every unique sample to its fixed version.The normalized string. Dates MUST be 'YYYY-MM-DD'. Numbers MUST be '1234.56'.")
    regex_pattern: str = Field(description="Regex with capture groups () to extract relevant data components.")
    replacement_format: str = Field(description="The template string. Dates MUST be 'YYYY-MM-DD'. Numbers MUST be '1234.56'.")


# Agent configured as a Data Normalizer
data_repair_agent = Agent(
    'groq:llama-3.3-70b-versatile',
    output_type=DataRepair,
    system_prompt=(
        "You are a data normalization agent. "
        "Return ONLY a valid JSON object matching the DataRepair schema. "
        "Do not use function-call syntax, do not add introductory text. "
        "1. For DATE columns: Convert any format to ISO 8601 'YYYY-MM-DD'. "
        "If this is a date column, prioritize using pl.to_datetime or pl.strptime logic. Return a regex only if a standard string replace can fully normalize the date format to YYYY-MM-DD."
        "This data uses the US format: MONTH/DAY/YEAR."
        "2. For NUMERIC columns: Convert any currency/format to a clean float string '1234.56'. If it cannot be fixed, return None instead of 0"
        )
    )
