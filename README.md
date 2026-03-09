# Self-Healing Data Pipeline

This project is an automated, LLM-powered data cleaning pipeline built with Polars and Pydantic-AI. It identifies structural inconsistencies in "messy" CSV data, clusters problematic rows into patterns, and uses an AI agent to generate and apply regex normalization rules to sanitize numeric and date fields.
## Key Features

    Intelligent Pattern Clustering: Automatically groups rows by structure (preserving specific digit lengths and delimiters) to isolate different date formats (e.g., MM/DD/YYYY vs YYYY.MM.DD) and numeric variations.

    AI-Driven Healing: Uses Llama-3.3 to analyze samples from these clusters and generate regex normalization rules that map diverse input formats to standardized outputs (YYYY-MM-DD for dates, 1234.56 for costs).

    Conditional Logic: Differentiates between numeric and date processing to apply the most effective cluiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii__________tering strategy for each data type.

    Observability: Integrated with Logfire to trace pattern detection, AI reasoning, and individual row transformations.

## Project Structure

    create_data.py: A synthetic data generator that simulates "real-world" noise, including mixed locales, varying date separators, and unfixable garbage values.

    src/agent.py: Defines the DataRepair Pydantic model and the system prompts for the Llama-3.3 agent.

    main.py: The primary orchestration script that handles clustering, AI request loops, rule application, and final type casting.

## Getting Started
### Prerequisites

    Python 3.11+

    A .env file containing your GROQ_API_KEY.

### Installation

    Clone the repository and navigate to the directory.

    Create and Activate Virtual Environment (Bash):
    uv venv --python 3.12
    source .venv/Scripts/activate
    
    Install the required dependencies:
    add pyproject.toml
    uv sync

### Execution

    Generate Data: Create the raw dataset with synthetic corruption (Bash):
    python create_data.py

    Run Pipeline: Clean the data and save the result (Bash):
    python main.py

## How It Works

### The pipeline follows a strict "Identify-Cluster-Repair" workflow:

    Identify: The pipeline identifies rows that fail strict type casting, preserving them as strings to prevent early truncation.

    Cluster: Rows are grouped by structural "blueprints." The pipeline uses conditional logic:

        Dates: Retains original separators to distinguish MM/DD/YYYY from YYYY-MM-DD.

        Numeric: Replaces digits and separators with generic tokens to group similar currency formats.

    Repair: For each pattern, the AI agent is sent a sample of the data and tasked with creating a regex mapping to the target format.

    Final Cast: The pipeline applies the discovered regexes and performs a final type cast to pl.Float64 or pl.Date, with strict=False to safely handle any remaining anomalies.
