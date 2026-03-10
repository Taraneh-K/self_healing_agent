# Self-Healing Data Pipeline

This project is an automated, LLM-powered data cleaning pipeline built with Polars and Pydantic-AI. It identifies structural inconsistencies in "messy" CSV data, clusters problematic rows into patterns, and uses an AI agent to generate and apply regex normalization rules to sanitize numeric and date fields.
## Key Features

- Intelligent Pattern Clustering: Automatically groups rows by structure (preserving specific digit lengths and delimiters) to isolate different date formats (e.g., MM/DD/YYYY vs YYYY.MM.DD) and numeric variations.

- AI-Driven Healing: Uses Llama-3.3 to analyze samples from these clusters and generate regex normalization rules that map diverse input formats to standardized outputs (YYYY-MM-DD for dates, 1234.56 for costs).

- Conditional Logic: Differentiates between numeric and date processing to apply the most effective clustering strategy for each data type.

- Observability: Integrated with Logfire to trace pattern detection, AI reasoning, and individual row transformations.

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

1. Identify: The pipeline identifies rows that fail strict type casting, preserving them as strings to prevent early truncation.

2. Cluster: Rows are grouped by structural "blueprints." The pipeline uses conditional logic:

   Dates: Retains original separators to distinguish MM/DD/YYYY from YYYY-MM-DD.

   Numeric: Replaces digits and separators with generic tokens to group similar currency formats.

3. Repair: For each pattern, the AI agent is sent a sample of the data and tasked with creating a regex mapping to the target format.

4. Final Cast: The pipeline applies the discovered regexes and performs a final type cast to pl.Float64 or pl.Date, with strict=False to safely handle any remaining anomalies.

## Sample of Logfire log

<img width="1748" height="755" alt="image" src="https://github.com/user-attachments/assets/c1ef88bf-3224-4677-ade2-c3203cbf1e66" />

# Key Learnings & Takeaways

This project served as an intensive experiment in applying Agentic AI directly to raw, un-sanitized data. Here are the core insights gathered from the development process:

- The "Long Prompt" Trap: Early iterations used long system prompts attempting to cover every edge case. This proved to be overkill. The model often ignored specific instructions when the context window became too cluttered, leading to inconsistent reasoning. Keep it focused: Break tasks into granular, type-specific prompts.

- The Power of Structural Clustering: Instead of sending every single row to the LLM—which would be prohibitively expensive and prone to hallucinations, we first group data into structural "blueprints" using regex abstraction. By only sending a representative sample of each unique pattern to the AI, we drastically reduce token consumption and latency while providing the model with clear, consistent context. This targeted approach transforms the task from an inefficient row-by-row guessing game into a scalable, high-accuracy mapping process.

- Model Performance & Ambiguity: Throughout testing, different models exhibited varying levels of "instruction following." While some models handled currency reasonably well, they often "flipped" dates (e.g., interpreting 01/10 as October 1st instead of January 10th). It was necessary to give a hint for that in the system prompt.

- Cost Optimization: While this project used free services and proved that AI can handle chaotic data, it is not always the most efficient path. In a production environment, apply manual, regex-based fixes first for known, deterministic formats (like standard ISO dates or known currency structures). Reserve expensive LLM calls for truly ambiguous, human-entered "junk" that cannot be easily defined by a standard rule.

- The Importance of Delimiters: By preserving original separators (like / or .), you give the LLM essential cues about the regional format of the data.

- Schema-First Design: Using Pydantic models (like DataRepair) is essential. It forces the LLM into a structured output format, making the results immediately usable by your downstream processing code.

- Observability is Mandatory: Without tools like Logfire to trace the "before and after" of every pattern repair, you are flying blind. You need to see exactly which row was affected by which regex to verify that the agent didn't make a "flipping" error.
