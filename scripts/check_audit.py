"""
File: audit_report.py
Description: Analytics and FinOps Reporting for the Self-Healing Pipeline.
This script parses the 'audit_trail.json' (NDJSON format) to calculate 
agent success rates, column-level performance, and token efficiency gains.
It provides visibility into the 'FinOps' aspect of using AI for data engineering.
"""
import polars as pl
from pathlib import Path

def generate_audit_report():
    """
    Reads the audit log and prints a formatted summary of AI performance.
    Logic:
    - Calculates Success/Failure/Rejection ratios.
    - Groups data by column to find the average cost (tokens) per repair.
    - Estimates 'Efficiency Gain' by comparing sampled repair vs. full dataset repair.
    """
    audit_file = Path("audit_trail.json")
    
    if not audit_file.exists():
        print("No audit trail found. Run the pipeline to generate data!")
        return

    try:
        # Read the newline-delimited JSON audit trail
        # Note: We use read_ndjson because we append one JSON object per line
        audit_df = pl.read_ndjson(audit_file)
        
        print("\n" + "="*60)
        print("AI AGENT PERFORMANCE & FINOPS REPORT")
        print("="*60)
        
        # 1. Overall Performance Stats
        total = len(audit_df)
        successes = audit_df.filter(pl.col("status") == "success").height
        rejections = audit_df.filter(pl.col("status") == "rejected").height
        failures = audit_df.filter(pl.col("status") == "failed").height
        
        print(f"Total Interventions: {total}")
        print(f"Successful Heals:   {successes}")
        print(f"Human Rejections:   {rejections}")
        print(f"AI Fix Failures:     {failures}")

        # 2. Multi-Column Breakdown
        print("\nBY COLUMN TYPE:")
        # Group by the column name to see specific performance
        col_summary = audit_df.group_by("column").agg([
            pl.count("status").alias("count"),
            pl.col("tokens").mean().cast(pl.Int64).alias("avg_tokens")
        ])
        print(col_summary)

        # 3. Token Efficiency (FinOps)
        # Match the keys from the updated main.py ('tokens', 'total_rows_processed')
        total_tokens = audit_df["tokens"].sum()
        
        # If total_rows_processed exists in your JSON (Point 5 in main)
        if "total_rows_processed" in audit_df.columns:
            total_rows = audit_df["total_rows_processed"].sum()
        else:
            # Fallback if the key is missing
            total_rows = 1000 * total 

        # FinOps Logic:
        # unoptimized = (every row sent to LLM ~100 tokens) + (prompt overhead ~500 tokens)
        est_raw_tokens = (total_rows * 100) + (total * 500)
        efficiency = (1 - (total_tokens / est_raw_tokens)) * 100 if est_raw_tokens > 0 else 0

        print(f"\nFINOPS METRICS:")
        print(f"Total Tokens Consumed: {total_tokens:,}")
        print(f"Efficiency Gain:       {max(0, efficiency):.1f}% (via Sampling)")

        # 4. Latest Intervention Details
        latest = audit_df.tail(1)
        if len(latest) > 0:
            print(f"\nLATEST INTERVENTION ({latest['timestamp'][0]})")
            print(f"Column: {latest['column'][0].upper()}")
            print(f"Status: {latest['status'][0].upper()}")
            print(f"Code:   {latest['fix'][0]}")
        
        print("="*60 + "\n")

    except Exception as e:
        print(f"Error generating report: {e}")

if __name__ == "__main__":
    generate_audit_report()