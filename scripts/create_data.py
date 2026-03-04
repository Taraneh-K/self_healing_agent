"""
File: create_data.py
Description: Advanced Synthetic Data Generator for Self-Healing Pipeline Testing.
Creates complex corruption patterns: mixed locales, varying date separators, 
and human-entered noise to challenge the AI's reasoning.
"""
import polars as pl
import random
import os
from datetime import datetime, timedelta

def generate_dirty_data():
    if not os.path.exists('data'):
        os.makedirs('data')

    rows = []
    base_date = datetime(2026, 1, 1)
    
    for i in range(1000):
        # --- 1. COMPLEX COST DATA (Numeric & Formatting Issues) ---
        r_cost = random.random()
        num = random.uniform(100, 2000)
        
        if r_cost > 0.85:
            # European format (comma as decimal: "1.234,50")
            cost = f"{num:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        elif r_cost > 0.75:
            # Human noise
            cost = f"Price: {num:.2f}"
        elif r_cost > 0.65:
            # Explicitly unfixable garbage (Should become None/Null)
            cost = random.choice(["ERR_VAL_99", "PENDING", "N/A", "Unknown"])
        else:
            # Standard Clean Float
            cost = f"{num:.2f}"

        # --- 2. COMPLEX TIMESTAMP DATA (Multi-Format Dates) ---
        valid_date = base_date + timedelta(days=i)
        r_date = random.random()
        
        if r_date > 0.8:
            # US Format with Slashes (MM/DD/YYYY)
            ts = valid_date.strftime("%m/%d/%Y")
        elif r_date > 0.6:
            # European/Scientific with Dots (YYYY.MM.DD)
            ts = valid_date.strftime("%Y.%m.%d")
        elif r_date > 0.5:
            # US with Dashes (MM-DD-YYYY) - Hard for Polars auto-inference
            ts = valid_date.strftime("%m-%d-%Y")
        else:
            # Standard ISO (YYYY-MM-DD)
            ts = valid_date.strftime("%Y-%m-%d")
        
        rows.append({
            "item_id": f"ID_{random.randint(1000, 9999)}",
            "cost": cost, 
            "timestamp": ts
        })
    
    df = pl.DataFrame(rows)
    df.write_csv("data/raw_inventory.csv")
    print(f"SUCCESS: Generated {len(df)} rows with complex errors in 'data/raw_inventory.csv'")

if __name__ == "__main__":
    generate_dirty_data()