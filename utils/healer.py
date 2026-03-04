import polars as pl
import json
import re
from datetime import datetime

def parse_ai_response(response_text: str):
    """Extracts JSON and handles backslash escape errors."""
    match = re.search(r"(\{.*\})", response_text, re.DOTALL)
    if not match:
        return None
    
    json_str = match.group(1)
    try:
        # Try standard load first
        return json.loads(json_str)
    except json.JSONDecodeError:
        try:
            # Healing: Fix backslash issues like \d or \s
            fixed_str = json_str.encode('utf-8').decode('unicode_escape')
            return json.loads(fixed_str)
        except:
            return None

def get_pipeline_health(df: pl.DataFrame):
    """Generates a health status based on the video's logic."""
    total = len(df)
    if total == 0:
        return {"status": "INACTIVE", "success_rate": 0}

    degraded_count = df.filter(pl.col("fix_status") == "degraded").height
    success_rate = (total - degraded_count) / total
    
    # Thresholds from the video
    if degraded_count > (total * 0.1):
        status = "CRITICAL"
    elif degraded_count > 0:
        status = "DEGRADED"
    else:
        status = "HEALTHY"
        
    return {
        "status": status,
        "success_rate": round(success_rate, 4),
        "timestamp": datetime.now().isoformat()
    }