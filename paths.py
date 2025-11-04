# paths.py
import os
from pathlib import Path

# Railway volume is mounted at /data
DATA_ROOT = Path(os.environ.get("DATA_ROOT", "/data")).resolve()

# Main directory structure
VISA_STATS_DIR = DATA_ROOT / "visa_stats"

# Create base directories
VISA_STATS_DIR.mkdir(parents=True, exist_ok=True)

def get_monthly_outdir(program: str, period: str) -> Path:
    """
    Get output directory for monthly files.
    
    Args:
        program: "IV" or "NIV"
        period: e.g., "FY2024-10"
    
    Returns:
        Path like /data/visa_stats/monthly/IV/FY2024/FY2024-10/
    """
    # Extract fiscal year from period (e.g., "FY2024-10" -> "FY2024")
    import re
    fy_match = re.match(r"(FY\d{4})", period)
    
    if fy_match:
        fiscal_year = fy_match.group(1)
        path = VISA_STATS_DIR / "monthly" / program / fiscal_year / period
    else:
        # Fallback for periods without FY format
        path = VISA_STATS_DIR / "monthly" / program / period
    
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_annual_outdir(year: str) -> Path:
    """
    Get output directory for annual files.
    
    Args:
        year: e.g., "2024"
    
    Returns:
        Path like /data/visa_stats/annual/2024/
    """
    path = VISA_STATS_DIR / "annual" / year
    path.mkdir(parents=True, exist_ok=True)
    return path
