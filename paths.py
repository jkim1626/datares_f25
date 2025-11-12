# paths.py
import os
import re
from pathlib import Path

# Railway volume is mounted at /data
DATA_ROOT = Path(os.environ.get("DATA_ROOT", "/data")).resolve()

# Main directory structure for each data source
VISA_STATS_DIR = DATA_ROOT / "visa_stats"
DOL_DIR = DATA_ROOT / "performance_data"
YEARBOOK_DIR = DATA_ROOT / "immigration_yearbook"
USCIS_DIR = DATA_ROOT / "uscis_data"

# Create base directories
VISA_STATS_DIR.mkdir(parents=True, exist_ok=True)
DOL_DIR.mkdir(parents=True, exist_ok=True)
YEARBOOK_DIR.mkdir(parents=True, exist_ok=True)
USCIS_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================================
# VISA STATISTICS PATHS (existing)
# ============================================================================

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


# ============================================================================
# DOL PERFORMANCE DATA PATHS (new)
# ============================================================================

def get_dol_outdir(program: str, year: str) -> Path:
    """
    Get output directory for DOL performance data files.
    
    Args:
        program: e.g., "PERM Program", "LCA Program", "H-2A Program"
        year: e.g., "2024"
    
    Returns:
        Path like /data/performance_data/PERM Program/2024/
    """
    # Sanitize program name for filesystem
    safe_program = re.sub(r'[\\/*?:"<>|]', '_', program.strip())
    path = DOL_DIR / safe_program / year
    path.mkdir(parents=True, exist_ok=True)
    return path


# ============================================================================
# IMMIGRATION YEARBOOK PATHS (new)
# ============================================================================

def get_yearbook_outdir(year: str) -> Path:
    """
    Get output directory for immigration yearbook files.
    
    Args:
        year: e.g., "2024"
    
    Returns:
        Path like /data/immigration_yearbook/2024/
    """
    path = YEARBOOK_DIR / year
    path.mkdir(parents=True, exist_ok=True)
    return path


# ============================================================================
# USCIS EMPLOYER DATA PATHS (new)
# ============================================================================

def get_uscis_outdir(visa_type: str, year_or_misc: str) -> Path:
    """
    Get output directory for USCIS employer data files.
    
    Args:
        visa_type: "h1b", "h2a", or "h2b"
        year_or_misc: e.g., "2024" or "misc"
    
    Returns:
        Path like /data/uscis_data/h1b/2024/ or /data/uscis_data/h1b/misc/
    """
    path = USCIS_DIR / visa_type / year_or_misc
    path.mkdir(parents=True, exist_ok=True)
    return path
