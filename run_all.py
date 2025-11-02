from re import sub
import subprocess, os
import sys

# Ensure ./data exists (both locally and in container)
os.makedirs("./data", exist_ok=True)

# Ensure ./data/visa_statistics exists
os.makedirs("./data/visa_statistics", exist_ok=True)

try:
    subprocess.check_call(["python", "crawl.py"])
except subprocess.CalledProcessError as e:
    print(f"crawl.py failed: {e}", file=sys.stderr)
    sys.exit(1)

# Run monthly then yearly; fail if either errors
try:
    subprocess.check_call(["python", "scrape_monthly.py"])
except subprocess.CalledProcessError as e:
    print(f"scrape_monthly.py failed: {e}", file=sys.stderr)
    sys.exit(1)

try:    
    subprocess.check_call(["python", "scrape_yearly.py"])
except subprocess.CalledProcessError as e:
    print(f"scrape_yearly.py failed: {e}", file=sys.stderr)
    sys.exit(1)