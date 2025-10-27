import subprocess, os

# Ensure ./data exists (both locally and in container)
os.makedirs("./data", exist_ok=True)

# Run monthly then yearly; fail if either errors
subprocess.check_call(["python", "scrape_monthly.py"])
subprocess.check_call(["python", "scrape_yearly.py"])