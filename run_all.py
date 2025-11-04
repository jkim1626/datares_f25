import subprocess
import sys
import os

# Ensure base directories exist
os.makedirs("/data/visa_stats", exist_ok=True)

print("=" * 60)
print("Starting visa statistics scraper pipeline")
print("=" * 60)

# Step 1: Reconcile manifest with disk
print("\n[1/3] Reconciling manifest with disk files...")
try:
    subprocess.check_call(["python", "crawl.py"])
except subprocess.CalledProcessError as e:
    print(f"⚠️  crawl.py failed: {e}", file=sys.stderr)
    print("Continuing with scraping anyway...", file=sys.stderr)

# Step 2: Scrape monthly files
print("\n[2/3] Scraping monthly visa statistics...")
try:
    subprocess.check_call(["python", "scrape_monthly.py"])
except subprocess.CalledProcessError as e:
    print(f"❌ scrape_monthly.py failed: {e}", file=sys.stderr)
    sys.exit(1)

# Step 3: Scrape annual files
print("\n[3/3] Scraping annual visa statistics...")
try:
    subprocess.check_call(["python", "scrape_yearly.py"])
except subprocess.CalledProcessError as e:
    print(f"❌ scrape_yearly.py failed: {e}", file=sys.stderr)
    sys.exit(1)

print("\n" + "=" * 60)
print("✅ Pipeline complete!")
print("=" * 60)
