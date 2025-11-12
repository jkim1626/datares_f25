import subprocess
import sys
import os

# Ensure base directories exist
os.makedirs("/data/visa_stats", exist_ok=True)
os.makedirs("/data/performance_data", exist_ok=True)
os.makedirs("/data/immigration_yearbook", exist_ok=True)
os.makedirs("/data/uscis_data", exist_ok=True)

print("=" * 60)
print("Starting FULL data scraper pipeline (all 5 sources)")
print("=" * 60)

# Step 1: Reconcile manifest with disk
print("\n[1/6] Reconciling manifest with disk files...")
try:
    subprocess.check_call(["python", "crawl.py"])
except subprocess.CalledProcessError as e:
    print(f"⚠️  crawl.py failed: {e}", file=sys.stderr)
    print("Continuing with scraping anyway...", file=sys.stderr)

# Step 2: Scrape monthly visa statistics
print("\n[2/6] Scraping monthly visa statistics...")
try:
    subprocess.check_call(["python", "scrape_monthly.py"])
except subprocess.CalledProcessError as e:
    print(f"❌ scrape_monthly.py failed: {e}", file=sys.stderr)
    sys.exit(1)

# Step 3: Scrape annual visa statistics
print("\n[3/6] Scraping annual visa statistics...")
try:
    subprocess.check_call(["python", "scrape_yearly.py"])
except subprocess.CalledProcessError as e:
    print(f"❌ scrape_yearly.py failed: {e}", file=sys.stderr)
    sys.exit(1)

# Step 4: Scrape DOL performance data
print("\n[4/6] Scraping DOL performance data...")
try:
    subprocess.check_call(["python", "scrape_dol.py"])
except subprocess.CalledProcessError as e:
    print(f"❌ scrape_dol.py failed: {e}", file=sys.stderr)
    sys.exit(1)

# Step 5: Scrape DHS immigration yearbooks
print("\n[5/6] Scraping DHS immigration yearbooks...")
try:
    subprocess.check_call(["python", "scrape_yearbook.py"])
except subprocess.CalledProcessError as e:
    print(f"❌ scrape_yearbook.py failed: {e}", file=sys.stderr)
    sys.exit(1)

# Step 6: Scrape USCIS employer data
print("\n[6/6] Scraping USCIS employer data...")
try:
    subprocess.check_call(["python", "scrape_uscis.py"])
except subprocess.CalledProcessError as e:
    print(f"❌ scrape_uscis.py failed: {e}", file=sys.stderr)
    sys.exit(1)

print("\n" + "=" * 60)
print("✅ Pipeline complete!")
print("=" * 60)
print("\nData sources scraped:")
print("  ✅ Visa Statistics (Monthly & Annual)")
print("  ✅ DOL Performance Data")
print("  ✅ DHS Immigration Yearbooks")
print("  ✅ USCIS Employer Data (H-1B, H-2A, H-2B)")
print("=" * 60)
