import subprocess
import sys
import os

# Ensure base directories exist
os.makedirs("/data/visa_stats", exist_ok=True)
os.makedirs("/data/performance_data", exist_ok=True)
os.makedirs("/data/immigration_yearbook", exist_ok=True)
os.makedirs("/data/uscis_data", exist_ok=True)

print("=" * 60)
print("TEST MODE: Complete Pipeline Test")
print("Downloads 1 file from each data source")
print("=" * 60)

test_scrapers = [
    ("test_monthly.py", "Visa Monthly"),
    ("test_annual.py", "Visa Annual"),
    ("test_dol.py", "DOL Performance"),
    ("test_yearbook.py", "DHS Yearbook"),
    ("test_uscis.py", "USCIS Employer"),
]

for i, (script, name) in enumerate(test_scrapers, 1):
    print(f"\n[{i}/{len(test_scrapers)}] Testing {name} scraper (1 file only)...")
    try:
        subprocess.check_call(["python", script])
    except subprocess.CalledProcessError as e:
        print(f"❌ {script} failed: {e}", file=sys.stderr)
        sys.exit(1)

print("\n" + "=" * 60)
print("✅ Test pipeline complete!")
print("=" * 60)
print("\nWhat was tested:")
print("  ✅ Database connection")
print("  ✅ Table creation (file_manifest)")
print("  ✅ File download from all 5 sources")
print("  ✅ File storage in volume")
print("  ✅ Metadata tracking in database")
print("\nNext step: Run full pipeline with run_all.py")
print("=" * 60)
