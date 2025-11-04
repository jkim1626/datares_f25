import subprocess
import sys
import os

# Ensure base directories exist
os.makedirs("/data/visa_stats", exist_ok=True)

print("=" * 60)
print("TEST MODE: Quick Pipeline Test")
print("Downloads 1 monthly file + 1 annual file")
print("=" * 60)

# Step 1: Test monthly scraper (1 file)
print("\n[1/2] Testing monthly scraper (1 file only)...")
try:
    subprocess.check_call(["python", "test_monthly.py"])
except subprocess.CalledProcessError as e:
    print(f"❌ test_monthly.py failed: {e}", file=sys.stderr)
    sys.exit(1)

# Step 2: Test annual scraper (1 file)
print("\n[2/2] Testing annual scraper (1 file only)...")
try:
    subprocess.check_call(["python", "test_annual.py"])
except subprocess.CalledProcessError as e:
    print(f"❌ test_annual.py failed: {e}", file=sys.stderr)
    sys.exit(1)

print("\n" + "=" * 60)
print("✅ Test pipeline complete!")
print("=" * 60)
print("\nWhat was tested:")
print("  ✅ Database connection")
print("  ✅ Table creation (file_manifest)")
print("  ✅ File download (1 monthly + 1 annual)")
print("  ✅ File storage in volume")
print("  ✅ Metadata tracking in database")
print("\nNext step: Run full pipeline with run_all.py")
print("=" * 60)