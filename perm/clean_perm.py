# cleans perm dataset and removes unnecessary columns and ones with too many NAs

import pandas as pd

print("Loading PERM data...")
df = pd.read_csv("perm_db.csv", low_memory=False)

print(f"Original shape: {df.shape}")
print(f"Total columns: {len(df.columns)}")

COLUMN_MAPPING = {
    "case_number": "Case Number",
    "case_status": "Status",
    "decision_date": "Decision Date",
    "year": "Fiscal Year",
    "form_type": "Form Type",
    "class_of_admission": "Current Visa Type",
}

available_columns = [col for col in COLUMN_MAPPING if col in df.columns]
df_clean = df[available_columns].copy()
df_clean = df_clean.rename(columns=COLUMN_MAPPING)

print(f"\nSelected {len(available_columns)} core columns")
print("Cleaning and standardizing data...")

if "Status" in df_clean.columns:
    df_clean["Status"] = df_clean["Status"].astype(str).str.strip().str.upper()
    df_clean["Status"] = df_clean["Status"].replace({
        "CERTIFIED": "Certified",
        "CERTIFIED-EXPIRED": "Certified - Expired",
        "CERTIFIED - EXPIRED": "Certified - Expired",
        "DENIED": "Denied",
        "WITHDRAWN": "Withdrawn",
        "INVALIDATED": "Invalidated",
        "NAN": None,
        "": None,
    })

if "Decision Date" in df_clean.columns:
    df_clean["Decision Date"] = pd.to_datetime(df_clean["Decision Date"], errors="coerce")

if "Form Type" in df_clean.columns:
    df_clean["Form Type"] = df_clean["Form Type"].astype(str).str.strip().str.upper()
    df_clean["Form Type"] = df_clean["Form Type"].replace({"NAN": None, "": None})

if "Current Visa Type" in df_clean.columns:
    df_clean["Current Visa Type"] = df_clean["Current Visa Type"].astype(str).str.strip().str.upper()
    df_clean["Current Visa Type"] = df_clean["Current Visa Type"].replace({"NAN": None, "": None})

df_clean = df_clean.drop_duplicates()

output_file = "perm_clean.csv"
df_clean.to_csv(output_file, index=False)

print("\n============================================================")
print("CLEANING COMPLETE")
print("============================================================")
print(f"Original rows: {len(df):,}")
print(f"Final rows: {len(df_clean):,}")
print(f"Original columns: {len(df.columns)}")
print(f"Final columns: {len(df_clean.columns)}")
print(f"Output saved to: {output_file}")
print("\n============================================================")
print("DATA SUMMARY")
print("============================================================")

if "Decision Date" in df_clean.columns:
    print("\nDecision Date Range:")
    print(f"  {df_clean['Decision Date'].min()} to {df_clean['Decision Date'].max()}")

if "Status" in df_clean.columns:
    print("\nStatus Breakdown:")
    print(df_clean["Status"].value_counts().head(10).to_string())

if "Form Type" in df_clean.columns:
    print("\nForm Type Breakdown:")
    print(df_clean["Form Type"].value_counts().head(10).to_string())

if "Current Visa Type" in df_clean.columns:
    print("\nCurrent Visa Type Breakdown:")
    print(df_clean["Current Visa Type"].value_counts().head(10).to_string())
