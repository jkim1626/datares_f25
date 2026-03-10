# cleans LCA dataset and removes unnecessary columns and ones with too many NAs

import pandas as pd
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

COLUMN_MAPPING = {
    # Case info
    "case_number": "Case Number",
    "case_status": "Status",
    "received_date": "Received Date",
    "decision_date": "Decision Date",
    "visa_class": "Visa Class",
    "year": "Fiscal Year",

    # Job info
    "job_title": "Job Title",
    "soc_code": "SOC Code",
    "soc_title": "SOC Title",
    "full_time_position": "Full Time",
    "begin_date": "Begin Date",
    "end_date": "End Date",
    "total_worker_positions": "Total Workers",

    # Employment type flags (populated in later years)
    "new_employment": "New Employment",
    "continued_employment": "Continued Employment",
    "change_previous_employment": "Change Previous Employment",
    "new_concurrent_employment": "New Concurrent Employment",
    "change_employer": "Change Employer",
    "amended_petition": "Amended Petition",

    # Employer
    "employer_name": "Employer Name",
    "employer_city": "Employer City",
    "employer_state": "Employer State",
    "employer_postal_code": "Employer Zip",
    "naics_code": "NAICS Code",

    # Worksite
    "worksite_city": "Worksite City",
    "worksite_state": "Worksite State",
    "worksite_postal_code": "Worksite Zip",

    # Wages
    "wage_rate_of_pay_from": "Wage From",
    "wage_rate_of_pay_to": "Wage To",
    "wage_unit_of_pay": "Wage Unit",
    "prevailing_wage": "Prevailing Wage",
    "pw_unit_of_pay": "PW Unit",
    "pw_wage_level": "PW Wage Level",

    # Compliance / flags
    "h_1b_dependent": "H-1B Dependent",
    "willful_violator": "Willful Violator",
    "withdrawn": "Withdrawn",
}

print("Loading LCA data (subset of cols)...")
df = pd.read_csv(os.path.join(SCRIPT_DIR, "lca_db.csv"), usecols=lambda c: c in COLUMN_MAPPING, dtype=str)
print(f"Loaded {len(df):,} rows x {len(df.columns)} columns")

df_clean = df.rename(columns=COLUMN_MAPPING)

print("Cleaning and standardizing data...")

# Status standardization
if "Status" in df_clean.columns:
    df_clean["Status"] = df_clean["Status"].astype(str).str.strip().str.upper()
    df_clean["Status"] = df_clean["Status"].replace({
        "CERTIFIED": "Certified",
        "CERTIFIED-EXPIRED": "Certified - Expired",
        "CERTIFIED - EXPIRED": "Certified - Expired",
        "CERTIFIED-WITHDRAWN": "Certified - Withdrawn",
        "CERTIFIED - WITHDRAWN": "Certified - Withdrawn",
        "DENIED": "Denied",
        "WITHDRAWN": "Withdrawn",
        "INVALIDATED": "Invalidated",
        "NAN": None,
        "": None,
    })

# Visa class standardization
if "Visa Class" in df_clean.columns:
    df_clean["Visa Class"] = df_clean["Visa Class"].astype(str).str.strip()
    df_clean["Visa Class"] = df_clean["Visa Class"].replace({
        "R": "H-1B",
        "A": "E-3 Australian",
        "S": "H-1B1 Singapore",
        "C": "H-1B1 Chile",
        "NAN": None,
        "nan": None,
        "": None,
    })

# Date parsing
for date_col in ["Received Date", "Decision Date", "Begin Date", "End Date"]:
    if date_col in df_clean.columns:
        df_clean[date_col] = pd.to_datetime(df_clean[date_col], errors="coerce")

# Wage cleaning
for wage_col in ["Wage From", "Wage To", "Prevailing Wage"]:
    if wage_col in df_clean.columns:
        df_clean[wage_col] = pd.to_numeric(df_clean[wage_col], errors="coerce")

# Wage unit standardization
WAGE_UNIT_MAP = {
    "yr": "Year", "YEAR": "Year", "year": "Year",
    "hr": "Hour", "HOUR": "Hour", "hour": "Hour",
    "mth": "Month", "MONTH": "Month", "month": "Month",
    "wk": "Week", "WEEK": "Week", "week": "Week",
    "bi": "Bi-Weekly", "BI-WEEKLY": "Bi-Weekly", "BI": "Bi-Weekly",
    "bi-weekly": "Bi-Weekly",
}

for unit_col in ["Wage Unit", "PW Unit"]:
    if unit_col in df_clean.columns:
        df_clean[unit_col] = (
            df_clean[unit_col]
            .astype(str)
            .str.strip()
            .replace(WAGE_UNIT_MAP)
        )
        df_clean[unit_col] = df_clean[unit_col].replace({"nan": None, "NAN": None, "": None})

# Boolean standardization
YES_NO_MAP = {
    "Y": "Yes", "N": "No",
    "y": "Yes", "n": "No",
    "YES": "Yes", "NO": "No",
    "1": "Yes", "0": "No",
    "1.0": "Yes", "0.0": "No",
    "nan": None, "NAN": None, "": None,
}

for flag_col in ["Full Time", "H-1B Dependent", "Willful Violator", "Withdrawn"]:
    if flag_col in df_clean.columns:
        df_clean[flag_col] = (
            df_clean[flag_col]
            .astype(str)
            .str.strip()
            .replace(YES_NO_MAP)
        )

# Employment type flags
for emp_col in ["New Employment", "Continued Employment",
                "Change Previous Employment", "New Concurrent Employment",
                "Change Employer", "Amended Petition"]:
    if emp_col in df_clean.columns:
        df_clean[emp_col] = pd.to_numeric(df_clean[emp_col], errors="coerce")
        # Convert to nullable int so NaN is preserved but values are 0/1
        df_clean[emp_col] = df_clean[emp_col].astype("Int64")

# String cleanup
text_columns = ["Job Title", "SOC Code", "SOC Title", "Employer Name",
                "Employer City", "Employer State", "Employer Zip",
                "Worksite City", "Worksite State", "Worksite Zip",
                "PW Wage Level", "NAICS Code", "Case Number"]

for col in text_columns:
    if col in df_clean.columns:
        df_clean[col] = df_clean[col].astype(str).str.strip()
        df_clean[col] = df_clean[col].replace({"nan": None, "NAN": None, "": None})

# Deduplication
before_dedup = len(df_clean)
df_clean = df_clean.drop_duplicates()
dupes_removed = before_dedup - len(df_clean)

# Save
output_file = os.path.join(SCRIPT_DIR, "lca_clean.csv")
df_clean.to_csv(output_file, index=False)

# Summary
print("\n============================================================")
print("CLEANING COMPLETE")
print("============================================================")
print(f"Original rows:    {len(df):,}")
print(f"Final rows:       {len(df_clean):,}")
print(f"Duplicates removed: {dupes_removed:,}")
print(f"Original columns: {len(df.columns)}")
print(f"Final columns:    {len(df_clean.columns)}")
print(f"Output saved to:  {output_file}")

print("\n============================================================")
print("DATA SUMMARY")
print("============================================================")

if "Decision Date" in df_clean.columns:
    print("\nDecision Date Range:")
    print(f"  {df_clean['Decision Date'].min()} to {df_clean['Decision Date'].max()}")

if "Fiscal Year" in df_clean.columns:
    print("\nFiscal Year Breakdown:")
    print(df_clean["Fiscal Year"].value_counts().sort_index().to_string())

if "Status" in df_clean.columns:
    print("\nStatus Breakdown:")
    print(df_clean["Status"].value_counts().to_string())

if "Visa Class" in df_clean.columns:
    print("\nVisa Class Breakdown:")
    print(df_clean["Visa Class"].value_counts().to_string())

if "Wage Unit" in df_clean.columns:
    print("\nWage Unit Breakdown:")
    print(df_clean["Wage Unit"].value_counts().to_string())

if "Full Time" in df_clean.columns:
    print("\nFull Time Breakdown:")
    print(df_clean["Full Time"].value_counts().to_string())

if "H-1B Dependent" in df_clean.columns:
    print("\nH-1B Dependent Breakdown:")
    print(df_clean["H-1B Dependent"].value_counts().to_string())

# Show remaining NA percentages
print("\nRemaining NA % per column:")
na_pct = (df_clean.isna().sum() / len(df_clean) * 100).sort_values(ascending=False)
for col, pct in na_pct.items():
    print(f"  {pct:6.1f}%  {col}")
