import pandas as pd
import os

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(PROJECT_ROOT)
BASE_PATH = os.path.join(REPO_ROOT, "data", "LCA Program")

print("Using LCA root:", BASE_PATH)

# ---------------------------------------------------------
# 1. FINAL SCHEMA (canonical unified column set)
# ---------------------------------------------------------
FINAL_SCHEMA = [

    # -----------------------------
    # CASE INFO
    # -----------------------------
    "case_number",
    "case_status",
    "received_date",
    "decision_date",
    "original_cert_date",
    "visa_class",

    # -----------------------------
    # JOB INFO
    # -----------------------------
    "job_title",
    "soc_code",
    "soc_title",
    "full_time_position",
    "begin_date",
    "end_date",
    "total_worker_positions",

    # -----------------------------
    # EMPLOYMENT TYPE FLAGS
    # -----------------------------
    "new_employment",
    "continued_employment",
    "change_previous_employment",
    "new_concurrent_employment",
    "change_employer",
    "amended_petition",

    # -----------------------------
    # EMPLOYER
    # -----------------------------
    "employer_name",
    "trade_name_dba",
    "employer_address1",
    "employer_address2",
    "employer_city",
    "employer_state",
    "employer_postal_code",
    "employer_country",
    "employer_province",
    "employer_phone",
    "employer_phone_ext",
    "employer_fein",
    "naics_code",
    "employer_county",

    # -----------------------------
    # EMPLOYER POC
    # -----------------------------
    "employer_poc_last_name",
    "employer_poc_first_name",
    "employer_poc_middle_name",
    "employer_poc_job_title",
    "employer_poc_address1",
    "employer_poc_address2",
    "employer_poc_city",
    "employer_poc_state",
    "employer_poc_postal_code",
    "employer_poc_country",
    "employer_poc_province",
    "employer_poc_phone",
    "employer_poc_phone_ext",
    "employer_poc_email",

    # -----------------------------
    # AGENT / ATTORNEY
    # -----------------------------
    "agent_representing_employer",
    "agent_attorney_last_name",
    "agent_attorney_first_name",
    "agent_attorney_middle_name",
    "agent_attorney_name",
    "agent_attorney_address1",
    "agent_attorney_address2",
    "agent_attorney_city",
    "agent_attorney_state",
    "agent_attorney_postal_code",
    "agent_attorney_country",
    "agent_attorney_province",
    "agent_attorney_phone",
    "agent_attorney_phone_ext",
    "agent_attorney_email_address",
    "lawfirm_name_business_name",
    "lawfirm_business_fein",
    "state_of_highest_court",
    "name_of_highest_state_court",

    # -----------------------------
    # WORKSITE (primary)
    # -----------------------------
    "worksite_workers",
    "secondary_entity",
    "secondary_entity_business_name",
    "worksite_address1",
    "worksite_address2",
    "worksite_city",
    "worksite_county",
    "worksite_state",
    "worksite_postal_code",

    # -----------------------------
    # WAGE / PAY
    # -----------------------------
    "wage_rate_of_pay_from",
    "wage_rate_of_pay_to",
    "wage_unit_of_pay",

    # -----------------------------
    # PREVAILING WAGE
    # -----------------------------
    "prevailing_wage",
    "pw_unit_of_pay",
    "pw_tracking_number",
    "pw_wage_level",
    "pw_wage_source",
    "pw_oes_year",
    "pw_other_source",
    "pw_other_year",
    "pw_survey_publisher",
    "pw_survey_name",

    # -----------------------------
    # COMPLIANCE
    # -----------------------------
    "total_worksite_locations",
    "agree_to_lc_statement",
    "h_1b_dependent",
    "willful_violator",
    "support_h1b",
    "statutory_basis",
    "appendix_a_attached",
    "public_disclosure",

    # -----------------------------
    # MISC (old-form fields)
    # -----------------------------
    "withdrawn",
    "labor_con_agree",
    "masters_exemption",
    "certified_begin_date",
    "certified_end_date",

    # -----------------------------
    # PREPARER
    # -----------------------------
    "preparer_last_name",
    "preparer_first_name",
    "preparer_middle_initial",
    "preparer_business_name",
    "preparer_email",

    # -----------------------------
    # INTERNAL METADATA
    # -----------------------------
    "year",
]


# ---------------------------------------------------------
# 2. ALIAS MAPPING (old names → canonical)
#    Maps every known variant to the unified column name.
# ---------------------------------------------------------
ALIAS_MAP = {
    # ---- CASE INFO ----
    "case_no":                          "case_number",
    "lca_case_number":                  "case_number",
    "approval_status":                  "case_status",
    "status":                           "case_status",
    "submitted_date":                   "received_date",
    "lca_case_submit":                  "received_date",
    "case_submitted":                   "received_date",
    "dol_decision_date":                "decision_date",
    "program":                          "visa_class",
    "program_designation":              "visa_class",

    # ---- JOB INFO ----
    "job_code":                         "soc_code",
    "occupational_code":                "soc_code",
    "lca_case_soc_code":                "soc_code",
    "occupational_title":               "soc_title",
    "soc_name":                         "soc_title",
    "lca_case_soc_name":                "soc_title",
    "lca_case_job_title":               "job_title",
    "full_time_pos":                    "full_time_position",
    "lca_case_employment_start_date":   "begin_date",
    "employment_start_date":            "begin_date",
    "period_of_employment_start_date":  "begin_date",
    "lca_case_employment_end_date":     "end_date",
    "employment_end_date":              "end_date",
    "period_of_employment_end_date":    "end_date",
    "nbr_immigrants":                   "total_worker_positions",
    "total_workers":                    "total_worker_positions",

    # ---- EMPLOYMENT TYPE ----
    "new_concurrent_emp":               "new_concurrent_employment",

    # ---- EMPLOYER ----
    "name":                             "employer_name",
    "lca_case_employer_name":           "employer_name",
    "employer_business_dba":            "trade_name_dba",
    "address1":                         "employer_address1",
    "employer_address":                 "employer_address1",
    "lca_case_employer_address":        "employer_address1",
    "lca_case_employer_address1":       "employer_address1",
    "address2":                         "employer_address2",
    "lca_case_employer_address2":       "employer_address2",
    "city":                             "employer_city",
    "lca_case_employer_city":           "employer_city",
    "state":                            "employer_state",
    "lca_case_employer_state":          "employer_state",
    "postal_code":                      "employer_postal_code",
    "lca_case_employer_postal_code":    "employer_postal_code",
    "naic_code":                        "naics_code",
    "lca_case_naics_code":              "naics_code",

    # ---- EMPLOYER POC (minor variants) ----
    "employer_poc_address_1":           "employer_poc_address1",
    "employer_poc_address_2":           "employer_poc_address2",

    # ---- ATTORNEY (old combined name) ----
    "agent_attorney_law_firm_business_name": "lawfirm_name_business_name",

    # ---- WORKSITE (2008/2009 dual-site → primary) ----
    "city_1":                           "worksite_city",
    "state_1":                          "worksite_state",
    "lca_case_workloc1_city":           "worksite_city",
    "lca_case_workloc1_state":          "worksite_state",
    "work_location_city1":              "worksite_city",
    "work_location_state1":             "worksite_state",

    # ---- WORKSITE (2019 multi-site _1 → primary) ----
    "worksite_workers_1":               "worksite_workers",
    "secondary_entity_1":               "secondary_entity",
    "secondary_entity_business_name_1": "secondary_entity_business_name",
    "worksite_address1_1":              "worksite_address1",
    "worksite_address2_1":              "worksite_address2",
    "worksite_city_1":                  "worksite_city",
    "worksite_county_1":                "worksite_county",
    "worksite_state_1":                 "worksite_state",
    "worksite_postal_code_1":           "worksite_postal_code",

    # ---- WAGE / PAY ----
    "wage_rate_1":                      "wage_rate_of_pay_from",
    "lca_case_wage_rate_from":          "wage_rate_of_pay_from",
    "wage_rate_of_pay":                 "wage_rate_of_pay_from",
    "wage_rate_of_pay_from_1":          "wage_rate_of_pay_from",
    "max_rate_1":                       "wage_rate_of_pay_to",
    "lca_case_wage_rate_to":            "wage_rate_of_pay_to",
    "wage_rate_of_pay_to_1":            "wage_rate_of_pay_to",
    "rate_per_1":                       "wage_unit_of_pay",
    "lca_case_wage_rate_unit":          "wage_unit_of_pay",
    "wage_unit_of_pay_1":               "wage_unit_of_pay",

    # ---- PREVAILING WAGE ----
    "prevailing_wage_1":                "prevailing_wage",
    "pw_1":                             "prevailing_wage",
    "pw_unit_1":                        "pw_unit_of_pay",
    "pw_unit_of_pay_1":                 "pw_unit_of_pay",
    "pw_tracking_number_1":             "pw_tracking_number",
    "pw_wage_level_1":                  "pw_wage_level",
    "wage_source_1":                    "pw_wage_source",
    "pw_source_1":                      "pw_wage_source",
    "pw_source":                        "pw_wage_source",
    "pw_wage_source":                   "pw_wage_source",
    "yr_source_pub_1":                  "pw_oes_year",
    "pw_source_year":                   "pw_oes_year",
    "pw_wage_source_year":              "pw_oes_year",
    "pw_oes_year_1":                    "pw_oes_year",
    "other_wage_source_1":              "pw_other_source",
    "pw_source_other":                  "pw_other_source",
    "pw_wage_source_other":             "pw_other_source",
    "pw_other_source_1":                "pw_other_source",
    "pw_non_oes_year_1":                "pw_other_year",
    "pw_survey_publisher_1":            "pw_survey_publisher",
    "pw_survey_name_1":                 "pw_survey_name",

    # ---- COMPLIANCE ----
    "h1b_dependent":                    "h_1b_dependent",
    "h_1b_dependent":                   "h_1b_dependent",
    "public_disclosure_location":       "public_disclosure",
}


# ---------------------------------------------------------
# 3. Normalize column names
# ---------------------------------------------------------
def normalize_columns(cols):
    cols = (
        cols.str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("-", "_")
    )
    cols = cols.str.replace(r"^unnamed.*", "", regex=True)
    return cols


# ---------------------------------------------------------
# 4. Load parquet file (skip Appendix A / Worksites)
# ---------------------------------------------------------
def load_lca_file(file_path):
    pq = file_path.replace(".xlsx", ".parquet")
    if os.path.exists(pq):
        print("  → Loading parquet:", pq)
        try:
            return pd.read_parquet(pq)
        except Exception as e:
            print(f"  ⚠️  Error reading parquet: {e}")
            print("  → Falling back to xlsx...")
            try:
                return pd.read_excel(file_path, dtype=str)
            except Exception as e2:
                print(f"  ⚠️  Error reading xlsx too: {e2}")
                return None
    # No parquet, try xlsx directly
    if os.path.exists(file_path):
        print("  → Loading xlsx:", file_path)
        try:
            return pd.read_excel(file_path, dtype=str)
        except Exception as e:
            print(f"  ⚠️  Error reading xlsx: {e}")
            return None
    print("  ⚠️  Skipping (file not found):", file_path)
    return None


# ---------------------------------------------------------
# 5. Clean + map alias → canonical
# ---------------------------------------------------------
def clean_and_map(df, year):
    df = df.copy()

    # Remove garbage / duplicate columns
    df = df.loc[:, ~df.columns.duplicated()]
    df = df.dropna(axis=1, how="all")
    df = df[[c for c in df.columns if c]]

    # Drop multi-worksite columns from FY2019 (keep only _1 via alias)
    # e.g. worksite_workers_2 .. worksite_workers_10, etc.
    multi_site_cols = [
        c for c in df.columns
        if any(
            c.startswith(prefix) and c.split("_")[-1].isdigit() and int(c.split("_")[-1]) >= 2
            for prefix in [
                "worksite_workers_", "secondary_entity_", "secondary_entity_business_name_",
                "worksite_address1_", "worksite_address2_",
                "worksite_city_", "worksite_county_", "worksite_state_", "worksite_postal_code_",
                "wage_rate_of_pay_from_", "wage_rate_of_pay_to_", "wage_unit_of_pay_",
                "prevailing_wage_", "pw_unit_of_pay_", "pw_tracking_number_",
                "pw_wage_level_", "pw_oes_year_", "pw_other_source_",
                "pw_non_oes_year_", "pw_survey_publisher_", "pw_survey_name_",
            ]
        )
    ]
    if multi_site_cols:
        df = df.drop(columns=multi_site_cols, errors="ignore")

    # Also drop secondary worksite cols from 2008/2009 (_2 suffix)
    old_site2_cols = [
        c for c in df.columns
        if c.endswith("_2") and any(
            c.startswith(p)
            for p in [
                "wage_rate", "rate_per", "max_rate", "part_time",
                "city", "state", "prevailing_wage", "wage_source",
                "yr_source_pub", "other_wage_source",
                "pw", "pw_unit", "pw_source",
                "lca_case_workloc2", "work_location",
            ]
        )
    ]
    if old_site2_cols:
        df = df.drop(columns=old_site2_cols, errors="ignore")

    # Apply alias mapping
    rename_map = {}
    for old, new in ALIAS_MAP.items():
        if old in df.columns and old != new:
            rename_map[old] = new
    df = df.rename(columns=rename_map)

    # If both old and new ended up as the same canonical, keep first
    df = df.loc[:, ~df.columns.duplicated()]

    # Add year
    df["year"] = year

    return df


# ---------------------------------------------------------
# 6. Reindex to FINAL_SCHEMA
# ---------------------------------------------------------
def enforce_final_schema(df):
    missing = [c for c in FINAL_SCHEMA if c not in df.columns]
    if missing:
        df = pd.concat(
            [df, pd.DataFrame({c: [None] * len(df) for c in missing})],
            axis=1,
        )
    df = df[FINAL_SCHEMA]
    df = df.copy()
    return df


# ---------------------------------------------------------
# 7. Main compiler  (incremental write to stay within RAM)
# ---------------------------------------------------------
def compile_lca():
    outpath = os.path.join(PROJECT_ROOT, "lca_db.csv")
    tmp_path = outpath + ".tmp"
    header_written = False
    total_rows = 0

    for year in sorted(os.listdir(BASE_PATH)):
        year_path = os.path.join(BASE_PATH, year)
        if not os.path.isdir(year_path):
            continue

        for fname in sorted(os.listdir(year_path)):
            if not fname.endswith(".xlsx"):
                continue

            # Skip Appendix A and Worksites companion files
            fl = fname.lower()
            if "appendix" in fl or "worksite" in fl:
                print(f"\n  Skipping companion file: {fname}")
                continue

            full_path = os.path.join(year_path, fname)
            print(f"\nLoading: {full_path}")

            df = load_lca_file(full_path)
            if df is None:
                continue

            df.columns = normalize_columns(df.columns)
            df = clean_and_map(df, year)
            df = enforce_final_schema(df)

            # Normalize case_number for later dedup
            if "case_number" in df.columns:
                df["case_number"] = df["case_number"].astype(str).str.strip().str.upper()

            # Append to temp CSV incrementally
            df.to_csv(tmp_path, index=False,
                       mode="a" if header_written else "w",
                       header=not header_written)
            total_rows += len(df)
            header_written = True
            print(f"  ✓ Wrote {len(df)} rows (running total: {total_rows})")

            del df  # free memory immediately

    if not header_written:
        print("No LCA files found!")
        return

    # Deduplicate by case_number in chunks to stay within RAM
    print("\nDeduplicating by case_number …")
    seen = set()
    dedup_rows = 0
    with open(outpath, "w") as out_f:
        for i, chunk in enumerate(pd.read_csv(tmp_path, dtype=str, chunksize=200_000)):
            before = len(chunk)
            chunk = chunk[~chunk["case_number"].isin(seen)]
            seen.update(chunk["case_number"])
            chunk.to_csv(out_f, index=False, header=(i == 0))
            dedup_rows += len(chunk)
            print(f"  chunk {i}: kept {len(chunk)}/{before}")

    os.remove(tmp_path)

    print("\n--------------------------------------------------")
    print("Saved unified LCA dataset to:", outpath)
    print("Rows:", dedup_rows)
    print("Columns:", len(FINAL_SCHEMA))
    print("--------------------------------------------------")


if __name__ == "__main__":
    compile_lca()
