"""
Merge & Export — Step 2.4
==========================
Merges ER visit data with CMS hospital reference data and timely care
measures into a single analysis-ready dataset.

Also generates a markdown cleaning documentation file.

Input:
  - data/processed/er_visits_validated.csv
  - data/raw/cms_hospital_general_info.csv
  - data/raw/cms_timely_effective_care.csv

Output:
  - data/processed/merged_er_data.csv
  - docs/cleaning_documentation.md

Usage:
    python scripts/merge_export.py
"""

import os
import pandas as pd

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
PROCESSED_DIR = os.path.join(BASE_DIR, "data", "processed")
DOCS_DIR = os.path.join(BASE_DIR, "docs")

VISITS_FILE = os.path.join(PROCESSED_DIR, "er_visits_validated.csv")
HOSPITAL_FILE = os.path.join(RAW_DIR, "cms_hospital_general_info.csv")
TIMELY_FILE = os.path.join(RAW_DIR, "cms_timely_effective_care.csv")

OUTPUT_FILE = os.path.join(PROCESSED_DIR, "merged_er_data.csv")
DOCS_FILE = os.path.join(DOCS_DIR, "cleaning_documentation.md")


def merge_datasets():
    """Merge ER visits with CMS hospital and timely care data."""
    print("=" * 65)
    print("  Merge & Export — Final Analysis-Ready Dataset")
    print("=" * 65)

    # Load validated visits
    visits = pd.read_csv(VISITS_FILE)
    print(f"\n  Visits:   {visits.shape[0]:,} rows × {visits.shape[1]} cols")

    # Load CMS hospital info
    hospitals = pd.read_csv(HOSPITAL_FILE)
    print(f"  Hospitals: {hospitals.shape[0]:,} rows × {hospitals.shape[1]} cols")

    # Select useful hospital columns to merge
    hospital_cols = [
        "facility_id", "hospital_type", "hospital_ownership",
        "emergency_services", "hospital_overall_rating"
    ]
    hospitals_slim = hospitals[[c for c in hospital_cols if c in hospitals.columns]]
    hospitals_slim = hospitals_slim.drop_duplicates(subset=["facility_id"])

    # Ensure facility_id types match
    visits["facility_id"] = visits["facility_id"].astype(str).str.strip()
    hospitals_slim["facility_id"] = hospitals_slim["facility_id"].astype(str).str.strip()

    # Merge visits ← hospital info
    merged = visits.merge(hospitals_slim, on="facility_id", how="left", suffixes=("", "_hospital"))
    matched = merged["hospital_type"].notna().sum()
    print(f"\n  Merged with hospital info: {matched:,}/{len(merged):,} matched "
          f"({matched/len(merged)*100:.1f}%)")

    # Load and process timely care — extract ER-specific measures
    timely = pd.read_csv(TIMELY_FILE)
    print(f"  Timely care: {timely.shape[0]:,} rows")

    # Look for ER-specific measures (ED-related measure IDs)
    er_measures = timely[timely["measure_id"].str.contains("ED|OP_18|OP_20|OP_22", na=False)]
    if len(er_measures) > 0:
        # Pivot to get one row per facility with measure scores
        er_pivot = er_measures.pivot_table(
            index="facility_id", columns="measure_id",
            values="score", aggfunc="first"
        ).reset_index()
        er_pivot.columns = [f"cms_{c}" if c != "facility_id" else c for c in er_pivot.columns]
        er_pivot["facility_id"] = er_pivot["facility_id"].astype(str).str.strip()

        merged = merged.merge(er_pivot, on="facility_id", how="left")
        print(f"  Merged ER-specific CMS measures: {len(er_pivot)} facilities, "
              f"{len(er_pivot.columns) - 1} measure columns")
    else:
        # Even without ER-specific measures, add the EDV (volume) data
        edv = timely[timely["measure_id"] == "EDV"][["facility_id", "score"]].copy()
        if len(edv) > 0:
            edv.columns = ["facility_id", "cms_ed_volume"]
            edv["facility_id"] = edv["facility_id"].astype(str).str.strip()
            merged = merged.merge(edv, on="facility_id", how="left")
            print(f"  Merged ED volume indicator: {edv['cms_ed_volume'].notna().sum()} facilities")

    # Save merged dataset
    merged.to_csv(OUTPUT_FILE, index=False)
    print(f"\n  ✅ Final dataset saved → {OUTPUT_FILE}")
    print(f"     {merged.shape[0]:,} rows × {merged.shape[1]} columns")

    return merged


def generate_cleaning_documentation():
    """Generate a comprehensive markdown cleaning doc from existing logs."""

    # Read existing logs
    cleaning_log_path = os.path.join(DOCS_DIR, "cleaning_log.txt")
    validation_path = os.path.join(DOCS_DIR, "validation_report.txt")

    cleaning_log = ""
    if os.path.exists(cleaning_log_path):
        with open(cleaning_log_path, "r", encoding="utf-8") as f:
            cleaning_log = f.read()

    validation_log = ""
    if os.path.exists(validation_path):
        with open(validation_path, "r", encoding="utf-8") as f:
            validation_log = f.read()

    md = f"""# Data Cleaning & Preparation Documentation

## Overview

This document summarizes all data cleaning, imputation, and validation decisions
made during the preparation of the ER patient visits dataset for analysis.

**Pipeline:** Raw Data → Missing Value Injection → Cleaning → Feature Engineering → Validation → Merge

| Stage | Script | Output |
|-------|--------|--------|
| Raw (clean) | `generate_synthetic_er_data.py` | `er_synthetic_patient_visits.csv` |
| Raw (messy) | `inject_missing_values.py` | `er_patient_visits_raw_messy.csv` |
| Cleaned | `clean_data.py` | `er_visits_cleaned.csv` |
| Featured | `feature_engineering.py` | `er_visits_featured.csv` |
| Validated | `validate_data.py` | `er_visits_validated.csv` |
| Final | `merge_export.py` | `merged_er_data.csv` |

---

## Missing Value Handling Decisions

| Column | Missing % | Decision | Rationale |
|--------|-----------|----------|-----------|
| `esi_level` | ~4% | **FLAG** (not fill) | Missing triage is clinically informative (LWBS) |
| `heart_rate`, `systolic_bp`, `diastolic_bp`, `respiratory_rate`, `spo2`, `temperature_f` | ~8-9% each | **Median within ESI level** | Acuity correlates with vital sign ranges |
| `pain_scale` | ~15% | **Flag + ESI-stratified fill** | Pediatric/AMS patients cannot self-report |
| `seen_by_provider_datetime` | ~7.5% | **Flag, keep NaN** | LWBS patients never seen by provider |
| `departure_datetime` | ~1.4% | **Flag, keep NaN** | Excluded from LOS analysis |
| `triage_datetime` | ~2.4% | **Flag, keep NaN** | Correlated with missing ESI |
| `race_ethnicity` | ~4% | **Fill "Unknown/Declined"** | Cannot assume race |
| `insurance_type` | ~2% | **Fill "Unknown"** | Cannot infer payer |
| `age` | ~1% | **Flag + median fill** | Unidentified patients |
| `chief_complaint` | ~1.8% | **Fill "Unknown/Not Documented"** | Preserves record |

### Key Principles
1. **Missing ESI is never imputed** — it indicates the patient wasn't formally triaged
2. **Vitals are imputed within acuity level** — a global median would be clinically inappropriate
3. **Timestamps are flagged, not filled** — `eligible_for_time_analysis` column tracks which records can be used for timing analysis

---

## Feature Engineering

| Feature | Description | Method |
|---------|-------------|--------|
| `hour_of_arrival` | Hour (0–23) | Extracted from `arrival_datetime` |
| `day_of_week` | Day name | Extracted from `arrival_datetime` |
| `month` | Month (1–12) | Extracted from `arrival_datetime` |
| `is_weekend` | Weekend flag | Saturday/Sunday = 1 |
| `shift` | Work shift | Morning (7–15), Evening (15–23), Night (23–7) |
| `acuity_label` | ESI description | 1=Resuscitation → 5=Non-Urgent |
| `time_to_disposition` | Minutes provider→departure | Calculated from timestamps |
| `age_group` | Age bins | 7 groups from 0-5 to 80+ |
| `wait_time_category` | Wait buckets | Short/Medium/Long/Very Long |

---

## Validation Checks Performed

1. **Negative wait times** — checked and corrected
2. **Extreme wait times (>24h)** — flagged as data errors
3. **Acuity sample sizes** — all ESI levels have adequate representation
4. **Date distribution** — verified no gaps in coverage
5. **Acuity vs wait time** — confirmed higher acuity → shorter wait
6. **Disposition logic** — LWBS/deceased/admitted patterns verified
7. **Vital sign plausibility** — all values within clinical ranges

---

## Detailed Logs

### Cleaning Log
```
{cleaning_log}
```

### Validation Report
```
{validation_log}
```
"""

    with open(DOCS_FILE, "w", encoding="utf-8") as f:
        f.write(md)

    print(f"\n  ✅ Cleaning documentation saved → {DOCS_FILE}")


def main():
    merge_datasets()
    generate_cleaning_documentation()

    print("\n" + "=" * 65)
    print("  Phase 2 complete! Dataset is analysis-ready.")
    print("=" * 65)


if __name__ == "__main__":
    main()
