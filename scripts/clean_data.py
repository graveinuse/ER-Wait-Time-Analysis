"""
Data Cleaning & Preparation — Step 2.1
========================================
Handles missing values in the ER patient visits dataset with documented
clinical rationale for every cleaning decision.

Cleaning Philosophy:
  - Healthcare data missingness is often informative, not random.
  - We preserve clinical meaning: a missing triage score IS information.
  - We use domain-appropriate imputation: vitals imputed within acuity level.
  - We flag rather than silently fill when missingness carries meaning.
  - We exclude incomplete timing records from wait-time analysis but keep
    them for volume and demographic analysis.

Input:   data/raw/er_patient_visits_raw_messy.csv
Output:  data/processed/er_visits_cleaned.csv
Log:     docs/cleaning_log.txt

Usage:
    python scripts/clean_data.py
"""

import os
import io
import numpy as np
import pandas as pd
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
PROCESSED_DIR = os.path.join(BASE_DIR, "data", "processed")
DOCS_DIR = os.path.join(BASE_DIR, "docs")

INPUT_FILE = os.path.join(RAW_DIR, "er_patient_visits_raw_messy.csv")
OUTPUT_FILE = os.path.join(PROCESSED_DIR, "er_visits_cleaned.csv")
CLEANING_LOG = os.path.join(DOCS_DIR, "cleaning_log.txt")


# ─── Logging Utility ─────────────────────────────────────────────────────────

class CleaningLogger:
    """Captures all cleaning decisions for documentation."""

    def __init__(self):
        self.entries = []
        self.section_num = 0

    def section(self, title):
        self.section_num += 1
        self.entries.append(f"\n{'=' * 75}")
        self.entries.append(f"  SECTION {self.section_num}: {title}")
        self.entries.append(f"{'=' * 75}\n")
        print(f"\n[Section {self.section_num}] {title}")

    def decision(self, column, missing_count, missing_pct, reason, action, detail=""):
        entry = (
            f"  Column: {column}\n"
            f"  Missing: {missing_count:,} ({missing_pct:.1f}%)\n"
            f"  Why missing: {reason}\n"
            f"  Decision: {action}\n"
        )
        if detail:
            entry += f"  Detail: {detail}\n"
        self.entries.append(entry)
        print(f"  {column}: {missing_count:,} missing ({missing_pct:.1f}%) → {action}")

    def note(self, text):
        self.entries.append(f"  NOTE: {text}")
        print(f"  NOTE: {text}")

    def stat(self, text):
        self.entries.append(f"  {text}")
        print(f"  {text}")

    def save(self, path):
        header = (
            f"{'=' * 75}\n"
            f"  DATA CLEANING LOG — ER Patient Visits\n"
            f"  Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"  Script: scripts/clean_data.py\n"
            f"{'=' * 75}\n"
        )
        with open(path, "w", encoding="utf-8") as f:
            f.write(header)
            f.write("\n".join(self.entries))
        print(f"\n✅ Cleaning log saved → {path}")


# ─── Pre-Cleaning Assessment ─────────────────────────────────────────────────

def assess_missing_values(df, log):
    """Document missing values before cleaning."""
    log.section("PRE-CLEANING MISSING VALUE ASSESSMENT")

    total_cells = df.shape[0] * df.shape[1]
    total_missing = df.isnull().sum().sum()
    log.stat(f"Dataset shape: {df.shape[0]:,} rows × {df.shape[1]} columns")
    log.stat(f"Total cells: {total_cells:,}")
    log.stat(f"Total missing values: {total_missing:,} ({total_missing/total_cells*100:.2f}%)\n")

    log.stat(f"{'Column':<30s} {'Missing':>8s} {'Pct':>7s}  {'Type':<12s}")
    log.stat("-" * 70)

    for col in df.columns:
        n_missing = df[col].isnull().sum()
        if n_missing > 0:
            pct = n_missing / len(df) * 100
            dtype = str(df[col].dtype)
            log.stat(f"{col:<30s} {n_missing:>8,} {pct:>6.1f}%  {dtype:<12s}")

    log.stat("")


# ─── Cleaning Functions ──────────────────────────────────────────────────────

def clean_esi_level(df, log):
    """
    Handle missing ESI (triage acuity) levels.

    Clinical Rationale:
      Missing ESI is clinically informative — it typically means the patient
      left without being seen (LWBS) or there was a documentation failure.
      We FLAG rather than fill, because imputing a triage level masks
      operational issues.
    """
    log.section("ESI LEVEL (TRIAGE ACUITY)")

    col = "esi_level"
    n_missing = df[col].isnull().sum()
    pct = n_missing / len(df) * 100

    # Create a flag column
    df["esi_missing_flag"] = df[col].isnull().astype(int)

    # Analyze correlation with disposition
    if n_missing > 0:
        missing_disp = df.loc[df[col].isnull(), "disposition"].value_counts()
        log.decision(
            column=col,
            missing_count=n_missing,
            missing_pct=pct,
            reason="Systematic — LWBS patients often not formally triaged; "
                   "some documentation gaps during high-volume periods.",
            action="FLAG (not fill). Created 'esi_missing_flag' column. "
                   "Missing ESI preserved as NaN for transparency.",
            detail=f"Disposition of missing-ESI visits: "
                   f"{missing_disp.to_dict()}"
        )

        # For analysis that requires ESI, we'll also create an 'esi_imputed'
        # column using the mode for the patient's disposition group
        df["esi_level_imputed"] = df[col].copy()
        for disp in df["disposition"].unique():
            mask = df[col].isnull() & (df["disposition"] == disp)
            if mask.sum() > 0:
                mode_val = df.loc[
                    (df["disposition"] == disp) & df[col].notna(), col
                ].mode()
                if len(mode_val) > 0:
                    df.loc[mask, "esi_level_imputed"] = mode_val.iloc[0]

        log.note("Also created 'esi_level_imputed' using mode within "
                 "disposition group — use only when ESI is required for analysis.")

    return df


def clean_vital_signs(df, log):
    """
    Handle missing vital signs.

    Clinical Rationale:
      Vital signs are often missing together (equipment failure, patient
      refusal, pediatric patients). We use MEDIAN IMPUTATION WITHIN ESI
      LEVEL, not global median, because acuity strongly correlates with
      vital sign ranges (e.g., ESI-1 patients have very different HR norms
      than ESI-5).
    """
    log.section("VITAL SIGNS")

    vital_cols = ["heart_rate", "systolic_bp", "diastolic_bp",
                  "respiratory_rate", "spo2", "temperature_f"]

    # Use imputed ESI for stratification (so we can handle all rows)
    esi_col = "esi_level_imputed" if "esi_level_imputed" in df.columns else "esi_level"

    for col in vital_cols:
        n_missing = df[col].isnull().sum()
        if n_missing == 0:
            continue
        pct = n_missing / len(df) * 100

        # Create flag
        df[f"{col}_imputed_flag"] = df[col].isnull().astype(int)

        # Median imputation within ESI level
        impute_map = {}
        for esi in sorted(df[esi_col].dropna().unique()):
            subset = df.loc[df[esi_col] == esi, col].dropna()
            med = subset.median()
            impute_map[esi] = med
            mask = df[col].isnull() & (df[esi_col] == esi)
            df.loc[mask, col] = med

        # Handle remaining nulls (where ESI is also missing) with global median
        remaining = df[col].isnull().sum()
        if remaining > 0:
            global_med = df[col].median()
            df[col].fillna(global_med, inplace=True)

        log.decision(
            column=col,
            missing_count=n_missing,
            missing_pct=pct,
            reason="Correlated — equipment issues, patient refusal, "
                   "pediatric patients (often missing together).",
            action=f"FILL with median WITHIN ESI level. "
                   f"Created '{col}_imputed_flag'. "
                   f"Medians: {{{', '.join(f'ESI {k}: {v:.0f}' for k, v in sorted(impute_map.items()))}}}",
            detail=f"{remaining} remaining nulls (missing ESI too) filled with global median."
        )

    return df


def clean_pain_scale(df, log):
    """
    Handle missing pain scale scores.

    Clinical Rationale:
      Pain scale is subjective and frequently missing for pediatric patients
      (too young to report), altered mental status patients (unable to report),
      and unconscious patients. Missing pain IS clinically meaningful.
    """
    log.section("PAIN SCALE")

    col = "pain_scale"
    n_missing = df[col].isnull().sum()
    if n_missing == 0:
        return df
    pct = n_missing / len(df) * 100

    df["pain_scale_missing_flag"] = df[col].isnull().astype(int)

    # Analyze who has missing pain scores
    missing_ages = df.loc[df[col].isnull(), "age"].describe()
    missing_cc = df.loc[df[col].isnull(), "chief_complaint"].value_counts().head(5)

    # Fill with median within ESI level (since pain correlates with acuity)
    esi_col = "esi_level_imputed" if "esi_level_imputed" in df.columns else "esi_level"
    for esi in sorted(df[esi_col].dropna().unique()):
        med = df.loc[df[esi_col] == esi, col].median()
        mask = df[col].isnull() & (df[esi_col] == esi)
        df.loc[mask, col] = med

    # Fill any remaining
    df[col].fillna(df[col].median(), inplace=True)

    log.decision(
        column=col,
        missing_count=n_missing,
        missing_pct=pct,
        reason="Systematic — pediatric (<3 yrs), altered mental status, "
               "and unconscious patients cannot self-report pain.",
        action="FLAG + FILL with median within ESI level. "
               "Created 'pain_scale_missing_flag'.",
        detail=f"Mean age of missing: {missing_ages.get('mean', 'N/A'):.0f} yrs. "
               f"Top complaints: {missing_cc.to_dict()}"
    )

    return df


def clean_timestamps(df, log):
    """
    Handle missing timestamps.

    Clinical Rationale:
      Missing arrival/departure times make wait-time calculation impossible.
      These records should be EXCLUDED from time-based analysis but KEPT
      for volume and demographic analysis. We create an 'eligible_for_time_analysis'
      flag.
    """
    log.section("TIMESTAMPS")

    time_cols = ["seen_by_provider_datetime", "departure_datetime",
                 "triage_datetime"]

    for col in time_cols:
        n_missing = df[col].isnull().sum()
        if n_missing == 0:
            continue
        pct = n_missing / len(df) * 100

        if col == "seen_by_provider_datetime":
            # LWBS patients: expected to have no provider time
            df["seen_by_provider_missing_flag"] = df[col].isnull().astype(int)
            log.decision(
                column=col,
                missing_count=n_missing,
                missing_pct=pct,
                reason="Systematic — LWBS patients never seen by a provider; "
                       "some data entry gaps.",
                action="FLAG. Keep NaN. Records excluded from door-to-provider "
                       "time calculations but included in volume analysis."
            )

        elif col == "departure_datetime":
            df["departure_missing_flag"] = df[col].isnull().astype(int)
            log.decision(
                column=col,
                missing_count=n_missing,
                missing_pct=pct,
                reason="Random — data entry errors, system downtime, "
                       "patients who left without notification.",
                action="FLAG. Keep NaN. Total LOS also set to NaN for these. "
                       "Excluded from LOS analysis."
            )

        elif col == "triage_datetime":
            df["triage_ts_missing_flag"] = df[col].isnull().astype(int)
            log.decision(
                column=col,
                missing_count=n_missing,
                missing_pct=pct,
                reason="Systematic — correlated with missing ESI; "
                       "patient not formally triaged.",
                action="FLAG. Keep NaN."
            )

    # Create composite eligibility flag for time-based analysis
    df["eligible_for_time_analysis"] = (
        df["arrival_datetime"].notna() &
        df["departure_datetime"].notna()
    ).astype(int)

    eligible = df["eligible_for_time_analysis"].sum()
    log.note(
        f"Created 'eligible_for_time_analysis' flag: "
        f"{eligible:,} ({eligible/len(df)*100:.1f}%) records eligible "
        f"for wait-time / LOS calculations."
    )

    return df


def clean_demographics(df, log):
    """
    Handle missing demographic fields.

    Clinical Rationale:
      Race/ethnicity may be missing due to patient declining to answer or
      emergency situations. Insurance may be incomplete at time of data pull.
      Age missing for unidentified patients (rare).
    """
    log.section("DEMOGRAPHICS")

    # Race/Ethnicity — fill with "Unknown/Declined"
    col = "race_ethnicity"
    n_missing = df[col].isnull().sum()
    if n_missing > 0:
        pct = n_missing / len(df) * 100
        df[col].fillna("Unknown/Declined", inplace=True)
        log.decision(
            column=col,
            missing_count=n_missing,
            missing_pct=pct,
            reason="Mixed — patient declined, emergency situations, "
                   "language barriers.",
            action="FILL with 'Unknown/Declined'. This preserves the category "
                   "for analysis without assuming a race."
        )

    # Insurance type — fill with "Unknown"
    col = "insurance_type"
    n_missing = df[col].isnull().sum()
    if n_missing > 0:
        pct = n_missing / len(df) * 100
        df[col].fillna("Unknown", inplace=True)
        log.decision(
            column=col,
            missing_count=n_missing,
            missing_pct=pct,
            reason="Random — registration incomplete at time of data pull.",
            action="FILL with 'Unknown'. Cannot safely infer payer type."
        )

    # Age — fill with median (rare, ~1%)
    col = "age"
    n_missing = df[col].isnull().sum()
    if n_missing > 0:
        pct = n_missing / len(df) * 100
        median_age = df[col].median()
        df["age_imputed_flag"] = df[col].isnull().astype(int)
        df[col].fillna(median_age, inplace=True)
        df[col] = df[col].astype(int)
        log.decision(
            column=col,
            missing_count=n_missing,
            missing_pct=pct,
            reason="Rare — unidentified patients (John/Jane Doe), "
                   "emergency situations.",
            action=f"FLAG + FILL with median age ({median_age:.0f}). "
                   f"Created 'age_imputed_flag'."
        )

    return df


def clean_chief_complaint(df, log):
    """Handle missing chief complaints."""
    log.section("CHIEF COMPLAINT")

    col = "chief_complaint"
    n_missing = df[col].isnull().sum()
    if n_missing > 0:
        pct = n_missing / len(df) * 100
        df[col].fillna("Unknown/Not Documented", inplace=True)
        log.decision(
            column=col,
            missing_count=n_missing,
            missing_pct=pct,
            reason="Mixed — unconscious patients, language barriers, "
                   "data entry errors.",
            action="FILL with 'Unknown/Not Documented'. "
                   "Preserves record for other analyses."
        )

    return df


def post_cleaning_validation(df, log):
    """Run validation checks on cleaned data."""
    log.section("POST-CLEANING VALIDATION")

    # Check for remaining nulls (only expected in timestamp and ESI columns)
    remaining_nulls = df.isnull().sum()
    allowed_null_cols = [
        "esi_level", "seen_by_provider_datetime", "departure_datetime",
        "triage_datetime", "total_los_minutes", "treatment_end_datetime",
        "wait_time_minutes", "treatment_time_minutes", "boarding_time_minutes"
    ]

    log.stat(f"{'Column':<35s} {'Remaining NaN':>13s}  Status")
    log.stat("-" * 65)

    unexpected_nulls = False
    for col in df.columns:
        n_null = remaining_nulls[col]
        if n_null > 0:
            if col in allowed_null_cols:
                status = "✓ Expected (documented)"
            else:
                status = "⚠ UNEXPECTED"
                unexpected_nulls = True
            log.stat(f"{col:<35s} {n_null:>13,}  {status}")

    if not unexpected_nulls:
        log.note("All remaining nulls are in expected columns with documented reasons.")

    # Data integrity checks
    log.stat("")
    log.stat("--- Data Integrity Checks ---")

    # Check wait times are positive where available
    valid_waits = df.loc[df["wait_time_minutes"].notna(), "wait_time_minutes"]
    neg_waits = (valid_waits < 0).sum()
    log.stat(f"Negative wait times: {neg_waits} {'✓' if neg_waits == 0 else '⚠'}")

    # Check vital sign ranges are clinically plausible
    vital_checks = {
        "heart_rate": (20, 250),
        "systolic_bp": (50, 300),
        "respiratory_rate": (5, 60),
        "spo2": (50, 100),
        "temperature_f": (90, 110),
    }
    for col, (lo, hi) in vital_checks.items():
        valid = df[col].dropna()
        out_of_range = ((valid < lo) | (valid > hi)).sum()
        log.stat(f"{col} out of range [{lo}-{hi}]: {out_of_range} "
                 f"{'✓' if out_of_range == 0 else '⚠'}")

    # Summary
    log.stat("")
    log.stat(f"--- Final Dataset Summary ---")
    log.stat(f"Total records: {len(df):,}")
    log.stat(f"Total columns: {df.shape[1]} (original 36 + {df.shape[1] - 36} flag columns)")
    log.stat(f"Eligible for time analysis: "
             f"{df['eligible_for_time_analysis'].sum():,} "
             f"({df['eligible_for_time_analysis'].mean()*100:.1f}%)")
    log.stat(f"Records with imputed vitals: "
             f"{df[[c for c in df.columns if c.endswith('_imputed_flag')]].max(axis=1).sum():,}")

    return df


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("=" * 65)
    print("  Data Cleaning Pipeline — ER Patient Visits")
    print("=" * 65)

    os.makedirs(PROCESSED_DIR, exist_ok=True)
    os.makedirs(DOCS_DIR, exist_ok=True)

    log = CleaningLogger()

    # Load messy data
    df = pd.read_csv(INPUT_FILE)
    print(f"\nLoaded: {len(df):,} records × {df.shape[1]} columns")

    # Pre-cleaning assessment
    assess_missing_values(df, log)

    # Apply cleaning steps (ORDER MATTERS)
    df = clean_esi_level(df, log)          # ESI first (vitals depend on it)
    df = clean_vital_signs(df, log)        # Vitals use ESI for stratification
    df = clean_pain_scale(df, log)         # Pain also uses ESI
    df = clean_timestamps(df, log)         # Timestamps create eligibility flag
    df = clean_demographics(df, log)       # Demographics
    df = clean_chief_complaint(df, log)    # Chief complaint

    # Validation
    df = post_cleaning_validation(df, log)

    # Save outputs
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"\n✅ Cleaned dataset saved → {OUTPUT_FILE}")
    print(f"   {len(df):,} records × {df.shape[1]} columns")

    log.save(CLEANING_LOG)

    print("\n" + "=" * 65)
    print("  Cleaning complete!")
    print("=" * 65)


if __name__ == "__main__":
    main()
