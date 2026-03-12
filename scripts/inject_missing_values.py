"""
Inject Realistic Missing Values
=================================
Simulates the kind of data quality issues found in real healthcare data:
  - Missing triage acuity (patient not formally triaged, e.g. LWBS)
  - Missing vital signs (equipment issues, patient refusal, pediatric)
  - Missing timestamps (data entry errors, system downtime)
  - Missing demographics (patient unable to provide, emergency situations)
  - Missing chief complaint (arrived unconscious, language barrier)

Run this ONCE to create the "messy" raw data that the cleaning script will fix.

Usage:
    python scripts/inject_missing_values.py
"""

import os
import numpy as np
import pandas as pd

SEED = 99
np.random.seed(SEED)

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
RAW_DIR = os.path.join(BASE_DIR, "data", "raw")

INPUT_FILE = os.path.join(RAW_DIR, "er_synthetic_patient_visits.csv")
OUTPUT_FILE = os.path.join(RAW_DIR, "er_patient_visits_raw_messy.csv")


def inject_missingness(df):
    """Inject realistic missing value patterns into the dataset."""
    n = len(df)
    report = []

    # ── 1. ESI Level (Triage Acuity) ─────────────────────────────────────
    # ~3% missing: LWBS patients often aren't triaged, plus some data entry gaps
    mask_esi = np.random.random(n) < 0.03
    # Make LWBS patients 5x more likely to have missing ESI
    lwbs_mask = df["disposition"] == "LWBS"
    mask_esi = mask_esi | (lwbs_mask & (np.random.random(n) < 0.15))
    df.loc[mask_esi, "esi_level"] = np.nan
    report.append(("esi_level", mask_esi.sum(), mask_esi.sum() / n * 100,
                    "Systematic — LWBS patients often not formally triaged"))

    # ── 2. Vital Signs ───────────────────────────────────────────────────
    # ~5-8% missing across vitals; correlated (if one is missing, others likely are too)
    # Pediatric patients (<5) more likely to have missing vitals (uncooperative)
    base_vitals_mask = np.random.random(n) < 0.05
    pediatric_mask = (df["age"] < 5) & (np.random.random(n) < 0.20)
    vitals_missing = base_vitals_mask | pediatric_mask

    # When vitals are missing, usually multiple are missing together
    vital_cols = ["heart_rate", "systolic_bp", "diastolic_bp",
                  "respiratory_rate", "spo2", "temperature_f"]
    for col in vital_cols:
        col_mask = vitals_missing.copy()
        # Each vital has a small additional independent chance of being missing
        col_mask = col_mask | (np.random.random(n) < 0.02)
        df.loc[col_mask, col] = np.nan
        report.append((col, col_mask.sum(), col_mask.sum() / n * 100,
                        "Correlated — equipment issues, patient refusal, pediatric"))

    # Pain scale: higher missingness (~10%) — subjective, pediatric, AMS patients
    pain_mask = np.random.random(n) < 0.08
    ams_mask = df["chief_complaint"] == "Altered Mental Status"
    pain_mask = pain_mask | (ams_mask & (np.random.random(n) < 0.50))
    pain_mask = pain_mask | ((df["age"] < 3) & (np.random.random(n) < 0.80))
    df.loc[pain_mask, "pain_scale"] = np.nan
    report.append(("pain_scale", pain_mask.sum(), pain_mask.sum() / n * 100,
                    "Systematic — pediatric, AMS patients cannot report"))

    # ── 3. Timestamps ────────────────────────────────────────────────────
    # ~2% missing seen_by_provider (LWBS patients never seen)
    seen_mask = lwbs_mask.copy()  # All LWBS have no provider time
    seen_mask = seen_mask | (np.random.random(n) < 0.01)
    df.loc[seen_mask, "seen_by_provider_datetime"] = np.nan
    report.append(("seen_by_provider_datetime", seen_mask.sum(),
                    seen_mask.sum() / n * 100,
                    "Systematic — LWBS patients never seen by provider"))

    # ~1.5% missing departure time (system glitches, walk-outs)
    depart_mask = np.random.random(n) < 0.015
    df.loc[depart_mask, "departure_datetime"] = np.nan
    df.loc[depart_mask, "total_los_minutes"] = np.nan
    report.append(("departure_datetime", depart_mask.sum(),
                    depart_mask.sum() / n * 100,
                    "Random — data entry errors, system downtime"))

    # ~1% missing triage timestamp
    triage_ts_mask = mask_esi.copy() & (np.random.random(n) < 0.60)
    df.loc[triage_ts_mask, "triage_datetime"] = np.nan
    report.append(("triage_datetime", triage_ts_mask.sum(),
                    triage_ts_mask.sum() / n * 100,
                    "Systematic — correlated with missing ESI"))

    # ── 4. Demographics ──────────────────────────────────────────────────
    # ~4% missing race/ethnicity (patient declined, emergency situations)
    race_mask = np.random.random(n) < 0.04
    df.loc[race_mask, "race_ethnicity"] = np.nan
    report.append(("race_ethnicity", race_mask.sum(), race_mask.sum() / n * 100,
                    "Random/Systematic — patient declined or unable to provide"))

    # ~2% missing insurance type
    ins_mask = np.random.random(n) < 0.02
    df.loc[ins_mask, "insurance_type"] = np.nan
    report.append(("insurance_type", ins_mask.sum(), ins_mask.sum() / n * 100,
                    "Random — registration incomplete at time of data pull"))

    # ~1% missing age (unidentified patients, emergency situations)
    age_mask = np.random.random(n) < 0.01
    df.loc[age_mask, "age"] = np.nan
    report.append(("age", age_mask.sum(), age_mask.sum() / n * 100,
                    "Rare — unidentified patients, John/Jane Doe cases"))

    # ── 5. Chief Complaint ───────────────────────────────────────────────
    # ~1.5% missing (arrived unconscious, language barrier, system error)
    cc_mask = np.random.random(n) < 0.015
    cc_mask = cc_mask | (ams_mask & (np.random.random(n) < 0.10))
    df.loc[cc_mask, "chief_complaint"] = np.nan
    report.append(("chief_complaint", cc_mask.sum(), cc_mask.sum() / n * 100,
                    "Mixed — language barrier, unconscious, data entry error"))

    return df, report


def main():
    print("=" * 65)
    print("  Injecting Realistic Missing Values")
    print("=" * 65)

    df = pd.read_csv(INPUT_FILE)
    print(f"\nLoaded: {len(df):,} records, {df.shape[1]} columns")
    print(f"Original nulls: {df.isnull().sum().sum()}")

    df, report = inject_missingness(df)

    print(f"\nAfter injection: {df.isnull().sum().sum():,} total null values\n")
    print(f"{'Column':<30s} {'Missing':>8s} {'Pct':>7s}  Reason")
    print("-" * 90)
    for col, count, pct, reason in report:
        print(f"{col:<30s} {count:>8,} {pct:>6.1f}%  {reason}")

    df.to_csv(OUTPUT_FILE, index=False)
    print(f"\n✅ Saved messy dataset → {OUTPUT_FILE}")
    print("=" * 65)


if __name__ == "__main__":
    main()
