"""
Feature Engineering — Step 2.2
================================
Creates calculated columns from the cleaned ER visit data for analysis
and dashboard use.

New columns added:
  - is_weekend           : Boolean — Saturday/Sunday arrivals
  - shift                : Morning (7–15), Evening (15–23), Night (23–7)
  - acuity_label         : ESI numeric → descriptive label
  - time_to_disposition  : Minutes from seen-by-provider to departure
  - hour_of_arrival      : Alias of arrival_hour for clarity
  - age_group            : Binned age ranges for demographic analysis
  - wait_time_category   : Short/Medium/Long/Very Long wait buckets

Input:   data/processed/er_visits_cleaned.csv
Output:  data/processed/er_visits_featured.csv

Usage:
    python scripts/feature_engineering.py
"""

import os
import numpy as np
import pandas as pd

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
PROCESSED_DIR = os.path.join(BASE_DIR, "data", "processed")

INPUT_FILE = os.path.join(PROCESSED_DIR, "er_visits_cleaned.csv")
OUTPUT_FILE = os.path.join(PROCESSED_DIR, "er_visits_featured.csv")


# ─── Acuity Labels ───────────────────────────────────────────────────────────
ESI_LABELS = {
    1: "Resuscitation",
    2: "Emergent",
    3: "Urgent",
    4: "Less Urgent",
    5: "Non-Urgent",
}

# ─── Shift Definitions ───────────────────────────────────────────────────────
# Morning: 7:00 AM – 2:59 PM  (hours 7–14)
# Evening: 3:00 PM – 10:59 PM (hours 15–22)
# Night:   11:00 PM – 6:59 AM (hours 23, 0–6)

def assign_shift(hour):
    """Map hour (0–23) to shift name."""
    if 7 <= hour <= 14:
        return "Morning"
    elif 15 <= hour <= 22:
        return "Evening"
    else:
        return "Night"


def main():
    print("=" * 65)
    print("  Feature Engineering — ER Patient Visits")
    print("=" * 65)

    df = pd.read_csv(INPUT_FILE)
    original_cols = df.shape[1]
    print(f"\nLoaded: {len(df):,} rows × {original_cols} columns")

    # ── Parse datetime columns ────────────────────────────────────────────
    datetime_cols = [
        "arrival_datetime", "triage_datetime",
        "seen_by_provider_datetime", "treatment_end_datetime",
        "departure_datetime",
    ]
    for col in datetime_cols:
        df[col] = pd.to_datetime(df[col], errors="coerce")

    # ── 1. Verify/create time extraction columns ─────────────────────────
    # These already exist from generation, but let's ensure they're derived
    # from the actual parsed timestamps for consistency
    df["hour_of_arrival"] = df["arrival_datetime"].dt.hour
    df["day_of_week"] = df["arrival_datetime"].dt.day_name()
    df["month"] = df["arrival_datetime"].dt.month
    df["arrival_year"] = df["arrival_datetime"].dt.year
    print("  ✓ hour_of_arrival, day_of_week, month — verified from timestamps")

    # ── 2. is_weekend ─────────────────────────────────────────────────────
    df["is_weekend"] = df["arrival_datetime"].dt.dayofweek.isin([5, 6]).astype(int)
    weekend_pct = df["is_weekend"].mean() * 100
    print(f"  ✓ is_weekend — {df['is_weekend'].sum():,} weekend visits ({weekend_pct:.1f}%)")

    # ── 3. shift ──────────────────────────────────────────────────────────
    df["shift"] = df["hour_of_arrival"].apply(assign_shift)
    shift_dist = df["shift"].value_counts()
    print(f"  ✓ shift — Morning: {shift_dist.get('Morning', 0):,}, "
          f"Evening: {shift_dist.get('Evening', 0):,}, "
          f"Night: {shift_dist.get('Night', 0):,}")

    # ── 4. acuity_label ──────────────────────────────────────────────────
    # Use esi_level_imputed for labeling (covers NaN cases)
    df["acuity_label"] = df["esi_level_imputed"].map(ESI_LABELS)
    # For rows where esi_level is NaN, also mark acuity_label
    df.loc[df["esi_level"].isna(), "acuity_label"] = "Unknown (Not Triaged)"
    acuity_dist = df["acuity_label"].value_counts()
    print(f"  ✓ acuity_label — mapped ESI 1–5 to descriptive labels")
    for label, count in acuity_dist.items():
        print(f"      {label:<25s}: {count:>6,}")

    # ── 5. time_to_disposition (minutes) ─────────────────────────────────
    # Time from seen-by-provider to departure
    # Only valid when both timestamps exist
    mask_valid = (
        df["seen_by_provider_datetime"].notna() &
        df["departure_datetime"].notna()
    )
    df["time_to_disposition"] = np.nan
    df.loc[mask_valid, "time_to_disposition"] = (
        (df.loc[mask_valid, "departure_datetime"] -
         df.loc[mask_valid, "seen_by_provider_datetime"])
        .dt.total_seconds() / 60
    ).round(0).astype(int)

    valid_ttd = df["time_to_disposition"].dropna()
    print(f"  ✓ time_to_disposition — {len(valid_ttd):,} valid records, "
          f"median: {valid_ttd.median():.0f} min, mean: {valid_ttd.mean():.0f} min")

    # ── 6. Recalculate wait_time from timestamps ────────────────────────
    # Verify wait_time_minutes matches timestamp-derived value
    mask_wait = (
        df["arrival_datetime"].notna() &
        df["seen_by_provider_datetime"].notna()
    )
    df.loc[mask_wait, "wait_time_calculated"] = (
        (df.loc[mask_wait, "seen_by_provider_datetime"] -
         df.loc[mask_wait, "arrival_datetime"])
        .dt.total_seconds() / 60
    ).round(0)

    # Use the timestamp-calculated value as source of truth
    df.loc[mask_wait, "wait_time_minutes"] = df.loc[mask_wait, "wait_time_calculated"]
    df.drop(columns=["wait_time_calculated"], inplace=True)
    print(f"  ✓ wait_time_minutes — recalculated from timestamps ({mask_wait.sum():,} records)")

    # ── 7. Recalculate total_los from timestamps ─────────────────────────
    mask_los = (
        df["arrival_datetime"].notna() &
        df["departure_datetime"].notna()
    )
    df.loc[mask_los, "total_los_minutes"] = (
        (df.loc[mask_los, "departure_datetime"] -
         df.loc[mask_los, "arrival_datetime"])
        .dt.total_seconds() / 60
    ).round(0)
    print(f"  ✓ total_los_minutes — recalculated from timestamps ({mask_los.sum():,} records)")

    # ── 8. BONUS: age_group ──────────────────────────────────────────────
    bins = [0, 5, 18, 35, 50, 65, 80, 120]
    labels = ["0-5", "6-17", "18-34", "35-49", "50-64", "65-79", "80+"]
    df["age_group"] = pd.cut(df["age"], bins=bins, labels=labels, right=False)
    print(f"  ✓ age_group — 7 bins from pediatric to elderly")

    # ── 9. BONUS: wait_time_category ─────────────────────────────────────
    def categorize_wait(minutes):
        if pd.isna(minutes):
            return np.nan
        elif minutes <= 30:
            return "Short (≤30 min)"
        elif minutes <= 60:
            return "Medium (31–60 min)"
        elif minutes <= 120:
            return "Long (61–120 min)"
        else:
            return "Very Long (>120 min)"

    df["wait_time_category"] = df["wait_time_minutes"].apply(categorize_wait)
    wait_cat_dist = df["wait_time_category"].value_counts()
    print(f"  ✓ wait_time_category — bucketed wait times:")
    for cat, count in wait_cat_dist.items():
        pct = count / len(df) * 100
        print(f"      {cat:<25s}: {count:>6,} ({pct:4.1f}%)")

    # ── Save ──────────────────────────────────────────────────────────────
    new_cols = df.shape[1] - original_cols
    df.to_csv(OUTPUT_FILE, index=False)

    print(f"\n{'=' * 65}")
    print(f"  Summary")
    print(f"{'=' * 65}")
    print(f"  Original columns:  {original_cols}")
    print(f"  New columns added: {new_cols}")
    print(f"  Total columns:     {df.shape[1]}")
    print(f"  Output: {OUTPUT_FILE}")
    print(f"{'=' * 65}")
    print(f"  ✅ Feature engineering complete!")
    print(f"{'=' * 65}")


if __name__ == "__main__":
    main()
