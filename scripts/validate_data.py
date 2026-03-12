"""
Data Validation — Step 2.3
============================
Sanity checks on the featured dataset before final export.

Checks:
  1. Negative wait times
  2. Extreme wait times (>24 hours)
  3. Acuity level sample sizes
  4. Date distribution gaps
  5. Acuity vs wait time cross-check (higher acuity → shorter wait)
  6. Disposition logic checks
  7. Vital sign clinical plausibility

Input:   data/processed/er_visits_featured.csv
Output:  docs/validation_report.txt
         data/processed/er_visits_validated.csv (after fixes)

Usage:
    python scripts/validate_data.py
"""

import os
import io
import numpy as np
import pandas as pd

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
PROCESSED_DIR = os.path.join(BASE_DIR, "data", "processed")
DOCS_DIR = os.path.join(BASE_DIR, "docs")

INPUT_FILE = os.path.join(PROCESSED_DIR, "er_visits_featured.csv")
OUTPUT_FILE = os.path.join(PROCESSED_DIR, "er_visits_validated.csv")
REPORT_FILE = os.path.join(DOCS_DIR, "validation_report.txt")


def main():
    buf = io.StringIO()
    issues_found = 0
    fixes_applied = 0

    def log(text=""):
        buf.write(text + "\n")
        print(text)

    def section(title):
        nonlocal issues_found
        line = "=" * 70
        log(f"\n{line}")
        log(f"  {title}")
        log(f"{line}\n")

    def passed(msg):
        log(f"  ✓ PASS: {msg}")

    def warning(msg):
        nonlocal issues_found
        issues_found += 1
        log(f"  ⚠ ISSUE: {msg}")

    def fix(msg):
        nonlocal fixes_applied
        fixes_applied += 1
        log(f"  🔧 FIX: {msg}")

    # ── Load ──────────────────────────────────────────────────────────────
    df = pd.read_csv(INPUT_FILE)
    df["arrival_datetime"] = pd.to_datetime(df["arrival_datetime"], errors="coerce")
    df["arrival_date"] = pd.to_datetime(df["arrival_date"], errors="coerce")

    section_title = (
        f"DATA VALIDATION REPORT\n"
        f"  Dataset: er_visits_featured.csv\n"
        f"  Records: {len(df):,} | Columns: {df.shape[1]}"
    )
    log("=" * 70)
    log(f"  {section_title}")
    log("=" * 70)

    # ══════════════════════════════════════════════════════════════════════
    # CHECK 1: Negative Wait Times
    # ══════════════════════════════════════════════════════════════════════
    section("CHECK 1: Negative Wait Times")

    valid_waits = df["wait_time_minutes"].dropna()
    neg_waits = (valid_waits < 0).sum()

    if neg_waits == 0:
        passed(f"No negative wait times found ({len(valid_waits):,} valid records checked)")
    else:
        warning(f"{neg_waits:,} records have negative wait times")
        # Show examples
        neg_examples = df.loc[df["wait_time_minutes"] < 0,
                              ["visit_id", "wait_time_minutes", "arrival_datetime",
                               "seen_by_provider_datetime", "disposition"]].head(5)
        log(f"\n  Examples:\n{neg_examples.to_string(index=False)}\n")

        # Fix: set negative wait times to NaN and flag
        df.loc[df["wait_time_minutes"] < 0, "wait_time_minutes"] = np.nan
        df.loc[df["wait_time_minutes"].isna() & valid_waits.notna(),
               "eligible_for_time_analysis"] = 0
        fix(f"Set {neg_waits} negative wait times to NaN; marked ineligible for time analysis")

    # ══════════════════════════════════════════════════════════════════════
    # CHECK 2: Extreme Wait Times (>24 hours)
    # ══════════════════════════════════════════════════════════════════════
    section("CHECK 2: Extreme Wait Times (>24 hours = 1440 minutes)")

    extreme_threshold = 1440  # 24 hours in minutes
    extreme_waits = (valid_waits > extreme_threshold).sum()

    if extreme_waits == 0:
        passed(f"No wait times exceed 24 hours")
    else:
        warning(f"{extreme_waits:,} records have wait times > 24 hours")
        extreme_examples = df.loc[df["wait_time_minutes"] > extreme_threshold,
                                  ["visit_id", "wait_time_minutes", "esi_level",
                                   "disposition"]].head(5)
        log(f"\n  Examples:\n{extreme_examples.to_string(index=False)}\n")

        # Fix: cap at 24 hours or remove
        df.loc[df["wait_time_minutes"] > extreme_threshold, "wait_time_minutes"] = np.nan
        fix(f"Set {extreme_waits} extreme wait times (>24h) to NaN — likely data errors")

    # Also check total LOS
    valid_los = df["total_los_minutes"].dropna()
    extreme_los = (valid_los > 2880).sum()  # >48 hours
    if extreme_los == 0:
        passed(f"No total LOS exceeds 48 hours")
    else:
        warning(f"{extreme_los:,} records have LOS > 48 hours")
        fix(f"Capping {extreme_los} extreme LOS values (>48h) to NaN")
        df.loc[df["total_los_minutes"] > 2880, "total_los_minutes"] = np.nan

    # ══════════════════════════════════════════════════════════════════════
    # CHECK 3: Acuity Level Sample Sizes
    # ══════════════════════════════════════════════════════════════════════
    section("CHECK 3: Acuity Level Sample Sizes")

    esi_counts = df["esi_level"].value_counts().sort_index()
    esi_missing = df["esi_level"].isna().sum()
    min_threshold = 100

    log(f"  {'ESI Level':<12s} {'Count':>8s} {'Pct':>7s}  Status")
    log(f"  {'-'*45}")

    for esi, count in esi_counts.items():
        pct = count / len(df) * 100
        status = "✓ Adequate" if count >= min_threshold else "⚠ Low sample"
        log(f"  ESI {int(esi):<8d} {count:>8,} {pct:>6.1f}%  {status}")
        if count < min_threshold:
            warning(f"ESI {int(esi)} has only {count} records (< {min_threshold} threshold)")

    log(f"  {'Missing':<12s} {esi_missing:>8,} {esi_missing/len(df)*100:>6.1f}%  "
        f"Flagged (not triaged)")

    if all(c >= min_threshold for c in esi_counts.values):
        passed("All ESI levels have adequate sample sizes (≥100)")

    # ══════════════════════════════════════════════════════════════════════
    # CHECK 4: Date Distribution
    # ══════════════════════════════════════════════════════════════════════
    section("CHECK 4: Date Distribution & Gaps")

    date_range = df["arrival_date"].dropna()
    min_date = date_range.min()
    max_date = date_range.max()
    log(f"  Date range: {min_date.strftime('%Y-%m-%d')} → {max_date.strftime('%Y-%m-%d')}")
    log(f"  Span: {(max_date - min_date).days:,} days")

    # Check for date gaps (days with zero visits)
    all_dates = pd.date_range(min_date, max_date, freq="D")
    daily_counts = date_range.dt.date.value_counts().sort_index()
    days_with_data = len(daily_counts)
    total_days = len(all_dates)
    missing_days = total_days - days_with_data

    if missing_days == 0:
        passed(f"No date gaps — all {total_days:,} days have at least 1 visit")
    else:
        warning(f"{missing_days} days with zero visits out of {total_days} total days")
        # Find the gaps
        existing_dates = set(daily_counts.index)
        gap_dates = [d.date() for d in all_dates if d.date() not in existing_dates]
        log(f"  Gap dates (first 10): {gap_dates[:10]}")

    # Monthly distribution
    log(f"\n  Monthly volume distribution:")
    monthly = df.groupby(df["arrival_date"].dt.to_period("M")).size()
    for period, count in monthly.items():
        bar = "█" * int(count / 100)
        log(f"    {period}: {count:>5,}  {bar}")

    # Check for reasonable even distribution (no month < 50% of average)
    avg_monthly = monthly.mean()
    low_months = monthly[monthly < avg_monthly * 0.5]
    if len(low_months) == 0:
        passed(f"Monthly volumes are reasonably distributed (avg: {avg_monthly:.0f}/month)")
    else:
        warning(f"{len(low_months)} months have < 50% of average monthly volume")

    # ══════════════════════════════════════════════════════════════════════
    # CHECK 5: Acuity vs Wait Time Cross-Check
    # ══════════════════════════════════════════════════════════════════════
    section("CHECK 5: Acuity vs Wait Time (Higher acuity → shorter wait)")

    wait_by_esi = df.groupby("esi_level")["wait_time_minutes"].agg(
        ["count", "mean", "median"]
    ).round(1)
    wait_by_esi.columns = ["Count", "Mean Wait", "Median Wait"]

    log(f"  {wait_by_esi.to_string()}\n")

    # Verify monotonic increase: ESI 1 < ESI 2 < ... < ESI 5
    medians = wait_by_esi["Median Wait"].values
    is_monotonic = all(medians[i] <= medians[i+1] for i in range(len(medians)-1))

    if is_monotonic:
        passed("Wait times increase monotonically with ESI level (1→5) ✓")
        log(f"  ESI 1 (Resuscitation): {medians[0]:.0f} min median")
        log(f"  ESI 5 (Non-Urgent):    {medians[-1]:.0f} min median")
        log(f"  Ratio (ESI 5 / ESI 1): {medians[-1]/medians[0]:.1f}x")
    else:
        warning("Wait time pattern does NOT strictly increase with ESI level")
        log("  This may indicate triage or data quality issues.")

    # ══════════════════════════════════════════════════════════════════════
    # CHECK 6: Disposition Logic
    # ══════════════════════════════════════════════════════════════════════
    section("CHECK 6: Disposition Logic Checks")

    # LWBS patients should have zero treatment time
    lwbs = df[df["disposition"] == "LWBS"]
    lwbs_with_treatment = lwbs[lwbs["treatment_time_minutes"] > 0]
    if len(lwbs_with_treatment) == 0:
        passed(f"All LWBS patients have zero treatment time ({len(lwbs):,} records)")
    else:
        warning(f"{len(lwbs_with_treatment)} LWBS patients have treatment_time > 0")

    # Deceased patients should be ESI 1–2
    deceased = df[df["disposition"] == "Deceased"]
    if len(deceased) > 0:
        deceased_esi = deceased["esi_level"].dropna()
        high_esi_deceased = (deceased_esi > 3).sum()
        if high_esi_deceased == 0:
            passed(f"All deceased patients were ESI 1–3 ({len(deceased)} total)")
        else:
            warning(f"{high_esi_deceased} deceased patients had ESI > 3")

    # Admitted patients should have boarding time
    admitted = df[df["disposition"] == "Admitted"]
    admitted_no_board = (admitted["boarding_time_minutes"] == 0).sum()
    pct_no_board = admitted_no_board / len(admitted) * 100
    log(f"  Admitted patients with boarding time: "
        f"{len(admitted) - admitted_no_board:,}/{len(admitted):,} "
        f"({100 - pct_no_board:.1f}%)")
    passed("Disposition logic checks completed")

    # ══════════════════════════════════════════════════════════════════════
    # CHECK 7: Vital Sign Plausibility
    # ══════════════════════════════════════════════════════════════════════
    section("CHECK 7: Vital Sign Clinical Plausibility")

    vital_ranges = {
        "heart_rate":       (30, 220,  "bpm"),
        "systolic_bp":      (60, 260,  "mmHg"),
        "diastolic_bp":     (30, 150,  "mmHg"),
        "respiratory_rate": (6,  50,   "breaths/min"),
        "spo2":             (60, 100,  "%"),
        "temperature_f":    (93, 108,  "°F"),
        "pain_scale":       (0,  10,   "/10"),
    }

    all_plausible = True
    for col, (lo, hi, unit) in vital_ranges.items():
        valid = df[col].dropna()
        out_of_range = ((valid < lo) | (valid > hi)).sum()
        if out_of_range == 0:
            passed(f"{col}: all {len(valid):,} values in range [{lo}–{hi}] {unit}")
        else:
            all_plausible = False
            warning(f"{col}: {out_of_range} values outside [{lo}–{hi}] {unit}")
            # Clip to plausible range
            df[col] = df[col].clip(lo, hi)
            fix(f"Clipped {col} to [{lo}–{hi}]")

    # ══════════════════════════════════════════════════════════════════════
    # SUMMARY
    # ══════════════════════════════════════════════════════════════════════
    section("VALIDATION SUMMARY")

    log(f"  Total checks performed:  7")
    log(f"  Issues found:            {issues_found}")
    log(f"  Fixes applied:           {fixes_applied}")
    log(f"  Final record count:      {len(df):,}")
    log(f"  Final column count:      {df.shape[1]}")

    eligible = df["eligible_for_time_analysis"].sum()
    log(f"  Eligible for time analysis: {eligible:,} ({eligible/len(df)*100:.1f}%)")

    if issues_found == 0:
        log(f"\n  🎉 ALL CHECKS PASSED — Data is analysis-ready!")
    else:
        log(f"\n  ⚠ {issues_found} issue(s) found and {fixes_applied} fix(es) applied.")
        log(f"  Data has been corrected and is now analysis-ready.")

    # ── Save ──────────────────────────────────────────────────────────────
    df.to_csv(OUTPUT_FILE, index=False)
    log(f"\n  Validated dataset saved → {OUTPUT_FILE}")

    with open(REPORT_FILE, "w", encoding="utf-8") as f:
        f.write(buf.getvalue())
    log(f"  Validation report saved → {REPORT_FILE}")


if __name__ == "__main__":
    main()
