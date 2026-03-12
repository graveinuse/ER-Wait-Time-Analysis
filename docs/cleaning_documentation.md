# Data Cleaning & Preparation Documentation

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
===========================================================================
  DATA CLEANING LOG — ER Patient Visits
  Generated: 2026-03-12 10:49:05
  Script: scripts/clean_data.py
===========================================================================

===========================================================================
  SECTION 1: PRE-CLEANING MISSING VALUE ASSESSMENT
===========================================================================

  Dataset shape: 50,000 rows × 36 columns
  Total cells: 1,800,000
  Total missing values: 46,408 (2.58%)

  Column                          Missing     Pct  Type        
  ----------------------------------------------------------------------
  age                                 479    1.0%  float64     
  race_ethnicity                    2,002    4.0%  object      
  insurance_type                      953    1.9%  object      
  chief_complaint                     921    1.8%  object      
  esi_level                         2,021    4.0%  float64     
  triage_datetime                   1,206    2.4%  object      
  seen_by_provider_datetime         3,774    7.5%  object      
  departure_datetime                  698    1.4%  object      
  total_los_minutes                   698    1.4%  float64     
  heart_rate                        4,356    8.7%  float64     
  systolic_bp                       4,346    8.7%  float64     
  diastolic_bp                      4,355    8.7%  float64     
  respiratory_rate                  4,375    8.8%  float64     
  spo2                              4,389    8.8%  float64     
  temperature_f                     4,397    8.8%  float64     
  pain_scale                        7,438   14.9%  float64     
  

===========================================================================
  SECTION 2: ESI LEVEL (TRIAGE ACUITY)
===========================================================================

  Column: esi_level
  Missing: 2,021 (4.0%)
  Why missing: Systematic — LWBS patients often not formally triaged; some documentation gaps during high-volume periods.
  Decision: FLAG (not fill). Created 'esi_missing_flag' column. Missing ESI preserved as NaN for transparency.
  Detail: Disposition of missing-ESI visits: {'Discharged': 971, 'LWBS': 566, 'Admitted': 338, 'AMA': 85, 'Transferred': 58, 'Deceased': 3}

  NOTE: Also created 'esi_level_imputed' using mode within disposition group — use only when ESI is required for analysis.

===========================================================================
  SECTION 3: VITAL SIGNS
===========================================================================

  Column: heart_rate
  Missing: 4,356 (8.7%)
  Why missing: Correlated — equipment issues, patient refusal, pediatric patients (often missing together).
  Decision: FILL with median WITHIN ESI level. Created 'heart_rate_imputed_flag'. Medians: {ESI 1.0: 106, ESI 2.0: 104, ESI 3.0: 82, ESI 4.0: 81, ESI 5.0: 81}
  Detail: 0 remaining nulls (missing ESI too) filled with global median.

  Column: systolic_bp
  Missing: 4,346 (8.7%)
  Why missing: Correlated — equipment issues, patient refusal, pediatric patients (often missing together).
  Decision: FILL with median WITHIN ESI level. Created 'systolic_bp_imputed_flag'. Medians: {ESI 1.0: 126, ESI 2.0: 129, ESI 3.0: 126, ESI 4.0: 127, ESI 5.0: 126}
  Detail: 0 remaining nulls (missing ESI too) filled with global median.

  Column: diastolic_bp
  Missing: 4,355 (8.7%)
  Why missing: Correlated — equipment issues, patient refusal, pediatric patients (often missing together).
  Decision: FILL with median WITHIN ESI level. Created 'diastolic_bp_imputed_flag'. Medians: {ESI 1.0: 78, ESI 2.0: 80, ESI 3.0: 78, ESI 4.0: 78, ESI 5.0: 78}
  Detail: 0 remaining nulls (missing ESI too) filled with global median.

  Column: respiratory_rate
  Missing: 4,375 (8.8%)
  Why missing: Correlated — equipment issues, patient refusal, pediatric patients (often missing together).
  Decision: FILL with median WITHIN ESI level. Created 'respiratory_rate_imputed_flag'. Medians: {ESI 1.0: 24, ESI 2.0: 23, ESI 3.0: 17, ESI 4.0: 17, ESI 5.0: 17}
  Detail: 0 remaining nulls (missing ESI too) filled with global median.

  Column: spo2
  Missing: 4,389 (8.8%)
  Why missing: Correlated — equipment issues, patient refusal, pediatric patients (often missing together).
  Decision: FILL with median WITHIN ESI level. Created 'spo2_imputed_flag'. Medians: {ESI 1.0: 89, ESI 2.0: 94, ESI 3.0: 96, ESI 4.0: 97, ESI 5.0: 96}
  Detail: 0 remaining nulls (missing ESI too) filled with global median.

  Column: temperature_f
  Missing: 4,397 (8.8%)
  Why missing: Correlated — equipment issues, patient refusal, pediatric patients (often missing together).
  Decision: FILL with median WITHIN ESI level. Created 'temperature_f_imputed_flag'. Medians: {ESI 1.0: 99, ESI 2.0: 99, ESI 3.0: 99, ESI 4.0: 99, ESI 5.0: 99}
  Detail: 0 remaining nulls (missing ESI too) filled with global median.


===========================================================================
  SECTION 4: PAIN SCALE
===========================================================================

  Column: pain_scale
  Missing: 7,438 (14.9%)
  Why missing: Systematic — pediatric (<3 yrs), altered mental status, and unconscious patients cannot self-report pain.
  Decision: FLAG + FILL with median within ESI level. Created 'pain_scale_missing_flag'.
  Detail: Mean age of missing: 29 yrs. Top complaints: {'Altered Mental Status': 744, 'Abdominal Pain': 685, 'Chest Pain': 507, 'Fracture / Injury': 477, 'Shortness of Breath': 475}


===========================================================================
  SECTION 5: TIMESTAMPS
===========================================================================

  Column: seen_by_provider_datetime
  Missing: 3,774 (7.5%)
  Why missing: Systematic — LWBS patients never seen by a provider; some data entry gaps.
  Decision: FLAG. Keep NaN. Records excluded from door-to-provider time calculations but included in volume analysis.

  Column: departure_datetime
  Missing: 698 (1.4%)
  Why missing: Random — data entry errors, system downtime, patients who left without notification.
  Decision: FLAG. Keep NaN. Total LOS also set to NaN for these. Excluded from LOS analysis.

  Column: triage_datetime
  Missing: 1,206 (2.4%)
  Why missing: Systematic — correlated with missing ESI; patient not formally triaged.
  Decision: FLAG. Keep NaN.

  NOTE: Created 'eligible_for_time_analysis' flag: 49,302 (98.6%) records eligible for wait-time / LOS calculations.

===========================================================================
  SECTION 6: DEMOGRAPHICS
===========================================================================

  Column: race_ethnicity
  Missing: 2,002 (4.0%)
  Why missing: Mixed — patient declined, emergency situations, language barriers.
  Decision: FILL with 'Unknown/Declined'. This preserves the category for analysis without assuming a race.

  Column: insurance_type
  Missing: 953 (1.9%)
  Why missing: Random — registration incomplete at time of data pull.
  Decision: FILL with 'Unknown'. Cannot safely infer payer type.

  Column: age
  Missing: 479 (1.0%)
  Why missing: Rare — unidentified patients (John/Jane Doe), emergency situations.
  Decision: FLAG + FILL with median age (45). Created 'age_imputed_flag'.


===========================================================================
  SECTION 7: CHIEF COMPLAINT
===========================================================================

  Column: chief_complaint
  Missing: 921 (1.8%)
  Why missing: Mixed — unconscious patients, language barriers, data entry errors.
  Decision: FILL with 'Unknown/Not Documented'. Preserves record for other analyses.


===========================================================================
  SECTION 8: POST-CLEANING VALIDATION
===========================================================================

  Column                              Remaining NaN  Status
  -----------------------------------------------------------------
  esi_level                                   2,021  ✓ Expected (documented)
  triage_datetime                             1,206  ✓ Expected (documented)
  seen_by_provider_datetime                   3,774  ✓ Expected (documented)
  departure_datetime                            698  ✓ Expected (documented)
  total_los_minutes                             698  ✓ Expected (documented)
  NOTE: All remaining nulls are in expected columns with documented reasons.
  
  --- Data Integrity Checks ---
  Negative wait times: 0 ✓
  heart_rate out of range [20-250]: 0 ✓
  systolic_bp out of range [50-300]: 0 ✓
  respiratory_rate out of range [5-60]: 0 ✓
  spo2 out of range [50-100]: 0 ✓
  temperature_f out of range [90-110]: 0 ✓
  
  --- Final Dataset Summary ---
  Total records: 50,000
  Total columns: 50 (original 36 + 14 flag columns)
  Eligible for time analysis: 49,302 (98.6%)
  Records with imputed vitals: 9,223
```

### Validation Report
```
======================================================================
  DATA VALIDATION REPORT
  Dataset: er_visits_featured.csv
  Records: 50,000 | Columns: 59
======================================================================

======================================================================
  CHECK 1: Negative Wait Times
======================================================================

  ✓ PASS: No negative wait times found (50,000 valid records checked)

======================================================================
  CHECK 2: Extreme Wait Times (>24 hours = 1440 minutes)
======================================================================

  ✓ PASS: No wait times exceed 24 hours
  ✓ PASS: No total LOS exceeds 48 hours

======================================================================
  CHECK 3: Acuity Level Sample Sizes
======================================================================

  ESI Level       Count     Pct  Status
  ---------------------------------------------
  ESI 1             723    1.4%  ✓ Adequate
  ESI 2           6,877   13.8%  ✓ Adequate
  ESI 3          20,717   41.4%  ✓ Adequate
  ESI 4          14,850   29.7%  ✓ Adequate
  ESI 5           4,812    9.6%  ✓ Adequate
  Missing         2,021    4.0%  Flagged (not triaged)
  ✓ PASS: All ESI levels have adequate sample sizes (≥100)

======================================================================
  CHECK 4: Date Distribution & Gaps
======================================================================

  Date range: 2022-01-01 → 2024-12-31
  Span: 1,095 days
  ✓ PASS: No date gaps — all 1,096 days have at least 1 visit

  Monthly volume distribution:
    2022-01: 1,499  ██████████████
    2022-02: 1,376  █████████████
    2022-03: 1,409  ██████████████
    2022-04: 1,387  █████████████
    2022-05: 1,391  █████████████
    2022-06: 1,330  █████████████
    2022-07: 1,280  ████████████
    2022-08: 1,290  ████████████
    2022-09: 1,308  █████████████
    2022-10: 1,417  ██████████████
    2022-11: 1,514  ███████████████
    2022-12: 1,563  ███████████████
    2023-01: 1,484  ██████████████
    2023-02: 1,374  █████████████
    2023-03: 1,403  ██████████████
    2023-04: 1,306  █████████████
    2023-05: 1,361  █████████████
    2023-06: 1,223  ████████████
    2023-07: 1,262  ████████████
    2023-08: 1,342  █████████████
    2023-09: 1,369  █████████████
    2023-10: 1,295  ████████████
    2023-11: 1,513  ███████████████
    2023-12: 1,527  ███████████████
    2024-01: 1,596  ███████████████
    2024-02: 1,481  ██████████████
    2024-03: 1,367  █████████████
    2024-04: 1,394  █████████████
    2024-05: 1,387  █████████████
    2024-06: 1,227  ████████████
    2024-07: 1,254  ████████████
    2024-08: 1,319  █████████████
    2024-09: 1,420  ██████████████
    2024-10: 1,343  █████████████
    2024-11: 1,442  ██████████████
    2024-12: 1,547  ███████████████
  ✓ PASS: Monthly volumes are reasonably distributed (avg: 1389/month)

======================================================================
  CHECK 5: Acuity vs Wait Time (Higher acuity → shorter wait)
======================================================================

             Count  Mean Wait  Median Wait
esi_level                               
1.0          723        6.3          6.0
2.0         6877       26.8         23.0
3.0        20717       60.3         52.0
4.0        14850      101.7         88.0
5.0         4812      122.7        107.0

  ✓ PASS: Wait times increase monotonically with ESI level (1→5) ✓
  ESI 1 (Resuscitation): 6 min median
  ESI 5 (Non-Urgent):    107 min median
  Ratio (ESI 5 / ESI 1): 17.8x

======================================================================
  CHECK 6: Disposition Logic Checks
======================================================================

  ✓ PASS: All LWBS patients have zero treatment time (3,322 records)
  ✓ PASS: All deceased patients were ESI 1–3 (75 total)
  Admitted patients with boarding time: 11,366/11,366 (100.0%)
  ✓ PASS: Disposition logic checks completed

======================================================================
  CHECK 7: Vital Sign Clinical Plausibility
======================================================================

  ✓ PASS: heart_rate: all 50,000 values in range [30–220] bpm
  ✓ PASS: systolic_bp: all 50,000 values in range [60–260] mmHg
  ✓ PASS: diastolic_bp: all 50,000 values in range [30–150] mmHg
  ✓ PASS: respiratory_rate: all 50,000 values in range [6–50] breaths/min
  ✓ PASS: spo2: all 50,000 values in range [60–100] %
  ✓ PASS: temperature_f: all 50,000 values in range [93–108] °F
  ✓ PASS: pain_scale: all 50,000 values in range [0–10] /10

======================================================================
  VALIDATION SUMMARY
======================================================================

  Total checks performed:  7
  Issues found:            0
  Fixes applied:           0
  Final record count:      50,000
  Final column count:      59
  Eligible for time analysis: 49,302 (98.6%)

  🎉 ALL CHECKS PASSED — Data is analysis-ready!

  Validated dataset saved → C:\Users\malli\Desktop\PROJECTS\HOSPITAL-ER\ER-Wait-Time-Analysis\data\processed\er_visits_validated.csv

```
