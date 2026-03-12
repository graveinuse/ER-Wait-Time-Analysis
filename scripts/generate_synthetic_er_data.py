"""
Synthetic ER Patient Data Generator
====================================
Downloads CMS hospital reference data and generates realistic synthetic
patient-level Emergency Room visit records for analysis.

Generates ~50,000 records with clinically plausible distributions for:
  - Patient demographics (age, gender, race/ethnicity)
  - Arrival patterns (time of day, day of week, seasonality)
  - Triage acuity levels (ESI 1–5)
  - Wait times, treatment times, and boarding times
  - Dispositions (discharged, admitted, transferred, LWBS, AMA)
  - Chief complaints and ICD-10 diagnosis codes
  - Insurance types

Usage:
    python scripts/generate_synthetic_er_data.py
"""

import os
import json
import random
import requests
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

# ─── Configuration ────────────────────────────────────────────────────────────
NUM_RECORDS = 50_000
DATE_START = datetime(2022, 1, 1)
DATE_END = datetime(2024, 12, 31)
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "raw")
SEED = 42

random.seed(SEED)
np.random.seed(SEED)

# ─── CMS Data Download ───────────────────────────────────────────────────────
CMS_HOSPITAL_API = (
    "https://data.cms.gov/provider-data/api/1/datastore/query/xubh-q36u/0"
    "?limit=500&offset=0&count=true&results=true&schema=true"
)
CMS_TIMELY_CARE_API = (
    "https://data.cms.gov/provider-data/api/1/datastore/query/yv7e-xc69/0"
    "?limit=500&offset=0&count=true&results=true&schema=true"
)


def download_cms_data():
    """Download CMS Hospital General Info and Timely & Effective Care data."""
    print("[1/4] Downloading CMS Hospital General Information...")
    try:
        resp = requests.get(CMS_HOSPITAL_API, timeout=30)
        resp.raise_for_status()
        hospital_data = resp.json()["results"]
        hospital_df = pd.DataFrame(hospital_data)
        hospital_path = os.path.join(OUTPUT_DIR, "cms_hospital_general_info.csv")
        hospital_df.to_csv(hospital_path, index=False)
        print(f"       Saved {len(hospital_df)} hospital records → {hospital_path}")
    except Exception as e:
        print(f"       ⚠ Could not download CMS hospital data: {e}")
        print("       → Using built-in hospital reference list instead.")
        hospital_df = None

    print("[2/4] Downloading CMS Timely & Effective Care data...")
    try:
        resp = requests.get(CMS_TIMELY_CARE_API, timeout=30)
        resp.raise_for_status()
        timely_data = resp.json()["results"]
        timely_df = pd.DataFrame(timely_data)
        timely_path = os.path.join(OUTPUT_DIR, "cms_timely_effective_care.csv")
        timely_df.to_csv(timely_path, index=False)
        print(f"       Saved {len(timely_df)} care measure records → {timely_path}")
    except Exception as e:
        print(f"       ⚠ Could not download CMS timely care data: {e}")
        timely_df = None

    return hospital_df, timely_df


# ─── Reference Data ──────────────────────────────────────────────────────────

# 20 representative hospitals (fallback if CMS download fails)
FALLBACK_HOSPITALS = [
    {"facility_id": "050454", "facility_name": "Cedars-Sinai Medical Center", "state": "CA", "hospital_type": "Acute Care"},
    {"facility_id": "330101", "facility_name": "NYU Langone Hospitals", "state": "NY", "hospital_type": "Acute Care"},
    {"facility_id": "220071", "facility_name": "Massachusetts General Hospital", "state": "MA", "hospital_type": "Acute Care"},
    {"facility_id": "170176", "facility_name": "University of Kansas Hospital", "state": "KS", "hospital_type": "Acute Care"},
    {"facility_id": "100007", "facility_name": "Florida Hospital Orlando", "state": "FL", "hospital_type": "Acute Care"},
    {"facility_id": "450358", "facility_name": "Baylor University Medical Center", "state": "TX", "hospital_type": "Acute Care"},
    {"facility_id": "140281", "facility_name": "Northwestern Memorial Hospital", "state": "IL", "hospital_type": "Acute Care"},
    {"facility_id": "390226", "facility_name": "Hospital of the Univ. of Pennsylvania", "state": "PA", "hospital_type": "Acute Care"},
    {"facility_id": "360180", "facility_name": "Cleveland Clinic", "state": "OH", "hospital_type": "Acute Care"},
    {"facility_id": "060024", "facility_name": "UCHealth University of Colorado Hospital", "state": "CO", "hospital_type": "Acute Care"},
    {"facility_id": "340113", "facility_name": "Duke University Hospital", "state": "NC", "hospital_type": "Acute Care"},
    {"facility_id": "520098", "facility_name": "UW Health University Hospital", "state": "WI", "hospital_type": "Acute Care"},
    {"facility_id": "230046", "facility_name": "University of Michigan Health", "state": "MI", "hospital_type": "Acute Care"},
    {"facility_id": "500024", "facility_name": "Harborview Medical Center", "state": "WA", "hospital_type": "Acute Care"},
    {"facility_id": "040062", "facility_name": "Mayo Clinic Hospital - Phoenix", "state": "AZ", "hospital_type": "Acute Care"},
    {"facility_id": "260032", "facility_name": "Barnes-Jewish Hospital", "state": "MO", "hospital_type": "Acute Care"},
    {"facility_id": "050376", "facility_name": "Stanford Health Care", "state": "CA", "hospital_type": "Acute Care"},
    {"facility_id": "210009", "facility_name": "Johns Hopkins Hospital", "state": "MD", "hospital_type": "Acute Care"},
    {"facility_id": "360085", "facility_name": "Ohio State University Wexner Medical Center", "state": "OH", "hospital_type": "Acute Care"},
    {"facility_id": "440039", "facility_name": "Vanderbilt University Medical Center", "state": "TN", "hospital_type": "Acute Care"},
]

# Chief complaints with relative frequencies and typical ESI levels
CHIEF_COMPLAINTS = [
    ("Chest Pain", 0.08, [2, 3]),
    ("Abdominal Pain", 0.10, [3, 4]),
    ("Shortness of Breath", 0.07, [2, 3]),
    ("Headache", 0.06, [3, 4]),
    ("Back Pain", 0.05, [3, 4]),
    ("Fever", 0.06, [3, 4, 5]),
    ("Laceration / Wound", 0.05, [4, 5]),
    ("Fracture / Injury", 0.07, [3, 4]),
    ("Nausea / Vomiting", 0.05, [3, 4]),
    ("Dizziness", 0.04, [3, 4]),
    ("Altered Mental Status", 0.03, [1, 2]),
    ("Allergic Reaction", 0.03, [2, 3, 4]),
    ("Seizure", 0.02, [2, 3]),
    ("Urinary Complaints", 0.04, [4, 5]),
    ("Upper Respiratory Infection", 0.05, [4, 5]),
    ("Dental Pain", 0.03, [4, 5]),
    ("Anxiety / Psychiatric", 0.04, [3, 4]),
    ("Motor Vehicle Accident", 0.03, [2, 3]),
    ("Fall", 0.04, [3, 4]),
    ("Other", 0.06, [3, 4, 5]),
]

# ICD-10 diagnosis codes mapped to chief complaints
ICD10_MAP = {
    "Chest Pain": ["R07.9", "I20.9", "I21.9", "R07.1"],
    "Abdominal Pain": ["R10.9", "R10.0", "K35.80", "K80.20"],
    "Shortness of Breath": ["R06.00", "J18.9", "J44.1", "I50.9"],
    "Headache": ["R51.9", "G43.909", "G44.1"],
    "Back Pain": ["M54.5", "M54.9", "M54.2"],
    "Fever": ["R50.9", "A49.9", "J06.9"],
    "Laceration / Wound": ["T14.8", "S61.009A", "T81.31XA"],
    "Fracture / Injury": ["S52.509A", "S82.009A", "S42.009A"],
    "Nausea / Vomiting": ["R11.2", "R11.0", "K21.0"],
    "Dizziness": ["R42", "H81.10", "R55"],
    "Altered Mental Status": ["R41.82", "E11.00", "G93.40"],
    "Allergic Reaction": ["T78.2XXA", "T78.40XA", "L50.0"],
    "Seizure": ["R56.9", "G40.909", "G40.501"],
    "Urinary Complaints": ["N39.0", "R30.0", "N20.0"],
    "Upper Respiratory Infection": ["J06.9", "J02.9", "J00"],
    "Dental Pain": ["K08.89", "K04.7", "K02.9"],
    "Anxiety / Psychiatric": ["F41.9", "F32.9", "F43.10"],
    "Motor Vehicle Accident": ["V43.52XA", "S13.4XXA", "S06.0X0A"],
    "Fall": ["W19.XXXA", "S06.0X0A", "S72.009A"],
    "Other": ["R69", "Z76.89", "R68.89"],
}

# Disposition probabilities by ESI level
DISPOSITION_BY_ESI = {
    1: {"Admitted": 0.70, "Transferred": 0.15, "Deceased": 0.10, "Discharged": 0.05},
    2: {"Admitted": 0.50, "Transferred": 0.10, "Discharged": 0.35, "AMA": 0.03, "LWBS": 0.02},
    3: {"Discharged": 0.55, "Admitted": 0.30, "Transferred": 0.05, "AMA": 0.05, "LWBS": 0.05},
    4: {"Discharged": 0.80, "Admitted": 0.05, "AMA": 0.05, "LWBS": 0.10},
    5: {"Discharged": 0.85, "AMA": 0.05, "LWBS": 0.10},
}

INSURANCE_TYPES = [
    ("Medicare", 0.25),
    ("Medicaid", 0.20),
    ("Private", 0.35),
    ("Self-Pay", 0.12),
    ("Other/Unknown", 0.08),
]

GENDER_DIST = [("Male", 0.48), ("Female", 0.50), ("Other/Unknown", 0.02)]

RACE_ETHNICITY = [
    ("White", 0.58),
    ("Black or African American", 0.18),
    ("Hispanic or Latino", 0.13),
    ("Asian", 0.06),
    ("Other/Multiracial", 0.05),
]


# ─── Helper Functions ────────────────────────────────────────────────────────

def weighted_choice(options):
    """Pick from a list of (value, weight) tuples."""
    values, weights = zip(*options)
    return random.choices(values, weights=weights, k=1)[0]


def generate_arrival_datetime():
    """
    Generate a realistic ER arrival datetime with:
      - Higher volume on Mon/Tue, lower on weekends
      - Peak hours: 10 AM – 10 PM, trough: 3–6 AM
      - Slight seasonality (winter/flu season bump)
    """
    # Random date in range
    total_days = (DATE_END - DATE_START).days
    day_offset = random.randint(0, total_days)
    base_date = DATE_START + timedelta(days=day_offset)

    # Day-of-week weighting (Mon=0 highest, Sun=6 lowest)
    dow_weight = [1.15, 1.12, 1.05, 1.00, 1.00, 0.88, 0.80]
    weight = dow_weight[base_date.weekday()]

    # Seasonality — flu season bump (Nov–Feb)
    month = base_date.month
    if month in [11, 12, 1, 2]:
        weight *= 1.12
    elif month in [6, 7, 8]:
        weight *= 0.92

    # Accept/reject sampling for day weighting
    if random.random() > weight / 1.3:
        return generate_arrival_datetime()  # re-roll

    # Hour of day distribution (bimodal: peak ~11 AM and ~7 PM)
    hour_weights = [
        0.02, 0.015, 0.012, 0.010, 0.010, 0.012,  # 0–5 AM
        0.020, 0.035, 0.050, 0.065, 0.075, 0.080,  # 6–11 AM
        0.078, 0.072, 0.065, 0.060, 0.062, 0.068,  # 12–5 PM
        0.075, 0.072, 0.065, 0.050, 0.035, 0.022,  # 6–11 PM
    ]
    hour = random.choices(range(24), weights=hour_weights, k=1)[0]
    minute = random.randint(0, 59)
    second = random.randint(0, 59)

    return base_date.replace(hour=hour, minute=minute, second=second)


def generate_age():
    """
    Generate patient age with realistic ER distribution:
      - Bimodal: peaks at pediatric (0–5) and elderly (65–85)
      - Working-age adults in between
    """
    category = random.choices(
        ["pediatric", "young_adult", "adult", "middle_age", "elderly"],
        weights=[0.12, 0.15, 0.25, 0.20, 0.28],
        k=1,
    )[0]

    if category == "pediatric":
        return max(0, int(np.random.exponential(3)))
    elif category == "young_adult":
        return int(np.random.triangular(18, 25, 35))
    elif category == "adult":
        return int(np.random.triangular(25, 38, 50))
    elif category == "middle_age":
        return int(np.random.triangular(45, 55, 65))
    else:
        return min(105, int(np.random.triangular(60, 75, 95)))


def generate_wait_and_treatment_times(esi_level, hour, dow):
    """
    Generate realistic wait, treatment, and boarding times based on:
      - ESI acuity level (higher acuity = faster to be seen)
      - Time of day (longer waits during peak hours)
      - Day of week (longer waits on weekdays)
    """
    # Base wait time in minutes by ESI (median values)
    base_wait = {1: 5, 2: 20, 3: 45, 4: 75, 5: 90}
    # Base treatment time in minutes by ESI
    base_treatment = {1: 240, 2: 180, 3: 120, 4: 60, 5: 35}

    # Peak-hour multiplier
    if 10 <= hour <= 21:
        peak_mult = 1.3
    elif 6 <= hour <= 9:
        peak_mult = 1.1
    else:
        peak_mult = 0.7

    # Day-of-week multiplier
    dow_mult = [1.15, 1.10, 1.05, 1.00, 0.95, 0.85, 0.80][dow]

    # Wait time (log-normal to get right-skewed distribution)
    mean_wait = base_wait[esi_level] * peak_mult * dow_mult
    wait_time = max(1, int(np.random.lognormal(
        mean=np.log(mean_wait), sigma=0.5
    )))

    # Treatment time
    mean_treat = base_treatment[esi_level] * (1 + 0.1 * (peak_mult - 1))
    treatment_time = max(10, int(np.random.lognormal(
        mean=np.log(mean_treat), sigma=0.35
    )))

    # Boarding time (only for admitted patients, generated later)
    boarding_time = 0

    return wait_time, treatment_time, boarding_time


def generate_vitals(age, esi_level):
    """Generate realistic triage vital signs based on age and acuity."""
    # Heart rate
    if esi_level <= 2:
        hr = int(np.random.normal(105, 20))
    else:
        hr = int(np.random.normal(82, 15))
    hr = max(40, min(200, hr))

    # Systolic BP
    if age < 18:
        sbp = int(np.random.normal(105, 10))
    elif age > 65:
        sbp = int(np.random.normal(145, 20))
    else:
        sbp = int(np.random.normal(125, 18))
    sbp = max(70, min(250, sbp))

    # Diastolic BP
    dbp = int(sbp * np.random.uniform(0.55, 0.70))
    dbp = max(40, min(140, dbp))

    # Respiratory rate
    if esi_level <= 2:
        rr = int(np.random.normal(24, 5))
    else:
        rr = int(np.random.normal(17, 3))
    rr = max(10, min(45, rr))

    # SpO2
    if esi_level == 1:
        spo2 = max(70, min(100, int(np.random.normal(90, 6))))
    elif esi_level == 2:
        spo2 = max(80, min(100, int(np.random.normal(94, 4))))
    else:
        spo2 = max(88, min(100, int(np.random.normal(97, 2))))

    # Temperature (Fahrenheit)
    temp = round(np.random.normal(98.6, 0.8), 1)
    if random.random() < 0.12:  # ~12% have fever
        temp = round(np.random.uniform(100.4, 104.0), 1)
    temp = max(95.0, min(106.0, temp))

    # Pain scale (0–10)
    if esi_level <= 2:
        pain = min(10, max(0, int(np.random.normal(7, 2))))
    elif esi_level == 3:
        pain = min(10, max(0, int(np.random.normal(5, 2))))
    else:
        pain = min(10, max(0, int(np.random.normal(3, 2.5))))

    return hr, sbp, dbp, rr, spo2, temp, pain


# ─── Main Generator ──────────────────────────────────────────────────────────

def generate_records(hospitals):
    """Generate synthetic ER visit records."""
    print(f"[3/4] Generating {NUM_RECORDS:,} synthetic ER visit records...")

    complaint_names = [c[0] for c in CHIEF_COMPLAINTS]
    complaint_weights = [c[1] for c in CHIEF_COMPLAINTS]
    complaint_esi_map = {c[0]: c[2] for c in CHIEF_COMPLAINTS}

    records = []
    for i in range(NUM_RECORDS):
        if (i + 1) % 10000 == 0:
            print(f"       ... generated {i + 1:,} records")

        # Hospital assignment
        hosp = random.choice(hospitals)

        # Patient demographics
        patient_id = f"P{100000 + i}"
        age = generate_age()
        gender = weighted_choice(GENDER_DIST)
        race = weighted_choice(RACE_ETHNICITY)
        insurance = weighted_choice(INSURANCE_TYPES)

        # Adjust insurance by age
        if age >= 65 and random.random() < 0.70:
            insurance = "Medicare"
        elif age < 18 and random.random() < 0.40:
            insurance = "Medicaid"

        # Arrival
        arrival_dt = generate_arrival_datetime()
        arrival_mode = random.choices(
            ["Walk-in", "Ambulance", "Transfer", "Police"],
            weights=[0.60, 0.30, 0.07, 0.03],
            k=1,
        )[0]

        # Chief complaint and ESI
        complaint = random.choices(complaint_names, weights=complaint_weights, k=1)[0]
        possible_esi = complaint_esi_map[complaint]
        esi_level = random.choice(possible_esi)

        # Age-based ESI adjustments
        if age >= 75 and esi_level > 2:
            esi_level = max(2, esi_level - 1) if random.random() < 0.3 else esi_level
        if age < 5 and esi_level > 3:
            esi_level = max(3, esi_level - 1) if random.random() < 0.2 else esi_level

        # Ambulance arrivals tend to be higher acuity
        if arrival_mode == "Ambulance" and esi_level > 3:
            esi_level = max(2, esi_level - 1) if random.random() < 0.5 else esi_level

        # ICD-10 diagnosis
        icd10 = random.choice(ICD10_MAP[complaint])

        # Wait and treatment times
        hour = arrival_dt.hour
        dow = arrival_dt.weekday()
        wait_min, treatment_min, boarding_min = generate_wait_and_treatment_times(
            esi_level, hour, dow
        )

        # Disposition
        disp_options = DISPOSITION_BY_ESI[esi_level]
        disposition = random.choices(
            list(disp_options.keys()), weights=list(disp_options.values()), k=1
        )[0]

        # LWBS patients have zero treatment time, short wait
        if disposition == "LWBS":
            treatment_min = 0
            wait_min = max(30, wait_min)  # waited at least 30 min before leaving

        # Boarding time for admitted patients (wait for inpatient bed)
        if disposition == "Admitted":
            boarding_min = max(0, int(np.random.lognormal(mean=np.log(120), sigma=0.6)))
        elif disposition == "Transferred":
            boarding_min = max(0, int(np.random.lognormal(mean=np.log(90), sigma=0.5)))

        # Calculate timestamps
        triage_dt = arrival_dt + timedelta(minutes=max(1, int(wait_min * 0.15)))
        seen_by_provider_dt = arrival_dt + timedelta(minutes=wait_min)
        treatment_end_dt = seen_by_provider_dt + timedelta(minutes=treatment_min)
        departure_dt = treatment_end_dt + timedelta(minutes=boarding_min)

        # Total LOS in minutes
        total_los_min = int((departure_dt - arrival_dt).total_seconds() / 60)

        # Vitals
        hr, sbp, dbp, rr, spo2, temp, pain = generate_vitals(age, esi_level)

        # Provider assignment
        provider_id = f"DR{random.randint(1001, 1050)}"

        # Build record
        records.append({
            "visit_id": f"V{200000 + i}",
            "patient_id": patient_id,
            "facility_id": hosp.get("facility_id", ""),
            "facility_name": hosp.get("facility_name", ""),
            "state": hosp.get("state", ""),
            "arrival_datetime": arrival_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "arrival_date": arrival_dt.strftime("%Y-%m-%d"),
            "arrival_hour": hour,
            "arrival_day_of_week": arrival_dt.strftime("%A"),
            "arrival_month": arrival_dt.month,
            "arrival_year": arrival_dt.year,
            "arrival_mode": arrival_mode,
            "age": age,
            "gender": gender,
            "race_ethnicity": race,
            "insurance_type": insurance,
            "chief_complaint": complaint,
            "esi_level": esi_level,
            "icd10_code": icd10,
            "triage_datetime": triage_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "seen_by_provider_datetime": seen_by_provider_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "treatment_end_datetime": treatment_end_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "departure_datetime": departure_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "wait_time_minutes": wait_min,
            "treatment_time_minutes": treatment_min,
            "boarding_time_minutes": boarding_min,
            "total_los_minutes": total_los_min,
            "heart_rate": hr,
            "systolic_bp": sbp,
            "diastolic_bp": dbp,
            "respiratory_rate": rr,
            "spo2": spo2,
            "temperature_f": temp,
            "pain_scale": pain,
            "disposition": disposition,
            "provider_id": provider_id,
        })

    return pd.DataFrame(records)


def main():
    print("=" * 65)
    print("  Hospital ER Synthetic Data Generator")
    print("=" * 65)

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Step 1–2: Download CMS reference data
    hospital_df, timely_df = download_cms_data()

    # Build hospital list from CMS data or fallback
    if hospital_df is not None and len(hospital_df) > 0:
        # Filter to acute care hospitals with emergency services
        mask = hospital_df["hospital_type"].str.contains("Acute", case=False, na=False)
        if "emergency_services" in hospital_df.columns:
            mask = mask & (hospital_df["emergency_services"].str.lower() == "yes")
        acute_hospitals = hospital_df[mask].to_dict("records")

        if len(acute_hospitals) >= 20:
            # Sample 20 hospitals for manageable variety
            hospitals = random.sample(acute_hospitals, min(30, len(acute_hospitals)))
        else:
            hospitals = acute_hospitals if acute_hospitals else FALLBACK_HOSPITALS
    else:
        hospitals = FALLBACK_HOSPITALS

    print(f"       Using {len(hospitals)} hospitals as facility anchors.\n")

    # Step 3: Generate synthetic records
    df = generate_records(hospitals)

    # Step 4: Save
    print(f"\n[4/4] Saving dataset...")
    output_path = os.path.join(OUTPUT_DIR, "er_synthetic_patient_visits.csv")
    df.to_csv(output_path, index=False)
    print(f"       Saved {len(df):,} records → {output_path}")

    # Print summary statistics
    print("\n" + "=" * 65)
    print("  Dataset Summary")
    print("=" * 65)
    print(f"  Total records:        {len(df):,}")
    print(f"  Date range:           {df['arrival_date'].min()} → {df['arrival_date'].max()}")
    print(f"  Unique hospitals:     {df['facility_id'].nunique()}")
    print(f"  Unique patients:      {df['patient_id'].nunique()}")
    print(f"\n  ESI Level Distribution:")
    for esi in sorted(df["esi_level"].unique()):
        count = (df["esi_level"] == esi).sum()
        pct = count / len(df) * 100
        print(f"    ESI {esi}: {count:>6,} ({pct:5.1f}%)")
    print(f"\n  Disposition Distribution:")
    for disp, count in df["disposition"].value_counts().items():
        pct = count / len(df) * 100
        print(f"    {disp:<15s}: {count:>6,} ({pct:5.1f}%)")
    print(f"\n  Wait Time (minutes):  median={df['wait_time_minutes'].median():.0f}, "
          f"mean={df['wait_time_minutes'].mean():.0f}, "
          f"95th pctile={df['wait_time_minutes'].quantile(0.95):.0f}")
    print(f"  LOS (minutes):        median={df['total_los_minutes'].median():.0f}, "
          f"mean={df['total_los_minutes'].mean():.0f}, "
          f"95th pctile={df['total_los_minutes'].quantile(0.95):.0f}")
    print("=" * 65)
    print("  ✅ Done! Data is ready for analysis.")
    print("=" * 65)


if __name__ == "__main__":
    main()
