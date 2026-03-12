# Hospital Emergency Room Wait Time & Patient Flow Analysis

## Problem Statement
Emergency Departments (ED) operate in high-stress, resource-constrained environments where inefficient patient flow results in prolonged wait times, increased Leaving Without Being Seen (LWBS) rates, and deteriorating patient outcomes. The goal of this project is to analyze clinical staging times and identify operational bottlenecks to provide actionable, data-driven recommendations that optimize scheduling, resource allocation, and patient prioritization.

## Data Source
This project utilizes a synthetic 50,000-record dataset mathematically modeled after real CMS (Centers for Medicare & Medicaid Services) public data and typical ED volume patterns. 
- **Records:** 50,000 ED visits
- **Date Range:** 2022-01-01 to 2024-12-31
- **Contents:** Clinical timestamps (arrival, triage, provider, departure), triage acuity (ESI 1-5), vital signs, chief complaints, and patient demographics.
- **Reference Data:** CMS Hospital General Information and Timely/Effective Care measures.

## Tech Stack
- **Languages:** Python
- **Data Processing:** Pandas, NumPy
- **Visualization:** Matplotlib, Seaborn, Tableau
- **Version Control:** Git

## Key Findings
1. **Wait Times Scale Exponentially with Acuity:** While ESI-1 (Resuscitation) patients are seen in a median of 6 minutes, low-acuity ESI-5 patients wait over 107 minutes, indicating triage is prioritizing correctly but low-acuity patients are severely backlogged.
2. **Peak Congestion Window:** The highest patient volumes arrive between 10:00 AM and 2:00 PM, and 6:00 PM and 10:00 PM. Average wait times closely mirror these two daily spikes.
3. **The Bottleneck Shifts by Acuity:** For urgent cases (ESI 2), the primary delay is *Provider to Disposition* (treatment time), but for non-urgent cases (ESI 4-5), the largest delay is simply *Triage to Provider* (the waiting room).
4. **Staffing Threshold:** A simulated staff-to-patient ratio analysis shows wait times begin to spike linearly once an ED experiences more than 1.5 new patient arrivals per provider, per hour. 
5. **Boarding Delays:** Nearly 23% of admitted patients experience total lengths of stay exceeding 8 hours, tying up critical bed space for incoming emergent arrivals.

## Dashboard Showcase
Interactive Tableau Dashboard built for ER Operations Managers:
- [Link to Interactive Tableau Dashboard on Tableau Public] *(Insert Link Here)*

*Screenshots from this project can be viewed below or inside the `dashboards/` directory.*

## How to Run This Project
If you would like to reproduce this analysis locally:

1. Clone the repository
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Generate the synthetic raw dataset:
   ```bash
   python scripts/generate_synthetic_er_data.py
   ```
4. Run the data cleaning pipeline:
   ```bash
   python scripts/inject_missing_values.py
   python scripts/clean_data.py
   ```
5. Run the feature engineering script:
   ```bash
   python scripts/feature_engineering.py
   ```
6. Run data validation and merge:
   ```bash
   python scripts/validate_data.py
   python scripts/merge_export.py
   ```
7. Generate EDA and Advanced Dashboards (outputs to `dashboards/`):
   ```bash
   python scripts/eda_analysis.py
   python scripts/advanced_analysis.py
   ```

## Repository Structure
- `data/` *(Ignored in Git)*: Houses raw and processed CSV data files
- `scripts/`: End-to-end Python pipeline (Generation, Cleaning, Feature Engineering, EDA)
- `docs/`: Executive summaries, cleaning documentation, and Tableau dashboard guide
- `dashboards/`: Exported high-resolution PNG analytical charts

---
*Created as a comprehensive healthcare analytics portfolio project.*
