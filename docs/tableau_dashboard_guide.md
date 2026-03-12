# Tableau Dashboard Build Guide: Hospital ER Wait Time Analysis

This guide provides step-by-step instructions for building the interactive Tableau dashboard that serves as the centerpiece of your portfolio.

## Data Preparation for Tableau
Before starting:
1. Open Tableau Desktop (or Tableau Public).
2. Connect to Data $\rightarrow$ Text file $\rightarrow$ Select `data/processed/merged_er_data.csv`.
3. Ensure Tableau correctly parses:
   * `arrival_datetime`, `seen_by_provider_datetime`, `departure_datetime` as **Date & Time**.
   * `wait_time_minutes`, `total_los_minutes` as **Numbers (Measures)**.
   * `esi_level` as a **Dimension** (convert to discrete if needed).

---

## Dashboard Pages Overview

You will create 3 dashboards in Tableau and link them together using navigation buttons or a storyboard.

### Global Design Best Practices (Apply to all pages)
* **Color Palette:** Stick to 2-3 primary colors (e.g., Tableau's default Blue/Orange, or a custom Teal/Grey palette).
* **Font:** Use Tableau Book or a clean sans-serif font like Arial/Segoe UI.
* **Layout:** Use layout containers (horizontal and vertical) with plenty of padding (white space).
* **Tooltips:** Edit tooltips to read as sentences (e.g., `"In <month>, there were <visit_id (count)> visits with an average wait of <wait_time_minutes (avg)> minutes."`).
* **Interactivity:** Make all filters apply to "All Using This Data Source".

---

## Dashboard Page 1: Executive Summary
**Goal:** Provide top-level KPIs and high-level trends for hospital executives.

### Step 1: KPI Cards (Top Row)
Create 5 separate sheets for KPIs. For each, drag the metric to the `Text` mark. Format the text large and bold.
1. **Total ER Visits:** `COUNT(visit_id)`
2. **Average Wait Time:** `AVG(wait_time_minutes)`
3. **Median Wait Time:** `MEDIAN(wait_time_minutes)`
4. **% Waiting > 60 Min:** Create a calculated field `[wait_time_minutes] > 60`, then find the `% of Total` of `COUNT(visit_id)` where this is True.
5. **Average Length of Stay:** `AVG(total_los_minutes)`

### Step 2: Monthly Trend Line
* **Columns:** `arrival_datetime` (Continuous Month/Year)
* **Rows:** `AVG(wait_time_minutes)` (Dual Axis) and `COUNT(visit_id)`
* **Marks:** 
  * Average Wait = Line Chart (Color: Orange)
  * Patient Volume = Bar Chart (Color: Light Blue)
* **Action:** Synchronize axes (if scales are similar) or keep dual. Add clear labels.

### Step 3: Day of Week Bars
* **Columns:** `day_of_week` (Sort Monday $\rightarrow$ Sunday)
* **Rows:** `AVG(wait_time_minutes)`
* **Marks:** Bar chart. Add a subtle color gradient based on total volume.

### Step 4: Page 1 Layout & Filters
* Assemble sheets in a vertical dashboard layout.
* Add global filters to the right or top: **Date Range** (Slider), **Acuity Level** (Dropdown), **Shift** (Dropdown).
* Add a text box highlighting the key finding (e.g., *"Mondays show the highest wait times, correlating with peak weekend backlog."*)

---

## Dashboard Page 2: Hourly Operations View
**Goal:** Detailed hour-by-hour analysis for shift and resource planning.

### Step 1: Patient Volume Heatmap
* **Columns:** `hour_of_arrival` (Discrete, 0-23)
* **Rows:** `day_of_week`
* **Marks:** Square. Drag `COUNT(visit_id)` to Color.
* **Color Palette:** Orange-Blue Diverging or Red-Black transparent.

### Step 2: Wait Time by Hour Line Chart
* **Columns:** `hour_of_arrival` (Continuous)
* **Rows:** `AVG(wait_time_minutes)`
* **Marks:** Line Chart. Combine with a shading band representing the 25th to 75th percentile wait time using a continuous Reference Band on the Y-Axis.

### Step 3: Peak vs Off-Peak Comparison
* **Calculated Field:** `IF [hour_of_arrival] >= 10 AND [hour_of_arrival] <= 14 OR [hour_of_arrival] >= 18 AND [hour_of_arrival] <= 22 THEN "Peak" ELSE "Off-Peak" END`
* Create a side-by-side grouped bar chart comparing `AVG(wait_time_minutes)` for Peak vs Off-Peak.

### Step 4: Page 2 Layout
* Organize heatmaps and line charts to share the same X-axis visual alignment if possible.
* Add text: *"Peak congestion occurs daily between 10A-2P and 6P-10P. Staffing overlaps should be scheduled here."*

---

## Dashboard Page 3: Acuity & Patient Flow
**Goal:** Clinical perspective proving patient prioritization is effective.

### Step 1: Wait Time by Acuity
* **Columns:** `acuity_label`
* **Rows:** `MEDIAN(wait_time_minutes)`
* **Marks:** Bar Chart. Color by Acuity Level. Show labels on top of the bars to explicitly prove ESI-1 patients wait the least.

### Step 2: Patient Flow Breakdown (Bottleneck Analysis)
* Create 4 calculated fields for the Medians of: `Stage 1 (Arrival->Triage)`, `Stage 2 (Triage->Provider)`, `Stage 3 (Treatment)`, `Stage 4 (Boarding)`.
* Make a 100% Stacked Bar Chart or standard Stacked Bar Chart broken down by `acuity_label`.
* **Color:** Distinct colors for each stage to visualize where the bulk of time is spent (e.g., boarding delays for ESI 2 vs triage delays for ESI 4).

### Step 3: Length of Stay Distribution (Box Plots)
* **Columns:** `acuity_label`
* **Rows:** `total_los_minutes` (Disaggregate measures: Analysis menu -> Uncheck Aggregate Measures).
* **Marks:** Box and Whisker Plot. Add a horizontal reference line at 480 minutes (8 Hours) labeled "Boarding Threshold".

### Step 4: Boarding Rate KPI
* **Calculated Field:** `IF [total_los_minutes] > 480 THEN 1 ELSE 0 END`
* Represent as a circular gauge or large BAN (Big Ass Number) showing the %.

---
## Final Touches
1. **Tooltips:** Strip out raw dimension names and format cleanly.
2. **Dashboard Actions:** Go to `Dashboard > Actions`. Add a Filter Action so that clicking a bar in the "Wait Time by Day" chart filters the entire dashboard to just that day.
3. **Publishing:** Export the finished dashboard to Tableau Public and embed the link in your GitHub `README.md`.
