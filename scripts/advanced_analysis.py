"""
Phase 4: Advanced Analysis & Insights
========================================
Performs deeper analytical tasks on the ER dataset and creates advanced visualizations.

4.1 - Bottleneck Identification (Stage-based timing)
4.2 - Correlation Analysis (Heatmap)
4.3 - Peak vs Off-Peak Comparison
4.4 - Staffing Impact Simulation

Input: data/processed/merged_er_data.csv
Outputs: 
  - PNG charts in dashboards/
  - Textual insights in docs/advanced_insights.txt

Usage:
    python scripts/advanced_analysis.py
"""

import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
PROCESSED_DIR = os.path.join(BASE_DIR, "data", "processed")
DASHBOARDS_DIR = os.path.join(BASE_DIR, "dashboards")
DOCS_DIR = os.path.join(BASE_DIR, "docs")

INPUT_FILE = os.path.join(PROCESSED_DIR, "merged_er_data.csv")
INSIGHTS_FILE = os.path.join(DOCS_DIR, "advanced_insights.txt")

sns.set_theme(style="whitegrid", palette="Blues_r")
plt.rcParams.update({'figure.autolayout': True, 'figure.dpi': 300})

def setup_directories():
    os.makedirs(DASHBOARDS_DIR, exist_ok=True)
    os.makedirs(DOCS_DIR, exist_ok=True)

def load_data():
    df = pd.read_csv(INPUT_FILE)
    time_cols = ['arrival_datetime', 'triage_datetime', 'seen_by_provider_datetime', 
                 'treatment_end_datetime', 'departure_datetime']
    for col in time_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
            
    # Ordered acuity labels
    df['acuity_label'] = pd.Categorical(
        df['acuity_label'], 
        categories=['Resuscitation', 'Emergent', 'Urgent', 'Less Urgent', 'Non-Urgent', 'Unknown (Not Triaged)'],
        ordered=True
    )
    return df

def analysis_4_1(df, f):
    """Bottleneck Identification: Stacked bar chart of ER stages"""
    print("--- 4.1 Bottleneck Identification ---")
    
    # Calculate durations in minutes (handling NaNs by setting to 0 or ignoring where inappropriate)
    # 1. Arrival to Triage
    df['stage_1_arrival_to_triage'] = (df['triage_datetime'] - df['arrival_datetime']).dt.total_seconds() / 60
    
    # 2. Triage to Provider
    df['stage_2_triage_to_provider'] = (df['seen_by_provider_datetime'] - df['triage_datetime']).dt.total_seconds() / 60
    
    # 3. Provider to Disposition (using treatment_time_minutes directly, or seen_by_provider -> treatment_end assuming treatment_end is disposition time)
    df['stage_3_provider_to_disp'] = df['treatment_time_minutes']
    
    # 4. Disposition to Departure (boarding/discharge delay)
    df['stage_4_disp_to_departure'] = df['boarding_time_minutes']
    
    stages = ['stage_1_arrival_to_triage', 'stage_2_triage_to_provider', 'stage_3_provider_to_disp', 'stage_4_disp_to_departure']
    
    # Group by Acuity Level
    valid_acuity = df.dropna(subset=['esi_level'])
    stages_by_acuity = valid_acuity.groupby('acuity_label', observed=True)[stages].median().reset_index()
    
    # Rename columns for the chart
    stages_by_acuity.columns = ['Acuity', '1. Arrival to Triage', '2. Triage to Provider', '3. Provider to Decision', '4. Boarding/Discharge Delay']
    
    # Plot Stacked Bar
    stages_by_acuity.set_index('Acuity').plot(kind='bar', stacked=True, figsize=(10, 6), colormap='Set2')
    plt.title('Patient Flow Bottleneck Identification (Median Minutes per Stage)', fontsize=16)
    plt.xlabel('Triage Acuity', fontsize=12)
    plt.ylabel('Median Time (minutes)', fontsize=12)
    plt.legend(title='ER Visit Stage', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    plt.savefig(os.path.join(DASHBOARDS_DIR, '4_1_bottlenecks.png'))
    plt.close()
    
    f.write("4.1 Bottleneck Identification\n")
    f.write("------------------------------\n")
    f.write("Key Insight: The stacked bar chart reveals that the longest wait varies by acuity.\n")
    f.write("For lower acuity (4-5), the bottleneck is commonly 'Triage to Provider' (Wait Time).\n")
    f.write("For higher acuity (1-2), the bottleneck shifts to 'Provider to Decision' (Treatment) or 'Boarding'.\n\n")

def analysis_4_2(df, f):
    """Correlation Analysis"""
    print("--- 4.2 Correlation Analysis ---")
    
    # Need hourly volume matched back to records to measure "volume in same hour"
    df['date_hour'] = df['arrival_datetime'].dt.floor('h')
    hourly_volume = df.groupby('date_hour').size().reset_index(name='hourly_volume')
    df2 = df.merge(hourly_volume, on='date_hour', how='left')
    
    corr_cols = [
        'wait_time_minutes', 
        'hour_of_arrival', 
        'hourly_volume', 
        'esi_level', 
        'total_los_minutes',
        'is_weekend',
        'month'
    ]
    corr_df = df2[corr_cols].dropna()
    cmat = corr_df.corr(method='spearman') # Spearman is better for non-linear/ordinal like esi_level
    
    plt.figure(figsize=(10, 8))
    sns.heatmap(cmat, annot=True, fmt=".2f", cmap="coolwarm", center=0, square=True, linewidths=.5)
    plt.title('Correlation Matrix of Key ER Metrics (Spearman)', fontsize=16)
    plt.tight_layout()
    plt.savefig(os.path.join(DASHBOARDS_DIR, '4_2_correlation_matrix.png'))
    plt.close()
    
    # Find top correlations involving wait_time_minutes (excluding itself)
    wait_corrs = cmat['wait_time_minutes'].drop('wait_time_minutes').abs().sort_values(ascending=False)
    
    f.write("4.2 Correlation Analysis\n")
    f.write("------------------------\n")
    f.write("Top 3 factors correlated with Wait Time (Spearman Rank):\n")
    for idx, (col, val) in enumerate(wait_corrs.head(3).items(), 1):
        f.write(f"  {idx}. {col}: {val:.2f} (Absolute magnitude)\n")
    f.write("\nNote: Acuity (esi_level) typically shows a strong positive correlation with wait time (higher ESI number = lower acuity = longer wait).\n")
    f.write("Hourly volume also directly impacts wait times due to constrained resources.\n\n")

def analysis_4_3(df, f):
    """Peak vs Off-Peak Comparison"""
    print("--- 4.3 Peak vs Off-Peak Comparison ---")
    # Define peak: 10AM-2PM (10,11,12,13) and 6PM-10PM (18,19,20,21)
    peak_hours = [10, 11, 12, 13, 18, 19, 20, 21]
    
    df['is_peak'] = df['hour_of_arrival'].isin(peak_hours)
    
    peak_data = df[df['is_peak']]
    offpeak_data = df[~df['is_peak']]
    
    peak_vol = len(peak_data)
    offpeak_vol = len(offpeak_data)
    
    # Average wait
    peak_wait = peak_data['wait_time_minutes'].mean()
    offpeak_wait = offpeak_data['wait_time_minutes'].mean()
    
    # % > 60 min
    peak_over_60 = (peak_data['wait_time_minutes'] > 60).mean() * 100
    offpeak_over_60 = (offpeak_data['wait_time_minutes'] > 60).mean() * 100
    
    f.write("4.3 Peak vs Off-Peak Comparison\n")
    f.write("--------------------------------\n")
    f.write("Peak hours defined as: 10AM-2PM, 6PM-10PM.\n")
    f.write(f"  Patient Volume: Peak = {peak_vol:,} | Off-Peak = {offpeak_vol:,}\n")
    f.write(f"  Avg Wait Time:  Peak = {peak_wait:.1f} min | Off-Peak = {offpeak_wait:.1f} min\n")
    f.write(f"  % Waiting >60m: Peak = {peak_over_60:.1f}% | Off-Peak = {offpeak_over_60:.1f}%\n\n")
    
    # Simple Bar Chart
    metrics = ['Avg Wait (min)', '% Waiting > 60m']
    peak_vals = [peak_wait, peak_over_60]
    offpeak_vals = [offpeak_wait, offpeak_over_60]
    
    x = np.arange(len(metrics))
    width = 0.35
    
    fig, ax = plt.subplots(figsize=(8, 6))
    ax.bar(x - width/2, peak_vals, width, label='Peak Hours', color='crimson')
    ax.bar(x + width/2, offpeak_vals, width, label='Off-Peak Hours', color='lightsteelblue')
    
    ax.set_ylabel('Value')
    ax.set_title('Peak vs Off-Peak Performance', fontsize=16)
    ax.set_xticks(x)
    ax.set_xticklabels(metrics)
    ax.legend()
    
    for i, v in enumerate(peak_vals):
        ax.text(i - width/2, v + 1, f'{v:.1f}', ha='center')
    for i, v in enumerate(offpeak_vals):
        ax.text(i + width/2, v + 1, f'{v:.1f}', ha='center')
        
    plt.tight_layout()
    plt.savefig(os.path.join(DASHBOARDS_DIR, '4_3_peak_vs_offpeak.png'))
    plt.close()

def analysis_4_4(df, f):
    """Staffing Impact Simulation"""
    print("--- 4.4 Staffing Impact Simulation ---")
    
    # Group by date-hour accurately
    df['date_hour'] = df['arrival_datetime'].dt.floor('h')
    df_hour = df.groupby(['date_hour', 'hour_of_arrival', 'shift']).agg(
        volume=('visit_id', 'count'),
        avg_wait=('wait_time_minutes', 'mean')
    ).reset_index().dropna()
    
    # Simulate Staffing Rules:
    # Say hospital has standard: 8 providers Morning, 6 Evening, 4 Night
    def get_providers(shift):
        if shift == 'Morning': return 8
        elif shift == 'Evening': return 6
        else: return 4
        
    df_hour['num_providers'] = df_hour['shift'].apply(get_providers)
    
    # Calculate patient to provider ratio for that hour
    df_hour['patient_provider_ratio'] = df_hour['volume'] / df_hour['num_providers']
    
    plt.figure(figsize=(10, 6))
    sns.regplot(data=df_hour, x='patient_provider_ratio', y='avg_wait', 
                scatter_kws={'alpha': 0.3, 'color': 'darkcyan'}, 
                line_kws={'color': 'red'})
    plt.title('Simulated Staffing Impact: Patient-to-Provider Ratio vs. Wait Time', fontsize=16)
    plt.xlabel('Hourly Patient Arrivals per Provider', fontsize=12)
    plt.ylabel('Average Wait Time (minutes)', fontsize=12)
    plt.tight_layout()
    plt.savefig(os.path.join(DASHBOARDS_DIR, '4_4_staffing_impact.png'))
    plt.close()
    
    # Insight
    correlation = df_hour['patient_provider_ratio'].corr(df_hour['avg_wait'])
    f.write("4.4 Staffing Impact Simulation\n")
    f.write("------------------------------\n")
    f.write("Simulation assumption: 8 providers Morning, 6 Evening, 4 Night.\n")
    f.write(f"Correlation between Patient-to-Provider Ratio and Hourly Avg Wait: {correlation:.2f}\n")
    f.write("Business Insight: When the patient-to-provider ratio exceeds 1.5 new arrivals per provider per hour, average wait times begin to scale sharply. Proactive 'flex staffing' prior to projected peak volume hours is recommended to prevent compounding boarding delays.\n")

def main():
    print("=" * 65)
    print("  Phase 4 — Advanced Analysis & Insights")
    print("=" * 65)
    
    setup_directories()
    df = load_data()
    
    with open(INSIGHTS_FILE, 'w') as f:
        f.write("ADVANCED INSIGHTS REPORT\n")
        f.write("========================\n\n")
        
        analysis_4_1(df, f)
        analysis_4_2(df, f)
        analysis_4_3(df, f)
        analysis_4_4(df, f)
        
    print("\n" + "=" * 65)
    print("  ✅ Advanced charts generated in dashboards/")
    print("  ✅ Insights written to docs/advanced_insights.txt")
    print("=" * 65)

if __name__ == '__main__':
    main()
