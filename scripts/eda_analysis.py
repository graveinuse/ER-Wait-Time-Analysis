"""
Phase 3: Exploratory Data Analysis (EDA)
========================================
Generates 7 key analyses and charts to answer specific business questions
about ER wait times and patient flow.

Input: data/processed/merged_er_data.csv
Output: PNG charts in dashboards/ folder

Usage:
    python scripts/eda_analysis.py
"""

import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
PROCESSED_DIR = os.path.join(BASE_DIR, "data", "processed")
DASHBOARDS_DIR = os.path.join(BASE_DIR, "dashboards")

INPUT_FILE = os.path.join(PROCESSED_DIR, "merged_er_data.csv")

# Set visualization styling
sns.set_theme(style="whitegrid", palette="Blues_r")
plt.rcParams.update({'figure.autolayout': True, 'figure.dpi': 300})

def setup_directories():
    os.makedirs(DASHBOARDS_DIR, exist_ok=True)

def load_data():
    df = pd.read_csv(INPUT_FILE)
    df['arrival_datetime'] = pd.to_datetime(df['arrival_datetime'])
    # Ordered categorical for day of week to ensure correct chart sorting
    days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    df['day_of_week'] = pd.Categorical(df['day_of_week'], categories=days, ordered=True)
    return df

def analysis_3_1(df):
    """Overall Wait Time Distribution"""
    print("--- 3.1 Overall Wait Time Distribution ---")
    valid_waits = df['wait_time_minutes'].dropna()
    mean = valid_waits.mean()
    median = valid_waits.median()
    p75 = valid_waits.quantile(0.75)
    p90 = valid_waits.quantile(0.90)
    max_wait = valid_waits.max()
    
    print(f"  Mean: {mean:.1f} min")
    print(f"  Median: {median:.1f} min")
    print(f"  75th Percentile: {p75:.1f} min")
    print(f"  90th Percentile: {p90:.1f} min")
    print(f"  Max: {max_wait:.1f} min")
    
    plt.figure(figsize=(10, 6))
    sns.histplot(valid_waits, bins=50, kde=True, color='royalblue')
    plt.axvline(median, color='tomato', linestyle='--', label=f'Median: {median:.0f} min')
    plt.axvline(p90, color='darkred', linestyle=':', label=f'90th Pct: {p90:.0f} min')
    plt.title('Overall Wait Time Distribution', fontsize=16)
    plt.xlabel('Wait Time (minutes)', fontsize=12)
    plt.ylabel('Number of Patients', fontsize=12)
    plt.legend()
    plt.savefig(os.path.join(DASHBOARDS_DIR, '3_1_wait_time_distribution.png'))
    plt.close()

def analysis_3_2(df):
    """Wait Time by Hour of Day"""
    print("\n--- 3.2 Wait Time by Hour of Day ---")
    hourly = df.groupby('hour_of_arrival').agg(
        avg_wait=('wait_time_minutes', 'mean'),
        volume=('visit_id', 'count')
    ).reset_index()
    
    fig, ax1 = plt.subplots(figsize=(12, 6))
    
    # Bar chart for patient volume
    color1 = 'lightsteelblue'
    ax1.bar(hourly['hour_of_arrival'], hourly['volume'], color=color1, alpha=0.7, label='Patient Volume')
    ax1.set_xlabel('Hour of Day (0-23)', fontsize=12)
    ax1.set_ylabel('Patient Volume', color='slategrey', fontsize=12)
    ax1.tick_params(axis='y', labelcolor='slategrey')
    ax1.set_xticks(range(24))
    
    # Line chart for average wait time
    ax2 = ax1.twinx()
    color2 = 'crimson'
    ax2.plot(hourly['hour_of_arrival'], hourly['avg_wait'], color=color2, marker='o', linewidth=2, label='Avg Wait Time')
    ax2.set_ylabel('Average Wait Time (minutes)', color=color2, fontsize=12)
    ax2.tick_params(axis='y', labelcolor=color2)
    
    plt.title('Wait Time vs. Patient Volume by Hour of Day', fontsize=16)
    fig.tight_layout()
    plt.savefig(os.path.join(DASHBOARDS_DIR, '3_2_wait_by_hour.png'))
    plt.close()

def analysis_3_3(df):
    """Wait Time by Day of Week"""
    print("\n--- 3.3 Wait Time by Day of Week ---")
    daily = df.groupby('day_of_week', observed=False).agg(
        avg_wait=('wait_time_minutes', 'mean'),
        volume=('visit_id', 'count')
    ).reset_index()
    
    fig, ax1 = plt.subplots(figsize=(10, 6))
    sns.barplot(data=daily, x='day_of_week', y='avg_wait', color='royalblue', alpha=0.8, ax=ax1)
    
    ax1.set_title('Average Wait Time by Day of Week', fontsize=16)
    ax1.set_xlabel('Day of Week', fontsize=12)
    ax1.set_ylabel('Average Wait Time (minutes)', fontsize=12)
    
    # Annotate bars
    for i, v in enumerate(daily['avg_wait']):
        ax1.text(i, v + 1, f'{v:.0f}m', ha='center', fontweight='bold')
        
    plt.savefig(os.path.join(DASHBOARDS_DIR, '3_3_wait_by_day.png'))
    plt.close()

def analysis_3_4(df):
    """Wait Time by Triage Acuity"""
    print("\n--- 3.4 Wait Time by Triage Acuity ---")
    acuity = df.dropna(subset=['esi_level']).groupby('esi_level').agg(
        avg_wait=('wait_time_minutes', 'mean'),
        median_wait=('wait_time_minutes', 'median')
    ).reset_index()
    
    esi_labels = {1: '1-Resuscitation', 2: '2-Emergent', 3: '3-Urgent', 4: '4-Less Urgent', 5: '5-Non-Urgent'}
    acuity['Acuity Label'] = acuity['esi_level'].map(esi_labels)
    
    plt.figure(figsize=(10, 6))
    ax = sns.barplot(data=acuity, x='Acuity Label', y='median_wait', palette='viridis')
    plt.title('Median Wait Time by Triage Acuity Level', fontsize=16)
    plt.xlabel('Triage Acuity', fontsize=12)
    plt.ylabel('Median Wait Time (minutes)', fontsize=12)
    
    for i, v in enumerate(acuity['median_wait']):
        plt.text(i, v + 1, f'{v:.0f}m', ha='center', fontweight='bold')
        
    plt.savefig(os.path.join(DASHBOARDS_DIR, '3_4_wait_by_acuity.png'))
    plt.close()

def analysis_3_5(df):
    """Seasonal / Monthly Trends"""
    print("\n--- 3.5 Seasonal / Monthly Trends ---")
    df['year_month'] = df['arrival_datetime'].dt.to_period('M')
    monthly = df.groupby('year_month').agg(
        avg_wait=('wait_time_minutes', 'mean'),
        volume=('visit_id', 'count')
    ).reset_index()
    monthly['year_month_str'] = monthly['year_month'].astype(str)
    
    fig, ax1 = plt.subplots(figsize=(14, 6))
    
    color1 = 'lightsteelblue'
    ax1.bar(monthly['year_month_str'], monthly['volume'], color=color1, alpha=0.7, label='Patient Volume')
    ax1.set_xlabel('Month', fontsize=12)
    ax1.set_ylabel('Patient Volume', color='slategrey', fontsize=12)
    ax1.tick_params(axis='y', labelcolor='slategrey')
    ax1.tick_params(axis='x', rotation=45)
    
    ax2 = ax1.twinx()
    color2 = 'darkorange'
    ax2.plot(monthly['year_month_str'], monthly['avg_wait'], color=color2, marker='o', linewidth=2, label='Avg Wait Time')
    ax2.set_ylabel('Average Wait Time (minutes)', color=color2, fontsize=12)
    ax2.tick_params(axis='y', labelcolor=color2)
    
    plt.title('Monthly Trends: Wait Time and Patient Volume', fontsize=16)
    fig.tight_layout()
    plt.savefig(os.path.join(DASHBOARDS_DIR, '3_5_seasonal_trends.png'))
    plt.close()

def analysis_3_6(df):
    """Patient Volume Heatmap"""
    print("\n--- 3.6 Patient Volume Heatmap ---")
    heatmap_data = df.pivot_table(
        index='day_of_week', 
        columns='hour_of_arrival', 
        values='visit_id', 
        aggfunc='count',
        observed=False,
        fill_value=0
    )
    
    plt.figure(figsize=(12, 6))
    sns.heatmap(heatmap_data, cmap='YlOrRd', linewidths=.5, annot=False)
    plt.title('ER Patient Volume Heatmap (Day of Week vs. Hour of Day)', fontsize=16)
    plt.xlabel('Hour of Day (0-23)', fontsize=12)
    plt.ylabel('Day of Week', fontsize=12)
    plt.yticks(rotation=0)
    plt.tight_layout()
    plt.savefig(os.path.join(DASHBOARDS_DIR, '3_6_volume_heatmap.png'))
    plt.close()

def analysis_3_7(df):
    """Length of Stay Analysis"""
    print("\n--- 3.7 Length of Stay Analysis ---")
    valid_los = df.dropna(subset=['total_los_minutes', 'esi_level']).copy()
    
    esi_labels = {1: '1-Resus', 2: '2-Emergent', 3: '3-Urgent', 4: '4-Less Urgent', 5: '5-Non-Urgent'}
    valid_los['Acuity Label'] = valid_los['esi_level'].map(esi_labels)
    valid_los = valid_los.sort_values('esi_level')
    
    plt.figure(figsize=(12, 6))
    sns.boxplot(data=valid_los, x='Acuity Label', y='total_los_minutes', palette='Set2')
    plt.axhline(y=480, color='red', linestyle='--', linewidth=2, label='8 Hours (Boarding Threshold)')
    
    plt.title('Total Length of Stay Distribution by Acuity Level', fontsize=16)
    plt.xlabel('Triage Acuity', fontsize=12)
    plt.ylabel('Total LOS (minutes)', fontsize=12)
    plt.legend()
    plt.savefig(os.path.join(DASHBOARDS_DIR, '3_7_los_by_acuity.png'))
    plt.close()

    boarders = df[df['total_los_minutes'] > 480]
    board_pct = len(boarders) / len(valid_los) * 100
    print(f"  Patients staying 8+ hours (Boarders): {len(boarders):,} ({board_pct:.1f}% of valid records)")

def main():
    print("=" * 65)
    print("  Phase 3 — Exploratory Data Analysis")
    print("=" * 65)
    
    setup_directories()
    df = load_data()
    print(f"Loaded dataset: {len(df):,} records\n")
    
    analysis_3_1(df)
    analysis_3_2(df)
    analysis_3_3(df)
    analysis_3_4(df)
    analysis_3_5(df)
    analysis_3_6(df)
    analysis_3_7(df)
    
    print("\n" + "=" * 65)
    print("  ✅ All charts generated and saved to dashboards/ directory.")
    print("=" * 65)

if __name__ == '__main__':
    main()
