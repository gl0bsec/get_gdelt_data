"""
African Countries Scatterplot Generator

Generates a scatterplot showing African countries positioned by their average 
Goldstein Score (x-axis) vs Average Tone (y-axis), with point size representing 
the number of events. Includes country labels for the top 20 countries by event count
and reference lines at x=0 and y=0.

Usage:
    python african_countries_scatterplot.py

The script will automatically:
1. Load GDELT data (prioritizing datasets with DATEADDED column)
2. Filter to last 6 months of data
3. Filter to African countries only
4. Generate PDF report with scatterplot
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from datetime import datetime, timedelta
import requests
import warnings
warnings.filterwarnings('ignore')

def load_country_names():
    """Load country code to name mapping from GDELT FIPS lookup."""
    try:
        response = requests.get('https://www.gdeltproject.org/data/lookups/FIPS.country.txt')
        response.raise_for_status()
        
        country_lookup = {}
        for line in response.text.strip().split('\n'):
            if '\t' in line:
                parts = line.strip().split('\t')
                if len(parts) >= 2:
                    code = parts[0].strip()
                    name = parts[1].strip()
                    country_lookup[code] = name
        
        print(f"Loaded {len(country_lookup)} country names")
        return country_lookup
    except Exception as e:
        print(f"Warning: Could not load country names: {e}")
        return {}

def load_gdelt_data():
    """Load GDELT data, prioritizing datasets with DATEADDED column."""
    try:
        df = pd.read_parquet('../test_dateadded.parquet')
        print("Using dataset with DATEADDED column")
        return df, True
    except FileNotFoundError:
        try:
            df = pd.read_parquet('../africa_russia_events.parquet')
            print("Using fallback dataset")
            return df, False
        except FileNotFoundError:
            raise FileNotFoundError("No GDELT data files found. Please ensure data files exist.")

def process_dates(df, has_dateadded):
    """Process date columns and filter to last 6 months."""
    # Convert SQLDATE to datetime
    df['Date'] = pd.to_datetime(df['SQLDATE'], format='%Y%m%d')
    
    # Process DATEADDED if available
    if has_dateadded and 'DATEADDED' in df.columns:
        sample_dateadded = str(df['DATEADDED'].iloc[0])
        if len(sample_dateadded) == 14:
            df['DateAdded'] = pd.to_datetime(df['DATEADDED'], format='%Y%m%d%H%M%S')
        else:
            df['DateAdded'] = pd.to_datetime(df['DATEADDED'], format='%Y%m%d')
        
        # Filter using DATEADDED
        latest_date = df['DateAdded'].max()
        six_months_ago = latest_date - timedelta(days=180)
        df = df[df['DateAdded'] >= six_months_ago].copy()
        date_source = "DATEADDED"
    else:
        # Filter using SQLDATE
        latest_date = df['Date'].max()
        six_months_ago = latest_date - timedelta(days=180)
        df = df[df['Date'] >= six_months_ago].copy()
        date_source = "SQLDATE"
    
    print(f"Filtered to last 6 months using {date_source}: {six_months_ago.date()} to {latest_date.date()}")
    return df

def filter_african_countries(df):
    """Filter dataset to include only African countries."""
    african_countries = [
        'DZ', 'AO', 'BJ', 'BW', 'BF', 'BI', 'CM', 'CV', 'CF', 'TD', 'KM', 'CG', 
        'CD', 'CI', 'DJ', 'EG', 'GQ', 'ER', 'ET', 'GA', 'GM', 'GH', 'GN', 'GW', 
        'KE', 'LS', 'LR', 'LY', 'MG', 'MW', 'ML', 'MR', 'MU', 'MA', 'MZ', 'NA', 
        'NE', 'NG', 'RW', 'ST', 'SN', 'SC', 'SL', 'SO', 'ZA', 'SS', 'SD', 'SZ', 
        'TZ', 'TG', 'TN', 'UG', 'ZM', 'ZW'
    ]
    
    africa_df = df[df['ActionGeo_CountryCode'].isin(african_countries)].copy()
    print(f"African countries events: {len(africa_df)}")
    return africa_df

def calculate_country_metrics(df):
    """Calculate aggregated metrics by country."""
    country_metrics = df.groupby('ActionGeo_CountryCode').agg({
        'AvgTone': 'mean',
        'GoldsteinScale': 'mean',
        'GLOBALEVENTID': 'count'
    }).reset_index()
    
    # Remove countries with null values and filter for reasonable sample sizes
    country_metrics = country_metrics.dropna()
    country_metrics = country_metrics[country_metrics['GLOBALEVENTID'] >= 10]
    
    print(f"Countries with ≥10 events: {len(country_metrics)}")
    return country_metrics

# Load country names and data
country_names = load_country_names()
df, has_dateadded = load_gdelt_data()
df = process_dates(df, has_dateadded)
africa_df = filter_african_countries(df)
country_metrics = calculate_country_metrics(africa_df)

print(f"Total events processed: {len(df)}")

def create_scatterplot(country_metrics, country_names, output_filename='african_countries_scatterplot.pdf'):
    """Generate the African countries scatterplot and save as PDF."""
    with PdfPages(output_filename) as pdf:
        # Create figure
        fig = plt.figure(figsize=(12, 10))
        
        # Create scatter plot with increased transparency
        scatter = plt.scatter(country_metrics['GoldsteinScale'], 
                             country_metrics['AvgTone'],
                             s=country_metrics['GLOBALEVENTID'] * 0.8,  # Size by event count
                             alpha=0.4,  # Increased transparency
                             c='#C73E1D',
                             edgecolors='white',
                             linewidth=1)
        
        # Add dotted black reference lines at x=0 and y=0
        plt.axhline(y=0, color='black', linewidth=1, alpha=0.8, linestyle=':', label='Neutral Tone')
        plt.axvline(x=0, color='black', linewidth=1, alpha=0.8, linestyle=':', label='Neutral Goldstein')
        
        # Formatting
        plt.xlabel('Average Goldstein Score', fontsize=14)
        plt.ylabel('Average Tone', fontsize=14)
        plt.title('African Countries: Tone vs Goldstein Score\n(Point size = Number of Events)', 
                  fontsize=16, fontweight='bold', pad=20)
        plt.grid(alpha=0.3)
        
        # Add annotations for top 20 countries by event count using full names
        top_countries = country_metrics.nlargest(20, 'GLOBALEVENTID')
        for _, row in top_countries.iterrows():
            country_code = row['ActionGeo_CountryCode']
            # Use full country name if available, otherwise use code
            country_label = country_names.get(country_code, country_code)
            
            plt.annotate(country_label, 
                        (row['GoldsteinScale'], row['AvgTone']),
                        xytext=(6, 6), textcoords='offset points',
                        fontsize=9, fontweight='bold', alpha=0.9,
                        bbox=dict(boxstyle='round,pad=0.3', facecolor='white', alpha=0.9, edgecolor='none'))
        
        # Adjust layout
        plt.tight_layout()
        
        # Save the page
        pdf.savefig(fig, bbox_inches='tight', dpi=300)
        plt.close()
    
    print(f"PDF generated successfully: {output_filename}")

# Generate the scatterplot
create_scatterplot(country_metrics, country_names)