import pandas as pd
from datetime import datetime, timedelta

# Load the data
df = pd.read_parquet('africa_russia_events.parquet')

# Convert SQLDATE to datetime
df['Date'] = pd.to_datetime(df['SQLDATE'], format='%Y%m%d')

# Check date range
print("Date range in the data:")
print(f"Earliest date: {df['Date'].min()}")
print(f"Latest date: {df['Date'].max()}")
print(f"Total date span: {(df['Date'].max() - df['Date'].min()).days} days")

# Calculate 6 months ago from the latest date
latest_date = df['Date'].max()
six_months_ago = latest_date - timedelta(days=180)  # Approximately 6 months

print(f"\nFor last 6 months filter:")
print(f"Latest date: {latest_date}")
print(f"6 months ago: {six_months_ago}")

# Check how much data we'll have in the last 6 months
recent_data = df[df['Date'] >= six_months_ago]
print(f"\nData in last 6 months: {len(recent_data):,} events ({len(recent_data)/len(df)*100:.1f}% of total)")
print(f"Date range for recent data: {recent_data['Date'].min()} to {recent_data['Date'].max()}")