from datetime import datetime, timedelta
from gdelt_data import collect_gdelt_data
from gdelt_data.parsing import (
    get_source_urls_with_metadata,
    parse_cameo_codes,
    map_event_codes
)

# Define date range - collect data from the past week
end_date = datetime.now()
start_date = end_date - timedelta(days=7)

# Collect GDELT data with default filters
# Uses batch_size parameter (not batch_days) and sleep_time for delays
output_file = "example_events.parquet"
df = collect_gdelt_data(
    start_date=start_date,
    end_date=end_date,
    output_file=output_file,
    batch_size=3,  # Process 3 days at a time
    sleep_time=1.0  # 1 second delay between batches
)

# Parse CAMEO event codes
cameo_dict = parse_cameo_codes('report_generation/CAMEO.eventcodes.txt')
df = map_event_codes(df, cameo_dict)

# Extract source URL metadata for US events (limit to 10 for demonstration)
us_events = df[df['ActionGeo_CountryCode'] == 'US'].head(10)

# Get URLs with metadata extraction
urls_with_metadata = get_source_urls_with_metadata(
    us_events, 
    geo_code='US',
    extract_metadata=True,
    max_workers=2,  # Conservative threading
    delay=1.0,      # 1 second delay between requests
    timeout=10      # 10 second timeout per request
)

# Show sample URL metadata
print("Sample source metadata:")
for i, row in urls_with_metadata.head(3).iterrows():
    print(f"  {row.get('domain', 'Unknown')}: {row['title'][:80]}...")

# Summary statistics
print(f"\nSummary:")
print(f"- Total events: {len(df)}")
print(f"- Unique event types: {df['EventRootCode'].nunique()}")
print(f"- Average mentions per event: {df['NumMentions'].mean():.1f}")
print(f"- Data saved to: {output_file}")

# Show how to create custom filter templates
# from gdelt_data import save_filter_rules_template
# save_filter_rules_template('my_filters.yaml')