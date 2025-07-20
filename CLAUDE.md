# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Python utility for collecting and filtering GDELT (Global Database of Events, Language, and Tone) event data. The project consists of two main components:

1. **Core data collection** (`gdelt_data/`): Downloads GDELT events for date ranges and applies configurable text-based filters
2. **Report generation** (`report_generation/`): Analysis tools that work with collected data, particularly for geographic and country-specific analysis

## Core Architecture

### Data Collection Flow
- `gdelt_data.collector.collect_gdelt_data()` is the main entry point
- Uses the `gdelt` Python library to fetch raw data in batches (default 7 days)
- `FilterRuleParser` converts human-readable filter rules into pandas operations
- Filters are defined in YAML/JSON with rules like "NumMentions greater than or equal 5"
- Output is saved as Parquet files for efficient storage and analysis

### Filter System
The filter system uses plain English expressions that get parsed into pandas operations:
- `"NumMentions greater than or equal 5"` → `df[df['NumMentions'] >= 5]`
- `"ActionGeo_CountryCode in [US, UK, FR]"` → `df[df['ActionGeo_CountryCode'].isin(['US', 'UK', 'FR'])]`
- `DEFAULT_FILTER_RULES` in collector.py defines sensible defaults for high-quality events

### Data Analysis and Parsing
- **URL Analysis**: `extract_url_metadata()` and `get_source_urls_with_metadata()` extract webpage metadata from GDELT source URLs
- **Event Code Mapping**: `parse_cameo_codes()` and `map_event_codes()` handle CAMEO event code translations
- **Data Processing**: `combine_multiple_columns()` and `analyze_source_metadata()` provide utilities for data transformation and analysis
- Uses multithreading for concurrent metadata extraction with configurable delays and timeouts

### Report Generation
- Uses CAMEO and FIPS country code lookup tables from GDELT
- `country_code_utils.py` provides utilities for working with these code systems
- Generates visualizations and analysis of collected event data

## Common Commands

### Installation
```bash
pip install -r requirements.txt
```

### CLI Usage
```bash
# Basic collection
python -m gdelt_data.cli 2024-01-01 2024-01-07 --output events.parquet

# With custom filters
python -m gdelt_data.cli 2024-01-01 2024-01-07 --filters my_filters.yaml --output events.parquet
```

### Python Library Usage
```python
from gdelt_data import collect_gdelt_data
from datetime import datetime

# Basic collection
collect_gdelt_data(
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 1, 7),
    output_file="events.parquet"
)

# Create filter template
from gdelt_data import save_filter_rules_template
save_filter_rules_template("my_filters.yaml")

# URL metadata extraction
from gdelt_data import get_source_urls_with_metadata
urls_with_metadata = get_source_urls_with_metadata(df, geo_code='US', extract_metadata=True)

# Event code mapping
from gdelt_data import parse_cameo_codes, map_event_codes
cameo_dict = parse_cameo_codes('report_generation/CAMEO.eventcodes.txt')
df_with_descriptions = map_event_codes(df, cameo_dict)
```

## Key Files

- `gdelt_data/collector.py`: Main data collection logic and filter system
- `gdelt_data/cli.py`: Command line interface
- `gdelt_data/parsing.py`: Additional parsing utilities  
- `report_generation/country_code_utils.py`: Country code mapping utilities
- `requirements.txt`: Dependencies (pandas, pyyaml, gdelt, requests, beautifulsoup4)

## Development Notes

- No test framework is currently configured
- Data is processed in batches with configurable sleep times to avoid overwhelming GDELT servers
- Memory is actively managed with `gc.collect()` calls for large datasets
- All filter rules support enabling/disabling via the "enabled" flag
- Report generation tools expect CAMEO and FIPS lookup files to be present in the report_generation directory