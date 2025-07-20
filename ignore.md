# ignore.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Python library for fetching, filtering, and saving GDELT (Global Database of Events, Language, and Tone) event data. The project provides both a CLI interface and library functions for downloading and processing GDELT data with customizable filtering rules.
## Architecture

The codebase consists of three main modules:

- `gdelt_data/__init__.py` - Public API exports for library functions
- `gdelt_data/cli.py` - Command-line interface using argparse
- `gdelt_data/collector.py` - Core functionality including data collection, filtering, and processing

### Key Components

**FilterRuleParser**: Parses natural language filter rules (e.g., "NumMentions greater than 5") into pandas operations. Supports comparison operators, text matching, null checks, and range queries.

**collect_gdelt_data()**: Main data collection function that:
- Fetches GDELT events data using the `gdelt` Python library
- Applies user-defined filters using the FilterRuleParser
- Processes data in batches to handle large date ranges efficiently  
- Optimizes data types and saves results as Parquet files
- Includes built-in retry logic and memory management

**Interactive Filter Builder**: Terminal-based UI for creating filter rules interactively, with options to save/load configurations from YAML/JSON files.

## Common Commands

### Installation
```bash
pip install -r requirements.txt
```

### CLI Usage
```bash
# Basic usage
python -m gdelt_data.cli 2024-01-01 2024-01-07 --output events.parquet

# With custom filters
python -m gdelt_data.cli 2024-01-01 2024-01-07 --filters my_filters.yaml --output events.parquet
```

### Library Usage
```python
from gdelt_data import collect_gdelt_data
from datetime import datetime

collect_gdelt_data(
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 1, 7),
    output_file="events.parquet"
)
```

### Generate Filter Template
```python
from gdelt_data import save_filter_rules_template
save_filter_rules_template('my_filters.yaml')
```

## Dependencies

- `pandas` - Data manipulation and analysis
- `pyyaml` - YAML file processing for filter configurations
- `gdelt` - Official GDELT Python client for data access

## Data Collection Workflows

### Large Date Range Collections
For collections spanning months or years:
- Use smaller batch sizes (3-7 days) to manage memory
- Increase sleep_time (1.0+ seconds) to respect API limits
- Consider splitting very large ranges into separate runs
- Monitor intermediate parquet file sizes

### Geographic and Actor-Based Filtering
For complex filtering involving multiple conditions or OR logic:

1. **Collect broader dataset first** with minimal filters
2. **Post-process using pandas** for complex logic
3. **Clean up intermediate files** to save disk space

Example workflow for "Events in Africa with Russian actors":
```python
# Step 1: Collect all Africa events
collect_gdelt_data(
    start_date=start_date,
    end_date=end_date,
    filter_rules={"africa_only": {"rule": "ActionGeo_CountryCode in [DZ, AO, ...]", "enabled": True}},
    output_file='temp_africa.parquet'
)

# Step 2: Post-process for Russian involvement
df = pd.read_parquet('temp_africa.parquet')
russia_mask = (df['Actor1CountryCode'] == 'RUS') | (df['Actor2CountryCode'] == 'RUS')
final_df = df[russia_mask]
final_df.to_parquet('africa_russia_events.parquet')
os.remove('temp_africa.parquet')
```

## Filter System Limitations and Workarounds

### Known Issues
- Filter rules use AND logic only - no native OR support
- Column name case sensitivity issues (use uppercase: `ACTIONGEO_COUNTRYCODE`)
- Some operators may not parse correctly (`is not null` vs `is not null`)

### Workarounds
1. **OR Logic**: Use post-processing with pandas boolean operations
2. **Case Issues**: Check GDELT column names and use exact case
3. **Complex Filters**: Implement in post-processing rather than filter rules

### Common Geographic Filters

**African Countries**:
```
ActionGeo_CountryCode in [DZ, AO, BJ, BW, BF, BI, CM, CV, CF, TD, KM, CG, CD, CI, DJ, EG, GQ, ER, ET, GA, GM, GH, GN, GW, KE, LS, LR, LY, MG, MW, ML, MR, MU, MA, MZ, NA, NE, NG, RW, ST, SN, SC, SL, SO, ZA, SS, SD, SZ, TZ, TG, TN, UG, ZM, ZW]
```

**Major Powers**:
- Russia: `RUS`
- United States: `USA` or `US`
- China: `CHN` or `CH`
- European Union countries: `[DE, FR, UK, IT, ES, NL, BE, AT, ...]`

### Performance Tips
- Start with recent, smaller date ranges (last 30 days) for testing
- Use `batch_size=5` and `sleep_time=1.0` for large collections
- GDELT 1.0 data is available with 1-day lag (avoid current date)
- Monitor memory usage during collection - parquet files can grow large quickly

## Development Notes

- No formal test suite or build system is currently configured
- The project uses simple pip-based dependency management via requirements.txt
- Code follows standard Python conventions with comprehensive docstrings
- Memory optimization is handled through dtype downcasting and garbage collection during batch processing
- The filtering system is designed to be extensible with natural language rule definitions

## GDELT Event Data Schema

The GDELT Event Database uses a structured schema with 21 core columns, following the CAMEO (Conflict and Mediation Event Observations) event taxonomy. This section provides detailed documentation of each column to assist with data analysis and filtering.

### Core Identifier Fields

**GLOBALEVENTID** (Integer)
- Unique identifier for each event record
- Primary key for the GDELT Events database
- Sequential numbering system used across all GDELT datasets

**SQLDATE** (Integer, Format: YYYYMMDD)
- Date when the event occurred in YYYYMMDD format
- Example: 20240401 represents April 1, 2024
- Used for temporal filtering and analysis

### Actor Classification Fields

**Actor1Name** / **Actor2Name** (String)
- Human-readable names of the primary actors involved in the event
- Can be individuals, organizations, countries, or other entities
- May be null if actor cannot be identified from source text

**Actor1CountryCode** / **Actor2CountryCode** (String, 3 characters)
- CAMEO country codes for the actors involved
- Uses 3-letter CAMEO country codes (e.g., "USA", "RUS", "CHN", "DEU", "SDN")
- Essential for geographic and geopolitical analysis
- Reference: https://www.gdeltproject.org/data/lookups/CAMEO.country.txt

**Actor1Type1Code** (String, 3 characters)
- CAMEO actor type classification code
- One of 40 possible CAMEO type codes describing the actor's role/type
- Examples: "GOV" (Government), "MIL" (Military), "JUD" (Judiciary)
- Used to categorize actors by institutional type

### Event Classification Fields

**EventCode** (String, 3 digits)
- Primary CAMEO event classification code
- One of 310 possible event types in the CAMEO taxonomy
- 4-tier hierarchical system for event categorization
- Examples: "010" (Make statement), "173" (Coerce), "190" (Use conventional military force)

**EventRootCode** (String, 2 digits)
- Root-level CAMEO event classification (top of hierarchy)
- One of 20 root event categories
- Examples: "01" (Make statement), "17" (Coerce), "19" (Fight)

**EventBaseCode** (String, 3 digits)
- Base-level CAMEO event classification
- Intermediate level in the 4-tier hierarchy
- Provides more specific categorization than root codes

### Impact and Sentiment Metrics

**GoldsteinScale** (Float, Range: -10.0 to +10.0)
- Theoretical impact score for the event type on country stability
- Negative values indicate conflictual/destabilizing events
- Positive values indicate cooperative/stabilizing events
- Based on Goldstein's conflict-cooperation scale for international events
- Does not consider specific context, only event type

**NumMentions** (Integer)
- Number of times this event was mentioned across all source documents
- Indicator of event prominence and media attention
- Higher values suggest more significant or widely reported events

**NumSources** (Integer)
- Number of distinct source documents that reported this event
- Measure of source diversity and corroboration
- Higher values indicate broader media coverage

**NumArticles** (Integer)
- Total number of articles that mentioned this event
- Similar to NumSources but may include multiple articles from same source
- Useful for measuring event coverage intensity

**AvgTone** (Float, Range: -100 to +100)
- Average sentiment/tone of all documents mentioning this event
- Negative values indicate negative tone/sentiment
- Positive values indicate positive tone/sentiment
- Based on automated sentiment analysis of source text

### Geographic Fields

**ActionGeo_Lat** / **ActionGeo_Long** (Float)
- Latitude and longitude coordinates where the event occurred
- Decimal degrees format
- May be null if location cannot be determined

**ActionGeo_CountryCode** (String, 2 characters)
- FIPS country code for the event location
- Uses 2-letter FIPS country codes (e.g., "US", "RS", "UP", "CA", "CH")
- Essential for geographic filtering and spatial analysis
- Reference: https://www.gdeltproject.org/data/lookups/FIPS.country.txt

**ActionGeo_ADM1Code** (String, 2-4 characters)
- Administrative division level 1 code (state/province)
- Format: 2-letter FIPS country code + 2-digit admin code
- Examples: "USCA" for California, "RS48" for Moscow region, "UP13" for Kyiv Oblast
- May sometimes contain only the country code (e.g., "UP", "RB")

**ActionGeo_FullName** (String)
- Complete human-readable location name
- Format: "City, State/Province, Country"
- Example: "Moscow, Moskva, Russia"

### Source Information

**SOURCEURL** (String)
- URL of the original news article or source document
- Used for source verification and additional context
- May be truncated in some datasets

## Data Analysis Guidelines

### Working with Event Codes
- Use EventRootCode (2 digits) for broad event categorization
- Use EventCode (3 digits) for specific event analysis
- Reference CAMEO codebooks for detailed event type definitions
- Common root codes: 01-05 (Verbal cooperation), 06-08 (Material cooperation), 09-13 (Verbal conflict), 14-18 (Material conflict), 19-20 (Fight)

### Geographic Analysis
- Use ActionGeo_CountryCode for country-level filtering
- Combine with Actor country codes for actor-location analysis
- ActionGeo_FullName provides context for location interpretation
- Coordinate fields enable spatial analysis and mapping

### Sentiment and Impact Analysis
- GoldsteinScale provides theoretical impact assessment
- AvgTone reflects actual media sentiment
- NumMentions/NumSources indicate event significance
- Combine metrics for comprehensive event importance scoring

### Data Quality Considerations
- Some fields may be null/empty when information cannot be extracted
- Geographic coordinates may be approximate city/region centroids
- Actor identification depends on source text quality
- Event classification is automated and may have occasional errors

## Sample Data Analysis Patterns

### High-Impact Events
```python
# Events with significant negative impact and high media attention
high_impact = df[
    (df['GoldsteinScale'] < -5.0) & 
    (df['NumMentions'] > 10) & 
    (df['NumSources'] > 3)
]
```

### Conflict vs Cooperation Events
```python
# Categorize events by conflict/cooperation
conflict_events = df[df['GoldsteinScale'] < 0]
cooperation_events = df[df['GoldsteinScale'] > 0]
```

### Geographic Event Distribution
```python
# Events by country with location data
located_events = df[df['ActionGeo_CountryCode'].notna()]
country_counts = located_events['ActionGeo_CountryCode'].value_counts()
```

## Country Code Reference System

GDELT uses two different country code systems depending on the field:

### CAMEO Country Codes (3-letter)
Used in **Actor fields**:
- `Actor1CountryCode` 
- `Actor2CountryCode`

**Format**: 3-letter codes (e.g., "USA", "RUS", "CHN", "DEU", "SDN")
**Reference**: https://www.gdeltproject.org/data/lookups/CAMEO.country.txt

Common CAMEO codes:
- USA: United States
- RUS: Russia  
- CHN: China
- DEU: Germany
- GBR: United Kingdom
- FRA: France
- EUR: Europe (regional designation)
- UKR: Ukraine
- CAN: Canada
- ISR: Israel

### FIPS Country Codes (2-letter)
Used in **Geographic fields**:
- `ActionGeo_CountryCode`
- `ActionGeo_ADM1Code` (first 2 characters)

**Format**: 2-letter codes (e.g., "US", "RS", "UP", "CA", "CH")
**Reference**: https://www.gdeltproject.org/data/lookups/FIPS.country.txt

Common FIPS codes:
- US: United States
- RS: Russia
- UP: Ukraine  
- CA: Canada
- CH: China
- AS: Australia
- GM: Germany
- FR: France
- IS: Israel
- BK: Bosnia and Herzegovina

### Code Conversion Considerations

When working with GDELT data, be aware that:

1. **Actor country codes** use 3-letter CAMEO format
2. **Geographic country codes** use 2-letter FIPS format
3. **No direct 1:1 mapping** exists between systems
4. **Regional codes** (e.g., "EUR" for Europe) exist in CAMEO but not FIPS
5. **Historical codes** may reference former countries (e.g., Soviet Union)

### Practical Usage Examples

```python
# Filter events by Russian actors (CAMEO code)
russian_actors = df[
    (df['Actor1CountryCode'] == 'RUS') | 
    (df['Actor2CountryCode'] == 'RUS')
]

# Filter events occurring in Russia (FIPS code)
events_in_russia = df[df['ActionGeo_CountryCode'] == 'RS']

# Combined filter: Russian actors involved in events in Ukraine
russian_actors_ukraine_events = df[
    ((df['Actor1CountryCode'] == 'RUS') | (df['Actor2CountryCode'] == 'RUS')) &
    (df['ActionGeo_CountryCode'] == 'UP')
]
```

### Working with Country Code Lookups

```python
# Load country code mappings for reference
import pandas as pd
import requests

# Load CAMEO country codes
cameo_url = "https://www.gdeltproject.org/data/lookups/CAMEO.country.txt"
cameo_codes = pd.read_csv(cameo_url, sep='\t', header=None, names=['Code', 'Country'])

# Load FIPS country codes  
fips_url = "https://www.gdeltproject.org/data/lookups/FIPS.country.txt"
fips_codes = pd.read_csv(fips_url, sep='\t', header=None, names=['Code', 'Country'])

# Example: Get country name for actor code
actor_country = cameo_codes[cameo_codes['Code'] == 'RUS']['Country'].iloc[0]
print(f"RUS = {actor_country}")  # Output: RUS = Russia

# Example: Get country name for geographic code
geo_country = fips_codes[fips_codes['Code'] == 'RS']['Country'].iloc[0]  
print(f"RS = {geo_country}")  # Output: RS = Russia
```