# GDELT Data Collector

A Python toolkit for fetching, filtering, enriching, and exporting [GDELT](https://www.gdeltproject.org/) event data.

## Features

- **Collect** events from the GDELT Events table for any date range
- **Filter** by country code, text rules (YAML/JSON), or custom criteria
- **Enrich** with CAMEO event descriptions and human-readable country names
- **Extract** article metadata (title, author, description) from source URLs
- **Export** to KML for Google Earth with Goldstein-scored placemarks
- **Convert** dates to ISO 8601 for visualization tools (Foursquare Studio, etc.)
- Bundled FIPS and CAMEO lookup tables with ISO-3 to FIPS conversion
- Full CLI with subcommands or use as a Python library

## Installation

```bash
pip install -r requirements.txt
```

## CLI Usage

```bash
# Collect events for a date range
python -m gdelt_data collect 2024-01-01 2024-01-07 -o events.parquet

# Collect with custom filter rules
python -m gdelt_data collect 2024-01-01 2024-01-07 -f my_filters.yaml -o events.parquet

# Collect raw unfiltered data
python -m gdelt_data collect 2024-01-01 2024-01-07 --no-filter -o raw.parquet

# Filter to a specific country (FIPS code)
python -m gdelt_data filter events.parquet ML -o mali.csv

# Enrich with event descriptions and country names
python -m gdelt_data enrich mali.csv -o mali_enriched.csv

# Extract metadata from source URLs
python -m gdelt_data extract-urls mali.csv -o mali_urls.csv --workers 8

# Export to KML (negative events only)
python -m gdelt_data kml mali_enriched.csv -o mali.kml --max-goldstein 0

# Generate a filter rules template
python -m gdelt_data template -o my_filters.yaml

# Inspect defaults
python -m gdelt_data filters      # show default filter rules
python -m gdelt_data columns      # show default output columns
python -m gdelt_data operators    # show all filter operators

# Full documentation
python -m gdelt_data --help-all
```

## Python Library Usage

### Collect, filter, enrich, export

```python
from datetime import datetime
import pandas as pd
from gdelt_data import (
    collect_gdelt_data,
    filter_by_country,
    add_event_descriptions,
    add_country_names,
    to_kml,
)

# Collect
collect_gdelt_data(
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 1, 7),
    output_file="events.parquet",
)

# Filter to Mali, enrich, and export
df = pd.read_parquet("events.parquet")
mali = filter_by_country(df, "ML")
mali = add_event_descriptions(mali)
mali = add_country_names(mali)
mali.to_csv("mali_events.csv", index=False)

# Export to KML
to_kml(mali, "mali.kml", max_goldstein=0)
```

### Country code utilities

GDELT uses FIPS 10-4 codes for geography fields and CAMEO/ISO-3 codes for actor fields. The package handles conversion automatically:

```python
from gdelt_data import iso3_to_fips, fips_to_iso3, load_fips_dict

iso3_to_fips("DEU")  # "GM" (Germany)
fips_to_iso3("GM")   # "DEU"

# Load the full lookup dictionary
fips = load_fips_dict()  # {"US": "United States", "GM": "Germany", ...}
```

### Filter rules

Filter rules use plain English syntax:

```
NumMentions greater than or equal 5
ActionGeo_CountryCode in [US, UK, FR]
GoldsteinScale between -5 and 5
Actor1Name contains protest
ActionGeo_Lat is not null _
```

Create a template and customize it:

```bash
python -m gdelt_data template -o my_filters.yaml
```

Or build filters interactively in Python:

```python
from gdelt_data import interactive_filter_builder
filters = interactive_filter_builder()
```

### URL metadata extraction

```python
from gdelt_data import get_source_urls_with_metadata

urls_df = get_source_urls_with_metadata(
    df, extract_metadata=True, dataF=True
)
```

### Date conversion

```python
from gdelt_data import convert_dates_to_iso

df = convert_dates_to_iso(df)
# SQLDATE: 20250803 -> 2025-08-03
```

## Country Codes

GDELT uses FIPS 10-4 country codes, **not** ISO Alpha-2. Some common differences:

| Country | FIPS | ISO |
|---|---|---|
| Germany | GM | DE |
| Australia | AS | AU |
| Switzerland | SZ | CH |
| Austria | AU | AT |
| South Korea | KS | KR |
| Japan | JA | JP |

The full FIPS lookup table is bundled with the package and available via `load_fips_dict()`.

## Project Structure

```
gdelt_data/              # Core package
  collector.py           # Data collection and filter engine
  cli.py                 # CLI with subcommands
  parsing.py             # URL extraction, CAMEO parsing, dates
  country_codes.py       # FIPS/CAMEO loaders, ISO-3 <-> FIPS
  enrich.py              # Event descriptions, country names, filtering
  export.py              # KML export
  data/                  # Bundled lookup files
    FIPS.country.txt
    CAMEO.country.txt
    CAMEO.eventcodes.txt
workflows/               # Project-specific collection scripts
outputs/                 # Generated data files (gitignored)
report_generation/       # Original lookup tables
```
