# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Python utility for collecting, filtering, enriching, and exporting GDELT (Global Database of Events, Language, and Tone) event data. The project consists of:

1. **Core package** (`gdelt_data/`): Downloads GDELT events, applies filters, enriches with descriptions/country names, and exports to various formats
2. **Report generation** (`report_generation/`): Contains CAMEO and FIPS lookup tables (also bundled inside the package at `gdelt_data/data/`)
3. **Workflows** (`workflows/`): Project-specific collection and analysis scripts that compose the package API

## Core Architecture

### Package Modules

- **`gdelt_data/collector.py`**: Data collection engine. Downloads GDELT v1 events day-by-day in configurable batches, applies text-based filter rules, writes Parquet output.
- **`gdelt_data/parsing.py`**: URL metadata extraction (`extract_url_metadata`, `get_source_urls_with_metadata`), CAMEO event code parsing (`parse_cameo_codes`, `map_event_codes`), date conversion (`convert_dates_to_iso`), and data utilities.
- **`gdelt_data/country_codes.py`**: Loads FIPS and CAMEO/ISO-3 lookup tables from bundled txt files. Builds ISO-3 to FIPS mapping dynamically by matching country names across the two files. Provides `iso3_to_fips()`, `fips_to_iso3()`, `map_country_names()`, and dict loaders.
- **`gdelt_data/enrich.py`**: DataFrame enrichment — `add_event_descriptions()`, `add_country_names()`, `filter_by_country()`.
- **`gdelt_data/export.py`**: Geographic export — `to_kml()` writes KML with Goldstein-scored placemarks.
- **`gdelt_data/cli.py`**: Full CLI with subcommands (see below).
- **`gdelt_data/data/`**: Bundled lookup files (FIPS.country.txt, CAMEO.country.txt, CAMEO.eventcodes.txt). These are the authoritative copies; `report_generation/` retains the originals.

### Data Collection Flow
- `gdelt_data.collector.collect_gdelt_data()` is the main entry point
- Uses the `gdelt` Python library to fetch raw data in batches (default 7 days)
- `FilterRuleParser` converts human-readable filter rules into pandas operations
- Filters are defined in YAML/JSON with rules like "NumMentions greater than or equal 5"
- `DEFAULT_FILTER_RULES` in collector.py defines sensible defaults for high-quality events
- Output is saved as Parquet files for efficient storage and analysis

### Filter System
The filter system uses plain English expressions that get parsed into pandas operations:
- `"NumMentions greater than or equal 5"` → `df[df['NumMentions'] >= 5]`
- `"ActionGeo_CountryCode in [US, UK, FR]"` → `df[df['ActionGeo_CountryCode'].isin(['US', 'UK', 'FR'])]`

### Country Code System
- GDELT uses **two different code systems**: FIPS 10-4 for geography fields (`ActionGeo_CountryCode`) and CAMEO/ISO-3 for actor fields (`Actor1CountryCode`, `Actor2CountryCode`)
- `country_codes.py` builds the ISO-3 ↔ FIPS mapping dynamically from the bundled txt files, with manual overrides for known name mismatches (Russia, Ukraine, Congo, etc.)
- Lookup tables are loaded once and cached via `@lru_cache`

## Common Commands

### Installation
```bash
pip install -r requirements.txt
```

### CLI Usage
```bash
# Collect events for a date range
python -m gdelt_data collect 2024-01-01 2024-01-07 -o events.parquet

# Collect with custom filters
python -m gdelt_data collect 2024-01-01 2024-01-07 -f my_filters.yaml -o events.parquet

# Collect raw unfiltered data
python -m gdelt_data collect 2024-01-01 2024-01-07 --no-filter -o raw.parquet

# Filter to a country
python -m gdelt_data filter events.parquet ML -o mali.csv

# Enrich with descriptions and country names
python -m gdelt_data enrich mali.csv -o mali_enriched.csv

# Extract URL metadata
python -m gdelt_data extract-urls mali.csv -o mali_urls.csv --workers 8

# Export to KML
python -m gdelt_data kml mali_enriched.csv -o mali.kml --max-goldstein 0

# Generate filter template
python -m gdelt_data template -o my_filters.yaml

# Show defaults and help
python -m gdelt_data filters
python -m gdelt_data columns
python -m gdelt_data operators
python -m gdelt_data --help-all
```

### Python Library Usage
```python
from gdelt_data import collect_gdelt_data, filter_by_country
from gdelt_data import add_event_descriptions, add_country_names, to_kml
from gdelt_data import load_fips_dict, iso3_to_fips, map_country_names
from datetime import datetime
import pandas as pd

# Collect
collect_gdelt_data(
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 1, 7),
    output_file="events.parquet"
)

# Filter, enrich, export
df = pd.read_parquet("events.parquet")
mali = filter_by_country(df, "ML")
mali = add_event_descriptions(mali)
mali = add_country_names(mali)
to_kml(mali, "mali.kml", max_goldstein=0)

# Country code conversion
from gdelt_data import iso3_to_fips, fips_to_iso3
iso3_to_fips("DEU")  # -> "GM"
fips_to_iso3("GM")   # -> "DEU"

# Load lookup dicts directly
fips = load_fips_dict()  # {"US": "United States", ...}
```

## Key Files

- `gdelt_data/collector.py`: Data collection and filter system
- `gdelt_data/cli.py`: CLI with subcommands (collect, filter, enrich, extract-urls, kml, template, filters, columns, operators)
- `gdelt_data/parsing.py`: URL metadata extraction, CAMEO parsing, date conversion
- `gdelt_data/country_codes.py`: Country code loading and ISO-3 ↔ FIPS conversion
- `gdelt_data/enrich.py`: DataFrame enrichment (descriptions, country names, filtering)
- `gdelt_data/export.py`: KML export
- `gdelt_data/data/`: Bundled FIPS and CAMEO lookup files
- `requirements.txt`: Dependencies (pandas, pyyaml, gdelt, requests, beautifulsoup4)

## Project Structure and Conventions

### Directory Organization
- **`gdelt_data/`**: Core package — all reusable logic lives here
- **`gdelt_data/data/`**: Bundled lookup files (FIPS, CAMEO). These are loaded automatically by `country_codes.py`; no need to specify paths manually.
- **`workflows/`**: Project-specific scripts that compose the package API
  - Run with `PYTHONPATH=. python3 workflows/script_name.py`
  - These import from `gdelt_data` — they should not contain reusable logic
- **`outputs/`**: All generated output files (CSV, Parquet, KML, etc.)
  - Gitignored; auto-created by workflows
- **`report_generation/`**: Original CAMEO and FIPS lookup tables (also bundled in package)

### Important: Country Codes
- **GDELT uses FIPS 10-4 country codes, NOT ISO Alpha-2 codes**
- FIPS codes differ from ISO codes (e.g., GM = Germany in FIPS, not Gambia)
- Use `gdelt_data.country_codes` for all country code operations — it loads from bundled files automatically
- Actor fields use ISO-3 / CAMEO codes; geography fields use FIPS

### Adding Event and Country Descriptions
Use the package API instead of manual file loading:
```python
from gdelt_data import add_event_descriptions, add_country_names

df = add_event_descriptions(df)   # adds EventDescription column
df = add_country_names(df)        # adds ActionGeo_CountryName, Actor1CountryName, Actor2CountryName
```

## Development Notes

- No test framework is currently configured
- Data is processed in batches with configurable sleep times to avoid overwhelming GDELT servers
- Memory is actively managed with `gc.collect()` calls for large datasets
- All filter rules support enabling/disabling via the "enabled" flag
- Country code lookups are cached with `@lru_cache` — first call reads files, subsequent calls are free
- Run workflows with `PYTHONPATH=. python3 workflows/script_name.py` to ensure proper imports
