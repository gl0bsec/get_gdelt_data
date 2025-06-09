# GDELT Data Collector

A small utility for fetching, filtering and saving [GDELT](https://www.gdeltproject.org/) event data.

## Features

- Download the Events table for a given date range
- Filter events using simple text rules loaded from YAML or JSON
- Save the resulting records as a Parquet file
- Access via a minimal CLI or use the Python functions directly

## Installation

```bash
pip install -r requirements.txt
```

## Quick start (CLI)

```bash
python -m gdelt_data.cli 2024-01-01 2024-01-07 \
  --filters my_filters.yaml \
  --output events.parquet
```

Create a filters file with `save_filter_rules_template()` or craft your own to control which events are kept.

## Library example

```python
from datetime import datetime
from gdelt_data import collect_gdelt_data

collect_gdelt_data(
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 1, 7),
    output_file="events.parquet",
)
```

## Building filters interactively

```python
from gdelt_data import interactive_filter_builder
filters = interactive_filter_builder()
```

The returned dictionary can be passed to `collect_gdelt_data` or written to a YAML/JSON file for reuse.

