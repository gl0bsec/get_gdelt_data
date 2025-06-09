# GDELT Data Collector

This project provides utilities for downloading and filtering data from the [GDELT](https://www.gdeltproject.org/) project.  
The code exposes a `collect_gdelt_data` function and a small command line interface.

## Installation

Install the required dependencies using `pip`:

```bash
pip install -r requirements.txt
```

## Command line usage

Run the CLI to download data between two dates (inclusive):

```bash
python -m gdelt_data.cli 2024-01-01 2024-01-07 --output events.parquet
```

A YAML or JSON file containing filter rules can be supplied via the `--filters` option.

## Library usage

You can also call `collect_gdelt_data` directly from Python:

```python
from datetime import datetime
from gdelt_data.collector import collect_gdelt_data

collect_gdelt_data(
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 1, 7),
)
```
