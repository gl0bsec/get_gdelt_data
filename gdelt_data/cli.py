import argparse
from datetime import datetime

from .collector import collect_gdelt_data


def main() -> None:
    parser = argparse.ArgumentParser(description="Download GDELT data")
    parser.add_argument("start_date", help="Start date YYYY-MM-DD")
    parser.add_argument("end_date", help="End date YYYY-MM-DD")
    parser.add_argument("--filters", help="Path to YAML/JSON file with filter rules")
    parser.add_argument("--output", default="gdelt_events.parquet", help="Output parquet file")
    args = parser.parse_args()

    start = datetime.fromisoformat(args.start_date)
    end = datetime.fromisoformat(args.end_date)

    collect_gdelt_data(
        start_date=start,
        end_date=end,
        filter_rules_file=args.filters,
        output_file=args.output,
    )


if __name__ == "__main__":
    main()
