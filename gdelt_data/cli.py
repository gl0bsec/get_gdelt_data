import argparse
import textwrap
from datetime import datetime

from .collector import (
    collect_gdelt_data,
    save_filter_rules_template,
    interactive_filter_builder,
    DEFAULT_FILTER_RULES,
)


EXTENDED_HELP = textwrap.dedent("""\
    ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
      GDELT Data Collector — Detailed Help
    ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    DESCRIPTION
      Downloads event records from the GDELT (Global Database of Events,
      Language, and Tone) project. Supports date-range batching, text-based
      filtering, and output to Parquet format.

    BASIC USAGE
      python -m gdelt_data 2024-01-01 2024-01-07
      python -m gdelt_data 2024-01-01 2024-01-07 --output events.parquet
      python -m gdelt_data 2024-01-01 2024-01-07 --filters my_rules.yaml

    SUBCOMMANDS
      collect (default)   Download and filter GDELT events for a date range.
      template            Generate a sample filter-rules YAML file.
      filters             Show the current default filter rules.
      columns             List the default columns retained in the output.
      operators           Show all supported filter operators with examples.

    ─── COLLECT ────────────────────────────────────────────────────
      The default action. Downloads GDELT v1 events day-by-day, applies
      filter rules, and writes results to a Parquet file.

      Positional arguments:
        start_date          Start of the date range (YYYY-MM-DD).
        end_date            End of the date range (YYYY-MM-DD).

      Options:
        --output, -o FILE   Output Parquet file (default: gdelt_events.parquet).
        --filters, -f FILE  Path to a YAML or JSON file with filter rules.
                            If omitted, sensible defaults are applied.
        --batch-size N      Days to accumulate before flushing to disk (default: 7).
        --sleep SECONDS     Delay between API requests (default: 0.5).
        --no-filter         Disable all filtering; collect raw events.

    ─── TEMPLATE ───────────────────────────────────────────────────
      Writes an example filter-rules YAML file with annotated examples.

      python -m gdelt_data template
      python -m gdelt_data template --output my_filters.yaml

    ─── FILTERS ────────────────────────────────────────────────────
      Prints the built-in default filter rules and their status.

      python -m gdelt_data filters

    ─── COLUMNS ────────────────────────────────────────────────────
      Lists every GDELT column that is retained by default.

      python -m gdelt_data columns

    ─── OPERATORS ──────────────────────────────────────────────────
      Shows every filter operator supported by the rule parser, with
      an example for each.

      python -m gdelt_data operators

    ─── FILTER RULE SYNTAX ─────────────────────────────────────────
      Rules are plain-English expressions of the form:

        <Column> <Operator> <Value>

      Supported operators:
        greater than              NumMentions greater than 10
        greater than or equal     NumSources greater than or equal 3
        less than                 GoldsteinScale less than -5
        less than or equal        AvgTone less than or equal 0
        equals                    EventRootCode equals 14
        not equals                Actor1CountryCode not equals US
        contains                  Actor1Name contains protest
        not contains              Actor1Name not contains military
        in                        ActionGeo_CountryCode in [US, UK, FR]
        not in                    EventRootCode not in [20, 21, 22, 23]
        is null                   Actor2Name is null _
        is not null               ActionGeo_Lat is not null _
        between                   GoldsteinScale between -5 and 5

      Note: 'is null' and 'is not null' require a trailing placeholder
      value (e.g. '_') to satisfy the parser's column-operator-value
      pattern, but the value itself is ignored.

    ─── FILTER FILE FORMAT ─────────────────────────────────────────
      Filters are defined in YAML or JSON. Each rule has a name, rule
      text, optional description, and an enabled flag:

        filter_rules:
          high_mentions:
            rule: "NumMentions greater than or equal 5"
            description: "Keep events with 5+ mentions"
            enabled: true
          country_filter:
            rule: "ActionGeo_CountryCode in [US, UK, FR]"
            description: "Limit to selected countries"
            enabled: false

      Run 'python -m gdelt_data template' to generate a full example.

    ─── COUNTRY CODES ──────────────────────────────────────────────
      GDELT uses FIPS 10-4 country codes, NOT ISO Alpha-2. Common
      differences:

        Country          FIPS    ISO
        ─────────────    ────    ───
        Germany          GM      DE
        Australia        AS      AU
        Switzerland      SZ      CH
        Austria          AU      AT
        South Korea      KS      KR
        Japan            JA      JP

      The full FIPS lookup table is at:
        report_generation/FIPS.country.txt

    ─── OUTPUT FORMAT ──────────────────────────────────────────────
      Data is saved as Apache Parquet with Snappy compression. Numeric
      columns are downcast to minimize file size. To read the output:

        import pandas as pd
        df = pd.read_parquet("gdelt_events.parquet")

    ─── EXAMPLES ───────────────────────────────────────────────────
      # Collect one week with default filters
      python -m gdelt_data 2024-06-01 2024-06-07

      # Collect a month, custom output path
      python -m gdelt_data 2024-01-01 2024-01-31 -o jan_events.parquet

      # Use custom filter rules
      python -m gdelt_data 2024-03-01 2024-03-15 -f my_filters.yaml

      # Collect raw unfiltered data
      python -m gdelt_data 2024-03-01 2024-03-07 --no-filter

      # Slower requests, bigger batches
      python -m gdelt_data 2024-01-01 2024-06-30 --batch-size 14 --sleep 2

      # Generate a filter template and customize it
      python -m gdelt_data template -o my_filters.yaml

      # Show default filters
      python -m gdelt_data filters

    ─── PYTHON LIBRARY USAGE ───────────────────────────────────────
      from gdelt_data import collect_gdelt_data
      from datetime import datetime

      collect_gdelt_data(
          start_date=datetime(2024, 1, 1),
          end_date=datetime(2024, 1, 7),
          output_file="events.parquet",
      )

    ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
""")

DEFAULT_COLUMNS = [
    'GLOBALEVENTID', 'SQLDATE', 'DATEADDED', 'MonthYear', 'Year',
    'FractionDate', 'Actor1Name', 'Actor2Name', 'Actor1CountryCode',
    'Actor2CountryCode', 'Actor1Type1Code', 'EventCode', 'EventRootCode',
    'EventBaseCode', 'GoldsteinScale', 'NumMentions', 'NumSources',
    'NumArticles', 'AvgTone', 'ActionGeo_Lat', 'ActionGeo_Long',
    'ActionGeo_CountryCode', 'ActionGeo_ADM1Code', 'ActionGeo_FullName',
    'SOURCEURL',
]

FILTER_OPERATORS = {
    'greater than':          ('NumMentions greater than 10',
                              'Keep events mentioned more than 10 times'),
    'greater than or equal': ('NumSources greater than or equal 3',
                              'Keep events with 3 or more sources'),
    'less than':             ('GoldsteinScale less than -5',
                              'Keep highly conflictual events'),
    'less than or equal':    ('AvgTone less than or equal 0',
                              'Keep negative-tone events'),
    'equals':                ('EventRootCode equals 14',
                              'Keep only protest events (CAMEO root 14)'),
    'not equals':            ('Actor1CountryCode not equals US',
                              'Exclude US actors'),
    'contains':              ('Actor1Name contains protest',
                              'Actor name includes "protest"'),
    'not contains':          ('Actor1Name not contains military',
                              'Actor name excludes "military"'),
    'in':                    ('ActionGeo_CountryCode in [US, UK, FR]',
                              'Keep events in US, UK, or France'),
    'not in':                ('EventRootCode not in [20, 21, 22, 23]',
                              'Exclude unconventional mass violence events'),
    'is null':               ('Actor2Name is null _',
                              'Keep events with no second actor'),
    'is not null':           ('ActionGeo_Lat is not null _',
                              'Keep events with geo-coordinates'),
    'between':               ('GoldsteinScale between -5 and 5',
                              'Keep moderate-impact events'),
}


def _cmd_collect(args):
    """Run the collect subcommand."""
    start = datetime.fromisoformat(args.start_date)
    end = datetime.fromisoformat(args.end_date)

    kwargs = dict(
        start_date=start,
        end_date=end,
        output_file=args.output,
        batch_size=args.batch_size,
        sleep_time=args.sleep,
    )

    if args.no_filter:
        # Pass an empty dict so no rules are applied
        kwargs['filter_rules'] = {}
    elif args.filters:
        kwargs['filter_rules_file'] = args.filters

    collect_gdelt_data(**kwargs)


def _cmd_template(args):
    """Run the template subcommand."""
    save_filter_rules_template(args.output)


def _cmd_filters(_args):
    """Run the filters subcommand."""
    print("Default filter rules:\n")
    for name, rule in DEFAULT_FILTER_RULES.items():
        status = "ENABLED" if rule.get('enabled', True) else "DISABLED"
        print(f"  [{status:>8}]  {name}")
        print(f"             Rule:  {rule['rule']}")
        if rule.get('description'):
            print(f"             Desc:  {rule['description']}")
        print()


def _cmd_columns(_args):
    """Run the columns subcommand."""
    print("Default columns retained in output:\n")
    for col in DEFAULT_COLUMNS:
        print(f"  - {col}")
    print(f"\nTotal: {len(DEFAULT_COLUMNS)} columns")


def _cmd_operators(_args):
    """Run the operators subcommand."""
    print("Supported filter operators:\n")
    for op, (example, description) in FILTER_OPERATORS.items():
        print(f"  {op}")
        print(f"    Example:  {example}")
        print(f"    Purpose:  {description}")
        print()


def main() -> None:
    """Entry point for the command line interface."""

    parser = argparse.ArgumentParser(
        prog='gdelt_data',
        description='Download and filter GDELT event data.',
        epilog=(
            "Run 'python -m gdelt_data --help-all' for detailed documentation "
            "including filter syntax, country codes, and examples."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        '--help-all',
        action='store_true',
        default=False,
        help='Show the extended help page with filter syntax, examples, and more.',
    )

    subparsers = parser.add_subparsers(dest='command')

    # ── collect (default) ──────────────────────────────────────
    collect_parser = subparsers.add_parser(
        'collect',
        help='Download GDELT events for a date range (default action).',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    collect_parser.add_argument('start_date', help='Start date (YYYY-MM-DD).')
    collect_parser.add_argument('end_date', help='End date (YYYY-MM-DD).')
    collect_parser.add_argument(
        '--filters', '-f',
        help='Path to a YAML/JSON file with filter rules.',
    )
    collect_parser.add_argument(
        '--output', '-o',
        default='gdelt_events.parquet',
        help='Output Parquet file (default: gdelt_events.parquet).',
    )
    collect_parser.add_argument(
        '--batch-size',
        type=int,
        default=7,
        help='Days per batch before flushing to disk (default: 7).',
    )
    collect_parser.add_argument(
        '--sleep',
        type=float,
        default=0.5,
        help='Seconds between API requests (default: 0.5).',
    )
    collect_parser.add_argument(
        '--no-filter',
        action='store_true',
        default=False,
        help='Disable all filtering; collect raw events.',
    )

    # ── template ───────────────────────────────────────────────
    template_parser = subparsers.add_parser(
        'template',
        help='Generate a sample filter-rules YAML file.',
    )
    template_parser.add_argument(
        '--output', '-o',
        default='filter_rules_template.yaml',
        help='Output file path (default: filter_rules_template.yaml).',
    )

    # ── filters ────────────────────────────────────────────────
    subparsers.add_parser(
        'filters',
        help='Show the built-in default filter rules.',
    )

    # ── columns ────────────────────────────────────────────────
    subparsers.add_parser(
        'columns',
        help='List the default columns retained in the output.',
    )

    # ── operators ──────────────────────────────────────────────
    subparsers.add_parser(
        'operators',
        help='Show all supported filter operators with examples.',
    )

    args = parser.parse_args()

    # --help-all prints the extended manual
    if args.help_all:
        print(EXTENDED_HELP)
        return

    # No subcommand and no positional args → show standard help
    if args.command is None:
        parser.print_help()
        return

    # Dispatch subcommands
    dispatch = {
        'collect': _cmd_collect,
        'template': _cmd_template,
        'filters': _cmd_filters,
        'columns': _cmd_columns,
        'operators': _cmd_operators,
    }
    dispatch[args.command](args)


if __name__ == "__main__":
    main()
