"""Utilities for fetching, filtering, enriching, and exporting GDELT data."""

from .collector import (
    collect_gdelt_data,
    save_filter_rules_template,
    load_filter_rules_from_file,
    interactive_filter_builder,
    DEFAULT_FILTER_RULES,
)

from .parsing import (
    extract_url_metadata,
    get_source_urls_with_metadata,
    analyze_source_metadata,
    combine_multiple_columns,
    map_event_codes,
    parse_cameo_codes,
    convert_dates_to_iso,
)

from .country_codes import (
    load_fips_dict,
    load_cameo_country_dict,
    load_cameo_eventcodes_dict,
    build_iso3_to_fips_map,
    build_fips_to_iso3_map,
    iso3_to_fips,
    fips_to_iso3,
    map_country_names,
)

from .enrich import (
    add_event_descriptions,
    add_country_names,
    filter_by_country,
)

from .export import to_kml

__all__ = [
    # collector
    "collect_gdelt_data",
    "save_filter_rules_template",
    "load_filter_rules_from_file",
    "interactive_filter_builder",
    "DEFAULT_FILTER_RULES",
    # parsing
    "extract_url_metadata",
    "get_source_urls_with_metadata",
    "analyze_source_metadata",
    "combine_multiple_columns",
    "map_event_codes",
    "parse_cameo_codes",
    "convert_dates_to_iso",
    # country_codes
    "load_fips_dict",
    "load_cameo_country_dict",
    "load_cameo_eventcodes_dict",
    "build_iso3_to_fips_map",
    "build_fips_to_iso3_map",
    "iso3_to_fips",
    "fips_to_iso3",
    "map_country_names",
    # enrich
    "add_event_descriptions",
    "add_country_names",
    "filter_by_country",
    # export
    "to_kml",
]
