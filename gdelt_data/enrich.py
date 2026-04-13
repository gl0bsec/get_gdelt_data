"""Enrichment and filtering utilities for GDELT DataFrames."""

import pandas as pd

from .country_codes import (
    load_cameo_eventcodes_dict,
    load_fips_dict,
    load_cameo_country_dict,
    map_country_names,
)


# ---------------------------------------------------------------------------
# Event descriptions
# ---------------------------------------------------------------------------

def add_event_descriptions(df, cameo_file=None, verbose=False):
    """Add an ``EventDescription`` column from CAMEO event codes.

    Parameters
    ----------
    df : pandas.DataFrame
        Must contain an ``EventCode`` column.
    cameo_file : str, optional
        Path to CAMEO event-codes file.  Uses the bundled file by default.
    verbose : bool
        Print mapping statistics.

    Returns
    -------
    pandas.DataFrame
        The input DataFrame with ``EventDescription`` added.
    """
    codes = load_cameo_eventcodes_dict(cameo_file)

    df = df.copy()
    df["EventDescription"] = (
        df["EventCode"]
        .astype(str)
        .str.strip()
        .map(codes)
    )

    if verbose:
        mapped = df["EventDescription"].notna().sum()
        total = len(df)
        print(f"Mapped {mapped:,}/{total:,} events "
              f"({mapped / total * 100:.1f}%)")
        unmapped = df.loc[df["EventDescription"].isna(), "EventCode"]
        if not unmapped.empty:
            print("Top unmapped codes:")
            for code, count in unmapped.value_counts().head(5).items():
                print(f"  {code}: {count}")

    return df


# ---------------------------------------------------------------------------
# Country names
# ---------------------------------------------------------------------------

def add_country_names(df, columns=None):
    """Add human-readable country-name columns.

    Delegates to :func:`gdelt_data.country_codes.map_country_names`.
    """
    return map_country_names(df, columns=columns)


# ---------------------------------------------------------------------------
# Filtering
# ---------------------------------------------------------------------------

def filter_by_country(df, country_code, column="ActionGeo_CountryCode",
                      convert_dates=True):
    """Filter a GDELT DataFrame to a single country.

    Parameters
    ----------
    df : pandas.DataFrame
        GDELT events.
    country_code : str
        Two-letter FIPS code (e.g. ``"ML"`` for Mali).
    column : str
        Column to filter on.  Defaults to ``ActionGeo_CountryCode``.
    convert_dates : bool
        Convert ``SQLDATE`` / ``DATEADDED`` to ISO-8601 strings.

    Returns
    -------
    pandas.DataFrame
        Filtered copy of the input.
    """
    out = df.loc[df[column] == country_code].copy()

    if convert_dates:
        out = _convert_gdelt_dates(out)

    return out


def _convert_gdelt_dates(df):
    """Convert GDELT integer dates to ISO-8601 strings (in-place)."""
    if "SQLDATE" in df.columns:
        df["SQLDATE"] = pd.to_datetime(
            df["SQLDATE"].astype(str), format="%Y%m%d", errors="coerce"
        ).dt.strftime("%Y-%m-%d")

    if "DATEADDED" in df.columns:
        date_str = df["DATEADDED"].astype(str)
        if date_str.str.len().max() > 8:
            df["DATEADDED"] = pd.to_datetime(
                date_str, format="%Y%m%d%H%M%S", errors="coerce"
            ).dt.strftime("%Y-%m-%d %H:%M:%S")
        else:
            df["DATEADDED"] = pd.to_datetime(
                date_str, format="%Y%m%d", errors="coerce"
            ).dt.strftime("%Y-%m-%d")

    return df
