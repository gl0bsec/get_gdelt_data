"""Country code loading and conversion utilities.

Reads FIPS 10-4 and CAMEO/ISO-3 lookup tables that ship with the package
(``gdelt_data/data/``) and provides helpers for mapping between the two
code systems used by GDELT.
"""

import os
from functools import lru_cache

_DATA_DIR = os.path.join(os.path.dirname(__file__), "data")

FIPS_FILE = os.path.join(_DATA_DIR, "FIPS.country.txt")
CAMEO_COUNTRY_FILE = os.path.join(_DATA_DIR, "CAMEO.country.txt")
CAMEO_EVENTCODES_FILE = os.path.join(_DATA_DIR, "CAMEO.eventcodes.txt")


# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------

@lru_cache(maxsize=1)
def load_fips_dict(filepath=None):
    """Load the FIPS code -> country name mapping.

    Parameters
    ----------
    filepath : str, optional
        Override path.  Defaults to the bundled ``FIPS.country.txt``.

    Returns
    -------
    dict[str, str]
        ``{"US": "United States", "GM": "Germany", ...}``
    """
    filepath = filepath or FIPS_FILE
    fips = {}
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and "\t" in line:
                parts = line.split("\t", 1)
                if len(parts) == 2:
                    fips[parts[0].strip()] = parts[1].strip()
    return fips


@lru_cache(maxsize=1)
def load_cameo_country_dict(filepath=None):
    """Load the CAMEO/ISO-3 code -> country name mapping.

    Parameters
    ----------
    filepath : str, optional
        Override path.  Defaults to the bundled ``CAMEO.country.txt``.

    Returns
    -------
    dict[str, str]
        ``{"USA": "United States", "DEU": "Germany", ...}``
    """
    filepath = filepath or CAMEO_COUNTRY_FILE
    cameo = {}
    with open(filepath, "r", encoding="utf-8") as f:
        next(f)  # skip header
        for line in f:
            line = line.strip()
            if line and "\t" in line:
                parts = line.split("\t", 1)
                if len(parts) == 2:
                    cameo[parts[0].strip()] = parts[1].strip()
    return cameo


@lru_cache(maxsize=1)
def load_cameo_eventcodes_dict(filepath=None):
    """Load the CAMEO event-code -> description mapping.

    Parameters
    ----------
    filepath : str, optional
        Override path.  Defaults to the bundled ``CAMEO.eventcodes.txt``.

    Returns
    -------
    dict[str, str]
        ``{"01": "MAKE PUBLIC STATEMENT", ...}``
    """
    filepath = filepath or CAMEO_EVENTCODES_FILE
    codes = {}
    with open(filepath, "r", encoding="utf-8") as f:
        next(f)  # skip header
        for line in f:
            line = line.strip()
            if line and "\t" in line:
                parts = line.split("\t", 1)
                if len(parts) == 2:
                    codes[parts[0].strip()] = parts[1].strip()
    return codes


# ---------------------------------------------------------------------------
# ISO-3 <-> FIPS mapping (built dynamically from the two txt files)
# ---------------------------------------------------------------------------

def _normalize_name(name):
    """Lowercase, strip punctuation for fuzzy matching."""
    return (
        name.lower()
        .replace("'", "")
        .replace(",", "")
        .replace("the ", "")
        .strip()
    )


# Name-based overrides for cases where the two files use different labels.
_CAMEO_TO_FIPS_OVERRIDES = {
    "RUS": "RS",   # Russia
    "UKR": "UP",   # Ukraine
    "KOR": "KS",   # South Korea
    "PRK": "KN",   # North Korea
    "ROM": "RO",   # Romania
    "TMP": "TT",   # East Timor / Timor-Leste
    "MMR": "BM",   # Myanmar / Burma
    "SRB": "RI",   # Serbia
    "MTN": "MJ",   # Montenegro
    "SSD": "OD",   # South Sudan
    "COD": "CG",   # DR Congo
    "COG": "CF",   # Congo (Brazzaville)
    "CIV": "IV",   # Ivory Coast / Cote d'Ivoire
    "MKD": "MK",   # North Macedonia
    "BRN": "BX",   # Brunei
    "LAO": "LA",   # Laos
    "PSE": "WE",   # Palestine / West Bank
    "VNM": "VM",   # Vietnam
    "LKA": "CE",   # Sri Lanka
}


@lru_cache(maxsize=1)
def build_iso3_to_fips_map():
    """Build a mapping from CAMEO/ISO-3 codes to FIPS codes.

    Uses country names to match entries across the two lookup tables, with
    manual overrides for known mismatches.

    Returns
    -------
    dict[str, str]
        ``{"USA": "US", "DEU": "GM", ...}``
    """
    cameo = load_cameo_country_dict()
    fips = load_fips_dict()

    # Invert FIPS: name -> code (lowercase key for matching)
    fips_by_name = {}
    for code, name in fips.items():
        fips_by_name[_normalize_name(name)] = code

    mapping = dict(_CAMEO_TO_FIPS_OVERRIDES)

    for iso3, cameo_name in cameo.items():
        if iso3 in mapping:
            continue
        norm = _normalize_name(cameo_name)
        if norm in fips_by_name:
            mapping[iso3] = fips_by_name[norm]

    return mapping


@lru_cache(maxsize=1)
def build_fips_to_iso3_map():
    """Inverse of :func:`build_iso3_to_fips_map`.

    Returns
    -------
    dict[str, str]
        ``{"US": "USA", "GM": "DEU", ...}``
    """
    return {v: k for k, v in build_iso3_to_fips_map().items()}


def iso3_to_fips(code):
    """Convert a single CAMEO/ISO-3 code to FIPS.

    Returns the code unchanged if no mapping is found.
    """
    if not code:
        return code
    return build_iso3_to_fips_map().get(code, code)


def fips_to_iso3(code):
    """Convert a single FIPS code to CAMEO/ISO-3.

    Returns the code unchanged if no mapping is found.
    """
    if not code:
        return code
    return build_fips_to_iso3_map().get(code, code)


def map_country_names(df, columns=None):
    """Add ``<column>Name`` columns with human-readable country names.

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame with GDELT country-code columns.
    columns : list[str], optional
        Columns to map.  Defaults to the three standard ones.

    Returns
    -------
    pandas.DataFrame
        The input DataFrame with new name columns added.
    """
    if columns is None:
        columns = [
            "ActionGeo_CountryCode",
            "Actor1CountryCode",
            "Actor2CountryCode",
        ]
    fips = load_fips_dict()
    cameo = load_cameo_country_dict()

    for col in columns:
        if col not in df.columns:
            continue
        name_col = col.replace("Code", "Name")
        if col == "ActionGeo_CountryCode":
            df[name_col] = df[col].map(fips)
        else:
            # Actor country codes are ISO-3 / CAMEO
            df[name_col] = df[col].map(cameo)
    return df
