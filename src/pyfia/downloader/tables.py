"""
FIA table definitions for download operations.

This module defines the tables available from the FIA DataMart and which
tables are required for various analysis workflows.

References
----------
- FIA DataMart: https://apps.fs.usda.gov/fia/datamart/datamart.html
- rFIA common tables: https://doserlab.com/files/rfia/reference/getfia
"""

from __future__ import annotations

from pyfia.constants.fiadb_schema import COLUMN_TYPES

# Common tables required for pyFIA estimation functions
# These match the rFIA "common=TRUE" default tables, plus pyfia-specific
# additions that the estimators need.
COMMON_TABLES: list[str] = [
    "COND",  # Condition data
    "COND_DWM_CALC",  # Down woody material calculations
    "COUNTY",  # County names and survey units (pyfia.reference.counties)
    "INVASIVE_SUBPLOT_SPP",  # Invasive species subplot data
    "PLOT",  # Plot-level data
    "PLOTGEOM",  # Plot geometry & ECOSUBCD (Bailey ecoprovince, used by
    # pyfia.carbon.live_tree for the Phase 1.5+ DIVISION coefficient
    # lookup; not in rFIA common tables).
    "POP_ESTN_UNIT",  # Population estimation units
    "POP_EVAL",  # Population evaluations
    "POP_EVAL_GRP",  # Population evaluation groups
    "POP_EVAL_TYP",  # Population evaluation types
    "POP_PLOT_STRATUM_ASSGN",  # Plot stratum assignments
    "POP_STRATUM",  # Stratum definitions
    "SUBPLOT",  # Subplot data
    "TREE",  # Tree-level data (largest table)
    "TREE_GRM_COMPONENT",  # Growth/removal/mortality components
    "TREE_GRM_MIDPT",  # GRM midpoint values
    "TREE_GRM_BEGIN",  # GRM beginning period values
    "SUBP_COND_CHNG_MTRX",  # Subplot condition change matrix
    "SEEDLING",  # Seedling data
    "SURVEY",  # Survey metadata
    "SUBP_COND",  # Subplot condition data
    "P2VEG_SUBP_STRUCTURE",  # Phase 2 vegetation structure
]

# Reference tables (state-independent) in FIADB's published schema
REFERENCE_TABLES: list[str] = sorted(t for t in COLUMN_TYPES if t.startswith("REF_"))

# National tables, published in FIADB_REFERENCE.zip rather than per state
NATIONAL_TABLE_PREFIXES = ("REF_", "EVALIDATOR_")
NATIONAL_TABLES = ("BEGINEND", "DATAMART_MOST_RECENT_INV")


def _is_national(table: str) -> bool:
    return table.startswith(NATIONAL_TABLE_PREFIXES) or table in NATIONAL_TABLES


# Every state table in FIADB's published schema (pyfia.constants.fiadb_schema).
# download(common=False) fetches each state's DataMart archive, which holds
# exactly the tables DataMart publishes for that state.
ALL_TABLES: list[str] = sorted(t for t in COLUMN_TYPES if not _is_national(t))

# Valid US state/territory codes (2-letter abbreviations)
VALID_STATE_CODES: dict[str, str] = {
    "AL": "Alabama",
    "AK": "Alaska",
    "AZ": "Arizona",
    "AR": "Arkansas",
    "CA": "California",
    "CO": "Colorado",
    "CT": "Connecticut",
    "DE": "Delaware",
    "FL": "Florida",
    "GA": "Georgia",
    "HI": "Hawaii",
    "ID": "Idaho",
    "IL": "Illinois",
    "IN": "Indiana",
    "IA": "Iowa",
    "KS": "Kansas",
    "KY": "Kentucky",
    "LA": "Louisiana",
    "ME": "Maine",
    "MD": "Maryland",
    "MA": "Massachusetts",
    "MI": "Michigan",
    "MN": "Minnesota",
    "MS": "Mississippi",
    "MO": "Missouri",
    "MT": "Montana",
    "NE": "Nebraska",
    "NV": "Nevada",
    "NH": "New Hampshire",
    "NJ": "New Jersey",
    "NM": "New Mexico",
    "NY": "New York",
    "NC": "North Carolina",
    "ND": "North Dakota",
    "OH": "Ohio",
    "OK": "Oklahoma",
    "OR": "Oregon",
    "PA": "Pennsylvania",
    "RI": "Rhode Island",
    "SC": "South Carolina",
    "SD": "South Dakota",
    "TN": "Tennessee",
    "TX": "Texas",
    "UT": "Utah",
    "VT": "Vermont",
    "VA": "Virginia",
    "WA": "Washington",
    "WV": "West Virginia",
    "WI": "Wisconsin",
    "WY": "Wyoming",
    # US Territories
    "AS": "American Samoa",
    "FM": "Federated States of Micronesia",
    "GU": "Guam",
    "MH": "Marshall Islands",
    "MP": "Northern Mariana Islands",
    "PW": "Palau",
    "PR": "Puerto Rico",
    "VI": "Virgin Islands",
}

# State FIPS codes mapping
STATE_FIPS_CODES: dict[str, int] = {
    "AL": 1,
    "AK": 2,
    "AZ": 4,
    "AR": 5,
    "CA": 6,
    "CO": 8,
    "CT": 9,
    "DE": 10,
    "FL": 12,
    "GA": 13,
    "HI": 15,
    "ID": 16,
    "IL": 17,
    "IN": 18,
    "IA": 19,
    "KS": 20,
    "KY": 21,
    "LA": 22,
    "ME": 23,
    "MD": 24,
    "MA": 25,
    "MI": 26,
    "MN": 27,
    "MS": 28,
    "MO": 29,
    "MT": 30,
    "NE": 31,
    "NV": 32,
    "NH": 33,
    "NJ": 34,
    "NM": 35,
    "NY": 36,
    "NC": 37,
    "ND": 38,
    "OH": 39,
    "OK": 40,
    "OR": 41,
    "PA": 42,
    "RI": 44,
    "SC": 45,
    "SD": 46,
    "TN": 47,
    "TX": 48,
    "UT": 49,
    "VT": 50,
    "VA": 51,
    "WA": 53,
    "WV": 54,
    "WI": 55,
    "WY": 56,
    "AS": 60,
    "GU": 66,
    "MP": 69,
    "PR": 72,
    "VI": 78,
}


def validate_state_code(state: str) -> str:
    """
    Validate and normalize a state code.

    Parameters
    ----------
    state : str
        State code to validate (case-insensitive).

    Returns
    -------
    str
        Normalized uppercase state code.

    Raises
    ------
    ValueError
        If the state code is invalid.
    """
    normalized = state.upper().strip()

    # Allow "REF" for reference tables
    if normalized == "REF":
        return normalized

    if normalized not in VALID_STATE_CODES:
        from pyfia.downloader.exceptions import StateNotFoundError

        raise StateNotFoundError(state, list(VALID_STATE_CODES.keys()))

    return normalized


def get_state_fips(state: str) -> int:
    """
    Get the FIPS code for a state.

    Parameters
    ----------
    state : str
        Two-letter state abbreviation.

    Returns
    -------
    int
        State FIPS code.

    Raises
    ------
    ValueError
        If the state code is invalid.
    """
    normalized = validate_state_code(state)
    if normalized == "REF":
        raise ValueError("Reference tables do not have a FIPS code")
    return STATE_FIPS_CODES[normalized]


def get_tables_for_download(
    common: bool = True, tables: list[str] | None = None
) -> list[str]:
    """
    Get the list of tables to download.

    Parameters
    ----------
    common : bool, default True
        If True and tables is None, return common tables only.
        If False and tables is None, return all tables.
    tables : list of str, optional
        Explicit list of tables to download. Overrides common parameter.

    Returns
    -------
    list of str
        List of table names to download.
    """
    if tables is not None:
        # Validate table names
        normalized = [t.upper().strip() for t in tables]
        return normalized

    return COMMON_TABLES if common else ALL_TABLES
