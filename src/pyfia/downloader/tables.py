"""
FIA table definitions for download operations.

This module defines the tables available from the FIA DataMart and which
tables are required for various analysis workflows.

References
----------
- FIA DataMart: https://apps.fs.usda.gov/fia/datamart/datamart.html
- rFIA common tables: https://doserlab.com/files/rfia/reference/getfia
"""

# Common tables required for pyFIA estimation functions
# These match the rFIA "common=TRUE" default tables
COMMON_TABLES: list[str] = [
    "COND",  # Condition data
    "COND_DWM_CALC",  # Down woody material calculations
    "INVASIVE_SUBPLOT_SPP",  # Invasive species subplot data
    "PLOT",  # Plot-level data
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

# Reference tables (state-independent)
REFERENCE_TABLES: list[str] = [
    "REF_SPECIES",
    "REF_SPECIES_GROUP",
    "REF_FOREST_TYPE",
    "REF_FOREST_TYPE_GROUP",
    "REF_CITATION",
    "REF_FIADB_VERSION",
    "REF_GRM_TYPE",
    "REF_HABTYP_DESCRIPTION",
    "REF_HABTYP_PUBLICATION",
    "REF_INVASIVE_SPECIES",
    "REF_OWNGRPCD",
    "REF_POP_ATTRIBUTE",
    "REF_POP_EVAL_TYP_DESCR",
    "REF_RESEARCH_STATION",
    "REF_STATE_ELEV",
    "REF_UNIT",
]

# All available FIA tables (comprehensive list)
ALL_TABLES: list[str] = [
    "BOUNDARY",
    "COND",
    "COND_DWM_CALC",
    "COUNTY",
    "DWM_COARSE_WOODY_DEBRIS",
    "DWM_DUFF_LITTER_FUEL",
    "DWM_FINE_WOODY_DEBRIS",
    "DWM_MICROPLOT_FUEL",
    "DWM_RESIDUAL_PILE",
    "DWM_TRANSECT_SEGMENT",
    "DWM_VISIT",
    "GRND_CVR",
    "INVASIVE_SUBPLOT_SPP",
    "LICHEN_LAB",
    "LICHEN_PLOT_SUMMARY",
    "LICHEN_VISIT",
    "OZONE_BIOSITE_SUMMARY",
    "OZONE_PLOT",
    "OZONE_PLOT_SUMMARY",
    "OZONE_SPECIES_SUMMARY",
    "OZONE_VALIDATION",
    "OZONE_VISIT",
    "P2VEG_SUBPLOT_SPP",
    "P2VEG_SUBP_STRUCTURE",
    "PLOT",
    "PLOTGEOM",
    "PLOTSNAP",
    "POP_ESTN_UNIT",
    "POP_EVAL",
    "POP_EVAL_ATTRIBUTE",
    "POP_EVAL_GRP",
    "POP_EVAL_TYP",
    "POP_PLOT_STRATUM_ASSGN",
    "POP_STRATUM",
    "SEEDLING",
    "SITETREE",
    "SOILS_EROSION",
    "SOILS_LAB",
    "SOILS_SAMPLE_LOC",
    "SOILS_VISIT",
    "SUBPLOT",
    "SUBP_COND",
    "SUBP_COND_CHNG_MTRX",
    "SURVEY",
    "TREE",
    "TREE_GRM_BEGIN",
    "TREE_GRM_COMPONENT",
    "TREE_GRM_ESTN",
    "TREE_GRM_MIDPT",
    "TREE_REGIONAL_BIOMASS",
    "TREE_WOODLAND_STEMS",
    "VEG_PLOT_SPECIES",
    "VEG_QUADRAT",
    "VEG_SUBPLOT",
    "VEG_SUBPLOT_SPP",
    "VEG_VISIT",
]

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
