"""
FIA constants and standard values.

This module contains all standard constants used throughout pyFIA,
including plot design parameters, diameter breakpoints, status codes,
and other FIA-specific values. These constants are based on the
USDA Forest Service FIA Database User Guide.
"""

from typing import Dict, Tuple

# =============================================================================
# Plot Design Constants
# =============================================================================


class PlotDesign:
    """FIA plot design parameters."""

    # Plot radii in feet
    MICROPLOT_RADIUS_FT = 6.8
    SUBPLOT_RADIUS_FT = 24.0
    MACROPLOT_RADIUS_FT = 58.9

    # Plot areas as fraction of acre
    MICROPLOT_AREA_ACRES = 1 / 300  # 0.00333 acres
    SUBPLOT_AREA_ACRES = 1 / 24  # 0.04167 acres
    MACROPLOT_AREA_ACRES = 1 / 4  # 0.25 acres

    # Standard plot design code
    STANDARD_DESIGN_CD = 1


# =============================================================================
# Diameter Classes and Breakpoints
# =============================================================================


class DiameterBreakpoints:
    """Standard diameter breakpoints in inches."""

    # Minimum measurable diameter
    MIN_DBH = 1.0

    # Microplot/Subplot boundary
    MICROPLOT_MAX_DIA = 5.0
    SUBPLOT_MIN_DIA = 5.0

    # Volume thresholds
    BOARD_FOOT_MIN_DIA = 9.0
    SAWLOG_MIN_DIA = 9.0

    # Size class boundaries for grouping
    SIZE_CLASS_BOUNDARIES = [1.0, 5.0, 10.0, 20.0, 30.0]


# Standard size class definitions
STANDARD_SIZE_CLASSES: Dict[str, Tuple[float, float]] = {
    "1.0-4.9": (1.0, 5.0),
    "5.0-9.9": (5.0, 10.0),
    "10.0-19.9": (10.0, 20.0),
    "20.0-29.9": (20.0, 30.0),
    "30.0+": (30.0, float("inf")),
}

# Descriptive size class definitions (alternative labeling)
DESCRIPTIVE_SIZE_CLASSES: Dict[str, Tuple[float, float]] = {
    "Saplings": (1.0, 5.0),
    "Small": (5.0, 10.0),
    "Medium": (10.0, 20.0),
    "Large": (20.0, float("inf")),
}


# =============================================================================
# Tree and Stand Status Codes
# =============================================================================


class TreeStatus:
    """Tree status codes (STATUSCD)."""

    LIVE = 1
    DEAD = 2
    REMOVED = 3  # Not typically used in estimation


class TreeClass:
    """Tree class codes (TREECLCD)."""

    GROWING_STOCK = 2
    ROUGH = 3
    ROTTEN = 4


class LandStatus:
    """Condition status codes (COND_STATUS_CD)."""

    FOREST = 1
    NONFOREST = 2
    WATER = 3
    CENSUS_WATER = 4
    DENIED_ACCESS = 5
    HAZARDOUS = 6
    INACCESSIBLE = 7


class SiteClass:
    """Site productivity class codes (SITECLCD)."""

    # Productive forest land classes
    PRODUCTIVE_CLASSES = [1, 2, 3, 4, 5, 6]
    # Class 7 is unproductive
    UNPRODUCTIVE = 7


class ReserveStatus:
    """Reserve status codes (RESERVCD)."""

    NOT_RESERVED = 0
    RESERVED = 1


# =============================================================================
# Damage and Agent Codes
# =============================================================================


class DamageAgent:
    """Damage agent code thresholds."""

    # Trees with AGENTCD < 30 have no severe damage
    SEVERE_DAMAGE_THRESHOLD = 30


# =============================================================================
# Ownership Groups
# =============================================================================


class OwnershipGroup:
    """Ownership group codes (OWNGRPCD)."""

    NATIONAL_FOREST = 10
    OTHER_FEDERAL = 20
    STATE_LOCAL_GOV = 30
    PRIVATE = 40


# =============================================================================
# Mathematical Constants
# =============================================================================


class MathConstants:
    """Mathematical conversion factors."""

    # Basal area factor: converts square inches to square feet
    # (π/4) / 144 = 0.005454154
    BASAL_AREA_FACTOR = 0.005454154

    # Biomass conversion: pounds to tons
    LBS_TO_TONS = 2000.0

    # Default temporal weighting parameter
    DEFAULT_LAMBDA = 0.5


# =============================================================================
# Component Identifiers
# =============================================================================


class TreeComponent:
    """Tree component identifiers for GRM tables."""

    SURVIVOR = "SURVIVOR"
    MORTALITY = "MORTALITY"
    HARVEST = "HARVEST"
    INGROWTH = "INGROWTH"


# =============================================================================
# Plot Basis Types
# =============================================================================


class PlotBasis:
    """Plot basis identifiers."""

    MICROPLOT = "MICR"
    SUBPLOT = "SUBP"
    MACROPLOT = "MACR"


# =============================================================================
# Evaluation Types
# =============================================================================


class EvaluationType:
    """FIA evaluation type codes."""

    VOLUME = "VOL"
    GROWTH_REMOVAL_MORTALITY = "GRM"
    CHANGE = "CHNG"
    DOWN_WOODY_MATERIAL = "DWM"
    REGENERATION = "REGEN"
    INVASIVE = "INVASIVE"
    OZONE = "OZONE"
    VEGETATION = "VEG"
    CROWNS = "CROWNS"


# =============================================================================
# Default Values
# =============================================================================


class Defaults:
    """Default values for various parameters."""

    # Default adjustment factors when not specified
    ADJ_FACTOR_DEFAULT = 1.0

    # Default expansion factor
    EXPNS_DEFAULT = 1.0

    # Default number of cores for parallel processing
    N_CORES_DEFAULT = 1

    # Default variance calculations
    INCLUDE_VARIANCE = False

    # Default totals calculation
    INCLUDE_TOTALS = False


# =============================================================================
# State Codes and Mappings
# =============================================================================


class StateCodes:
    """State FIPS codes and mappings."""

    # State abbreviations to FIPS codes
    ABBR_TO_CODE: Dict[str, int] = {
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
    }

    # State names to FIPS codes
    NAME_TO_CODE: Dict[str, int] = {
        "alabama": 1,
        "alaska": 2,
        "arizona": 4,
        "arkansas": 5,
        "california": 6,
        "colorado": 8,
        "connecticut": 9,
        "delaware": 10,
        "florida": 12,
        "georgia": 13,
        "hawaii": 15,
        "idaho": 16,
        "illinois": 17,
        "indiana": 18,
        "iowa": 19,
        "kansas": 20,
        "kentucky": 21,
        "louisiana": 22,
        "maine": 23,
        "maryland": 24,
        "massachusetts": 25,
        "michigan": 26,
        "minnesota": 27,
        "mississippi": 28,
        "missouri": 29,
        "montana": 30,
        "nebraska": 31,
        "nevada": 32,
        "new hampshire": 33,
        "new jersey": 34,
        "new mexico": 35,
        "new york": 36,
        "north carolina": 37,
        "north dakota": 38,
        "ohio": 39,
        "oklahoma": 40,
        "oregon": 41,
        "pennsylvania": 42,
        "rhode island": 44,
        "south carolina": 45,
        "south dakota": 46,
        "tennessee": 47,
        "texas": 48,
        "utah": 49,
        "vermont": 50,
        "virginia": 51,
        "washington": 53,
        "west virginia": 54,
        "wisconsin": 55,
        "wyoming": 56,
    }

    # Derived mappings
    CODE_TO_NAME: Dict[int, str] = {v: k.title() for k, v in NAME_TO_CODE.items()}
    CODE_TO_ABBR: Dict[int, str] = {v: k for k, v in ABBR_TO_CODE.items()}


# =============================================================================
# Validation Ranges
# =============================================================================


class ValidationRanges:
    """Valid ranges for various FIA values."""

    # Valid state codes (FIPS)
    MIN_STATE_CODE = 1
    MAX_STATE_CODE = 78  # Includes territories

    # Valid diameter range (inches)
    MIN_DIAMETER = 0.1
    MAX_DIAMETER = 999.9

    # Valid year range
    MIN_INVENTORY_YEAR = 1999
    MAX_INVENTORY_YEAR = 2099

    # Valid plot counts
    MIN_PLOTS = 1
    MAX_PLOTS = 1_000_000


# =============================================================================
# Estimator Types
# =============================================================================


class EstimatorType:
    """Types of FIA estimators."""

    AREA = "AREA"
    BIOMASS = "BIOMASS"
    VOLUME = "VOLUME"
    TPA = "TPA"
    MORTALITY = "MORTALITY"
    GROWTH = "GROWTH"


# =============================================================================
# Error Messages
# =============================================================================


class ErrorMessages:
    """Standard error messages."""

    NO_EVALID = "No EVALID specified. Use find_evalid() or clip_by_evalid() first."
    INVALID_TREE_TYPE = "Invalid tree_type. Valid options: 'all', 'live', 'dead', 'gs'"
    INVALID_LAND_TYPE = "Invalid land_type. Valid options: 'all', 'forest', 'timber'"
    INVALID_METHOD = "Invalid method. Currently only 'TI' is supported."
    NO_DATA = "No data found matching the specified criteria."
    MISSING_TABLE = "Required table '{}' not found in database."
    INVALID_DOMAIN = "Invalid domain expression: {}"


# =============================================================================
# Table Names
# =============================================================================


class TableNames:
    """Standard FIA table names."""

    # Core tables
    PLOT = "PLOT"
    TREE = "TREE"
    COND = "COND"
    SUBPLOT = "SUBPLOT"

    # Population estimation tables
    POP_EVAL = "POP_EVAL"
    POP_EVAL_TYP = "POP_EVAL_TYP"
    POP_STRATUM = "POP_STRATUM"
    POP_PLOT_STRATUM_ASSGN = "POP_PLOT_STRATUM_ASSGN"
    POP_ESTN_UNIT = "POP_ESTN_UNIT"

    # GRM tables for growth/mortality
    TREE_GRM_BEGIN = "TREE_GRM_BEGIN"
    TREE_GRM_MIDPT = "TREE_GRM_MIDPT"
    TREE_GRM_COMPONENT = "TREE_GRM_COMPONENT"

    # Reference tables
    REF_SPECIES = "REF_SPECIES"
    REF_FOREST_TYPE = "REF_FOREST_TYPE"
    REF_STATE = "REF_STATE"
