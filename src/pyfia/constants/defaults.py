"""
Default values, validation ranges, and error messages.

Contains default parameter values, mathematical constants, validation
ranges, and standard error messages used throughout pyFIA.
"""


class MathConstants:
    """Mathematical conversion factors."""

    # Basal area factor: converts square inches to square feet
    # (pi/4) / 144 = 0.005454154
    BASAL_AREA_FACTOR = 0.005454154

    # Biomass conversion: pounds to tons
    LBS_TO_TONS = 2000.0

    # Default temporal weighting parameter
    DEFAULT_LAMBDA = 0.5


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


class ErrorMessages:
    """Standard error messages."""

    NO_EVALID = "No EVALID specified. Use find_evalid() or clip_by_evalid() first."
    INVALID_TREE_TYPE = "Invalid tree_type. Valid options: 'all', 'live', 'dead', 'gs'"
    INVALID_LAND_TYPE = "Invalid land_type. Valid options: 'all', 'forest', 'timber'"
    INVALID_METHOD = "Invalid method. Currently only 'TI' is supported."
    NO_DATA = "No data found matching the specified criteria."
    MISSING_TABLE = "Required table '{}' not found in database."
    INVALID_DOMAIN = "Invalid domain expression: {}"
