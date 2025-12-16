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


class EVALIDYearParsing:
    """Constants for parsing years from EVALID codes.

    EVALID format: SSYYTT where:
    - SS = State FIPS code (2 digits)
    - YY = Year (2 digits, requires Y2K windowing)
    - TT = Evaluation type code (2 digits)

    FIA uses Y2K windowing to interpret 2-digit years:
    - Years 00-30 are interpreted as 2000-2030
    - Years 31-99 are interpreted as 1931-1999

    Note: The FIA program began annual inventory in 1999, so valid
    evaluation years are typically 1999-present. Earlier years may
    appear in legacy data.

    References:
        FIA Database User Guide, Appendix B: EVALID Construction
    """

    # Y2K windowing threshold: years <= this value are 20xx, > are 19xx
    Y2K_WINDOW_THRESHOLD = 30

    # Century bases for Y2K windowing
    CENTURY_2000 = 2000
    CENTURY_1900 = 1900

    # Alternative threshold used in some contexts (90-99 are 1990s)
    LEGACY_THRESHOLD = 90

    # Valid year range for FIA evaluations
    # FIA annual inventory began in 1999; evaluations extend through near-future
    MIN_VALID_YEAR = 1990  # Allow some pre-annual inventory data
    MAX_VALID_YEAR = 2050  # Reasonable future limit

    # Default year offset when year cannot be determined
    # FIA data typically has ~2 year processing lag
    DEFAULT_YEAR_OFFSET = 2


class ErrorMessages:
    """Standard error messages."""

    NO_EVALID = "No EVALID specified. Use find_evalid() or clip_by_evalid() first."
    INVALID_TREE_TYPE = "Invalid tree_type. Valid options: 'all', 'live', 'dead', 'gs'"
    INVALID_LAND_TYPE = "Invalid land_type. Valid options: 'all', 'forest', 'timber'"
    INVALID_METHOD = "Invalid method. Currently only 'TI' is supported."
    NO_DATA = "No data found matching the specified criteria."
    MISSING_TABLE = "Required table '{}' not found in database."
    INVALID_DOMAIN = "Invalid domain expression: {}"
