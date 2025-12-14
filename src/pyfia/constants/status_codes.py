"""
FIA status and classification codes.

Contains tree status, land status, ownership, and other classification
codes used in FIA data.
"""


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


class OwnershipGroup:
    """Ownership group codes (OWNGRPCD)."""

    NATIONAL_FOREST = 10
    OTHER_FEDERAL = 20
    STATE_LOCAL_GOV = 30
    PRIVATE = 40


class DamageAgent:
    """Damage agent code thresholds."""

    # Trees with AGENTCD < 30 have no severe damage
    SEVERE_DAMAGE_THRESHOLD = 30


class TreeComponent:
    """Tree component identifiers for GRM tables."""

    SURVIVOR = "SURVIVOR"
    MORTALITY = "MORTALITY"
    HARVEST = "HARVEST"
    INGROWTH = "INGROWTH"


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


class EstimatorType:
    """Types of FIA estimators."""

    AREA = "AREA"
    BIOMASS = "BIOMASS"
    VOLUME = "VOLUME"
    TPA = "TPA"
    MORTALITY = "MORTALITY"
    GROWTH = "GROWTH"
