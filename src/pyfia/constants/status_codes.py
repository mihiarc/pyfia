"""
FIA status and classification codes.

Contains tree status, land status, ownership, and other classification
codes used in FIA data.
"""

from __future__ import annotations

import warnings
from typing import Any


class _DeprecatedCode:
    """Class attribute that warns on access; removed in the next minor release."""

    def __init__(self, value: Any, message: str):
        self.value = value
        self.message = message

    def __set_name__(self, owner: type, name: str) -> None:
        self.qualname = f"{owner.__name__}.{name}"

    def __get__(self, obj: object, owner: type | None = None) -> Any:
        warnings.warn(
            f"{self.qualname} is deprecated and will be removed in pyFIA 1.5.0: "
            f"{self.message}",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.value


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
    """Condition status codes (COND_STATUS_CD), FIADB User Guide section 2.5.9.

    Code 5 covers every nonsampled condition; the reason (denied access,
    hazardous, and so on) is in ``COND.COND_NONSAMPLE_REASN_CD``.
    """

    FOREST = 1
    NONFOREST = 2
    WATER = 3  # Noncensus water
    CENSUS_WATER = 4
    NONSAMPLED = 5  # Nonsampled, possibility of forest land
    DENIED_ACCESS = _DeprecatedCode(
        5,
        "COND_STATUS_CD 5 is 'Nonsampled, possibility of forest land' for any "
        "reason; use LandStatus.NONSAMPLED and COND_NONSAMPLE_REASN_CD.",
    )
    HAZARDOUS = _DeprecatedCode(
        6,
        "COND_STATUS_CD 6 is not an FIADB code; hazardous conditions are "
        "LandStatus.NONSAMPLED with a COND_NONSAMPLE_REASN_CD.",
    )
    INACCESSIBLE = _DeprecatedCode(
        7,
        "COND_STATUS_CD 7 is not an FIADB code; inaccessible conditions are "
        "LandStatus.NONSAMPLED with a COND_NONSAMPLE_REASN_CD.",
    )


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
    """GRM component values in ``TREE_GRM_COMPONENT`` (User Guide section 3.3).

    Exact values match a component column with ``==``. The numbered
    components share a prefix (``CUT``, ``MORTALITY``, ``DIVERSION``,
    ``REVERSION``) for matching with ``str.starts_with``.
    """

    SURVIVOR = "SURVIVOR"
    INGROWTH = "INGROWTH"
    CUT1 = "CUT1"
    CUT2 = "CUT2"
    MORTALITY1 = "MORTALITY1"
    MORTALITY2 = "MORTALITY2"
    DIVERSION1 = "DIVERSION1"
    DIVERSION2 = "DIVERSION2"
    REVERSION1 = "REVERSION1"
    REVERSION2 = "REVERSION2"
    NOT_USED = "NOT USED"

    # Prefixes
    CUT = "CUT"
    MORTALITY = "MORTALITY"
    DIVERSION = "DIVERSION"
    REVERSION = "REVERSION"

    HARVEST = _DeprecatedCode(
        "CUT",
        "no GRM component is named HARVEST; harvest removals are CUT1 and CUT2, "
        "matched by the TreeComponent.CUT prefix.",
    )


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
