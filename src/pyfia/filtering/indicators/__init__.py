"""
Domain calculation components for pyFIA estimation.

This module provides domain indicator calculation functionality including
land type classification, tree domain filtering, and domain indicator
computation following FIA methodology.
"""

from .calculator import DomainIndicatorCalculator
from .land_types import (
    classify_land_types,
    get_land_domain_indicator,
    add_land_type_categories,
    LandTypeCategory,
)

__all__ = [
    "DomainIndicatorCalculator",
    "classify_land_types",
    "get_land_domain_indicator",
    "add_land_type_categories",
    "LandTypeCategory",
]