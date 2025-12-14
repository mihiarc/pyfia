"""
Domain calculation components for pyFIA estimation.

This module provides domain indicator calculation functionality including
land type classification and domain indicator computation following FIA methodology.
"""

from .land_types import (
    LandTypeCategory,
    add_land_type_categories,
    classify_land_types,
    get_land_domain_indicator,
)

__all__ = [
    "classify_land_types",
    "get_land_domain_indicator",
    "add_land_type_categories",
    "LandTypeCategory",
]
