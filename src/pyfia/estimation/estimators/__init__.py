"""
FIA estimators.

Simple, focused estimator implementations without unnecessary abstractions.
"""

from __future__ import annotations

from .area import AreaEstimator, area
from .area_change import AreaChangeEstimator, area_change
from .biomass import BiomassEstimator, biomass
from .carbon import carbon
from .carbon_flux import carbon_flux
from .carbon_pools import CarbonPoolEstimator, carbon_pool
from .growth import GrowthEstimator, growth
from .mortality import MortalityEstimator, mortality
from .panel import PanelBuilder, panel
from .panel_validation import (
    ComparisonResult,
    compare_panel_to_removals,
    diagnose_panel_removals_diff,
    validate_panel_harvest,
)
from .removals import RemovalsEstimator, removals
from .site_index import SiteIndexEstimator, site_index
from .tpa import TPAEstimator, tpa
from .volume import VolumeEstimator, volume

__all__ = [
    # Functions (primary API)
    "area",
    "area_change",
    "biomass",
    "carbon",
    "carbon_flux",
    "carbon_pool",
    "growth",
    "mortality",
    "panel",
    "removals",
    "site_index",
    "tpa",
    "volume",
    # Panel validation functions
    "compare_panel_to_removals",
    "diagnose_panel_removals_diff",
    "validate_panel_harvest",
    # Classes (for advanced usage)
    "AreaEstimator",
    "AreaChangeEstimator",
    "BiomassEstimator",
    "CarbonPoolEstimator",
    "ComparisonResult",
    "GrowthEstimator",
    "MortalityEstimator",
    "PanelBuilder",
    "RemovalsEstimator",
    "SiteIndexEstimator",
    "TPAEstimator",
    "VolumeEstimator",
]
