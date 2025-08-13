"""
Statistical estimation methods for FIA data analysis.

This module provides all the core estimation functions following FIA methodology:
- Forest area estimation
- Biomass and carbon calculations
- Volume estimation
- Trees per acre (TPA)
- Mortality analysis
- Growth estimation
- Tree counting

All functions support proper statistical methodology including:
- EVALID-based filtering
- Stratified sampling
- Domain specifications
- Standard error calculations

The module now includes a refactored BaseEstimator architecture that provides
a cleaner, more maintainable implementation while preserving exact functionality.
"""

from .area import area

# Base estimator classes for refactored architecture
from .base import BaseEstimator, EstimatorConfig
from .config import EstimatorConfigV2, MortalityConfig
from .biomass import biomass
from .growth import growth
from .mortality import mortality as _mortality_func  # Import from mortality.py file
mortality = _mortality_func  # Re-export with original name
from .tpa import tpa
from .tree import tree_count

# Estimation utilities
from .utils import (
    apply_domain_filter,
    calculate_adjustment_factors,
    calculate_population_estimates,
    calculate_ratio_estimates,
    calculate_stratum_estimates,
    merge_estimation_data,
    summarize_by_groups,
)
from .volume import volume

# Refactored estimators (for advanced usage)
from .area import AreaEstimator
from .volume import VolumeEstimator
from .mortality import MortalityCalculator, MortalityEstimatorConfig

__all__ = [
    # Main estimation functions
    "area",
    "biomass",
    "volume",
    "tpa",
    "mortality",
    "growth",
    "tree_count",
    # Base estimator architecture
    "BaseEstimator",
    "EstimatorConfig",
    "EstimatorConfigV2",
    "MortalityConfig",
    "AreaEstimator",
    "VolumeEstimator",
    "MortalityCalculator",
    "MortalityEstimatorConfig",
    # Utility functions
    "merge_estimation_data",
    "calculate_adjustment_factors",
    "calculate_stratum_estimates",
    "calculate_population_estimates",
    "apply_domain_filter",
    "calculate_ratio_estimates",
    "summarize_by_groups",
]
