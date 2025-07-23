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
"""

from .area import area
from .biomass import biomass
from .growth import growth
from .mortality import mortality
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

__all__ = [
    # Main estimation functions
    "area",
    "biomass",
    "volume",
    "tpa",
    "mortality",
    "growth",
    "tree_count",
    # Utility functions
    "merge_estimation_data",
    "calculate_adjustment_factors",
    "calculate_stratum_estimates",
    "calculate_population_estimates",
    "apply_domain_filter",
    "calculate_ratio_estimates",
    "summarize_by_groups",
]
