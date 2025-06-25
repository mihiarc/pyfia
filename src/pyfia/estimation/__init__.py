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
from .volume import volume
from .tpa import tpa
from .mortality import mortality
from .growth import growth
from .tree import tree_count

# Estimation utilities
from .utils import (
    merge_estimation_data,
    calculate_adjustment_factors,
    calculate_stratum_estimates,
    calculate_population_estimates,
    apply_domain_filter,
    calculate_ratio_estimates,
    summarize_by_groups,
)

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