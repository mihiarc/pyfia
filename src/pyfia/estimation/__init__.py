"""
Estimation module for pyFIA.

This module provides high-level functions for estimating various forest
attributes from FIA data, following FIA statistical procedures.
"""

from .base import (
    BaseEstimator,
    EstimatorConfig,
)

from .config import (
    EstimatorConfigV2,
    MortalityConfig,
)

from .formatters import OutputFormatter, format_estimation_output

from .area import area
from .biomass import biomass
from .growth import growth
from .mortality.mortality import mortality
from .tpa import tpa
from .tree.tree import tree_count, tree_count_simple
from .volume import volume

# Variance calculation components
from .variance_calculator import (
    FIAVarianceCalculator,
    calculate_cv,
    calculate_relative_se,
)

__all__ = [
    # Base classes
    "BaseEstimator",
    "EstimatorConfig",
    "EstimatorConfigV2",
    
    # Configs
    "MortalityConfig",
    
    # Formatters
    "OutputFormatter",
    "format_estimation_output",
    
    # Estimation functions
    "area",
    "biomass",
    "growth",
    "mortality",
    "tpa",
    "tree_count",
    "tree_count_simple",
    "volume",
    
    # Variance calculation
    "FIAVarianceCalculator",
    "calculate_cv",
    "calculate_relative_se",
]