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

from .area import area
from .biomass import biomass
from .growth import growth
from .mortality.mortality import mortality
from .tpa import tpa
from .tree.tree import tree_count, tree_count_simple
from .volume import volume

__all__ = [
    # Base classes
    "BaseEstimator",
    "EstimatorConfig",
    "EstimatorConfigV2",
    
    # Configs
    "MortalityConfig",
    
    # Estimation functions
    "area",
    "biomass",
    "growth",
    "mortality",
    "tpa",
    "tree_count",
    "tree_count_simple",
    "volume",
]