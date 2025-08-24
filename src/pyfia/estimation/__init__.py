"""
Estimation module for pyFIA.

This module provides high-level functions for estimating various forest
attributes from FIA data, following FIA statistical procedures.
"""

from .base import BaseEstimator

from .config import (
    EstimatorConfig,
    MortalityConfig,
    ModularEstimatorConfig,
    VolumeConfig,
    BiomassConfig,
    GrowthConfig,
    AreaConfig,
    ConfigFactory,
)

from .formatters import OutputFormatter, format_estimation_output

from .area import area, AreaEstimator
from .biomass import biomass, BiomassEstimator
from .growth import growth, GrowthEstimator
from .mortality import mortality, MortalityEstimator
from .tpa import tpa, TPAEstimator
from .tree.tree import tree_count, tree_count_simple
from .volume import volume, VolumeEstimator

# Variance calculation components
from .variance_calculator import (
    FIAVarianceCalculator,
    calculate_cv,
    calculate_relative_se,
)

# Lazy evaluation components
from .lazy_evaluation import (
    LazyComputationNode,
    LazyEstimatorMixin,
    LazyFrameWrapper,
    lazy_operation,
    CollectionStrategy,
    LazyConfigMixin,
)

from .lazy_base import LazyBaseEstimator

from .caching import (
    CacheKey,
    CacheEntry,
    LazyFrameCache,
    QueryPlanCache,
    cached_operation,
    cached_lazy_operation,
    CacheConfig,
)

from .progress import (
    LazyOperationProgress,
    EstimatorProgressMixin,
    CollectionProgress,
    OperationType,
    ProgressConfig,
)

__all__ = [
    # Base classes
    "BaseEstimator",
    "LazyBaseEstimator",
    
    # Configs
    "EstimatorConfig",
    "MortalityConfig",
    "ModularEstimatorConfig",
    "VolumeConfig",
    "BiomassConfig",
    "GrowthConfig",
    "AreaConfig",
    "ConfigFactory",
    "LazyConfigMixin",
    "CacheConfig",
    "ProgressConfig",
    
    # Formatters
    "OutputFormatter",
    "format_estimation_output",
    
    # Estimation functions
    "area",
    "AreaEstimator",
    "biomass",
    "BiomassEstimator",
    "growth",
    "GrowthEstimator",
    "mortality",
    "MortalityEstimator",
    "tpa",
    "TPAEstimator",
    "tree_count",
    "tree_count_simple",
    "volume",
    "VolumeEstimator",
    
    # Variance calculation
    "FIAVarianceCalculator",
    "calculate_cv",
    "calculate_relative_se",
    
    # Lazy evaluation
    "LazyComputationNode",
    "LazyEstimatorMixin",
    "LazyFrameWrapper",
    "lazy_operation",
    "CollectionStrategy",
    
    # Caching
    "CacheKey",
    "CacheEntry",
    "LazyFrameCache",
    "QueryPlanCache",
    "cached_operation",
    "cached_lazy_operation",
    
    # Progress tracking
    "LazyOperationProgress",
    "EstimatorProgressMixin",
    "CollectionProgress",
    "OperationType",
]