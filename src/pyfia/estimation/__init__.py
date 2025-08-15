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
from .growth_lazy import LazyGrowthEstimator, growth_lazy
from .mortality.mortality import mortality
from .mortality_lazy import LazyMortalityEstimator, mortality_lazy
from .tpa import tpa
from .tree.tree import tree_count, tree_count_simple
from .volume import volume
from .volume_lazy import LazyVolumeEstimator, volume_lazy

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
    "EstimatorConfig",
    "EstimatorConfigV2",
    "LazyBaseEstimator",
    
    # Configs
    "MortalityConfig",
    "LazyConfigMixin",
    "CacheConfig",
    "ProgressConfig",
    
    # Formatters
    "OutputFormatter",
    "format_estimation_output",
    
    # Estimation functions
    "area",
    "biomass",
    "growth",
    "LazyGrowthEstimator",
    "growth_lazy",
    "mortality",
    "mortality_lazy",
    "tpa",
    "tree_count",
    "tree_count_simple",
    "volume",
    "LazyVolumeEstimator",
    "volume_lazy",
    "LazyMortalityEstimator",
    
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