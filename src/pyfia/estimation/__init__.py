"""
Estimation module for pyFIA.

This module provides high-level functions for estimating various forest
attributes from FIA data, following FIA statistical procedures.
"""

from .base_estimator import BaseEstimator

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

# Lazy evaluation components
from .evaluation import (
    LazyComputationNode,
    LazyEstimatorMixin,
    FrameWrapper,
    operation,
    CollectionStrategy,
    LazyConfigMixin,
)

from .caching import (
    CacheKey,
    CacheEntry,
    LazyFrameCache,
    QueryPlanCache,
    cached_operation,
    cached_operation,
    CacheConfig,
)

from .progress import (
    LazyOperationProgress,
    EstimatorProgressMixin,
    CollectionProgress,
    OperationType,
    ProgressConfig,
)

# Query optimization components
from .query_builders import (
    QueryBuilderFactory,
    CompositeQueryBuilder,
    BaseQueryBuilder,
    TreeQueryBuilder,
    ConditionQueryBuilder,
    PlotQueryBuilder,
    StratificationQueryBuilder,
    QueryPlan,
    QueryColumn,
    QueryFilter,
    QueryJoin,
    QueryJoinStrategy,
    FilterPushDownLevel,
)

from .join import (
    JoinManager,
    JoinOptimizer,
    JoinPlan,
    JoinType,
    JoinStrategy,
    TableStatistics,
    FIATableInfo,
    get_join_manager,
    optimized_join,
)

__all__ = [
    # Base classes
    "BaseEstimator",
    
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
    
    # Lazy evaluation
    "LazyComputationNode",
    "LazyEstimatorMixin",
    "FrameWrapper",
    "operation",
    "CollectionStrategy",
    
    # Caching
    "CacheKey",
    "CacheEntry",
    "LazyFrameCache",
    "QueryPlanCache",
    "cached_operation",
    "cached_operation",
    
    # Progress tracking
    "LazyOperationProgress",
    "EstimatorProgressMixin",
    "CollectionProgress",
    "OperationType",
    
    # Query optimization
    "QueryBuilderFactory",
    "CompositeQueryBuilder",
    "BaseQueryBuilder",
    "TreeQueryBuilder",
    "ConditionQueryBuilder",
    "PlotQueryBuilder",
    "StratificationQueryBuilder",
    "QueryPlan",
    "QueryColumn",
    "QueryFilter",
    "QueryJoin",
    "QueryJoinStrategy",
    "FilterPushDownLevel",
    
    # Join optimization
    "JoinManager",
    "JoinOptimizer",
    "JoinPlan",
    "JoinType",
    "JoinStrategy",
    "TableStatistics",
    "FIATableInfo",
    "get_join_manager",
    "optimized_join",
]