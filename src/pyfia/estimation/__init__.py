"""
Estimation module for pyFIA.

This module provides high-level functions for estimating various forest
attributes from FIA data, following FIA statistical procedures.
"""

from .framework.base import BaseEstimator

from .framework.config import (
    EstimatorConfig,
    ModuleEstimatorConfig,
    ModuleParameters,
    PerformanceConfig,
    CacheConfig,
    LoggingConfig,
    ConfigFactory,
)

from .infrastructure.formatters import OutputFormatter, format_estimation_output

from .api.area import area, AreaEstimator
from .api.biomass import biomass, BiomassEstimator
from .api.growth import growth, GrowthEstimator
from .api.mortality import mortality, MortalityEstimator
from .api.tpa import tpa, TPAEstimator
from .api.tree import tree_count, tree_count_simple
from .api.volume import volume, VolumeEstimator

# Lazy evaluation components
from .infrastructure.evaluation import (
    LazyComputationNode,
    LazyEstimatorMixin,
    FrameWrapper,
    operation,
    CollectionStrategy,
    LazyConfigMixin,
)

from .infrastructure.caching import (
    CacheKey,
    CacheEntry,
    LazyFrameCache,
    QueryPlanCache,
    cached_operation,
    cached_operation,
    CacheConfig,
)

from .infrastructure.progress import (
    LazyOperationProgress,
    EstimatorProgressMixin,
    CollectionProgress,
    OperationType,
    ProgressConfig,
)

# Query optimization components
from .framework.builder import (
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

from .processing.join import (
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