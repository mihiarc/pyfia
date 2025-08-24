# Phase 3: Architectural Improvements for pyFIA Estimation Module

## Executive Summary

Phase 3 focuses on architectural improvements that build upon the lazy evaluation infrastructure from Phase 2. The primary objectives are to:

1. **Unify the configuration system** to eliminate the dual-system confusion while maintaining backward compatibility
2. **Implement specialized query builders** for optimized and reusable query patterns
3. **Optimize join operations** through intelligent filter push-down and join strategy selection
4. **Establish patterns** that prepare for the Pipeline Framework in Phase 4

This design leverages the existing lazy evaluation infrastructure while addressing the configuration fragmentation and query inefficiencies identified in the current implementation.

## Current State Analysis

### Configuration System Issues

The codebase currently has two parallel configuration systems:

1. **`EstimatorConfig` (dataclass)** in `base.py`:
   - Used by all existing estimators
   - Simple dataclass with `extra_params` dictionary
   - No validation beyond Python type hints
   - Backward compatible with existing code

2. **`EstimatorConfigV2` and `MortalityConfig` (Pydantic v2)** in `config.py`:
   - Modern Pydantic v2 models with validation
   - Only partially integrated (mortality module)
   - Better type safety and validation
   - Not fully backward compatible

### Query Building Issues

Current query patterns show:
- Ad-hoc query construction in each estimator
- No reuse of common query patterns
- Inefficient column selection (SELECT *)
- Missing query plan optimization
- Repeated similar queries across estimators

### Join Operation Issues

Analysis reveals:
- Eager joins without filter push-down
- Redundant joins in some workflows
- Inefficient join ordering
- No join strategy selection (hash vs sort-merge)
- Missing broadcast join opportunities for small tables

## Phase 3 Architecture Design

### 1. Unified Configuration System

#### Design Principles

1. **Single Source of Truth**: One configuration system for all estimators
2. **Backward Compatibility**: Existing code continues to work
3. **Progressive Enhancement**: New features available without breaking changes
4. **Type Safety**: Full Pydantic v2 validation with proper types
5. **Extensibility**: Module-specific parameters without class proliferation

#### Implementation Architecture

```python
# src/pyfia/estimation/config_v3.py

from typing import Any, Dict, List, Literal, Optional, Union, TypeVar, Generic
from pydantic import BaseModel, Field, ConfigDict, field_validator, model_validator
from dataclasses import dataclass, asdict
import warnings

# Type variable for module-specific configs
TModuleConfig = TypeVar('TModuleConfig', bound='ModuleConfig')

class ModuleConfig(BaseModel):
    """Base class for module-specific configurations."""
    model_config = ConfigDict(extra='forbid')

class TemporalConfig(ModuleConfig):
    """Temporal method configuration."""
    method: Literal["TI", "SMA", "LMA", "EMA", "ANNUAL"] = Field(
        default="TI",
        description="Temporal estimation method"
    )
    lambda_: float = Field(
        default=0.5,
        ge=0.0,
        le=1.0,
        description="EMA weighting parameter"
    )
    window_size: int = Field(
        default=5,
        ge=1,
        description="Window size for moving averages"
    )

class MortalityModuleConfig(ModuleConfig):
    """Mortality-specific configuration."""
    mortality_type: Literal["tpa", "volume", "both"] = Field(
        default="tpa",
        description="Type of mortality to calculate"
    )
    tree_class: Literal["all", "timber", "growing_stock"] = Field(
        default="all",
        description="Tree classification"
    )
    include_natural: bool = Field(default=True)
    include_harvest: bool = Field(default=True)
    variance_method: Literal["standard", "ratio", "hybrid"] = Field(
        default="ratio",
        description="Variance calculation method"
    )

class LazyEvaluationConfig(ModuleConfig):
    """Lazy evaluation settings."""
    enabled: bool = Field(default=True)
    threshold_rows: int = Field(
        default=10000,
        ge=0,
        description="Row count threshold for auto-lazy"
    )
    collection_strategy: Literal["adaptive", "aggressive", "conservative"] = Field(
        default="adaptive"
    )
    cache_ttl_seconds: int = Field(default=3600, ge=0)
    show_progress: bool = Field(default=True)

class UnifiedEstimatorConfig(BaseModel, Generic[TModuleConfig]):
    """
    Unified configuration for all FIA estimators.
    
    This configuration system provides:
    - Type-safe parameter validation
    - Backward compatibility with EstimatorConfig
    - Module-specific extensions
    - Lazy evaluation settings
    """
    
    model_config = ConfigDict(
        validate_assignment=True,
        extra='allow',  # Allow for backward compatibility
        arbitrary_types_allowed=True
    )
    
    # === Core Grouping Parameters ===
    grp_by: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="Column(s) to group estimates by"
    )
    by_species: bool = Field(default=False)
    by_size_class: bool = Field(default=False)
    
    # === Domain Filters ===
    land_type: Literal["forest", "timber", "all"] = Field(default="forest")
    tree_type: Literal["live", "dead", "gs", "all"] = Field(default="live")
    tree_domain: Optional[str] = Field(default=None)
    area_domain: Optional[str] = Field(default=None)
    
    # === Output Options ===
    totals: bool = Field(default=False)
    variance: bool = Field(default=False)
    by_plot: bool = Field(default=False)
    most_recent: bool = Field(default=False)
    
    # === Module Configurations ===
    temporal: TemporalConfig = Field(default_factory=TemporalConfig)
    lazy: LazyEvaluationConfig = Field(default_factory=LazyEvaluationConfig)
    
    # Module-specific config (optional, typed)
    module_config: Optional[TModuleConfig] = Field(default=None)
    
    # === Backward Compatibility ===
    extra_params: Dict[str, Any] = Field(
        default_factory=dict,
        description="Additional parameters for backward compatibility"
    )
    
    @classmethod
    def from_legacy(cls, legacy_config: 'EstimatorConfig') -> 'UnifiedEstimatorConfig':
        """
        Create unified config from legacy EstimatorConfig.
        
        Provides seamless migration path from dataclass configs.
        """
        # Extract standard fields
        config_dict = {
            k: v for k, v in asdict(legacy_config).items()
            if k != 'extra_params' and v is not None
        }
        
        # Handle temporal parameters
        if 'method' in config_dict or 'lambda_' in config_dict:
            config_dict['temporal'] = TemporalConfig(
                method=config_dict.pop('method', 'TI'),
                lambda_=config_dict.pop('lambda_', 0.5)
            )
        
        # Handle lazy parameters from extra_params
        if 'extra_params' in legacy_config.__dict__:
            extra = legacy_config.extra_params
            if any(k in extra for k in ['lazy_enabled', 'show_progress', 'lazy_threshold_rows']):
                config_dict['lazy'] = LazyEvaluationConfig(
                    enabled=extra.get('lazy_enabled', True),
                    show_progress=extra.get('show_progress', True),
                    threshold_rows=extra.get('lazy_threshold_rows', 10000)
                )
        
        # Keep remaining extra_params for compatibility
        config_dict['extra_params'] = {
            k: v for k, v in legacy_config.extra_params.items()
            if k not in ['lazy_enabled', 'show_progress', 'lazy_threshold_rows']
        }
        
        return cls(**config_dict)
    
    def to_legacy(self) -> 'EstimatorConfig':
        """
        Convert to legacy EstimatorConfig for backward compatibility.
        """
        from pyfia.estimation.base import EstimatorConfig
        
        # Extract base parameters
        params = {
            'grp_by': self.grp_by,
            'by_species': self.by_species,
            'by_size_class': self.by_size_class,
            'land_type': self.land_type,
            'tree_type': self.tree_type,
            'tree_domain': self.tree_domain,
            'area_domain': self.area_domain,
            'method': self.temporal.method,
            'lambda_': self.temporal.lambda_,
            'totals': self.totals,
            'variance': self.variance,
            'by_plot': self.by_plot,
            'most_recent': self.most_recent,
        }
        
        # Add lazy settings to extra_params
        extra_params = self.extra_params.copy()
        extra_params.update({
            'lazy_enabled': self.lazy.enabled,
            'show_progress': self.lazy.show_progress,
            'lazy_threshold_rows': self.lazy.threshold_rows,
        })
        
        # Add module config if present
        if self.module_config:
            extra_params.update(self.module_config.model_dump())
        
        return EstimatorConfig(**params, extra_params=extra_params)
    
    def get_grouping_columns(self) -> List[str]:
        """Get all grouping columns based on configuration."""
        columns = []
        
        if self.grp_by:
            if isinstance(self.grp_by, str):
                columns.append(self.grp_by)
            else:
                columns.extend(self.grp_by)
        
        if self.by_species:
            columns.append("SPCD")
        
        if self.by_size_class:
            columns.append("SIZE_CLASS")
        
        # Remove duplicates while preserving order
        seen = set()
        return [x for x in columns if not (x in seen or seen.add(x))]


# Specialized configurations for specific modules
class MortalityEstimatorConfig(UnifiedEstimatorConfig[MortalityModuleConfig]):
    """Configuration for mortality estimation with type-safe module config."""
    module_config: MortalityModuleConfig = Field(default_factory=MortalityModuleConfig)


# Backward compatibility adapter
def adapt_config(config: Union[EstimatorConfig, UnifiedEstimatorConfig]) -> UnifiedEstimatorConfig:
    """
    Adapt any configuration to UnifiedEstimatorConfig.
    
    This function ensures backward compatibility by accepting either
    configuration type and returning a unified config.
    """
    if isinstance(config, UnifiedEstimatorConfig):
        return config
    elif hasattr(config, '__dataclass_fields__'):  # It's a dataclass (EstimatorConfig)
        return UnifiedEstimatorConfig.from_legacy(config)
    else:
        raise TypeError(f"Unsupported config type: {type(config)}")
```

### 2. Specialized Query Builder Framework

#### Design Principles

1. **Query Plan Optimization**: Build optimized query plans before execution
2. **Column Pruning**: Select only required columns
3. **Filter Push-Down**: Apply filters as early as possible
4. **Query Caching**: Cache and reuse common query patterns
5. **Lazy-First**: All queries built as lazy operations

#### Implementation Architecture

```python
# src/pyfia/estimation/query_builder.py

from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Any, Set, Tuple
from dataclasses import dataclass, field
import hashlib
import polars as pl
from functools import lru_cache

@dataclass
class QueryPlan:
    """Represents an optimized query execution plan."""
    steps: List['QueryStep'] = field(default_factory=list)
    estimated_rows: Optional[int] = None
    estimated_memory_mb: Optional[float] = None
    cache_key: Optional[str] = None
    
    def add_step(self, step: 'QueryStep') -> 'QueryPlan':
        self.steps.append(step)
        return self
    
    def optimize(self) -> 'QueryPlan':
        """Optimize query plan by reordering operations."""
        # Move filters before joins
        filters = [s for s in self.steps if s.operation == 'filter']
        joins = [s for s in self.steps if s.operation == 'join']
        others = [s for s in self.steps if s.operation not in ['filter', 'join']]
        
        self.steps = filters + joins + others
        return self

@dataclass
class QueryStep:
    """Single step in a query plan."""
    operation: str
    params: Dict[str, Any]
    estimated_selectivity: float = 1.0
    
class QueryBuilder(ABC):
    """Abstract base class for specialized query builders."""
    
    def __init__(self, lazy_enabled: bool = True):
        self.lazy_enabled = lazy_enabled
        self._query_cache: Dict[str, pl.LazyFrame] = {}
        self._column_requirements: Set[str] = set()
        
    @abstractmethod
    def build(self) -> pl.LazyFrame:
        """Build the query as a LazyFrame."""
        pass
    
    def _cache_key(self, **params) -> str:
        """Generate cache key for query parameters."""
        param_str = str(sorted(params.items()))
        return hashlib.md5(param_str.encode()).hexdigest()
    
    def add_required_columns(self, columns: List[str]) -> None:
        """Track required columns for pruning."""
        self._column_requirements.update(columns)

class StratificationQueryBuilder(QueryBuilder):
    """Optimized query builder for stratification data."""
    
    def __init__(self, db, evalids: Optional[List[int]] = None, **kwargs):
        super().__init__(**kwargs)
        self.db = db
        self.evalids = evalids
        self.plan = QueryPlan()
    
    @lru_cache(maxsize=32)
    def build(self) -> pl.LazyFrame:
        """Build optimized stratification query."""
        cache_key = self._cache_key(evalids=tuple(self.evalids) if self.evalids else None)
        
        if cache_key in self._query_cache:
            return self._query_cache[cache_key]
        
        # Build query plan
        self.plan.add_step(QueryStep(
            'load', 
            {'table': 'POP_PLOT_STRATUM_ASSGN'},
            estimated_selectivity=1.0
        ))
        
        # Apply EVALID filter early if present
        if self.evalids:
            self.plan.add_step(QueryStep(
                'filter',
                {'condition': f"EVALID IN {self.evalids}"},
                estimated_selectivity=0.1  # Typically filters 90% of data
            ))
        
        # Column pruning - only select needed columns
        required_cols = ['PLT_CN', 'STRATUM_CN', 'EVALID', 'INVYR', 'STATECD']
        if self._column_requirements:
            required_cols.extend(list(self._column_requirements))
        
        self.plan.add_step(QueryStep(
            'select',
            {'columns': required_cols},
            estimated_selectivity=1.0
        ))
        
        # Optimize plan
        self.plan = self.plan.optimize()
        
        # Execute plan
        ppsa = self.db.tables.get('POP_PLOT_STRATUM_ASSGN')
        if ppsa is None:
            self.db.load_table('POP_PLOT_STRATUM_ASSGN')
            ppsa = self.db.tables['POP_PLOT_STRATUM_ASSGN']
        
        # Convert to lazy if needed
        if isinstance(ppsa, pl.DataFrame):
            query = ppsa.lazy()
        else:
            query = ppsa
        
        # Apply EVALID filter
        if self.evalids:
            query = query.filter(pl.col('EVALID').is_in(self.evalids))
        
        # Select only required columns
        query = query.select(required_cols)
        
        # Cache result
        self._query_cache[cache_key] = query
        return query
    
    def with_pop_stratum(self) -> pl.LazyFrame:
        """Join with POP_STRATUM table efficiently."""
        base_query = self.build()
        
        # Load POP_STRATUM
        if 'POP_STRATUM' not in self.db.tables:
            self.db.load_table('POP_STRATUM')
        
        pop_stratum = self.db.tables['POP_STRATUM']
        if isinstance(pop_stratum, pl.DataFrame):
            pop_stratum = pop_stratum.lazy()
        
        # Apply EVALID filter to POP_STRATUM before join
        if self.evalids:
            pop_stratum = pop_stratum.filter(pl.col('EVALID').is_in(self.evalids))
        
        # Select only needed columns from POP_STRATUM
        pop_cols = ['CN', 'EXPNS', 'ADJ_FACTOR_MICR', 'ADJ_FACTOR_SUBP', 'ADJ_FACTOR_MACR']
        pop_stratum = pop_stratum.select(pop_cols).rename({'CN': 'STRATUM_CN'})
        
        # Perform optimized join
        return base_query.join(
            pop_stratum,
            on='STRATUM_CN',
            how='inner'
        )

class TreeQueryBuilder(QueryBuilder):
    """Optimized query builder for tree data."""
    
    def __init__(self, db, filters: Optional[Dict[str, Any]] = None, **kwargs):
        super().__init__(**kwargs)
        self.db = db
        self.filters = filters or {}
        self.plan = QueryPlan()
    
    def build(self) -> pl.LazyFrame:
        """Build optimized tree query."""
        # Load tree table
        if 'TREE' not in self.db.tables:
            self.db.load_table('TREE')
        
        tree = self.db.tables['TREE']
        if isinstance(tree, pl.DataFrame):
            query = tree.lazy()
        else:
            query = tree
        
        # Apply filters efficiently
        for col, value in self.filters.items():
            if isinstance(value, list):
                query = query.filter(pl.col(col).is_in(value))
            else:
                query = query.filter(pl.col(col) == value)
        
        # Column pruning based on requirements
        if self._column_requirements:
            available_cols = query.collect_schema().names()
            cols_to_select = [c for c in self._column_requirements if c in available_cols]
            if cols_to_select:
                query = query.select(cols_to_select)
        
        return query
    
    def with_conditions(self, condition_filters: Optional[Dict[str, Any]] = None) -> pl.LazyFrame:
        """Efficiently join with condition data."""
        tree_query = self.build()
        
        # Build condition query
        cond_builder = ConditionQueryBuilder(self.db, filters=condition_filters)
        cond_query = cond_builder.build()
        
        # Optimize join - use broadcast join if conditions are small
        return tree_query.join(
            cond_query,
            on=['PLT_CN', 'CONDID'],
            how='inner'
        )

class ConditionQueryBuilder(QueryBuilder):
    """Optimized query builder for condition data."""
    
    def __init__(self, db, filters: Optional[Dict[str, Any]] = None, **kwargs):
        super().__init__(**kwargs)
        self.db = db
        self.filters = filters or {}
    
    def build(self) -> pl.LazyFrame:
        """Build optimized condition query."""
        if 'COND' not in self.db.tables:
            self.db.load_table('COND')
        
        cond = self.db.tables['COND']
        if isinstance(cond, pl.DataFrame):
            query = cond.lazy()
        else:
            query = cond
        
        # Apply filters
        for col, value in self.filters.items():
            if isinstance(value, list):
                query = query.filter(pl.col(col).is_in(value))
            else:
                query = query.filter(pl.col(col) == value)
        
        return query

class AggregationQueryBuilder(QueryBuilder):
    """Build optimized aggregation queries."""
    
    def __init__(self, group_cols: List[str], agg_exprs: List[pl.Expr], **kwargs):
        super().__init__(**kwargs)
        self.group_cols = group_cols
        self.agg_exprs = agg_exprs
    
    def build_with_optimization(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """Build aggregation with optimization hints."""
        # Check if we should use streaming aggregation
        estimated_groups = self._estimate_cardinality(data, self.group_cols)
        
        if estimated_groups > 100000:
            # Use streaming for high cardinality
            return data.group_by(self.group_cols).agg(self.agg_exprs)
        else:
            # Standard aggregation for lower cardinality
            return data.group_by(self.group_cols, maintain_order=True).agg(self.agg_exprs)
    
    def _estimate_cardinality(self, data: pl.LazyFrame, cols: List[str]) -> int:
        """Estimate the cardinality of grouping columns."""
        # This is a simplified estimation
        # In practice, could use statistics or sampling
        base_cardinality = {
            'STATECD': 50,
            'COUNTYCD': 3000,
            'PLOT': 100000,
            'SPCD': 1000,
            'INVYR': 50,
        }
        
        estimated = 1
        for col in cols:
            estimated *= base_cardinality.get(col, 100)
        
        return min(estimated, 1000000)  # Cap at 1M

# Factory function for creating appropriate query builders
def create_query_builder(
    query_type: str,
    db,
    config: UnifiedEstimatorConfig,
    **kwargs
) -> QueryBuilder:
    """Factory function to create appropriate query builder."""
    
    builders = {
        'stratification': StratificationQueryBuilder,
        'tree': TreeQueryBuilder,
        'condition': ConditionQueryBuilder,
        'aggregation': AggregationQueryBuilder,
    }
    
    builder_class = builders.get(query_type)
    if not builder_class:
        raise ValueError(f"Unknown query type: {query_type}")
    
    # Pass lazy evaluation settings from config
    kwargs['lazy_enabled'] = config.lazy.enabled
    
    return builder_class(db, **kwargs)
```

### 3. Optimized Join Operations

#### Design Principles

1. **Filter Push-Down**: Apply filters before joins to reduce data volume
2. **Join Order Optimization**: Order joins by selectivity
3. **Join Strategy Selection**: Choose appropriate join algorithm
4. **Broadcast Joins**: Use for small dimension tables
5. **Lazy Evaluation**: Defer join execution until necessary

#### Implementation Architecture

```python
# src/pyfia/estimation/join_optimizer.py

from typing import List, Tuple, Optional, Dict, Any
from dataclasses import dataclass
from enum import Enum
import polars as pl

class JoinStrategy(Enum):
    """Join strategy selection."""
    HASH = "hash"
    SORT_MERGE = "sort_merge"
    BROADCAST = "broadcast"
    NESTED_LOOP = "nested_loop"

@dataclass
class JoinSpec:
    """Specification for a join operation."""
    left_frame: pl.LazyFrame
    right_frame: pl.LazyFrame
    on: List[str]
    how: str = "inner"
    strategy: Optional[JoinStrategy] = None
    estimated_left_rows: Optional[int] = None
    estimated_right_rows: Optional[int] = None

class JoinOptimizer:
    """Optimize join operations for FIA data."""
    
    # Small tables that should use broadcast joins
    BROADCAST_TABLES = {
        'REF_SPECIES': 1000,
        'REF_UNIT': 100,
        'REF_FOREST_TYPE': 500,
        'POP_EVAL': 10000,
    }
    
    def __init__(self, config: UnifiedEstimatorConfig):
        self.config = config
        self.join_stats: Dict[str, Any] = {}
    
    def optimize_join_sequence(self, joins: List[JoinSpec]) -> List[JoinSpec]:
        """
        Optimize the sequence of joins based on selectivity.
        
        Principles:
        1. Perform most selective joins first
        2. Apply filters before joins
        3. Use appropriate join strategies
        """
        # Estimate selectivity for each join
        for join in joins:
            join.estimated_selectivity = self._estimate_selectivity(join)
        
        # Sort by selectivity (most selective first)
        joins.sort(key=lambda j: j.estimated_selectivity)
        
        # Select appropriate strategies
        for join in joins:
            join.strategy = self._select_strategy(join)
        
        return joins
    
    def apply_filter_pushdown(self, 
                            frame: pl.LazyFrame,
                            filters: Dict[str, Any]) -> pl.LazyFrame:
        """
        Push filters down before joins.
        
        This reduces the amount of data that needs to be joined.
        """
        for col, value in filters.items():
            if col in frame.collect_schema().names():
                if isinstance(value, list):
                    frame = frame.filter(pl.col(col).is_in(value))
                else:
                    frame = frame.filter(pl.col(col) == value)
        
        return frame
    
    def optimize_tree_plot_join(self,
                               tree_data: pl.LazyFrame,
                               plot_data: pl.LazyFrame,
                               tree_filters: Optional[Dict[str, Any]] = None,
                               plot_filters: Optional[Dict[str, Any]] = None) -> pl.LazyFrame:
        """
        Optimized join between TREE and PLOT tables.
        
        This is one of the most common and expensive joins in FIA.
        """
        # Apply filters before join
        if tree_filters:
            tree_data = self.apply_filter_pushdown(tree_data, tree_filters)
        
        if plot_filters:
            plot_data = self.apply_filter_pushdown(plot_data, plot_filters)
        
        # Select only necessary columns
        tree_cols_needed = ['PLT_CN', 'TPA_UNADJ', 'DIA', 'SPCD', 'STATUSCD', 'CONDID']
        plot_cols_needed = ['CN', 'STATECD', 'COUNTYCD', 'PLOT', 'INVYR']
        
        tree_data = tree_data.select([c for c in tree_cols_needed 
                                     if c in tree_data.collect_schema().names()])
        plot_data = plot_data.select([c for c in plot_cols_needed 
                                     if c in plot_data.collect_schema().names()])
        
        # Rename CN to PLT_CN for join
        plot_data = plot_data.rename({'CN': 'PLT_CN'})
        
        # Perform optimized join
        return tree_data.join(
            plot_data,
            on='PLT_CN',
            how='inner'
        )
    
    def optimize_stratification_join(self,
                                    data: pl.LazyFrame,
                                    ppsa: pl.LazyFrame,
                                    pop_stratum: pl.LazyFrame) -> pl.LazyFrame:
        """
        Optimized stratification join pattern.
        
        This is critical for variance calculations.
        """
        # First join with PPSA (usually smaller after EVALID filtering)
        result = data.join(
            ppsa.select(['PLT_CN', 'STRATUM_CN']),
            on='PLT_CN',
            how='inner'
        )
        
        # Then join with POP_STRATUM
        # Select only needed columns to reduce memory
        pop_stratum_cols = pop_stratum.select([
            'CN', 'EXPNS', 'ADJ_FACTOR_MICR', 'ADJ_FACTOR_SUBP', 'ADJ_FACTOR_MACR'
        ]).rename({'CN': 'STRATUM_CN'})
        
        result = result.join(
            pop_stratum_cols,
            on='STRATUM_CN',
            how='inner'
        )
        
        return result
    
    def _estimate_selectivity(self, join: JoinSpec) -> float:
        """Estimate the selectivity of a join operation."""
        # Simplified estimation based on join type and data characteristics
        if join.how == 'inner':
            # Inner joins typically have high selectivity
            return 0.3
        elif join.how == 'left':
            return 0.7
        else:
            return 1.0
    
    def _select_strategy(self, join: JoinSpec) -> JoinStrategy:
        """Select the appropriate join strategy."""
        # Check if right side is a small broadcast candidate
        if join.estimated_right_rows and join.estimated_right_rows < 10000:
            return JoinStrategy.BROADCAST
        
        # Use hash join for equi-joins
        if join.how in ['inner', 'left']:
            return JoinStrategy.HASH
        
        # Default to sort-merge for other cases
        return JoinStrategy.SORT_MERGE
    
    def create_optimized_join_plan(self, 
                                  base_table: str,
                                  join_tables: List[str],
                                  filters: Dict[str, Dict[str, Any]]) -> 'JoinPlan':
        """
        Create an optimized join plan for multiple tables.
        """
        plan = JoinPlan(base_table)
        
        # Determine optimal join order based on estimated sizes
        table_sizes = self._estimate_table_sizes(join_tables, filters)
        
        # Sort tables by size (join smaller tables first)
        sorted_tables = sorted(join_tables, key=lambda t: table_sizes.get(t, float('inf')))
        
        for table in sorted_tables:
            table_filters = filters.get(table, {})
            strategy = JoinStrategy.BROADCAST if table in self.BROADCAST_TABLES else JoinStrategy.HASH
            
            plan.add_join(
                table=table,
                on=self._get_join_keys(base_table, table),
                filters=table_filters,
                strategy=strategy
            )
        
        return plan
    
    def _estimate_table_sizes(self, 
                             tables: List[str], 
                             filters: Dict[str, Dict[str, Any]]) -> Dict[str, int]:
        """Estimate table sizes after filtering."""
        sizes = {}
        
        base_sizes = {
            'TREE': 1000000,
            'PLOT': 100000,
            'COND': 200000,
            'POP_PLOT_STRATUM_ASSGN': 150000,
            'POP_STRATUM': 10000,
        }
        
        for table in tables:
            base_size = base_sizes.get(table, 50000)
            
            # Estimate reduction from filters
            if table in filters:
                selectivity = 0.5 ** len(filters[table])  # Each filter reduces by ~50%
                sizes[table] = int(base_size * selectivity)
            else:
                sizes[table] = base_size
        
        return sizes
    
    def _get_join_keys(self, left_table: str, right_table: str) -> List[str]:
        """Get the join keys for two tables."""
        join_keys = {
            ('TREE', 'PLOT'): ['PLT_CN'],
            ('TREE', 'COND'): ['PLT_CN', 'CONDID'],
            ('PLOT', 'POP_PLOT_STRATUM_ASSGN'): ['CN'],
            ('POP_PLOT_STRATUM_ASSGN', 'POP_STRATUM'): ['STRATUM_CN'],
        }
        
        key = (left_table, right_table)
        if key in join_keys:
            return join_keys[key]
        
        # Try reverse
        key = (right_table, left_table)
        if key in join_keys:
            return join_keys[key]
        
        # Default
        return ['CN']

@dataclass
class JoinPlan:
    """Execution plan for a series of joins."""
    base_table: str
    joins: List[Dict[str, Any]] = field(default_factory=list)
    
    def add_join(self, table: str, on: List[str], 
                filters: Optional[Dict[str, Any]] = None,
                strategy: JoinStrategy = JoinStrategy.HASH):
        """Add a join to the plan."""
        self.joins.append({
            'table': table,
            'on': on,
            'filters': filters or {},
            'strategy': strategy
        })
    
    def execute(self, db, lazy: bool = True) -> pl.LazyFrame:
        """Execute the join plan."""
        # Load base table
        if self.base_table not in db.tables:
            db.load_table(self.base_table)
        
        result = db.tables[self.base_table]
        if isinstance(result, pl.DataFrame) and lazy:
            result = result.lazy()
        
        # Execute joins in order
        for join_spec in self.joins:
            # Load join table
            if join_spec['table'] not in db.tables:
                db.load_table(join_spec['table'])
            
            right = db.tables[join_spec['table']]
            if isinstance(right, pl.DataFrame) and lazy:
                right = right.lazy()
            
            # Apply filters to right table
            for col, value in join_spec['filters'].items():
                if isinstance(value, list):
                    right = right.filter(pl.col(col).is_in(value))
                else:
                    right = right.filter(pl.col(col) == value)
            
            # Perform join
            result = result.join(
                right,
                on=join_spec['on'],
                how='inner'
            )
        
        return result
```

## Migration Path and Backward Compatibility

### Phase 1: Parallel Implementation (Weeks 1-2)

1. **Implement UnifiedEstimatorConfig** alongside existing configs
2. **Add adapter functions** for seamless conversion
3. **Update LazyBaseEstimator** to use unified config internally
4. **Test with existing estimators** without modification

### Phase 2: Query Builder Integration (Weeks 2-3)

1. **Implement query builders** as standalone components
2. **Add to LazyBaseEstimator** as optional optimization
3. **Benchmark performance** improvements
4. **Document query patterns** for each estimator

### Phase 3: Join Optimization (Weeks 3-4)

1. **Implement JoinOptimizer** class
2. **Integrate with query builders**
3. **Update estimators** to use optimized joins
4. **Performance validation** with large datasets

### Phase 4: Gradual Migration (Weeks 4-6)

1. **Update estimators one by one** to use new architecture
2. **Maintain backward compatibility** through adapters
3. **Add deprecation warnings** for old patterns
4. **Update documentation** with migration guides

## Implementation Priorities and Dependencies

### Critical Path Items

1. **UnifiedEstimatorConfig** (Week 1)
   - No dependencies
   - Enables all other improvements
   - Must maintain backward compatibility

2. **StratificationQueryBuilder** (Week 2)
   - Depends on: UnifiedEstimatorConfig
   - Used by: All estimators
   - High performance impact

3. **JoinOptimizer for Tree-Plot** (Week 2)
   - Depends on: Query builders
   - Used by: Most estimators
   - Highest performance impact

### Parallel Work Streams

**Stream 1: Configuration**
- UnifiedEstimatorConfig implementation
- Config adapters and validators
- Documentation updates

**Stream 2: Query Optimization**
- Query builder framework
- Specialized builders for each table
- Query plan caching

**Stream 3: Join Optimization**
- JoinOptimizer implementation
- Filter push-down logic
- Join strategy selection

## Success Metrics

### Performance Metrics

1. **Query Performance**
   - 30-50% reduction in query execution time
   - 40-60% reduction in memory usage for joins
   - 10x improvement for cached query patterns

2. **Configuration Overhead**
   - < 1ms overhead for config validation
   - Zero runtime overhead for backward compatibility

3. **Join Optimization**
   - 50% reduction in data shuffled during joins
   - 2-3x speedup for multi-table joins

### Code Quality Metrics

1. **Reduced Duplication**
   - Single configuration system
   - Reusable query patterns
   - Centralized join logic

2. **Improved Maintainability**
   - Clear separation of concerns
   - Well-defined interfaces
   - Comprehensive test coverage

3. **Enhanced Developer Experience**
   - Type-safe configuration
   - Better error messages
   - Clearer debugging information

## Risk Mitigation

### Technical Risks

1. **Backward Compatibility Breaking**
   - Mitigation: Comprehensive adapter layer
   - Testing: Full regression test suite
   - Rollback: Feature flags for new system

2. **Performance Regression**
   - Mitigation: Benchmark before/after
   - Testing: Performance test suite
   - Rollback: Keep old implementation available

3. **Complex Migration**
   - Mitigation: Gradual, estimator-by-estimator migration
   - Testing: Parallel testing of old/new
   - Documentation: Detailed migration guides

### Schedule Risks

1. **Dependency on Phase 2.5 Completion**
   - Mitigation: Can start config work in parallel
   - Testing: Mock lazy evaluation for testing

2. **Integration Complexity**
   - Mitigation: Modular design allows independent testing
   - Testing: Integration tests for each component

## Conclusion

Phase 3 provides the architectural improvements necessary to support the Pipeline Framework in Phase 4. The unified configuration system eliminates confusion and duplication, the query builder framework enables reusable and optimized query patterns, and the join optimizer significantly improves performance for complex multi-table operations.

The design maintains full backward compatibility while providing a clear migration path to the improved architecture. By building on the lazy evaluation infrastructure from Phase 2, these improvements will deliver substantial performance gains and improved maintainability for the pyFIA estimation module.

## Appendix: Example Usage

### Using the Unified Configuration

```python
# Old way (still works)
from pyfia.estimation.base import EstimatorConfig

old_config = EstimatorConfig(
    by_species=True,
    tree_domain="DIA > 10",
    method="TI"
)

# New way with validation
from pyfia.estimation.config_v3 import UnifiedEstimatorConfig

new_config = UnifiedEstimatorConfig(
    by_species=True,
    tree_domain="DIA > 10",
    temporal=TemporalConfig(method="TI"),
    lazy=LazyEvaluationConfig(enabled=True, show_progress=True)
)

# Automatic conversion
unified = adapt_config(old_config)  # Works with either type

# Use in estimator
estimator = VolumeEstimator(db, unified)
```

### Using Query Builders

```python
# Create optimized stratification query
strat_builder = StratificationQueryBuilder(
    db, 
    evalids=[231001, 231002]
)
strat_builder.add_required_columns(['INVYR', 'STATECD'])
strat_data = strat_builder.with_pop_stratum()

# Create optimized tree query with conditions
tree_builder = TreeQueryBuilder(
    db,
    filters={'STATUSCD': 1}
)
tree_with_cond = tree_builder.with_conditions(
    condition_filters={'COND_STATUS_CD': 1}
)

# Results are lazy frames ready for further operations
result = tree_with_cond.join(strat_data, on='PLT_CN')
```

### Using Join Optimizer

```python
# Create optimizer
optimizer = JoinOptimizer(config)

# Optimize a complex join sequence
plan = optimizer.create_optimized_join_plan(
    base_table='TREE',
    join_tables=['PLOT', 'COND', 'POP_PLOT_STRATUM_ASSGN'],
    filters={
        'TREE': {'STATUSCD': 1},
        'COND': {'COND_STATUS_CD': 1}
    }
)

# Execute optimized plan
result = plan.execute(db, lazy=True)

# Or optimize specific join pattern
optimized = optimizer.optimize_tree_plot_join(
    tree_data=tree_lazy,
    plot_data=plot_lazy,
    tree_filters={'DIA': [10, 20, 30]},
    plot_filters={'INVYR': 2023}
)
```