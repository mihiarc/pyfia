"""
Unified join system for pyFIA with automatic optimization.

This module provides a centralized, optimized joining system that replaces
all direct .join() calls throughout the pyFIA codebase. It automatically
optimizes join operations based on table statistics, FIA-specific patterns,
and runtime performance metrics.

Key Features:
- Automatic join order optimization
- Filter push-down before joins
- Strategy selection (hash, broadcast, sort-merge)
- FIA-specific optimizations
- Caching and statistics tracking
- Integration with lazy evaluation
"""

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Dict, List, Optional, Tuple, Union, Any, Set
import logging
import time
from functools import lru_cache, wraps
import hashlib

import polars as pl
import numpy as np

from .evaluation import FrameWrapper
from .caching import CacheKey, MemoryCache
from .config import EstimatorConfig

logger = logging.getLogger(__name__)


# === Join Types and Strategies ===

class JoinType(Enum):
    """Types of joins supported."""
    INNER = "inner"
    LEFT = "left"
    RIGHT = "right"
    OUTER = "outer"
    SEMI = "semi"
    ANTI = "anti"


class JoinStrategy(Enum):
    """Join execution strategies."""
    HASH = "hash"
    BROADCAST = "broadcast"
    SORT_MERGE = "sort_merge"
    AUTO = "auto"


# === Statistics and Metadata ===

@dataclass
class TableStatistics:
    """Statistics for a table used in join optimization."""
    name: str
    row_count: int
    size_bytes: Optional[int] = None
    key_cardinality: Dict[str, int] = field(default_factory=dict)
    null_ratios: Dict[str, float] = field(default_factory=dict)
    is_sorted: Dict[str, bool] = field(default_factory=dict)
    last_updated: Optional[float] = None
    
    @property
    def is_small(self) -> bool:
        """Check if table is small enough for broadcast join."""
        # Tables under 10MB or 50k rows are considered small
        if self.size_bytes and self.size_bytes < 10_000_000:
            return True
        return self.row_count < 50_000
    
    def estimate_join_selectivity(self, other: 'TableStatistics', join_key: str) -> float:
        """Estimate selectivity of join with another table."""
        if join_key in self.key_cardinality and join_key in other.key_cardinality:
            # Use cardinality information
            self_card = self.key_cardinality[join_key]
            other_card = other.key_cardinality[join_key]
            return min(self_card, other_card) / max(self_card, other_card)
        # Default conservative estimate
        return 0.5


@dataclass 
class JoinPlan:
    """Execution plan for a join operation."""
    left_table: str
    right_table: str
    join_keys: Union[str, List[str]]
    join_type: JoinType
    strategy: JoinStrategy
    filters_to_push: Dict[str, List[pl.Expr]] = field(default_factory=dict)
    estimated_cost: float = 0.0
    estimated_rows: int = 0
    
    def __str__(self) -> str:
        return (f"JoinPlan({self.left_table} ⋈ {self.right_table} "
                f"on {self.join_keys}, strategy={self.strategy.value})")


# === FIA-Specific Knowledge ===

class FIATableInfo:
    """FIA-specific table metadata and relationships."""
    
    # Typical cardinalities for FIA tables
    CARDINALITIES = {
        "TREE": 1_000_000,
        "PLOT": 100_000,
        "COND": 150_000,
        "SEEDLING": 500_000,
        "POP_PLOT_STRATUM_ASSGN": 100_000,
        "POP_STRATUM": 5_000,
        "POP_ESTN_UNIT": 500,
        "POP_EVAL": 100,
        "REF_SPECIES": 1_000,
        "REF_FOREST_TYPE": 200,
        "REF_UNIT": 100,
    }
    
    # Tables small enough for broadcast joins
    BROADCAST_TABLES = {
        "POP_STRATUM", "POP_ESTN_UNIT", "POP_EVAL",
        "REF_SPECIES", "REF_FOREST_TYPE", "REF_UNIT",
        "REF_POP_ATTRIBUTE", "REF_STATE_ELEV"
    }
    
    # Common join patterns and their optimal order
    JOIN_PATTERNS = {
        "tree_analysis": ["PLOT", "COND", "TREE"],  # Plot first reduces intermediate size
        "stratification": ["POP_PLOT_STRATUM_ASSGN", "POP_STRATUM", "PLOT"],
        "area_estimation": ["PLOT", "COND", "POP_PLOT_STRATUM_ASSGN", "POP_STRATUM"],
        "tree_species": ["TREE", "REF_SPECIES"],  # Broadcast REF_SPECIES
    }
    
    # Join key relationships
    JOIN_KEYS = {
        ("TREE", "PLOT"): ["PLT_CN"],
        ("TREE", "COND"): ["PLT_CN", "CONDID"],
        ("PLOT", "COND"): ["PLT_CN"],
        ("PLOT", "POP_PLOT_STRATUM_ASSGN"): ["CN", "PLT_CN"],
        ("POP_PLOT_STRATUM_ASSGN", "POP_STRATUM"): ["STRATUM_CN", "CN"],
        ("POP_STRATUM", "POP_ESTN_UNIT"): ["ESTN_UNIT_CN", "CN"],
        ("TREE", "REF_SPECIES"): ["SPCD"],
        ("COND", "REF_FOREST_TYPE"): ["FORTYPCD"],
    }
    
    @classmethod
    def get_join_keys(cls, table1: str, table2: str) -> Optional[List[str]]:
        """Get standard join keys between two tables."""
        key = (table1, table2)
        if key in cls.JOIN_KEYS:
            return cls.JOIN_KEYS[key]
        # Try reverse order
        key_rev = (table2, table1)
        if key_rev in cls.JOIN_KEYS:
            return cls.JOIN_KEYS[key_rev]
        return None
    
    @classmethod
    def should_broadcast(cls, table_name: str, row_count: Optional[int] = None) -> bool:
        """Determine if table should use broadcast join."""
        if table_name in cls.BROADCAST_TABLES:
            return True
        if row_count and row_count < 10_000:
            return True
        return False


# === Join Optimization Engine ===

class JoinOptimizer:
    """Optimizes join operations based on statistics and patterns."""
    
    def __init__(self, config: Optional[EstimatorConfig] = None):
        self.config = config or EstimatorConfig()
        self.statistics_cache: Dict[str, TableStatistics] = {}
        self.execution_history: List[Dict[str, Any]] = []
        
    def optimize_join(
        self,
        left_table: str,
        right_table: str,
        join_keys: Union[str, List[str]],
        join_type: JoinType = JoinType.INNER,
        left_stats: Optional[TableStatistics] = None,
        right_stats: Optional[TableStatistics] = None,
        filters: Optional[Dict[str, List[pl.Expr]]] = None
    ) -> JoinPlan:
        """
        Optimize a single join operation.
        
        Parameters
        ----------
        left_table : str
            Name of left table
        right_table : str
            Name of right table
        join_keys : Union[str, List[str]]
            Join key column(s)
        join_type : JoinType
            Type of join operation
        left_stats : Optional[TableStatistics]
            Statistics for left table
        right_stats : Optional[TableStatistics]
            Statistics for right table
        filters : Optional[Dict[str, List[pl.Expr]]]
            Filters to potentially push down
            
        Returns
        -------
        JoinPlan
            Optimized join execution plan
        """
        # Ensure join_keys is a list
        if isinstance(join_keys, str):
            join_keys = [join_keys]
        
        # Get statistics if not provided
        if not left_stats:
            left_stats = self._get_table_stats(left_table)
        if not right_stats:
            right_stats = self._get_table_stats(right_table)
        
        # Determine join strategy
        strategy = self._select_strategy(
            left_stats, right_stats, join_type, join_keys
        )
        
        # Estimate join cost and output size
        estimated_cost = self._estimate_cost(
            left_stats, right_stats, strategy, join_keys
        )
        estimated_rows = self._estimate_output_rows(
            left_stats, right_stats, join_type, join_keys
        )
        
        # Determine filter push-down
        filters_to_push = {}
        if filters:
            filters_to_push = self._analyze_filter_pushdown(
                filters, left_table, right_table, join_keys
            )
        
        return JoinPlan(
            left_table=left_table,
            right_table=right_table,
            join_keys=join_keys,
            join_type=join_type,
            strategy=strategy,
            filters_to_push=filters_to_push,
            estimated_cost=estimated_cost,
            estimated_rows=estimated_rows
        )
    
    def optimize_multi_join(
        self,
        tables: List[str],
        join_specs: Dict[Tuple[str, str], Union[str, List[str]]],
        table_stats: Optional[Dict[str, TableStatistics]] = None
    ) -> List[JoinPlan]:
        """
        Optimize a sequence of joins between multiple tables.
        
        Parameters
        ----------
        tables : List[str]
            List of table names to join
        join_specs : Dict[Tuple[str, str], Union[str, List[str]]]
            Join specifications as {(table1, table2): join_keys}
        table_stats : Optional[Dict[str, TableStatistics]]
            Statistics for tables
            
        Returns
        -------
        List[JoinPlan]
            Optimized sequence of join plans
        """
        if len(tables) < 2:
            return []
        
        # Check for known FIA patterns
        pattern_order = self._check_fia_pattern(tables)
        if pattern_order:
            tables = pattern_order
        
        # Build join sequence (for now, left-deep tree)
        plans = []
        result_table = tables[0]
        result_stats = self._get_table_stats(result_table)
        
        for next_table in tables[1:]:
            # Find join keys
            join_keys = None
            for (t1, t2), keys in join_specs.items():
                if (t1 == result_table and t2 == next_table) or \
                   (t2 == result_table and t1 == next_table):
                    join_keys = keys if isinstance(keys, list) else [keys]
                    break
            
            if not join_keys:
                # Try FIA standard keys
                join_keys = FIATableInfo.get_join_keys(result_table, next_table)
                if not join_keys:
                    raise ValueError(f"No join keys found for {result_table} and {next_table}")
            
            # Optimize this join
            next_stats = self._get_table_stats(next_table)
            plan = self.optimize_join(
                result_table, next_table, join_keys,
                left_stats=result_stats, right_stats=next_stats
            )
            plans.append(plan)
            
            # Update result for next iteration
            result_table = f"{result_table}_{next_table}"
            result_stats = TableStatistics(
                name=result_table,
                row_count=plan.estimated_rows
            )
        
        return plans
    
    def _select_strategy(
        self,
        left_stats: TableStatistics,
        right_stats: TableStatistics,
        join_type: JoinType,
        join_keys: List[str]
    ) -> JoinStrategy:
        """Select optimal join strategy based on statistics."""
        # Broadcast join for small tables
        if right_stats.is_small or FIATableInfo.should_broadcast(right_stats.name):
            return JoinStrategy.BROADCAST
        
        if left_stats.is_small or FIATableInfo.should_broadcast(left_stats.name):
            # Swap sides for broadcast
            return JoinStrategy.BROADCAST
        
        # Sort-merge for sorted data
        if join_keys and all(
            left_stats.is_sorted.get(k, False) and 
            right_stats.is_sorted.get(k, False)
            for k in join_keys
        ):
            return JoinStrategy.SORT_MERGE
        
        # Default to hash join
        return JoinStrategy.HASH
    
    def _estimate_cost(
        self,
        left_stats: TableStatistics,
        right_stats: TableStatistics,
        strategy: JoinStrategy,
        join_keys: List[str]
    ) -> float:
        """Estimate join execution cost."""
        left_rows = left_stats.row_count
        right_rows = right_stats.row_count
        
        if strategy == JoinStrategy.BROADCAST:
            # Cost of broadcasting smaller side + probe cost
            broadcast_cost = min(left_rows, right_rows) * 2
            probe_cost = max(left_rows, right_rows) * 0.5
            return broadcast_cost + probe_cost
        
        elif strategy == JoinStrategy.SORT_MERGE:
            # Cost of sorting (if needed) + merge
            sort_cost = 0
            if not all(left_stats.is_sorted.get(k, False) for k in join_keys):
                sort_cost += left_rows * np.log2(left_rows)
            if not all(right_stats.is_sorted.get(k, False) for k in join_keys):
                sort_cost += right_rows * np.log2(right_rows)
            merge_cost = left_rows + right_rows
            return sort_cost + merge_cost
        
        else:  # HASH
            # Cost of building hash table + probe
            build_cost = min(left_rows, right_rows) * 1.5
            probe_cost = max(left_rows, right_rows) * 0.8
            return build_cost + probe_cost
    
    def _estimate_output_rows(
        self,
        left_stats: TableStatistics,
        right_stats: TableStatistics,
        join_type: JoinType,
        join_keys: List[str]
    ) -> int:
        """Estimate number of output rows."""
        selectivity = 0.5  # Default
        
        # Try to estimate selectivity from statistics
        if join_keys and len(join_keys) == 1:
            key = join_keys[0]
            selectivity = left_stats.estimate_join_selectivity(right_stats, key)
        
        if join_type == JoinType.INNER:
            return int(min(left_stats.row_count, right_stats.row_count) * selectivity)
        elif join_type == JoinType.LEFT:
            return left_stats.row_count
        elif join_type == JoinType.RIGHT:
            return right_stats.row_count
        elif join_type == JoinType.OUTER:
            return left_stats.row_count + right_stats.row_count
        else:
            return int(left_stats.row_count * selectivity)
    
    def _analyze_filter_pushdown(
        self,
        filters: Dict[str, List[pl.Expr]],
        left_table: str,
        right_table: str,
        join_keys: List[str]
    ) -> Dict[str, List[pl.Expr]]:
        """Analyze which filters can be pushed down before join."""
        pushable = {}
        
        for table, table_filters in filters.items():
            if table == left_table or table == right_table:
                # Can push down filters that only reference the table's columns
                pushable[table] = table_filters
        
        return pushable
    
    def _get_table_stats(self, table_name: str) -> TableStatistics:
        """Get or estimate table statistics."""
        if table_name in self.statistics_cache:
            return self.statistics_cache[table_name]
        
        # Use FIA knowledge for estimation
        estimated_rows = FIATableInfo.CARDINALITIES.get(table_name, 50_000)
        stats = TableStatistics(
            name=table_name,
            row_count=estimated_rows,
            last_updated=time.time()
        )
        
        self.statistics_cache[table_name] = stats
        return stats
    
    def _check_fia_pattern(self, tables: List[str]) -> Optional[List[str]]:
        """Check if tables match a known FIA pattern."""
        table_set = set(tables)
        
        for pattern_name, pattern_tables in FIATableInfo.JOIN_PATTERNS.items():
            if set(pattern_tables) == table_set:
                return pattern_tables
        
        return None
    
    def update_statistics(self, table_name: str, stats: TableStatistics):
        """Update cached statistics for a table."""
        self.statistics_cache[table_name] = stats
        stats.last_updated = time.time()


# === Main Join Manager ===

class JoinManager:
    """
    Central manager for all join operations in pyFIA.
    
    This class provides high-level join functions with automatic optimization,
    caching, and monitoring. It replaces all direct .join() calls throughout
    the codebase.
    """
    
    def __init__(
        self,
        config: Optional[EstimatorConfig] = None,
        cache: Optional[MemoryCache] = None,
        enable_optimization: bool = True,
        enable_caching: bool = True,
        collect_statistics: bool = True
    ):
        """
        Initialize join manager.
        
        Parameters
        ----------
        config : Optional[EstimatorConfig]
            Configuration for estimation
        cache : Optional[MemoryCache]
            Cache for join results
        enable_optimization : bool
            Whether to enable join optimization
        enable_caching : bool
            Whether to cache join results
        collect_statistics : bool
            Whether to collect runtime statistics
        """
        self.config = config or EstimatorConfig()
        self.cache = cache or MemoryCache(max_size_mb=512)
        self.optimizer = JoinOptimizer(config) if enable_optimization else None
        self.enable_caching = enable_caching
        self.collect_statistics = collect_statistics
        
        # Performance tracking
        self.stats = {
            "total_joins": 0,
            "optimized_joins": 0,
            "cache_hits": 0,
            "total_time": 0.0,
            "rows_processed": 0
        }
    
    # === Core Join Method ===
    
    def join(
        self,
        left: Union[pl.DataFrame, pl.LazyFrame, FrameWrapper],
        right: Union[pl.DataFrame, pl.LazyFrame, FrameWrapper],
        on: Union[str, List[str], None] = None,
        left_on: Union[str, List[str], None] = None,
        right_on: Union[str, List[str], None] = None,
        how: str = "inner",
        suffix: str = "_right",
        optimize: bool = True,
        left_name: Optional[str] = None,
        right_name: Optional[str] = None
    ) -> FrameWrapper:
        """
        Perform an optimized join operation.
        
        This is the main entry point that replaces all direct .join() calls.
        
        Parameters
        ----------
        left : Union[pl.DataFrame, pl.LazyFrame, FrameWrapper]
            Left side of join
        right : Union[pl.DataFrame, pl.LazyFrame, FrameWrapper]
            Right side of join
        on : Union[str, List[str], None]
            Column(s) to join on (same in both tables)
        left_on : Union[str, List[str], None]
            Left join column(s) if different
        right_on : Union[str, List[str], None]
            Right join column(s) if different
        how : str
            Join type: "inner", "left", "right", "outer", "semi", "anti"
        suffix : str
            Suffix for overlapping columns
        optimize : bool
            Whether to apply optimization
        left_name : Optional[str]
            Name of left table for optimization
        right_name : Optional[str]
            Name of right table for optimization
            
        Returns
        -------
        FrameWrapper
            Join result as lazy frame wrapper
        """
        start_time = time.time()
        self.stats["total_joins"] += 1
        
        # Convert inputs to lazy frames
        left_lazy = self._to_lazy(left)
        right_lazy = self._to_lazy(right)
        
        # Determine join keys
        if on is not None:
            left_on = right_on = on if isinstance(on, list) else [on]
        elif left_on is not None and right_on is not None:
            left_on = left_on if isinstance(left_on, list) else [left_on]
            right_on = right_on if isinstance(right_on, list) else [right_on]
        else:
            raise ValueError("Must specify either 'on' or both 'left_on' and 'right_on'")
        
        # Check cache
        cache_key = self._make_cache_key(left_lazy, right_lazy, left_on, right_on, how)
        if self.enable_caching and cache_key:
            cached = self.cache.get(cache_key)
            if cached is not None:
                self.stats["cache_hits"] += 1
                return FrameWrapper(cached)
        
        # Optimize if enabled
        if optimize and self.optimizer and left_name and right_name:
            result_lazy = self._optimized_join(
                left_lazy, right_lazy,
                left_on, right_on,
                how, suffix,
                left_name, right_name
            )
            self.stats["optimized_joins"] += 1
        else:
            # Direct join without optimization
            result_lazy = left_lazy.join(
                right_lazy,
                left_on=left_on,
                right_on=right_on,
                how=how,
                suffix=suffix
            )
        
        # Cache result
        if self.enable_caching and cache_key:
            self.cache.put(cache_key, result_lazy)
        
        # Track statistics
        if self.collect_statistics:
            self.stats["total_time"] += time.time() - start_time
        
        return FrameWrapper(result_lazy)
    
    # === FIA-Specific Join Functions ===
    
    def join_tree_plot(
        self,
        tree_df: Union[pl.DataFrame, pl.LazyFrame, FrameWrapper],
        plot_df: Union[pl.DataFrame, pl.LazyFrame, FrameWrapper],
        how: str = "inner"
    ) -> FrameWrapper:
        """
        Join tree and plot tables.
        
        Optimized for the common TREE ⋈ PLOT pattern.
        """
        return self.join(
            tree_df, plot_df,
            left_on="PLT_CN", right_on="CN",
            how=how,
            left_name="TREE", right_name="PLOT"
        )
    
    def join_tree_condition(
        self,
        tree_df: Union[pl.DataFrame, pl.LazyFrame, FrameWrapper],
        cond_df: Union[pl.DataFrame, pl.LazyFrame, FrameWrapper],
        how: str = "inner"
    ) -> FrameWrapper:
        """
        Join tree and condition tables.
        
        Optimized for TREE ⋈ COND on PLT_CN and CONDID.
        """
        return self.join(
            tree_df, cond_df,
            on=["PLT_CN", "CONDID"],
            how=how,
            left_name="TREE", right_name="COND"
        )
    
    def join_plot_condition(
        self,
        plot_df: Union[pl.DataFrame, pl.LazyFrame, FrameWrapper],
        cond_df: Union[pl.DataFrame, pl.LazyFrame, FrameWrapper],
        how: str = "inner"
    ) -> FrameWrapper:
        """
        Join plot and condition tables.
        
        Optimized for PLOT ⋈ COND pattern.
        """
        return self.join(
            plot_df, cond_df,
            left_on="CN", right_on="PLT_CN",
            how=how,
            left_name="PLOT", right_name="COND"
        )
    
    def join_stratification(
        self,
        data_df: Union[pl.DataFrame, pl.LazyFrame, FrameWrapper],
        ppsa_df: Union[pl.DataFrame, pl.LazyFrame, FrameWrapper],
        pop_stratum_df: Union[pl.DataFrame, pl.LazyFrame, FrameWrapper],
        data_name: str = "PLOT"
    ) -> FrameWrapper:
        """
        Join data with stratification tables.
        
        Performs the common pattern:
        DATA ⋈ POP_PLOT_STRATUM_ASSGN ⋈ POP_STRATUM
        """
        # First join with assignment table
        with_assgn = self.join(
            data_df, ppsa_df,
            left_on="PLT_CN" if data_name == "TREE" else "CN",
            right_on="PLT_CN",
            how="left",
            left_name=data_name,
            right_name="POP_PLOT_STRATUM_ASSGN"
        )
        
        # Then join with stratum table (broadcast)
        with_stratum = self.join(
            with_assgn, pop_stratum_df,
            left_on="STRATUM_CN",
            right_on="CN",
            how="left",
            left_name=f"{data_name}_PPSA",
            right_name="POP_STRATUM"
        )
        
        return with_stratum
    
    def join_reference(
        self,
        data_df: Union[pl.DataFrame, pl.LazyFrame, FrameWrapper],
        ref_df: Union[pl.DataFrame, pl.LazyFrame, FrameWrapper],
        data_col: str,
        ref_col: str,
        data_name: str,
        ref_name: str
    ) -> FrameWrapper:
        """
        Join with reference table.
        
        Reference tables are always broadcast joined.
        """
        return self.join(
            data_df, ref_df,
            left_on=data_col,
            right_on=ref_col,
            how="left",
            left_name=data_name,
            right_name=ref_name
        )
    
    def join_multi(
        self,
        tables: Dict[str, Union[pl.DataFrame, pl.LazyFrame, FrameWrapper]],
        join_sequence: List[Tuple[str, str, Union[str, List[str]]]],
        optimize_order: bool = True
    ) -> FrameWrapper:
        """
        Perform multiple joins in sequence.
        
        Parameters
        ----------
        tables : Dict[str, Union[pl.DataFrame, pl.LazyFrame, FrameWrapper]]
            Dictionary of table name to dataframe
        join_sequence : List[Tuple[str, str, Union[str, List[str]]]]
            Sequence of (left_table, right_table, join_keys)
        optimize_order : bool
            Whether to optimize join order
            
        Returns
        -------
        FrameWrapper
            Result of all joins
        """
        if not join_sequence:
            raise ValueError("No joins specified")
        
        # Optimize join order if requested
        if optimize_order and self.optimizer:
            table_names = list(tables.keys())
            join_specs = {
                (left, right): keys
                for left, right, keys in join_sequence
            }
            plans = self.optimizer.optimize_multi_join(table_names, join_specs)
            
            # Execute optimized plan
            result = None
            for plan in plans:
                if result is None:
                    result = tables[plan.left_table]
                
                result = self.join(
                    result,
                    tables[plan.right_table],
                    on=plan.join_keys,
                    how=plan.join_type.value,
                    left_name=plan.left_table,
                    right_name=plan.right_table
                )
        else:
            # Execute joins in given order
            result = None
            for left_name, right_name, join_keys in join_sequence:
                if result is None:
                    result = tables[left_name]
                
                result = self.join(
                    result,
                    tables[right_name],
                    on=join_keys,
                    left_name=left_name,
                    right_name=right_name
                )
        
        return result
    
    # === Helper Methods ===
    
    def _to_lazy(
        self,
        df: Union[pl.DataFrame, pl.LazyFrame, FrameWrapper]
    ) -> pl.LazyFrame:
        """Convert input to lazy frame."""
        if isinstance(df, FrameWrapper):
            return df.frame
        elif isinstance(df, pl.DataFrame):
            return df.lazy()
        else:
            return df
    
    def _optimized_join(
        self,
        left_lazy: pl.LazyFrame,
        right_lazy: pl.LazyFrame,
        left_on: List[str],
        right_on: List[str],
        how: str,
        suffix: str,
        left_name: str,
        right_name: str
    ) -> pl.LazyFrame:
        """Execute optimized join based on plan."""
        # Get optimization plan
        join_type = JoinType(how)
        plan = self.optimizer.optimize_join(
            left_name, right_name,
            list(zip(left_on, right_on)) if len(left_on) == len(right_on) else left_on,
            join_type
        )
        
        # Apply filter push-down
        if plan.filters_to_push:
            if left_name in plan.filters_to_push:
                for filter_expr in plan.filters_to_push[left_name]:
                    left_lazy = left_lazy.filter(filter_expr)
            if right_name in plan.filters_to_push:
                for filter_expr in plan.filters_to_push[right_name]:
                    right_lazy = right_lazy.filter(filter_expr)
        
        # Execute with selected strategy
        if plan.strategy == JoinStrategy.BROADCAST:
            # Determine which side to broadcast
            if FIATableInfo.should_broadcast(right_name):
                # Collect right side for broadcast
                right_df = right_lazy.collect()
                result = left_lazy.join(
                    right_df.lazy(),
                    left_on=left_on,
                    right_on=right_on,
                    how=how,
                    suffix=suffix
                )
            else:
                # Collect left side for broadcast
                left_df = left_lazy.collect()
                result = left_df.lazy().join(
                    right_lazy,
                    left_on=left_on,
                    right_on=right_on,
                    how=how,
                    suffix=suffix
                )
        else:
            # Standard join (hash or sort-merge handled by Polars)
            result = left_lazy.join(
                right_lazy,
                left_on=left_on,
                right_on=right_on,
                how=how,
                suffix=suffix
            )
        
        return result
    
    def _make_cache_key(
        self,
        left: pl.LazyFrame,
        right: pl.LazyFrame,
        left_on: List[str],
        right_on: List[str],
        how: str
    ) -> Optional[str]:
        """Generate cache key for join result."""
        if not self.enable_caching:
            return None
        
        try:
            # Create a deterministic key from join parameters
            key_parts = [
                str(left.collect_schema()),
                str(right.collect_schema()),
                ",".join(left_on),
                ",".join(right_on),
                how
            ]
            key_str = "|".join(key_parts)
            return hashlib.md5(key_str.encode()).hexdigest()
        except:
            return None
    
    def update_table_statistics(
        self,
        table_name: str,
        df: Union[pl.DataFrame, pl.LazyFrame]
    ):
        """Update statistics for a table."""
        if not self.optimizer:
            return
        
        if isinstance(df, pl.LazyFrame):
            # Collect basic statistics without materializing full data
            stats_df = df.select([
                pl.count().alias("row_count")
            ]).collect()
            row_count = stats_df["row_count"][0]
        else:
            row_count = len(df)
        
        stats = TableStatistics(
            name=table_name,
            row_count=row_count,
            last_updated=time.time()
        )
        
        self.optimizer.update_statistics(table_name, stats)
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get join manager statistics."""
        stats = self.stats.copy()
        if self.optimizer:
            stats["optimizer_stats"] = {
                "cached_tables": len(self.optimizer.statistics_cache),
                "execution_history": len(self.optimizer.execution_history)
            }
        return stats
    
    def reset_statistics(self):
        """Reset statistics counters."""
        self.stats = {
            "total_joins": 0,
            "optimized_joins": 0,
            "cache_hits": 0,
            "total_time": 0.0,
            "rows_processed": 0
        }
    
    def explain_join(
        self,
        left_name: str,
        right_name: str,
        join_keys: Union[str, List[str]],
        join_type: str = "inner"
    ) -> str:
        """
        Explain how a join would be optimized.
        
        Useful for debugging and understanding optimization decisions.
        """
        if not self.optimizer:
            return "Optimization disabled"
        
        plan = self.optimizer.optimize_join(
            left_name, right_name,
            join_keys,
            JoinType(join_type)
        )
        
        explanation = [
            f"Join Plan: {left_name} ⋈ {right_name}",
            f"  Join keys: {plan.join_keys}",
            f"  Strategy: {plan.strategy.value}",
            f"  Estimated cost: {plan.estimated_cost:.2f}",
            f"  Estimated rows: {plan.estimated_rows:,}",
        ]
        
        if plan.filters_to_push:
            explanation.append("  Filters to push:")
            for table, filters in plan.filters_to_push.items():
                explanation.append(f"    {table}: {len(filters)} filters")
        
        return "\n".join(explanation)


# === Convenience Functions ===

# Global join manager instance (can be overridden)
_global_join_manager = None


def get_join_manager(config: Optional[EstimatorConfig] = None) -> JoinManager:
    """Get or create global join manager instance."""
    global _global_join_manager
    if _global_join_manager is None:
        _global_join_manager = JoinManager(config)
    return _global_join_manager


def optimized_join(
    left: Union[pl.DataFrame, pl.LazyFrame],
    right: Union[pl.DataFrame, pl.LazyFrame],
    **kwargs
) -> FrameWrapper:
    """Convenience function for optimized join using global manager."""
    manager = get_join_manager()
    return manager.join(left, right, **kwargs)


# Export main classes and functions
__all__ = [
    "JoinManager",
    "JoinOptimizer",
    "JoinPlan",
    "JoinType",
    "JoinStrategy",
    "TableStatistics",
    "FIATableInfo",
    "get_join_manager",
    "optimized_join"
]