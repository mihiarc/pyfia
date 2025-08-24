"""
Join optimizer for pyFIA Phase 3.

This module provides comprehensive join optimization including join order
optimization, filter push-down, join strategy selection, and FIA-specific
optimizations for common join patterns.

Key features:
- Join order optimization based on selectivity and cardinality
- Filter push-down to reduce intermediate data size
- Join strategy selection (hash, sort-merge, broadcast)
- FIA-specific optimizations for common patterns
- Integration with lazy evaluation framework
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum, auto
from functools import lru_cache
from typing import Any, Dict, List, Optional, Set, Tuple, Union
import hashlib
import json
import logging
from collections import defaultdict

import polars as pl
import numpy as np
from pydantic import BaseModel, Field, field_validator

from .query_builders import (
    QueryPlan, QueryJoin, QueryFilter, QueryColumn,
    JoinStrategy, BaseQueryBuilder
)
from .lazy_evaluation import LazyFrameWrapper, LazyComputationNode, ComputationStatus
from .caching import CacheKey, MemoryCache
from .config import EstimatorConfig


logger = logging.getLogger(__name__)


# === Join Types and Metadata ===

class JoinType(Enum):
    """Types of joins supported by the optimizer."""
    INNER = "inner"
    LEFT = "left"
    RIGHT = "right"
    FULL = "outer"
    CROSS = "cross"
    SEMI = "semi"
    ANTI = "anti"


@dataclass
class JoinStatistics:
    """Statistics for a join operation."""
    
    left_cardinality: int
    right_cardinality: int
    estimated_output_rows: int
    selectivity: float
    key_uniqueness_left: float  # 0.0 to 1.0
    key_uniqueness_right: float
    null_ratio_left: float
    null_ratio_right: float
    
    @property
    def is_one_to_one(self) -> bool:
        """Check if join is likely one-to-one."""
        return self.key_uniqueness_left > 0.9 and self.key_uniqueness_right > 0.9
    
    @property
    def is_one_to_many(self) -> bool:
        """Check if join is likely one-to-many."""
        return self.key_uniqueness_left > 0.9 and self.key_uniqueness_right < 0.5
    
    @property
    def is_many_to_one(self) -> bool:
        """Check if join is likely many-to-one."""
        return self.key_uniqueness_left < 0.5 and self.key_uniqueness_right > 0.9
    
    @property
    def is_many_to_many(self) -> bool:
        """Check if join is likely many-to-many."""
        return self.key_uniqueness_left < 0.5 and self.key_uniqueness_right < 0.5


@dataclass
class JoinNode:
    """
    Represents a join operation in the optimization tree.
    
    This node tracks the join operation, its inputs, cost estimates,
    and optimization decisions.
    """
    
    node_id: str
    left_input: Union[str, 'JoinNode']  # Table name or another JoinNode
    right_input: Union[str, 'JoinNode']
    join_keys_left: List[str]
    join_keys_right: List[str]
    join_type: JoinType = JoinType.INNER
    strategy: JoinStrategy = JoinStrategy.AUTO
    
    # Cost estimates
    estimated_rows: Optional[int] = None
    estimated_cost: Optional[float] = None
    statistics: Optional[JoinStatistics] = None
    
    # Optimization metadata
    filters_pushed: List[QueryFilter] = field(default_factory=list)
    projections: Optional[Set[str]] = None
    is_broadcast_candidate: bool = False
    optimization_hints: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        """Generate node ID if not provided."""
        if not self.node_id:
            content = f"{self.left_input}:{self.right_input}:{self.join_keys_left}:{self.join_keys_right}"
            self.node_id = hashlib.md5(content.encode()).hexdigest()[:12]
    
    @property
    def is_leaf(self) -> bool:
        """Check if this is a leaf node (both inputs are tables)."""
        return (isinstance(self.left_input, str) and 
                isinstance(self.right_input, str))
    
    def get_input_tables(self) -> Set[str]:
        """Get all input tables for this join subtree."""
        tables = set()
        
        # Process left input
        if isinstance(self.left_input, str):
            tables.add(self.left_input)
        else:
            tables.update(self.left_input.get_input_tables())
        
        # Process right input
        if isinstance(self.right_input, str):
            tables.add(self.right_input)
        else:
            tables.update(self.right_input.get_input_tables())
        
        return tables


# === Cost Estimation ===

class JoinCostEstimator:
    """
    Estimates costs for join operations.
    
    Uses statistics and heuristics to estimate the cost of different
    join strategies and orders.
    """
    
    # Cost factors for different operations
    HASH_BUILD_COST_PER_ROW = 1.0
    HASH_PROBE_COST_PER_ROW = 0.5
    SORT_COST_PER_ROW = 2.0  # O(n log n)
    MERGE_COST_PER_ROW = 0.3
    BROADCAST_COST_PER_ROW = 3.0  # Network overhead
    NESTED_LOOP_COST_FACTOR = 10.0  # Very expensive
    
    def __init__(self, table_stats: Optional[Dict[str, Dict[str, Any]]] = None):
        """
        Initialize cost estimator.
        
        Parameters
        ----------
        table_stats : Optional[Dict[str, Dict[str, Any]]]
            Table statistics for cardinality estimation
        """
        self.table_stats = table_stats or {}
        self._selectivity_cache: Dict[str, float] = {}
    
    def estimate_join_cost(self,
                          node: JoinNode,
                          left_rows: int,
                          right_rows: int,
                          strategy: JoinStrategy) -> float:
        """
        Estimate cost for a specific join strategy.
        
        Parameters
        ----------
        node : JoinNode
            Join node to estimate
        left_rows : int
            Estimated rows in left input
        right_rows : int
            Estimated rows in right input
        strategy : JoinStrategy
            Join strategy to evaluate
            
        Returns
        -------
        float
            Estimated cost
        """
        if strategy == JoinStrategy.HASH:
            # Build hash table on smaller side + probe with larger side
            if left_rows < right_rows:
                build_cost = left_rows * self.HASH_BUILD_COST_PER_ROW
                probe_cost = right_rows * self.HASH_PROBE_COST_PER_ROW
            else:
                build_cost = right_rows * self.HASH_BUILD_COST_PER_ROW
                probe_cost = left_rows * self.HASH_PROBE_COST_PER_ROW
            return build_cost + probe_cost
        
        elif strategy == JoinStrategy.SORT_MERGE:
            # Sort both sides + merge
            sort_cost_left = left_rows * np.log2(max(left_rows, 2)) * self.SORT_COST_PER_ROW
            sort_cost_right = right_rows * np.log2(max(right_rows, 2)) * self.SORT_COST_PER_ROW
            merge_cost = (left_rows + right_rows) * self.MERGE_COST_PER_ROW
            return sort_cost_left + sort_cost_right + merge_cost
        
        elif strategy == JoinStrategy.BROADCAST:
            # Broadcast smaller side + hash join
            broadcast_rows = min(left_rows, right_rows)
            probe_rows = max(left_rows, right_rows)
            
            broadcast_cost = broadcast_rows * self.BROADCAST_COST_PER_ROW
            hash_cost = broadcast_rows * self.HASH_BUILD_COST_PER_ROW
            probe_cost = probe_rows * self.HASH_PROBE_COST_PER_ROW
            return broadcast_cost + hash_cost + probe_cost
        
        elif strategy == JoinStrategy.NESTED_LOOP:
            # Nested loop - very expensive
            return left_rows * right_rows * self.NESTED_LOOP_COST_FACTOR
        
        else:  # AUTO or unknown
            # Use hash join as default estimate
            return self.estimate_join_cost(node, left_rows, right_rows, JoinStrategy.HASH)
    
    def estimate_output_cardinality(self,
                                   node: JoinNode,
                                   left_rows: int,
                                   right_rows: int) -> int:
        """
        Estimate output cardinality for a join.
        
        Parameters
        ----------
        node : JoinNode
            Join node
        left_rows : int
            Rows in left input
        right_rows : int
            Rows in right input
            
        Returns
        -------
        int
            Estimated output rows
        """
        # Simple estimation based on join type
        if node.join_type == JoinType.INNER:
            # Estimate based on selectivity
            if node.statistics and node.statistics.selectivity > 0:
                selectivity = node.statistics.selectivity
            else:
                # Default selectivity heuristic
                selectivity = 1.0 / max(len(node.join_keys_left), 1)
            
            max_rows = left_rows * right_rows
            estimated = int(max_rows * selectivity)
            return min(estimated, max_rows)
        
        elif node.join_type == JoinType.LEFT:
            # At least all left rows
            return left_rows
        
        elif node.join_type == JoinType.RIGHT:
            # At least all right rows
            return right_rows
        
        elif node.join_type == JoinType.FULL:
            # At least max of both sides
            return max(left_rows, right_rows)
        
        elif node.join_type == JoinType.CROSS:
            # Cartesian product
            return left_rows * right_rows
        
        elif node.join_type == JoinType.SEMI:
            # At most left rows
            return min(left_rows, right_rows)
        
        elif node.join_type == JoinType.ANTI:
            # At most left rows
            return left_rows
        
        else:
            # Conservative estimate
            return max(left_rows, right_rows)
    
    def estimate_filter_selectivity(self, filter: QueryFilter) -> float:
        """
        Estimate selectivity of a filter.
        
        Parameters
        ----------
        filter : QueryFilter
            Filter to estimate
            
        Returns
        -------
        float
            Estimated selectivity (0.0 to 1.0)
        """
        # Check cache
        cache_key = f"{filter.column}:{filter.operator}:{filter.value}"
        if cache_key in self._selectivity_cache:
            return self._selectivity_cache[cache_key]
        
        # Estimate based on operator
        if filter.operator == "==":
            selectivity = 0.1  # Equality is usually selective
        elif filter.operator in [">", "<", ">=", "<="]:
            selectivity = 0.3  # Range filters
        elif filter.operator == "IN":
            n_values = len(filter.value) if isinstance(filter.value, (list, tuple)) else 1
            selectivity = min(0.1 * n_values, 0.5)
        elif filter.operator == "BETWEEN":
            selectivity = 0.2
        elif filter.operator == "IS NULL":
            selectivity = 0.05  # Usually few nulls in FIA
        elif filter.operator == "IS NOT NULL":
            selectivity = 0.95
        else:
            selectivity = 0.5  # Unknown
        
        self._selectivity_cache[cache_key] = selectivity
        return selectivity


# === Filter Push-Down ===

class FilterPushDown:
    """
    Handles filter push-down optimization.
    
    Pushes filters as close to the data source as possible to reduce
    intermediate data size.
    """
    
    def __init__(self):
        """Initialize filter push-down optimizer."""
        self.pushed_filters: Dict[str, List[QueryFilter]] = defaultdict(list)
        self.remaining_filters: List[QueryFilter] = []
    
    def analyze_filters(self,
                       filters: List[QueryFilter],
                       join_tree: JoinNode) -> Dict[str, List[QueryFilter]]:
        """
        Analyze which filters can be pushed to which tables.
        
        Parameters
        ----------
        filters : List[QueryFilter]
            Filters to analyze
        join_tree : JoinNode
            Join tree structure
            
        Returns
        -------
        Dict[str, List[QueryFilter]]
            Mapping of table names to pushable filters
        """
        pushable = defaultdict(list)
        
        # Get all tables in join tree
        tables = join_tree.get_input_tables()
        
        for filter in filters:
            # Determine which table the filter belongs to
            if filter.table:
                if filter.table in tables and filter.can_push_down:
                    pushable[filter.table].append(filter)
                else:
                    self.remaining_filters.append(filter)
            else:
                # Try to infer table from column name
                # This would require schema information
                self.remaining_filters.append(filter)
        
        self.pushed_filters = pushable
        return pushable
    
    def rewrite_join_tree(self, join_tree: JoinNode) -> JoinNode:
        """
        Rewrite join tree with pushed filters.
        
        Parameters
        ----------
        join_tree : JoinNode
            Original join tree
            
        Returns
        -------
        JoinNode
            Rewritten join tree with filters pushed
        """
        # Deep copy the tree
        import copy
        new_tree = copy.deepcopy(join_tree)
        
        # Add pushed filters to the tree
        new_tree.filters_pushed = self.pushed_filters.get(
            new_tree.node_id, []
        )
        
        return new_tree


# === Join Rewriter ===

class JoinRewriter:
    """
    Rewrites join plans for better performance.
    
    Applies various rewrite rules to optimize join execution including
    join reordering, strategy selection, and FIA-specific optimizations.
    """
    
    def __init__(self, cost_estimator: JoinCostEstimator):
        """
        Initialize join rewriter.
        
        Parameters
        ----------
        cost_estimator : JoinCostEstimator
            Cost estimator for evaluating plans
        """
        self.cost_estimator = cost_estimator
        self.rewrite_rules = [
            self._apply_broadcast_rule,
            self._apply_sort_merge_rule,
            self._apply_pushdown_rule,
            self._apply_fia_specific_rules
        ]
    
    def rewrite_plan(self, join_tree: JoinNode) -> JoinNode:
        """
        Apply rewrite rules to optimize join plan.
        
        Parameters
        ----------
        join_tree : JoinNode
            Original join tree
            
        Returns
        -------
        JoinNode
            Optimized join tree
        """
        # Apply each rewrite rule
        optimized = join_tree
        for rule in self.rewrite_rules:
            optimized = rule(optimized)
        
        return optimized
    
    def _apply_broadcast_rule(self, node: JoinNode) -> JoinNode:
        """Apply broadcast join optimization for small tables."""
        if node.statistics:
            # Check if one side is small enough for broadcast
            small_threshold = 10000  # rows
            
            if node.statistics.left_cardinality < small_threshold:
                node.strategy = JoinStrategy.BROADCAST
                node.is_broadcast_candidate = True
                node.optimization_hints["broadcast_side"] = "left"
            elif node.statistics.right_cardinality < small_threshold:
                node.strategy = JoinStrategy.BROADCAST
                node.is_broadcast_candidate = True
                node.optimization_hints["broadcast_side"] = "right"
        
        # Recursively apply to children
        if not node.is_leaf:
            if isinstance(node.left_input, JoinNode):
                node.left_input = self._apply_broadcast_rule(node.left_input)
            if isinstance(node.right_input, JoinNode):
                node.right_input = self._apply_broadcast_rule(node.right_input)
        
        return node
    
    def _apply_sort_merge_rule(self, node: JoinNode) -> JoinNode:
        """Apply sort-merge join for pre-sorted data."""
        # Check if data is already sorted on join keys
        if node.optimization_hints.get("left_sorted") and node.optimization_hints.get("right_sorted"):
            node.strategy = JoinStrategy.SORT_MERGE
        
        # Recursively apply to children
        if not node.is_leaf:
            if isinstance(node.left_input, JoinNode):
                node.left_input = self._apply_sort_merge_rule(node.left_input)
            if isinstance(node.right_input, JoinNode):
                node.right_input = self._apply_sort_merge_rule(node.right_input)
        
        return node
    
    def _apply_pushdown_rule(self, node: JoinNode) -> JoinNode:
        """Apply filter pushdown optimization."""
        # Filters are pushed in FilterPushDown class
        # This is a placeholder for additional pushdown logic
        return node
    
    def _apply_fia_specific_rules(self, node: JoinNode) -> JoinNode:
        """Apply FIA-specific optimization rules."""
        # Tree-Plot join optimization
        if self._is_tree_plot_join(node):
            # This is typically a many-to-one join
            node.strategy = JoinStrategy.HASH
            node.optimization_hints["fia_pattern"] = "tree_plot"
        
        # Stratification join optimization
        elif self._is_stratification_join(node):
            # Strata tables are usually small - use broadcast
            node.strategy = JoinStrategy.BROADCAST
            node.optimization_hints["fia_pattern"] = "stratification"
        
        # Condition-Plot join optimization
        elif self._is_condition_plot_join(node):
            # One-to-many relationship
            node.strategy = JoinStrategy.HASH
            node.optimization_hints["fia_pattern"] = "condition_plot"
        
        # Reference table joins (species, units, etc.)
        elif self._is_reference_join(node):
            # Reference tables are small - always broadcast
            node.strategy = JoinStrategy.BROADCAST
            node.optimization_hints["fia_pattern"] = "reference"
        
        # Recursively apply to children
        if not node.is_leaf:
            if isinstance(node.left_input, JoinNode):
                node.left_input = self._apply_fia_specific_rules(node.left_input)
            if isinstance(node.right_input, JoinNode):
                node.right_input = self._apply_fia_specific_rules(node.right_input)
        
        return node
    
    def _is_tree_plot_join(self, node: JoinNode) -> bool:
        """Check if this is a tree-plot join."""
        tables = node.get_input_tables()
        return "TREE" in tables and "PLOT" in tables and len(tables) == 2
    
    def _is_stratification_join(self, node: JoinNode) -> bool:
        """Check if this is a stratification join."""
        tables = node.get_input_tables()
        return any(t in tables for t in ["POP_STRATUM", "POP_PLOT_STRATUM_ASSGN"])
    
    def _is_condition_plot_join(self, node: JoinNode) -> bool:
        """Check if this is a condition-plot join."""
        tables = node.get_input_tables()
        return "COND" in tables and "PLOT" in tables and len(tables) == 2
    
    def _is_reference_join(self, node: JoinNode) -> bool:
        """Check if this involves reference tables."""
        tables = node.get_input_tables()
        ref_tables = {"REF_SPECIES", "REF_UNIT", "REF_FOREST_TYPE", "REF_POP_EVAL"}
        return bool(tables & ref_tables)


# === Main Join Optimizer ===

class JoinOptimizer:
    """
    Main join optimizer for pyFIA Phase 3.
    
    Orchestrates join optimization including order optimization,
    filter push-down, strategy selection, and execution.
    """
    
    def __init__(self,
                 config: Optional[EstimatorConfig] = None,
                 cache: Optional[MemoryCache] = None):
        """
        Initialize join optimizer.
        
        Parameters
        ----------
        config : Optional[EstimatorConfig]
            Estimator configuration
        cache : Optional[MemoryCache]
            Cache for optimization results
        """
        self.config = config or EstimatorConfig()
        self.cache = cache or MemoryCache(max_size_mb=256, max_entries=100)
        
        # Initialize components
        self.cost_estimator = JoinCostEstimator()
        self.filter_pushdown = FilterPushDown()
        self.join_rewriter = JoinRewriter(self.cost_estimator)
        
        # Optimization statistics
        self.stats = {
            "joins_optimized": 0,
            "filters_pushed": 0,
            "broadcast_joins": 0,
            "cost_reduction": 0.0
        }
    
    def optimize(self, query_plan: QueryPlan) -> QueryPlan:
        """
        Optimize a query plan.
        
        Parameters
        ----------
        query_plan : QueryPlan
            Original query plan
            
        Returns
        -------
        QueryPlan
            Optimized query plan
        """
        # Check cache
        cache_key = CacheKey("join_optimization", {"plan": query_plan.cache_key})
        cached_result = self.cache.get(cache_key.key)
        if cached_result is not None:
            return cached_result
        
        # Build join tree from query plan
        join_tree = self._build_join_tree(query_plan)
        
        if join_tree is None:
            # No joins to optimize
            return query_plan
        
        # Estimate statistics for joins
        self._estimate_statistics(join_tree, query_plan)
        
        # Optimize join order
        optimized_tree = self._optimize_join_order(join_tree)
        
        # Push down filters
        pushed_filters = self.filter_pushdown.analyze_filters(
            query_plan.filters, optimized_tree
        )
        optimized_tree = self.filter_pushdown.rewrite_join_tree(optimized_tree)
        
        # Apply rewrite rules
        optimized_tree = self.join_rewriter.rewrite_plan(optimized_tree)
        
        # Convert back to query plan
        optimized_plan = self._tree_to_query_plan(optimized_tree, query_plan)
        
        # Update statistics
        self.stats["joins_optimized"] += len(query_plan.joins)
        self.stats["filters_pushed"] += sum(len(f) for f in pushed_filters.values())
        
        # Cache result
        self.cache.put(cache_key.key, optimized_plan, ttl_seconds=300)
        
        return optimized_plan
    
    def _build_join_tree(self, query_plan: QueryPlan) -> Optional[JoinNode]:
        """Build join tree from query plan."""
        if not query_plan.joins:
            return None
        
        # For simplicity, build left-deep tree
        # In production, would use more sophisticated algorithm
        nodes = {}
        
        for join in query_plan.joins:
            node = JoinNode(
                node_id="",
                left_input=join.left_table,
                right_input=join.right_table,
                join_keys_left=join.left_on if isinstance(join.left_on, list) else [join.left_on],
                join_keys_right=join.right_on if isinstance(join.right_on, list) else [join.right_on],
                join_type=JoinType(join.how),
                strategy=join.strategy
            )
            nodes[f"{join.left_table}_{join.right_table}"] = node
        
        # Build tree (simplified - just return first join for now)
        return list(nodes.values())[0] if nodes else None
    
    def _estimate_statistics(self, join_tree: JoinNode, query_plan: QueryPlan):
        """Estimate statistics for join nodes."""
        # Get table statistics
        left_stats = self._get_table_statistics(join_tree.left_input)
        right_stats = self._get_table_statistics(join_tree.right_input)
        
        # Estimate join statistics
        join_tree.statistics = JoinStatistics(
            left_cardinality=left_stats.get("rows", 100000),
            right_cardinality=right_stats.get("rows", 100000),
            estimated_output_rows=self.cost_estimator.estimate_output_cardinality(
                join_tree,
                left_stats.get("rows", 100000),
                right_stats.get("rows", 100000)
            ),
            selectivity=query_plan.filter_selectivity or 0.5,
            key_uniqueness_left=left_stats.get("uniqueness", 0.5),
            key_uniqueness_right=right_stats.get("uniqueness", 0.5),
            null_ratio_left=left_stats.get("null_ratio", 0.01),
            null_ratio_right=right_stats.get("null_ratio", 0.01)
        )
    
    def _get_table_statistics(self, table: Union[str, JoinNode]) -> Dict[str, Any]:
        """Get statistics for a table or join result."""
        if isinstance(table, str):
            # FIA-specific table statistics
            fia_stats = {
                "TREE": {"rows": 1000000, "uniqueness": 0.99, "null_ratio": 0.01},
                "PLOT": {"rows": 100000, "uniqueness": 0.99, "null_ratio": 0.01},
                "COND": {"rows": 150000, "uniqueness": 0.95, "null_ratio": 0.02},
                "POP_STRATUM": {"rows": 5000, "uniqueness": 0.99, "null_ratio": 0.01},
                "POP_PLOT_STRATUM_ASSGN": {"rows": 100000, "uniqueness": 0.9, "null_ratio": 0.01},
                "REF_SPECIES": {"rows": 1000, "uniqueness": 1.0, "null_ratio": 0.0},
            }
            return fia_stats.get(table, {"rows": 50000, "uniqueness": 0.5, "null_ratio": 0.05})
        else:
            # Join result statistics
            if table.statistics:
                return {
                    "rows": table.statistics.estimated_output_rows,
                    "uniqueness": 0.5,  # Conservative estimate
                    "null_ratio": 0.02
                }
            return {"rows": 100000, "uniqueness": 0.5, "null_ratio": 0.05}
    
    def _optimize_join_order(self, join_tree: JoinNode) -> JoinNode:
        """
        Optimize join order using dynamic programming or greedy algorithm.
        
        For now, uses a simple greedy approach based on estimated costs.
        """
        # For single join, no reordering needed
        if join_tree.is_leaf:
            return join_tree
        
        # Estimate costs for current order
        current_cost = self._estimate_tree_cost(join_tree)
        
        # Try swapping inputs if both are tables (simplified)
        if join_tree.is_leaf:
            swapped = JoinNode(
                node_id=join_tree.node_id,
                left_input=join_tree.right_input,
                right_input=join_tree.left_input,
                join_keys_left=join_tree.join_keys_right,
                join_keys_right=join_tree.join_keys_left,
                join_type=join_tree.join_type,
                strategy=join_tree.strategy,
                statistics=join_tree.statistics
            )
            
            swapped_cost = self._estimate_tree_cost(swapped)
            
            if swapped_cost < current_cost:
                logger.debug(f"Swapped join order: {swapped_cost:.2f} < {current_cost:.2f}")
                return swapped
        
        return join_tree
    
    def _estimate_tree_cost(self, join_tree: JoinNode) -> float:
        """Estimate total cost for a join tree."""
        if join_tree.statistics:
            return self.cost_estimator.estimate_join_cost(
                join_tree,
                join_tree.statistics.left_cardinality,
                join_tree.statistics.right_cardinality,
                join_tree.strategy
            )
        return float('inf')
    
    def _tree_to_query_plan(self, join_tree: JoinNode, original_plan: QueryPlan) -> QueryPlan:
        """Convert optimized join tree back to query plan."""
        # Create new query plan with optimized joins
        optimized_joins = []
        
        # Convert join tree to list of joins (simplified)
        if join_tree:
            optimized_join = QueryJoin(
                left_table=str(join_tree.left_input),
                right_table=str(join_tree.right_input),
                left_on=join_tree.join_keys_left,
                right_on=join_tree.join_keys_right,
                how=join_tree.join_type.value,
                strategy=join_tree.strategy
            )
            optimized_joins.append(optimized_join)
        
        # Create new query plan
        return QueryPlan(
            tables=original_plan.tables,
            columns=original_plan.columns,
            filters=self.filter_pushdown.remaining_filters,  # Non-pushed filters
            joins=optimized_joins,
            group_by=original_plan.group_by,
            order_by=original_plan.order_by,
            limit=original_plan.limit,
            estimated_rows=join_tree.statistics.estimated_output_rows if join_tree and join_tree.statistics else None,
            filter_selectivity=original_plan.filter_selectivity,
            preferred_strategy=join_tree.strategy if join_tree else None
        )
    
    def execute_optimized_join(self,
                              left: LazyFrameWrapper,
                              right: LazyFrameWrapper,
                              node: JoinNode) -> LazyFrameWrapper:
        """
        Execute an optimized join operation.
        
        Parameters
        ----------
        left : LazyFrameWrapper
            Left input frame
        right : LazyFrameWrapper
            Right input frame
        node : JoinNode
            Optimized join node
            
        Returns
        -------
        LazyFrameWrapper
            Join result
        """
        # Convert to lazy frames
        left_lazy = left.to_lazy()
        right_lazy = right.to_lazy()
        
        # Apply pushed filters
        for filter in node.filters_pushed:
            if filter.table == str(node.left_input):
                left_lazy = left_lazy.filter(filter.to_polars_expr())
            elif filter.table == str(node.right_input):
                right_lazy = right_lazy.filter(filter.to_polars_expr())
        
        # Apply projections if specified
        if node.projections:
            left_cols = [c for c in node.projections if c in left_lazy.columns]
            right_cols = [c for c in node.projections if c in right_lazy.columns]
            
            if left_cols:
                left_lazy = left_lazy.select(left_cols)
            if right_cols:
                right_lazy = right_lazy.select(right_cols)
        
        # Execute join with optimized strategy
        if node.strategy == JoinStrategy.BROADCAST and node.is_broadcast_candidate:
            # Force collection of small side for broadcast
            if node.optimization_hints.get("broadcast_side") == "right":
                right_df = right_lazy.collect()
                result = left_lazy.join(
                    right_df.lazy(),
                    left_on=node.join_keys_left,
                    right_on=node.join_keys_right,
                    how=node.join_type.value
                )
            else:
                left_df = left_lazy.collect()
                result = left_df.lazy().join(
                    right_lazy,
                    left_on=node.join_keys_left,
                    right_on=node.join_keys_right,
                    how=node.join_type.value
                )
            
            self.stats["broadcast_joins"] += 1
        else:
            # Standard join
            result = left_lazy.join(
                right_lazy,
                left_on=node.join_keys_left,
                right_on=node.join_keys_right,
                how=node.join_type.value
            )
        
        return LazyFrameWrapper(result)
    
    def get_optimization_stats(self) -> Dict[str, Any]:
        """Get optimization statistics."""
        return self.stats.copy()


# === FIA-Specific Join Patterns ===

class FIAJoinPatterns:
    """
    Predefined optimized join patterns for common FIA operations.
    
    These patterns are pre-optimized for the most common join sequences
    in FIA estimation workflows.
    """
    
    @staticmethod
    def tree_plot_condition_pattern() -> JoinNode:
        """Optimized pattern for tree-plot-condition joins."""
        # Plot-Condition join first (smaller intermediate result)
        plot_cond = JoinNode(
            node_id="plot_cond",
            left_input="PLOT",
            right_input="COND",
            join_keys_left=["CN"],
            join_keys_right=["PLT_CN"],
            join_type=JoinType.INNER,
            strategy=JoinStrategy.HASH
        )
        
        # Then join with trees
        tree_join = JoinNode(
            node_id="tree_plot_cond",
            left_input="TREE",
            right_input=plot_cond,
            join_keys_left=["PLT_CN"],
            join_keys_right=["CN"],
            join_type=JoinType.INNER,
            strategy=JoinStrategy.HASH
        )
        
        return tree_join
    
    @staticmethod
    def stratification_pattern() -> JoinNode:
        """Optimized pattern for stratification joins."""
        # Assignment table with plots
        assgn_plot = JoinNode(
            node_id="assgn_plot",
            left_input="POP_PLOT_STRATUM_ASSGN",
            right_input="PLOT",
            join_keys_left=["PLT_CN"],
            join_keys_right=["CN"],
            join_type=JoinType.INNER,
            strategy=JoinStrategy.HASH
        )
        
        # Then join with strata (broadcast small table)
        strata_join = JoinNode(
            node_id="stratification",
            left_input=assgn_plot,
            right_input="POP_STRATUM",
            join_keys_left=["STRATUM_CN"],
            join_keys_right=["CN"],
            join_type=JoinType.INNER,
            strategy=JoinStrategy.BROADCAST
        )
        
        return strata_join
    
    @staticmethod
    def species_reference_pattern() -> JoinNode:
        """Optimized pattern for species reference joins."""
        return JoinNode(
            node_id="species_ref",
            left_input="TREE",
            right_input="REF_SPECIES",
            join_keys_left=["SPCD"],
            join_keys_right=["SPCD"],
            join_type=JoinType.LEFT,
            strategy=JoinStrategy.BROADCAST  # Reference table is small
        )


# === Integration with Query Builders ===

class OptimizedQueryExecutor:
    """
    Executes optimized query plans using the join optimizer.
    
    This class integrates the join optimizer with the query builder
    framework for seamless optimization.
    """
    
    def __init__(self,
                 optimizer: JoinOptimizer,
                 cache: Optional[MemoryCache] = None):
        """
        Initialize optimized executor.
        
        Parameters
        ----------
        optimizer : JoinOptimizer
            Join optimizer instance
        cache : Optional[MemoryCache]
            Cache for results
        """
        self.optimizer = optimizer
        self.cache = cache or MemoryCache(max_size_mb=512, max_entries=200)
    
    def execute_plan(self,
                     plan: QueryPlan,
                     data_sources: Dict[str, LazyFrameWrapper]) -> LazyFrameWrapper:
        """
        Execute an optimized query plan.
        
        Parameters
        ----------
        plan : QueryPlan
            Query plan to execute
        data_sources : Dict[str, LazyFrameWrapper]
            Available data sources by table name
            
        Returns
        -------
        LazyFrameWrapper
            Query result
        """
        # Optimize the plan
        optimized_plan = self.optimizer.optimize(plan)
        
        # Check cache
        cache_key = CacheKey("query_result", {"plan": optimized_plan.cache_key})
        cached_result = self.cache.get(cache_key.key)
        if cached_result is not None:
            return LazyFrameWrapper(cached_result)
        
        # Execute joins in optimized order
        if not optimized_plan.joins:
            # No joins - return single table
            if optimized_plan.tables:
                result = data_sources[optimized_plan.tables[0]]
            else:
                raise ValueError("No tables in query plan")
        else:
            # Execute joins
            result = self._execute_joins(optimized_plan, data_sources)
        
        # Apply remaining filters
        for filter in optimized_plan.filters:
            if not filter.can_push_down:
                result = LazyFrameWrapper(result.frame.filter(filter.to_polars_expr()))
        
        # Apply grouping if specified
        if optimized_plan.group_by:
            grouped = result.frame.group_by(optimized_plan.group_by).agg(
                pl.all().first()  # Simplified aggregation
            )
            result = LazyFrameWrapper(grouped)
        
        # Apply ordering if specified
        if optimized_plan.order_by:
            for col, direction in optimized_plan.order_by:
                sorted_frame = result.frame.sort(col, descending=(direction == "DESC"))
                result = LazyFrameWrapper(sorted_frame)
        
        # Apply limit if specified
        if optimized_plan.limit:
            limited = result.frame.limit(optimized_plan.limit)
            result = LazyFrameWrapper(limited)
        
        # Cache and return
        self.cache.put(cache_key.key, result.frame if hasattr(result, 'frame') else result, ttl_seconds=300)
        return result
    
    def _execute_joins(self,
                      plan: QueryPlan,
                      data_sources: Dict[str, LazyFrameWrapper]) -> LazyFrameWrapper:
        """Execute joins from optimized plan."""
        # For simplicity, execute joins sequentially
        # In production, would build and execute join tree
        
        result = None
        for join in plan.joins:
            left = data_sources.get(join.left_table) if result is None else result
            right = data_sources.get(join.right_table)
            
            if left is None or right is None:
                raise ValueError(f"Missing data source for join: {join.left_table} or {join.right_table}")
            
            # Create join node for execution
            node = JoinNode(
                node_id="",
                left_input=join.left_table,
                right_input=join.right_table,
                join_keys_left=join.left_on if isinstance(join.left_on, list) else [join.left_on],
                join_keys_right=join.right_on if isinstance(join.right_on, list) else [join.right_on],
                join_type=JoinType(join.how),
                strategy=join.strategy
            )
            
            result = self.optimizer.execute_optimized_join(left, right, node)
        
        return result


# === Export public API ===

__all__ = [
    # Main optimizer
    "JoinOptimizer",
    
    # Components
    "JoinNode",
    "JoinCostEstimator",
    "FilterPushDown",
    "JoinRewriter",
    
    # Patterns and execution
    "FIAJoinPatterns",
    "OptimizedQueryExecutor",
    
    # Types
    "JoinType",
    "JoinStatistics",
]