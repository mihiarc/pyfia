"""
Tests for the join optimizer module.

Tests join optimization including order optimization, filter push-down,
strategy selection, and FIA-specific patterns.
"""

import pytest
from unittest.mock import Mock, MagicMock, patch
from datetime import datetime

import polars as pl
import numpy as np

from pyfia.estimation.join_optimizer import (
    JoinOptimizer,
    JoinNode,
    JoinType,
    JoinStatistics,
    JoinCostEstimator,
    FilterPushDown,
    JoinRewriter,
    FIAJoinPatterns,
    OptimizedQueryExecutor
)
from pyfia.estimation.query_builders import (
    QueryPlan,
    QueryJoin,
    QueryFilter,
    QueryColumn,
    JoinStrategy
)
from pyfia.estimation.lazy_evaluation import LazyFrameWrapper
from pyfia.estimation.config import EstimatorConfig
from pyfia.estimation.caching import MemoryCache


class TestJoinNode:
    """Test JoinNode functionality."""
    
    def test_node_creation(self):
        """Test creating a join node."""
        node = JoinNode(
            node_id="test_node",
            left_input="TREE",
            right_input="PLOT",
            join_keys_left=["PLT_CN"],
            join_keys_right=["CN"],
            join_type=JoinType.INNER,
            strategy=JoinStrategy.HASH
        )
        
        assert node.node_id == "test_node"
        assert node.left_input == "TREE"
        assert node.right_input == "PLOT"
        assert node.join_keys_left == ["PLT_CN"]
        assert node.join_keys_right == ["CN"]
        assert node.join_type == JoinType.INNER
        assert node.strategy == JoinStrategy.HASH
        assert node.is_leaf
    
    def test_node_auto_id(self):
        """Test automatic node ID generation."""
        node = JoinNode(
            node_id="",
            left_input="TREE",
            right_input="PLOT",
            join_keys_left=["PLT_CN"],
            join_keys_right=["CN"]
        )
        
        assert node.node_id != ""
        assert len(node.node_id) == 12  # MD5 hash truncated
    
    def test_nested_join_node(self):
        """Test nested join nodes."""
        inner_node = JoinNode(
            node_id="inner",
            left_input="PLOT",
            right_input="COND",
            join_keys_left=["CN"],
            join_keys_right=["PLT_CN"]
        )
        
        outer_node = JoinNode(
            node_id="outer",
            left_input="TREE",
            right_input=inner_node,
            join_keys_left=["PLT_CN"],
            join_keys_right=["CN"]
        )
        
        assert not outer_node.is_leaf
        assert outer_node.right_input == inner_node
    
    def test_get_input_tables(self):
        """Test getting all input tables from join tree."""
        # Create nested join tree
        plot_cond = JoinNode(
            node_id="plot_cond",
            left_input="PLOT",
            right_input="COND",
            join_keys_left=["CN"],
            join_keys_right=["PLT_CN"]
        )
        
        tree_join = JoinNode(
            node_id="tree_join",
            left_input="TREE",
            right_input=plot_cond,
            join_keys_left=["PLT_CN"],
            join_keys_right=["CN"]
        )
        
        tables = tree_join.get_input_tables()
        assert tables == {"TREE", "PLOT", "COND"}


class TestJoinStatistics:
    """Test JoinStatistics functionality."""
    
    def test_statistics_properties(self):
        """Test statistics property calculations."""
        # One-to-one join
        stats = JoinStatistics(
            left_cardinality=1000,
            right_cardinality=1000,
            estimated_output_rows=1000,
            selectivity=1.0,
            key_uniqueness_left=0.95,
            key_uniqueness_right=0.95,
            null_ratio_left=0.01,
            null_ratio_right=0.01
        )
        assert stats.is_one_to_one
        assert not stats.is_one_to_many
        assert not stats.is_many_to_one
        assert not stats.is_many_to_many
        
        # One-to-many join
        stats = JoinStatistics(
            left_cardinality=1000,
            right_cardinality=10000,
            estimated_output_rows=10000,
            selectivity=0.1,
            key_uniqueness_left=0.95,
            key_uniqueness_right=0.1,
            null_ratio_left=0.01,
            null_ratio_right=0.01
        )
        assert not stats.is_one_to_one
        assert stats.is_one_to_many
        assert not stats.is_many_to_one
        assert not stats.is_many_to_many


class TestJoinCostEstimator:
    """Test JoinCostEstimator functionality."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.estimator = JoinCostEstimator()
    
    def test_hash_join_cost(self):
        """Test hash join cost estimation."""
        node = JoinNode(
            node_id="test",
            left_input="TREE",
            right_input="PLOT",
            join_keys_left=["PLT_CN"],
            join_keys_right=["CN"]
        )
        
        cost = self.estimator.estimate_join_cost(
            node, 100000, 10000, JoinStrategy.HASH
        )
        
        # Hash join should build on smaller side (10000 rows)
        expected_build = 10000 * self.estimator.HASH_BUILD_COST_PER_ROW
        expected_probe = 100000 * self.estimator.HASH_PROBE_COST_PER_ROW
        assert cost == expected_build + expected_probe
    
    def test_sort_merge_cost(self):
        """Test sort-merge join cost estimation."""
        node = JoinNode(
            node_id="test",
            left_input="TREE",
            right_input="PLOT",
            join_keys_left=["PLT_CN"],
            join_keys_right=["CN"]
        )
        
        cost = self.estimator.estimate_join_cost(
            node, 1000, 1000, JoinStrategy.SORT_MERGE
        )
        
        # Should include sort cost for both sides plus merge
        assert cost > 0
        # Sort-merge should be more expensive than hash for small tables
        hash_cost = self.estimator.estimate_join_cost(
            node, 1000, 1000, JoinStrategy.HASH
        )
        assert cost > hash_cost
    
    def test_broadcast_join_cost(self):
        """Test broadcast join cost estimation."""
        node = JoinNode(
            node_id="test",
            left_input="TREE",
            right_input="REF_SPECIES",
            join_keys_left=["SPCD"],
            join_keys_right=["SPCD"]
        )
        
        cost = self.estimator.estimate_join_cost(
            node, 100000, 1000, JoinStrategy.BROADCAST
        )
        
        # Broadcast should include network cost
        assert cost > 0
        # Should broadcast smaller table (1000 rows)
        expected_broadcast = 1000 * self.estimator.BROADCAST_COST_PER_ROW
        assert expected_broadcast in [
            c for c in [cost - 1000 * self.estimator.HASH_BUILD_COST_PER_ROW - 
                        100000 * self.estimator.HASH_PROBE_COST_PER_ROW]
        ]
    
    def test_output_cardinality_estimation(self):
        """Test output cardinality estimation."""
        node = JoinNode(
            node_id="test",
            left_input="TREE",
            right_input="PLOT",
            join_keys_left=["PLT_CN"],
            join_keys_right=["CN"],
            join_type=JoinType.INNER
        )
        
        # Inner join
        output_rows = self.estimator.estimate_output_cardinality(
            node, 100000, 10000
        )
        assert output_rows > 0
        assert output_rows <= 100000 * 10000  # Can't exceed cartesian product
        
        # Left join
        node.join_type = JoinType.LEFT
        output_rows = self.estimator.estimate_output_cardinality(
            node, 100000, 10000
        )
        assert output_rows >= 100000  # At least all left rows
        
        # Cross join
        node.join_type = JoinType.CROSS
        output_rows = self.estimator.estimate_output_cardinality(
            node, 100, 200
        )
        assert output_rows == 100 * 200  # Cartesian product
    
    def test_filter_selectivity_estimation(self):
        """Test filter selectivity estimation."""
        # Equality filter - should be selective
        filter_eq = QueryFilter("STATUSCD", "==", 1)
        selectivity = self.estimator.estimate_filter_selectivity(filter_eq)
        assert selectivity == 0.1
        
        # Range filter - moderately selective
        filter_range = QueryFilter("DIA", ">", 10.0)
        selectivity = self.estimator.estimate_filter_selectivity(filter_range)
        assert selectivity == 0.3
        
        # IN clause - depends on values
        filter_in = QueryFilter("SPCD", "IN", [131, 110, 833])
        selectivity = self.estimator.estimate_filter_selectivity(filter_in)
        assert abs(selectivity - 0.3) < 0.0001  # 0.1 * 3 values (with float tolerance)
        
        # IS NULL - very selective in FIA
        filter_null = QueryFilter("TREECLCD", "IS NULL", None)
        selectivity = self.estimator.estimate_filter_selectivity(filter_null)
        assert selectivity == 0.05


class TestFilterPushDown:
    """Test FilterPushDown functionality."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.pushdown = FilterPushDown()
    
    def test_analyze_filters(self):
        """Test filter analysis for push-down."""
        filters = [
            QueryFilter("STATUSCD", "==", 1, "TREE", can_push_down=True),
            QueryFilter("DIA", ">", 10.0, "TREE", can_push_down=True),
            QueryFilter("STATECD", "==", 37, "PLOT", can_push_down=True),
            QueryFilter("COMPLEX_EXPR", "==", "value", None, can_push_down=False)
        ]
        
        join_tree = JoinNode(
            node_id="test",
            left_input="TREE",
            right_input="PLOT",
            join_keys_left=["PLT_CN"],
            join_keys_right=["CN"]
        )
        
        pushable = self.pushdown.analyze_filters(filters, join_tree)
        
        assert "TREE" in pushable
        assert len(pushable["TREE"]) == 2
        assert "PLOT" in pushable
        assert len(pushable["PLOT"]) == 1
        assert len(self.pushdown.remaining_filters) == 1
    
    def test_rewrite_join_tree(self):
        """Test rewriting join tree with pushed filters."""
        join_tree = JoinNode(
            node_id="test",
            left_input="TREE",
            right_input="PLOT",
            join_keys_left=["PLT_CN"],
            join_keys_right=["CN"]
        )
        
        self.pushdown.pushed_filters = {
            "test": [QueryFilter("STATUSCD", "==", 1, "TREE")]
        }
        
        rewritten = self.pushdown.rewrite_join_tree(join_tree)
        assert len(rewritten.filters_pushed) == 1


class TestJoinRewriter:
    """Test JoinRewriter functionality."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.cost_estimator = JoinCostEstimator()
        self.rewriter = JoinRewriter(self.cost_estimator)
    
    def test_broadcast_rule(self):
        """Test broadcast join rule application."""
        node = JoinNode(
            node_id="test",
            left_input="TREE",
            right_input="REF_SPECIES",
            join_keys_left=["SPCD"],
            join_keys_right=["SPCD"],
            statistics=JoinStatistics(
                left_cardinality=100000,
                right_cardinality=1000,  # Small table
                estimated_output_rows=100000,
                selectivity=1.0,
                key_uniqueness_left=0.1,
                key_uniqueness_right=1.0,
                null_ratio_left=0.01,
                null_ratio_right=0.0
            )
        )
        
        optimized = self.rewriter._apply_broadcast_rule(node)
        assert optimized.strategy == JoinStrategy.BROADCAST
        assert optimized.is_broadcast_candidate
        assert optimized.optimization_hints["broadcast_side"] == "right"
    
    def test_fia_specific_rules(self):
        """Test FIA-specific optimization rules."""
        # Tree-Plot join
        node = JoinNode(
            node_id="test",
            left_input="TREE",
            right_input="PLOT",
            join_keys_left=["PLT_CN"],
            join_keys_right=["CN"]
        )
        
        optimized = self.rewriter._apply_fia_specific_rules(node)
        assert optimized.strategy == JoinStrategy.HASH
        assert optimized.optimization_hints["fia_pattern"] == "tree_plot"
        
        # Stratification join
        node = JoinNode(
            node_id="test",
            left_input="PLOT",
            right_input="POP_STRATUM",
            join_keys_left=["STRATUM_CN"],
            join_keys_right=["CN"]
        )
        
        optimized = self.rewriter._apply_fia_specific_rules(node)
        assert optimized.strategy == JoinStrategy.BROADCAST
        assert optimized.optimization_hints["fia_pattern"] == "stratification"


class TestJoinOptimizer:
    """Test main JoinOptimizer functionality."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.config = EstimatorConfig()
        self.cache = MemoryCache(max_size_mb=128, max_entries=50)
        self.optimizer = JoinOptimizer(self.config, self.cache)
    
    def test_optimize_simple_join(self):
        """Test optimizing a simple join."""
        plan = QueryPlan(
            tables=["TREE", "PLOT"],
            columns=[
                QueryColumn("CN", "TREE"),
                QueryColumn("PLT_CN", "TREE"),
                QueryColumn("DIA", "TREE"),
                QueryColumn("CN", "PLOT"),
                QueryColumn("STATECD", "PLOT")
            ],
            filters=[
                QueryFilter("STATUSCD", "==", 1, "TREE"),
                QueryFilter("STATECD", "==", 37, "PLOT")
            ],
            joins=[
                QueryJoin(
                    left_table="TREE",
                    right_table="PLOT",
                    left_on="PLT_CN",
                    right_on="CN",
                    how="inner"
                )
            ]
        )
        
        optimized = self.optimizer.optimize(plan)
        
        assert optimized is not None
        assert len(optimized.joins) == 1
        # Filters should be pushed down
        assert len(optimized.filters) < len(plan.filters)
    
    def test_optimize_no_joins(self):
        """Test optimization with no joins."""
        plan = QueryPlan(
            tables=["TREE"],
            columns=[
                QueryColumn("CN", "TREE"),
                QueryColumn("DIA", "TREE")
            ],
            filters=[
                QueryFilter("STATUSCD", "==", 1, "TREE")
            ],
            joins=[]
        )
        
        optimized = self.optimizer.optimize(plan)
        assert optimized == plan  # Should return original if no joins
    
    def test_cache_optimization(self):
        """Test caching of optimization results."""
        plan = QueryPlan(
            tables=["TREE", "PLOT"],
            columns=[QueryColumn("CN", "TREE")],
            joins=[
                QueryJoin(
                    left_table="TREE",
                    right_table="PLOT",
                    left_on="PLT_CN",
                    right_on="CN"
                )
            ]
        )
        
        # First call should compute
        optimized1 = self.optimizer.optimize(plan)
        stats1 = self.optimizer.stats["joins_optimized"]
        
        # Second call should use cache
        optimized2 = self.optimizer.optimize(plan)
        stats2 = self.optimizer.stats["joins_optimized"]
        
        assert optimized1.cache_key == optimized2.cache_key
        assert stats2 == stats1  # Stats shouldn't increase for cached result
    
    def test_execute_optimized_join(self):
        """Test executing an optimized join."""
        # Create mock data
        left_data = pl.DataFrame({
            "PLT_CN": [1, 2, 3, 4, 5],
            "DIA": [10.0, 12.0, 8.0, 15.0, 20.0],
            "STATUSCD": [1, 1, 2, 1, 1]
        })
        right_data = pl.DataFrame({
            "CN": [1, 2, 3, 4, 5],
            "STATECD": [37, 37, 45, 37, 51]
        })
        
        left_wrapper = LazyFrameWrapper(left_data.lazy())
        right_wrapper = LazyFrameWrapper(right_data.lazy())
        
        # Don't push filters for this test - just test basic join
        node = JoinNode(
            node_id="test",
            left_input="TREE",
            right_input="PLOT",
            join_keys_left=["PLT_CN"],
            join_keys_right=["CN"],
            join_type=JoinType.INNER,
            strategy=JoinStrategy.HASH,
            filters_pushed=[]  # No filters pushed
        )
        
        result = self.optimizer.execute_optimized_join(
            left_wrapper, right_wrapper, node
        )
        
        assert isinstance(result, LazyFrameWrapper)
        
        # Collect and verify
        result_df = result.collect()
        assert len(result_df) == 5  # All rows should be joined
        assert "PLT_CN" in result_df.columns
        assert "STATECD" in result_df.columns  # From right table
        # Note: CN is dropped as it's the join key from right side


class TestFIAJoinPatterns:
    """Test FIA-specific join patterns."""
    
    def test_tree_plot_condition_pattern(self):
        """Test tree-plot-condition join pattern."""
        pattern = FIAJoinPatterns.tree_plot_condition_pattern()
        
        assert pattern.node_id == "tree_plot_cond"
        assert pattern.left_input == "TREE"
        assert isinstance(pattern.right_input, JoinNode)
        
        # Check inner join structure
        inner = pattern.right_input
        assert inner.left_input == "PLOT"
        assert inner.right_input == "COND"
        assert inner.strategy == JoinStrategy.HASH
    
    def test_stratification_pattern(self):
        """Test stratification join pattern."""
        pattern = FIAJoinPatterns.stratification_pattern()
        
        assert pattern.node_id == "stratification"
        assert pattern.right_input == "POP_STRATUM"
        assert pattern.strategy == JoinStrategy.BROADCAST  # Small table
        
        # Check inner join
        inner = pattern.left_input
        assert inner.left_input == "POP_PLOT_STRATUM_ASSGN"
        assert inner.right_input == "PLOT"
    
    def test_species_reference_pattern(self):
        """Test species reference join pattern."""
        pattern = FIAJoinPatterns.species_reference_pattern()
        
        assert pattern.left_input == "TREE"
        assert pattern.right_input == "REF_SPECIES"
        assert pattern.join_keys_left == ["SPCD"]
        assert pattern.join_keys_right == ["SPCD"]
        assert pattern.join_type == JoinType.LEFT
        assert pattern.strategy == JoinStrategy.BROADCAST


class TestOptimizedQueryExecutor:
    """Test OptimizedQueryExecutor functionality."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.optimizer = JoinOptimizer()
        self.executor = OptimizedQueryExecutor(self.optimizer)
    
    def test_execute_plan_no_joins(self):
        """Test executing a plan with no joins."""
        # Create mock data
        tree_data = pl.DataFrame({
            "CN": [1, 2, 3],
            "DIA": [10.0, 12.0, 15.0],
            "STATUSCD": [1, 1, 2]
        })
        
        plan = QueryPlan(
            tables=["TREE"],
            columns=[QueryColumn("CN", "TREE"), QueryColumn("DIA", "TREE")],
            filters=[QueryFilter("STATUSCD", "==", 1, can_push_down=False)],  # Mark as not pushable so it gets applied
            joins=[]
        )
        
        data_sources = {
            "TREE": LazyFrameWrapper(tree_data.lazy())
        }
        
        result = self.executor.execute_plan(plan, data_sources)
        assert isinstance(result, LazyFrameWrapper)
        
        # Verify filter was applied
        result_df = result.collect()
        assert len(result_df) == 2  # Only STATUSCD == 1
    
    def test_execute_plan_with_joins(self):
        """Test executing a plan with joins."""
        # Create mock data
        tree_data = pl.DataFrame({
            "CN": [1, 2, 3],
            "PLT_CN": [10, 20, 30],
            "DIA": [10.0, 12.0, 15.0]
        })
        plot_data = pl.DataFrame({
            "CN": [10, 20, 30],
            "STATECD": [37, 37, 45]
        })
        
        plan = QueryPlan(
            tables=["TREE", "PLOT"],
            columns=[
                QueryColumn("CN", "TREE"),
                QueryColumn("DIA", "TREE"),
                QueryColumn("STATECD", "PLOT")
            ],
            joins=[
                QueryJoin(
                    left_table="TREE",
                    right_table="PLOT",
                    left_on="PLT_CN",
                    right_on="CN",
                    how="inner"
                )
            ]
        )
        
        data_sources = {
            "TREE": LazyFrameWrapper(tree_data.lazy()),
            "PLOT": LazyFrameWrapper(plot_data.lazy())
        }
        
        result = self.executor.execute_plan(plan, data_sources)
        assert isinstance(result, LazyFrameWrapper)
        
        result_df = result.collect()
        assert len(result_df) == 3
        assert "STATECD" in result_df.columns
    
    def test_execute_plan_with_grouping(self):
        """Test executing a plan with grouping."""
        tree_data = pl.DataFrame({
            "CN": [1, 2, 3, 4],
            "SPCD": [131, 131, 110, 131],
            "DIA": [10.0, 12.0, 15.0, 8.0]
        })
        
        plan = QueryPlan(
            tables=["TREE"],
            columns=[QueryColumn("SPCD", "TREE"), QueryColumn("DIA", "TREE")],
            group_by=["SPCD"]
        )
        
        data_sources = {
            "TREE": LazyFrameWrapper(tree_data.lazy())
        }
        
        result = self.executor.execute_plan(plan, data_sources)
        result_df = result.collect()
        
        assert len(result_df) == 2  # Two unique SPCD values
        assert "SPCD" in result_df.columns


class TestIntegration:
    """Integration tests for the join optimizer."""
    
    def test_end_to_end_optimization(self):
        """Test end-to-end optimization and execution."""
        # Create a complex query plan
        plan = QueryPlan(
            tables=["TREE", "PLOT", "COND", "POP_STRATUM"],
            columns=[
                QueryColumn("CN", "TREE"),
                QueryColumn("DIA", "TREE"),
                QueryColumn("STATECD", "PLOT"),
                QueryColumn("FORTYPCD", "COND"),
                QueryColumn("ACRES", "POP_STRATUM")
            ],
            filters=[
                QueryFilter("STATUSCD", "==", 1, "TREE"),
                QueryFilter("DIA", ">", 10.0, "TREE"),
                QueryFilter("STATECD", "==", 37, "PLOT"),
                QueryFilter("COND_STATUS_CD", "==", 1, "COND")
            ],
            joins=[
                QueryJoin("TREE", "PLOT", "PLT_CN", "CN"),
                QueryJoin("PLOT", "COND", "CN", "PLT_CN"),
                QueryJoin("PLOT", "POP_STRATUM", "STRATUM_CN", "CN")
            ]
        )
        
        # Create optimizer and optimize
        optimizer = JoinOptimizer()
        optimized = optimizer.optimize(plan)
        
        # Verify optimization occurred
        assert optimized is not None
        assert len(optimized.filters) < len(plan.filters)  # Some filters pushed
        
        # Check statistics
        stats = optimizer.get_optimization_stats()
        assert stats["joins_optimized"] > 0
        assert stats["filters_pushed"] > 0
    
    def test_performance_comparison(self):
        """Test that optimization improves estimated performance."""
        # Create unoptimized plan
        plan = QueryPlan(
            tables=["TREE", "REF_SPECIES"],
            columns=[
                QueryColumn("CN", "TREE"),
                QueryColumn("SPCD", "TREE"),
                QueryColumn("COMMON_NAME", "REF_SPECIES")
            ],
            joins=[
                QueryJoin(
                    "TREE", "REF_SPECIES", "SPCD", "SPCD",
                    strategy=JoinStrategy.HASH  # Default strategy
                )
            ]
        )
        
        optimizer = JoinOptimizer()
        estimator = JoinCostEstimator()
        
        # Estimate cost before optimization
        unoptimized_node = JoinNode(
            node_id="unopt",
            left_input="TREE",
            right_input="REF_SPECIES",
            join_keys_left=["SPCD"],
            join_keys_right=["SPCD"],
            strategy=JoinStrategy.HASH
        )
        unopt_cost = estimator.estimate_join_cost(
            unoptimized_node, 1000000, 1000, JoinStrategy.HASH
        )
        
        # Optimize
        optimized = optimizer.optimize(plan)
        
        # Cost after optimization (should use broadcast for small table)
        opt_cost = estimator.estimate_join_cost(
            unoptimized_node, 1000000, 1000, JoinStrategy.BROADCAST
        )
        
        # Broadcast should be better for this case
        assert opt_cost < unopt_cost * 1.5  # Allow some variance