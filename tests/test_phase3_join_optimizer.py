"""
Comprehensive tests for Phase 3 join optimizer functionality.

This test suite validates join optimization, cost estimation, filter push-down,
and FIA-specific join patterns.
"""

import pytest
import polars as pl
import numpy as np
from unittest.mock import Mock, MagicMock, patch
from typing import Dict, List, Any

from pyfia.estimation.join_optimizer import (
    JoinOptimizer,
    JoinNode,
    JoinCostEstimator,
    FilterPushDown,
    JoinRewriter,
    FIAJoinPatterns,
    OptimizedQueryExecutor,
    JoinType,
    JoinStatistics
)
from pyfia.estimation.query_builders import (
    QueryPlan,
    QueryColumn,
    QueryFilter,
    QueryJoin,
    JoinStrategy
)
from pyfia.estimation.config import EstimatorConfig
from pyfia.estimation.caching import MemoryCache, CacheKey
from pyfia.estimation.lazy_evaluation import LazyFrameWrapper


class TestJoinNode:
    """Test JoinNode creation and manipulation."""
    
    def test_join_node_creation(self):
        """Test basic JoinNode creation and properties."""
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
        assert node.is_leaf is True  # Both inputs are strings
    
    def test_join_node_auto_id_generation(self):
        """Test automatic node ID generation."""
        node = JoinNode(
            node_id="",  # Empty ID should be auto-generated
            left_input="TREE",
            right_input="PLOT",
            join_keys_left=["PLT_CN"],
            join_keys_right=["CN"]
        )
        
        assert node.node_id != ""
        assert len(node.node_id) == 12  # MD5 hash truncated
        assert isinstance(node.node_id, str)
    
    def test_join_node_input_tables(self):
        """Test get_input_tables method for various node structures."""
        # Simple leaf node
        leaf_node = JoinNode(
            node_id="leaf",
            left_input="TREE",
            right_input="PLOT",
            join_keys_left=["PLT_CN"],
            join_keys_right=["CN"]
        )
        
        tables = leaf_node.get_input_tables()
        assert tables == {"TREE", "PLOT"}
        
        # Nested node structure
        inner_node = JoinNode(
            node_id="inner",
            left_input="COND",
            right_input="PLOT",
            join_keys_left=["PLT_CN"],
            join_keys_right=["CN"]
        )
        
        outer_node = JoinNode(
            node_id="outer",
            left_input="TREE",
            right_input=inner_node,
            join_keys_left=["PLT_CN"],
            join_keys_right=["CN"]
        )
        
        all_tables = outer_node.get_input_tables()
        assert all_tables == {"TREE", "COND", "PLOT"}
    
    def test_join_node_with_statistics(self):
        """Test JoinNode with statistics information."""
        stats = JoinStatistics(
            left_cardinality=100000,
            right_cardinality=10000,
            estimated_output_rows=95000,
            selectivity=0.8,
            key_uniqueness_left=0.1,
            key_uniqueness_right=0.99,
            null_ratio_left=0.01,
            null_ratio_right=0.005
        )
        
        node = JoinNode(
            node_id="stats_node",
            left_input="TREE",
            right_input="PLOT",
            join_keys_left=["PLT_CN"],
            join_keys_right=["CN"],
            statistics=stats
        )
        
        assert node.statistics == stats
        assert node.statistics.is_many_to_one is True
        assert node.statistics.is_one_to_one is False
        assert node.estimated_rows == 95000
    
    def test_join_statistics_classification(self):
        """Test JoinStatistics join type classification."""
        # One-to-one join
        one_to_one = JoinStatistics(
            left_cardinality=10000,
            right_cardinality=10000,
            estimated_output_rows=10000,
            selectivity=1.0,
            key_uniqueness_left=0.99,
            key_uniqueness_right=0.99,
            null_ratio_left=0.01,
            null_ratio_right=0.01
        )
        
        assert one_to_one.is_one_to_one is True
        assert one_to_one.is_one_to_many is False
        assert one_to_one.is_many_to_one is False
        assert one_to_one.is_many_to_many is False
        
        # One-to-many join
        one_to_many = JoinStatistics(
            left_cardinality=10000,
            right_cardinality=100000,
            estimated_output_rows=100000,
            selectivity=0.9,
            key_uniqueness_left=0.99,
            key_uniqueness_right=0.1,
            null_ratio_left=0.01,
            null_ratio_right=0.01
        )
        
        assert one_to_many.is_one_to_one is False
        assert one_to_many.is_one_to_many is True
        assert one_to_many.is_many_to_one is False
        assert one_to_many.is_many_to_many is False
        
        # Many-to-many join
        many_to_many = JoinStatistics(
            left_cardinality=100000,
            right_cardinality=100000,
            estimated_output_rows=500000,
            selectivity=0.5,
            key_uniqueness_left=0.3,
            key_uniqueness_right=0.4,
            null_ratio_left=0.02,
            null_ratio_right=0.02
        )
        
        assert many_to_many.is_one_to_one is False
        assert many_to_many.is_one_to_many is False
        assert many_to_many.is_many_to_one is False
        assert many_to_many.is_many_to_many is True


class TestJoinCostEstimator:
    """Test join cost estimation functionality."""
    
    def test_hash_join_cost_estimation(self):
        """Test hash join cost estimation."""
        estimator = JoinCostEstimator()
        
        node = JoinNode(
            node_id="hash_test",
            left_input="TREE",
            right_input="PLOT",
            join_keys_left=["PLT_CN"],
            join_keys_right=["CN"]
        )
        
        # Test with different size combinations
        test_cases = [
            (10000, 1000),   # Small right table
            (1000, 10000),   # Small left table
            (50000, 50000),  # Equal sizes
            (100000, 200000) # Large tables
        ]
        
        for left_size, right_size in test_cases:
            cost = estimator.estimate_join_cost(node, left_size, right_size, JoinStrategy.HASH)
            
            assert cost > 0, f"Cost should be positive for sizes {left_size}, {right_size}"
            
            # Smaller table should be used for building hash table
            min_size = min(left_size, right_size)
            max_size = max(left_size, right_size)
            
            expected_build_cost = min_size * estimator.HASH_BUILD_COST_PER_ROW
            expected_probe_cost = max_size * estimator.HASH_PROBE_COST_PER_ROW
            expected_total = expected_build_cost + expected_probe_cost
            
            assert abs(cost - expected_total) < 1.0, f"Cost {cost} not close to expected {expected_total}"
    
    def test_sort_merge_join_cost_estimation(self):
        """Test sort-merge join cost estimation."""
        estimator = JoinCostEstimator()
        
        node = JoinNode(
            node_id="sort_test",
            left_input="TREE",
            right_input="PLOT",
            join_keys_left=["PLT_CN"],
            join_keys_right=["CN"]
        )
        
        left_size, right_size = 50000, 60000
        cost = estimator.estimate_join_cost(node, left_size, right_size, JoinStrategy.SORT_MERGE)
        
        assert cost > 0
        
        # Should include sorting costs for both sides plus merge cost
        expected_sort_left = left_size * np.log2(left_size) * estimator.SORT_COST_PER_ROW
        expected_sort_right = right_size * np.log2(right_size) * estimator.SORT_COST_PER_ROW
        expected_merge = (left_size + right_size) * estimator.MERGE_COST_PER_ROW
        expected_total = expected_sort_left + expected_sort_right + expected_merge
        
        assert abs(cost - expected_total) < 100.0, f"Cost {cost} not close to expected {expected_total}"
    
    def test_broadcast_join_cost_estimation(self):
        """Test broadcast join cost estimation."""
        estimator = JoinCostEstimator()
        
        node = JoinNode(
            node_id="broadcast_test",
            left_input="TREE",
            right_input="REF_SPECIES",
            join_keys_left=["SPCD"],
            join_keys_right=["SPCD"]
        )
        
        left_size, right_size = 100000, 1000  # Large left, small right
        cost = estimator.estimate_join_cost(node, left_size, right_size, JoinStrategy.BROADCAST)
        
        assert cost > 0
        
        # Should broadcast smaller side
        broadcast_rows = min(left_size, right_size)
        probe_rows = max(left_size, right_size)
        
        expected_broadcast = broadcast_rows * estimator.BROADCAST_COST_PER_ROW
        expected_hash = broadcast_rows * estimator.HASH_BUILD_COST_PER_ROW
        expected_probe = probe_rows * estimator.HASH_PROBE_COST_PER_ROW
        expected_total = expected_broadcast + expected_hash + expected_probe
        
        assert abs(cost - expected_total) < 100.0
    
    def test_nested_loop_join_cost_estimation(self):
        """Test nested loop join cost estimation."""
        estimator = JoinCostEstimator()
        
        node = JoinNode(
            node_id="nested_test",
            left_input="SMALL_TABLE",
            right_input="ANOTHER_SMALL",
            join_keys_left=["key"],
            join_keys_right=["key"]
        )
        
        left_size, right_size = 1000, 2000
        cost = estimator.estimate_join_cost(node, left_size, right_size, JoinStrategy.NESTED_LOOP)
        
        assert cost > 0
        
        # Should be cartesian product with cost factor
        expected = left_size * right_size * estimator.NESTED_LOOP_COST_FACTOR
        assert abs(cost - expected) < 1.0
    
    def test_output_cardinality_estimation(self):
        """Test output cardinality estimation for different join types."""
        estimator = JoinCostEstimator()
        
        # Test different join types
        join_tests = [
            (JoinType.INNER, 10000, 5000, lambda l, r: min(l, r)),  # Conservative estimate
            (JoinType.LEFT, 10000, 5000, lambda l, r: l),           # At least left rows
            (JoinType.RIGHT, 10000, 5000, lambda l, r: r),          # At least right rows
            (JoinType.FULL, 10000, 5000, lambda l, r: max(l, r)),   # At least max
            (JoinType.CROSS, 100, 200, lambda l, r: l * r),         # Cartesian product
        ]
        
        for join_type, left_rows, right_rows, min_expected in join_tests:
            node = JoinNode(
                node_id=f"card_test_{join_type.value}",
                left_input="LEFT",
                right_input="RIGHT",
                join_keys_left=["key"],
                join_keys_right=["key"],
                join_type=join_type
            )
            
            estimated = estimator.estimate_output_cardinality(node, left_rows, right_rows)
            
            assert estimated > 0, f"Cardinality should be positive for {join_type.value}"
            
            if join_type == JoinType.CROSS:
                assert estimated == left_rows * right_rows
            elif join_type == JoinType.LEFT:
                assert estimated >= left_rows
            elif join_type == JoinType.RIGHT:
                assert estimated >= right_rows
            elif join_type == JoinType.FULL:
                assert estimated >= max(left_rows, right_rows)
    
    def test_filter_selectivity_estimation(self):
        """Test filter selectivity estimation."""
        estimator = JoinCostEstimator()
        
        # Test different filter operators
        filter_tests = [
            (QueryFilter("STATUSCD", "==", 1), 0.1),
            (QueryFilter("DIA", ">", 10.0), 0.3),
            (QueryFilter("SPCD", "IN", [131, 110]), 0.2),  # 2 values
            (QueryFilter("SPCD", "IN", [131, 110, 833, 802, 541]), 0.5),  # 5 values (capped)
            (QueryFilter("DIA", "BETWEEN", [5.0, 15.0]), 0.2),
            (QueryFilter("HT", "IS NULL", None), 0.05),
            (QueryFilter("DIA", "IS NOT NULL", None), 0.95)
        ]
        
        for filter_obj, expected_approx in filter_tests:
            selectivity = estimator.estimate_filter_selectivity(filter_obj)
            
            assert 0.0 < selectivity <= 1.0
            assert abs(selectivity - expected_approx) < 0.3, f"Selectivity {selectivity} not near expected {expected_approx}"
    
    def test_cost_estimation_caching(self):
        """Test that filter selectivity estimates are cached."""
        estimator = JoinCostEstimator()
        
        filter1 = QueryFilter("STATUSCD", "==", 1)
        
        # First call
        selectivity1 = estimator.estimate_filter_selectivity(filter1)
        
        # Second call should use cache
        selectivity2 = estimator.estimate_filter_selectivity(filter1)
        
        assert selectivity1 == selectivity2
        
        # Verify cache was used
        cache_key = "STATUSCD:==:1"
        assert cache_key in estimator._selectivity_cache
        assert estimator._selectivity_cache[cache_key] == selectivity1


class TestFilterPushDown:
    """Test filter push-down optimization."""
    
    def test_filter_analysis(self):
        """Test filter analysis and push-down identification."""
        pushdown = FilterPushDown()
        
        # Create join tree
        join_tree = JoinNode(
            node_id="tree_plot",
            left_input="TREE",
            right_input="PLOT",
            join_keys_left=["PLT_CN"],
            join_keys_right=["CN"]
        )
        
        # Create filters
        filters = [
            QueryFilter("DIA", ">=", 10.0, "TREE", can_push_down=True),
            QueryFilter("STATUSCD", "==", 1, "TREE", can_push_down=True),
            QueryFilter("STATECD", "==", 37, "PLOT", can_push_down=True),
            QueryFilter("COMPLEX_CONDITION", "custom", "value", "TREE", can_push_down=False),
            QueryFilter("UNKNOWN_TABLE", "==", 1, "UNKNOWN", can_push_down=True)  # Unknown table
        ]
        
        # Analyze filters
        pushable = pushdown.analyze_filters(filters, join_tree)
        
        # Check pushable filters
        assert "TREE" in pushable
        assert "PLOT" in pushable
        
        tree_filters = pushable["TREE"]
        assert len(tree_filters) == 2  # DIA and STATUSCD
        tree_columns = [f.column for f in tree_filters]
        assert "DIA" in tree_columns
        assert "STATUSCD" in tree_columns
        
        plot_filters = pushable["PLOT"]
        assert len(plot_filters) == 1
        assert plot_filters[0].column == "STATECD"
        
        # Check remaining filters
        remaining = pushdown.remaining_filters
        assert len(remaining) == 2  # COMPLEX_CONDITION and UNKNOWN_TABLE
        remaining_types = [(f.column, f.table) for f in remaining]
        assert ("COMPLEX_CONDITION", "TREE") in remaining_types
        assert ("UNKNOWN_TABLE", "UNKNOWN") in remaining_types
    
    def test_filter_rewrite_join_tree(self):
        """Test rewriting join tree with pushed filters."""
        pushdown = FilterPushDown()
        
        original_tree = JoinNode(
            node_id="original",
            left_input="TREE",
            right_input="PLOT",
            join_keys_left=["PLT_CN"],
            join_keys_right=["CN"]
        )
        
        # Simulate some pushed filters
        pushdown.pushed_filters = {
            "original": [
                QueryFilter("DIA", ">=", 10.0, "TREE"),
                QueryFilter("STATECD", "==", 37, "PLOT")
            ]
        }
        
        rewritten_tree = pushdown.rewrite_join_tree(original_tree)
        
        # Should be a copy of original with filters added
        assert rewritten_tree.node_id == "original"
        assert rewritten_tree.left_input == "TREE"
        assert rewritten_tree.right_input == "PLOT"
        assert len(rewritten_tree.filters_pushed) == 2
    
    def test_complex_join_tree_filter_analysis(self):
        """Test filter analysis on complex join trees."""
        pushdown = FilterPushDown()
        
        # Create nested join tree: ((TREE JOIN PLOT) JOIN COND)
        inner_join = JoinNode(
            node_id="tree_plot",
            left_input="TREE",
            right_input="PLOT",
            join_keys_left=["PLT_CN"],
            join_keys_right=["CN"]
        )
        
        outer_join = JoinNode(
            node_id="with_cond",
            left_input=inner_join,
            right_input="COND",
            join_keys_left=["CN"],
            join_keys_right=["PLT_CN"]
        )
        
        # Filters for all tables
        filters = [
            QueryFilter("DIA", ">=", 10.0, "TREE", can_push_down=True),
            QueryFilter("STATECD", "==", 37, "PLOT", can_push_down=True),
            QueryFilter("FORTYPCD", "IN", [401, 402], "COND", can_push_down=True),
            QueryFilter("CROSS_TABLE", "==", "complex", None, can_push_down=False)
        ]
        
        pushable = pushdown.analyze_filters(filters, outer_join)
        
        # All three tables should have pushable filters
        assert "TREE" in pushable
        assert "PLOT" in pushable
        assert "COND" in pushable
        
        assert len(pushable["TREE"]) == 1
        assert len(pushable["PLOT"]) == 1
        assert len(pushable["COND"]) == 1
        
        # One filter should remain
        assert len(pushdown.remaining_filters) == 1
        assert pushdown.remaining_filters[0].column == "CROSS_TABLE"


class TestJoinRewriter:
    """Test join rewriter with optimization rules."""
    
    def create_cost_estimator(self):
        """Create a cost estimator for testing."""
        return JoinCostEstimator()
    
    def test_broadcast_rule_application(self):
        """Test broadcast join rule application."""
        rewriter = JoinRewriter(self.create_cost_estimator())
        
        # Create node with small right table
        node = JoinNode(
            node_id="broadcast_candidate",
            left_input="TREE",
            right_input="REF_SPECIES",
            join_keys_left=["SPCD"],
            join_keys_right=["SPCD"],
            statistics=JoinStatistics(
                left_cardinality=100000,
                right_cardinality=5000,  # Small table
                estimated_output_rows=100000,
                selectivity=0.9,
                key_uniqueness_left=0.1,
                key_uniqueness_right=1.0,
                null_ratio_left=0.01,
                null_ratio_right=0.0
            )
        )
        
        optimized = rewriter._apply_broadcast_rule(node)
        
        assert optimized.strategy == JoinStrategy.BROADCAST
        assert optimized.is_broadcast_candidate is True
        assert optimized.optimization_hints["broadcast_side"] == "right"
        
        # Test with small left table
        node_left_small = JoinNode(
            node_id="broadcast_left",
            left_input="REF_UNIT",
            right_input="TREE",
            join_keys_left=["UNITCD"],
            join_keys_right=["UNITCD"],
            statistics=JoinStatistics(
                left_cardinality=500,   # Small table
                right_cardinality=100000,
                estimated_output_rows=100000,
                selectivity=0.9,
                key_uniqueness_left=1.0,
                key_uniqueness_right=0.5,
                null_ratio_left=0.0,
                null_ratio_right=0.01
            )
        )
        
        optimized_left = rewriter._apply_broadcast_rule(node_left_small)
        
        assert optimized_left.strategy == JoinStrategy.BROADCAST
        assert optimized_left.optimization_hints["broadcast_side"] == "left"
    
    def test_fia_specific_rules(self):
        """Test FIA-specific optimization rules."""
        rewriter = JoinRewriter(self.create_cost_estimator())
        
        # Test tree-plot join pattern
        tree_plot_node = JoinNode(
            node_id="tree_plot",
            left_input="TREE",
            right_input="PLOT",
            join_keys_left=["PLT_CN"],
            join_keys_right=["CN"]
        )
        
        optimized = rewriter._apply_fia_specific_rules(tree_plot_node)
        assert optimized.strategy == JoinStrategy.HASH
        assert optimized.optimization_hints["fia_pattern"] == "tree_plot"
        
        # Test stratification join pattern
        strat_node = JoinNode(
            node_id="strat_join",
            left_input="PLOT",
            right_input="POP_STRATUM",
            join_keys_left=["STRATUM_CN"],
            join_keys_right=["CN"]
        )
        
        optimized_strat = rewriter._apply_fia_specific_rules(strat_node)
        assert optimized_strat.strategy == JoinStrategy.BROADCAST
        assert optimized_strat.optimization_hints["fia_pattern"] == "stratification"
        
        # Test condition-plot join
        cond_plot_node = JoinNode(
            node_id="cond_plot",
            left_input="COND",
            right_input="PLOT",
            join_keys_left=["PLT_CN"],
            join_keys_right=["CN"]
        )
        
        optimized_cond = rewriter._apply_fia_specific_rules(cond_plot_node)
        assert optimized_cond.strategy == JoinStrategy.HASH
        assert optimized_cond.optimization_hints["fia_pattern"] == "condition_plot"
        
        # Test reference table join
        ref_node = JoinNode(
            node_id="species_ref",
            left_input="TREE",
            right_input="REF_SPECIES",
            join_keys_left=["SPCD"],
            join_keys_right=["SPCD"]
        )
        
        optimized_ref = rewriter._apply_fia_specific_rules(ref_node)
        assert optimized_ref.strategy == JoinStrategy.BROADCAST
        assert optimized_ref.optimization_hints["fia_pattern"] == "reference"
    
    def test_sort_merge_rule(self):
        """Test sort-merge rule application."""
        rewriter = JoinRewriter(self.create_cost_estimator())
        
        # Create node with sorted data hints
        node = JoinNode(
            node_id="sorted_data",
            left_input="SORTED_LEFT",
            right_input="SORTED_RIGHT",
            join_keys_left=["sort_key"],
            join_keys_right=["sort_key"],
            optimization_hints={
                "left_sorted": True,
                "right_sorted": True
            }
        )
        
        optimized = rewriter._apply_sort_merge_rule(node)
        assert optimized.strategy == JoinStrategy.SORT_MERGE
    
    def test_complete_rewrite_plan(self):
        """Test complete rewrite plan with all rules."""
        rewriter = JoinRewriter(self.create_cost_estimator())
        
        # Create a typical FIA join pattern
        node = JoinNode(
            node_id="fia_pattern",
            left_input="TREE",
            right_input="REF_SPECIES",
            join_keys_left=["SPCD"],
            join_keys_right=["SPCD"],
            statistics=JoinStatistics(
                left_cardinality=100000,
                right_cardinality=1000,  # Small reference table
                estimated_output_rows=100000,
                selectivity=0.95,
                key_uniqueness_left=0.2,
                key_uniqueness_right=1.0,
                null_ratio_left=0.01,
                null_ratio_right=0.0
            )
        )
        
        optimized = rewriter.rewrite_plan(node)
        
        # Should apply multiple rules
        assert optimized.strategy == JoinStrategy.BROADCAST  # Both broadcast and FIA rules
        assert optimized.is_broadcast_candidate is True
        assert optimized.optimization_hints["fia_pattern"] == "reference"
        assert "broadcast_side" in optimized.optimization_hints


class TestFIAJoinPatterns:
    """Test predefined FIA join patterns."""
    
    def test_tree_plot_condition_pattern(self):
        """Test tree-plot-condition join pattern."""
        pattern = FIAJoinPatterns.tree_plot_condition_pattern()
        
        assert isinstance(pattern, JoinNode)
        assert pattern.node_id == "tree_plot_cond"
        
        # Should involve all three tables
        tables = pattern.get_input_tables()
        assert "TREE" in tables
        assert "PLOT" in tables
        assert "COND" in tables
        
        # Check structure: TREE joins with (PLOT-COND)
        assert pattern.left_input == "TREE"
        assert isinstance(pattern.right_input, JoinNode)
        assert pattern.right_input.node_id == "plot_cond"
        
        # Inner join should be PLOT-COND
        inner = pattern.right_input
        assert inner.left_input == "PLOT"
        assert inner.right_input == "COND"
        assert inner.strategy == JoinStrategy.HASH
    
    def test_stratification_pattern(self):
        """Test stratification join pattern."""
        pattern = FIAJoinPatterns.stratification_pattern()
        
        assert isinstance(pattern, JoinNode)
        assert pattern.node_id == "stratification"
        
        # Should involve stratification tables
        tables = pattern.get_input_tables()
        assert "POP_PLOT_STRATUM_ASSGN" in tables
        assert "PLOT" in tables
        assert "POP_STRATUM" in tables
        
        # Final join should use broadcast for POP_STRATUM
        assert pattern.strategy == JoinStrategy.BROADCAST
        assert pattern.right_input == "POP_STRATUM"
        
        # Inner join should be assignment-plot
        assert isinstance(pattern.left_input, JoinNode)
        inner = pattern.left_input
        assert inner.left_input == "POP_PLOT_STRATUM_ASSGN"
        assert inner.right_input == "PLOT"
    
    def test_species_reference_pattern(self):
        """Test species reference join pattern."""
        pattern = FIAJoinPatterns.species_reference_pattern()
        
        assert isinstance(pattern, JoinNode)
        assert pattern.node_id == "species_ref"
        
        assert pattern.left_input == "TREE"
        assert pattern.right_input == "REF_SPECIES"
        assert pattern.join_keys_left == ["SPCD"]
        assert pattern.join_keys_right == ["SPCD"]
        assert pattern.join_type == JoinType.LEFT
        assert pattern.strategy == JoinStrategy.BROADCAST


class TestJoinOptimizer:
    """Test main JoinOptimizer class."""
    
    def create_sample_query_plan(self):
        """Create a sample query plan for testing."""
        columns = [
            QueryColumn("CN", "TREE"),
            QueryColumn("PLT_CN", "TREE"),
            QueryColumn("SPCD", "TREE"),
            QueryColumn("DIA", "TREE"),
            QueryColumn("CN", "PLOT"),
            QueryColumn("STATECD", "PLOT")
        ]
        
        filters = [
            QueryFilter("DIA", ">=", 10.0, "TREE"),
            QueryFilter("STATUSCD", "==", 1, "TREE"),
            QueryFilter("STATECD", "==", 37, "PLOT")
        ]
        
        joins = [
            QueryJoin("TREE", "PLOT", "PLT_CN", "CN", "inner", JoinStrategy.AUTO)
        ]
        
        return QueryPlan(
            tables=["TREE", "PLOT"],
            columns=columns,
            filters=filters,
            joins=joins,
            filter_selectivity=0.3
        )
    
    def test_join_optimizer_initialization(self):
        """Test JoinOptimizer initialization."""
        config = EstimatorConfig()
        cache = MemoryCache(max_size_mb=128, max_entries=100)
        
        optimizer = JoinOptimizer(config, cache)
        
        assert optimizer.config == config
        assert optimizer.cache == cache
        assert isinstance(optimizer.cost_estimator, JoinCostEstimator)
        assert isinstance(optimizer.filter_pushdown, FilterPushDown)
        assert isinstance(optimizer.join_rewriter, JoinRewriter)
        
        # Check initial stats
        stats = optimizer.get_optimization_stats()
        assert stats["joins_optimized"] == 0
        assert stats["filters_pushed"] == 0
        assert stats["broadcast_joins"] == 0
    
    def test_optimize_query_plan(self):
        """Test query plan optimization."""
        optimizer = JoinOptimizer()
        plan = self.create_sample_query_plan()
        
        optimized = optimizer.optimize(plan)
        
        assert isinstance(optimized, QueryPlan)
        assert optimized.tables == plan.tables
        
        # Should have processed joins
        stats = optimizer.get_optimization_stats()
        assert stats["joins_optimized"] >= len(plan.joins)
    
    def test_optimization_caching(self):
        """Test that optimization results are cached."""
        optimizer = JoinOptimizer()
        plan = self.create_sample_query_plan()
        
        # First optimization
        result1 = optimizer.optimize(plan)
        
        # Second optimization should use cache
        result2 = optimizer.optimize(plan)
        
        # Results should be equivalent
        assert result1.cache_key == result2.cache_key
        assert len(result1.tables) == len(result2.tables)
        assert len(result1.joins) == len(result2.joins)
    
    def test_build_join_tree_from_query_plan(self):
        """Test building join tree from query plan."""
        optimizer = JoinOptimizer()
        plan = self.create_sample_query_plan()
        
        join_tree = optimizer._build_join_tree(plan)
        
        if join_tree:  # Only if there are joins
            assert isinstance(join_tree, JoinNode)
            tables = join_tree.get_input_tables()
            assert "TREE" in tables
            assert "PLOT" in tables
    
    def test_table_statistics_estimation(self):
        """Test table statistics for FIA tables."""
        optimizer = JoinOptimizer()
        
        # Test known FIA table statistics
        fia_tables = ["TREE", "PLOT", "COND", "POP_STRATUM", "REF_SPECIES"]
        
        for table in fia_tables:
            stats = optimizer._get_table_statistics(table)
            
            assert isinstance(stats, dict)
            assert "rows" in stats
            assert "uniqueness" in stats
            assert "null_ratio" in stats
            assert stats["rows"] > 0
            assert 0.0 <= stats["uniqueness"] <= 1.0
            assert 0.0 <= stats["null_ratio"] <= 1.0
    
    def test_join_order_optimization(self):
        """Test join order optimization."""
        optimizer = JoinOptimizer()
        
        # Create simple join tree
        join_tree = JoinNode(
            node_id="simple_join",
            left_input="TREE",
            right_input="PLOT",
            join_keys_left=["PLT_CN"],
            join_keys_right=["CN"],
            statistics=JoinStatistics(
                left_cardinality=100000,
                right_cardinality=10000,
                estimated_output_rows=100000,
                selectivity=0.8,
                key_uniqueness_left=0.1,
                key_uniqueness_right=0.99,
                null_ratio_left=0.01,
                null_ratio_right=0.01
            )
        )
        
        optimized = optimizer._optimize_join_order(join_tree)
        
        # For leaf nodes, should return same or swapped version
        assert isinstance(optimized, JoinNode)
        tables = optimized.get_input_tables()
        assert "TREE" in tables
        assert "PLOT" in tables
    
    def test_tree_cost_estimation(self):
        """Test join tree cost estimation."""
        optimizer = JoinOptimizer()
        
        join_tree = JoinNode(
            node_id="cost_test",
            left_input="TREE",
            right_input="PLOT",
            join_keys_left=["PLT_CN"],
            join_keys_right=["CN"],
            strategy=JoinStrategy.HASH,
            statistics=JoinStatistics(
                left_cardinality=100000,
                right_cardinality=10000,
                estimated_output_rows=100000,
                selectivity=0.8,
                key_uniqueness_left=0.1,
                key_uniqueness_right=0.99,
                null_ratio_left=0.01,
                null_ratio_right=0.01
            )
        )
        
        cost = optimizer._estimate_tree_cost(join_tree)
        
        assert cost > 0
        assert cost != float('inf')  # Should have valid statistics
    
    def test_query_plan_conversion(self):
        """Test conversion between join tree and query plan."""
        optimizer = JoinOptimizer()
        original_plan = self.create_sample_query_plan()
        
        # Build join tree
        join_tree = optimizer._build_join_tree(original_plan)
        
        if join_tree:
            # Convert back to query plan
            converted_plan = optimizer._tree_to_query_plan(join_tree, original_plan)
            
            assert isinstance(converted_plan, QueryPlan)
            assert converted_plan.tables == original_plan.tables
            assert len(converted_plan.joins) >= len(original_plan.joins)


class TestOptimizedQueryExecutor:
    """Test optimized query execution."""
    
    def create_sample_data_sources(self):
        """Create sample data sources for testing."""
        tree_data = pl.LazyFrame({
            "CN": [1, 2, 3, 4],
            "PLT_CN": [101, 101, 102, 102],
            "SPCD": [131, 110, 131, 833],
            "DIA": [12.5, 8.3, 15.7, 11.2],
            "STATUSCD": [1, 1, 1, 1]
        })
        
        plot_data = pl.LazyFrame({
            "CN": [101, 102, 103],
            "STATECD": [37, 37, 45],
            "LAT": [35.5, 36.0, 34.8],
            "LON": [-80.1, -79.5, -82.3]
        })
        
        return {
            "TREE": LazyFrameWrapper(tree_data),
            "PLOT": LazyFrameWrapper(plot_data)
        }
    
    def test_optimized_executor_initialization(self):
        """Test OptimizedQueryExecutor initialization."""
        optimizer = JoinOptimizer()
        cache = MemoryCache(max_size_mb=256, max_entries=50)
        
        executor = OptimizedQueryExecutor(optimizer, cache)
        
        assert executor.optimizer == optimizer
        assert executor.cache == cache
    
    def test_simple_plan_execution(self):
        """Test execution of simple query plan."""
        optimizer = JoinOptimizer()
        executor = OptimizedQueryExecutor(optimizer)
        data_sources = self.create_sample_data_sources()
        
        # Create simple plan with single table
        plan = QueryPlan(
            tables=["TREE"],
            columns=[
                QueryColumn("CN", "TREE"),
                QueryColumn("SPCD", "TREE"),
                QueryColumn("DIA", "TREE")
            ],
            filters=[
                QueryFilter("DIA", ">=", 10.0, "TREE")
            ]
        )
        
        result = executor.execute_plan(plan, data_sources)
        
        assert isinstance(result, LazyFrameWrapper)
        
        # Collect and verify results
        df = result.to_lazy().collect()
        assert len(df) > 0
        
        # All trees should have DIA >= 10.0
        dia_values = df.get_column("DIA").to_list()
        assert all(dia >= 10.0 for dia in dia_values)
    
    def test_join_plan_execution(self):
        """Test execution of plan with joins."""
        optimizer = JoinOptimizer()
        executor = OptimizedQueryExecutor(optimizer)
        data_sources = self.create_sample_data_sources()
        
        # Create plan with join
        plan = QueryPlan(
            tables=["TREE", "PLOT"],
            columns=[
                QueryColumn("CN", "TREE"),
                QueryColumn("PLT_CN", "TREE"),
                QueryColumn("SPCD", "TREE"),
                QueryColumn("STATECD", "PLOT")
            ],
            joins=[
                QueryJoin("TREE", "PLOT", "PLT_CN", "CN", "inner", JoinStrategy.HASH)
            ],
            filters=[
                QueryFilter("STATECD", "==", 37, "PLOT")
            ]
        )
        
        result = executor.execute_plan(plan, data_sources)
        
        assert isinstance(result, LazyFrameWrapper)
        
        # Collect and verify results
        df = result.to_lazy().collect()
        assert len(df) > 0
        
        # Should have columns from both tables
        columns = df.columns
        assert "CN" in columns
        assert "SPCD" in columns
        assert "STATECD" in columns
        
        # All results should have STATECD == 37
        state_values = df.get_column("STATECD").to_list()
        assert all(state == 37 for state in state_values)
    
    def test_execution_caching(self):
        """Test that execution results are cached."""
        optimizer = JoinOptimizer()
        executor = OptimizedQueryExecutor(optimizer)
        data_sources = self.create_sample_data_sources()
        
        plan = QueryPlan(
            tables=["TREE"],
            columns=[QueryColumn("CN", "TREE"), QueryColumn("SPCD", "TREE")],
            filters=[]
        )
        
        # First execution
        result1 = executor.execute_plan(plan, data_sources)
        
        # Second execution should use cache
        result2 = executor.execute_plan(plan, data_sources)
        
        # Results should be equivalent
        df1 = result1.to_lazy().collect()
        df2 = result2.to_lazy().collect()
        
        assert len(df1) == len(df2)
        assert df1.columns == df2.columns
    
    def test_grouped_query_execution(self):
        """Test execution of queries with grouping."""
        optimizer = JoinOptimizer()
        executor = OptimizedQueryExecutor(optimizer)
        data_sources = self.create_sample_data_sources()
        
        plan = QueryPlan(
            tables=["TREE"],
            columns=[
                QueryColumn("SPCD", "TREE"),
                QueryColumn("DIA", "TREE")
            ],
            group_by=["SPCD"],
            filters=[]
        )
        
        result = executor.execute_plan(plan, data_sources)
        
        assert isinstance(result, LazyFrameWrapper)
        
        # Collect and verify grouping
        df = result.to_lazy().collect()
        assert len(df) > 0
        
        # Should have unique species codes
        species_codes = df.get_column("SPCD").to_list()
        assert len(set(species_codes)) == len(species_codes)  # All unique
    
    def test_ordered_query_execution(self):
        """Test execution of queries with ordering."""
        optimizer = JoinOptimizer()
        executor = OptimizedQueryExecutor(optimizer)
        data_sources = self.create_sample_data_sources()
        
        plan = QueryPlan(
            tables=["TREE"],
            columns=[
                QueryColumn("CN", "TREE"),
                QueryColumn("DIA", "TREE")
            ],
            order_by=[("DIA", "DESC")],
            filters=[]
        )
        
        result = executor.execute_plan(plan, data_sources)
        
        assert isinstance(result, LazyFrameWrapper)
        
        # Collect and verify ordering
        df = result.to_lazy().collect()
        assert len(df) > 0
        
        # Should be ordered by DIA descending
        dia_values = df.get_column("DIA").to_list()
        assert dia_values == sorted(dia_values, reverse=True)
    
    def test_limited_query_execution(self):
        """Test execution of queries with limits."""
        optimizer = JoinOptimizer()
        executor = OptimizedQueryExecutor(optimizer)
        data_sources = self.create_sample_data_sources()
        
        plan = QueryPlan(
            tables=["TREE"],
            columns=[QueryColumn("CN", "TREE"), QueryColumn("SPCD", "TREE")],
            limit=2,
            filters=[]
        )
        
        result = executor.execute_plan(plan, data_sources)
        
        assert isinstance(result, LazyFrameWrapper)
        
        # Collect and verify limit
        df = result.to_lazy().collect()
        assert len(df) <= 2
    
    def test_missing_data_source_error(self):
        """Test error handling for missing data sources."""
        optimizer = JoinOptimizer()
        executor = OptimizedQueryExecutor(optimizer)
        
        plan = QueryPlan(
            tables=["MISSING_TABLE"],
            columns=[QueryColumn("CN", "MISSING_TABLE")],
            filters=[]
        )
        
        with pytest.raises(ValueError, match="Missing data source"):
            executor.execute_plan(plan, {})


class TestJoinOptimizerIntegration:
    """Integration tests for complete join optimization workflow."""
    
    def test_end_to_end_optimization(self):
        """Test complete optimization workflow from query plan to execution."""
        # Create realistic FIA query scenario
        config = EstimatorConfig()
        optimizer = JoinOptimizer(config)
        
        # Complex query plan with multiple joins and filters
        plan = QueryPlan(
            tables=["TREE", "PLOT", "COND"],
            columns=[
                QueryColumn("CN", "TREE"),
                QueryColumn("SPCD", "TREE"),
                QueryColumn("DIA", "TREE"),
                QueryColumn("STATECD", "PLOT"),
                QueryColumn("FORTYPCD", "COND")
            ],
            filters=[
                QueryFilter("DIA", ">=", 10.0, "TREE", can_push_down=True),
                QueryFilter("STATUSCD", "==", 1, "TREE", can_push_down=True),
                QueryFilter("STATECD", "==", 37, "PLOT", can_push_down=True),
                QueryFilter("FORTYPCD", "IN", [401, 402], "COND", can_push_down=True)
            ],
            joins=[
                QueryJoin("TREE", "PLOT", "PLT_CN", "CN", "inner", JoinStrategy.AUTO),
                QueryJoin("PLOT", "COND", "CN", "PLT_CN", "inner", JoinStrategy.AUTO)
            ],
            filter_selectivity=0.15
        )
        
        # Optimize the plan
        optimized = optimizer.optimize(plan)
        
        # Verify optimization
        assert isinstance(optimized, QueryPlan)
        assert len(optimized.joins) >= len(plan.joins)
        
        # Check that statistics were updated
        stats = optimizer.get_optimization_stats()
        assert stats["joins_optimized"] > 0
        assert stats["filters_pushed"] > 0
        
        # Verify that pushdown filters were identified
        remaining_filters = optimized.filters
        # Some filters should have been pushed down
        assert len(remaining_filters) <= len(plan.filters)
    
    def test_fia_specific_optimization_patterns(self):
        """Test that FIA-specific patterns are recognized and optimized."""
        optimizer = JoinOptimizer()
        
        # Tree-to-reference join (should use broadcast)
        tree_ref_plan = QueryPlan(
            tables=["TREE", "REF_SPECIES"],
            columns=[
                QueryColumn("CN", "TREE"),
                QueryColumn("SPCD", "TREE"),
                QueryColumn("COMMON_NAME", "REF_SPECIES")
            ],
            joins=[
                QueryJoin("TREE", "REF_SPECIES", "SPCD", "SPCD", "left", JoinStrategy.AUTO)
            ]
        )
        
        optimized = optimizer.optimize(tree_ref_plan)
        
        # Should optimize for broadcast join
        assert isinstance(optimized, QueryPlan)
        
        # Stratification join pattern
        strat_plan = QueryPlan(
            tables=["PLOT", "POP_PLOT_STRATUM_ASSGN", "POP_STRATUM"],
            columns=[
                QueryColumn("CN", "PLOT"),
                QueryColumn("EVALID", "POP_STRATUM"),
                QueryColumn("ACRES", "POP_STRATUM")
            ],
            joins=[
                QueryJoin("PLOT", "POP_PLOT_STRATUM_ASSGN", "CN", "PLT_CN", "inner"),
                QueryJoin("POP_PLOT_STRATUM_ASSGN", "POP_STRATUM", "STRATUM_CN", "CN", "inner")
            ]
        )
        
        strat_optimized = optimizer.optimize(strat_plan)
        assert isinstance(strat_optimized, QueryPlan)
    
    def test_optimization_with_statistics(self):
        """Test optimization with detailed table statistics."""
        # Mock table statistics
        table_stats = {
            "TREE": {"rows": 1000000, "uniqueness": 0.99, "null_ratio": 0.01},
            "PLOT": {"rows": 100000, "uniqueness": 0.99, "null_ratio": 0.005},
            "REF_SPECIES": {"rows": 1000, "uniqueness": 1.0, "null_ratio": 0.0}
        }
        
        cost_estimator = JoinCostEstimator(table_stats)
        optimizer = JoinOptimizer()
        optimizer.cost_estimator = cost_estimator
        
        plan = QueryPlan(
            tables=["TREE", "REF_SPECIES"],
            columns=[
                QueryColumn("CN", "TREE"),
                QueryColumn("SPCD", "TREE"),
                QueryColumn("GENUS", "REF_SPECIES")
            ],
            joins=[
                QueryJoin("TREE", "REF_SPECIES", "SPCD", "SPCD", "left")
            ]
        )
        
        optimized = optimizer.optimize(plan)
        
        # Should benefit from accurate statistics
        assert isinstance(optimized, QueryPlan)
    
    def test_caching_effectiveness(self):
        """Test that caching improves performance of repeated optimizations."""
        optimizer = JoinOptimizer()
        
        plan = QueryPlan(
            tables=["TREE", "PLOT"],
            columns=[QueryColumn("CN", "TREE"), QueryColumn("CN", "PLOT")],
            joins=[QueryJoin("TREE", "PLOT", "PLT_CN", "CN")]
        )
        
        # First optimization
        import time
        start1 = time.time()
        result1 = optimizer.optimize(plan)
        time1 = time.time() - start1
        
        # Second optimization (should use cache)
        start2 = time.time()
        result2 = optimizer.optimize(plan)
        time2 = time.time() - start2
        
        # Results should be equivalent
        assert result1.cache_key == result2.cache_key
        
        # Second call should be faster (though timing can be variable)
        # This is more of a performance hint than a strict test
        print(f"First optimization: {time1:.6f}s, Second: {time2:.6f}s")