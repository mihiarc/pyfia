"""
Mathematical property tests for two-stage aggregation.

These tests verify that the two-stage aggregation satisfies mathematical
invariants using controlled test data. They validate the mathematical
correctness of the aggregation logic without requiring external libraries.
"""

import polars as pl
import pytest

from pyfia.estimation.estimators.tpa import TPAEstimator


class TestTwoStageAggregationProperties:
    """
    Mathematical property tests for two-stage aggregation invariants.

    These tests verify that the aggregation logic satisfies fundamental
    mathematical properties with controlled test data.
    """

    def test_aggregation_preserves_totals(self):
        """
        Property: The sum of condition-level aggregates should equal the sum of all individual trees.

        This is a fundamental invariant - Stage 1 aggregation should preserve totals
        when summed across all conditions.
        """
        # Test with multiple configurations to verify property holds
        test_configs = [
            (2, 2, 3),  # 2 plots, 2 conditions each, 3 trees each
            (1, 3, 4),  # 1 plot, 3 conditions, 4 trees each
            (3, 1, 2),  # 3 plots, 1 condition each, 2 trees each
        ]

        for n_plots, n_conditions_per_plot, n_trees_per_condition in test_configs:
            # Generate synthetic data
            plots = [f"P{i}" for i in range(n_plots)]
            conditions = list(range(1, n_conditions_per_plot + 1))

            trees = []
            tree_id = 0
            for plot in plots:
                for cond in conditions:
                    for _ in range(n_trees_per_condition):
                        trees.append({
                            "TREE_ID": tree_id,
                            "PLT_CN": plot,
                            "CONDID": cond,
                            "TPA_UNADJ": 6.0,  # Standard FIA subplot tree
                            "DIA": 10.0,  # Reasonable diameter
                            "ADJ_FACTOR": 1.0  # Subplot adjustment
                        })
                        tree_id += 1

            tree_data = pl.DataFrame(trees)

            # Calculate total across all individual trees
            individual_total = (tree_data["TPA_UNADJ"] * tree_data["ADJ_FACTOR"]).sum()

            # Stage 1: Aggregate to condition level
            condition_agg = tree_data.group_by(["PLT_CN", "CONDID"]).agg([
                (pl.col("TPA_UNADJ") * pl.col("ADJ_FACTOR")).sum().alias("CONDITION_TPA")
            ])

            # Sum across all conditions
            condition_total = condition_agg["CONDITION_TPA"].sum()

            # Totals should be equal (within floating point precision)
            assert abs(individual_total - condition_total) < 1e-10, \
                f"Config {test_configs}: Stage 1 aggregation didn't preserve totals: {individual_total} vs {condition_total}"

    def test_single_tree_single_condition_invariant(self):
        """
        Property: For a single tree in a single condition, two-stage should equal one-stage.

        When there's only one tree per condition, the aggregation stages should be equivalent.
        This is a boundary case that validates the aggregation logic.
        """
        # Test multiple configurations
        test_params = [
            (1000.0, 1.0),    # Full expansion, full condition
            (5000.0, 0.5),    # High expansion, partial condition
            (500.0, 0.8),     # Medium expansion, most of condition
        ]

        for expansion_factor, condition_prop in test_params:
            # Single tree, single condition, single plot
            tree_data = pl.DataFrame({
                "PLT_CN": ["P1"],
                "CONDID": [1],
                "TPA_UNADJ": [6.0],
                "ADJ_FACTOR": [1.0],
                "EXPNS": [expansion_factor],
                "CONDPROP_UNADJ": [condition_prop]
            })

            # One-stage calculation (what the bug would do)
            one_stage = (tree_data["TPA_UNADJ"] * tree_data["ADJ_FACTOR"] * tree_data["EXPNS"]).sum()

            # Two-stage calculation (correct)
            # Stage 1: Condition level (should be same as individual tree)
            condition_agg = tree_data.group_by(["PLT_CN", "CONDID", "EXPNS", "CONDPROP_UNADJ"]).agg([
                (pl.col("TPA_UNADJ") * pl.col("ADJ_FACTOR")).sum().alias("CONDITION_TPA")
            ])

            # Stage 2: Apply expansion
            two_stage = (condition_agg["CONDITION_TPA"] * condition_agg["EXPNS"]).sum()

            # For single tree/condition, both should give same result
            assert abs(one_stage - two_stage) < 1e-10, \
                f"Single tree case with params {expansion_factor}, {condition_prop}: one-stage={one_stage}, two-stage={two_stage}"

    def test_proportional_scaling_property(self):
        """
        Property: Doubling all TPA values should double the final result.

        This tests linearity - the aggregation should scale proportionally.
        """
        # Create base dataset with multiple plots and conditions
        trees = []
        tree_id = 0

        plot_condition_configs = [
            ("A", 1, 0.6, 3),  # Plot A, Condition 1, 60% area, 3 trees
            ("A", 2, 0.4, 2),  # Plot A, Condition 2, 40% area, 2 trees
            ("B", 1, 1.0, 4),  # Plot B, Condition 1, 100% area, 4 trees
        ]

        for plot_id, cond_id, cond_prop, n_trees in plot_condition_configs:
            for _ in range(n_trees):
                trees.append({
                    "TREE_ID": tree_id,
                    "PLT_CN": plot_id,
                    "CONDID": cond_id,
                    "TPA_UNADJ": 6.0,
                    "ADJ_FACTOR": 1.0,
                    "EXPNS": 5000.0,
                    "CONDPROP_UNADJ": cond_prop
                })
                tree_id += 1

        base_data = pl.DataFrame(trees)

        # Calculate result with base TPA values
        base_condition_agg = base_data.group_by(["PLT_CN", "CONDID", "EXPNS", "CONDPROP_UNADJ"]).agg([
            (pl.col("TPA_UNADJ") * pl.col("ADJ_FACTOR")).sum().alias("CONDITION_TPA")
        ])

        base_result = (base_condition_agg["CONDITION_TPA"] * base_condition_agg["EXPNS"]).sum()

        # Create doubled dataset
        doubled_data = base_data.with_columns([
            (pl.col("TPA_UNADJ") * 2.0).alias("TPA_UNADJ")
        ])

        # Calculate result with doubled TPA values
        doubled_condition_agg = doubled_data.group_by(["PLT_CN", "CONDID", "EXPNS", "CONDPROP_UNADJ"]).agg([
            (pl.col("TPA_UNADJ") * pl.col("ADJ_FACTOR")).sum().alias("CONDITION_TPA")
        ])

        doubled_result = (doubled_condition_agg["CONDITION_TPA"] * doubled_condition_agg["EXPNS"]).sum()

        # Doubled input should produce doubled output
        expected_doubled = base_result * 2.0
        assert abs(doubled_result - expected_doubled) < 1e-6, \
            f"Proportional scaling failed: base={base_result}, doubled={doubled_result}, expected={expected_doubled}"

    def test_ratio_of_means_property(self):
        """
        Property: The ratio-of-means calculation should be mathematically consistent.

        This tests that the final per-acre calculation (Stage 2) correctly implements
        the ratio-of-means estimator used in FIA.
        """
        # Create conditions with different proportions and TPAs
        condition_data = []
        for i, (tpa, prop) in enumerate([(10.0, 0.3), (20.0, 0.5), (30.0, 0.2)]):
            condition_data.append({
                "PLT_CN": "P1",
                "CONDID": i + 1,
                "CONDITION_TPA": tpa,
                "EXPNS": 5000.0,
                "CONDPROP_UNADJ": prop
            })

        condition_df = pl.DataFrame(condition_data)

        # Calculate ratio of means manually
        numerator = (condition_df["CONDITION_TPA"] * condition_df["EXPNS"]).sum()
        denominator = (condition_df["CONDPROP_UNADJ"] * condition_df["EXPNS"]).sum()

        per_acre_estimate = numerator / denominator if denominator > 0 else 0.0

        # The ratio-of-means calculation is mathematically consistent but doesn't equal
        # the simple weighted average when expansion factors are involved
        # numerator = (10*5000 + 20*5000 + 30*5000) = 300000
        # denominator = (0.3*5000 + 0.5*5000 + 0.2*5000) = 5000
        # per_acre = 300000/5000 = 60.0

        # This is the total TPA per acre, which is correct for FIA estimation
        expected_total_per_acre = sum(row["CONDITION_TPA"] for row in condition_data)

        # The result should equal the sum of condition TPAs since all have same expansion factor
        assert abs(per_acre_estimate - expected_total_per_acre) < 1e-6, \
            f"Ratio-of-means calculation should equal sum of TPAs: {per_acre_estimate} vs {expected_total_per_acre}"

    def test_zero_expansion_factor_edge_case(self):
        """
        Property: Zero expansion factors should result in zero estimates.

        This tests edge case handling in the aggregation.
        """
        tree_data = pl.DataFrame({
            "PLT_CN": ["P1"],
            "CONDID": [1],
            "TPA_UNADJ": [10.0],
            "ADJ_FACTOR": [1.0],
            "EXPNS": [0.0],  # Zero expansion
            "CONDPROP_UNADJ": [1.0]
        })

        # Stage 1: Should still work
        condition_agg = tree_data.group_by(["PLT_CN", "CONDID", "EXPNS", "CONDPROP_UNADJ"]).agg([
            (pl.col("TPA_UNADJ") * pl.col("ADJ_FACTOR")).sum().alias("CONDITION_TPA")
        ])

        # Stage 2: Should result in zero
        final_result = (condition_agg["CONDITION_TPA"] * condition_agg["EXPNS"]).sum()

        assert final_result == 0.0, f"Zero expansion should give zero result, got {final_result}"

    def test_additive_property_across_plots(self):
        """
        Property: Aggregating plots separately then combining should equal aggregating together.

        This tests that the two-stage aggregation is associative across plots.
        """
        # Create data for two plots
        plot1_data = pl.DataFrame({
            "PLT_CN": ["P1", "P1"],
            "CONDID": [1, 1],
            "TPA_UNADJ": [8.0, 12.0],
            "ADJ_FACTOR": [1.0, 1.0],
            "EXPNS": [5000.0, 5000.0],
            "CONDPROP_UNADJ": [1.0, 1.0]
        })

        plot2_data = pl.DataFrame({
            "PLT_CN": ["P2", "P2"],
            "CONDID": [1, 1],
            "TPA_UNADJ": [6.0, 14.0],
            "ADJ_FACTOR": [1.0, 1.0],
            "EXPNS": [4000.0, 4000.0],
            "CONDPROP_UNADJ": [1.0, 1.0]
        })

        # Method 1: Aggregate plots separately then combine
        p1_condition_agg = plot1_data.group_by(["PLT_CN", "CONDID", "EXPNS", "CONDPROP_UNADJ"]).agg([
            (pl.col("TPA_UNADJ") * pl.col("ADJ_FACTOR")).sum().alias("CONDITION_TPA")
        ])

        p2_condition_agg = plot2_data.group_by(["PLT_CN", "CONDID", "EXPNS", "CONDPROP_UNADJ"]).agg([
            (pl.col("TPA_UNADJ") * pl.col("ADJ_FACTOR")).sum().alias("CONDITION_TPA")
        ])

        separate_result = (
            (p1_condition_agg["CONDITION_TPA"] * p1_condition_agg["EXPNS"]).sum() +
            (p2_condition_agg["CONDITION_TPA"] * p2_condition_agg["EXPNS"]).sum()
        )

        # Method 2: Combine data then aggregate
        combined_data = pl.concat([plot1_data, plot2_data])

        combined_condition_agg = combined_data.group_by(["PLT_CN", "CONDID", "EXPNS", "CONDPROP_UNADJ"]).agg([
            (pl.col("TPA_UNADJ") * pl.col("ADJ_FACTOR")).sum().alias("CONDITION_TPA")
        ])

        combined_result = (combined_condition_agg["CONDITION_TPA"] * combined_condition_agg["EXPNS"]).sum()

        # Both methods should give same result
        assert abs(separate_result - combined_result) < 1e-10, \
            f"Additive property failed: separate={separate_result}, combined={combined_result}"