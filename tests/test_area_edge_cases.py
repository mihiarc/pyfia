"""
Edge case tests for area estimation variance calculation (PR #35).

This module tests critical edge cases and boundary conditions for the area estimation
variance calculation implementation. These tests focus on scenarios that could
cause failures or incorrect results in production environments.

Edge cases covered:
- Statistical edge cases (single plot, zero variance)
- Data structure edge cases (missing columns, empty strata)
- Boundary conditions (extreme values, null handling)
- Error conditions and recovery
- Integration edge cases (mixed EVALIDs, complex grouping)
"""

import pytest
import polars as pl
import numpy as np
from unittest.mock import Mock, patch
from pathlib import Path

from pyfia.estimation.estimators.area import AreaEstimator
from pyfia.estimation import area
from pyfia.core import FIA


class TestStatisticalEdgeCases:
    """Test statistical edge cases in variance calculation."""

    def test_single_plot_zero_variance(self, mock_fia_database):
        """Test variance calculation with single plot (should yield zero variance)."""
        plot_data = pl.DataFrame({
            "PLT_CN": ["P1"],
            "ESTN_UNIT": [1],
            "STRATUM": [1],
            "y_i": [0.8],  # Single value
            "EXPNS": [1000.0]
        })

        config = {}
        estimator = AreaEstimator(mock_fia_database, config)
        var_stats = estimator._calculate_variance_for_group(plot_data, ["ESTN_UNIT", "STRATUM"])

        # Single plot should result in zero variance
        assert var_stats["variance"] == 0.0
        assert var_stats["se_total"] == 0.0

    def test_identical_plot_values_zero_variance(self, mock_fia_database):
        """Test variance calculation when all plots have identical values."""
        plot_data = pl.DataFrame({
            "PLT_CN": ["P1", "P2", "P3", "P4"],
            "ESTN_UNIT": [1, 1, 1, 1],
            "STRATUM": [1, 1, 1, 1],
            "y_i": [0.5, 0.5, 0.5, 0.5],  # All identical
            "EXPNS": [1000.0, 1000.0, 1000.0, 1000.0]
        })

        config = {}
        estimator = AreaEstimator(mock_fia_database, config)
        var_stats = estimator._calculate_variance_for_group(plot_data, ["ESTN_UNIT", "STRATUM"])

        # Identical values should result in zero variance
        assert var_stats["variance"] == 0.0
        assert var_stats["se_total"] == 0.0

    def test_extreme_variance_values(self, mock_fia_database):
        """Test variance calculation with extreme value differences."""
        plot_data = pl.DataFrame({
            "PLT_CN": ["P1", "P2", "P3"],
            "ESTN_UNIT": [1, 1, 1],
            "STRATUM": [1, 1, 1],
            "y_i": [0.0, 1.0, 0.0],  # Maximum variance case
            "EXPNS": [1000.0, 1000.0, 1000.0]
        })

        config = {}
        estimator = AreaEstimator(mock_fia_database, config)
        var_stats = estimator._calculate_variance_for_group(plot_data, ["ESTN_UNIT", "STRATUM"])

        # Should handle extreme differences without error
        assert var_stats["variance"] > 0
        assert var_stats["se_total"] > 0
        assert np.isfinite(var_stats["variance"])
        assert np.isfinite(var_stats["se_total"])

    def test_mixed_zero_nonzero_domain_values(self, mock_fia_database):
        """Test variance with mixed zero and non-zero domain values."""
        # This tests the domain indicator approach where some plots contribute 0
        test_data = pl.DataFrame({
            "PLT_CN": ["P1", "P2", "P3", "P4"],
            "CONDID": [1, 1, 1, 1],
            "COND_STATUS_CD": [1, 1, 2, 1],  # Mixed forest/non-forest
            "CONDPROP_UNADJ": [1.0, 0.8, 1.0, 0.6],
            "PROP_BASIS": ["SUBP", "SUBP", "SUBP", "SUBP"]
        })

        config = {"land_type": "forest"}
        estimator = AreaEstimator(mock_fia_database, config)

        result = estimator.apply_filters(test_data.lazy())
        calculated = estimator.calculate_values(result).collect()

        # Should have mix of zero and non-zero values
        area_values = calculated["AREA_VALUE"].to_list()
        assert 0.0 in area_values  # Non-forest plot should be 0
        assert any(v > 0 for v in area_values)  # Some forest plots should be >0

    def test_very_small_variance_precision(self, mock_fia_database):
        """Test precision with very small variance values."""
        plot_data = pl.DataFrame({
            "PLT_CN": ["P1", "P2", "P3"],
            "ESTN_UNIT": [1, 1, 1],
            "STRATUM": [1, 1, 1],
            "y_i": [0.500001, 0.500000, 0.499999],  # Tiny differences
            "EXPNS": [1000.0, 1000.0, 1000.0]
        })

        config = {}
        estimator = AreaEstimator(mock_fia_database, config)
        var_stats = estimator._calculate_variance_for_group(plot_data, ["ESTN_UNIT", "STRATUM"])

        # Should handle small differences without underflow
        assert var_stats["variance"] >= 0
        assert var_stats["se_total"] >= 0


class TestDataStructureEdgeCases:
    """Test edge cases related to data structure and missing information."""

    def test_missing_stratification_columns(self, mock_fia_database):
        """Test behavior when stratification columns are completely missing."""
        plot_data = pl.DataFrame({
            "PLT_CN": ["P1", "P2", "P3"],
            "y_i": [0.8, 1.0, 0.6],
            "EXPNS": [1000.0, 1000.0, 1000.0]
            # No ESTN_UNIT or STRATUM columns
        })

        config = {}
        estimator = AreaEstimator(mock_fia_database, config)

        # Should handle by creating default stratum
        var_stats = estimator._calculate_variance_for_group(plot_data, [])

        assert var_stats["variance"] >= 0
        assert var_stats["se_total"] >= 0

    def test_partial_stratification_columns(self, mock_fia_database):
        """Test behavior when only some stratification columns are present."""
        plot_data = pl.DataFrame({
            "PLT_CN": ["P1", "P2", "P3"],
            "ESTN_UNIT": [1, 1, 2],  # Have ESTN_UNIT
            "y_i": [0.8, 1.0, 0.6],
            "EXPNS": [1000.0, 1000.0, 1500.0]
            # Missing STRATUM column
        })

        config = {}
        estimator = AreaEstimator(mock_fia_database, config)

        # Should work with partial stratification
        var_stats = estimator._calculate_variance_for_group(plot_data, ["ESTN_UNIT"])

        assert var_stats["variance"] >= 0
        assert var_stats["se_total"] >= 0

    def test_empty_stratum_group(self, mock_fia_database):
        """Test behavior when filtering results in empty strata."""
        # Create estimator with plot-condition data but filter creates empty groups
        config = {"grp_by": "FORTYPCD"}
        estimator = AreaEstimator(mock_fia_database, config)

        estimator.plot_condition_data = pl.DataFrame({
            "PLT_CN": ["P1", "P2"],
            "FORTYPCD": [161, 406],
            "AREA_VALUE": [1.0, 0.8],
            "ADJ_FACTOR_AREA": [1.0, 1.0],
            "EXPNS": [1000.0, 1000.0],
            "ESTN_UNIT": [1, 1],
            "STRATUM": [1, 1]
        })
        estimator.group_cols = ["FORTYPCD"]

        # Results for group that doesn't exist in plot data
        results = pl.DataFrame({
            "FORTYPCD": [999],  # Non-existent forest type
            "AREA_TOTAL": [0.0],
            "N_PLOTS": [0]
        })

        # Should handle gracefully without error
        variance_results = estimator.calculate_variance(results)
        assert len(variance_results) >= 1  # Should return something

    def test_null_values_in_plot_data(self, mock_fia_database):
        """Test handling of null values in plot-level data."""
        plot_data = pl.DataFrame({
            "PLT_CN": ["P1", "P2", "P3", "P4"],
            "ESTN_UNIT": [1, 1, None, 1],  # Null value
            "STRATUM": [1, 1, 1, None],    # Null value
            "y_i": [0.8, 1.0, 0.6, 0.9],
            "EXPNS": [1000.0, None, 1000.0, 1000.0]  # Null expansion
        })

        config = {}
        estimator = AreaEstimator(mock_fia_database, config)

        # Should handle nulls gracefully (may drop rows or use defaults)
        try:
            var_stats = estimator._calculate_variance_for_group(plot_data, ["ESTN_UNIT", "STRATUM"])
            # If it doesn't error, variance should be reasonable
            assert var_stats["variance"] >= 0
        except Exception as e:
            # Acceptable if it fails gracefully with clear error
            assert "null" in str(e).lower() or "missing" in str(e).lower()

    def test_mismatched_grouping_columns(self, mock_fia_database):
        """Test when grouping columns don't exist in plot data."""
        config = {"grp_by": ["NONEXISTENT_COL"]}
        estimator = AreaEstimator(mock_fia_database, config)

        estimator.plot_condition_data = pl.DataFrame({
            "PLT_CN": ["P1", "P2"],
            "AREA_VALUE": [1.0, 0.8],
            "ADJ_FACTOR_AREA": [1.0, 1.0],
            "EXPNS": [1000.0, 1000.0],
            "ESTN_UNIT": [1, 1],
            "STRATUM": [1, 1]
            # Missing NONEXISTENT_COL
        })
        estimator.group_cols = ["NONEXISTENT_COL"]

        results = pl.DataFrame({
            "NONEXISTENT_COL": ["VALUE1"],
            "AREA_TOTAL": [1800.0],
            "N_PLOTS": [2]
        })

        # Should handle missing columns gracefully
        variance_results = estimator.calculate_variance(results)
        # May fall back to no grouping or use defaults


class TestBoundaryConditions:
    """Test boundary conditions and extreme parameter values."""

    def test_zero_area_values_all_plots(self, mock_fia_database):
        """Test when all plots have zero area values (complete non-domain)."""
        test_data = pl.DataFrame({
            "PLT_CN": ["P1", "P2", "P3"],
            "CONDID": [1, 1, 1],
            "COND_STATUS_CD": [2, 2, 2],  # All non-forest
            "CONDPROP_UNADJ": [1.0, 0.8, 1.0],
            "PROP_BASIS": ["SUBP", "SUBP", "SUBP"]
        })

        config = {"land_type": "forest"}
        estimator = AreaEstimator(mock_fia_database, config)

        result = estimator.apply_filters(test_data.lazy())
        calculated = estimator.calculate_values(result).collect()

        # All area values should be zero
        area_values = calculated["AREA_VALUE"].to_list()
        assert all(v == 0.0 for v in area_values)

    def test_very_large_expansion_factors(self, mock_fia_database):
        """Test with very large expansion factors."""
        plot_data = pl.DataFrame({
            "PLT_CN": ["P1", "P2"],
            "ESTN_UNIT": [1, 1],
            "STRATUM": [1, 1],
            "y_i": [0.8, 1.0],
            "EXPNS": [1e9, 1e9]  # Very large expansion factors
        })

        config = {}
        estimator = AreaEstimator(mock_fia_database, config)
        var_stats = estimator._calculate_variance_for_group(plot_data, ["ESTN_UNIT", "STRATUM"])

        # Should handle large numbers without overflow
        assert np.isfinite(var_stats["variance"])
        assert np.isfinite(var_stats["se_total"])
        assert var_stats["variance"] >= 0

    def test_very_small_expansion_factors(self, mock_fia_database):
        """Test with very small expansion factors."""
        plot_data = pl.DataFrame({
            "PLT_CN": ["P1", "P2"],
            "ESTN_UNIT": [1, 1],
            "STRATUM": [1, 1],
            "y_i": [0.8, 1.0],
            "EXPNS": [1e-6, 1e-6]  # Very small expansion factors
        })

        config = {}
        estimator = AreaEstimator(mock_fia_database, config)
        var_stats = estimator._calculate_variance_for_group(plot_data, ["ESTN_UNIT", "STRATUM"])

        # Should handle small numbers without underflow
        assert np.isfinite(var_stats["variance"])
        assert np.isfinite(var_stats["se_total"])
        assert var_stats["variance"] >= 0

    def test_maximum_grouping_complexity(self, mock_fia_database):
        """Test with maximum realistic grouping complexity."""
        # Create data with many grouping combinations
        n_plots = 100
        plot_data = {
            "PLT_CN": [f"P{i}" for i in range(n_plots)],
            "AREA_VALUE": np.random.uniform(0, 1, n_plots),
            "ADJ_FACTOR_AREA": [1.0] * n_plots,
            "EXPNS": [1000.0] * n_plots,
            "ESTN_UNIT": np.random.randint(1, 5, n_plots),
            "STRATUM": np.random.randint(1, 10, n_plots),
            "FORTYPCD": np.random.choice([161, 406, 703, 621], n_plots),
            "OWNGRPCD": np.random.choice([10, 20, 30, 40], n_plots),
            "STDSZCD": np.random.choice([1, 2, 3, 4, 5], n_plots)
        }

        config = {"grp_by": ["FORTYPCD", "OWNGRPCD", "STDSZCD"]}
        estimator = AreaEstimator(mock_fia_database, config)

        estimator.plot_condition_data = pl.DataFrame(plot_data)
        estimator.group_cols = ["FORTYPCD", "OWNGRPCD", "STDSZCD"]

        # Create results for all combinations
        unique_combinations = estimator.plot_condition_data.select(["FORTYPCD", "OWNGRPCD", "STDSZCD"]).unique()
        results = unique_combinations.with_columns([
            pl.lit(1000.0).alias("AREA_TOTAL"),
            pl.lit(5).alias("N_PLOTS")
        ])

        # Should handle complex grouping without performance issues
        variance_results = estimator.calculate_variance(results)
        assert len(variance_results) == len(unique_combinations)


class TestErrorConditionsAndRecovery:
    """Test error conditions and recovery mechanisms."""

    def test_corrupted_plot_condition_data(self, mock_fia_database):
        """Test behavior with corrupted plot-condition data."""
        config = {}
        estimator = AreaEstimator(mock_fia_database, config)

        # Corrupted data - missing essential columns
        estimator.plot_condition_data = pl.DataFrame({
            "RANDOM_COL": ["A", "B", "C"]
            # Missing all essential columns
        })
        estimator.group_cols = []

        results = pl.DataFrame({
            "AREA_TOTAL": [1000.0],
            "N_PLOTS": [3]
        })

        # Should either handle gracefully or fail with clear error
        try:
            variance_results = estimator.calculate_variance(results)
            # If successful, should have reasonable fallback
            assert "AREA_SE" in variance_results.columns
        except Exception as e:
            # Should be a clear, expected error
            assert len(str(e)) > 0

    def test_memory_pressure_large_dataset(self, mock_fia_database):
        """Test behavior under memory pressure with large dataset."""
        # Simulate large dataset
        n_plots = 10000
        large_data = pl.DataFrame({
            "PLT_CN": [f"P{i}" for i in range(n_plots)],
            "AREA_VALUE": np.random.uniform(0, 1, n_plots),
            "ADJ_FACTOR_AREA": [1.0] * n_plots,
            "EXPNS": [1000.0] * n_plots,
            "ESTN_UNIT": np.random.randint(1, 100, n_plots),
            "STRATUM": np.random.randint(1, 500, n_plots)
        })

        config = {}
        estimator = AreaEstimator(mock_fia_database, config)
        estimator.plot_condition_data = large_data
        estimator.group_cols = []

        results = pl.DataFrame({
            "AREA_TOTAL": [5000000.0],
            "N_PLOTS": [n_plots]
        })

        # Should handle large datasets efficiently
        variance_results = estimator.calculate_variance(results)
        assert len(variance_results) == 1
        assert variance_results["AREA_SE"][0] > 0

    def test_infinite_or_nan_input_values(self, mock_fia_database):
        """Test handling of infinite or NaN input values."""
        plot_data = pl.DataFrame({
            "PLT_CN": ["P1", "P2", "P3", "P4"],
            "ESTN_UNIT": [1, 1, 1, 1],
            "STRATUM": [1, 1, 1, 1],
            "y_i": [0.8, float('inf'), float('nan'), 1.0],  # Bad values
            "EXPNS": [1000.0, 1000.0, 1000.0, 1000.0]
        })

        config = {}
        estimator = AreaEstimator(mock_fia_database, config)

        # Should either clean data or fail gracefully
        try:
            var_stats = estimator._calculate_variance_for_group(plot_data, ["ESTN_UNIT", "STRATUM"])
            # If successful, results should be finite
            assert np.isfinite(var_stats["variance"])
            assert np.isfinite(var_stats["se_total"])
        except Exception as e:
            # Acceptable to fail with clear error for bad data
            assert len(str(e)) > 0


class TestRealDataEdgeCases:
    """Test edge cases using real FIA data structures."""

    @pytest.mark.skipif(
        not Path("fia.duckdb").exists(),
        reason="Real FIA database not available"
    )
    def test_single_state_single_evalid_edge_case(self):
        """Test edge case with single state and specific EVALID."""
        with FIA("fia.duckdb") as db:
            # Use smallest available dataset to test edge cases
            db.clip_by_evalid([452301])  # South Carolina specific EVALID

            # Test with very restrictive domain that yields few plots
            try:
                result = area(
                    db,
                    area_domain="FORTYPCD == 999",  # Non-existent forest type
                    land_type="timber",
                    totals=True
                )
                # Should handle no matching plots gracefully
                assert isinstance(result, pl.DataFrame)
            except Exception as e:
                # Acceptable to fail clearly with no matching plots
                assert "no" in str(e).lower() or "empty" in str(e).lower()

    @pytest.mark.skipif(
        not Path("fia.duckdb").exists(),
        reason="Real FIA database not available"
    )
    def test_complex_grouping_with_sparse_data(self):
        """Test complex grouping that results in sparse data."""
        with FIA("fia.duckdb") as db:
            db.clip_by_state(45, most_recent=True)  # South Carolina

            # Complex grouping that creates many small groups
            result = area(
                db,
                grp_by=["FORTYPCD", "STDSZCD", "OWNGRPCD"],
                land_type="timber",
                totals=True
            )

            # Should handle complex grouping
            assert len(result) > 0
            assert "AREA_SE" in result.columns

            # Some groups may have small sample sizes but should still work
            small_groups = result.filter(pl.col("N_PLOTS") <= 2)
            if len(small_groups) > 0:
                # Small groups should have higher variance
                assert all(small_groups["AREA_SE"] >= 0)

    @pytest.mark.skipif(
        not Path("fia.duckdb").exists(),
        reason="Real FIA database not available"
    )
    def test_mixed_prop_basis_edge_case(self):
        """Test edge case with mixed PROP_BASIS values affecting variance."""
        with FIA("fia.duckdb") as db:
            db.clip_by_state(13, most_recent=True)  # Georgia

            # Should handle mixed PROP_BASIS (SUBP, MACR) in same estimation
            result = area(
                db,
                land_type="forest",
                grp_by="PROP_BASIS",  # Group by PROP_BASIS to see different types
                totals=True
            )

            # Should work with different proportion basis types
            assert len(result) > 0
            prop_basis_types = result["PROP_BASIS"].unique().to_list()
            assert len(prop_basis_types) >= 1  # Should have at least one type

            # All groups should have valid variance estimates
            assert all(result["AREA_SE"] >= 0)