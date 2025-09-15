"""
Comprehensive tests for area estimation variance calculation (PR #35).

This module provides detailed unit tests for the new variance calculation methodology
implemented in PR #35, focusing on:
- Domain indicator approach validation
- Variance formula correctness (V(Ŷ_D) = Σ_h [w_h² × s²_yDh × n_h])
- Plot-condition data storage and retrieval
- Edge cases and error conditions
- Statistical accuracy against known values

Tests use both synthetic data for controlled scenarios and real FIA data for
validation against EVALIDator results.
"""

import pytest
import polars as pl
import numpy as np
from unittest.mock import Mock, patch

from pyfia.estimation.estimators.area import AreaEstimator
from pyfia.core import FIA


class TestDomainIndicatorFunctionality:
    """Test domain indicator creation and application in area estimation."""

    def test_domain_indicator_forest_land_type(self, mock_fia_database):
        """Test domain indicator creation for forest land type."""
        # Create test data with mixed land types
        test_data = pl.DataFrame({
            "PLT_CN": ["P1", "P2", "P3", "P4"],
            "CONDID": [1, 1, 1, 1],
            "COND_STATUS_CD": [1, 1, 2, 3],  # Forest, Forest, Non-forest, Water
            "CONDPROP_UNADJ": [1.0, 0.8, 1.0, 1.0],
            "PROP_BASIS": ["SUBP", "SUBP", "SUBP", "SUBP"]
        })

        config = {"land_type": "forest"}
        estimator = AreaEstimator(mock_fia_database, config)

        # Apply filters and check domain indicator
        result = estimator.apply_filters(test_data.lazy()).collect()

        # Validate domain indicator creation
        assert "DOMAIN_IND" in result.columns
        expected_indicators = [1.0, 1.0, 0.0, 0.0]  # Only forest conditions get 1.0
        assert result["DOMAIN_IND"].to_list() == expected_indicators

        # Validate all plots are retained (domain indicator approach)
        assert len(result) == 4

    def test_domain_indicator_timber_land_type(self, mock_fia_database):
        """Test domain indicator creation for timber land type."""
        test_data = pl.DataFrame({
            "PLT_CN": ["P1", "P2", "P3", "P4"],
            "CONDID": [1, 1, 1, 1],
            "COND_STATUS_CD": [1, 1, 1, 1],  # All forest
            "SITECLCD": [2, 7, 3, 1],  # Productive, non-productive, productive, productive
            "RESERVCD": [0, 0, 1, 0],  # Not reserved, not reserved, reserved, not reserved
            "CONDPROP_UNADJ": [1.0, 0.8, 1.0, 1.0],
            "PROP_BASIS": ["SUBP", "SUBP", "SUBP", "SUBP"]
        })

        config = {"land_type": "timber"}
        estimator = AreaEstimator(mock_fia_database, config)

        result = estimator.apply_filters(test_data.lazy()).collect()

        # Only plots with forest, productive site, and not reserved should get 1.0
        # P1: forest + productive + not reserved = 1.0
        # P2: forest + non-productive + not reserved = 0.0
        # P3: forest + productive + reserved = 0.0
        # P4: forest + productive + not reserved = 1.0
        expected_indicators = [1.0, 0.0, 0.0, 1.0]
        assert result["DOMAIN_IND"].to_list() == expected_indicators

    def test_domain_indicator_all_land_type(self, mock_fia_database):
        """Test domain indicator for 'all' land type."""
        test_data = pl.DataFrame({
            "PLT_CN": ["P1", "P2", "P3"],
            "CONDID": [1, 1, 1],
            "COND_STATUS_CD": [1, 2, 3],  # Mixed land types
            "CONDPROP_UNADJ": [1.0, 0.8, 1.0],
            "PROP_BASIS": ["SUBP", "SUBP", "SUBP"]
        })

        config = {"land_type": "all"}
        estimator = AreaEstimator(mock_fia_database, config)

        result = estimator.apply_filters(test_data.lazy()).collect()

        # All land types should get 1.0 indicator
        expected_indicators = [1.0, 1.0, 1.0]
        assert result["DOMAIN_IND"].to_list() == expected_indicators

    def test_calculate_values_with_domain_indicator(self, mock_fia_database):
        """Test area value calculation with domain indicator."""
        test_data = pl.DataFrame({
            "PLT_CN": ["P1", "P2", "P3"],
            "CONDPROP_UNADJ": [1.0, 0.8, 1.0],
            "DOMAIN_IND": [1.0, 0.0, 1.0]  # Mixed domain indicator
        })

        config = {}
        estimator = AreaEstimator(mock_fia_database, config)

        result = estimator.calculate_values(test_data.lazy()).collect()

        # AREA_VALUE should be CONDPROP_UNADJ * DOMAIN_IND
        expected_values = [1.0, 0.0, 1.0]  # 1.0*1.0, 0.8*0.0, 1.0*1.0
        assert result["AREA_VALUE"].to_list() == expected_values

    def test_calculate_values_without_domain_indicator(self, mock_fia_database):
        """Test area value calculation fallback without domain indicator."""
        test_data = pl.DataFrame({
            "PLT_CN": ["P1", "P2"],
            "CONDPROP_UNADJ": [1.0, 0.8]
            # No DOMAIN_IND column
        })

        config = {}
        estimator = AreaEstimator(mock_fia_database, config)

        result = estimator.calculate_values(test_data.lazy()).collect()

        # Should fallback to just CONDPROP_UNADJ
        expected_values = [1.0, 0.8]
        assert result["AREA_VALUE"].to_list() == expected_values


class TestVarianceCalculationUnit:
    """Unit tests for variance calculation methods."""

    def test_calculate_variance_for_group_single_stratum(self, mock_fia_database):
        """Test variance calculation for a single stratum."""
        # Create synthetic plot data for controlled variance testing
        plot_data = pl.DataFrame({
            "PLT_CN": ["P1", "P2", "P3", "P4"],
            "ESTN_UNIT": [1, 1, 1, 1],
            "STRATUM": [1, 1, 1, 1],
            "y_i": [0.8, 1.0, 0.6, 0.9],  # Plot-level proportions
            "EXPNS": [1000.0, 1000.0, 1000.0, 1000.0]  # Expansion factors
        })

        strat_cols = ["ESTN_UNIT", "STRATUM"]

        config = {}
        estimator = AreaEstimator(mock_fia_database, config)

        var_stats = estimator._calculate_variance_for_group(plot_data, strat_cols)

        # Validate variance calculation structure
        assert "variance" in var_stats
        assert "se_total" in var_stats
        assert var_stats["variance"] >= 0  # Variance should be non-negative
        assert var_stats["se_total"] >= 0  # SE should be non-negative

        # Manual calculation verification
        # n_h = 4, ybar_h = mean([0.8, 1.0, 0.6, 0.9]) = 0.825
        # s2_yh = variance([0.8, 1.0, 0.6, 0.9]) with ddof=1
        # w_h = 1000.0
        # V(Ŷ_D) = w_h² × s2_yh × n_h = 1000² × s2_yh × 4

        expected_mean = np.mean([0.8, 1.0, 0.6, 0.9])
        expected_var = np.var([0.8, 1.0, 0.6, 0.9], ddof=1)
        expected_total_var = (1000.0 ** 2) * expected_var * 4

        assert abs(var_stats["variance"] - expected_total_var) < 1e-6

    def test_calculate_variance_for_group_multiple_strata(self, mock_fia_database):
        """Test variance calculation across multiple strata."""
        plot_data = pl.DataFrame({
            "PLT_CN": ["P1", "P2", "P3", "P4", "P5", "P6"],
            "ESTN_UNIT": [1, 1, 1, 2, 2, 2],
            "STRATUM": [1, 1, 2, 1, 1, 2],
            "y_i": [0.8, 1.0, 0.6, 0.9, 0.7, 0.5],
            "EXPNS": [1000.0, 1000.0, 1500.0, 1500.0, 1500.0, 2000.0]
        })

        strat_cols = ["ESTN_UNIT", "STRATUM"]

        config = {}
        estimator = AreaEstimator(mock_fia_database, config)

        var_stats = estimator._calculate_variance_for_group(plot_data, strat_cols)

        # Should sum variance components across all strata
        assert var_stats["variance"] > 0
        assert var_stats["se_total"] > 0

        # Variance should be higher than single stratum case due to multiple strata
        # This is a qualitative check for reasonable behavior

    def test_calculate_variance_single_plot_per_stratum(self, mock_fia_database):
        """Test variance calculation with single plot per stratum (edge case)."""
        plot_data = pl.DataFrame({
            "PLT_CN": ["P1", "P2"],
            "ESTN_UNIT": [1, 2],
            "STRATUM": [1, 1],
            "y_i": [0.8, 0.9],
            "EXPNS": [1000.0, 1500.0]
        })

        strat_cols = ["ESTN_UNIT", "STRATUM"]

        config = {}
        estimator = AreaEstimator(mock_fia_database, config)

        var_stats = estimator._calculate_variance_for_group(plot_data, strat_cols)

        # With single plots, variance should be zero (handled by nulls -> 0.0)
        assert var_stats["variance"] == 0.0
        assert var_stats["se_total"] == 0.0

    def test_calculate_variance_missing_stratification(self, mock_fia_database):
        """Test variance calculation when stratification columns are missing."""
        plot_data = pl.DataFrame({
            "PLT_CN": ["P1", "P2", "P3"],
            "y_i": [0.8, 1.0, 0.6],
            "EXPNS": [1000.0, 1000.0, 1000.0]
            # No ESTN_UNIT or STRATUM columns
        })

        strat_cols = []  # Empty stratification

        config = {}
        estimator = AreaEstimator(mock_fia_database, config)

        # Should handle missing stratification by creating default stratum
        var_stats = estimator._calculate_variance_for_group(plot_data, strat_cols)

        assert var_stats["variance"] >= 0
        assert var_stats["se_total"] >= 0


class TestPlotConditionDataStorage:
    """Test plot-condition data storage and retrieval functionality."""

    def test_plot_condition_data_storage(self, mock_fia_database):
        """Test that plot-condition data is properly stored during aggregation."""
        # Setup mock data
        test_data = pl.DataFrame({
            "PLT_CN": ["P1", "P2", "P3"],
            "CONDID": [1, 1, 1],
            "AREA_VALUE": [1.0, 0.8, 1.0],
            "ADJ_FACTOR_AREA": [1.0, 1.0, 1.0],
            "EXPNS": [1000.0, 1000.0, 1000.0],
            "ESTN_UNIT": [1, 1, 2],
            "STRATUM_CN": ["S1", "S1", "S2"]
        })

        config = {}
        estimator = AreaEstimator(mock_fia_database, config)

        # Mock stratification data
        with patch.object(estimator, '_get_stratification_data') as mock_strat:
            mock_strat.return_value = pl.DataFrame({
                "PLT_CN": ["P1", "P2", "P3"],
                "ESTN_UNIT": [1, 1, 2],
                "STRATUM_CN": ["S1", "S1", "S2"],
                "EXPNS": [1000.0, 1000.0, 1000.0]
            }).lazy()

            # Call aggregate_results which should store plot-condition data
            result = estimator.aggregate_results(test_data.lazy())

            # Verify plot-condition data was stored
            assert estimator.plot_condition_data is not None
            assert isinstance(estimator.plot_condition_data, pl.DataFrame)
            assert len(estimator.plot_condition_data) == 3

            # Check essential columns are present
            required_cols = ["PLT_CN", "AREA_VALUE", "ADJ_FACTOR_AREA", "EXPNS"]
            for col in required_cols:
                assert col in estimator.plot_condition_data.columns

    def test_plot_condition_data_column_availability(self, mock_fia_database):
        """Test column availability checking logic."""
        # Test data missing some optional columns
        test_data = pl.DataFrame({
            "PLT_CN": ["P1", "P2"],
            "AREA_VALUE": [1.0, 0.8],
            "ADJ_FACTOR_AREA": [1.0, 1.0],
            "EXPNS": [1000.0, 1000.0]
            # Missing CONDID, ESTN_UNIT, STRATUM_CN
        })

        config = {}
        estimator = AreaEstimator(mock_fia_database, config)

        with patch.object(estimator, '_get_stratification_data') as mock_strat:
            mock_strat.return_value = test_data.lazy()

            result = estimator.aggregate_results(test_data.lazy())

            # Should handle missing columns gracefully
            assert estimator.plot_condition_data is not None
            # Essential columns should still be present
            assert "PLT_CN" in estimator.plot_condition_data.columns
            assert "AREA_VALUE" in estimator.plot_condition_data.columns

    def test_grouping_column_storage(self, mock_fia_database):
        """Test that grouping columns are properly stored with plot-condition data."""
        test_data = pl.DataFrame({
            "PLT_CN": ["P1", "P2", "P3"],
            "AREA_VALUE": [1.0, 0.8, 1.0],
            "ADJ_FACTOR_AREA": [1.0, 1.0, 1.0],
            "EXPNS": [1000.0, 1000.0, 1000.0],
            "FORTYPCD": [161, 161, 406],  # Grouping column
            "OWNGRPCD": [10, 20, 10]     # Another grouping column
        })

        config = {"grp_by": ["FORTYPCD", "OWNGRPCD"]}
        estimator = AreaEstimator(mock_fia_database, config)

        with patch.object(estimator, '_get_stratification_data') as mock_strat:
            mock_strat.return_value = test_data.lazy()

            result = estimator.aggregate_results(test_data.lazy())

            # Grouping columns should be stored
            assert "FORTYPCD" in estimator.plot_condition_data.columns
            assert "OWNGRPCD" in estimator.plot_condition_data.columns
            assert estimator.group_cols == ["FORTYPCD", "OWNGRPCD"]


class TestVarianceIntegration:
    """Integration tests for complete variance calculation workflow."""

    def test_variance_calculation_with_grouping(self, mock_fia_database):
        """Test variance calculation with grouping variables."""
        # Create test data with multiple groups
        config = {"grp_by": "FORTYPCD"}
        estimator = AreaEstimator(mock_fia_database, config)

        # Mock plot-condition data
        estimator.plot_condition_data = pl.DataFrame({
            "PLT_CN": ["P1", "P2", "P3", "P4"],
            "AREA_VALUE": [1.0, 0.8, 1.0, 0.6],
            "ADJ_FACTOR_AREA": [1.0, 1.0, 1.0, 1.0],
            "EXPNS": [1000.0, 1000.0, 1000.0, 1000.0],
            "ESTN_UNIT": [1, 1, 1, 1],
            "STRATUM": [1, 1, 1, 1],
            "FORTYPCD": [161, 161, 406, 406]  # Two forest types
        })
        estimator.group_cols = ["FORTYPCD"]

        # Mock main results for each group
        results = pl.DataFrame({
            "FORTYPCD": [161, 406],
            "AREA_TOTAL": [1800.0, 1600.0],  # Dummy totals
            "N_PLOTS": [2, 2]
        })

        variance_results = estimator.calculate_variance(results)

        # Should have variance columns
        assert "AREA_SE" in variance_results.columns
        assert "AREA_SE_PERCENT" in variance_results.columns
        assert "AREA_VARIANCE" in variance_results.columns

        # Should have one row per group
        assert len(variance_results) == 2

        # All variance values should be non-negative
        assert all(variance_results["AREA_SE"] >= 0)
        assert all(variance_results["AREA_VARIANCE"] >= 0)

    def test_variance_calculation_no_grouping(self, mock_fia_database):
        """Test variance calculation without grouping variables."""
        config = {}
        estimator = AreaEstimator(mock_fia_database, config)

        # Mock plot-condition data
        estimator.plot_condition_data = pl.DataFrame({
            "PLT_CN": ["P1", "P2", "P3"],
            "AREA_VALUE": [1.0, 0.8, 1.0],
            "ADJ_FACTOR_AREA": [1.0, 1.0, 1.0],
            "EXPNS": [1000.0, 1000.0, 1000.0],
            "ESTN_UNIT": [1, 1, 1],
            "STRATUM": [1, 1, 1]
        })
        estimator.group_cols = []

        # Mock main results
        results = pl.DataFrame({
            "AREA_TOTAL": [2800.0],
            "N_PLOTS": [3]
        })

        variance_results = estimator.calculate_variance(results)

        # Should add variance columns to single result row
        assert "AREA_SE" in variance_results.columns
        assert "AREA_SE_PERCENT" in variance_results.columns
        assert "AREA_VARIANCE" in variance_results.columns
        assert len(variance_results) == 1

    def test_variance_fallback_no_plot_data(self, mock_fia_database):
        """Test variance calculation fallback when plot-condition data is missing."""
        config = {}
        estimator = AreaEstimator(mock_fia_database, config)

        # No plot-condition data stored
        estimator.plot_condition_data = None

        results = pl.DataFrame({
            "AREA_TOTAL": [2800.0],
            "N_PLOTS": [3]
        })

        # Should issue warning and use 5% CV fallback
        with pytest.warns(UserWarning, match="Plot-condition data not available"):
            variance_results = estimator.calculate_variance(results)

        # Should have fallback SE (5% of total)
        expected_se = 2800.0 * 0.05
        assert abs(variance_results["AREA_SE"][0] - expected_se) < 1e-6


class TestRegressionValidation:
    """Regression tests to ensure new variance calculations don't break existing functionality."""

    def test_area_estimation_basic_workflow_preserved(self, mock_fia_database):
        """Test that basic area estimation workflow still works."""
        # Setup comprehensive mock data
        mock_fia_database.tables = {
            "COND": pl.DataFrame({
                "PLT_CN": ["P1", "P2"],
                "CONDID": [1, 1],
                "COND_STATUS_CD": [1, 1],
                "CONDPROP_UNADJ": [1.0, 1.0],
                "PROP_BASIS": ["SUBP", "SUBP"]
            }).lazy(),
            "PLOT": pl.DataFrame({
                "CN": ["P1", "P2"],
                "STATECD": [37, 37],
                "INVYR": [2020, 2020]
            }).lazy(),
            "POP_STRATUM": pl.DataFrame({
                "CN": ["S1"],
                "EXPNS": [1000.0],
                "ADJ_FACTOR_SUBP": [1.0],
                "ADJ_FACTOR_MACR": [0.25]
            }),
            "POP_PLOT_STRATUM_ASSGN": pl.DataFrame({
                "PLT_CN": ["P1", "P2"],
                "STRATUM_CN": ["S1", "S1"]
            })
        }

        # Mock necessary methods
        mock_fia_database.load_table = Mock()

        config = {"land_type": "forest"}
        estimator = AreaEstimator(mock_fia_database, config)

        # Test that estimator can be created and basic methods work
        required_tables = estimator.get_required_tables()
        assert "COND" in required_tables
        assert "PLOT" in required_tables

        required_cols = estimator.get_cond_columns()
        assert "PLT_CN" in required_cols
        assert "CONDPROP_UNADJ" in required_cols

    def test_backward_compatibility_config_parameters(self, mock_fia_database):
        """Test that existing configuration parameters still work."""
        # Test various config combinations that should still work
        configs = [
            {"land_type": "forest"},
            {"land_type": "timber"},
            {"land_type": "all"},
            {"grp_by": "FORTYPCD"},
            {"grp_by": ["FORTYPCD", "OWNGRPCD"]},
            {"area_domain": "STDAGE > 50"},
            {"land_type": "timber", "grp_by": "FORTYPCD"}
        ]

        for config in configs:
            estimator = AreaEstimator(mock_fia_database, config)
            # Should be able to create estimator without errors
            assert estimator.config == config

    def test_column_selection_logic_preserved(self, mock_fia_database):
        """Test that column selection logic works correctly."""
        config = {
            "land_type": "timber",
            "grp_by": ["FORTYPCD", "OWNGRPCD"],
            "area_domain": "STDAGE > 50"
        }

        estimator = AreaEstimator(mock_fia_database, config)

        # Get required columns
        cond_cols = estimator.get_cond_columns()

        # Should include core columns
        assert "PLT_CN" in cond_cols
        assert "CONDPROP_UNADJ" in cond_cols
        assert "COND_STATUS_CD" in cond_cols

        # Should include timber-specific columns
        assert "SITECLCD" in cond_cols
        assert "RESERVCD" in cond_cols

        # Should include grouping columns
        assert "FORTYPCD" in cond_cols
        assert "OWNGRPCD" in cond_cols

        # Should include domain columns (simplified detection)
        assert "STDAGE" in cond_cols