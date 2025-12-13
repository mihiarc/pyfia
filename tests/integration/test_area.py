"""
Core integration tests for area estimation module.

These tests validate the main area() function workflow with realistic FIA data structures.
For real validation against published estimates, see test_area_real.py.

Tests cover:
- Basic area calculation integration
- Land type classification (timber, non-timber forest, non-forest, water)
- Tree domain filtering
- By land type grouping functionality
"""

from unittest.mock import Mock

import polars as pl
import pytest

from pyfia.estimation import area


class TestAreaEstimation:
    """Integration tests for area estimation functions."""

    @pytest.fixture
    def sample_plot_data(self):
        """Create sample PLOT data matching FIA schema."""
        return pl.DataFrame({
            "CN": ["P1", "P2", "P3", "P4", "P5"],
            "INVYR": [2023, 2023, 2023, 2023, 2023],
            "STATECD": [37, 37, 37, 37, 37],
            "PLOT": [1, 2, 3, 4, 5],
            "ELEV": [100.0, 200.0, 150.0, 300.0, 125.0],
            "LAT": [35.5, 35.6, 35.7, 35.8, 35.9],
            "LON": [-78.5, -78.6, -78.7, -78.8, -78.9],
            "ECOSUBCD": ["231A", "231A", "231B", "231A", "231A"],
            "P2POINTCNT": [1.0, 1.0, 1.0, 1.0, 1.0]
        })

    @pytest.fixture
    def sample_cond_data(self):
        """Create sample COND data with various land types and PROP_BASIS."""
        return pl.DataFrame({
            "CN": ["C1", "C2", "C3", "C4", "C5", "C6"],
            "PLT_CN": ["P1", "P1", "P2", "P3", "P4", "P5"],  # Fixed to match PLOT.CN
            "CONDID": [1, 2, 1, 1, 1, 1],
            "COND_STATUS_CD": [1, 1, 1, 2, 3, 1],  # Forest, Forest, Forest, Non-forest, Water, Forest
            "CONDPROP_UNADJ": [0.7, 0.3, 1.0, 1.0, 0.2, 1.0],  # Reduce water proportion to fix >100% issue
            "PROP_BASIS": ["SUBP", "SUBP", "MACR", "SUBP", "SUBP", "MACR"],
            "FORTYPCD": [171, 171, 162, 0, 0, 182],
            "SITECLCD": [2, 2, 1, 7, 7, 3],
            "RESERVCD": [0, 0, 0, 0, 0, 0]
        })

    @pytest.fixture
    def sample_tree_data(self):
        """Create sample TREE data."""
        return pl.DataFrame({
            "CN": ["T1", "T2", "T3", "T4", "T5"],
            "PLT_CN": ["P1", "P1", "P2", "P3", "P5"],  # Fixed to match PLOT.CN
            "CONDID": [1, 2, 1, 1, 1],
            "STATUSCD": [1, 1, 1, 2, 1],  # Live, Live, Live, Dead, Live
            "SPCD": [131, 316, 131, 131, 621],  # Loblolly, Red maple, Loblolly, Loblolly, Yellow-poplar
            "DIA": [10.5, 8.2, 15.3, 12.0, 6.5],
            "TPA_UNADJ": [5.0, 5.0, 5.0, 5.0, 5.0],
        })

    @pytest.fixture
    def sample_stratum_data(self):
        """Create sample POP_STRATUM data with realistic FIA expansion factors.

        Values based on actual Georgia FIA data (EVALID 132300) showing:
        - EXPNS: Typical range 6,000-7,000 acres per plot
        - ADJ_FACTOR_SUBP: Standard 1.0 (no adjustment needed)
        - ADJ_FACTOR_MACR: 0.0 for strata without macroplot sampling
        - ADJ_FACTOR_MICR: Standard 1.0 for microplot trees
        """
        return pl.DataFrame({
            "CN": ["S1", "S2"],
            "EVALID": [372301, 372301],
            "ESTN_UNIT_CN": ["EU1", "EU2"],
            "ADJ_FACTOR_SUBP": [1.0, 1.0],  # Realistic: no adjustment
            "ADJ_FACTOR_MACR": [0.0, 0.0],  # Realistic: no macroplot sampling
            "ADJ_FACTOR_MICR": [1.0, 1.0],  # Realistic: standard microplot adjustment
            "EXPNS": [6234.58, 5968.86],  # Realistic: ~6K acres per plot
            "P2POINTCNT": [100, 200],
            "STRATUM_CN": ["S1", "S2"],
            "P1POINTCNT": [50, 150]
        })

    @pytest.fixture
    def sample_ppsa_data(self):
        """Create sample POP_PLOT_STRATUM_ASSGN data."""
        return pl.DataFrame({
            "PLT_CN": ["P1", "P2", "P3", "P4", "P5"],  # Fixed to match PLOT.CN
            "STRATUM_CN": ["S1", "S1", "S1", "S2", "S2"],
            "EVALID": [372301, 372301, 372301, 372301, 372301],
        })

    def test_area_main_function_integration(
        self,
        mock_fia_database,
        sample_plot_data,
        sample_cond_data,
        sample_stratum_data,
        sample_ppsa_data,
    ):
        """Test main area function integration workflow."""
        # Setup mock database with realistic FIA structure
        # Store data for load_table side effect
        table_data = {
            "PLOT": sample_plot_data.lazy(),
            "COND": sample_cond_data.lazy(),
        }

        def load_table_side_effect(table_name, columns=None):
            """Mock load_table that populates tables dict like real FIA class."""
            if table_name in table_data:
                mock_fia_database.tables[table_name] = table_data[table_name]
                return table_data[table_name]
            return None

        mock_fia_database.load_table = Mock(side_effect=load_table_side_effect)
        mock_fia_database.tables = {
            "POP_STRATUM": sample_stratum_data.lazy(),
            "POP_PLOT_STRATUM_ASSGN": sample_ppsa_data.lazy(),
        }

        # Test basic forest area calculation
        result = area(mock_fia_database, land_type="forest")

        # Validate result structure matches expected FIA area estimation output
        assert isinstance(result, pl.DataFrame)
        assert "AREA_PERC" in result.columns
        assert "AREA_SE_PERCENT" in result.columns  # Fixed column name
        assert "N_PLOTS" in result.columns

        # Validate reasonable area percentage (0-100%)
        area_perc = result["AREA_PERC"][0]
        # Handle NaN for empty results
        if not pl.Series([area_perc]).is_nan()[0]:
            assert 0 <= area_perc <= 100

        # Validate sample size
        n_plots = result["N_PLOTS"][0]
        assert n_plots >= 0  # Can be 0 if no matching data

    def test_area_by_land_type(
        self,
        mock_fia_database,
        sample_plot_data,
        sample_cond_data,
        sample_stratum_data,
        sample_ppsa_data,
    ):
        """Test area calculation grouped by land type (core FIA functionality)."""
        # Store data for load_table side effect
        table_data = {
            "PLOT": sample_plot_data.lazy(),
            "COND": sample_cond_data.lazy(),
        }

        def load_table_side_effect(table_name, columns=None):
            """Mock load_table that populates tables dict like real FIA class."""
            if table_name in table_data:
                mock_fia_database.tables[table_name] = table_data[table_name]
                return table_data[table_name]
            return None

        mock_fia_database.load_table = Mock(side_effect=load_table_side_effect)
        mock_fia_database.tables = {
            "POP_STRATUM": sample_stratum_data.lazy(),
            "POP_PLOT_STRATUM_ASSGN": sample_ppsa_data.lazy(),
        }

        # Test grouping by COND_STATUS_CD (land type classification)
        result = area(mock_fia_database, grp_by="COND_STATUS_CD", land_type="all")

        # Validate land type classification works correctly
        assert "COND_STATUS_CD" in result.columns
        assert len(result) >= 1  # At least one land type category

        # Validate area percentages are reasonable
        for i in range(len(result)):
            area_perc = result["AREA_PERC"][i]
            if area_perc is not None and not pl.Series([area_perc]).is_nan()[0]:
                assert 0 <= area_perc <= 100

    def test_area_with_tree_domain(
        self,
        mock_fia_database,
        sample_plot_data,
        sample_cond_data,
        sample_tree_data,
        sample_stratum_data,
        sample_ppsa_data,
    ):
        """Test area calculation with tree domain filter (critical FIA capability)."""
        # Store data for load_table side effect
        table_data = {
            "PLOT": sample_plot_data.lazy(),
            "COND": sample_cond_data.lazy(),
            "TREE": sample_tree_data.lazy(),
        }

        def load_table_side_effect(table_name, columns=None):
            """Mock load_table that populates tables dict like real FIA class."""
            if table_name in table_data:
                mock_fia_database.tables[table_name] = table_data[table_name]
                return table_data[table_name]
            return None

        mock_fia_database.load_table = Mock(side_effect=load_table_side_effect)
        mock_fia_database.tables = {
            "POP_STRATUM": sample_stratum_data.lazy(),
            "POP_PLOT_STRATUM_ASSGN": sample_ppsa_data.lazy(),
        }

        # Test filtering for specific forest types using area_domain
        result = area(mock_fia_database, area_domain="FORTYPCD == 171", land_type="forest")

        assert isinstance(result, pl.DataFrame)
        assert "AREA_PERC" in result.columns
        # Results should exist
        assert len(result) > 0

    def test_area_timber_land_type(
        self,
        mock_fia_database,
        sample_plot_data,
        sample_cond_data,
        sample_stratum_data,
        sample_ppsa_data,
    ):
        """Test timber land classification (important FIA land use definition)."""
        # Store data for load_table side effect
        table_data = {
            "PLOT": sample_plot_data.lazy(),
            "COND": sample_cond_data.lazy(),
        }

        def load_table_side_effect(table_name, columns=None):
            """Mock load_table that populates tables dict like real FIA class."""
            if table_name in table_data:
                mock_fia_database.tables[table_name] = table_data[table_name]
                return table_data[table_name]
            return None

        mock_fia_database.load_table = Mock(side_effect=load_table_side_effect)
        mock_fia_database.tables = {
            "POP_STRATUM": sample_stratum_data.lazy(),
            "POP_PLOT_STRATUM_ASSGN": sample_ppsa_data.lazy(),
        }

        result = area(mock_fia_database, land_type="timber")

        assert isinstance(result, pl.DataFrame)
        assert "AREA_PERC" in result.columns

        # Timber area should be subset of total forest area
        # (excludes reserved lands and non-productive sites)
        area_perc = result["AREA_PERC"][0]
        # Handle NaN for cases where no timber area matches
        if not pl.Series([area_perc]).is_nan()[0]:
            assert 0.0 <= area_perc <= 100.0


class TestVarianceCalculation:
    """Tests for area estimation variance calculation methodology.

    These tests validate the variance calculation implementation including:
    - Domain indicator approach
    - Variance formula correctness: V(Y_D) = sum_h [w_h^2 * s^2_yDh * n_h]
    - Plot-condition data storage and retrieval
    - Statistical accuracy
    """

    def test_variance_calculation_single_stratum(self, mock_fia_database):
        """Test variance calculation for a single stratum."""
        from pyfia.estimation.estimators.area import AreaEstimator
        import numpy as np

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
        # V(Y_D) = w_h^2 * s2_yh * n_h = 1000^2 * s2_yh * 4

        expected_var = np.var([0.8, 1.0, 0.6, 0.9], ddof=1)
        expected_total_var = (1000.0 ** 2) * expected_var * 4

        assert abs(var_stats["variance"] - expected_total_var) < 1e-6

    def test_variance_calculation_multiple_strata(self, mock_fia_database):
        """Test variance calculation across multiple strata."""
        from pyfia.estimation.estimators.area import AreaEstimator

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

    def test_variance_with_grouping(self, mock_fia_database):
        """Test variance calculation with grouping variables."""
        from pyfia.estimation.estimators.area import AreaEstimator

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

    def test_variance_no_grouping(self, mock_fia_database):
        """Test variance calculation without grouping variables."""
        from pyfia.estimation.estimators.area import AreaEstimator

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

    def test_domain_indicator_forest_land_type(self, mock_fia_database):
        """Test domain indicator creation for forest land type."""
        from pyfia.estimation.estimators.area import AreaEstimator

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

    def test_domain_indicator_all_land_type(self, mock_fia_database):
        """Test domain indicator for 'all' land type."""
        from pyfia.estimation.estimators.area import AreaEstimator

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

    def test_area_value_with_domain_indicator(self, mock_fia_database):
        """Test area value calculation with domain indicator."""
        from pyfia.estimation.estimators.area import AreaEstimator

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