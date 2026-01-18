"""Unit tests for SiteIndexEstimator class.

Tests the SiteIndexEstimator methods in isolation using mock data.
No database connection required.
"""

import polars as pl
import pytest

from pyfia.estimation.estimators.site_index import SiteIndexEstimator


class MockDB:
    """Mock database for testing estimator methods in isolation."""

    def __init__(self):
        self.db_path = "/fake/path"
        self.tables = {}
        self.evalid = None
        self.evalids = None
        self._state_filter = None


class TestGetRequiredTables:
    """Tests for get_required_tables method."""

    def test_returns_required_tables(self):
        """Test that required tables for site index estimation are returned."""
        config = {"land_type": "forest"}
        estimator = SiteIndexEstimator(MockDB(), config)
        tables = estimator.get_required_tables()

        assert "COND" in tables
        assert "PLOT" in tables
        assert "POP_PLOT_STRATUM_ASSGN" in tables
        assert "POP_STRATUM" in tables
        assert len(tables) == 4

    def test_does_not_require_tree_table(self):
        """Test that TREE table is not required (site index is condition-level)."""
        config = {"land_type": "forest"}
        estimator = SiteIndexEstimator(MockDB(), config)
        tables = estimator.get_required_tables()

        assert "TREE" not in tables

    def test_tables_are_consistent_across_configs(self):
        """Test that table requirements do not change with different configs."""
        configs = [
            {"land_type": "forest"},
            {"land_type": "timber"},
            {"grp_by": "OWNGRPCD"},
            {"grp_by": ["COUNTYCD", "FORTYPCD"]},
        ]

        for config in configs:
            estimator = SiteIndexEstimator(MockDB(), config)
            tables = estimator.get_required_tables()
            assert "COND" in tables
            assert "PLOT" in tables
            assert len(tables) == 4


class TestGetCondColumns:
    """Tests for get_cond_columns method."""

    def test_includes_site_index_columns(self):
        """Test that site index columns are included."""
        config = {"land_type": "forest"}
        estimator = SiteIndexEstimator(MockDB(), config)
        cols = estimator.get_cond_columns()

        assert "SICOND" in cols
        assert "SIBASE" in cols
        assert "SISP" in cols

    def test_includes_core_columns(self):
        """Test that core condition columns are included."""
        config = {"land_type": "forest"}
        estimator = SiteIndexEstimator(MockDB(), config)
        cols = estimator.get_cond_columns()

        assert "PLT_CN" in cols
        assert "CONDID" in cols
        assert "COND_STATUS_CD" in cols
        assert "CONDPROP_UNADJ" in cols
        assert "PROP_BASIS" in cols

    def test_timber_land_type_adds_columns(self):
        """Test timber land type adds required filtering columns."""
        config = {"land_type": "timber"}
        estimator = SiteIndexEstimator(MockDB(), config)
        cols = estimator.get_cond_columns()

        assert "SITECLCD" in cols
        assert "RESERVCD" in cols

    def test_forest_land_type_no_extra_columns(self):
        """Test forest land type does not add timber-specific columns."""
        config = {"land_type": "forest"}
        estimator = SiteIndexEstimator(MockDB(), config)
        cols = estimator.get_cond_columns()

        assert "SITECLCD" not in cols
        assert "RESERVCD" not in cols

    def test_grp_by_adds_columns(self):
        """Test grouping columns are added."""
        config = {"land_type": "forest", "grp_by": "OWNGRPCD"}
        estimator = SiteIndexEstimator(MockDB(), config)
        cols = estimator.get_cond_columns()

        assert "OWNGRPCD" in cols

    def test_multiple_grp_by_columns(self):
        """Test multiple grouping columns are added."""
        config = {"land_type": "forest", "grp_by": ["COUNTYCD", "FORTYPCD"]}
        estimator = SiteIndexEstimator(MockDB(), config)
        cols = estimator.get_cond_columns()

        assert "COUNTYCD" in cols
        assert "FORTYPCD" in cols

    def test_no_duplicate_columns(self):
        """Test that columns are not duplicated."""
        config = {"land_type": "forest", "grp_by": "SICOND"}
        estimator = SiteIndexEstimator(MockDB(), config)
        cols = estimator.get_cond_columns()

        # SICOND should appear only once even if in grp_by
        assert cols.count("SICOND") == 1


class TestCalculateValues:
    """Tests for calculate_values method."""

    @pytest.fixture
    def mock_db(self):
        return MockDB()

    def test_calculates_weighted_values(self, mock_db):
        """Test weighted site index calculation."""
        config = {"land_type": "forest"}
        estimator = SiteIndexEstimator(mock_db, config)

        data = pl.DataFrame(
            {
                "SICOND": [70.0, 80.0, 65.0],
                "CONDPROP_UNADJ": [1.0, 0.5, 0.75],
            }
        ).lazy()

        result = estimator.calculate_values(data).collect()

        # SI_WEIGHTED = SICOND * CONDPROP_UNADJ
        assert result["SI_WEIGHTED"][0] == 70.0
        assert result["SI_WEIGHTED"][1] == 40.0  # 80 * 0.5
        assert result["SI_WEIGHTED"][2] == 48.75  # 65 * 0.75

        # AREA_WEIGHTED = CONDPROP_UNADJ
        assert result["AREA_WEIGHTED"][0] == 1.0
        assert result["AREA_WEIGHTED"][1] == 0.5
        assert result["AREA_WEIGHTED"][2] == 0.75

    def test_handles_domain_indicator(self, mock_db):
        """Test calculation with domain indicator present."""
        config = {"land_type": "forest"}
        estimator = SiteIndexEstimator(mock_db, config)

        data = pl.DataFrame(
            {
                "SICOND": [70.0, 80.0, 65.0],
                "CONDPROP_UNADJ": [1.0, 1.0, 1.0],
                "DOMAIN_IND": [1.0, 0.0, 1.0],  # Second row excluded
            }
        ).lazy()

        result = estimator.calculate_values(data).collect()

        # SI_WEIGHTED includes domain indicator
        assert result["SI_WEIGHTED"][0] == 70.0  # 70 * 1.0 * 1.0
        assert result["SI_WEIGHTED"][1] == 0.0  # 80 * 1.0 * 0.0
        assert result["SI_WEIGHTED"][2] == 65.0  # 65 * 1.0 * 1.0

    def test_handles_null_sicond(self, mock_db):
        """Test that null SICOND produces null weighted values."""
        config = {"land_type": "forest"}
        estimator = SiteIndexEstimator(mock_db, config)

        data = pl.DataFrame(
            {
                "SICOND": [70.0, None, 65.0],
                "CONDPROP_UNADJ": [1.0, 1.0, 1.0],
            }
        ).lazy()

        result = estimator.calculate_values(data).collect()

        assert result["SI_WEIGHTED"][0] == 70.0
        assert result["SI_WEIGHTED"][1] is None
        assert result["SI_WEIGHTED"][2] == 65.0


class TestApplyFilters:
    """Tests for apply_filters method."""

    @pytest.fixture
    def mock_db(self):
        return MockDB()

    def test_creates_domain_indicator(self, mock_db):
        """Test that domain indicator is created for forest land."""
        config = {"land_type": "forest"}
        estimator = SiteIndexEstimator(mock_db, config)

        data = pl.DataFrame(
            {
                "SICOND": [70.0, 80.0],
                "SIBASE": [50, 50],
                "COND_STATUS_CD": [1, 2],  # Forest, Non-forest
                "CONDPROP_UNADJ": [1.0, 1.0],
            }
        ).lazy()

        result = estimator.apply_filters(data).collect()

        assert "DOMAIN_IND" in result.columns
        # Only forest (COND_STATUS_CD=1) should have domain=1
        domain_vals = result["DOMAIN_IND"].to_list()
        assert domain_vals[0] == 1.0  # Forest

    def test_filters_null_sicond(self, mock_db):
        """Test that null SICOND values are filtered out."""
        config = {"land_type": "forest"}
        estimator = SiteIndexEstimator(mock_db, config)

        data = pl.DataFrame(
            {
                "SICOND": [70.0, None, 65.0],
                "SIBASE": [50, 50, 50],
                "COND_STATUS_CD": [1, 1, 1],
                "CONDPROP_UNADJ": [1.0, 1.0, 1.0],
            }
        ).lazy()

        result = estimator.apply_filters(data).collect()

        # Null SICOND should be filtered out
        assert len(result) == 2
        assert result["SICOND"].to_list() == [70.0, 65.0]


class TestSelectVarianceColumns:
    """Tests for _select_variance_columns method."""

    @pytest.fixture
    def mock_db(self):
        return MockDB()

    def test_sibase_always_in_group_cols(self, mock_db):
        """Test that SIBASE is always included in group columns."""
        config = {"land_type": "forest"}
        estimator = SiteIndexEstimator(mock_db, config)

        available_cols = [
            "PLT_CN",
            "CONDID",
            "SIBASE",
            "SICOND",
            "SI_WEIGHTED",
            "AREA_WEIGHTED",
            "ADJ_FACTOR_AREA",
            "EXPNS",
            "DOMAIN_IND",
            "STRATUM_CN",
        ]

        cols_to_select, group_cols = estimator._select_variance_columns(available_cols)

        assert "SIBASE" in group_cols

    def test_user_grp_by_added_to_group_cols(self, mock_db):
        """Test that user grouping columns are added."""
        config = {"land_type": "forest", "grp_by": "COUNTYCD"}
        estimator = SiteIndexEstimator(mock_db, config)

        available_cols = [
            "PLT_CN",
            "CONDID",
            "SIBASE",
            "COUNTYCD",
            "SICOND",
            "SI_WEIGHTED",
            "AREA_WEIGHTED",
            "ADJ_FACTOR_AREA",
            "EXPNS",
            "DOMAIN_IND",
            "STRATUM_CN",
        ]

        cols_to_select, group_cols = estimator._select_variance_columns(available_cols)

        assert "SIBASE" in group_cols
        assert "COUNTYCD" in group_cols

    def test_no_duplicate_sibase_in_group_cols(self, mock_db):
        """Test that SIBASE is not duplicated if user specifies it."""
        config = {"land_type": "forest", "grp_by": "SIBASE"}
        estimator = SiteIndexEstimator(mock_db, config)

        available_cols = [
            "PLT_CN",
            "SIBASE",
            "SICOND",
            "SI_WEIGHTED",
            "AREA_WEIGHTED",
            "ADJ_FACTOR_AREA",
            "EXPNS",
            "DOMAIN_IND",
            "STRATUM_CN",
        ]

        cols_to_select, group_cols = estimator._select_variance_columns(available_cols)

        # SIBASE should appear only once
        assert group_cols.count("SIBASE") == 1


class TestEdgeCases:
    """Test edge cases."""

    @pytest.fixture
    def mock_db(self):
        return MockDB()

    def test_empty_dataframe(self, mock_db):
        """Test handling of empty data."""
        config = {"land_type": "forest"}
        estimator = SiteIndexEstimator(mock_db, config)

        data = pl.DataFrame(
            {
                "SICOND": [],
                "CONDPROP_UNADJ": [],
            }
        ).lazy()

        result = estimator.calculate_values(data).collect()
        assert len(result) == 0

    def test_all_null_sicond_filtered(self, mock_db):
        """Test when all site index values are null - all filtered out."""
        config = {"land_type": "forest"}
        estimator = SiteIndexEstimator(mock_db, config)

        data = pl.DataFrame(
            {
                "SICOND": [None, None],
                "SIBASE": [50, 50],
                "COND_STATUS_CD": [1, 1],
                "CONDPROP_UNADJ": [1.0, 1.0],
            }
        ).lazy()

        # After filtering null SICOND, should be empty
        filtered = estimator.apply_filters(data).collect()
        assert len(filtered) == 0

    def test_all_non_forest_domain_zero(self, mock_db):
        """Test that non-forest conditions get domain indicator of 0."""
        config = {"land_type": "forest"}
        estimator = SiteIndexEstimator(mock_db, config)

        data = pl.DataFrame(
            {
                "SICOND": [70.0, 80.0],
                "SIBASE": [50, 50],
                "COND_STATUS_CD": [2, 3],  # Non-forest, Water
                "CONDPROP_UNADJ": [1.0, 1.0],
            }
        ).lazy()

        result = estimator.apply_filters(data).collect()

        # All should have domain indicator of 0
        domain_vals = result["DOMAIN_IND"].to_list()
        assert all(d == 0.0 for d in domain_vals)


class TestRatioVarianceCalculation:
    """Tests for ratio variance calculation."""

    @pytest.fixture
    def mock_db(self):
        return MockDB()

    def test_variance_formula_components(self, mock_db):
        """Test that variance calculation uses correct components."""
        config = {"land_type": "forest"}
        estimator = SiteIndexEstimator(mock_db, config)

        # Create test plot data with known values
        plot_data = pl.DataFrame(
            {
                "PLT_CN": ["P1", "P2", "P3"],
                "STRATUM_CN": ["S1", "S1", "S1"],
                "EXPNS": [100.0, 100.0, 100.0],
                "y_i": [70.0, 80.0, 75.0],  # SI weighted values
                "x_i": [1.0, 1.0, 1.0],  # Area values
            }
        )

        # Mean SI = (70+80+75)/3 = 75
        # Total area = 3
        ratio = 75.0
        total_x = 300.0  # sum(x_i * EXPNS) = 3 * 100

        result = estimator._calculate_ratio_variance(plot_data, ratio, total_x)

        assert "variance" in result
        assert "se" in result
        assert result["variance"] >= 0
        assert result["se"] >= 0

    def test_variance_zero_for_identical_values(self, mock_db):
        """Test that variance is zero when all values are identical."""
        config = {"land_type": "forest"}
        estimator = SiteIndexEstimator(mock_db, config)

        # All plots have same SI
        plot_data = pl.DataFrame(
            {
                "PLT_CN": ["P1", "P2", "P3"],
                "STRATUM_CN": ["S1", "S1", "S1"],
                "EXPNS": [100.0, 100.0, 100.0],
                "y_i": [75.0, 75.0, 75.0],  # All same
                "x_i": [1.0, 1.0, 1.0],  # All same
            }
        )

        ratio = 75.0
        total_x = 300.0

        result = estimator._calculate_ratio_variance(plot_data, ratio, total_x)

        # Variance should be 0 when all values are identical
        assert result["variance"] == pytest.approx(0.0, abs=1e-10)
        assert result["se"] == pytest.approx(0.0, abs=1e-10)
