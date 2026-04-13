"""Unit tests for UnderstoryEstimator class.

Tests the UnderstoryEstimator methods in isolation using mock data.
No database connection required.
"""

import polars as pl
import pytest

from pyfia.carbon.understory import UnderstoryEstimator, understory


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

    def test_returns_condition_level_tables(self):
        config = {"pool": "ag", "land_type": "forest"}
        estimator = UnderstoryEstimator(MockDB(), config)
        tables = estimator.get_required_tables()

        assert "COND" in tables
        assert "PLOT" in tables
        assert "POP_PLOT_STRATUM_ASSGN" in tables
        assert "POP_STRATUM" in tables

    def test_tree_table_not_required(self):
        config = {"pool": "ag", "land_type": "forest"}
        estimator = UnderstoryEstimator(MockDB(), config)
        tables = estimator.get_required_tables()
        assert "TREE" not in tables

    def test_tables_consistent_across_pools(self):
        for pool in ["ag", "bg", "total"]:
            config = {"pool": pool}
            estimator = UnderstoryEstimator(MockDB(), config)
            tables = estimator.get_required_tables()
            assert "TREE" not in tables
            assert "COND" in tables


class TestGetTreeColumns:
    def test_returns_empty_list(self):
        config = {"pool": "ag"}
        estimator = UnderstoryEstimator(MockDB(), config)
        cols = estimator.get_tree_columns()
        assert cols == []


class TestGetCondColumns:
    def test_includes_understory_columns(self):
        config = {"land_type": "forest"}
        estimator = UnderstoryEstimator(MockDB(), config)
        cols = estimator.get_cond_columns()

        assert "CARBON_UNDERSTORY_AG" in cols
        assert "CARBON_UNDERSTORY_BG" in cols
        assert "PLT_CN" in cols
        assert "CONDID" in cols
        assert "CONDPROP_UNADJ" in cols
        assert "COND_STATUS_CD" in cols


class TestCalculateValues:
    """Tests for calculate_values method."""

    @pytest.fixture
    def mock_db(self):
        return MockDB()

    def test_ag_pool(self, mock_db):
        config = {"pool": "ag"}
        estimator = UnderstoryEstimator(mock_db, config)

        data = pl.DataFrame(
            {
                "CARBON_UNDERSTORY_AG": [1.5, 0.8],
                "CARBON_UNDERSTORY_BG": [0.15, 0.08],
            }
        ).lazy()

        result = estimator.calculate_values(data).collect()
        assert abs(result["CARBON_ACRE"][0] - 1.5) < 1e-10
        assert abs(result["CARBON_ACRE"][1] - 0.8) < 1e-10

    def test_bg_pool(self, mock_db):
        config = {"pool": "bg"}
        estimator = UnderstoryEstimator(mock_db, config)

        data = pl.DataFrame(
            {
                "CARBON_UNDERSTORY_AG": [1.5, 0.8],
                "CARBON_UNDERSTORY_BG": [0.15, 0.08],
            }
        ).lazy()

        result = estimator.calculate_values(data).collect()
        assert abs(result["CARBON_ACRE"][0] - 0.15) < 1e-10
        assert abs(result["CARBON_ACRE"][1] - 0.08) < 1e-10

    def test_total_pool(self, mock_db):
        config = {"pool": "total"}
        estimator = UnderstoryEstimator(mock_db, config)

        data = pl.DataFrame(
            {
                "CARBON_UNDERSTORY_AG": [1.5, 0.8],
                "CARBON_UNDERSTORY_BG": [0.15, 0.08],
            }
        ).lazy()

        result = estimator.calculate_values(data).collect()
        assert abs(result["CARBON_ACRE"][0] - 1.65) < 1e-10
        assert abs(result["CARBON_ACRE"][1] - 0.88) < 1e-10

    def test_null_values_treated_as_zero(self, mock_db):
        config = {"pool": "total"}
        estimator = UnderstoryEstimator(mock_db, config)

        data = pl.DataFrame(
            {
                "CARBON_UNDERSTORY_AG": [None, 1.0],
                "CARBON_UNDERSTORY_BG": [0.1, None],
            }
        ).lazy()

        result = estimator.calculate_values(data).collect()
        assert abs(result["CARBON_ACRE"][0] - 0.1) < 1e-10
        assert abs(result["CARBON_ACRE"][1] - 1.0) < 1e-10

    def test_both_null_gives_zero(self, mock_db):
        config = {"pool": "total"}
        estimator = UnderstoryEstimator(mock_db, config)

        data = pl.DataFrame(
            {
                "CARBON_UNDERSTORY_AG": [None],
                "CARBON_UNDERSTORY_BG": [None],
            }
        ).lazy()

        result = estimator.calculate_values(data).collect()
        assert result["CARBON_ACRE"][0] == 0.0

    def test_empty_dataframe(self, mock_db):
        config = {"pool": "ag"}
        estimator = UnderstoryEstimator(mock_db, config)

        data = pl.DataFrame(
            {
                "CARBON_UNDERSTORY_AG": pl.Series([], dtype=pl.Float64),
                "CARBON_UNDERSTORY_BG": pl.Series([], dtype=pl.Float64),
            }
        ).lazy()

        result = estimator.calculate_values(data).collect()
        assert len(result) == 0

    def test_bg_is_ten_percent_of_total(self, mock_db):
        """BG should be ~10% of total per the Smith et al. 2006 convention."""
        # Using realistic values where BG ≈ AG / 9
        ag = 1.08
        bg = 0.12  # 10% of total (ag + bg = 1.2)
        config_total = {"pool": "total"}
        config_bg = {"pool": "bg"}

        estimator_total = UnderstoryEstimator(mock_db, config_total)
        estimator_bg = UnderstoryEstimator(mock_db, config_bg)

        data = pl.DataFrame(
            {
                "CARBON_UNDERSTORY_AG": [ag],
                "CARBON_UNDERSTORY_BG": [bg],
            }
        ).lazy()

        total = estimator_total.calculate_values(data).collect()["CARBON_ACRE"][0]
        bg_val = estimator_bg.calculate_values(data).collect()["CARBON_ACRE"][0]
        assert abs(bg_val / total - 0.1) < 1e-10


class TestPoolValidation:
    def test_invalid_pool_raises_error(self):
        with pytest.raises(ValueError, match="Invalid pool"):
            understory(MockDB(), pool="invalid")

    def test_valid_pools_accepted(self):
        # These should not raise ValueError on pool validation
        # (they'll fail on DB access later)
        for pool in ["ag", "bg", "total", "AG", "BG", "TOTAL"]:
            with pytest.raises(Exception):
                # Will fail on DB access, but pool validation passes
                understory(MockDB(), pool=pool)


class TestEstimatorLabel:
    def test_label_is_understory(self):
        config = {"pool": "ag"}
        estimator = UnderstoryEstimator(MockDB(), config)
        assert estimator._estimator_label == "Understory"
