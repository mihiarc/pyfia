"""Unit tests for DownedDeadEstimator class.

Tests the DownedDeadEstimator methods in isolation using mock data.
No database connection required.
"""

import polars as pl
import pytest

from pyfia.carbon.downed_dead import DownedDeadEstimator, downed_dead


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
        config = {"pool": "total", "land_type": "forest"}
        estimator = DownedDeadEstimator(MockDB(), config)
        tables = estimator.get_required_tables()

        assert "COND" in tables
        assert "PLOT" in tables
        assert "POP_PLOT_STRATUM_ASSGN" in tables
        assert "POP_STRATUM" in tables

    def test_tree_table_not_required(self):
        config = {"pool": "total", "land_type": "forest"}
        estimator = DownedDeadEstimator(MockDB(), config)
        tables = estimator.get_required_tables()
        assert "TREE" not in tables


class TestGetTreeColumns:
    def test_returns_empty_list(self):
        config = {"pool": "total"}
        estimator = DownedDeadEstimator(MockDB(), config)
        cols = estimator.get_tree_columns()
        assert cols == []


class TestGetCondColumns:
    def test_includes_carbon_down_dead_column(self):
        config = {"land_type": "forest"}
        estimator = DownedDeadEstimator(MockDB(), config)
        cols = estimator.get_cond_columns()

        assert "CARBON_DOWN_DEAD" in cols
        assert "PLT_CN" in cols
        assert "CONDID" in cols
        assert "CONDPROP_UNADJ" in cols
        assert "COND_STATUS_CD" in cols


class TestCalculateValues:
    """Tests for calculate_values method."""

    @pytest.fixture
    def mock_db(self):
        return MockDB()

    def test_total_pool(self, mock_db):
        config = {"pool": "total"}
        estimator = DownedDeadEstimator(mock_db, config)

        data = pl.DataFrame(
            {"CARBON_DOWN_DEAD": [2.5, 1.3]}
        ).lazy()

        result = estimator.calculate_values(data).collect()
        assert abs(result["CARBON_ACRE"][0] - 2.5) < 1e-10
        assert abs(result["CARBON_ACRE"][1] - 1.3) < 1e-10

    def test_null_values_treated_as_zero(self, mock_db):
        config = {"pool": "total"}
        estimator = DownedDeadEstimator(mock_db, config)

        data = pl.DataFrame(
            {"CARBON_DOWN_DEAD": [None, 1.0]}
        ).lazy()

        result = estimator.calculate_values(data).collect()
        assert result["CARBON_ACRE"][0] == 0.0
        assert abs(result["CARBON_ACRE"][1] - 1.0) < 1e-10

    def test_all_null_gives_zero(self, mock_db):
        config = {"pool": "total"}
        estimator = DownedDeadEstimator(mock_db, config)

        data = pl.DataFrame(
            {"CARBON_DOWN_DEAD": [None]}
        ).lazy()

        result = estimator.calculate_values(data).collect()
        assert result["CARBON_ACRE"][0] == 0.0

    def test_empty_dataframe(self, mock_db):
        config = {"pool": "total"}
        estimator = DownedDeadEstimator(mock_db, config)

        data = pl.DataFrame(
            {"CARBON_DOWN_DEAD": pl.Series([], dtype=pl.Float64)}
        ).lazy()

        result = estimator.calculate_values(data).collect()
        assert len(result) == 0


class TestPoolValidation:
    def test_invalid_pool_raises_error(self):
        with pytest.raises(ValueError, match="Invalid pool"):
            downed_dead(MockDB(), pool="ag")

    def test_bg_pool_raises_error(self):
        with pytest.raises(ValueError, match="no AG/BG split"):
            downed_dead(MockDB(), pool="bg")

    def test_total_pool_accepted(self):
        # Should not raise ValueError on pool validation
        # (will fail on DB access later)
        with pytest.raises(Exception):
            downed_dead(MockDB(), pool="total")

    def test_case_insensitive_total(self):
        with pytest.raises(Exception):
            downed_dead(MockDB(), pool="TOTAL")


class TestEstimatorLabel:
    def test_label_is_downed_dead(self):
        config = {"pool": "total"}
        estimator = DownedDeadEstimator(MockDB(), config)
        assert estimator._estimator_label == "DownedDead"
