"""Unit tests for CarbonStockChangeEstimator and stock_change function.

Tests the stock-change logic in isolation using mock data.
No database connection required.
"""

import polars as pl
import pytest

from pyfia.carbon.stock_change import (
    CarbonStockChangeEstimator,
    _POOL_COLUMNS,
    _VALID_POOLS,
    stock_change,
)


class MockDB:
    """Mock database for testing estimator methods in isolation."""

    def __init__(self):
        self.db_path = "/fake/path"
        self.tables = {}
        self.evalid = None
        self.evalids = None
        self._state_filter = None


class TestGetRequiredTables:
    def test_returns_condition_level_tables(self):
        config = {"pool": "downed_dead", "land_type": "forest"}
        estimator = CarbonStockChangeEstimator(MockDB(), config)
        tables = estimator.get_required_tables()

        assert "COND" in tables
        assert "PLOT" in tables
        assert "POP_PLOT_STRATUM_ASSGN" in tables
        assert "POP_STRATUM" in tables

    def test_tree_table_not_required(self):
        config = {"pool": "litter", "land_type": "forest"}
        estimator = CarbonStockChangeEstimator(MockDB(), config)
        assert "TREE" not in estimator.get_required_tables()


class TestGetTreeColumns:
    def test_returns_empty_list(self):
        config = {"pool": "soil_organic"}
        estimator = CarbonStockChangeEstimator(MockDB(), config)
        assert estimator.get_tree_columns() == []


class TestGetCondColumns:
    def test_includes_carbon_column_for_pool(self):
        for pool, expected_cols in _POOL_COLUMNS.items():
            config = {"pool": pool, "land_type": "forest"}
            estimator = CarbonStockChangeEstimator(MockDB(), config)
            cols = estimator.get_cond_columns()
            for ec in expected_cols:
                assert ec in cols, f"{ec} missing for pool {pool}"


class TestCalculateValues:
    """Tests for the delta calculation logic."""

    @pytest.fixture
    def mock_db(self):
        return MockDB()

    def test_basic_delta(self, mock_db):
        config = {"pool": "downed_dead", "annualize": False}
        estimator = CarbonStockChangeEstimator(mock_db, config)

        data = pl.DataFrame(
            {
                "t2_CARBON_DOWN_DEAD": [5.0, 3.0],
                "t1_CARBON_DOWN_DEAD": [4.0, 4.0],
                "REMPER": [5.0, 5.0],
            }
        ).lazy()

        result = estimator.calculate_values(data).collect()
        assert abs(result["CARBON_CHANGE_ACRE"][0] - 1.0) < 1e-10
        assert abs(result["CARBON_CHANGE_ACRE"][1] - (-1.0)) < 1e-10

    def test_annualized_delta(self, mock_db):
        config = {"pool": "litter", "annualize": True}
        estimator = CarbonStockChangeEstimator(mock_db, config)

        data = pl.DataFrame(
            {
                "t2_CARBON_LITTER": [10.0],
                "t1_CARBON_LITTER": [5.0],
                "REMPER": [5.0],
            }
        ).lazy()

        result = estimator.calculate_values(data).collect()
        # (10 - 5) / 5 = 1.0
        assert abs(result["CARBON_CHANGE_ACRE"][0] - 1.0) < 1e-10

    def test_understory_sums_ag_bg(self, mock_db):
        config = {"pool": "understory", "annualize": False}
        estimator = CarbonStockChangeEstimator(mock_db, config)

        data = pl.DataFrame(
            {
                "t2_CARBON_UNDERSTORY_AG": [1.2],
                "t2_CARBON_UNDERSTORY_BG": [0.3],
                "t1_CARBON_UNDERSTORY_AG": [1.0],
                "t1_CARBON_UNDERSTORY_BG": [0.2],
                "REMPER": [5.0],
            }
        ).lazy()

        result = estimator.calculate_values(data).collect()
        # (1.2+0.3) - (1.0+0.2) = 0.3
        assert abs(result["CARBON_CHANGE_ACRE"][0] - 0.3) < 1e-10

    def test_null_t2_treated_as_zero(self, mock_db):
        config = {"pool": "soil_organic", "annualize": False}
        estimator = CarbonStockChangeEstimator(mock_db, config)

        data = pl.DataFrame(
            {
                "t2_CARBON_SOIL_ORG": [None],
                "t1_CARBON_SOIL_ORG": [20.0],
                "REMPER": [5.0],
            }
        ).lazy()

        result = estimator.calculate_values(data).collect()
        assert abs(result["CARBON_CHANGE_ACRE"][0] - (-20.0)) < 1e-10

    def test_null_t1_treated_as_zero(self, mock_db):
        config = {"pool": "soil_organic", "annualize": False}
        estimator = CarbonStockChangeEstimator(mock_db, config)

        data = pl.DataFrame(
            {
                "t2_CARBON_SOIL_ORG": [25.0],
                "t1_CARBON_SOIL_ORG": [None],
                "REMPER": [5.0],
            }
        ).lazy()

        result = estimator.calculate_values(data).collect()
        assert abs(result["CARBON_CHANGE_ACRE"][0] - 25.0) < 1e-10

    def test_negative_change(self, mock_db):
        """Carbon loss produces negative values."""
        config = {"pool": "downed_dead", "annualize": True}
        estimator = CarbonStockChangeEstimator(mock_db, config)

        data = pl.DataFrame(
            {
                "t2_CARBON_DOWN_DEAD": [2.0],
                "t1_CARBON_DOWN_DEAD": [5.0],
                "REMPER": [6.0],
            }
        ).lazy()

        result = estimator.calculate_values(data).collect()
        # (2 - 5) / 6 = -0.5
        assert abs(result["CARBON_CHANGE_ACRE"][0] - (-0.5)) < 1e-10


class TestPoolValidation:
    def test_live_tree_raises_error(self):
        with pytest.raises(ValueError, match="not yet implemented"):
            stock_change(MockDB(), pool="live_tree")

    def test_standing_dead_raises_error(self):
        with pytest.raises(ValueError, match="not yet implemented"):
            stock_change(MockDB(), pool="standing_dead")

    def test_invalid_pool_raises_error(self):
        with pytest.raises(ValueError, match="Invalid pool"):
            stock_change(MockDB(), pool="invalid")

    def test_valid_pools_accepted(self):
        """Valid pools should pass pool validation (fail later on DB access)."""
        for pool in _VALID_POOLS:
            with pytest.raises(Exception):
                stock_change(MockDB(), pool=pool)

    def test_all_pool_accepted(self):
        with pytest.raises(Exception):
            stock_change(MockDB(), pool="all")

    def test_list_of_pools_accepted(self):
        with pytest.raises(Exception):
            stock_change(MockDB(), pool=["downed_dead", "litter"])

    def test_list_with_tree_pool_raises(self):
        with pytest.raises(ValueError, match="not yet implemented"):
            stock_change(MockDB(), pool=["downed_dead", "live_tree"])


class TestEstimatorLabel:
    def test_label(self):
        config = {"pool": "downed_dead"}
        estimator = CarbonStockChangeEstimator(MockDB(), config)
        assert estimator._estimator_label == "CarbonStockChange"


class TestPoolColumnMapping:
    def test_all_valid_pools_have_columns(self):
        for pool in _VALID_POOLS:
            assert pool in _POOL_COLUMNS
            assert len(_POOL_COLUMNS[pool]) > 0

    def test_understory_has_ag_and_bg(self):
        cols = _POOL_COLUMNS["understory"]
        assert "CARBON_UNDERSTORY_AG" in cols
        assert "CARBON_UNDERSTORY_BG" in cols
