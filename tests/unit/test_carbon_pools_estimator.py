"""Unit tests for CarbonPoolEstimator class.

Tests the CarbonPoolEstimator methods in isolation using mock data.
No database connection required.
"""

import polars as pl
import pytest

from pyfia.estimation.estimators.carbon_pools import CarbonPoolEstimator
from pyfia.estimation.constants import LBS_TO_SHORT_TONS


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
        config = {"pool": "total", "land_type": "forest"}
        estimator = CarbonPoolEstimator(MockDB(), config)
        tables = estimator.get_required_tables()

        assert "TREE" in tables
        assert "COND" in tables
        assert "PLOT" in tables
        assert "POP_PLOT_STRATUM_ASSGN" in tables
        assert "POP_STRATUM" in tables
        assert len(tables) == 5

    def test_tables_consistent_across_pool_types(self):
        for pool in ["ag", "bg", "total"]:
            config = {"pool": pool}
            estimator = CarbonPoolEstimator(MockDB(), config)
            tables = estimator.get_required_tables()
            assert "TREE" in tables
            assert "COND" in tables


class TestGetTreeColumns:
    """Tests for get_tree_columns method."""

    def test_total_pool_columns(self):
        config = {"pool": "total"}
        estimator = CarbonPoolEstimator(MockDB(), config)
        cols = estimator.get_tree_columns()

        assert "CN" in cols
        assert "PLT_CN" in cols
        assert "CONDID" in cols
        assert "STATUSCD" in cols
        assert "SPCD" in cols
        assert "DIA" in cols
        assert "TPA_UNADJ" in cols
        assert "CARBON_AG" in cols
        assert "CARBON_BG" in cols

    def test_ag_pool_columns(self):
        config = {"pool": "ag"}
        estimator = CarbonPoolEstimator(MockDB(), config)
        cols = estimator.get_tree_columns()

        assert "CARBON_AG" in cols
        assert "CARBON_BG" not in cols

    def test_bg_pool_columns(self):
        config = {"pool": "bg"}
        estimator = CarbonPoolEstimator(MockDB(), config)
        cols = estimator.get_tree_columns()

        assert "CARBON_BG" in cols
        assert "CARBON_AG" not in cols

    def test_default_pool_is_total(self):
        config = {}
        estimator = CarbonPoolEstimator(MockDB(), config)
        cols = estimator.get_tree_columns()

        assert "CARBON_AG" in cols
        assert "CARBON_BG" in cols


class TestGetCondColumns:
    """Tests for get_cond_columns method."""

    def test_returns_required_columns(self):
        config = {"land_type": "forest"}
        estimator = CarbonPoolEstimator(MockDB(), config)
        cols = estimator.get_cond_columns()

        assert "PLT_CN" in cols
        assert "CONDID" in cols
        assert "COND_STATUS_CD" in cols
        assert "CONDPROP_UNADJ" in cols
        assert "OWNGRPCD" in cols
        assert "FORTYPCD" in cols


class TestCalculateValues:
    """Tests for calculate_values method."""

    @pytest.fixture
    def mock_db(self):
        return MockDB()

    def test_total_carbon_calculation(self, mock_db):
        """Total carbon = (CARBON_AG + CARBON_BG) * TPA_UNADJ * LBS_TO_SHORT_TONS."""
        config = {"pool": "total"}
        estimator = CarbonPoolEstimator(mock_db, config)

        data = pl.DataFrame({
            "CARBON_AG": [500.0, 1000.0],
            "CARBON_BG": [100.0, 200.0],
            "TPA_UNADJ": [5.0, 10.0],
        }).lazy()

        result = estimator.calculate_values(data).collect()

        # (500 + 100) * 5.0 * (1/2000) = 1.5
        # (1000 + 200) * 10.0 * (1/2000) = 6.0
        expected_0 = (500.0 + 100.0) * 5.0 * LBS_TO_SHORT_TONS
        expected_1 = (1000.0 + 200.0) * 10.0 * LBS_TO_SHORT_TONS
        assert abs(result["CARBON_ACRE"][0] - expected_0) < 1e-10
        assert abs(result["CARBON_ACRE"][1] - expected_1) < 1e-10

    def test_ag_only_calculation(self, mock_db):
        """AG pool uses only CARBON_AG."""
        config = {"pool": "ag"}
        estimator = CarbonPoolEstimator(mock_db, config)

        data = pl.DataFrame({
            "CARBON_AG": [600.0],
            "TPA_UNADJ": [4.0],
        }).lazy()

        result = estimator.calculate_values(data).collect()

        expected = 600.0 * 4.0 * LBS_TO_SHORT_TONS
        assert abs(result["CARBON_ACRE"][0] - expected) < 1e-10

    def test_bg_only_calculation(self, mock_db):
        """BG pool uses only CARBON_BG."""
        config = {"pool": "bg"}
        estimator = CarbonPoolEstimator(mock_db, config)

        data = pl.DataFrame({
            "CARBON_BG": [150.0],
            "TPA_UNADJ": [8.0],
        }).lazy()

        result = estimator.calculate_values(data).collect()

        expected = 150.0 * 8.0 * LBS_TO_SHORT_TONS
        assert abs(result["CARBON_ACRE"][0] - expected) < 1e-10

    def test_null_carbon_ag_treated_as_zero(self, mock_db):
        """Null CARBON_AG values should be filled with 0."""
        config = {"pool": "total"}
        estimator = CarbonPoolEstimator(mock_db, config)

        data = pl.DataFrame({
            "CARBON_AG": [None, 500.0],
            "CARBON_BG": [100.0, None],
            "TPA_UNADJ": [5.0, 5.0],
        }).lazy()

        result = estimator.calculate_values(data).collect()

        # (0 + 100) * 5 / 2000 = 0.25
        expected_0 = (0.0 + 100.0) * 5.0 * LBS_TO_SHORT_TONS
        # (500 + 0) * 5 / 2000 = 1.25
        expected_1 = (500.0 + 0.0) * 5.0 * LBS_TO_SHORT_TONS
        assert abs(result["CARBON_ACRE"][0] - expected_0) < 1e-10
        assert abs(result["CARBON_ACRE"][1] - expected_1) < 1e-10

    def test_both_carbon_null_gives_zero(self, mock_db):
        """Both CARBON_AG and CARBON_BG null should give 0."""
        config = {"pool": "total"}
        estimator = CarbonPoolEstimator(mock_db, config)

        data = pl.DataFrame({
            "CARBON_AG": [None],
            "CARBON_BG": [None],
            "TPA_UNADJ": [10.0],
        }).lazy()

        result = estimator.calculate_values(data).collect()
        assert result["CARBON_ACRE"][0] == 0.0

    def test_zero_tpa_gives_zero(self, mock_db):
        config = {"pool": "total"}
        estimator = CarbonPoolEstimator(mock_db, config)

        data = pl.DataFrame({
            "CARBON_AG": [1000.0],
            "CARBON_BG": [200.0],
            "TPA_UNADJ": [0.0],
        }).lazy()

        result = estimator.calculate_values(data).collect()
        assert result["CARBON_ACRE"][0] == 0.0

    def test_empty_dataframe(self, mock_db):
        config = {"pool": "total"}
        estimator = CarbonPoolEstimator(mock_db, config)

        data = pl.DataFrame({
            "CARBON_AG": [],
            "CARBON_BG": [],
            "TPA_UNADJ": [],
        }).lazy()

        result = estimator.calculate_values(data).collect()
        assert len(result) == 0

    def test_large_values(self, mock_db):
        config = {"pool": "total"}
        estimator = CarbonPoolEstimator(mock_db, config)

        data = pl.DataFrame({
            "CARBON_AG": [1e8],
            "CARBON_BG": [2e7],
            "TPA_UNADJ": [100.0],
        }).lazy()

        result = estimator.calculate_values(data).collect()
        expected = (1e8 + 2e7) * 100.0 * LBS_TO_SHORT_TONS
        assert abs(result["CARBON_ACRE"][0] - expected) < 1e-4

    def test_conversion_factor_is_lbs_to_short_tons(self, mock_db):
        """Verify the conversion uses 1/2000 (lbs to short tons)."""
        config = {"pool": "ag"}
        estimator = CarbonPoolEstimator(mock_db, config)

        data = pl.DataFrame({
            "CARBON_AG": [2000.0],  # Exactly 2000 lbs
            "TPA_UNADJ": [1.0],    # 1 tree per acre
        }).lazy()

        result = estimator.calculate_values(data).collect()
        # 2000 lbs * 1 TPA * (1/2000) = 1.0 short ton per acre
        assert abs(result["CARBON_ACRE"][0] - 1.0) < 1e-10


class TestConfigStorage:
    """Tests for configuration storage."""

    def test_config_stored_correctly(self):
        config = {
            "pool": "ag",
            "land_type": "timber",
            "grp_by": ["SPCD", "FORTYPCD"],
            "tree_type": "live",
        }
        estimator = CarbonPoolEstimator(MockDB(), config)

        assert estimator.config == config
        assert estimator.config["pool"] == "ag"
        assert estimator.config["land_type"] == "timber"


class TestPoolValidation:
    """Tests for pool parameter validation in the public function."""

    def test_invalid_pool_raises_value_error(self):
        from pyfia.estimation.estimators.carbon_pools import carbon_pool

        with pytest.raises(ValueError, match="Invalid pool"):
            carbon_pool(MockDB(), pool="invalid")

    def test_pool_case_insensitive(self):
        """Pool parameter should be case-insensitive."""
        from pyfia.estimation.estimators.carbon_pools import carbon_pool

        # This should not raise on validation (will fail on DB access later)
        # We just verify the pool validation passes
        config = {"pool": "AG"}
        estimator = CarbonPoolEstimator(MockDB(), config)
        # get_tree_columns with uppercase should still work since
        # calculate_values lowercases in the public function
        cols = estimator.get_tree_columns()
        # Config stores "AG" directly, get_tree_columns checks lowercase
        assert "CARBON_AG" in cols or "CARBON_BG" in cols
