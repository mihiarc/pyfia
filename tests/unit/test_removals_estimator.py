"""Unit tests for RemovalsEstimator class.

Tests the RemovalsEstimator methods in isolation using mock data.
No database connection required.
"""

import polars as pl
import pytest

from pyfia.estimation.estimators.removals import RemovalsEstimator
from pyfia.estimation.constants import LBS_TO_SHORT_TONS


class MockDB:
    """Mock database for testing estimator methods in isolation."""

    def __init__(self):
        self.db_path = "/fake/path"
        self.tables = {}
        self.evalid = [132303]  # Pre-set to avoid GRM auto-filter
        self.evalids = None
        self._state_filter = None


class TestComponentType:
    """Tests for component_type property."""

    def test_component_type_is_removals(self):
        config = {"measure": "volume"}
        estimator = RemovalsEstimator(MockDB(), config)
        assert estimator.component_type == "removals"

    def test_metric_prefix_is_remv(self):
        config = {"measure": "volume"}
        estimator = RemovalsEstimator(MockDB(), config)
        assert estimator.metric_prefix == "REMV"


class TestGetTreeColumns:
    """Tests for get_tree_columns method."""

    def test_base_columns_always_present(self):
        config = {"measure": "volume"}
        estimator = RemovalsEstimator(MockDB(), config)
        cols = estimator.get_tree_columns()

        assert "CN" in cols
        assert "PLT_CN" in cols
        assert "CONDID" in cols
        assert "STATUSCD" in cols
        assert "SPCD" in cols
        assert "DIA" in cols
        assert "TPA_UNADJ" in cols

    def test_biomass_columns_added(self):
        config = {"measure": "biomass"}
        estimator = RemovalsEstimator(MockDB(), config)
        cols = estimator.get_tree_columns()

        assert "DRYBIO_AG" in cols
        assert "DRYBIO_BG" in cols

    def test_volume_doesnt_add_biomass_columns(self):
        config = {"measure": "volume"}
        estimator = RemovalsEstimator(MockDB(), config)
        cols = estimator.get_tree_columns()

        assert "DRYBIO_AG" not in cols
        assert "DRYBIO_BG" not in cols


class TestCalculateValues:
    """Tests for calculate_values method."""

    @pytest.fixture
    def mock_db(self):
        return MockDB()

    def test_volume_measure(self, mock_db):
        """Volume removals = TPA_UNADJ * VOLCFNET."""
        config = {"measure": "volume"}
        estimator = RemovalsEstimator(mock_db, config)

        data = pl.DataFrame({
            "TPA_UNADJ": [5.0, 10.0, 2.5],
            "VOLCFNET": [100.0, 200.0, 50.0],
        }).lazy()

        result = estimator.calculate_values(data).collect()

        assert result["REMV_ANNUAL"][0] == 500.0
        assert result["REMV_ANNUAL"][1] == 2000.0
        assert result["REMV_ANNUAL"][2] == 125.0

    def test_biomass_measure(self, mock_db):
        """Biomass removals = TPA_UNADJ * (DRYBIO_BOLE + DRYBIO_BRANCH) * LBS_TO_SHORT_TONS."""
        config = {"measure": "biomass"}
        estimator = RemovalsEstimator(mock_db, config)

        data = pl.DataFrame({
            "TPA_UNADJ": [4.0],
            "DRYBIO_BOLE": [1000.0],
            "DRYBIO_BRANCH": [500.0],
        }).lazy()

        result = estimator.calculate_values(data).collect()
        expected = 4.0 * (1000.0 + 500.0) * LBS_TO_SHORT_TONS
        assert abs(result["REMV_ANNUAL"][0] - expected) < 1e-10

    def test_count_measure(self, mock_db):
        """Count measure = TPA_UNADJ directly."""
        config = {"measure": "count"}
        estimator = RemovalsEstimator(mock_db, config)

        data = pl.DataFrame({
            "TPA_UNADJ": [7.5, 3.2],
        }).lazy()

        result = estimator.calculate_values(data).collect()
        assert result["REMV_ANNUAL"][0] == 7.5
        assert result["REMV_ANNUAL"][1] == 3.2

    def test_null_volume_handling(self, mock_db):
        """Null volumes should propagate as null."""
        config = {"measure": "volume"}
        estimator = RemovalsEstimator(mock_db, config)

        data = pl.DataFrame({
            "TPA_UNADJ": [5.0, 10.0],
            "VOLCFNET": [None, 200.0],
        }).lazy()

        result = estimator.calculate_values(data).collect()
        assert result["REMV_ANNUAL"][0] is None
        assert result["REMV_ANNUAL"][1] == 2000.0

    def test_zero_tpa_gives_zero(self, mock_db):
        config = {"measure": "volume"}
        estimator = RemovalsEstimator(mock_db, config)

        data = pl.DataFrame({
            "TPA_UNADJ": [0.0],
            "VOLCFNET": [100.0],
        }).lazy()

        result = estimator.calculate_values(data).collect()
        assert result["REMV_ANNUAL"][0] == 0.0

    def test_empty_dataframe(self, mock_db):
        config = {"measure": "volume"}
        estimator = RemovalsEstimator(mock_db, config)

        data = pl.DataFrame({
            "TPA_UNADJ": [],
            "VOLCFNET": [],
        }).lazy()

        result = estimator.calculate_values(data).collect()
        assert len(result) == 0

    def test_large_values(self, mock_db):
        config = {"measure": "volume"}
        estimator = RemovalsEstimator(mock_db, config)

        data = pl.DataFrame({
            "TPA_UNADJ": [1e6],
            "VOLCFNET": [1e4],
        }).lazy()

        result = estimator.calculate_values(data).collect()
        assert result["REMV_ANNUAL"][0] == 1e10

    def test_biomass_conversion_to_short_tons(self, mock_db):
        """Verify biomass uses lbs-to-short-tons conversion (1/2000)."""
        config = {"measure": "biomass"}
        estimator = RemovalsEstimator(mock_db, config)

        data = pl.DataFrame({
            "TPA_UNADJ": [1.0],
            "DRYBIO_BOLE": [2000.0],
            "DRYBIO_BRANCH": [0.0],
        }).lazy()

        result = estimator.calculate_values(data).collect()
        # 1.0 * 2000 * (1/2000) = 1.0 short ton
        assert abs(result["REMV_ANNUAL"][0] - 1.0) < 1e-10


class TestConfigStorage:
    """Tests for configuration storage."""

    def test_config_stored_correctly(self):
        config = {
            "measure": "volume",
            "land_type": "forest",
            "tree_type": "gs",
            "grp_by": "FORTYPCD",
            "remeasure_period": 5.0,
        }
        estimator = RemovalsEstimator(MockDB(), config)

        assert estimator.config == config
        assert estimator.config["measure"] == "volume"
        assert estimator.config["remeasure_period"] == 5.0


class TestSizeClassValidation:
    """Tests for size_class_type validation in the public function."""

    def test_invalid_size_class_type_raises(self):
        from pyfia.estimation.estimators.removals import removals

        with pytest.raises(ValueError, match="size_class_type must be one of"):
            removals(MockDB(), size_class_type="invalid")
