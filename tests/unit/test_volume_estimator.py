"""Unit tests for VolumeEstimator class.

Tests the VolumeEstimator methods in isolation using mock data.
No database connection required.
"""

import math

import polars as pl
import pytest

from pyfia.estimation.estimators.volume import VolumeEstimator


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
        """Test that required tables for volume estimation are returned."""
        config = {"vol_type": "net", "land_type": "forest"}
        estimator = VolumeEstimator(MockDB(), config)
        tables = estimator.get_required_tables()

        assert "TREE" in tables
        assert "COND" in tables
        assert "PLOT" in tables
        assert "POP_PLOT_STRATUM_ASSGN" in tables
        assert "POP_STRATUM" in tables
        assert len(tables) == 5

    def test_tables_are_consistent_across_configs(self):
        """Test that table requirements do not change with different configs."""
        configs = [
            {"vol_type": "net"},
            {"vol_type": "gross"},
            {"vol_type": "sawlog"},
            {"land_type": "timber"},
            {"grp_by": "SPCD"},
        ]

        for config in configs:
            estimator = VolumeEstimator(MockDB(), config)
            tables = estimator.get_required_tables()
            assert "TREE" in tables
            assert "COND" in tables


class TestGetTreeColumns:
    """Tests for get_tree_columns method."""

    def test_net_volume_columns(self):
        """Test tree columns for net volume estimation."""
        config = {"vol_type": "net"}
        estimator = VolumeEstimator(MockDB(), config)
        cols = estimator.get_tree_columns()

        # Core columns
        assert "CN" in cols
        assert "PLT_CN" in cols
        assert "CONDID" in cols
        assert "STATUSCD" in cols
        assert "SPCD" in cols
        assert "DIA" in cols
        assert "TPA_UNADJ" in cols
        assert "TREECLCD" in cols
        # Net volume column
        assert "VOLCFNET" in cols

    def test_gross_volume_columns(self):
        """Test tree columns for gross volume estimation."""
        config = {"vol_type": "gross"}
        estimator = VolumeEstimator(MockDB(), config)
        cols = estimator.get_tree_columns()

        assert "VOLCFGRS" in cols
        assert "VOLCFNET" not in cols

    def test_sound_volume_columns(self):
        """Test tree columns for sound volume estimation."""
        config = {"vol_type": "sound"}
        estimator = VolumeEstimator(MockDB(), config)
        cols = estimator.get_tree_columns()

        assert "VOLCFSND" in cols

    def test_sawlog_volume_columns(self):
        """Test tree columns for sawlog (board feet) volume estimation."""
        config = {"vol_type": "sawlog"}
        estimator = VolumeEstimator(MockDB(), config)
        cols = estimator.get_tree_columns()

        assert "VOLBFNET" in cols
        assert "VOLBFGRS" in cols

    def test_with_grp_by_tree_column(self):
        """Test that grouping columns from TREE table are included."""
        config = {"vol_type": "net", "grp_by": "SPCD"}
        estimator = VolumeEstimator(MockDB(), config)
        cols = estimator.get_tree_columns()

        # SPCD is already in base columns, so no duplicate
        assert cols.count("SPCD") == 1

    def test_with_grp_by_height_column(self):
        """Test that HT column is added when grouping by height."""
        config = {"vol_type": "net", "grp_by": "HT"}
        estimator = VolumeEstimator(MockDB(), config)
        cols = estimator.get_tree_columns()

        assert "HT" in cols

    def test_with_multiple_grp_by_columns(self):
        """Test with multiple grouping columns."""
        config = {"vol_type": "net", "grp_by": ["SPCD", "CCLCD"]}
        estimator = VolumeEstimator(MockDB(), config)
        cols = estimator.get_tree_columns()

        assert "SPCD" in cols
        assert "CCLCD" in cols


class TestGetCondColumns:
    """Tests for get_cond_columns method."""

    def test_forest_land_type(self):
        """Test condition columns for forest land type."""
        config = {"land_type": "forest"}
        estimator = VolumeEstimator(MockDB(), config)
        cols = estimator.get_cond_columns()

        assert "PLT_CN" in cols
        assert "CONDID" in cols
        assert "COND_STATUS_CD" in cols
        assert "CONDPROP_UNADJ" in cols
        assert "PROP_BASIS" in cols
        # Timberland columns should NOT be present for forest
        assert "SITECLCD" not in cols
        assert "RESERVCD" not in cols

    def test_timber_land_type(self):
        """Test condition columns for timber land type."""
        config = {"land_type": "timber"}
        estimator = VolumeEstimator(MockDB(), config)
        cols = estimator.get_cond_columns()

        # Should include timberland columns
        assert "SITECLCD" in cols
        assert "RESERVCD" in cols

    def test_with_grp_by_cond_column(self):
        """Test that condition grouping columns are included."""
        config = {"land_type": "forest", "grp_by": "FORTYPCD"}
        estimator = VolumeEstimator(MockDB(), config)
        cols = estimator.get_cond_columns()

        assert "FORTYPCD" in cols

    def test_with_multiple_cond_grp_by(self):
        """Test with multiple condition grouping columns."""
        config = {"land_type": "forest", "grp_by": ["OWNGRPCD", "FORTYPCD"]}
        estimator = VolumeEstimator(MockDB(), config)
        cols = estimator.get_cond_columns()

        assert "OWNGRPCD" in cols
        assert "FORTYPCD" in cols


class TestCalculateValues:
    """Tests for calculate_values method."""

    @pytest.fixture
    def mock_db(self):
        """Create mock database for testing."""
        return MockDB()

    def test_net_volume_calculation(self, mock_db):
        """Test net volume per acre calculation."""
        config = {"vol_type": "net"}
        estimator = VolumeEstimator(mock_db, config)

        # Create test data with known values
        data = pl.DataFrame({
            "VOLCFNET": [100.0, 200.0, 150.0],
            "TPA_UNADJ": [5.0, 10.0, 7.5],
        }).lazy()

        result = estimator.calculate_values(data).collect()

        # Volume per acre = VOLCFNET * TPA_UNADJ
        expected = [500.0, 2000.0, 1125.0]
        assert result["VOLUME_ACRE"].to_list() == expected

    def test_gross_volume_calculation(self, mock_db):
        """Test gross volume per acre calculation."""
        config = {"vol_type": "gross"}
        estimator = VolumeEstimator(mock_db, config)

        data = pl.DataFrame({
            "VOLCFGRS": [120.0, 250.0],
            "TPA_UNADJ": [5.0, 10.0],
        }).lazy()

        result = estimator.calculate_values(data).collect()

        expected = [600.0, 2500.0]
        assert result["VOLUME_ACRE"].to_list() == expected

    def test_sound_volume_calculation(self, mock_db):
        """Test sound volume per acre calculation."""
        config = {"vol_type": "sound"}
        estimator = VolumeEstimator(mock_db, config)

        data = pl.DataFrame({
            "VOLCFSND": [110.0, 220.0],
            "TPA_UNADJ": [5.0, 10.0],
        }).lazy()

        result = estimator.calculate_values(data).collect()

        expected = [550.0, 2200.0]
        assert result["VOLUME_ACRE"].to_list() == expected

    def test_sawlog_volume_calculation(self, mock_db):
        """Test sawlog (board feet) volume calculation."""
        config = {"vol_type": "sawlog"}
        estimator = VolumeEstimator(mock_db, config)

        data = pl.DataFrame({
            "VOLBFNET": [500.0, 1000.0],
            "TPA_UNADJ": [2.0, 3.0],
        }).lazy()

        result = estimator.calculate_values(data).collect()

        # Board feet per acre
        expected = [1000.0, 3000.0]
        assert result["VOLUME_ACRE"].to_list() == expected

    def test_null_volume_handling(self, mock_db):
        """Test handling of null volume values."""
        config = {"vol_type": "net"}
        estimator = VolumeEstimator(mock_db, config)

        data = pl.DataFrame({
            "VOLCFNET": [100.0, None, 150.0],
            "TPA_UNADJ": [5.0, 10.0, 7.5],
        }).lazy()

        result = estimator.calculate_values(data).collect()

        # Null * number = null
        assert result["VOLUME_ACRE"][0] == 500.0
        assert result["VOLUME_ACRE"][1] is None
        assert result["VOLUME_ACRE"][2] == 1125.0

    def test_null_tpa_handling(self, mock_db):
        """Test handling of null TPA values."""
        config = {"vol_type": "net"}
        estimator = VolumeEstimator(mock_db, config)

        data = pl.DataFrame({
            "VOLCFNET": [100.0, 200.0],
            "TPA_UNADJ": [5.0, None],
        }).lazy()

        result = estimator.calculate_values(data).collect()

        assert result["VOLUME_ACRE"][0] == 500.0
        assert result["VOLUME_ACRE"][1] is None

    def test_zero_volume_handling(self, mock_db):
        """Test handling of zero volume values."""
        config = {"vol_type": "net"}
        estimator = VolumeEstimator(mock_db, config)

        data = pl.DataFrame({
            "VOLCFNET": [0.0, 100.0],
            "TPA_UNADJ": [5.0, 0.0],
        }).lazy()

        result = estimator.calculate_values(data).collect()

        assert result["VOLUME_ACRE"][0] == 0.0
        assert result["VOLUME_ACRE"][1] == 0.0

    def test_default_vol_type(self, mock_db):
        """Test that default vol_type is 'net'."""
        config = {}  # No vol_type specified
        estimator = VolumeEstimator(mock_db, config)

        data = pl.DataFrame({
            "VOLCFNET": [100.0],
            "TPA_UNADJ": [5.0],
        }).lazy()

        result = estimator.calculate_values(data).collect()
        assert result["VOLUME_ACRE"][0] == 500.0


class TestEdgeCases:
    """Test edge cases and error conditions."""

    def test_empty_dataframe(self):
        """Test handling of empty dataframe."""
        config = {"vol_type": "net"}
        estimator = VolumeEstimator(MockDB(), config)

        data = pl.DataFrame({
            "VOLCFNET": [],
            "TPA_UNADJ": [],
        }).lazy()

        result = estimator.calculate_values(data).collect()
        assert len(result) == 0

    def test_large_values(self):
        """Test handling of very large volume values."""
        config = {"vol_type": "net"}
        estimator = VolumeEstimator(MockDB(), config)

        data = pl.DataFrame({
            "VOLCFNET": [1e10],
            "TPA_UNADJ": [100.0],
        }).lazy()

        result = estimator.calculate_values(data).collect()
        assert result["VOLUME_ACRE"][0] == 1e12

    def test_small_values(self):
        """Test handling of very small volume values."""
        config = {"vol_type": "net"}
        estimator = VolumeEstimator(MockDB(), config)

        data = pl.DataFrame({
            "VOLCFNET": [0.001],
            "TPA_UNADJ": [0.01],
        }).lazy()

        result = estimator.calculate_values(data).collect()
        assert abs(result["VOLUME_ACRE"][0] - 0.00001) < 1e-10

    def test_negative_volume_preserved(self):
        """Test that negative volumes are preserved (could indicate data issues)."""
        config = {"vol_type": "net"}
        estimator = VolumeEstimator(MockDB(), config)

        # Negative volumes shouldn't happen in real data but test handling
        data = pl.DataFrame({
            "VOLCFNET": [-100.0],
            "TPA_UNADJ": [5.0],
        }).lazy()

        result = estimator.calculate_values(data).collect()
        assert result["VOLUME_ACRE"][0] == -500.0


class TestConfigStorage:
    """Tests for configuration storage."""

    def test_config_stored_correctly(self):
        """Test that config is stored correctly in estimator."""
        config = {
            "vol_type": "net",
            "land_type": "timber",
            "grp_by": ["SPCD", "FORTYPCD"],
            "tree_type": "live",
        }
        estimator = VolumeEstimator(MockDB(), config)

        assert estimator.config == config
        assert estimator.config["vol_type"] == "net"
        assert estimator.config["land_type"] == "timber"
        assert estimator.config["grp_by"] == ["SPCD", "FORTYPCD"]

