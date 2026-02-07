"""Unit tests for GrowthEstimator class.

Tests the GrowthEstimator methods in isolation using mock data.
No database connection required.
"""

import polars as pl
import pytest

from pyfia.estimation.estimators.growth import GrowthEstimator
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

    def test_component_type_is_growth(self):
        config = {"measure": "volume"}
        estimator = GrowthEstimator(MockDB(), config)
        assert estimator.component_type == "growth"

    def test_metric_prefix_is_growth(self):
        config = {"measure": "volume"}
        estimator = GrowthEstimator(MockDB(), config)
        assert estimator.metric_prefix == "GROWTH"


class TestGetCondColumns:
    """Tests for get_cond_columns method."""

    def test_returns_required_columns(self):
        config = {"land_type": "forest"}
        estimator = GrowthEstimator(MockDB(), config)
        cols = estimator.get_cond_columns()

        assert "PLT_CN" in cols
        assert "CONDID" in cols
        assert "COND_STATUS_CD" in cols
        assert "CONDPROP_UNADJ" in cols

    def test_adds_grp_by_columns(self):
        config = {"land_type": "forest", "grp_by": ["FORTYPCD", "OWNGRPCD"]}
        estimator = GrowthEstimator(MockDB(), config)
        cols = estimator.get_cond_columns()

        assert "FORTYPCD" in cols
        assert "OWNGRPCD" in cols

    def test_grp_by_string_handled(self):
        config = {"land_type": "forest", "grp_by": "FORTYPCD"}
        estimator = GrowthEstimator(MockDB(), config)
        cols = estimator.get_cond_columns()

        assert "FORTYPCD" in cols


class TestCalculateValues:
    """Tests for calculate_values method with BEGINEND ONEORTWO methodology."""

    @pytest.fixture
    def mock_db(self):
        return MockDB()

    def test_volume_survivor_ending(self, mock_db):
        """ONEORTWO=2 with SURVIVOR: adds TREE_VOLCFNET / REMPER."""
        config = {"measure": "volume"}
        estimator = GrowthEstimator(mock_db, config)

        data = pl.DataFrame({
            "ONEORTWO": [2],
            "COMPONENT": ["SURVIVOR"],
            "TREE_VOLCFNET": [500.0],
            "MIDPT_VOLCFNET": [400.0],
            "BEGIN_VOLCFNET": [300.0],
            "PTREE_VOLCFNET": [280.0],
            "TPA_UNADJ": [6.0],
            "REMPER": [5.0],
        }).lazy()

        result = estimator.calculate_values(data).collect()

        # ONEORTWO=2, SURVIVOR: ending_volume = TREE_VOLCFNET / REMPER = 500/5 = 100
        # GROWTH_VALUE = TPA_UNADJ * volume_contribution = 6.0 * 100.0 = 600.0
        assert abs(result["GROWTH_VALUE"][0] - 600.0) < 1e-10

    def test_volume_survivor_beginning(self, mock_db):
        """ONEORTWO=1 with SURVIVOR: subtracts BEGIN_VOLCFNET / REMPER."""
        config = {"measure": "volume"}
        estimator = GrowthEstimator(mock_db, config)

        data = pl.DataFrame({
            "ONEORTWO": [1],
            "COMPONENT": ["SURVIVOR"],
            "TREE_VOLCFNET": [500.0],
            "MIDPT_VOLCFNET": [400.0],
            "BEGIN_VOLCFNET": [300.0],
            "PTREE_VOLCFNET": [280.0],
            "TPA_UNADJ": [6.0],
            "REMPER": [5.0],
        }).lazy()

        result = estimator.calculate_values(data).collect()

        # ONEORTWO=1, SURVIVOR: beginning_volume = -(BEGIN_VOLCFNET / REMPER) = -(300/5) = -60
        # GROWTH_VALUE = TPA_UNADJ * volume_contribution = 6.0 * (-60.0) = -360.0
        assert abs(result["GROWTH_VALUE"][0] - (-360.0)) < 1e-10

    def test_volume_ingrowth_ending(self, mock_db):
        """ONEORTWO=2 with INGROWTH: adds TREE_VOLCFNET / REMPER."""
        config = {"measure": "volume"}
        estimator = GrowthEstimator(mock_db, config)

        data = pl.DataFrame({
            "ONEORTWO": [2],
            "COMPONENT": ["INGROWTH"],
            "TREE_VOLCFNET": [200.0],
            "MIDPT_VOLCFNET": [None],
            "BEGIN_VOLCFNET": [None],
            "PTREE_VOLCFNET": [None],
            "TPA_UNADJ": [4.0],
            "REMPER": [10.0],
        }).lazy()

        result = estimator.calculate_values(data).collect()

        # ONEORTWO=2, INGROWTH: ending_volume = 200/10 = 20
        # GROWTH_VALUE = 4.0 * 20.0 = 80.0
        assert abs(result["GROWTH_VALUE"][0] - 80.0) < 1e-10

    def test_volume_ingrowth_beginning_is_zero(self, mock_db):
        """ONEORTWO=1 with INGROWTH: no beginning volume (returns 0)."""
        config = {"measure": "volume"}
        estimator = GrowthEstimator(mock_db, config)

        data = pl.DataFrame({
            "ONEORTWO": [1],
            "COMPONENT": ["INGROWTH"],
            "TREE_VOLCFNET": [200.0],
            "MIDPT_VOLCFNET": [None],
            "BEGIN_VOLCFNET": [None],
            "PTREE_VOLCFNET": [None],
            "TPA_UNADJ": [4.0],
            "REMPER": [10.0],
        }).lazy()

        result = estimator.calculate_values(data).collect()

        # INGROWTH is not in beginning_volume matches, so 0
        assert result["GROWTH_VALUE"][0] == 0.0

    def test_volume_cut_uses_midpt(self, mock_db):
        """ONEORTWO=2 with CUT: uses MIDPT_VOLCFNET / REMPER for ending."""
        config = {"measure": "volume"}
        estimator = GrowthEstimator(mock_db, config)

        data = pl.DataFrame({
            "ONEORTWO": [2],
            "COMPONENT": ["CUT1"],
            "TREE_VOLCFNET": [500.0],
            "MIDPT_VOLCFNET": [350.0],
            "BEGIN_VOLCFNET": [300.0],
            "PTREE_VOLCFNET": [280.0],
            "TPA_UNADJ": [3.0],
            "REMPER": [5.0],
        }).lazy()

        result = estimator.calculate_values(data).collect()

        # ONEORTWO=2, CUT1: uses MIDPT = 350/5 = 70
        # GROWTH_VALUE = 3.0 * 70.0 = 210.0
        assert abs(result["GROWTH_VALUE"][0] - 210.0) < 1e-10

    def test_volume_cut1_beginning(self, mock_db):
        """ONEORTWO=1 with CUT1: subtracts BEGIN_VOLCFNET / REMPER."""
        config = {"measure": "volume"}
        estimator = GrowthEstimator(mock_db, config)

        data = pl.DataFrame({
            "ONEORTWO": [1],
            "COMPONENT": ["CUT1"],
            "TREE_VOLCFNET": [500.0],
            "MIDPT_VOLCFNET": [350.0],
            "BEGIN_VOLCFNET": [300.0],
            "PTREE_VOLCFNET": [280.0],
            "TPA_UNADJ": [3.0],
            "REMPER": [5.0],
        }).lazy()

        result = estimator.calculate_values(data).collect()

        # ONEORTWO=1, CUT1: beginning = -(300/5) = -60
        # GROWTH_VALUE = 3.0 * (-60.0) = -180.0
        assert abs(result["GROWTH_VALUE"][0] - (-180.0)) < 1e-10

    def test_volume_mortality_ending_is_zero(self, mock_db):
        """ONEORTWO=2 with MORTALITY: 0 ending (trees died)."""
        config = {"measure": "volume"}
        estimator = GrowthEstimator(mock_db, config)

        data = pl.DataFrame({
            "ONEORTWO": [2],
            "COMPONENT": ["MORTALITY1"],
            "TREE_VOLCFNET": [500.0],
            "MIDPT_VOLCFNET": [350.0],
            "BEGIN_VOLCFNET": [300.0],
            "PTREE_VOLCFNET": [280.0],
            "TPA_UNADJ": [3.0],
            "REMPER": [5.0],
        }).lazy()

        result = estimator.calculate_values(data).collect()

        # MORTALITY doesn't match any ending condition -> 0
        assert result["GROWTH_VALUE"][0] == 0.0

    def test_volume_mortality1_beginning(self, mock_db):
        """ONEORTWO=1 with MORTALITY1: subtracts BEGIN or PTREE."""
        config = {"measure": "volume"}
        estimator = GrowthEstimator(mock_db, config)

        data = pl.DataFrame({
            "ONEORTWO": [1],
            "COMPONENT": ["MORTALITY1"],
            "TREE_VOLCFNET": [500.0],
            "MIDPT_VOLCFNET": [350.0],
            "BEGIN_VOLCFNET": [300.0],
            "PTREE_VOLCFNET": [280.0],
            "TPA_UNADJ": [3.0],
            "REMPER": [5.0],
        }).lazy()

        result = estimator.calculate_values(data).collect()

        # MORTALITY1: begin_col present = -(300/5) = -60
        # GROWTH_VALUE = 3.0 * (-60.0) = -180.0
        assert abs(result["GROWTH_VALUE"][0] - (-180.0)) < 1e-10

    def test_volume_begin_null_uses_ptree(self, mock_db):
        """When BEGIN_VOLCFNET is null, falls back to PTREE_VOLCFNET."""
        config = {"measure": "volume"}
        estimator = GrowthEstimator(mock_db, config)

        data = pl.DataFrame({
            "ONEORTWO": [1],
            "COMPONENT": ["SURVIVOR"],
            "TREE_VOLCFNET": [500.0],
            "MIDPT_VOLCFNET": [350.0],
            "BEGIN_VOLCFNET": [None],
            "PTREE_VOLCFNET": [280.0],
            "TPA_UNADJ": [3.0],
            "REMPER": [5.0],
        }).lazy()

        result = estimator.calculate_values(data).collect()

        # BEGIN is null -> fallback to PTREE: -(280/5) = -56
        # GROWTH_VALUE = 3.0 * (-56.0) = -168.0
        assert abs(result["GROWTH_VALUE"][0] - (-168.0)) < 1e-10

    def test_count_measure_oneortwo_logic(self, mock_db):
        """Count measure: ONEORTWO=2 adds TPA, ONEORTWO=1 subtracts."""
        config = {"measure": "count"}
        estimator = GrowthEstimator(mock_db, config)

        data = pl.DataFrame({
            "ONEORTWO": [2, 1],
            "COMPONENT": ["SURVIVOR", "SURVIVOR"],
            "TPA_UNADJ": [5.0, 5.0],
            "REMPER": [5.0, 5.0],
        }).lazy()

        result = estimator.calculate_values(data).collect()

        # ONEORTWO=2: +5.0, ONEORTWO=1: -5.0
        assert result["GROWTH_VALUE"][0] == 5.0
        assert result["GROWTH_VALUE"][1] == -5.0

    def test_biomass_conversion(self, mock_db):
        """Biomass measure should apply LBS_TO_SHORT_TONS conversion."""
        config = {"measure": "biomass"}
        estimator = GrowthEstimator(mock_db, config)

        data = pl.DataFrame({
            "ONEORTWO": [2],
            "COMPONENT": ["SURVIVOR"],
            "TREE_DRYBIO_AG": [2000.0],
            "MIDPT_DRYBIO_AG": [1500.0],
            "BEGIN_DRYBIO_AG": [1000.0],
            "PTREE_DRYBIO_AG": [900.0],
            "TPA_UNADJ": [1.0],
            "REMPER": [5.0],
        }).lazy()

        result = estimator.calculate_values(data).collect()

        # ONEORTWO=2, SURVIVOR: 2000/5 = 400
        # GROWTH_VALUE = 1.0 * 400 * LBS_TO_SHORT_TONS
        expected = 1.0 * (2000.0 / 5.0) * LBS_TO_SHORT_TONS
        assert abs(result["GROWTH_VALUE"][0] - expected) < 1e-10

    def test_null_remper_defaults_to_5(self, mock_db):
        """Null REMPER should default to 5.0 years."""
        config = {"measure": "volume"}
        estimator = GrowthEstimator(mock_db, config)

        data = pl.DataFrame({
            "ONEORTWO": [2],
            "COMPONENT": ["SURVIVOR"],
            "TREE_VOLCFNET": [500.0],
            "MIDPT_VOLCFNET": [400.0],
            "BEGIN_VOLCFNET": [300.0],
            "PTREE_VOLCFNET": [280.0],
            "TPA_UNADJ": [1.0],
            "REMPER": [None],
        }).lazy()

        result = estimator.calculate_values(data).collect()

        # REMPER defaults to 5.0: 500/5 = 100, GROWTH_VALUE = 1.0 * 100 = 100
        assert abs(result["GROWTH_VALUE"][0] - 100.0) < 1e-10

    def test_empty_dataframe(self, mock_db):
        config = {"measure": "volume"}
        estimator = GrowthEstimator(mock_db, config)

        data = pl.DataFrame({
            "ONEORTWO": pl.Series([], dtype=pl.Int64),
            "COMPONENT": pl.Series([], dtype=pl.Utf8),
            "TREE_VOLCFNET": pl.Series([], dtype=pl.Float64),
            "MIDPT_VOLCFNET": pl.Series([], dtype=pl.Float64),
            "BEGIN_VOLCFNET": pl.Series([], dtype=pl.Float64),
            "PTREE_VOLCFNET": pl.Series([], dtype=pl.Float64),
            "TPA_UNADJ": pl.Series([], dtype=pl.Float64),
            "REMPER": pl.Series([], dtype=pl.Float64),
        }).lazy()

        result = estimator.calculate_values(data).collect()
        assert len(result) == 0

    def test_net_growth_from_paired_rows(self, mock_db):
        """Net growth = ending - beginning when summing ONEORTWO=1 and ONEORTWO=2."""
        config = {"measure": "volume"}
        estimator = GrowthEstimator(mock_db, config)

        # Same tree, two ONEORTWO rows
        data = pl.DataFrame({
            "ONEORTWO": [2, 1],
            "COMPONENT": ["SURVIVOR", "SURVIVOR"],
            "TREE_VOLCFNET": [500.0, 500.0],
            "MIDPT_VOLCFNET": [None, None],
            "BEGIN_VOLCFNET": [400.0, 400.0],
            "PTREE_VOLCFNET": [380.0, 380.0],
            "TPA_UNADJ": [6.0, 6.0],
            "REMPER": [5.0, 5.0],
        }).lazy()

        result = estimator.calculate_values(data).collect()

        # ONEORTWO=2: 500/5 * 6 = 600
        # ONEORTWO=1: -(400/5) * 6 = -480
        # Net = 600 + (-480) = 120
        total = result["GROWTH_VALUE"].sum()
        assert abs(total - 120.0) < 1e-10


class TestConfigStorage:
    """Tests for configuration storage."""

    def test_config_stored_correctly(self):
        config = {
            "measure": "volume",
            "land_type": "forest",
            "tree_type": "gs",
            "grp_by": ["SPCD"],
            "by_size_class": True,
            "size_class_type": "market",
        }
        estimator = GrowthEstimator(MockDB(), config)

        assert estimator.config == config
        assert estimator.config["measure"] == "volume"
        assert estimator.config["by_size_class"] is True
        assert estimator.config["size_class_type"] == "market"
