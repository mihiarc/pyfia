"""Unit tests for MortalityEstimator class.

Tests the MortalityEstimator methods in isolation using mock data.
No database connection required.
"""

import polars as pl
import pytest

from pyfia.estimation.estimators.mortality import MortalityEstimator
from pyfia.estimation.constants import BASAL_AREA_FACTOR, LBS_TO_SHORT_TONS


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

    def test_component_type_is_mortality(self):
        config = {"measure": "volume"}
        estimator = MortalityEstimator(MockDB(), config)
        assert estimator.component_type == "mortality"

    def test_metric_prefix_is_mort(self):
        config = {"measure": "volume"}
        estimator = MortalityEstimator(MockDB(), config)
        assert estimator.metric_prefix == "MORT"


class TestGetComponentFilter:
    """Tests for get_component_filter method."""

    def test_filters_to_mortality_components(self):
        config = {"measure": "volume"}
        estimator = MortalityEstimator(MockDB(), config)
        filter_expr = estimator.get_component_filter()

        assert filter_expr is not None

        # Apply the filter to test data
        data = pl.DataFrame({
            "COMPONENT": [
                "MORTALITY1", "MORTALITY2", "SURVIVOR",
                "CUT1", "INGROWTH", "MORTALITY3"
            ]
        })
        result = data.filter(filter_expr)

        # Should only keep MORTALITY* rows
        assert len(result) == 3
        for val in result["COMPONENT"].to_list():
            assert val.startswith("MORTALITY")


class TestCalculateValues:
    """Tests for calculate_values method."""

    @pytest.fixture
    def mock_db(self):
        return MockDB()

    def test_volume_measure(self, mock_db):
        """Volume mortality = TPA_UNADJ * VOLCFNET."""
        config = {"measure": "volume"}
        estimator = MortalityEstimator(mock_db, config)

        data = pl.DataFrame({
            "TPA_UNADJ": [5.0, 10.0, 2.5],
            "VOLCFNET": [100.0, 200.0, 50.0],
        }).lazy()

        result = estimator.calculate_values(data).collect()

        assert result["MORT_VALUE"][0] == 500.0
        assert result["MORT_VALUE"][1] == 2000.0
        assert result["MORT_VALUE"][2] == 125.0
        # MORT_ANNUAL should equal MORT_VALUE
        assert result["MORT_ANNUAL"][0] == 500.0

    def test_sawlog_measure(self, mock_db):
        """Sawlog mortality = TPA_UNADJ * VOLCSNET."""
        config = {"measure": "sawlog"}
        estimator = MortalityEstimator(mock_db, config)

        data = pl.DataFrame({
            "TPA_UNADJ": [3.0],
            "VOLCSNET": [400.0],
        }).lazy()

        result = estimator.calculate_values(data).collect()
        assert result["MORT_VALUE"][0] == 1200.0

    def test_biomass_measure(self, mock_db):
        """Biomass mortality = TPA_UNADJ * (DRYBIO_BOLE + DRYBIO_BRANCH) * LBS_TO_SHORT_TONS."""
        config = {"measure": "biomass"}
        estimator = MortalityEstimator(mock_db, config)

        data = pl.DataFrame({
            "TPA_UNADJ": [4.0],
            "DRYBIO_BOLE": [1000.0],
            "DRYBIO_BRANCH": [500.0],
        }).lazy()

        result = estimator.calculate_values(data).collect()
        expected = 4.0 * (1000.0 + 500.0) * LBS_TO_SHORT_TONS
        assert abs(result["MORT_VALUE"][0] - expected) < 1e-10

    def test_basal_area_measure(self, mock_db):
        """Basal area mortality = TPA_UNADJ * DIA^2 * BASAL_AREA_FACTOR."""
        config = {"measure": "basal_area"}
        estimator = MortalityEstimator(mock_db, config)

        data = pl.DataFrame({
            "TPA_UNADJ": [6.0],
            "DIA": [12.0],
        }).lazy()

        result = estimator.calculate_values(data).collect()
        expected = 6.0 * (12.0**2 * BASAL_AREA_FACTOR)
        assert abs(result["MORT_VALUE"][0] - expected) < 1e-10

    def test_tpa_measure(self, mock_db):
        """TPA/count mortality = TPA_UNADJ directly."""
        config = {"measure": "tpa"}
        estimator = MortalityEstimator(mock_db, config)

        data = pl.DataFrame({
            "TPA_UNADJ": [7.5, 3.2],
        }).lazy()

        result = estimator.calculate_values(data).collect()
        assert result["MORT_VALUE"][0] == 7.5
        assert result["MORT_VALUE"][1] == 3.2

    def test_count_measure_same_as_tpa(self, mock_db):
        """Count measure should behave same as TPA."""
        config = {"measure": "count"}
        estimator = MortalityEstimator(mock_db, config)

        data = pl.DataFrame({
            "TPA_UNADJ": [5.0],
        }).lazy()

        result = estimator.calculate_values(data).collect()
        assert result["MORT_VALUE"][0] == 5.0

    def test_null_volume_handling(self, mock_db):
        """Null volumes should propagate as null in multiplication."""
        config = {"measure": "volume"}
        estimator = MortalityEstimator(mock_db, config)

        data = pl.DataFrame({
            "TPA_UNADJ": [5.0, 10.0],
            "VOLCFNET": [None, 200.0],
        }).lazy()

        result = estimator.calculate_values(data).collect()
        assert result["MORT_VALUE"][0] is None
        assert result["MORT_VALUE"][1] == 2000.0

    def test_zero_tpa_gives_zero(self, mock_db):
        config = {"measure": "volume"}
        estimator = MortalityEstimator(mock_db, config)

        data = pl.DataFrame({
            "TPA_UNADJ": [0.0],
            "VOLCFNET": [100.0],
        }).lazy()

        result = estimator.calculate_values(data).collect()
        assert result["MORT_VALUE"][0] == 0.0

    def test_empty_dataframe(self, mock_db):
        config = {"measure": "volume"}
        estimator = MortalityEstimator(mock_db, config)

        data = pl.DataFrame({
            "TPA_UNADJ": [],
            "VOLCFNET": [],
        }).lazy()

        result = estimator.calculate_values(data).collect()
        assert len(result) == 0

    def test_mort_annual_equals_mort_value(self, mock_db):
        """MORT_ANNUAL should be an alias for MORT_VALUE (already annualized)."""
        config = {"measure": "volume"}
        estimator = MortalityEstimator(mock_db, config)

        data = pl.DataFrame({
            "TPA_UNADJ": [5.0, 10.0],
            "VOLCFNET": [100.0, 200.0],
        }).lazy()

        result = estimator.calculate_values(data).collect()
        assert result["MORT_ANNUAL"].to_list() == result["MORT_VALUE"].to_list()

    def test_large_values(self, mock_db):
        config = {"measure": "volume"}
        estimator = MortalityEstimator(mock_db, config)

        data = pl.DataFrame({
            "TPA_UNADJ": [1e6],
            "VOLCFNET": [1e4],
        }).lazy()

        result = estimator.calculate_values(data).collect()
        assert result["MORT_VALUE"][0] == 1e10


class TestConfigStorage:
    """Tests for configuration storage."""

    def test_config_stored_correctly(self):
        config = {
            "measure": "biomass",
            "land_type": "timber",
            "tree_type": "gs",
            "grp_by": "SPCD",
            "as_rate": True,
        }
        estimator = MortalityEstimator(MockDB(), config)

        assert estimator.config == config
        assert estimator.config["measure"] == "biomass"
        assert estimator.config["as_rate"] is True


class TestMeasureValidation:
    """Tests for measure parameter validation in the public function."""

    def test_valid_measures_accepted(self):
        """All valid measure types should be accepted."""
        valid_measures = ["volume", "sawlog", "biomass", "tpa", "count", "basal_area"]
        for measure in valid_measures:
            config = {"measure": measure}
            estimator = MortalityEstimator(MockDB(), config)
            # Should not raise
            assert estimator.config["measure"] == measure
