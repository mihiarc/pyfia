"""Unit tests for BiomassEstimator class.

Tests the BiomassEstimator methods in isolation using mock data.
No database connection required.
"""

import polars as pl
import pytest

from pyfia.estimation.estimators.biomass import BiomassEstimator


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
        """Test that required tables for biomass estimation are returned."""
        config = {"component": "AG", "land_type": "forest"}
        estimator = BiomassEstimator(MockDB(), config)
        tables = estimator.get_required_tables()

        assert "TREE" in tables
        assert "COND" in tables
        assert "PLOT" in tables
        assert "POP_PLOT_STRATUM_ASSGN" in tables
        assert "POP_STRATUM" in tables
        assert len(tables) == 5

    def test_tables_are_consistent_across_components(self):
        """Test that table requirements do not change with different components."""
        components = ["AG", "BG", "TOTAL"]

        for component in components:
            config = {"component": component}
            estimator = BiomassEstimator(MockDB(), config)
            tables = estimator.get_required_tables()
            assert "TREE" in tables
            assert "COND" in tables


class TestGetTreeColumns:
    """Tests for get_tree_columns method."""

    def test_aboveground_biomass_columns(self):
        """Test tree columns for aboveground biomass estimation."""
        config = {"component": "AG"}
        estimator = BiomassEstimator(MockDB(), config)
        cols = estimator.get_tree_columns()

        # Core columns
        assert "CN" in cols
        assert "PLT_CN" in cols
        assert "CONDID" in cols
        assert "STATUSCD" in cols
        assert "SPCD" in cols
        assert "DIA" in cols
        assert "TPA_UNADJ" in cols
        # Biomass columns
        assert "DRYBIO_AG" in cols
        assert "DRYBIO_BG" in cols

    def test_belowground_biomass_columns(self):
        """Test tree columns for belowground biomass estimation."""
        config = {"component": "BG"}
        estimator = BiomassEstimator(MockDB(), config)
        cols = estimator.get_tree_columns()

        assert "DRYBIO_AG" in cols
        assert "DRYBIO_BG" in cols

    def test_total_biomass_columns(self):
        """Test tree columns for total biomass estimation."""
        config = {"component": "TOTAL"}
        estimator = BiomassEstimator(MockDB(), config)
        cols = estimator.get_tree_columns()

        assert "DRYBIO_AG" in cols
        assert "DRYBIO_BG" in cols

    def test_specific_component_column(self):
        """Test tree columns for specific biomass component."""
        config = {"component": "BOLE"}
        estimator = BiomassEstimator(MockDB(), config)
        cols = estimator.get_tree_columns()

        # Specific component column should be added
        assert "DRYBIO_BOLE" in cols

    def test_with_grp_by_tree_column(self):
        """Test that grouping columns from TREE table are included."""
        config = {"component": "AG", "grp_by": "SPCD"}
        estimator = BiomassEstimator(MockDB(), config)
        cols = estimator.get_tree_columns()

        # SPCD is already in base columns, so no duplicate
        assert cols.count("SPCD") == 1

    def test_with_grp_by_height_column(self):
        """Test that HT column is added when grouping by height."""
        config = {"component": "AG", "grp_by": "HT"}
        estimator = BiomassEstimator(MockDB(), config)
        cols = estimator.get_tree_columns()

        assert "HT" in cols

    def test_with_multiple_grp_by_columns(self):
        """Test with multiple grouping columns."""
        config = {"component": "AG", "grp_by": ["SPCD", "CCLCD"]}
        estimator = BiomassEstimator(MockDB(), config)
        cols = estimator.get_tree_columns()

        assert "SPCD" in cols
        assert "CCLCD" in cols


class TestGetCondColumns:
    """Tests for get_cond_columns method."""

    def test_standard_cond_columns(self):
        """Test standard condition columns for biomass estimation.

        Base columns are always included. Timber land columns (SITECLCD, RESERVCD)
        are only included when land_type='timber'. Grouping columns (OWNGRPCD, etc.)
        are only included when specified in grp_by.
        """
        config = {"component": "AG"}
        estimator = BiomassEstimator(MockDB(), config)
        cols = estimator.get_cond_columns()

        # Base columns always included
        assert "PLT_CN" in cols
        assert "CONDID" in cols
        assert "COND_STATUS_CD" in cols
        assert "CONDPROP_UNADJ" in cols

        # Extra columns NOT included by default (only when needed)
        assert "OWNGRPCD" not in cols  # Only when grp_by includes it
        assert "FORTYPCD" not in cols  # Only when grp_by includes it

    def test_timber_land_type_columns(self):
        """Test that timber land type includes SITECLCD and RESERVCD."""
        config = {"component": "AG", "land_type": "timber"}
        estimator = BiomassEstimator(MockDB(), config)
        cols = estimator.get_cond_columns()

        assert "SITECLCD" in cols
        assert "RESERVCD" in cols

    def test_grp_by_cond_columns(self):
        """Test that grp_by adds condition grouping columns."""
        config = {"component": "AG", "grp_by": ["OWNGRPCD", "FORTYPCD"]}
        estimator = BiomassEstimator(MockDB(), config)
        cols = estimator.get_cond_columns()

        assert "OWNGRPCD" in cols
        assert "FORTYPCD" in cols


class TestCalculateValues:
    """Tests for calculate_values method."""

    @pytest.fixture
    def mock_db(self):
        """Create mock database for testing."""
        return MockDB()

    def test_aboveground_biomass_calculation(self, mock_db):
        """Test aboveground biomass per acre calculation."""
        config = {"component": "AG"}
        estimator = BiomassEstimator(mock_db, config)

        # Create test data with known values
        # DRYBIO_AG is in pounds, result should be in tons (/ 2000)
        data = pl.DataFrame(
            {
                "DRYBIO_AG": [2000.0, 4000.0, 3000.0],  # pounds per tree
                "DRYBIO_BG": [500.0, 1000.0, 750.0],  # not used for AG
                "TPA_UNADJ": [10.0, 10.0, 10.0],
            }
        ).lazy()

        result = estimator.calculate_values(data).collect()

        # Biomass per acre (tons) = (DRYBIO_AG * TPA_UNADJ) / 2000
        # = (2000 * 10) / 2000 = 10 tons/acre
        expected_biomass = [10.0, 20.0, 15.0]
        assert result["BIOMASS_ACRE"].to_list() == expected_biomass

        # Carbon = biomass * 0.47
        expected_carbon = [4.7, 9.4, 7.05]
        for i, expected in enumerate(expected_carbon):
            assert abs(result["CARBON_ACRE"][i] - expected) < 0.001

    def test_belowground_biomass_calculation(self, mock_db):
        """Test belowground biomass per acre calculation."""
        config = {"component": "BG"}
        estimator = BiomassEstimator(mock_db, config)

        data = pl.DataFrame(
            {
                "DRYBIO_AG": [2000.0],  # not used for BG
                "DRYBIO_BG": [1000.0],  # pounds per tree
                "TPA_UNADJ": [10.0],
            }
        ).lazy()

        result = estimator.calculate_values(data).collect()

        # Biomass per acre (tons) = (DRYBIO_BG * TPA_UNADJ) / 2000
        # = (1000 * 10) / 2000 = 5 tons/acre
        assert result["BIOMASS_ACRE"][0] == 5.0

    def test_total_biomass_calculation(self, mock_db):
        """Test total biomass (AG + BG) per acre calculation."""
        config = {"component": "TOTAL"}
        estimator = BiomassEstimator(mock_db, config)

        data = pl.DataFrame(
            {
                "DRYBIO_AG": [2000.0],
                "DRYBIO_BG": [500.0],
                "TPA_UNADJ": [10.0],
            }
        ).lazy()

        result = estimator.calculate_values(data).collect()

        # Total biomass = (AG + BG) * TPA / 2000
        # = (2000 + 500) * 10 / 2000 = 12.5 tons/acre
        assert result["BIOMASS_ACRE"][0] == 12.5

    def test_carbon_is_47_percent_of_biomass(self, mock_db):
        """Test that carbon is calculated as 47% of biomass."""
        config = {"component": "AG"}
        estimator = BiomassEstimator(mock_db, config)

        data = pl.DataFrame(
            {
                "DRYBIO_AG": [20000.0],  # 100 tons/acre when * 10 TPA / 2000
                "DRYBIO_BG": [0.0],
                "TPA_UNADJ": [10.0],
            }
        ).lazy()

        result = estimator.calculate_values(data).collect()

        biomass_acre = result["BIOMASS_ACRE"][0]
        carbon_acre = result["CARBON_ACRE"][0]

        assert biomass_acre == 100.0
        assert carbon_acre == 47.0  # 100 * 0.47

    def test_null_drybio_handling(self, mock_db):
        """Test handling of null DRYBIO values."""
        config = {"component": "AG"}
        estimator = BiomassEstimator(mock_db, config)

        data = pl.DataFrame(
            {
                "DRYBIO_AG": [2000.0, None, 3000.0],
                "DRYBIO_BG": [500.0, 500.0, 500.0],
                "TPA_UNADJ": [10.0, 10.0, 10.0],
            }
        ).lazy()

        result = estimator.calculate_values(data).collect()

        assert result["BIOMASS_ACRE"][0] == 10.0
        assert result["BIOMASS_ACRE"][1] is None
        assert result["BIOMASS_ACRE"][2] == 15.0

    def test_null_tpa_handling(self, mock_db):
        """Test handling of null TPA values."""
        config = {"component": "AG"}
        estimator = BiomassEstimator(mock_db, config)

        data = pl.DataFrame(
            {
                "DRYBIO_AG": [2000.0, 2000.0],
                "DRYBIO_BG": [500.0, 500.0],
                "TPA_UNADJ": [10.0, None],
            }
        ).lazy()

        result = estimator.calculate_values(data).collect()

        assert result["BIOMASS_ACRE"][0] == 10.0
        assert result["BIOMASS_ACRE"][1] is None

    def test_zero_biomass_handling(self, mock_db):
        """Test handling of zero biomass values."""
        config = {"component": "AG"}
        estimator = BiomassEstimator(mock_db, config)

        data = pl.DataFrame(
            {
                "DRYBIO_AG": [0.0, 2000.0],
                "DRYBIO_BG": [0.0, 500.0],
                "TPA_UNADJ": [10.0, 0.0],
            }
        ).lazy()

        result = estimator.calculate_values(data).collect()

        assert result["BIOMASS_ACRE"][0] == 0.0
        assert result["BIOMASS_ACRE"][1] == 0.0

    def test_default_component(self, mock_db):
        """Test that default component is 'AG'."""
        config = {}  # No component specified
        estimator = BiomassEstimator(mock_db, config)

        data = pl.DataFrame(
            {
                "DRYBIO_AG": [2000.0],
                "DRYBIO_BG": [500.0],
                "TPA_UNADJ": [10.0],
            }
        ).lazy()

        result = estimator.calculate_values(data).collect()
        # Should use AG, not total
        assert result["BIOMASS_ACRE"][0] == 10.0


class TestEdgeCases:
    """Test edge cases and error conditions."""

    def test_empty_dataframe(self):
        """Test handling of empty dataframe."""
        config = {"component": "AG"}
        estimator = BiomassEstimator(MockDB(), config)

        data = pl.DataFrame(
            {
                "DRYBIO_AG": [],
                "DRYBIO_BG": [],
                "TPA_UNADJ": [],
            }
        ).lazy()

        result = estimator.calculate_values(data).collect()
        assert len(result) == 0

    def test_large_values(self):
        """Test handling of very large biomass values."""
        config = {"component": "AG"}
        estimator = BiomassEstimator(MockDB(), config)

        data = pl.DataFrame(
            {
                "DRYBIO_AG": [1e10],  # 10 billion pounds
                "DRYBIO_BG": [1e9],
                "TPA_UNADJ": [100.0],
            }
        ).lazy()

        result = estimator.calculate_values(data).collect()
        # (1e10 * 100) / 2000 = 5e8 tons/acre
        assert result["BIOMASS_ACRE"][0] == 5e8

    def test_small_values(self):
        """Test handling of very small biomass values."""
        config = {"component": "AG"}
        estimator = BiomassEstimator(MockDB(), config)

        data = pl.DataFrame(
            {
                "DRYBIO_AG": [0.01],  # 0.01 pounds
                "DRYBIO_BG": [0.001],
                "TPA_UNADJ": [0.1],
            }
        ).lazy()

        result = estimator.calculate_values(data).collect()
        # (0.01 * 0.1) / 2000 = 5e-7
        assert abs(result["BIOMASS_ACRE"][0] - 5e-7) < 1e-10

    def test_total_with_null_component(self):
        """Test total biomass when one component is null."""
        config = {"component": "TOTAL"}
        estimator = BiomassEstimator(MockDB(), config)

        data = pl.DataFrame(
            {
                "DRYBIO_AG": [2000.0],
                "DRYBIO_BG": [None],  # Null belowground
                "TPA_UNADJ": [10.0],
            }
        ).lazy()

        result = estimator.calculate_values(data).collect()
        # AG + null = null
        assert result["BIOMASS_ACRE"][0] is None


class TestConfigStorage:
    """Tests for configuration storage."""

    def test_config_stored_correctly(self):
        """Test that config is stored correctly in estimator."""
        config = {
            "component": "AG",
            "land_type": "timber",
            "grp_by": ["SPCD", "FORTYPCD"],
            "tree_type": "live",
        }
        estimator = BiomassEstimator(MockDB(), config)

        assert estimator.config == config
        assert estimator.config["component"] == "AG"
        assert estimator.config["land_type"] == "timber"


class TestConversionFactors:
    """Tests for unit conversions and conversion factors."""

    def test_pounds_to_tons_conversion(self):
        """Verify pounds to tons conversion factor (/ 2000)."""
        config = {"component": "AG"}
        estimator = BiomassEstimator(MockDB(), config)

        # 2000 pounds per tree * 1 TPA = 2000 pounds/acre = 1 ton/acre
        data = pl.DataFrame(
            {
                "DRYBIO_AG": [2000.0],
                "DRYBIO_BG": [0.0],
                "TPA_UNADJ": [1.0],
            }
        ).lazy()

        result = estimator.calculate_values(data).collect()
        assert result["BIOMASS_ACRE"][0] == 1.0

    def test_carbon_fraction_is_0_47(self):
        """Verify carbon fraction is exactly 0.47 (IPCC standard)."""
        config = {"component": "AG"}
        estimator = BiomassEstimator(MockDB(), config)

        # 1 ton biomass * 0.47 = 0.47 tons carbon
        data = pl.DataFrame(
            {
                "DRYBIO_AG": [2000.0],  # 1 ton/acre
                "DRYBIO_BG": [0.0],
                "TPA_UNADJ": [1.0],
            }
        ).lazy()

        result = estimator.calculate_values(data).collect()
        assert result["BIOMASS_ACRE"][0] == 1.0
        assert result["CARBON_ACRE"][0] == 0.47
