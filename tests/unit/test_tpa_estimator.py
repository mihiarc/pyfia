"""Unit tests for TPAEstimator class.

Tests the TPAEstimator methods in isolation using mock data.
No database connection required.
"""

import math

import polars as pl
import pytest

from pyfia.estimation.estimators.tpa import TPAEstimator


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
        """Test that required tables for TPA estimation are returned."""
        config = {"land_type": "forest"}
        estimator = TPAEstimator(MockDB(), config)
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
            {"land_type": "forest"},
            {"land_type": "timber"},
            {"grp_by": "SPCD"},
            {"by_species": True},
            {"by_size_class": True},
        ]

        for config in configs:
            estimator = TPAEstimator(MockDB(), config)
            tables = estimator.get_required_tables()
            assert "TREE" in tables
            assert "COND" in tables


class TestGetTreeColumns:
    """Tests for get_tree_columns method."""

    def test_base_tree_columns(self):
        """Test base tree columns for TPA estimation."""
        config = {}
        estimator = TPAEstimator(MockDB(), config)
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

    def test_with_grp_by_tree_column(self):
        """Test that grouping columns from TREE table are included."""
        config = {"grp_by": "SPCD"}
        estimator = TPAEstimator(MockDB(), config)
        cols = estimator.get_tree_columns()

        # SPCD is already in base columns, so no duplicate
        assert cols.count("SPCD") == 1

    def test_with_grp_by_height_column(self):
        """Test that HT column is added when grouping by height."""
        config = {"grp_by": "HT"}
        estimator = TPAEstimator(MockDB(), config)
        cols = estimator.get_tree_columns()

        assert "HT" in cols

    def test_with_multiple_grp_by_columns(self):
        """Test with multiple grouping columns."""
        config = {"grp_by": ["SPCD", "CCLCD", "TREECLCD"]}
        estimator = TPAEstimator(MockDB(), config)
        cols = estimator.get_tree_columns()

        assert "SPCD" in cols
        assert "CCLCD" in cols
        assert "TREECLCD" in cols


class TestGetCondColumns:
    """Tests for get_cond_columns method."""

    def test_standard_cond_columns(self):
        """Test standard condition columns for TPA estimation.

        Base columns are always included. Timber land columns (SITECLCD, RESERVCD)
        are only included when land_type='timber'. Grouping columns (OWNGRPCD, etc.)
        are only included when specified in grp_by.
        """
        config = {}
        estimator = TPAEstimator(MockDB(), config)
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
        config = {"land_type": "timber"}
        estimator = TPAEstimator(MockDB(), config)
        cols = estimator.get_cond_columns()

        assert "SITECLCD" in cols
        assert "RESERVCD" in cols

    def test_grp_by_cond_columns(self):
        """Test that grp_by adds condition grouping columns."""
        config = {"grp_by": ["OWNGRPCD", "FORTYPCD"]}
        estimator = TPAEstimator(MockDB(), config)
        cols = estimator.get_cond_columns()

        assert "OWNGRPCD" in cols
        assert "FORTYPCD" in cols


class TestCalculateValues:
    """Tests for calculate_values method."""

    @pytest.fixture
    def mock_db(self):
        """Create mock database for testing."""
        return MockDB()

    def test_tpa_calculation(self, mock_db):
        """Test TPA calculation is direct copy of TPA_UNADJ."""
        config = {}
        estimator = TPAEstimator(mock_db, config)

        data = pl.DataFrame(
            {
                "TPA_UNADJ": [6.018046, 24.072184, 12.036092],
                "DIA": [10.0, 15.0, 20.0],
            }
        ).lazy()

        result = estimator.calculate_values(data).collect()

        # TPA should equal TPA_UNADJ exactly
        expected_tpa = [6.018046, 24.072184, 12.036092]
        for i, expected in enumerate(expected_tpa):
            assert abs(result["TPA"][i] - expected) < 1e-6

    def test_baa_calculation(self, mock_db):
        """Test BAA calculation using pi * (DIA/24)^2 * TPA_UNADJ formula."""
        config = {}
        estimator = TPAEstimator(mock_db, config)

        # Test with known values
        # BAA = pi * (DIA/24)^2 * TPA_UNADJ
        data = pl.DataFrame(
            {
                "TPA_UNADJ": [1.0],
                "DIA": [12.0],  # 12 inches = 0.5 feet radius
            }
        ).lazy()

        result = estimator.calculate_values(data).collect()

        # BAA = pi * (12/24)^2 * 1 = pi * 0.25 = 0.7854
        expected_baa = math.pi * (12.0 / 24.0) ** 2 * 1.0
        assert abs(result["BAA"][0] - expected_baa) < 1e-6

    def test_baa_with_various_diameters(self, mock_db):
        """Test BAA calculation with various diameter values."""
        config = {}
        estimator = TPAEstimator(mock_db, config)

        data = pl.DataFrame(
            {
                "TPA_UNADJ": [1.0, 1.0, 1.0],
                "DIA": [6.0, 12.0, 24.0],
            }
        ).lazy()

        result = estimator.calculate_values(data).collect()

        # BAA calculations:
        # 6" tree: pi * (6/24)^2 = pi * 0.0625 = 0.196
        # 12" tree: pi * (12/24)^2 = pi * 0.25 = 0.785
        # 24" tree: pi * (24/24)^2 = pi * 1.0 = 3.14
        expected_baa = [
            math.pi * (6.0 / 24.0) ** 2,
            math.pi * (12.0 / 24.0) ** 2,
            math.pi * (24.0 / 24.0) ** 2,
        ]

        for i, expected in enumerate(expected_baa):
            assert abs(result["BAA"][i] - expected) < 1e-6

    def test_baa_scales_with_tpa(self, mock_db):
        """Test that BAA scales linearly with TPA."""
        config = {}
        estimator = TPAEstimator(mock_db, config)

        data = pl.DataFrame(
            {
                "TPA_UNADJ": [10.0, 20.0],
                "DIA": [12.0, 12.0],
            }
        ).lazy()

        result = estimator.calculate_values(data).collect()

        # BAA with 20 TPA should be exactly 2x BAA with 10 TPA
        assert abs(result["BAA"][1] - 2 * result["BAA"][0]) < 1e-6

    def test_null_tpa_handling(self, mock_db):
        """Test handling of null TPA values."""
        config = {}
        estimator = TPAEstimator(mock_db, config)

        data = pl.DataFrame(
            {
                "TPA_UNADJ": [10.0, None, 10.0],
                "DIA": [12.0, 12.0, 12.0],
            }
        ).lazy()

        result = estimator.calculate_values(data).collect()

        assert result["TPA"][0] == 10.0
        assert result["TPA"][1] is None
        assert result["BAA"][1] is None

    def test_null_dia_handling(self, mock_db):
        """Test handling of null DIA values."""
        config = {}
        estimator = TPAEstimator(mock_db, config)

        data = pl.DataFrame(
            {
                "TPA_UNADJ": [10.0, 10.0],
                "DIA": [12.0, None],
            }
        ).lazy()

        result = estimator.calculate_values(data).collect()

        # TPA is independent of DIA
        assert result["TPA"][0] == 10.0
        assert result["TPA"][1] == 10.0
        # BAA requires DIA, so null DIA = null BAA
        assert result["BAA"][1] is None

    def test_zero_tpa_handling(self, mock_db):
        """Test handling of zero TPA values."""
        config = {}
        estimator = TPAEstimator(mock_db, config)

        data = pl.DataFrame(
            {
                "TPA_UNADJ": [0.0, 10.0],
                "DIA": [12.0, 12.0],
            }
        ).lazy()

        result = estimator.calculate_values(data).collect()

        assert result["TPA"][0] == 0.0
        assert result["BAA"][0] == 0.0

    def test_zero_dia_handling(self, mock_db):
        """Test handling of zero DIA values."""
        config = {}
        estimator = TPAEstimator(mock_db, config)

        data = pl.DataFrame(
            {
                "TPA_UNADJ": [10.0],
                "DIA": [0.0],
            }
        ).lazy()

        result = estimator.calculate_values(data).collect()

        # Zero DIA means zero BAA
        assert result["BAA"][0] == 0.0

    def test_size_class_calculation(self, mock_db):
        """Test size class calculation when by_size_class is True."""
        config = {"by_size_class": True}
        estimator = TPAEstimator(mock_db, config)

        # Size classes are 2-inch classes: 0, 2, 4, 6, 8, ...
        data = pl.DataFrame(
            {
                "TPA_UNADJ": [10.0, 10.0, 10.0, 10.0, 10.0],
                "DIA": [1.0, 2.5, 5.0, 10.5, 20.0],
            }
        ).lazy()

        result = estimator.calculate_values(data).collect()

        # Size class = floor(DIA / 2) * 2
        # 1.0 -> 0, 2.5 -> 2, 5.0 -> 4, 10.5 -> 10, 20.0 -> 20
        assert result["SIZE_CLASS"][0] == 0
        assert result["SIZE_CLASS"][1] == 2
        assert result["SIZE_CLASS"][2] == 4
        assert result["SIZE_CLASS"][3] == 10
        assert result["SIZE_CLASS"][4] == 20

    def test_size_class_not_added_when_not_requested(self, mock_db):
        """Test that SIZE_CLASS is not added when by_size_class is False."""
        config = {"by_size_class": False}
        estimator = TPAEstimator(mock_db, config)

        data = pl.DataFrame(
            {
                "TPA_UNADJ": [10.0],
                "DIA": [12.0],
            }
        ).lazy()

        result = estimator.calculate_values(data).collect()

        assert "SIZE_CLASS" not in result.columns


class TestEdgeCases:
    """Test edge cases and error conditions."""

    def test_empty_dataframe(self):
        """Test handling of empty dataframe."""
        config = {}
        estimator = TPAEstimator(MockDB(), config)

        data = pl.DataFrame(
            {
                "TPA_UNADJ": [],
                "DIA": [],
            }
        ).lazy()

        result = estimator.calculate_values(data).collect()
        assert len(result) == 0

    def test_large_dia_values(self):
        """Test handling of very large diameter values."""
        config = {}
        estimator = TPAEstimator(MockDB(), config)

        data = pl.DataFrame(
            {
                "TPA_UNADJ": [1.0],
                "DIA": [100.0],  # 100 inch diameter tree
            }
        ).lazy()

        result = estimator.calculate_values(data).collect()

        # BAA = pi * (100/24)^2 = pi * 17.36 = 54.54
        expected_baa = math.pi * (100.0 / 24.0) ** 2
        assert abs(result["BAA"][0] - expected_baa) < 1e-6

    def test_small_dia_values(self):
        """Test handling of very small diameter values."""
        config = {}
        estimator = TPAEstimator(MockDB(), config)

        data = pl.DataFrame(
            {
                "TPA_UNADJ": [100.0],  # High TPA for small trees
                "DIA": [1.0],  # 1 inch seedling
            }
        ).lazy()

        result = estimator.calculate_values(data).collect()

        # BAA = pi * (1/24)^2 * 100
        expected_baa = math.pi * (1.0 / 24.0) ** 2 * 100.0
        assert abs(result["BAA"][0] - expected_baa) < 1e-6

    def test_fractional_dia_values(self):
        """Test handling of fractional diameter values."""
        config = {}
        estimator = TPAEstimator(MockDB(), config)

        data = pl.DataFrame(
            {
                "TPA_UNADJ": [10.0],
                "DIA": [10.5],  # Common FIA diameter
            }
        ).lazy()

        result = estimator.calculate_values(data).collect()

        expected_baa = math.pi * (10.5 / 24.0) ** 2 * 10.0
        assert abs(result["BAA"][0] - expected_baa) < 1e-6


class TestConfigStorage:
    """Tests for configuration storage."""

    def test_config_stored_correctly(self):
        """Test that config is stored correctly in estimator."""
        config = {
            "land_type": "timber",
            "tree_type": "live",
            "grp_by": ["SPCD", "FORTYPCD"],
            "by_species": True,
            "by_size_class": True,
        }
        estimator = TPAEstimator(MockDB(), config)

        assert estimator.config == config
        assert estimator.config["land_type"] == "timber"
        assert estimator.config["tree_type"] == "live"
        assert estimator.config["by_species"] is True


class TestBAFormulaDerivation:
    """Tests to verify the BAA formula derivation is correct.

    The formula: BAA = pi * (DIA/24)^2 * TPA_UNADJ

    Derivation:
    1. Basal area of one tree = pi * radius^2 (in square feet)
    2. DIA is diameter at breast height in inches
    3. Radius in inches = DIA/2
    4. Radius in feet = DIA/2 / 12 = DIA/24
    5. Basal area of one tree = pi * (DIA/24)^2 square feet
    6. Basal area per acre = basal area per tree * trees per acre
       = pi * (DIA/24)^2 * TPA_UNADJ
    """

    def test_12_inch_tree_basal_area(self):
        """Verify a 12-inch tree has ~0.785 sq ft basal area."""
        config = {}
        estimator = TPAEstimator(MockDB(), config)

        data = pl.DataFrame(
            {
                "TPA_UNADJ": [1.0],
                "DIA": [12.0],
            }
        ).lazy()

        result = estimator.calculate_values(data).collect()

        # 12 inch diameter = 6 inch radius = 0.5 ft radius
        # BA = pi * 0.5^2 = pi * 0.25 = 0.7854 sq ft
        expected = math.pi * 0.25
        assert abs(result["BAA"][0] - expected) < 1e-6
        assert abs(result["BAA"][0] - 0.7854) < 0.0001

    def test_24_inch_tree_basal_area(self):
        """Verify a 24-inch tree has ~3.14 sq ft basal area."""
        config = {}
        estimator = TPAEstimator(MockDB(), config)

        data = pl.DataFrame(
            {
                "TPA_UNADJ": [1.0],
                "DIA": [24.0],
            }
        ).lazy()

        result = estimator.calculate_values(data).collect()

        # 24 inch diameter = 12 inch radius = 1 ft radius
        # BA = pi * 1^2 = pi = 3.14159 sq ft
        expected = math.pi
        assert abs(result["BAA"][0] - expected) < 1e-6

    def test_6_inch_tree_basal_area(self):
        """Verify a 6-inch tree has ~0.196 sq ft basal area."""
        config = {}
        estimator = TPAEstimator(MockDB(), config)

        data = pl.DataFrame(
            {
                "TPA_UNADJ": [1.0],
                "DIA": [6.0],
            }
        ).lazy()

        result = estimator.calculate_values(data).collect()

        # 6 inch diameter = 3 inch radius = 0.25 ft radius
        # BA = pi * 0.25^2 = pi * 0.0625 = 0.1963 sq ft
        expected = math.pi * (0.25**2)
        assert abs(result["BAA"][0] - expected) < 1e-6


class TestSizeClassBoundaries:
    """Test size class boundary behavior."""

    def test_size_class_edge_cases(self):
        """Test size class calculation at boundary values."""
        config = {"by_size_class": True}
        estimator = TPAEstimator(MockDB(), config)

        # Test boundary cases
        data = pl.DataFrame(
            {
                "TPA_UNADJ": [1.0] * 6,
                "DIA": [1.9, 2.0, 2.1, 3.9, 4.0, 4.1],
            }
        ).lazy()

        result = estimator.calculate_values(data).collect()

        # 1.9 -> floor(1.9/2)*2 = 0
        # 2.0 -> floor(2.0/2)*2 = 2
        # 2.1 -> floor(2.1/2)*2 = 2
        # 3.9 -> floor(3.9/2)*2 = 2
        # 4.0 -> floor(4.0/2)*2 = 4
        # 4.1 -> floor(4.1/2)*2 = 4
        expected_classes = [0, 2, 2, 2, 4, 4]
        assert result["SIZE_CLASS"].to_list() == expected_classes
