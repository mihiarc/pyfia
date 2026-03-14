"""Tests for pyFIA constants and defaults."""


from pyfia.constants.defaults import (
    Defaults,
    ErrorMessages,
    MathConstants,
    ValidationRanges,
)


class TestMathConstants:
    """Tests for mathematical constants."""

    def test_basal_area_factor(self):
        """Test basal area factor is correct."""
        # (pi/4) / 144 = 0.005454154...
        import math

        expected = (math.pi / 4) / 144
        assert abs(MathConstants.BASAL_AREA_FACTOR - expected) < 1e-6

    def test_lbs_to_tons(self):
        """Test pounds to tons conversion factor."""
        assert MathConstants.LBS_TO_TONS == 2000.0

    def test_default_lambda(self):
        """Test default temporal weighting parameter."""
        assert MathConstants.DEFAULT_LAMBDA == 0.5


class TestDefaults:
    """Tests for default values."""

    def test_adj_factor_default(self):
        """Test default adjustment factor."""
        assert Defaults.ADJ_FACTOR_DEFAULT == 1.0

    def test_expns_default(self):
        """Test default expansion factor."""
        assert Defaults.EXPNS_DEFAULT == 1.0

    def test_n_cores_default(self):
        """Test default number of cores."""
        assert Defaults.N_CORES_DEFAULT == 1

    def test_include_variance_default(self):
        """Test default variance inclusion flag."""
        assert Defaults.INCLUDE_VARIANCE is False

    def test_include_totals_default(self):
        """Test default totals inclusion flag."""
        assert Defaults.INCLUDE_TOTALS is False


class TestValidationRanges:
    """Tests for validation ranges."""

    def test_state_code_range(self):
        """Test state code range is valid."""
        assert ValidationRanges.MIN_STATE_CODE == 1
        assert ValidationRanges.MAX_STATE_CODE == 78  # Includes territories
        assert ValidationRanges.MIN_STATE_CODE < ValidationRanges.MAX_STATE_CODE

    def test_diameter_range(self):
        """Test diameter range is valid."""
        assert ValidationRanges.MIN_DIAMETER == 0.1
        assert ValidationRanges.MAX_DIAMETER == 999.9
        assert ValidationRanges.MIN_DIAMETER < ValidationRanges.MAX_DIAMETER

    def test_inventory_year_range(self):
        """Test inventory year range is valid."""
        assert ValidationRanges.MIN_INVENTORY_YEAR == 1999
        assert ValidationRanges.MAX_INVENTORY_YEAR == 2099
        assert ValidationRanges.MIN_INVENTORY_YEAR < ValidationRanges.MAX_INVENTORY_YEAR

    def test_plot_count_range(self):
        """Test plot count range is valid."""
        assert ValidationRanges.MIN_PLOTS == 1
        assert ValidationRanges.MAX_PLOTS == 1_000_000
        assert ValidationRanges.MIN_PLOTS < ValidationRanges.MAX_PLOTS



class TestErrorMessages:
    """Tests for standard error messages."""

    def test_no_evalid_message(self):
        """Test NO_EVALID error message."""
        assert "EVALID" in ErrorMessages.NO_EVALID
        assert "find_evalid()" in ErrorMessages.NO_EVALID

    def test_invalid_tree_type_message(self):
        """Test INVALID_TREE_TYPE error message."""
        assert "tree_type" in ErrorMessages.INVALID_TREE_TYPE
        assert "live" in ErrorMessages.INVALID_TREE_TYPE
        assert "dead" in ErrorMessages.INVALID_TREE_TYPE

    def test_invalid_land_type_message(self):
        """Test INVALID_LAND_TYPE error message."""
        assert "land_type" in ErrorMessages.INVALID_LAND_TYPE
        assert "forest" in ErrorMessages.INVALID_LAND_TYPE
        assert "timber" in ErrorMessages.INVALID_LAND_TYPE

    def test_invalid_method_message(self):
        """Test INVALID_METHOD error message."""
        assert "method" in ErrorMessages.INVALID_METHOD
        assert "TI" in ErrorMessages.INVALID_METHOD

    def test_no_data_message(self):
        """Test NO_DATA error message."""
        assert "No data" in ErrorMessages.NO_DATA

    def test_missing_table_message_format(self):
        """Test MISSING_TABLE message has format placeholder."""
        assert "{}" in ErrorMessages.MISSING_TABLE
        formatted = ErrorMessages.MISSING_TABLE.format("TREE")
        assert "TREE" in formatted

    def test_invalid_domain_message_format(self):
        """Test INVALID_DOMAIN message has format placeholder."""
        assert "{}" in ErrorMessages.INVALID_DOMAIN


class TestConstantsIntegration:
    """Integration tests verifying constants work together."""

    def test_defaults_are_reasonable(self):
        """Test that default values are reasonable for FIA analysis."""
        # Adjustment and expansion factors default to 1 (no adjustment)
        assert Defaults.ADJ_FACTOR_DEFAULT == 1.0
        assert Defaults.EXPNS_DEFAULT == 1.0

        # Single core by default (safe, always works)
        assert Defaults.N_CORES_DEFAULT >= 1

        # Variance and totals off by default (faster computation)
        assert Defaults.INCLUDE_VARIANCE is False
        assert Defaults.INCLUDE_TOTALS is False
