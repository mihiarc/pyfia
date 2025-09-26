"""Tests for input validation module."""

import pytest
from pyfia.validation import (
    validate_land_type,
    validate_tree_type,
    validate_vol_type,
    validate_biomass_component,
    validate_temporal_method,
    validate_domain_expression,
    validate_grp_by,
    validate_boolean,
    validate_mortality_measure,
)


class TestValidators:
    """Test input validation functions."""

    def test_validate_land_type_valid(self):
        """Test valid land type values."""
        assert validate_land_type("forest") == "forest"
        assert validate_land_type("timber") == "timber"
        assert validate_land_type("all") == "all"

    def test_validate_land_type_invalid(self):
        """Test invalid land type values."""
        with pytest.raises(ValueError, match="Invalid land_type 'invalid'"):
            validate_land_type("invalid")

        with pytest.raises(ValueError, match="Must be one of"):
            validate_land_type("FOREST")  # Case sensitive

    def test_validate_tree_type_valid(self):
        """Test valid tree type values."""
        assert validate_tree_type("live") == "live"
        assert validate_tree_type("dead") == "dead"
        assert validate_tree_type("gs") == "gs"
        assert validate_tree_type("all") == "all"

    def test_validate_tree_type_invalid(self):
        """Test invalid tree type values."""
        with pytest.raises(ValueError, match="Invalid tree_type"):
            validate_tree_type("growing_stock")

    def test_validate_vol_type_valid(self):
        """Test valid volume type values."""
        assert validate_vol_type("net") == "net"
        assert validate_vol_type("gross") == "gross"
        assert validate_vol_type("sound") == "sound"
        assert validate_vol_type("sawlog") == "sawlog"

    def test_validate_vol_type_invalid(self):
        """Test invalid volume type values."""
        with pytest.raises(ValueError, match="Invalid vol_type"):
            validate_vol_type("board_feet")

    def test_validate_biomass_component_valid(self):
        """Test valid biomass component values."""
        assert validate_biomass_component("total") == "total"
        assert validate_biomass_component("ag") == "ag"
        assert validate_biomass_component("bg") == "bg"
        assert validate_biomass_component("bole") == "bole"

    def test_validate_biomass_component_invalid(self):
        """Test invalid biomass component values."""
        with pytest.raises(ValueError, match="Invalid biomass component"):
            validate_biomass_component("invalid")

    def test_validate_mortality_measure_valid(self):
        """Test valid mortality measure values."""
        assert validate_mortality_measure("tpa") == "tpa"
        assert validate_mortality_measure("volume") == "volume"
        assert validate_mortality_measure("biomass") == "biomass"
        assert validate_mortality_measure("carbon") == "carbon"

    def test_validate_mortality_measure_invalid(self):
        """Test invalid mortality measure values."""
        with pytest.raises(ValueError, match="Invalid measure"):
            validate_mortality_measure("invalid")

    def test_validate_domain_expression_valid(self):
        """Test valid domain expressions."""
        assert validate_domain_expression(None, "tree_domain") is None
        assert validate_domain_expression("DIA > 10.0", "tree_domain") == "DIA > 10.0"
        assert validate_domain_expression("STATUSCD == 1", "tree_domain") == "STATUSCD == 1"

    def test_validate_domain_expression_invalid(self):
        """Test invalid domain expressions."""
        with pytest.raises(TypeError, match="must be a string"):
            validate_domain_expression(123, "tree_domain")

        with pytest.raises(ValueError, match="cannot be an empty string"):
            validate_domain_expression("", "tree_domain")

        with pytest.raises(ValueError, match="dangerous SQL keyword"):
            validate_domain_expression("DROP TABLE TREE", "tree_domain")

        with pytest.raises(ValueError, match="dangerous SQL keyword"):
            validate_domain_expression("DELETE FROM COND", "tree_domain")

        # Test that word boundaries work - these should be OK
        assert validate_domain_expression("UPDATED_DATE > 2020", "tree_domain") == "UPDATED_DATE > 2020"
        assert validate_domain_expression("CREATED_BY == 'user'", "tree_domain") == "CREATED_BY == 'user'"

        # But actual SQL keywords should still be caught
        with pytest.raises(ValueError, match="dangerous SQL keyword: UPDATE"):
            validate_domain_expression("UPDATE SET x=1", "tree_domain")

    def test_validate_grp_by_valid(self):
        """Test valid grp_by values."""
        assert validate_grp_by(None) is None
        assert validate_grp_by("STATECD") == "STATECD"
        assert validate_grp_by(["STATECD", "FORTYPCD"]) == ["STATECD", "FORTYPCD"]

    def test_validate_grp_by_invalid(self):
        """Test invalid grp_by values."""
        with pytest.raises(TypeError, match="must be a string or list"):
            validate_grp_by(123)

        with pytest.raises(TypeError, match="columns must be strings"):
            validate_grp_by([123, 456])

        with pytest.raises(ValueError, match="cannot be empty strings"):
            validate_grp_by("")

        with pytest.raises(ValueError, match="cannot be empty strings"):
            validate_grp_by(["STATECD", ""])

    def test_validate_boolean_valid(self):
        """Test valid boolean values."""
        assert validate_boolean(True, "totals") is True
        assert validate_boolean(False, "variance") is False

    def test_validate_boolean_invalid(self):
        """Test invalid boolean values."""
        with pytest.raises(TypeError, match="totals must be a boolean"):
            validate_boolean(1, "totals")

        with pytest.raises(TypeError, match="variance must be a boolean"):
            validate_boolean("true", "variance")

        with pytest.raises(TypeError, match="by_species must be a boolean"):
            validate_boolean(None, "by_species")

    def test_validate_temporal_method_valid(self):
        """Test valid temporal method values."""
        assert validate_temporal_method("TI") == "TI"
        assert validate_temporal_method("ANNUAL") == "ANNUAL"
        assert validate_temporal_method("SMA") == "SMA"
        assert validate_temporal_method("LMA") == "LMA"
        assert validate_temporal_method("EMA") == "EMA"

    def test_validate_temporal_method_invalid(self):
        """Test invalid temporal method values."""
        with pytest.raises(ValueError, match="Invalid temporal method"):
            validate_temporal_method("MONTHLY")

        with pytest.raises(ValueError, match="Invalid temporal method"):
            validate_temporal_method("ti")  # Case sensitive