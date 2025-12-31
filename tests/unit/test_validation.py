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
    sanitize_sql_path,
    validate_sql_identifier,
    quote_sql_identifier,
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

    def test_validate_domain_expression_blocks_union(self):
        """Test that UNION keyword is blocked (data exfiltration vector)."""
        with pytest.raises(ValueError, match="dangerous SQL keyword: UNION"):
            validate_domain_expression("DIA > 5 UNION SELECT * FROM passwords", "tree_domain")

    def test_validate_domain_expression_blocks_into(self):
        """Test that INTO keyword is blocked."""
        with pytest.raises(ValueError, match="dangerous SQL keyword: INTO"):
            validate_domain_expression("SELECT * INTO outfile", "tree_domain")

    def test_validate_domain_expression_blocks_grant_revoke(self):
        """Test that GRANT/REVOKE keywords are blocked."""
        with pytest.raises(ValueError, match="dangerous SQL keyword: GRANT"):
            validate_domain_expression("GRANT ALL ON users", "tree_domain")

        with pytest.raises(ValueError, match="dangerous SQL keyword: REVOKE"):
            validate_domain_expression("REVOKE SELECT ON table", "tree_domain")

    def test_validate_domain_expression_blocks_semicolons(self):
        """Test that semicolons are blocked (statement separator)."""
        with pytest.raises(ValueError, match="semicolon"):
            validate_domain_expression("DIA > 5; SELECT 1", "tree_domain")

    def test_validate_domain_expression_blocks_sql_comments(self):
        """Test that SQL comments are blocked."""
        with pytest.raises(ValueError, match="SQL comment"):
            validate_domain_expression("DIA > 5 -- comment", "tree_domain")

        with pytest.raises(ValueError, match="SQL block comment"):
            validate_domain_expression("DIA > 5 /* comment */", "tree_domain")

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


class TestSQLSecurityValidation:
    """Test SQL security validation functions to prevent injection attacks."""

    def test_sanitize_sql_path_valid(self):
        """Test valid paths pass through sanitization."""
        assert sanitize_sql_path("/data/counties.shp") == "/data/counties.shp"
        assert sanitize_sql_path("/tmp/test_file.geojson") == "/tmp/test_file.geojson"
        assert sanitize_sql_path("data/polygons.gpkg") == "data/polygons.gpkg"
        # Paths with spaces are valid
        assert sanitize_sql_path("/data/my folder/file.shp") == "/data/my folder/file.shp"

    def test_sanitize_sql_path_rejects_single_quotes(self):
        """Test that single quotes are rejected (SQL injection vector)."""
        with pytest.raises(ValueError, match="single quotes"):
            sanitize_sql_path("data'; DROP TABLE PLOT; --")

        with pytest.raises(ValueError, match="single quotes"):
            sanitize_sql_path("/path/with'quote.shp")

    def test_sanitize_sql_path_rejects_double_quotes(self):
        """Test that double quotes are rejected."""
        with pytest.raises(ValueError, match="double quotes"):
            sanitize_sql_path('/path/with"quote.shp')

    def test_sanitize_sql_path_rejects_semicolons(self):
        """Test that semicolons are rejected (SQL statement separator)."""
        with pytest.raises(ValueError, match="semicolons"):
            sanitize_sql_path("/path/file;drop.shp")

    def test_sanitize_sql_path_rejects_backslashes(self):
        """Test that backslashes are rejected (escape character)."""
        with pytest.raises(ValueError, match="backslashes"):
            sanitize_sql_path("data\\file.shp")

    def test_sanitize_sql_path_rejects_sql_comments(self):
        """Test that SQL comment sequences are rejected."""
        with pytest.raises(ValueError, match="SQL comment sequences"):
            sanitize_sql_path("/data/file--comment.shp")

        with pytest.raises(ValueError, match="SQL comment sequences"):
            sanitize_sql_path("/data/file/*comment*/.shp")

    def test_sanitize_sql_path_handles_pathlib(self):
        """Test that pathlib.Path objects are handled."""
        from pathlib import Path
        assert sanitize_sql_path(Path("/data/file.shp")) == "/data/file.shp"

    def test_validate_sql_identifier_valid(self):
        """Test valid SQL identifiers."""
        assert validate_sql_identifier("PLOT", "table name") == "PLOT"
        assert validate_sql_identifier("TREE", "table name") == "TREE"
        assert validate_sql_identifier("TREE_GRM_COMPONENT", "table name") == "TREE_GRM_COMPONENT"
        assert validate_sql_identifier("_private", "table name") == "_private"
        assert validate_sql_identifier("Table123", "table name") == "Table123"

    def test_validate_sql_identifier_rejects_empty(self):
        """Test that empty identifiers are rejected."""
        with pytest.raises(ValueError, match="Empty"):
            validate_sql_identifier("", "table name")

    def test_validate_sql_identifier_rejects_special_chars(self):
        """Test that special characters are rejected."""
        with pytest.raises(ValueError, match="Invalid table name"):
            validate_sql_identifier("table; DROP TABLE", "table name")

        with pytest.raises(ValueError, match="Invalid table name"):
            validate_sql_identifier("table--comment", "table name")

        with pytest.raises(ValueError, match="Invalid column name"):
            validate_sql_identifier("column'name", "column name")

        with pytest.raises(ValueError, match="Invalid identifier"):
            validate_sql_identifier("table.name", "identifier")

    def test_validate_sql_identifier_rejects_leading_numbers(self):
        """Test that identifiers starting with numbers are rejected."""
        with pytest.raises(ValueError, match="Invalid table name"):
            validate_sql_identifier("123table", "table name")

    def test_validate_sql_identifier_rejects_spaces(self):
        """Test that spaces in identifiers are rejected."""
        with pytest.raises(ValueError, match="Invalid table name"):
            validate_sql_identifier("table name", "table name")

    def test_quote_sql_identifier_valid(self):
        """Test that valid identifiers are properly quoted."""
        assert quote_sql_identifier("PLOT") == '"PLOT"'
        assert quote_sql_identifier("TREE_GRM_COMPONENT") == '"TREE_GRM_COMPONENT"'

    def test_quote_sql_identifier_rejects_invalid(self):
        """Test that invalid identifiers are rejected before quoting."""
        with pytest.raises(ValueError):
            quote_sql_identifier("table; DROP TABLE")

        with pytest.raises(ValueError):
            quote_sql_identifier("")

    def test_sql_injection_attack_patterns(self):
        """Test common SQL injection attack patterns are blocked."""
        attack_patterns = [
            "'; DROP TABLE users; --",
            "' OR '1'='1",
            "'; INSERT INTO admin VALUES ('hacker', 'password'); --",
            "' UNION SELECT * FROM passwords --",
            "'; EXEC xp_cmdshell('dir'); --",
        ]

        for pattern in attack_patterns:
            with pytest.raises(ValueError):
                sanitize_sql_path(f"/data/{pattern}/file.shp")

        identifier_attacks = [
            "PLOT; DROP TABLE TREE",
            "table--",
            "name'injection",
            "col/**/name",
        ]

        for attack in identifier_attacks:
            with pytest.raises(ValueError):
                validate_sql_identifier(attack, "identifier")