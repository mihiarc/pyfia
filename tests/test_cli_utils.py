"""
Tests for pyFIA CLI utilities module.

This module tests the shared utilities used by CLI interfaces,
including state parsing, validation helpers, and formatting functions.
"""

import pytest
from pathlib import Path
from unittest.mock import MagicMock

import polars as pl
from rich.console import Console

from pyfia.cli.utils import (
    STATE_ABBR_TO_CODE,
    STATE_NAME_TO_CODE,
    STATE_CODE_TO_NAME,
    STATE_CODE_TO_ABBR,
    parse_state_identifier,
    get_state_name,
    get_state_abbreviation,
    validate_evalid,
    format_file_size,
    create_database_info_panel,
    create_help_table,
    parse_area_arguments,
    parse_biomass_arguments,
    parse_volume_arguments,
    parse_tpa_arguments,
    format_estimation_results_help,
)


class TestStateMappings:
    """Test state code mapping dictionaries."""
    
    def test_state_abbr_to_code_consistency(self):
        """Test that state abbreviation mappings are consistent."""
        # Test some known states
        assert STATE_ABBR_TO_CODE["NC"] == 37
        assert STATE_ABBR_TO_CODE["CA"] == 6
        assert STATE_ABBR_TO_CODE["TX"] == 48
        
        # Test total count
        assert len(STATE_ABBR_TO_CODE) == 50  # All US states
    
    def test_state_name_to_code_consistency(self):
        """Test that state name mappings are consistent."""
        assert STATE_NAME_TO_CODE["north carolina"] == 37
        assert STATE_NAME_TO_CODE["california"] == 6
        assert STATE_NAME_TO_CODE["texas"] == 48
    
    def test_reverse_mappings(self):
        """Test that reverse mappings are consistent."""
        # Test code to name
        assert STATE_CODE_TO_NAME[37] == "North Carolina"
        assert STATE_CODE_TO_NAME[6] == "California"
        
        # Test code to abbreviation
        assert STATE_CODE_TO_ABBR[37] == "NC"
        assert STATE_CODE_TO_ABBR[6] == "CA"


class TestStateIdentifierParsing:
    """Test state identifier parsing functions."""
    
    def test_parse_state_code(self):
        """Test parsing numeric state codes."""
        assert parse_state_identifier("37") == 37
        assert parse_state_identifier("6") == 6
        assert parse_state_identifier("48") == 48
    
    def test_parse_state_abbreviation(self):
        """Test parsing state abbreviations."""
        assert parse_state_identifier("NC") == 37
        assert parse_state_identifier("nc") == 37
        assert parse_state_identifier("CA") == 6
        assert parse_state_identifier("TX") == 48
    
    def test_parse_state_name(self):
        """Test parsing full state names."""
        assert parse_state_identifier("North Carolina") == 37
        assert parse_state_identifier("north carolina") == 37
        assert parse_state_identifier("California") == 6
        assert parse_state_identifier("texas") == 48
    
    def test_parse_invalid_identifier(self):
        """Test parsing invalid identifiers."""
        assert parse_state_identifier("") is None
        assert parse_state_identifier("XY") is None
        assert parse_state_identifier("Invalid State") is None
        assert parse_state_identifier("999") is None
    
    def test_parse_with_whitespace(self):
        """Test parsing with whitespace."""
        assert parse_state_identifier("  NC  ") == 37
        assert parse_state_identifier(" north carolina ") == 37


class TestStateHelperFunctions:
    """Test state helper functions."""
    
    def test_get_state_name(self):
        """Test getting state name from code."""
        assert get_state_name(37) == "North Carolina"
        assert get_state_name(6) == "California"
        assert get_state_name(999) is None
    
    def test_get_state_abbreviation(self):
        """Test getting state abbreviation from code."""
        assert get_state_abbreviation(37) == "NC"
        assert get_state_abbreviation(6) == "CA"
        assert get_state_abbreviation(999) is None


class TestValidationFunctions:
    """Test validation helper functions."""
    
    def test_validate_evalid_valid(self):
        """Test EVALID validation with valid codes."""
        assert validate_evalid("372301") is True
        assert validate_evalid("060123") is True
        assert validate_evalid("481504") is True
    
    def test_validate_evalid_invalid(self):
        """Test EVALID validation with invalid codes."""
        assert validate_evalid("") is False
        assert validate_evalid("12345") is False  # Too short
        assert validate_evalid("1234567") is False  # Too long
        assert validate_evalid("abcdef") is False  # Not numeric
        assert validate_evalid("37230a") is False  # Mixed characters


class TestFormatHelpers:
    """Test formatting helper functions."""
    
    def test_format_file_size(self):
        """Test file size formatting."""
        assert format_file_size(500) == "500.0 B"
        assert format_file_size(1536) == "1.5 KB"  # 1.5 * 1024
        assert format_file_size(2097152) == "2.0 MB"  # 2 * 1024^2
        assert format_file_size(3221225472) == "3.0 GB"  # 3 * 1024^3


class TestRichPanelCreation:
    """Test Rich panel creation functions."""
    
    def test_create_database_info_panel(self):
        """Test database info panel creation."""
        # Create a temporary file to test with
        test_file = Path("/tmp/test.db")
        test_file.touch()
        
        try:
            console = Console()
            panel = create_database_info_panel(test_file, console)
            
            assert panel.title == f"🗄️ {test_file.name}"
            assert "test.db" in str(panel.renderable)
            assert "DuckDB Database" in str(panel.renderable)
        finally:
            test_file.unlink()
    
    def test_create_help_table(self):
        """Test help table creation."""
        commands = {
            "connect": "Connect to database",
            "area": "Calculate forest area",
            "help": "Show help information"
        }
        
        table = create_help_table(commands, "Test Commands")
        
        assert table.title == "Test Commands"
        assert len(table.columns) == 2
        assert table.columns[0].header == "Command"
        assert table.columns[1].header == "Description"


class TestArgumentParsing:
    """Test command argument parsing functions."""
    
    def test_parse_area_arguments_defaults(self):
        """Test area argument parsing with defaults."""
        result = parse_area_arguments([])
        
        assert result["land_type"] == "forest"
        assert result["by_land_type"] is False
        assert result["totals"] is False
        assert result["tree_domain"] is None
        assert result["area_domain"] is None
    
    def test_parse_area_arguments_with_options(self):
        """Test area argument parsing with options."""
        args = ["timber", "by_land_type", "totals", "tree_domain", "DIA > 10"]
        result = parse_area_arguments(args)
        
        assert result["land_type"] == "timber"
        assert result["by_land_type"] is True
        assert result["totals"] is True
        assert result["tree_domain"] == "DIA > 10"
    
    def test_parse_biomass_arguments_defaults(self):
        """Test biomass argument parsing with defaults."""
        result = parse_biomass_arguments([])
        
        assert result["component"] == "AG"
        assert result["tree_type"] == "live"
        assert result["land_type"] == "forest"
        assert result["by_species"] is False
        assert result["by_size_class"] is False
        assert result["totals"] is False
    
    def test_parse_biomass_arguments_with_options(self):
        """Test biomass argument parsing with options."""
        args = ["BG", "dead", "timber", "by_species", "area_domain", "OWNGRPCD == 10"]
        result = parse_biomass_arguments(args)
        
        assert result["component"] == "BG"
        assert result["tree_type"] == "dead"
        assert result["land_type"] == "timber"
        assert result["by_species"] is True
        assert result["area_domain"] == "OWNGRPCD == 10"
    
    def test_parse_volume_arguments_defaults(self):
        """Test volume argument parsing with defaults."""
        result = parse_volume_arguments([])
        
        assert result["vol_type"] == "net"
        assert result["tree_type"] == "live"
        assert result["land_type"] == "forest"
        assert result["by_species"] is False
        assert result["by_size_class"] is False
        assert result["totals"] is False
    
    def test_parse_volume_arguments_with_options(self):
        """Test volume argument parsing with options."""
        args = ["gross", "gs", "by_size_class", "totals"]
        result = parse_volume_arguments(args)
        
        assert result["vol_type"] == "gross"
        assert result["tree_type"] == "gs"
        assert result["by_size_class"] is True
        assert result["totals"] is True
    
    def test_parse_tpa_arguments_defaults(self):
        """Test TPA argument parsing with defaults."""
        result = parse_tpa_arguments([])
        
        assert result["tree_type"] == "live"
        assert result["land_type"] == "forest"
        assert result["by_species"] is False
        assert result["by_size_class"] is False
        assert result["totals"] is False
    
    def test_parse_tpa_arguments_with_options(self):
        """Test TPA argument parsing with options."""
        args = ["all", "timber", "by_species", "tree_domain", "STATUSCD = 1"]
        result = parse_tpa_arguments(args)
        
        assert result["tree_type"] == "all"
        assert result["land_type"] == "timber"
        assert result["by_species"] is True
        assert result["tree_domain"] == "STATUSCD = 1"


class TestResultsHelp:
    """Test estimation results help formatting."""
    
    def test_format_area_results_help(self):
        """Test area results help formatting."""
        help_text = format_estimation_results_help("area")
        
        assert "AREA = Total acres" in help_text
        assert "AREA_PERC = Percentage of total area" in help_text
        assert "SE = Standard Error" in help_text
        assert "N_PLOTS = Number of plots used" in help_text
    
    def test_format_biomass_results_help(self):
        """Test biomass results help formatting."""
        help_text = format_estimation_results_help("biomass")
        
        assert "BIO_ACRE = Biomass per acre (tons/acre)" in help_text
        assert "SE = Standard Error" in help_text
        assert "SE_PERCENT = Standard Error as % of estimate" in help_text
        assert "N_PLOTS = Number of plots used" in help_text
    
    def test_format_volume_results_help(self):
        """Test volume results help formatting."""
        help_text = format_estimation_results_help("volume")
        
        assert "VOL_ACRE = Volume per acre (cubic feet/acre)" in help_text
        assert "SE = Standard Error" in help_text
        assert "SE_PERCENT = Standard Error as % of estimate" in help_text
        assert "N_PLOTS = Number of plots used" in help_text
    
    def test_format_tpa_results_help(self):
        """Test TPA results help formatting."""
        help_text = format_estimation_results_help("tpa")
        
        assert "TPA = Trees per acre" in help_text
        assert "BAA = Basal area per acre (sq ft/acre)" in help_text
        assert "SE = Standard Error" in help_text
        assert "N_PLOTS = Number of plots used" in help_text
    
    def test_format_unknown_type_help(self):
        """Test help formatting for unknown result type."""
        help_text = format_estimation_results_help("unknown")
        
        # Should return empty list for unknown types
        assert help_text == []


@pytest.fixture
def sample_dataframe():
    """Create a sample DataFrame for testing."""
    return pl.DataFrame({
        "STATE": ["NC", "CA", "TX"],
        "PLOTS": [100, 150, 200],
        "AREA": [1000.5, 2000.7, 1500.3]
    })


class TestIntegration:
    """Integration tests for CLI utilities."""
    
    def test_state_parsing_workflow(self):
        """Test complete state parsing workflow."""
        # Test the full cycle: abbreviation -> code -> name -> abbreviation
        state_code = parse_state_identifier("NC")
        assert state_code == 37
        
        state_name = get_state_name(state_code)
        assert state_name == "North Carolina"
        
        state_abbr = get_state_abbreviation(state_code)
        assert state_abbr == "NC"
    
    def test_argument_parsing_consistency(self):
        """Test that argument parsing is consistent across functions."""
        # All parsing functions should handle similar argument patterns
        area_args = parse_area_arguments(["totals"])
        biomass_args = parse_biomass_arguments(["totals"])
        volume_args = parse_volume_arguments(["totals"])
        tpa_args = parse_tpa_arguments(["totals"])
        
        assert area_args["totals"] is True
        assert biomass_args["totals"] is True
        assert volume_args["totals"] is True
        assert tpa_args["totals"] is True