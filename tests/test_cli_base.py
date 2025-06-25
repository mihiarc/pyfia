"""
Tests for pyFIA CLI base class.

This module tests the shared CLI functionality used by both
the direct CLI and AI CLI interfaces.
"""

import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch, mock_open

import pytest
import polars as pl

from pyfia.cli.base import BaseCLI


class BaseCLITestHelper(BaseCLI):
    """Test implementation of BaseCLI for testing purposes."""
    
    def __init__(self):
        # Use a temporary directory for test history
        self.temp_dir = tempfile.mkdtemp()
        history_file = Path(self.temp_dir) / ".test_history"
        super().__init__(history_filename=str(history_file))
        
        # Mock the database connection for testing
        self.test_connected = False
    
    def _connect_to_database(self, db_path_str: str) -> bool:
        """Test implementation of database connection."""
        if db_path_str == "test.db":
            self.test_connected = True
            return True
        return False
    
    def cleanup(self):
        """Clean up test files."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)


@pytest.fixture
def cli():
    """Create a test CLI instance."""
    test_cli = BaseCLITestHelper()
    yield test_cli
    test_cli.cleanup()


@pytest.fixture
def sample_dataframe():
    """Create a sample DataFrame for testing."""
    return pl.DataFrame({
        "SPECIES": ["Pine", "Oak", "Maple"],
        "COUNT": [100, 150, 75],
        "BIOMASS": [25.5, 30.2, 18.7],
        "VOLUME": [1200.0, 1500.0, 900.0]
    })


class TestBaseCLIInitialization:
    """Test CLI initialization and setup."""
    
    def test_initialization(self, cli):
        """Test basic CLI initialization."""
        assert cli.console is not None
        assert cli.last_result is None
        assert cli.config is not None
        assert cli.history_file.name == ".test_history"
    
    @patch('pyfia.cli.base.HAS_READLINE', False)
    def test_initialization_without_readline(self):
        """Test initialization when readline is not available."""
        # This should not raise any errors
        test_cli = BaseCLITestHelper()
        assert test_cli.console is not None
        test_cli.cleanup()


class TestHistoryManagement:
    """Test command history functionality."""
    
    @patch('pyfia.cli.base.HAS_READLINE', True)
    @patch('pyfia.cli.base.readline')
    def test_setup_history_with_existing_file(self, mock_readline, cli):
        """Test history setup when history file exists."""
        # Create a mock history file
        cli.history_file.touch()
        
        # Re-setup history
        cli._setup_history()
        
        mock_readline.read_history_file.assert_called_with(cli.history_file)
        mock_readline.set_history_length.assert_called_with(1000)
    
    @patch('pyfia.cli.base.HAS_READLINE', True)
    @patch('pyfia.cli.base.readline')
    def test_save_history(self, mock_readline, cli):
        """Test saving command history."""
        cli._save_history()
        mock_readline.write_history_file.assert_called_with(cli.history_file)
    
    @patch('pyfia.cli.base.HAS_READLINE', False)
    def test_history_operations_without_readline(self, cli):
        """Test history operations when readline is not available."""
        # These should not raise errors
        cli._setup_history()
        cli._save_history()


class TestDatabaseOperations:
    """Test database connection operations."""
    
    def test_validate_database_path_valid(self, cli):
        """Test database path validation with valid path."""
        # Create a temporary file
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp_path = Path(tmp.name)
        
        try:
            result = cli._validate_database_path(str(tmp_path))
            assert result == tmp_path
        finally:
            tmp_path.unlink()
    
    def test_validate_database_path_invalid(self, cli):
        """Test database path validation with invalid path."""
        result = cli._validate_database_path("/nonexistent/path.db")
        assert result is None
    
    def test_validate_database_path_empty(self, cli):
        """Test database path validation with empty path."""
        result = cli._validate_database_path("")
        assert result is None
    
    def test_auto_connect_database_with_default(self, cli):
        """Test auto-connection when default database is set."""
        cli.config.default_database = "test.db"
        result = cli._auto_connect_database()
        assert result is True
        assert cli.test_connected is True
    
    def test_auto_connect_database_without_default(self, cli):
        """Test auto-connection when no default database is set."""
        cli.config.default_database = None
        result = cli._auto_connect_database()
        assert result is False
    
    @patch('pyfia.cli.base.BaseCLI._validate_database_path')
    def test_connect_to_database_not_implemented(self, mock_validate, cli):
        """Test that base _connect_to_database raises NotImplementedError."""
        # Create a proper BaseCLI instance (not our test subclass)
        base_cli = BaseCLI()
        mock_validate.return_value = Path("test.db")
        
        with pytest.raises(NotImplementedError):
            base_cli._connect_to_database("test.db")


class TestProgressAndDisplay:
    """Test progress bars and display functionality."""
    
    def test_create_progress_bar(self, cli):
        """Test progress bar creation."""
        progress = cli._create_progress_bar("Testing...")
        assert progress is not None
        # Progress bar should be configured correctly
        assert progress.console == cli.console
    
    def test_show_connection_status_success(self, cli):
        """Test showing successful connection status."""
        test_path = Path("test.db")
        # This should not raise any errors
        cli._show_connection_status(test_path, success=True)
    
    def test_show_connection_status_failure(self, cli):
        """Test showing failed connection status."""
        test_path = Path("test.db")
        # This should not raise any errors
        cli._show_connection_status(test_path, success=False)


class TestDataFrameDisplay:
    """Test DataFrame display functionality."""
    
    def test_display_dataframe_with_data(self, cli, sample_dataframe):
        """Test displaying a DataFrame with data."""
        cli.last_result = sample_dataframe
        # This should not raise any errors
        cli._display_dataframe(sample_dataframe, "Test Results")
    
    def test_display_dataframe_empty(self, cli):
        """Test displaying an empty DataFrame."""
        empty_df = pl.DataFrame()
        # This should not raise any errors
        cli._display_dataframe(empty_df, "Empty Results")
    
    def test_display_dataframe_with_nulls(self, cli):
        """Test displaying DataFrame with null values."""
        df_with_nulls = pl.DataFrame({
            "A": [1, None, 3],
            "B": [1.5, 2.7, None],
            "C": ["text", None, "more"]
        })
        # This should not raise any errors
        cli._display_dataframe(df_with_nulls, "Nulls Test")
    
    def test_display_dataframe_large_numbers(self, cli):
        """Test displaying DataFrame with very small and large numbers."""
        df_numbers = pl.DataFrame({
            "SMALL": [0.001, 0.0001, 0.0],
            "LARGE": [1000000.5, 999999999.99, 0.5]
        })
        # This should not raise any errors
        cli._display_dataframe(df_numbers, "Numbers Test")
    
    def test_display_dataframe_truncation(self, cli):
        """Test DataFrame display with row truncation."""
        # Create a large DataFrame
        large_df = pl.DataFrame({
            "ID": range(100),
            "VALUE": [f"item_{i}" for i in range(100)]
        })
        cli.last_result = large_df
        # Test with small max_rows
        cli._display_dataframe(large_df, "Large Dataset", max_rows=5)


class TestStatisticsAndColumnInfo:
    """Test DataFrame statistics and column information."""
    
    def test_show_dataframe_stats_with_numeric(self, cli, sample_dataframe):
        """Test showing statistics for DataFrame with numeric columns."""
        cli.last_result = sample_dataframe
        # This should not raise any errors
        cli._show_dataframe_stats(sample_dataframe)
    
    def test_show_dataframe_stats_no_numeric(self, cli):
        """Test showing statistics for DataFrame with no numeric columns."""
        text_df = pl.DataFrame({
            "NAME": ["A", "B", "C"],
            "TYPE": ["X", "Y", "Z"]
        })
        cli.last_result = text_df
        # This should not raise any errors
        cli._show_dataframe_stats(text_df)
    
    def test_show_dataframe_columns(self, cli, sample_dataframe):
        """Test showing column information."""
        cli.last_result = sample_dataframe
        # This should not raise any errors
        cli._show_dataframe_columns(sample_dataframe)


class TestCommands:
    """Test CLI command functionality."""
    
    def test_do_quit(self, cli):
        """Test quit command."""
        result = cli.do_quit("")
        assert result is True
    
    def test_do_exit(self, cli):
        """Test exit command."""
        result = cli.do_exit("")
        assert result is True
    
    @patch('os.system')
    def test_do_clear(self, mock_system, cli):
        """Test clear command."""
        cli.do_clear("")
        mock_system.assert_called_once()
    
    def test_do_last_no_results(self, cli):
        """Test last command with no previous results."""
        cli.last_result = None
        # This should not raise any errors
        cli.do_last("")
    
    def test_do_last_with_results(self, cli, sample_dataframe):
        """Test last command with previous results."""
        cli.last_result = sample_dataframe
        # Test different last command options
        cli.do_last("")  # Default
        cli.do_last("stats")  # Statistics
        cli.do_last("columns")  # Column info
        cli.do_last("n=5")  # Custom row count
    
    def test_do_last_invalid_option(self, cli, sample_dataframe):
        """Test last command with invalid option."""
        cli.last_result = sample_dataframe
        # This should not raise any errors
        cli.do_last("invalid_option")


class TestExportFunctionality:
    """Test data export functionality."""
    
    def test_do_export_no_data(self, cli):
        """Test export command with no data."""
        cli.last_result = None
        # This should not raise any errors
        cli.do_export("test.csv")
    
    def test_do_export_no_filename(self, cli, sample_dataframe):
        """Test export command with no filename."""
        cli.last_result = sample_dataframe
        # This should not raise any errors
        cli.do_export("")
    
    def test_do_export_csv(self, cli, sample_dataframe):
        """Test CSV export."""
        cli.last_result = sample_dataframe
        
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp:
            tmp_path = Path(tmp.name)
        
        try:
            cli.do_export(str(tmp_path))
            # File should exist and contain data
            assert tmp_path.exists()
            assert tmp_path.stat().st_size > 0
        finally:
            tmp_path.unlink(missing_ok=True)
    
    def test_do_export_excel(self, cli, sample_dataframe):
        """Test Excel export."""
        cli.last_result = sample_dataframe
        
        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            tmp_path = Path(tmp.name)
        
        try:
            cli.do_export(str(tmp_path))
            # File should exist
            assert tmp_path.exists()
        finally:
            tmp_path.unlink(missing_ok=True)
    
    def test_do_export_json(self, cli, sample_dataframe):
        """Test JSON export."""
        cli.last_result = sample_dataframe
        
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tmp:
            tmp_path = Path(tmp.name)
        
        try:
            cli.do_export(str(tmp_path))
            # File should exist and contain data
            assert tmp_path.exists()
            assert tmp_path.stat().st_size > 0
        finally:
            tmp_path.unlink(missing_ok=True)
    
    def test_do_export_unsupported_format(self, cli, sample_dataframe):
        """Test export with unsupported format."""
        cli.last_result = sample_dataframe
        # This should not raise any errors
        cli.do_export("test.txt")


class TestCommandHandling:
    """Test command handling functionality."""
    
    def test_emptyline(self, cli):
        """Test empty line handling."""
        # Should return None (do nothing)
        result = cli.emptyline()
        assert result is None
    
    def test_default_unknown_command(self, cli):
        """Test handling of unknown commands."""
        # This should not raise any errors
        cli.default("unknown_command")


class TestIntegration:
    """Integration tests for CLI base functionality."""
    
    def test_full_workflow(self, cli, sample_dataframe):
        """Test a complete CLI workflow."""
        # Set up data
        cli.last_result = sample_dataframe
        
        # Test various operations
        cli.do_last("")  # Display results
        cli.do_last("stats")  # Show statistics
        cli.do_last("columns")  # Show column info
        
        # Test export
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp:
            tmp_path = Path(tmp.name)
        
        try:
            cli.do_export(str(tmp_path))
            assert tmp_path.exists()
        finally:
            tmp_path.unlink(missing_ok=True)
    
    def test_error_handling(self, cli):
        """Test error handling in various scenarios."""
        # Test with invalid operations that should be handled gracefully
        cli.do_last("n=invalid")  # Invalid number
        cli._show_dataframe_stats(pl.DataFrame())  # Empty DataFrame
        cli.do_export("/invalid/path/file.csv")  # Invalid export path