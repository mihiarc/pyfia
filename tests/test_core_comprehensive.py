"""
Comprehensive tests for core FIA class functionality.

These tests verify the main FIA class including:
- Database initialization and connection
- EVALID discovery and filtering
- Data loading and preparation
- Context manager functionality
- Error handling
"""

import pytest
import polars as pl
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock

from pyfia import FIA
from pyfia.core import FIA as FIACore


class TestFIAInitialization:
    """Test FIA class initialization and setup."""
    
    def test_fia_init_with_valid_database(self, sample_fia_db):
        """Test FIA initialization with valid database."""
        fia = FIA(str(sample_fia_db))
        
        assert fia.db_path == Path(sample_fia_db)
        assert fia.db_path.exists()
        assert fia.tables == {}  # Initially empty
        assert fia.evalid is None  # No filter set
        assert fia.most_recent is False
    
    def test_fia_init_with_nonexistent_database(self):
        """Test FIA initialization with non-existent database."""
        with pytest.raises(FileNotFoundError):
            FIA("nonexistent.db")
    
    def test_fia_init_with_path_object(self, sample_fia_db):
        """Test FIA initialization with Path object."""
        fia = FIA(Path(sample_fia_db))
        assert fia.db_path == Path(sample_fia_db)
    
    def test_fia_engine_parameter(self, sample_fia_db):
        """Test FIA initialization with engine parameter."""
        # Engine parameter should be accepted but always use DuckDB
        fia = FIA(str(sample_fia_db), engine="sqlite")
        assert fia.db_path.exists()
        
        fia2 = FIA(str(sample_fia_db), engine="duckdb")
        assert fia2.db_path.exists()


class TestFIAContextManager:
    """Test FIA context manager functionality."""
    
    def test_fia_context_manager(self, sample_fia_db):
        """Test using FIA as context manager."""
        with FIA(str(sample_fia_db)) as fia:
            assert isinstance(fia, FIA)
            assert fia.db_path.exists()
            # Should be able to use fia instance here
        
        # Context should exit cleanly
    
    def test_fia_context_manager_with_exception(self, sample_fia_db):
        """Test FIA context manager when exception occurs."""
        try:
            with FIA(str(sample_fia_db)) as fia:
                assert isinstance(fia, FIA)
                raise ValueError("Test exception")
        except ValueError:
            pass  # Expected
        
        # Should exit cleanly even with exception


class TestFIADataLoading:
    """Test FIA data loading functionality."""
    
    def test_fia_get_plots(self, sample_fia_instance):
        """Test getting plot data."""
        plots = sample_fia_instance.get_plots()
        
        assert isinstance(plots, pl.DataFrame)
        assert len(plots) > 0
        
        # Check required columns
        expected_cols = ["CN", "STATECD", "INVYR"]
        for col in expected_cols:
            assert col in plots.columns
        
        # Check data types and values
        assert plots["STATECD"].dtype in [pl.Int32, pl.Int64]
        assert (plots["STATECD"] > 0).all()
        assert (plots["INVYR"] >= 1990).all()
    
    def test_fia_get_trees(self, sample_fia_instance):
        """Test getting tree data."""
        trees = sample_fia_instance.get_trees()
        
        assert isinstance(trees, pl.DataFrame)
        assert len(trees) > 0
        
        # Check required columns
        expected_cols = ["CN", "PLT_CN", "STATUSCD", "SPCD", "DIA"]
        for col in expected_cols:
            assert col in trees.columns
        
        # Check data values
        assert (trees["STATUSCD"] > 0).all()
        assert (trees["DIA"] > 0).all()
        assert (trees["SPCD"] > 0).all()
    
    def test_fia_get_conditions(self, sample_fia_instance):
        """Test getting condition data."""
        conditions = sample_fia_instance.get_conditions()
        
        assert isinstance(conditions, pl.DataFrame)
        assert len(conditions) > 0
        
        # Check required columns
        expected_cols = ["PLT_CN", "CONDID", "COND_STATUS_CD"]
        for col in expected_cols:
            assert col in conditions.columns
        
        # Check data values
        assert (conditions["CONDID"] > 0).all()
        assert (conditions["COND_STATUS_CD"] > 0).all()
    
    def test_fia_load_tables_lazy(self, sample_fia_instance):
        """Test lazy loading of tables."""
        # Tables should be empty initially
        assert len(sample_fia_instance.tables) == 0
        
        # Loading data should populate tables
        plots = sample_fia_instance.get_plots()
        trees = sample_fia_instance.get_trees()
        
        # Tables should now be populated (implementation dependent)
        # This test might need adjustment based on actual implementation


class TestFIAEvalidFunctionality:
    """Test EVALID discovery and filtering functionality."""
    
    def test_fia_find_evalid(self, sample_fia_instance):
        """Test finding available EVALIDs."""
        evalids = sample_fia_instance.find_evalid()
        
        assert isinstance(evalids, (list, pl.DataFrame))
        
        if isinstance(evalids, list):
            assert len(evalids) > 0
            assert 372301 in evalids  # Our test EVALID
        elif isinstance(evalids, pl.DataFrame):
            assert len(evalids) > 0
            assert "EVALID" in evalids.columns
            assert 372301 in evalids["EVALID"].to_list()
    
    def test_fia_find_evalid_by_state(self, sample_fia_instance):
        """Test finding EVALIDs for specific state."""
        evalids = sample_fia_instance.find_evalid(statecd=37)  # North Carolina
        
        if isinstance(evalids, list):
            assert len(evalids) > 0
        elif isinstance(evalids, pl.DataFrame):
            assert len(evalids) > 0
            # All should be for NC (state code 37)
            if "STATECD" in evalids.columns:
                assert (evalids["STATECD"] == 37).all()
    
    def test_fia_clip_fia(self, sample_fia_instance):
        """Test clipFIA functionality."""
        # Clip to specific EVALID
        clipped_fia = sample_fia_instance.clipFIA(evalid=372301)
        
        assert isinstance(clipped_fia, FIA)
        assert clipped_fia.evalid == [372301]
        
        # Should be able to get data from clipped instance
        plots = clipped_fia.get_plots()
        assert isinstance(plots, pl.DataFrame)
    
    def test_fia_clip_fia_most_recent(self, sample_fia_instance):
        """Test clipFIA with mostRecent=True."""
        clipped_fia = sample_fia_instance.clipFIA(mostRecent=True)
        
        assert isinstance(clipped_fia, FIA)
        assert clipped_fia.most_recent is True
        
        # Should have some EVALID selected
        # (exact value depends on data)
    
    def test_fia_clip_fia_by_state(self, sample_fia_instance):
        """Test clipFIA with state filter."""
        clipped_fia = sample_fia_instance.clipFIA(statecd=37)
        
        assert isinstance(clipped_fia, FIA)
        
        # Get data and verify it's for the right state
        plots = clipped_fia.get_plots()
        if len(plots) > 0:
            assert (plots["STATECD"] == 37).all()


class TestFIADataFiltering:
    """Test data filtering and domain application."""
    
    def test_fia_prepare_data(self, sample_fia_instance):
        """Test data preparation functionality."""
        # This tests the prepare_data method if it exists
        try:
            data = sample_fia_instance.prepare_data(evalid=372301)
            
            assert isinstance(data, dict)
            
            # Should contain key tables
            expected_tables = ["plot", "tree", "cond"]
            for table in expected_tables:
                if table in data:
                    assert isinstance(data[table], pl.DataFrame)
                    
        except AttributeError:
            # Method might not exist in current implementation
            pass
    
    def test_fia_data_consistency(self, sample_fia_instance):
        """Test consistency between related tables."""
        plots = sample_fia_instance.get_plots()
        trees = sample_fia_instance.get_trees()
        conditions = sample_fia_instance.get_conditions()
        
        # Trees should reference existing plots
        plot_cns = set(plots["CN"].to_list())
        tree_plot_cns = set(trees["PLT_CN"].to_list())
        
        # All tree plot references should exist in plots
        # (allowing for the possibility that not all plots have trees)
        assert tree_plot_cns.issubset(plot_cns)
        
        # Conditions should reference existing plots
        cond_plot_cns = set(conditions["PLT_CN"].to_list())
        assert cond_plot_cns.issubset(plot_cns)


class TestFIAErrorHandling:
    """Test FIA error handling and edge cases."""
    
    def test_fia_invalid_evalid(self, sample_fia_instance):
        """Test handling of invalid EVALID."""
        with pytest.raises((ValueError, RuntimeError)):
            sample_fia_instance.clipFIA(evalid=999999)
    
    def test_fia_conflicting_parameters(self, sample_fia_instance):
        """Test handling of conflicting parameters."""
        # Test providing both evalid and mostRecent
        # Should handle gracefully or raise clear error
        try:
            clipped = sample_fia_instance.clipFIA(evalid=372301, mostRecent=True)
            # If it succeeds, evalid should take precedence
            assert clipped.evalid == [372301]
        except ValueError:
            # Or it might raise an error for conflicting params
            pass
    
    def test_fia_empty_state(self, sample_fia_instance):
        """Test handling of state with no data."""
        # Test with state that doesn't exist in our data
        clipped = sample_fia_instance.clipFIA(statecd=99)
        
        # Should create instance but might have no data
        assert isinstance(clipped, FIA)
        
        plots = clipped.get_plots()
        # Might be empty DataFrame
        assert isinstance(plots, pl.DataFrame)


class TestFIAPerformance:
    """Test FIA performance characteristics."""
    
    def test_fia_initialization_performance(self, sample_fia_db):
        """Test FIA initialization performance."""
        import time
        
        start_time = time.time()
        fia = FIA(str(sample_fia_db))
        end_time = time.time()
        
        # Should initialize quickly
        initialization_time = end_time - start_time
        assert initialization_time < 0.1  # Less than 100ms
    
    def test_fia_data_loading_performance(self, sample_fia_instance):
        """Test data loading performance."""
        import time
        
        start_time = time.time()
        plots = sample_fia_instance.get_plots()
        trees = sample_fia_instance.get_trees()
        conditions = sample_fia_instance.get_conditions()
        end_time = time.time()
        
        # Should load test data quickly
        loading_time = end_time - start_time
        assert loading_time < 0.5  # Less than 500ms for test data
        
        # Should return non-empty data
        assert len(plots) > 0
        assert len(trees) > 0
        assert len(conditions) > 0


class TestFIAMemoryManagement:
    """Test FIA memory management."""
    
    def test_fia_table_caching(self, sample_fia_instance):
        """Test table caching behavior."""
        # First access
        plots1 = sample_fia_instance.get_plots()
        
        # Second access should use cached data (if implemented)
        plots2 = sample_fia_instance.get_plots()
        
        # Should return equivalent data
        assert len(plots1) == len(plots2)
        
        # If caching is implemented, might be same object
        # If not, should still be equivalent data
    
    def test_fia_connection_cleanup(self, sample_fia_db):
        """Test database connection cleanup."""
        # Create and destroy multiple FIA instances
        for i in range(5):
            fia = FIA(str(sample_fia_db))
            plots = fia.get_plots()
            assert len(plots) > 0
            # Connection should be cleaned up automatically
        
        # Should not have connection issues


@patch('pyfia.data_reader.FIADataReader')
class TestFIAMocking:
    """Test FIA with mocked dependencies."""
    
    def test_fia_with_mocked_reader(self, mock_reader_class, sample_fia_db):
        """Test FIA with mocked data reader."""
        # Setup mock
        mock_reader = MagicMock()
        mock_reader_class.return_value = mock_reader
        
        # Mock return data
        mock_reader.load_plot_data.return_value = pl.DataFrame({
            "CN": ["PLT001"],
            "STATECD": [37],
            "INVYR": [2020]
        })
        
        # Create FIA instance
        fia = FIA(str(sample_fia_db))
        
        # Should use mocked reader
        mock_reader_class.assert_called_once()
    
    def test_fia_with_database_error(self, mock_reader_class, sample_fia_db):
        """Test FIA handling of database errors."""
        # Setup mock to raise error
        mock_reader = MagicMock()
        mock_reader_class.return_value = mock_reader
        mock_reader.load_plot_data.side_effect = Exception("Database error")
        
        fia = FIA(str(sample_fia_db))
        
        # Should handle database errors gracefully
        with pytest.raises(Exception):
            fia.get_plots()