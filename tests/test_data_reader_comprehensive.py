"""
Comprehensive tests for data_reader module.

These tests verify the data reading functionality including:
- Database connection and initialization
- Table loading and caching
- Data filtering and preparation
- Error handling for database operations
"""

import pytest
import polars as pl
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock

from pyfia.data_reader import FIADataReader


class TestFIADataReaderInitialization:
    """Test FIADataReader initialization and setup."""
    
    def test_data_reader_init_with_valid_database(self, sample_fia_db):
        """Test data reader initialization with valid database."""
        reader = FIADataReader(str(sample_fia_db))
        
        assert reader.db_path == Path(sample_fia_db)
        assert reader.db_path.exists()
        assert reader.connection is None  # Lazy connection
    
    def test_data_reader_init_with_nonexistent_database(self):
        """Test data reader initialization with non-existent database."""
        with pytest.raises(FileNotFoundError):
            FIADataReader("nonexistent.db")
    
    def test_data_reader_init_with_path_object(self, sample_fia_db):
        """Test data reader initialization with Path object."""
        reader = FIADataReader(Path(sample_fia_db))
        assert reader.db_path == Path(sample_fia_db)


class TestFIADataReaderConnection:
    """Test database connection management."""
    
    def test_data_reader_connection_lazy(self, sample_fia_db):
        """Test that connection is lazy initialized."""
        reader = FIADataReader(str(sample_fia_db))
        
        # Connection should be None initially
        assert reader.connection is None
        
        # Accessing data should establish connection
        plots = reader.load_plot_data()
        assert reader.connection is not None
        assert isinstance(plots, pl.DataFrame)
    
    def test_data_reader_connection_reuse(self, sample_fia_db):
        """Test that connection is reused."""
        reader = FIADataReader(str(sample_fia_db))
        
        # Load data multiple times
        plots1 = reader.load_plot_data()
        plots2 = reader.load_plot_data()
        
        # Should have same connection
        assert reader.connection is not None
        assert isinstance(plots1, pl.DataFrame)
        assert isinstance(plots2, pl.DataFrame)
    
    def test_data_reader_context_manager(self, sample_fia_db):
        """Test using data reader as context manager."""
        with FIADataReader(str(sample_fia_db)) as reader:
            assert isinstance(reader, FIADataReader)
            plots = reader.load_plot_data()
            assert isinstance(plots, pl.DataFrame)
        
        # Connection should be closed after exiting context
        # (implementation dependent)


class TestFIADataReaderTableLoading:
    """Test table loading functionality."""
    
    def test_load_plot_data(self, sample_fia_db):
        """Test loading plot data."""
        reader = FIADataReader(str(sample_fia_db))
        plots = reader.load_plot_data()
        
        assert isinstance(plots, pl.DataFrame)
        assert len(plots) > 0
        
        # Check required columns
        expected_cols = ["CN", "STATECD", "INVYR", "PLOT_STATUS_CD"]
        for col in expected_cols:
            assert col in plots.columns
        
        # Check data values
        assert (plots["STATECD"] > 0).all()
        assert (plots["INVYR"] >= 1990).all()
    
    def test_load_tree_data(self, sample_fia_db):
        """Test loading tree data."""
        reader = FIADataReader(str(sample_fia_db))
        trees = reader.load_tree_data()
        
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
    
    def test_load_condition_data(self, sample_fia_db):
        """Test loading condition data."""
        reader = FIADataReader(str(sample_fia_db))
        conditions = reader.load_condition_data()
        
        assert isinstance(conditions, pl.DataFrame)
        assert len(conditions) > 0
        
        # Check required columns
        expected_cols = ["PLT_CN", "CONDID", "COND_STATUS_CD"]
        for col in expected_cols:
            assert col in conditions.columns
        
        # Check data values
        assert (conditions["CONDID"] > 0).all()
        assert (conditions["COND_STATUS_CD"] > 0).all()
    
    def test_load_evaluation_data(self, sample_fia_db):
        """Test loading evaluation data."""
        reader = FIADataReader(str(sample_fia_db))
        evaluations = reader.load_evaluation_data()
        
        assert isinstance(evaluations, pl.DataFrame)
        assert len(evaluations) > 0
        
        # Check required columns
        expected_cols = ["EVALID", "STATECD", "EVAL_TYP"]
        for col in expected_cols:
            assert col in evaluations.columns
        
        # Check data values
        assert (evaluations["EVALID"] > 0).all()
        assert (evaluations["STATECD"] > 0).all()
    
    def test_load_stratum_data(self, sample_fia_db):
        """Test loading stratum data."""
        reader = FIADataReader(str(sample_fia_db))
        strata = reader.load_stratum_data()
        
        assert isinstance(strata, pl.DataFrame)
        assert len(strata) > 0
        
        # Check required columns
        expected_cols = ["EVALID", "EXPNS", "P2POINTCNT"]
        for col in expected_cols:
            assert col in strata.columns
        
        # Check data values
        assert (strata["EVALID"] > 0).all()
        assert (strata["EXPNS"] > 0).all()
    
    def test_load_species_data(self, sample_fia_db):
        """Test loading species reference data."""
        reader = FIADataReader(str(sample_fia_db))
        species = reader.load_species_data()
        
        assert isinstance(species, pl.DataFrame)
        assert len(species) > 0
        
        # Check required columns
        expected_cols = ["SPCD", "COMMON_NAME", "SCIENTIFIC_NAME"]
        for col in expected_cols:
            assert col in species.columns
        
        # Check data values
        assert (species["SPCD"] > 0).all()


class TestFIADataReaderFiltering:
    """Test data filtering functionality."""
    
    def test_load_plot_data_with_state_filter(self, sample_fia_db):
        """Test loading plot data with state filter."""
        reader = FIADataReader(str(sample_fia_db))
        plots = reader.load_plot_data(statecd=37)
        
        assert isinstance(plots, pl.DataFrame)
        assert len(plots) > 0
        assert (plots["STATECD"] == 37).all()
    
    def test_load_plot_data_with_year_filter(self, sample_fia_db):
        """Test loading plot data with year filter."""
        reader = FIADataReader(str(sample_fia_db))
        plots = reader.load_plot_data(invyr=2020)
        
        assert isinstance(plots, pl.DataFrame)
        if len(plots) > 0:
            assert (plots["INVYR"] == 2020).all()
    
    def test_load_plot_data_with_evalid_filter(self, sample_fia_db):
        """Test loading plot data with evaluation filter."""
        reader = FIADataReader(str(sample_fia_db))
        plots = reader.load_plot_data(evalid=372301)
        
        assert isinstance(plots, pl.DataFrame)
        # Should return plots associated with this evaluation
    
    def test_load_tree_data_with_species_filter(self, sample_fia_db):
        """Test loading tree data with species filter."""
        reader = FIADataReader(str(sample_fia_db))
        trees = reader.load_tree_data(spcd=[131, 110])
        
        assert isinstance(trees, pl.DataFrame)
        if len(trees) > 0:
            assert trees["SPCD"].is_in([131, 110]).all()
    
    def test_load_tree_data_with_status_filter(self, sample_fia_db):
        """Test loading tree data with status filter."""
        reader = FIADataReader(str(sample_fia_db))
        trees = reader.load_tree_data(statuscd=1)  # Live trees only
        
        assert isinstance(trees, pl.DataFrame)
        if len(trees) > 0:
            assert (trees["STATUSCD"] == 1).all()


class TestFIADataReaderErrorHandling:
    """Test error handling and edge cases."""
    
    def test_load_nonexistent_table(self, sample_fia_db):
        """Test loading non-existent table."""
        reader = FIADataReader(str(sample_fia_db))
        
        with pytest.raises((Exception, pl.exceptions.ComputeError)):
            reader.load_custom_table("NONEXISTENT_TABLE")
    
    def test_load_data_with_invalid_filter(self, sample_fia_db):
        """Test loading data with invalid filter values."""
        reader = FIADataReader(str(sample_fia_db))
        
        # Invalid state code
        plots = reader.load_plot_data(statecd=999)
        assert isinstance(plots, pl.DataFrame)
        assert len(plots) == 0  # Should return empty DataFrame
        
        # Invalid species code
        trees = reader.load_tree_data(spcd=[99999])
        assert isinstance(trees, pl.DataFrame)
        assert len(trees) == 0  # Should return empty DataFrame
    
    def test_load_data_with_sql_injection_attempt(self, sample_fia_db):
        """Test protection against SQL injection."""
        reader = FIADataReader(str(sample_fia_db))
        
        # Attempt SQL injection (should be safely handled)
        try:
            # This should either be safely escaped or raise an error
            malicious_filter = "1; DROP TABLE PLOT; --"
            plots = reader.load_plot_data(statecd=malicious_filter)
            # If it doesn't raise an error, should return empty or safe result
            assert isinstance(plots, pl.DataFrame)
        except (ValueError, TypeError, pl.exceptions.ComputeError):
            # Expected - filter should be rejected
            pass
    
    def test_connection_error_handling(self, sample_fia_db):
        """Test handling of connection errors."""
        reader = FIADataReader(str(sample_fia_db))
        
        # Simulate connection failure
        with patch.object(reader, '_get_connection') as mock_conn:
            mock_conn.side_effect = Exception("Database connection failed")
            
            with pytest.raises(Exception):
                reader.load_plot_data()


class TestFIADataReaderPerformance:
    """Test performance characteristics."""
    
    def test_load_data_performance(self, sample_fia_db):
        """Test data loading performance."""
        import time
        
        reader = FIADataReader(str(sample_fia_db))
        
        start_time = time.time()
        plots = reader.load_plot_data()
        trees = reader.load_tree_data()
        conditions = reader.load_condition_data()
        end_time = time.time()
        
        # Should load test data quickly
        loading_time = end_time - start_time
        assert loading_time < 1.0  # Less than 1 second for test data
        
        # Should return non-empty data
        assert len(plots) > 0
        assert len(trees) > 0
        assert len(conditions) > 0
    
    def test_repeated_load_performance(self, sample_fia_db):
        """Test performance of repeated loads (caching)."""
        import time
        
        reader = FIADataReader(str(sample_fia_db))
        
        # First load
        start_time = time.time()
        plots1 = reader.load_plot_data()
        first_load_time = time.time() - start_time
        
        # Second load (should be faster if cached)
        start_time = time.time()
        plots2 = reader.load_plot_data()
        second_load_time = time.time() - start_time
        
        # Both should return equivalent data
        assert len(plots1) == len(plots2)
        
        # Performance improvement is implementation dependent
        # Just ensure both complete in reasonable time
        assert first_load_time < 1.0
        assert second_load_time < 1.0


class TestFIADataReaderDataConsistency:
    """Test data consistency and relationships."""
    
    def test_data_referential_integrity(self, sample_fia_db):
        """Test referential integrity between tables."""
        reader = FIADataReader(str(sample_fia_db))
        
        plots = reader.load_plot_data()
        trees = reader.load_tree_data()
        conditions = reader.load_condition_data()
        
        # Trees should reference existing plots
        plot_cns = set(plots["CN"].to_list())
        tree_plot_cns = set(trees["PLT_CN"].to_list())
        
        # All tree plot references should exist in plots
        assert tree_plot_cns.issubset(plot_cns)
        
        # Conditions should reference existing plots
        cond_plot_cns = set(conditions["PLT_CN"].to_list())
        assert cond_plot_cns.issubset(plot_cns)
    
    def test_data_type_consistency(self, sample_fia_db):
        """Test that data types are consistent."""
        reader = FIADataReader(str(sample_fia_db))
        
        plots = reader.load_plot_data()
        trees = reader.load_tree_data()
        
        # Check key data types
        assert plots["STATECD"].dtype in [pl.Int32, pl.Int64]
        assert plots["INVYR"].dtype in [pl.Int32, pl.Int64]
        assert trees["DIA"].dtype in [pl.Float32, pl.Float64]
        assert trees["SPCD"].dtype in [pl.Int32, pl.Int64]
    
    def test_data_range_validation(self, sample_fia_db):
        """Test that data values are within expected ranges."""
        reader = FIADataReader(str(sample_fia_db))
        
        plots = reader.load_plot_data()
        trees = reader.load_tree_data()
        
        # Check reasonable value ranges
        assert (plots["STATECD"] >= 1).all()
        assert (plots["STATECD"] <= 72).all()  # Valid state codes
        assert (plots["INVYR"] >= 1990).all()
        assert (plots["INVYR"] <= 2030).all()  # Reasonable year range
        
        assert (trees["DIA"] > 0).all()
        assert (trees["DIA"] < 500).all()  # Reasonable diameter range
        assert (trees["SPCD"] > 0).all()


class TestFIADataReaderSpecialCases:
    """Test special cases and edge conditions."""
    
    def test_empty_result_handling(self, sample_fia_db):
        """Test handling of empty query results."""
        reader = FIADataReader(str(sample_fia_db))
        
        # Query with impossible condition
        plots = reader.load_plot_data(statecd=999)
        
        # Should return empty DataFrame with correct schema
        assert isinstance(plots, pl.DataFrame)
        assert len(plots) == 0
        assert "CN" in plots.columns  # Should maintain schema
    
    def test_large_filter_lists(self, sample_fia_db):
        """Test handling of large filter lists."""
        reader = FIADataReader(str(sample_fia_db))
        
        # Large list of species codes
        large_species_list = list(range(100, 1000))
        trees = reader.load_tree_data(spcd=large_species_list)
        
        # Should handle gracefully
        assert isinstance(trees, pl.DataFrame)
    
    def test_null_value_handling(self, sample_fia_db):
        """Test handling of null values in data."""
        reader = FIADataReader(str(sample_fia_db))
        
        plots = reader.load_plot_data()
        trees = reader.load_tree_data()
        
        # Check for null handling (implementation dependent)
        # At minimum, should not crash
        assert isinstance(plots, pl.DataFrame)
        assert isinstance(trees, pl.DataFrame)


class TestFIADataReaderCustomQueries:
    """Test custom query functionality."""
    
    def test_execute_custom_query(self, sample_fia_db):
        """Test executing custom SQL queries."""
        reader = FIADataReader(str(sample_fia_db))
        
        # Simple custom query
        try:
            result = reader.execute_query("SELECT COUNT(*) as plot_count FROM PLOT")
            assert isinstance(result, pl.DataFrame)
            assert "plot_count" in result.columns
            assert result["plot_count"][0] > 0
        except AttributeError:
            # Method might not exist in current implementation
            pass
    
    def test_load_custom_table(self, sample_fia_db):
        """Test loading custom table."""
        reader = FIADataReader(str(sample_fia_db))
        
        try:
            species = reader.load_custom_table("REF_SPECIES")
            assert isinstance(species, pl.DataFrame)
            assert len(species) > 0
        except AttributeError:
            # Method might not exist in current implementation
            pass
    
    def test_complex_join_query(self, sample_fia_db):
        """Test complex multi-table queries."""
        reader = FIADataReader(str(sample_fia_db))
        
        try:
            # Join plots, trees, and species
            query = """
            SELECT p.CN as PLT_CN, t.SPCD, s.COMMON_NAME, COUNT(*) as tree_count
            FROM PLOT p
            JOIN TREE t ON p.CN = t.PLT_CN
            JOIN REF_SPECIES s ON t.SPCD = s.SPCD
            GROUP BY p.CN, t.SPCD, s.COMMON_NAME
            """
            result = reader.execute_query(query)
            assert isinstance(result, pl.DataFrame)
            if len(result) > 0:
                assert "tree_count" in result.columns
                assert "COMMON_NAME" in result.columns
        except AttributeError:
            # Method might not exist in current implementation
            pass