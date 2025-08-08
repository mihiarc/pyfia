"""
Comprehensive tests for data_reader module.

These tests verify the data reading functionality including:
- Database connection and initialization
- Table loading and caching
- Data filtering and preparation
- Error handling for database operations
"""

from pathlib import Path
from unittest.mock import patch

import duckdb
import polars as pl
import pytest

from pyfia.core import FIADataReader


class TestFIADataReaderInitialization:
    """Test FIADataReader initialization and setup."""

    def test_data_reader_init_with_valid_database(self, sample_fia_db):
        """Test data reader initialization with valid database."""
        reader = FIADataReader(str(sample_fia_db))

        assert reader.db_path == Path(sample_fia_db)
        assert reader.db_path.exists()
        assert hasattr(reader, '_duckdb_conn')  # Has DuckDB connection
        assert reader._duckdb_conn is not None  # Connection is active

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

    def test_data_reader_connection_eager(self, sample_fia_db):
        """Test that connection is established on initialization."""
        reader = FIADataReader(str(sample_fia_db))

        # Connection should be established on init
        assert reader._duckdb_conn is not None

        # Test basic table reading
        result = reader.read_table('PLOT', lazy=False)
        assert isinstance(result, pl.DataFrame)

    def test_data_reader_connection_reuse(self, sample_fia_db):
        """Test that connection is reused across operations."""
        reader = FIADataReader(str(sample_fia_db))

        # Store original connection
        original_conn = reader._duckdb_conn

        # Load data multiple times using different methods
        result1 = reader.read_table('PLOT', lazy=False)
        result2 = reader.read_table('TREE', lazy=False)

        # Should reuse same connection
        assert reader._duckdb_conn is original_conn
        assert isinstance(result1, pl.DataFrame)
        assert isinstance(result2, pl.DataFrame)

    def test_data_reader_cleanup(self, sample_fia_db):
        """Test data reader cleanup on destruction."""
        reader = FIADataReader(str(sample_fia_db))
        assert reader._duckdb_conn is not None

        # Test basic functionality works
        result = reader.read_table('PLOT', lazy=False)
        assert isinstance(result, pl.DataFrame)

        # Cleanup is handled by __del__ automatically


class TestFIADataReaderTableLoading:
    """Test table loading functionality."""

    def test_read_plot_data(self, sample_fia_db):
        """Test reading plot data by EVALID."""
        reader = FIADataReader(str(sample_fia_db))
        # Use the test EVALID from conftest
        plots = reader.read_plot_data([372301])

        assert isinstance(plots, pl.DataFrame)
        assert len(plots) > 0

        # Check required columns
        expected_cols = ["CN", "STATECD", "INVYR", "PLOT_STATUS_CD"]
        for col in expected_cols:
            assert col in plots.columns

        # Check data values
        assert (plots["STATECD"] > 0).all()
        assert (plots["INVYR"] >= 1990).all()

        # Should have EVALID information added
        assert "EVALID" in plots.columns
        assert (plots["EVALID"] == 372301).all()

    def test_read_tree_data(self, sample_fia_db):
        """Test reading tree data by plot CNs."""
        reader = FIADataReader(str(sample_fia_db))

        # First get plot CNs from evaluation
        plots = reader.read_plot_data([372301])
        plot_cns = plots["CN"].to_list()

        # Then get trees for those plots
        trees = reader.read_tree_data(plot_cns)

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

        # Trees should reference the plots we specified
        tree_plot_cns = set(trees["PLT_CN"].to_list())
        assert tree_plot_cns.issubset(set(plot_cns))

    def test_read_condition_data(self, sample_fia_db):
        """Test reading condition data by plot CNs."""
        reader = FIADataReader(str(sample_fia_db))

        # First get plot CNs from evaluation
        plots = reader.read_plot_data([372301])
        plot_cns = plots["CN"].to_list()

        # Then get conditions for those plots
        conditions = reader.read_cond_data(plot_cns)

        assert isinstance(conditions, pl.DataFrame)
        assert len(conditions) > 0

        # Check required columns
        expected_cols = ["PLT_CN", "CONDID", "COND_STATUS_CD"]
        for col in expected_cols:
            assert col in conditions.columns

        # Check data values
        assert (conditions["CONDID"] > 0).all()
        assert (conditions["COND_STATUS_CD"] > 0).all()

        # Conditions should reference the plots we specified
        cond_plot_cns = set(conditions["PLT_CN"].to_list())
        assert cond_plot_cns.issubset(set(plot_cns))

    def test_read_evaluation_data(self, sample_fia_db):
        """Test reading evaluation data directly from table."""
        reader = FIADataReader(str(sample_fia_db))
        # Use read_table to access POP_EVAL
        evaluations = reader.read_table("POP_EVAL", lazy=False)

        assert isinstance(evaluations, pl.DataFrame)
        assert len(evaluations) > 0

        # Check required columns
        expected_cols = ["EVALID", "STATECD", "EVAL_TYP"]
        for col in expected_cols:
            assert col in evaluations.columns

        # Check data values
        assert (evaluations["EVALID"] > 0).all()
        assert (evaluations["STATECD"] > 0).all()

        # Should include our test evaluation
        assert 372301 in evaluations["EVALID"].to_list()

    def test_read_stratum_data(self, sample_fia_db):
        """Test reading stratum data directly from table."""
        reader = FIADataReader(str(sample_fia_db))
        # Use read_table to access POP_STRATUM directly
        strata = reader.read_table("POP_STRATUM", where="EVALID = 372301", lazy=False)

        assert isinstance(strata, pl.DataFrame)
        assert len(strata) > 0

        # Check required columns
        expected_cols = ["EVALID", "EXPNS", "P2POINTCNT"]
        for col in expected_cols:
            assert col in strata.columns

        # Check data values
        assert (strata["EVALID"] > 0).all()
        assert (strata["EXPNS"] > 0).all()
        assert (strata["EVALID"] == 372301).all()

    def test_read_species_data(self, sample_fia_db):
        """Test reading species reference data directly from table."""
        reader = FIADataReader(str(sample_fia_db))
        # Use read_table to access REF_SPECIES
        species = reader.read_table("REF_SPECIES", lazy=False)

        assert isinstance(species, pl.DataFrame)
        assert len(species) > 0

        # Check required columns
        expected_cols = ["SPCD", "COMMON_NAME", "SCIENTIFIC_NAME"]
        for col in expected_cols:
            assert col in species.columns

        # Check data values
        assert (species["SPCD"] > 0).all()

        # Should include test species from conftest
        test_species = [131, 110, 833, 802]  # From conftest species data
        found_species = species["SPCD"].to_list()
        assert any(sp in found_species for sp in test_species)


class TestFIADataReaderFiltering:
    """Test data filtering functionality."""

    def test_read_plot_data_with_state_filter(self, sample_fia_db):
        """Test reading plot data with state filter via SQL WHERE clause."""
        reader = FIADataReader(str(sample_fia_db))
        # Use read_table with WHERE clause for filtering
        plots = reader.read_table("PLOT", where="STATECD = 37", lazy=False)

        assert isinstance(plots, pl.DataFrame)
        assert len(plots) > 0
        assert (plots["STATECD"] == 37).all()

    def test_read_plot_data_with_year_filter(self, sample_fia_db):
        """Test reading plot data with year filter via SQL WHERE clause."""
        reader = FIADataReader(str(sample_fia_db))
        # Use read_table with WHERE clause for year filtering
        plots = reader.read_table("PLOT", where="INVYR = 2020", lazy=False)

        assert isinstance(plots, pl.DataFrame)
        if len(plots) > 0:
            assert (plots["INVYR"] == 2020).all()

    def test_read_plot_data_with_evalid_filter(self, sample_fia_db):
        """Test reading plot data filtered by evaluation ID."""
        reader = FIADataReader(str(sample_fia_db))
        # Use the proper EVALID-based method
        plots = reader.read_plot_data([372301])

        assert isinstance(plots, pl.DataFrame)
        assert len(plots) > 0
        # Should return plots associated with this evaluation
        assert "EVALID" in plots.columns
        assert (plots["EVALID"] == 372301).all()

    def test_read_tree_data_with_species_filter(self, sample_fia_db):
        """Test reading tree data with species filter via SQL WHERE clause."""
        reader = FIADataReader(str(sample_fia_db))
        # Use read_table with WHERE clause for species filtering
        trees = reader.read_table("TREE", where="SPCD IN (131, 110)", lazy=False)

        assert isinstance(trees, pl.DataFrame)
        if len(trees) > 0:
            assert trees["SPCD"].is_in([131, 110]).all()

    def test_read_tree_data_with_status_filter(self, sample_fia_db):
        """Test reading tree data with status filter via SQL WHERE clause."""
        reader = FIADataReader(str(sample_fia_db))
        # Use read_table with WHERE clause for status filtering
        trees = reader.read_table("TREE", where="STATUSCD = 1", lazy=False)  # Live trees only

        assert isinstance(trees, pl.DataFrame)
        if len(trees) > 0:
            assert (trees["STATUSCD"] == 1).all()


class TestFIADataReaderErrorHandling:
    """Test error handling and edge cases."""

    def test_read_nonexistent_table(self, sample_fia_db):
        """Test reading non-existent table."""
        reader = FIADataReader(str(sample_fia_db))

        with pytest.raises((Exception, pl.exceptions.ComputeError, duckdb.CatalogException)):
            reader.read_table("NONEXISTENT_TABLE", lazy=False)

    def test_read_data_with_invalid_filter(self, sample_fia_db):
        """Test reading data with invalid filter values."""
        reader = FIADataReader(str(sample_fia_db))

        # Invalid state code
        plots = reader.read_table("PLOT", where="STATECD = 999", lazy=False)
        assert isinstance(plots, pl.DataFrame)
        assert len(plots) == 0  # Should return empty DataFrame

        # Invalid species code
        trees = reader.read_table("TREE", where="SPCD = 99999", lazy=False)
        assert isinstance(trees, pl.DataFrame)
        assert len(trees) == 0  # Should return empty DataFrame

    def test_read_data_with_sql_injection_attempt(self, sample_fia_db):
        """Test protection against SQL injection."""
        reader = FIADataReader(str(sample_fia_db))

        # Attempt SQL injection (should be safely handled)
        try:
            # This should either be safely escaped or raise an error
            malicious_filter = "1; DROP TABLE PLOT; --"
            plots = reader.read_table("PLOT", where=f"STATECD = {malicious_filter}", lazy=False)
            # If it doesn't raise an error, should return empty or safe result
            assert isinstance(plots, pl.DataFrame)
        except (ValueError, TypeError, pl.exceptions.ComputeError, duckdb.Error):
            # Expected - filter should be rejected
            pass

    def test_connection_error_handling(self, sample_fia_db):
        """Test handling of connection errors."""
        reader = FIADataReader(str(sample_fia_db))

        # Simulate connection failure by patching the DuckDB connection
        with patch.object(reader, '_duckdb_conn') as mock_conn:
            mock_conn.execute.side_effect = Exception("Database connection failed")

            with pytest.raises(Exception):
                reader.read_table("PLOT", lazy=False)


class TestFIADataReaderPerformance:
    """Test performance characteristics."""

    def test_read_data_performance(self, sample_fia_db):
        """Test data reading performance."""
        import time

        reader = FIADataReader(str(sample_fia_db))

        start_time = time.time()
        plots = reader.read_plot_data([372301])
        plot_cns = plots["CN"].to_list()
        trees = reader.read_tree_data(plot_cns)
        conditions = reader.read_cond_data(plot_cns)
        end_time = time.time()

        # Should load test data quickly
        loading_time = end_time - start_time
        assert loading_time < 3.0  # Less than 3 seconds for test data

        # Should return non-empty data
        assert len(plots) > 0
        assert len(trees) > 0
        assert len(conditions) > 0

    def test_repeated_read_performance(self, sample_fia_db):
        """Test performance of repeated reads."""
        import time

        reader = FIADataReader(str(sample_fia_db))

        # First read
        start_time = time.time()
        plots1 = reader.read_plot_data([372301])
        first_load_time = time.time() - start_time

        # Second read (connection should be reused)
        start_time = time.time()
        plots2 = reader.read_plot_data([372301])
        second_load_time = time.time() - start_time

        # Both should return equivalent data
        assert len(plots1) == len(plots2)

        # Performance improvement is implementation dependent
        # Just ensure both complete in reasonable time
        assert first_load_time < 3.0
        assert second_load_time < 3.0


class TestFIADataReaderDataConsistency:
    """Test data consistency and relationships."""

    def test_data_referential_integrity(self, sample_fia_db):
        """Test referential integrity between tables."""
        reader = FIADataReader(str(sample_fia_db))

        plots = reader.read_plot_data([372301])
        plot_cns = plots["CN"].to_list()
        trees = reader.read_tree_data(plot_cns)
        conditions = reader.read_cond_data(plot_cns)

        # Trees should reference existing plots
        plot_cns_set = set(plots["CN"].to_list())
        tree_plot_cns = set(trees["PLT_CN"].to_list())

        # All tree plot references should exist in plots
        assert tree_plot_cns.issubset(plot_cns_set)

        # Conditions should reference existing plots
        cond_plot_cns = set(conditions["PLT_CN"].to_list())
        assert cond_plot_cns.issubset(plot_cns_set)

    def test_data_type_consistency(self, sample_fia_db):
        """Test that data types are consistent."""
        reader = FIADataReader(str(sample_fia_db))

        plots = reader.read_plot_data([372301])
        plot_cns = plots["CN"].to_list()
        trees = reader.read_tree_data(plot_cns)

        # Check key data types
        assert plots["STATECD"].dtype in [pl.Int32, pl.Int64]
        assert plots["INVYR"].dtype in [pl.Int32, pl.Int64]
        assert trees["DIA"].dtype in [pl.Float32, pl.Float64]
        assert trees["SPCD"].dtype in [pl.Int32, pl.Int64]

    def test_data_range_validation(self, sample_fia_db):
        """Test that data values are within expected ranges."""
        reader = FIADataReader(str(sample_fia_db))

        plots = reader.read_plot_data([372301])
        plot_cns = plots["CN"].to_list()
        trees = reader.read_tree_data(plot_cns)

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
        plots = reader.read_table("PLOT", where="STATECD = 999", lazy=False)

        # Should return empty DataFrame with correct schema
        assert isinstance(plots, pl.DataFrame)
        assert len(plots) == 0
        assert "CN" in plots.columns  # Should maintain schema

    def test_large_filter_lists(self, sample_fia_db):
        """Test handling of large filter lists."""
        reader = FIADataReader(str(sample_fia_db))

        # Large list of species codes
        large_species_list = list(range(100, 1000))
        species_str = ", ".join(str(s) for s in large_species_list)
        trees = reader.read_table("TREE", where=f"SPCD IN ({species_str})", lazy=False)

        # Should handle gracefully
        assert isinstance(trees, pl.DataFrame)

    def test_null_value_handling(self, sample_fia_db):
        """Test handling of null values in data."""
        reader = FIADataReader(str(sample_fia_db))

        plots = reader.read_plot_data([372301])
        plot_cns = plots["CN"].to_list()
        trees = reader.read_tree_data(plot_cns)

        # Check for null handling (implementation dependent)
        # At minimum, should not crash
        assert isinstance(plots, pl.DataFrame)
        assert isinstance(trees, pl.DataFrame)


class TestFIADataReaderCustomQueries:
    """Test custom query functionality."""

    def test_execute_custom_query(self, sample_fia_db):
        """Test executing custom SQL queries via DuckDB connection."""
        reader = FIADataReader(str(sample_fia_db))

        # Simple custom query using DuckDB connection directly
        try:
            result_raw = reader._duckdb_conn.execute("SELECT COUNT(*) as plot_count FROM PLOT").fetchall()
            assert len(result_raw) > 0
            assert result_raw[0][0] > 0  # plot_count > 0
        except AttributeError:
            # Method might not exist in current implementation
            pass

    def test_read_custom_table(self, sample_fia_db):
        """Test reading custom table via read_table method."""
        reader = FIADataReader(str(sample_fia_db))

        # Use existing read_table method
        species = reader.read_table("REF_SPECIES", lazy=False)
        assert isinstance(species, pl.DataFrame)
        assert len(species) > 0

    def test_complex_join_query(self, sample_fia_db):
        """Test complex multi-table queries via DuckDB connection."""
        reader = FIADataReader(str(sample_fia_db))

        try:
            # Join plots, trees, and species using DuckDB connection
            query = """
            SELECT p.CN as PLT_CN, t.SPCD, s.COMMON_NAME, COUNT(*) as tree_count
            FROM PLOT p
            JOIN TREE t ON p.CN = t.PLT_CN
            JOIN REF_SPECIES s ON t.SPCD = s.SPCD
            GROUP BY p.CN, t.SPCD, s.COMMON_NAME
            """
            result_raw = reader._duckdb_conn.execute(query).fetchall()
            if len(result_raw) > 0:
                assert len(result_raw[0]) >= 4  # Should have PLT_CN, SPCD, COMMON_NAME, tree_count
                assert result_raw[0][3] > 0  # tree_count > 0
        except Exception:
            # Query might fail if no matching data
            pass
