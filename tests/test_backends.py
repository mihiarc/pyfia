"""Tests for database backend functionality."""

import sqlite3
import tempfile
from pathlib import Path

import duckdb
import polars as pl
import pytest

from pyfia.core.backends import DatabaseBackend, DuckDBBackend, SQLiteBackend, detect_engine
from pyfia.core.data_reader import FIADataReader
from pyfia import FIA


@pytest.fixture
def sample_data():
    """Create sample FIA data for testing."""
    return {
        "TREE": pl.DataFrame({
            "CN": ["1", "2", "3", "4", "5"],
            "PLT_CN": ["P1", "P1", "P2", "P2", "P3"],
            "STATECD": [37, 37, 45, 45, 41],
            "STATUSCD": [1, 1, 2, 1, 1],
            "DIA": [10.5, 15.2, 8.3, 22.1, 18.7],
            "SPCD": [131, 110, 833, 802, 202],
        }),
        "PLOT": pl.DataFrame({
            "CN": ["P1", "P2", "P3"],
            "STATECD": [37, 45, 41],
            "PLOT": [1, 2, 3],
        }),
        "POP_EVAL": pl.DataFrame({
            "CN": ["E1", "E2", "E3"],
            "EVALID": [371001, 451001, 411001],
            "STATECD": [37, 45, 41],
            "END_INVYR": [2020, 2021, 2019],
        }),
        "POP_EVAL_TYP": pl.DataFrame({
            "EVAL_CN": ["E1", "E2", "E3"],
            "EVAL_TYP": ["EXPVOL", "EXPVOL", "EXPVOL"],
        }),
    }


@pytest.fixture
def sqlite_test_db(sample_data):
    """Create a temporary SQLite database with sample data."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        db_path = Path(tmp.name)
    
    conn = sqlite3.connect(str(db_path))
    for table_name, df in sample_data.items():
        df.write_database(table_name, conn, if_table_exists="replace")
    conn.close()
    
    yield db_path
    
    # Cleanup
    db_path.unlink(missing_ok=True)


@pytest.fixture
def duckdb_test_db(sample_data):
    """Create a temporary DuckDB database with sample data."""
    with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as tmp:
        db_path = Path(tmp.name)
    
    conn = duckdb.connect(str(db_path))
    for table_name, df in sample_data.items():
        conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
    conn.close()
    
    yield db_path
    
    # Cleanup
    db_path.unlink(missing_ok=True)


class TestBackendDetection:
    """Test automatic backend detection."""
    
    def test_detect_duckdb(self, duckdb_test_db):
        """Test DuckDB detection."""
        engine = detect_engine(str(duckdb_test_db))
        assert engine == "duckdb"
    
    def test_detect_sqlite(self, sqlite_test_db):
        """Test SQLite detection."""
        engine = detect_engine(str(sqlite_test_db))
        assert engine == "sqlite"
    
    def test_detect_nonexistent(self):
        """Test detection with non-existent file."""
        with pytest.raises(ValueError, match="Could not determine database type"):
            detect_engine("/path/to/nonexistent.db")


class TestDuckDBBackend:
    """Test DuckDB backend functionality."""
    
    def test_connect_disconnect(self, duckdb_test_db):
        """Test connection management."""
        backend = DuckDBBackend(duckdb_test_db)
        assert not backend.is_connected
        
        backend.connect()
        assert backend.is_connected
        
        backend.disconnect()
        assert not backend.is_connected
    
    def test_execute_query(self, duckdb_test_db):
        """Test query execution."""
        backend = DuckDBBackend(duckdb_test_db)
        backend.connect()
        
        result = backend.execute_query("SELECT COUNT(*) as cnt FROM TREE")
        assert len(result) == 1
        assert result["cnt"][0] == 5
        
        backend.disconnect()
    
    def test_table_operations(self, duckdb_test_db):
        """Test table operations."""
        backend = DuckDBBackend(duckdb_test_db)
        backend.connect()
        
        # Check table exists
        assert backend.table_exists("TREE")
        assert not backend.table_exists("NONEXISTENT")
        
        # Get schema
        schema = backend.get_table_schema("TREE")
        assert "CN" in schema
        assert "STATECD" in schema
        
        # Describe table
        desc = backend.describe_table("TREE")
        assert len(desc) == 6  # 6 columns
        
        backend.disconnect()
    
    def test_context_manager(self, duckdb_test_db):
        """Test context manager usage."""
        with DuckDBBackend(duckdb_test_db) as backend:
            assert backend.is_connected
            result = backend.execute_query("SELECT 1 as test")
            assert result["test"][0] == 1


class TestSQLiteBackend:
    """Test SQLite backend functionality."""
    
    def test_connect_disconnect(self, sqlite_test_db):
        """Test connection management."""
        backend = SQLiteBackend(sqlite_test_db)
        assert not backend.is_connected
        
        backend.connect()
        assert backend.is_connected
        
        backend.disconnect()
        assert not backend.is_connected
    
    def test_execute_query(self, sqlite_test_db):
        """Test query execution."""
        backend = SQLiteBackend(sqlite_test_db)
        backend.connect()
        
        result = backend.execute_query("SELECT COUNT(*) as cnt FROM TREE")
        assert len(result) == 1
        assert result["cnt"][0] == 5
        
        backend.disconnect()
    
    def test_table_operations(self, sqlite_test_db):
        """Test table operations."""
        backend = SQLiteBackend(sqlite_test_db)
        backend.connect()
        
        # Check table exists
        assert backend.table_exists("TREE")
        assert not backend.table_exists("NONEXISTENT")
        
        # Get schema
        schema = backend.get_table_schema("TREE")
        assert "CN" in schema
        assert "STATECD" in schema
        
        # Describe table
        desc = backend.describe_table("TREE")
        assert len(desc) == 6  # 6 columns
        
        backend.disconnect()
    
    def test_pragma_optimizations(self, sqlite_test_db):
        """Test that PRAGMA optimizations are applied."""
        backend = SQLiteBackend(sqlite_test_db)
        backend.connect()
        
        # Check that some optimizations are applied
        result = backend.execute_query("PRAGMA journal_mode")
        assert result["journal_mode"][0] == "wal"
        
        backend.disconnect()


class TestFIADataReaderBackends:
    """Test FIADataReader with different backends."""
    
    def test_auto_detection(self, duckdb_test_db, sqlite_test_db):
        """Test automatic backend detection."""
        # DuckDB
        reader_duck = FIADataReader(duckdb_test_db)
        assert reader_duck._backend.__class__.__name__ == "DuckDBBackend"
        
        # SQLite
        reader_sqlite = FIADataReader(sqlite_test_db)
        assert reader_sqlite._backend.__class__.__name__ == "SQLiteBackend"
    
    def test_explicit_backend(self, duckdb_test_db):
        """Test explicit backend selection."""
        reader = FIADataReader(duckdb_test_db, engine="duckdb")
        assert reader._backend.__class__.__name__ == "DuckDBBackend"
    
    def test_read_table_duckdb(self, duckdb_test_db):
        """Test table reading with DuckDB."""
        reader = FIADataReader(duckdb_test_db)
        
        # Read full table
        trees = reader.read_table("TREE")
        assert len(trees) == 5
        assert "CN" in trees.columns
        
        # Read with columns
        trees_subset = reader.read_table("TREE", columns=["CN", "DIA"])
        assert len(trees_subset.columns) == 2
        
        # Read with WHERE clause
        live_trees = reader.read_table("TREE", where="STATUSCD = 1")
        assert len(live_trees) == 4
    
    def test_read_table_sqlite(self, sqlite_test_db):
        """Test table reading with SQLite."""
        reader = FIADataReader(sqlite_test_db, engine="sqlite")
        
        # Read full table
        trees = reader.read_table("TREE")
        assert len(trees) == 5
        assert "CN" in trees.columns
        
        # Read with WHERE clause
        live_trees = reader.read_table("TREE", where="STATUSCD = 1")
        assert len(live_trees) == 4
    
    def test_batch_processing(self, duckdb_test_db):
        """Test batch processing for large IN clauses."""
        reader = FIADataReader(duckdb_test_db)
        
        # Create a large list of plot CNs (simulating batch need)
        plot_cns = ["P1", "P2", "P3"] * 400  # 1200 items
        
        # This should use batching internally
        trees = reader.read_filtered_data("TREE", "PLT_CN", plot_cns)
        assert len(trees) == 5  # Still only 5 trees in test data
    
    def test_type_standardization(self, duckdb_test_db):
        """Test FIA-specific type standardization."""
        reader = FIADataReader(duckdb_test_db)
        
        trees = reader.read_table("TREE")
        
        # CN fields should be strings
        assert trees["CN"].dtype == pl.Utf8
        assert trees["PLT_CN"].dtype == pl.Utf8
        
        # EVALID should be handled if present
        pop_eval = reader.read_table("POP_EVAL")
        # EVALID might be string or int depending on standardization


class TestFIAWithBackends:
    """Test FIA class with different backends."""
    
    def test_auto_detection(self, duckdb_test_db):
        """Test FIA with auto-detected backend."""
        fia = FIA(duckdb_test_db)
        
        # Should work normally
        evalids = fia.find_evalid()
        assert len(evalids) > 0
    
    def test_explicit_sqlite(self, sqlite_test_db):
        """Test FIA with explicit SQLite backend."""
        fia = FIA(sqlite_test_db, engine="sqlite")
        
        # Should work with SQLite
        evalids = fia.find_evalid()
        assert len(evalids) > 0
    
    def test_data_loading(self, duckdb_test_db):
        """Test data loading through FIA class."""
        fia = FIA(duckdb_test_db)
        
        # Load tables
        fia.load_table("PLOT")
        assert "PLOT" in fia.tables
        
        # Get plots
        plots = fia.get_plots()
        assert len(plots) == 3


if __name__ == "__main__":
    pytest.main([__file__, "-v"])