"""Tests for the database interface layer."""

import sqlite3
import tempfile
from pathlib import Path

import duckdb
import polars as pl
import pytest

from pyfia.database import DuckDBInterface, QueryInterface, SQLiteInterface
from pyfia.database.interface import ConnectionConfig, QueryResult, create_interface


@pytest.fixture
def sample_data():
    """Create sample data for testing."""
    return pl.DataFrame(
        {
            "CN": ["1", "2", "3", "4", "5"],
            "STATECD": [37, 37, 45, 45, 41],
            "STATUSCD": [1, 1, 2, 1, 1],
            "DIA": [10.5, 15.2, 8.3, 22.1, 18.7],
            "SPCD": [131, 110, 833, 802, 202],
        }
    )


@pytest.fixture
def sqlite_db(sample_data):
    """Create a temporary SQLite database with sample data."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        db_path = Path(tmp.name)

    conn = sqlite3.connect(str(db_path))
    sample_data.write_database("TREE", conn, if_table_exists="replace")
    conn.close()

    yield db_path

    # Cleanup
    db_path.unlink(missing_ok=True)


@pytest.fixture
def duckdb_db(sample_data):
    """Create a temporary DuckDB database with sample data."""
    with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as tmp:
        db_path = Path(tmp.name)

    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE TABLE TREE AS SELECT * FROM sample_data")
    conn.close()

    yield db_path

    # Cleanup
    db_path.unlink(missing_ok=True)


class TestConnectionConfig:
    """Test ConnectionConfig model."""

    def test_valid_config(self, sqlite_db):
        """Test creating valid configuration."""
        config = ConnectionConfig(db_path=sqlite_db, read_only=True, timeout=10.0)
        assert config.db_path == sqlite_db
        assert config.read_only is True
        assert config.timeout == 10.0

    def test_invalid_path(self):
        """Test configuration with non-existent path."""
        with pytest.raises(FileNotFoundError):
            ConnectionConfig(db_path=Path("/nonexistent/database.db"))


class TestQueryResult:
    """Test QueryResult model."""

    def test_auto_row_count(self, sample_data):
        """Test automatic row count calculation."""
        result = QueryResult(data=sample_data)
        assert result.row_count == 5

    def test_explicit_row_count(self, sample_data):
        """Test explicit row count."""
        result = QueryResult(data=sample_data, row_count=10)
        assert result.row_count == 10


class TestSQLiteInterface:
    """Test SQLite interface implementation."""

    def test_connect_disconnect(self, sqlite_db):
        """Test connection lifecycle."""
        config = ConnectionConfig(db_path=sqlite_db)
        interface = SQLiteInterface(config)

        # Initially not connected
        assert interface._connection is None

        # Connect
        interface.connect()
        assert interface._connection is not None

        # Disconnect
        interface.disconnect()
        assert interface._connection is None

    def test_context_manager(self, sqlite_db):
        """Test context manager behavior."""
        config = ConnectionConfig(db_path=sqlite_db)

        with SQLiteInterface(config) as interface:
            assert interface._connection is not None

        # Should be disconnected after context
        assert interface._connection is None

    def test_execute_query(self, sqlite_db):
        """Test query execution."""
        config = ConnectionConfig(db_path=sqlite_db)

        with SQLiteInterface(config) as interface:
            result = interface.execute_query("SELECT * FROM TREE WHERE STATECD = 37")
            assert isinstance(result, QueryResult)
            assert isinstance(result.data, pl.DataFrame)
            assert result.row_count == 2
            assert result.execution_time_ms is not None

    def test_execute_query_with_params(self, sqlite_db):
        """Test parameterized query execution."""
        config = ConnectionConfig(db_path=sqlite_db)

        with SQLiteInterface(config) as interface:
            result = interface.execute_query(
                "SELECT * FROM TREE WHERE STATECD = :state AND DIA > :min_dia",
                params={"state": 37, "min_dia": 12.0},
            )
            assert result.row_count == 1
            assert result.data["CN"][0] == "2"

    def test_read_table(self, sqlite_db):
        """Test table reading with filters."""
        config = ConnectionConfig(db_path=sqlite_db)

        with SQLiteInterface(config) as interface:
            # Read full table
            df = interface.read_table("TREE")
            assert len(df) == 5

            # Read with column selection
            df = interface.read_table("TREE", columns=["CN", "STATECD"])
            assert df.columns == ["CN", "STATECD"]

            # Read with WHERE clause
            df = interface.read_table("TREE", where="STATUSCD = 1")
            assert len(df) == 4

            # Read with limit
            df = interface.read_table("TREE", limit=2)
            assert len(df) == 2

    def test_get_table_schema(self, sqlite_db):
        """Test schema retrieval."""
        config = ConnectionConfig(db_path=sqlite_db)

        with SQLiteInterface(config) as interface:
            schema = interface.get_table_schema("TREE")
            assert isinstance(schema, dict)
            assert "CN" in schema
            assert "STATECD" in schema

            # Test caching
            schema2 = interface.get_table_schema("TREE")
            assert schema2 is schema  # Should be the same cached object

    def test_table_exists(self, sqlite_db):
        """Test table existence check."""
        config = ConnectionConfig(db_path=sqlite_db)

        with SQLiteInterface(config) as interface:
            assert interface.table_exists("TREE") is True
            assert interface.table_exists("NONEXISTENT") is False


class TestDuckDBInterface:
    """Test DuckDB interface implementation."""

    def test_connect_disconnect(self, duckdb_db):
        """Test connection lifecycle."""
        config = ConnectionConfig(db_path=duckdb_db)
        interface = DuckDBInterface(config)

        # Initially not connected
        assert interface._connection is None

        # Connect
        interface.connect()
        assert interface._connection is not None

        # Disconnect
        interface.disconnect()
        assert interface._connection is None

    def test_context_manager(self, duckdb_db):
        """Test context manager behavior."""
        config = ConnectionConfig(db_path=duckdb_db)

        with DuckDBInterface(config) as interface:
            assert interface._connection is not None

        # Should be disconnected after context
        assert interface._connection is None

    def test_execute_query(self, duckdb_db):
        """Test query execution."""
        config = ConnectionConfig(db_path=duckdb_db)

        with DuckDBInterface(config) as interface:
            result = interface.execute_query("SELECT * FROM TREE WHERE STATECD = 37")
            assert isinstance(result, QueryResult)
            assert isinstance(result.data, pl.DataFrame)
            assert result.row_count == 2
            assert result.execution_time_ms is not None

    def test_execute_query_with_params(self, duckdb_db):
        """Test parameterized query execution."""
        config = ConnectionConfig(db_path=duckdb_db)

        with DuckDBInterface(config) as interface:
            result = interface.execute_query(
                "SELECT * FROM TREE WHERE STATECD = :state AND DIA > :min_dia",
                params={"state": 37, "min_dia": 12.0},
            )
            assert result.row_count == 1
            assert result.data["CN"][0] == "2"

    def test_configuration_options(self, duckdb_db):
        """Test DuckDB-specific configuration options."""
        config = ConnectionConfig(
            db_path=duckdb_db,
            memory_limit="1GB",
            threads=2,
        )

        with DuckDBInterface(config) as interface:
            # Connection should work with these options
            result = interface.execute_query("SELECT COUNT(*) as cnt FROM TREE")
            assert result.data["cnt"][0] == 5

    def test_get_table_schema(self, duckdb_db):
        """Test schema retrieval."""
        config = ConnectionConfig(db_path=duckdb_db)

        with DuckDBInterface(config) as interface:
            schema = interface.get_table_schema("TREE")
            assert isinstance(schema, dict)
            assert "CN" in schema
            assert "STATECD" in schema

    def test_table_exists(self, duckdb_db):
        """Test table existence check."""
        config = ConnectionConfig(db_path=duckdb_db)

        with DuckDBInterface(config) as interface:
            assert interface.table_exists("TREE") is True
            assert interface.table_exists("NONEXISTENT") is False


class TestCreateInterface:
    """Test the factory function."""

    def test_auto_detect_sqlite(self, sqlite_db):
        """Test auto-detection of SQLite database."""
        interface = create_interface(sqlite_db)
        assert isinstance(interface, SQLiteInterface)

    def test_auto_detect_duckdb(self, duckdb_db):
        """Test auto-detection of DuckDB database."""
        interface = create_interface(duckdb_db)
        assert isinstance(interface, DuckDBInterface)

    def test_explicit_engine(self, sqlite_db, duckdb_db):
        """Test explicit engine specification."""
        # SQLite with explicit engine
        interface = create_interface(sqlite_db, engine="sqlite")
        assert isinstance(interface, SQLiteInterface)

        # DuckDB with explicit engine
        interface = create_interface(duckdb_db, engine="duckdb")
        assert isinstance(interface, DuckDBInterface)

    def test_invalid_engine(self, sqlite_db):
        """Test invalid engine specification."""
        with pytest.raises(ValueError, match="Unsupported database engine"):
            create_interface(sqlite_db, engine="postgresql")

    def test_additional_kwargs(self, sqlite_db):
        """Test passing additional configuration options."""
        interface = create_interface(
            sqlite_db,
            engine="sqlite",
            read_only=False,
            timeout=60.0,
        )
        assert interface.config.read_only is False
        assert interface.config.timeout == 60.0


class TestInterfaceCompatibility:
    """Test that both interfaces provide consistent behavior."""

    @pytest.mark.parametrize("db_fixture", ["sqlite_db", "duckdb_db"])
    def test_consistent_results(self, request, db_fixture):
        """Test that both interfaces return consistent results."""
        db_path = request.getfixturevalue(db_fixture)
        interface = create_interface(db_path)

        with interface:
            # Test basic query
            result = interface.execute_query("SELECT COUNT(*) as cnt FROM TREE")
            assert result.data["cnt"][0] == 5

            # Test filtered query
            result = interface.execute_query(
                "SELECT * FROM TREE WHERE STATUSCD = :status ORDER BY CN",
                params={"status": 1},
            )
            assert result.row_count == 4

            # Test table reading
            df = interface.read_table("TREE", where="STATECD = 45")
            assert len(df) == 2

    @pytest.mark.parametrize("db_fixture", ["sqlite_db", "duckdb_db"])
    def test_transaction_behavior(self, request, db_fixture):
        """Test transaction behavior across interfaces."""
        db_path = request.getfixturevalue(db_fixture)

        # Create interface with write permissions
        interface = create_interface(db_path, read_only=False)

        with interface:
            # Start transaction
            with interface.transaction():
                # This would normally do an insert/update
                # For testing, we just verify the transaction context works
                pass

            # Verify data unchanged (since we didn't actually modify)
            result = interface.execute_query("SELECT COUNT(*) as cnt FROM TREE")
            assert result.data["cnt"][0] == 5