"""
Tests for FIA SQLite to DuckDB converter functionality.

This module tests the simplified converter that uses DuckDB's native
sqlite_scanner extension for efficient conversion.
"""

import tempfile
from pathlib import Path
import sqlite3

import pytest
import duckdb

from pyfia.converter import (
    convert_sqlite_to_duckdb,
    merge_states,
    append_state,
    get_database_info,
    compare_databases,
)


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files."""
    with tempfile.TemporaryDirectory() as td:
        yield Path(td)


@pytest.fixture
def sample_sqlite_db(temp_dir):
    """Create a sample SQLite database for testing."""
    db_path = temp_dir / "test_fia.db"

    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    # Create PLOT table
    cursor.execute("""
        CREATE TABLE PLOT (
            CN INTEGER PRIMARY KEY,
            STATECD INTEGER,
            INVYR INTEGER,
            LAT REAL,
            LON REAL
        )
    """)
    cursor.execute("""
        INSERT INTO PLOT VALUES
        (1001, 37, 2020, 35.123, -80.123),
        (1002, 37, 2020, 35.234, -80.234),
        (1003, 37, 2021, 35.345, -80.345)
    """)

    # Create COND table
    cursor.execute("""
        CREATE TABLE COND (
            CN INTEGER PRIMARY KEY,
            PLT_CN INTEGER,
            CONDID INTEGER,
            COND_STATUS_CD INTEGER
        )
    """)
    cursor.execute("""
        INSERT INTO COND VALUES
        (2001, 1001, 1, 1),
        (2002, 1002, 1, 1),
        (2003, 1003, 1, 1)
    """)

    # Create TREE table
    cursor.execute("""
        CREATE TABLE TREE (
            CN INTEGER PRIMARY KEY,
            PLT_CN INTEGER,
            CONDID INTEGER,
            STATUSCD INTEGER,
            SPCD INTEGER,
            DIA REAL
        )
    """)
    cursor.execute("""
        INSERT INTO TREE VALUES
        (3001, 1001, 1, 1, 131, 10.5),
        (3002, 1001, 1, 1, 110, 12.3),
        (3003, 1002, 1, 1, 833, 8.7),
        (3004, 1003, 1, 2, 131, 15.2)
    """)

    conn.commit()
    conn.close()

    return db_path


@pytest.fixture
def second_sqlite_db(temp_dir):
    """Create a second SQLite database for merge testing."""
    db_path = temp_dir / "test_fia2.db"

    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    # Create PLOT table with different state
    cursor.execute("""
        CREATE TABLE PLOT (
            CN INTEGER PRIMARY KEY,
            STATECD INTEGER,
            INVYR INTEGER,
            LAT REAL,
            LON REAL
        )
    """)
    cursor.execute("""
        INSERT INTO PLOT VALUES
        (4001, 45, 2020, 34.123, -81.123),
        (4002, 45, 2020, 34.234, -81.234)
    """)

    # Create COND table
    cursor.execute("""
        CREATE TABLE COND (
            CN INTEGER PRIMARY KEY,
            PLT_CN INTEGER,
            CONDID INTEGER,
            COND_STATUS_CD INTEGER
        )
    """)
    cursor.execute("""
        INSERT INTO COND VALUES
        (5001, 4001, 1, 1),
        (5002, 4002, 1, 1)
    """)

    # Create TREE table
    cursor.execute("""
        CREATE TABLE TREE (
            CN INTEGER PRIMARY KEY,
            PLT_CN INTEGER,
            CONDID INTEGER,
            STATUSCD INTEGER,
            SPCD INTEGER,
            DIA REAL
        )
    """)
    cursor.execute("""
        INSERT INTO TREE VALUES
        (6001, 4001, 1, 1, 802, 11.5),
        (6002, 4002, 1, 1, 833, 9.8)
    """)

    conn.commit()
    conn.close()

    return db_path


class TestConvertSqliteToDuckdb:
    """Test the convert_sqlite_to_duckdb function."""

    def test_basic_conversion(self, sample_sqlite_db, temp_dir):
        """Test basic SQLite to DuckDB conversion."""
        target_path = temp_dir / "output.duckdb"

        row_counts = convert_sqlite_to_duckdb(
            source_path=sample_sqlite_db,
            target_path=target_path,
            state_code=37,
            show_progress=False,
        )

        assert target_path.exists()
        assert "PLOT" in row_counts
        assert "COND" in row_counts
        assert "TREE" in row_counts
        assert row_counts["PLOT"] == 3
        assert row_counts["COND"] == 3
        assert row_counts["TREE"] == 4

    def test_conversion_creates_valid_duckdb(self, sample_sqlite_db, temp_dir):
        """Test that converted database is valid DuckDB."""
        target_path = temp_dir / "output.duckdb"

        convert_sqlite_to_duckdb(
            source_path=sample_sqlite_db, target_path=target_path, show_progress=False
        )

        # Verify we can query the DuckDB database
        with duckdb.connect(str(target_path), read_only=True) as conn:
            plots = conn.execute("SELECT COUNT(*) FROM PLOT").fetchone()[0]
            trees = conn.execute("SELECT COUNT(*) FROM TREE").fetchone()[0]

            assert plots == 3
            assert trees == 4

    def test_conversion_with_state_code(self, sample_sqlite_db, temp_dir):
        """Test conversion adds STATE_ADDED column when state_code provided."""
        target_path = temp_dir / "output.duckdb"

        convert_sqlite_to_duckdb(
            source_path=sample_sqlite_db,
            target_path=target_path,
            state_code=37,
            show_progress=False,
        )

        with duckdb.connect(str(target_path), read_only=True) as conn:
            # Check STATE_ADDED column exists
            columns = conn.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_name = 'PLOT'"
            ).fetchall()
            column_names = [c[0] for c in columns]

            assert "STATE_ADDED" in column_names

            # Check all rows have correct state code
            state_added = conn.execute(
                "SELECT DISTINCT STATE_ADDED FROM PLOT"
            ).fetchone()[0]
            assert state_added == 37

    def test_conversion_specific_tables(self, sample_sqlite_db, temp_dir):
        """Test converting only specific tables."""
        target_path = temp_dir / "output.duckdb"

        row_counts = convert_sqlite_to_duckdb(
            source_path=sample_sqlite_db,
            target_path=target_path,
            tables=["PLOT", "TREE"],
            show_progress=False,
        )

        # Should only have PLOT and TREE
        assert "PLOT" in row_counts
        assert "TREE" in row_counts
        assert "COND" not in row_counts

    def test_conversion_nonexistent_source(self, temp_dir):
        """Test conversion with nonexistent source file."""
        with pytest.raises(FileNotFoundError):
            convert_sqlite_to_duckdb(
                source_path=temp_dir / "nonexistent.db",
                target_path=temp_dir / "output.duckdb",
            )


class TestMergeStates:
    """Test the merge_states function."""

    def test_merge_two_states(self, sample_sqlite_db, second_sqlite_db, temp_dir):
        """Test merging two state databases."""
        target_path = temp_dir / "merged.duckdb"

        results = merge_states(
            source_paths=[sample_sqlite_db, second_sqlite_db],
            state_codes=[37, 45],
            target_path=target_path,
            show_progress=False,
        )

        assert target_path.exists()
        assert "37" in results
        assert "45" in results

        # Verify merged data
        with duckdb.connect(str(target_path), read_only=True) as conn:
            # Total plots should be 5 (3 from first + 2 from second)
            total_plots = conn.execute("SELECT COUNT(*) FROM PLOT").fetchone()[0]
            assert total_plots == 5

            # Total trees should be 6 (4 from first + 2 from second)
            total_trees = conn.execute("SELECT COUNT(*) FROM TREE").fetchone()[0]
            assert total_trees == 6

            # Both states should be present
            states = conn.execute(
                "SELECT DISTINCT STATE_ADDED FROM PLOT ORDER BY STATE_ADDED"
            ).fetchall()
            assert [s[0] for s in states] == [37, 45]

    def test_merge_mismatched_lengths(self, sample_sqlite_db, temp_dir):
        """Test merge with mismatched source_paths and state_codes."""
        with pytest.raises(ValueError):
            merge_states(
                source_paths=[sample_sqlite_db],
                state_codes=[37, 45],  # More state codes than paths
                target_path=temp_dir / "merged.duckdb",
            )


class TestAppendState:
    """Test the append_state function."""

    def test_basic_append(self, sample_sqlite_db, second_sqlite_db, temp_dir):
        """Test basic append operation."""
        target_path = temp_dir / "target.duckdb"

        # First conversion
        convert_sqlite_to_duckdb(
            source_path=sample_sqlite_db,
            target_path=target_path,
            state_code=37,
            show_progress=False,
        )

        # Append second state
        row_counts = append_state(
            source_path=second_sqlite_db,
            target_path=target_path,
            state_code=45,
            show_progress=False,
        )

        assert "PLOT" in row_counts

        # Verify both states are present
        with duckdb.connect(str(target_path), read_only=True) as conn:
            total_plots = conn.execute("SELECT COUNT(*) FROM PLOT").fetchone()[0]
            assert total_plots == 5  # 3 + 2

    def test_append_to_nonexistent_target(self, sample_sqlite_db, temp_dir):
        """Test append to nonexistent target."""
        with pytest.raises(FileNotFoundError):
            append_state(
                source_path=sample_sqlite_db,
                target_path=temp_dir / "nonexistent.duckdb",
                state_code=37,
            )


class TestGetDatabaseInfo:
    """Test the get_database_info function."""

    def test_basic_info(self, sample_sqlite_db, temp_dir):
        """Test getting database info."""
        target_path = temp_dir / "output.duckdb"

        convert_sqlite_to_duckdb(
            source_path=sample_sqlite_db, target_path=target_path, show_progress=False
        )

        info = get_database_info(target_path)

        assert "path" in info
        assert "file_size_mb" in info
        assert "tables" in info
        assert "total_tables" in info
        assert "total_rows" in info

        assert info["total_tables"] == 3
        assert info["total_rows"] == 10  # 3 plots + 3 conds + 4 trees

        assert "PLOT" in info["tables"]
        assert info["tables"]["PLOT"]["rows"] == 3

    def test_info_nonexistent_db(self, temp_dir):
        """Test getting info from nonexistent database."""
        with pytest.raises(FileNotFoundError):
            get_database_info(temp_dir / "nonexistent.duckdb")


class TestCompareDatabases:
    """Test the compare_databases function."""

    def test_compare_source_and_target(self, sample_sqlite_db, temp_dir):
        """Test comparing source SQLite and target DuckDB."""
        target_path = temp_dir / "output.duckdb"

        convert_sqlite_to_duckdb(
            source_path=sample_sqlite_db, target_path=target_path, show_progress=False
        )

        comparison = compare_databases(sample_sqlite_db, target_path)

        assert "source_tables" in comparison
        assert "target_tables" in comparison
        assert "common_tables" in comparison
        assert "row_counts" in comparison

        # All tables should be common
        assert comparison["common_tables"] == 3

        # Row counts should match for all tables
        for table, counts in comparison["row_counts"].items():
            assert counts["difference"] == 0


class TestFIAClassMethods:
    """Test FIA class converter methods."""

    def test_fia_convert_from_sqlite(self, sample_sqlite_db, temp_dir):
        """Test FIA.convert_from_sqlite class method."""
        from pyfia import FIA

        target_path = temp_dir / "output.duckdb"

        result = FIA.convert_from_sqlite(
            source_path=sample_sqlite_db,
            target_path=target_path,
            state_code=37,
            show_progress=False,
        )

        assert target_path.exists()
        assert isinstance(result, dict)
        assert "PLOT" in result

    def test_fia_merge_states(self, sample_sqlite_db, second_sqlite_db, temp_dir):
        """Test FIA.merge_states class method."""
        from pyfia import FIA

        target_path = temp_dir / "merged.duckdb"

        result = FIA.merge_states(
            source_paths=[sample_sqlite_db, second_sqlite_db],
            target_path=target_path,
            state_codes=[37, 45],
            show_progress=False,
        )

        assert target_path.exists()
        assert isinstance(result, dict)
        assert "37" in result
        assert "45" in result


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
