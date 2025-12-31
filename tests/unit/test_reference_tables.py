"""
Tests for reference table joining utilities.

These tests verify the functions that join FIA reference tables
(species, forest types, states) with estimation results.
"""

import os
from pathlib import Path

import polars as pl
import pytest

from pyfia import FIA
from pyfia.utils.reference_tables import (
    join_forest_type_names,
    join_multiple_references,
    join_species_names,
    join_state_names,
)


def _find_database() -> Path | str | None:
    """Find FIA database for tests."""
    env_path = os.getenv("PYFIA_DATABASE_PATH")
    if env_path:
        # MotherDuck connection strings don't need file existence check
        if env_path.startswith("md:") or env_path.startswith("motherduck:"):
            return env_path
        if Path(env_path).exists():
            return Path(env_path)

    paths_to_try = [
        Path.cwd() / "data" / "georgia.duckdb",
        Path.cwd() / "fia.duckdb",
    ]

    for path in paths_to_try:
        if path.exists():
            return path

    return None


@pytest.fixture(scope="module")
def fia_db():
    """Get FIA database path for tests."""
    db_path = _find_database()

    if db_path is None:
        pytest.skip(
            "No FIA database found. "
            "Set PYFIA_DATABASE_PATH or place database in data/georgia.duckdb"
        )

    return str(db_path)


class TestJoinSpeciesNames:
    """Tests for join_species_names function."""

    def test_join_species_names_basic(self, fia_db):
        """Test basic species name joining."""
        # Create sample data with species codes
        data = pl.DataFrame({
            "SPCD": [131, 316, 802],  # Loblolly pine, Red maple, White oak
            "VALUE": [100, 200, 300],
        })

        with FIA(fia_db) as db:
            result = join_species_names(data, db)

        assert "COMMON_NAME" in result.columns
        assert len(result) == 3
        # Values should be preserved
        assert result["VALUE"].to_list() == [100, 200, 300]

    def test_join_species_names_with_scientific(self, fia_db):
        """Test joining with scientific names included."""
        data = pl.DataFrame({
            "SPCD": [131, 316],
        })

        with FIA(fia_db) as db:
            result = join_species_names(data, db, include_scientific=True)

        assert "COMMON_NAME" in result.columns
        assert "SCIENTIFIC_NAME" in result.columns
        assert len(result) == 2

    def test_join_species_names_custom_columns(self, fia_db):
        """Test joining with custom column names."""
        data = pl.DataFrame({
            "SPCD": [131],
        })

        with FIA(fia_db) as db:
            result = join_species_names(
                data,
                db,
                common_name_col="SPECIES_NAME",
                include_scientific=True,
                scientific_name_col="LATIN_NAME",
            )

        assert "SPECIES_NAME" in result.columns
        assert "LATIN_NAME" in result.columns
        assert "COMMON_NAME" not in result.columns

    def test_join_species_names_missing_column(self, fia_db):
        """Test that missing SPCD column returns data unchanged."""
        data = pl.DataFrame({
            "OTHER_COL": [1, 2, 3],
        })

        with FIA(fia_db) as db:
            result = join_species_names(data, db)

        # Should return unchanged
        assert result.columns == ["OTHER_COL"]
        assert len(result) == 3

    def test_join_species_names_with_path(self, fia_db):
        """Test that passing db path string works."""
        data = pl.DataFrame({
            "SPCD": [131],
        })

        # Pass path as string instead of FIA object
        result = join_species_names(data, fia_db)

        assert "COMMON_NAME" in result.columns


class TestJoinForestTypeNames:
    """Tests for join_forest_type_names function."""

    def test_join_forest_type_names_basic(self, fia_db):
        """Test basic forest type name joining."""
        data = pl.DataFrame({
            "FORTYPCD": [161, 166, 503],  # Various forest types
            "AREA": [1000, 2000, 3000],
        })

        with FIA(fia_db) as db:
            result = join_forest_type_names(data, db)

        assert "FOREST_TYPE_NAME" in result.columns
        assert len(result) == 3
        assert result["AREA"].to_list() == [1000, 2000, 3000]

    def test_join_forest_type_names_custom_column(self, fia_db):
        """Test joining with custom output column name."""
        data = pl.DataFrame({
            "FORTYPCD": [161],
        })

        with FIA(fia_db) as db:
            result = join_forest_type_names(
                data,
                db,
                name_col="FOREST_TYPE",
            )

        assert "FOREST_TYPE" in result.columns
        assert "FOREST_TYPE_NAME" not in result.columns

    def test_join_forest_type_names_missing_column(self, fia_db):
        """Test that missing FORTYPCD column returns data unchanged."""
        data = pl.DataFrame({
            "OTHER_COL": [1, 2],
        })

        with FIA(fia_db) as db:
            result = join_forest_type_names(data, db)

        assert result.columns == ["OTHER_COL"]


def _has_ref_state_table(fia_db: str) -> bool:
    """Check if database has REF_STATE table."""
    import duckdb

    conn = duckdb.connect(fia_db, read_only=True)
    try:
        tables = conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_name = 'REF_STATE'"
        ).fetchall()
        return len(tables) > 0
    finally:
        conn.close()


class TestJoinStateNames:
    """Tests for join_state_names function.

    Note: REF_STATE table may not exist in all databases.
    The Georgia database only has REF_STATE_ELEV which lacks state names.
    """

    def test_join_state_names_basic(self, fia_db):
        """Test basic state name joining."""
        if not _has_ref_state_table(fia_db):
            pytest.skip("REF_STATE table not available in database")

        data = pl.DataFrame({
            "STATECD": [13, 12, 1],  # Georgia, Florida, Alabama
            "VALUE": [100, 200, 300],
        })

        with FIA(fia_db) as db:
            result = join_state_names(data, db)

        assert "STATE_NAME" in result.columns
        assert "STATE_ABBR" in result.columns
        assert len(result) == 3

    def test_join_state_names_no_abbr(self, fia_db):
        """Test joining without abbreviations."""
        if not _has_ref_state_table(fia_db):
            pytest.skip("REF_STATE table not available in database")

        data = pl.DataFrame({
            "STATECD": [13],
        })

        with FIA(fia_db) as db:
            result = join_state_names(data, db, include_abbr=False)

        assert "STATE_NAME" in result.columns
        assert "STATE_ABBR" not in result.columns

    def test_join_state_names_custom_columns(self, fia_db):
        """Test joining with custom column names."""
        if not _has_ref_state_table(fia_db):
            pytest.skip("REF_STATE table not available in database")

        data = pl.DataFrame({
            "STATECD": [13],
        })

        with FIA(fia_db) as db:
            result = join_state_names(
                data,
                db,
                state_name_col="STATE",
                state_abbr_col="ABBR",
            )

        assert "STATE" in result.columns
        assert "ABBR" in result.columns

    def test_join_state_names_missing_column(self, fia_db):
        """Test that missing STATECD column returns data unchanged."""
        data = pl.DataFrame({
            "OTHER_COL": [1],
        })

        with FIA(fia_db) as db:
            result = join_state_names(data, db)

        assert result.columns == ["OTHER_COL"]


class TestJoinMultipleReferences:
    """Tests for join_multiple_references function."""

    def test_join_multiple_all(self, fia_db):
        """Test joining all available reference tables."""
        has_state = _has_ref_state_table(fia_db)

        data = pl.DataFrame({
            "SPCD": [131],
            "FORTYPCD": [161],
            "STATECD": [13],
        })

        with FIA(fia_db) as db:
            result = join_multiple_references(
                data,
                db,
                species=True,
                forest_type=True,
                state=has_state,  # Only join if table exists
            )

        assert "COMMON_NAME" in result.columns
        assert "FOREST_TYPE_NAME" in result.columns
        if has_state:
            assert "STATE_NAME" in result.columns

    def test_join_multiple_species_and_forest_type(self, fia_db):
        """Test joining species and forest type tables."""
        data = pl.DataFrame({
            "SPCD": [131],
            "FORTYPCD": [161],
        })

        with FIA(fia_db) as db:
            result = join_multiple_references(
                data,
                db,
                species=True,
                forest_type=True,
                state=False,
            )

        assert "COMMON_NAME" in result.columns
        assert "FOREST_TYPE_NAME" in result.columns
        assert "STATE_NAME" not in result.columns

    def test_join_multiple_selective(self, fia_db):
        """Test joining only some reference tables."""
        data = pl.DataFrame({
            "SPCD": [131],
            "FORTYPCD": [161],
            "STATECD": [13],
        })

        with FIA(fia_db) as db:
            result = join_multiple_references(
                data,
                db,
                species=True,
                forest_type=False,
                state=False,
            )

        assert "COMMON_NAME" in result.columns
        assert "FOREST_TYPE_NAME" not in result.columns
        assert "STATE_NAME" not in result.columns

    def test_join_multiple_none(self, fia_db):
        """Test with no reference tables selected."""
        data = pl.DataFrame({
            "SPCD": [131],
        })

        with FIA(fia_db) as db:
            result = join_multiple_references(
                data,
                db,
                species=False,
                forest_type=False,
                state=False,
            )

        # Should return data unchanged
        assert result.columns == ["SPCD"]

    def test_join_multiple_missing_columns(self, fia_db):
        """Test joining when some columns are missing."""
        data = pl.DataFrame({
            "SPCD": [131],
            # No FORTYPCD or STATECD
        })

        with FIA(fia_db) as db:
            result = join_multiple_references(
                data,
                db,
                species=True,
                forest_type=True,  # Column missing, should be skipped
                state=False,
            )

        # Only species should be joined
        assert "COMMON_NAME" in result.columns
        assert "FOREST_TYPE_NAME" not in result.columns
        assert "STATE_NAME" not in result.columns
