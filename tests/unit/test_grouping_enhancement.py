"""
Tests for automatic grouping column enhancement in estimator results.

These tests verify that when users group by columns like FORTYPCD and OWNGRPCD,
descriptive name columns are automatically added to the results.
"""

import os
from pathlib import Path

import polars as pl
import pytest

from pyfia import FIA, area, volume
from pyfia.estimation.utils import _enhance_grouping_columns, format_output_columns


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

    # Fall back to MotherDuck if token available
    if os.getenv("MOTHERDUCK_TOKEN"):
        return "md:fia_ga_eval2023"

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


class TestEnhanceGroupingColumns:
    """Tests for _enhance_grouping_columns helper function."""

    def test_enhance_fortypcd(self):
        """Test that FORTYPCD gets FOREST_TYPE_GROUP added."""
        df = pl.DataFrame({
            "FORTYPCD": [161, 166, 503, 504],
            "VALUE": [100, 200, 300, 400],
        })

        result = _enhance_grouping_columns(df)

        assert "FOREST_TYPE_GROUP" in result.columns
        assert "FORTYPCD" in result.columns  # Original preserved
        assert len(result) == 4

    def test_enhance_owngrpcd(self):
        """Test that OWNGRPCD gets OWNERSHIP_GROUP added."""
        df = pl.DataFrame({
            "OWNGRPCD": [10, 20, 30, 40],
            "VALUE": [100, 200, 300, 400],
        })

        result = _enhance_grouping_columns(df)

        assert "OWNERSHIP_GROUP" in result.columns
        assert "OWNGRPCD" in result.columns  # Original preserved
        assert len(result) == 4

        # Verify correct mappings
        ownership_names = result["OWNERSHIP_GROUP"].to_list()
        assert "Forest Service" in ownership_names
        assert "Other Federal" in ownership_names
        assert "State and Local Government" in ownership_names
        assert "Private" in ownership_names

    def test_enhance_both_columns(self):
        """Test that both FORTYPCD and OWNGRPCD are enhanced."""
        df = pl.DataFrame({
            "FORTYPCD": [161, 503],
            "OWNGRPCD": [10, 40],
            "VALUE": [100, 200],
        })

        result = _enhance_grouping_columns(df)

        assert "FOREST_TYPE_GROUP" in result.columns
        assert "OWNERSHIP_GROUP" in result.columns
        assert "FORTYPCD" in result.columns
        assert "OWNGRPCD" in result.columns

    def test_no_enhancement_without_columns(self):
        """Test that no columns added when grouping columns not present."""
        df = pl.DataFrame({
            "SPCD": [131, 316],
            "VALUE": [100, 200],
        })

        result = _enhance_grouping_columns(df)

        assert "FOREST_TYPE_GROUP" not in result.columns
        assert "OWNERSHIP_GROUP" not in result.columns
        assert result.columns == ["SPCD", "VALUE"]

    def test_no_double_enhancement(self):
        """Test that columns aren't enhanced twice."""
        df = pl.DataFrame({
            "FORTYPCD": [161],
            "FOREST_TYPE_GROUP": ["Already present"],
        })

        result = _enhance_grouping_columns(df)

        # Should not add another FOREST_TYPE_GROUP
        assert result.columns.count("FOREST_TYPE_GROUP") == 1
        assert result["FOREST_TYPE_GROUP"][0] == "Already present"


class TestFormatOutputColumnsEnhancement:
    """Tests for enhancement via format_output_columns."""

    def test_format_output_enhances_fortypcd(self):
        """Test that format_output_columns enhances FORTYPCD."""
        df = pl.DataFrame({
            "FORTYPCD": [161, 503],
            "VOLUME_ACRE": [100.0, 200.0],
            "VOLUME_TOTAL": [1000.0, 2000.0],
        })

        result = format_output_columns(df, estimation_type="volume")

        assert "FOREST_TYPE_GROUP" in result.columns

    def test_format_output_enhances_owngrpcd(self):
        """Test that format_output_columns enhances OWNGRPCD."""
        df = pl.DataFrame({
            "OWNGRPCD": [10, 40],
            "AREA_TOTAL": [1000.0, 2000.0],
        })

        result = format_output_columns(df, estimation_type="area")

        assert "OWNERSHIP_GROUP" in result.columns


class TestEstimatorIntegration:
    """Integration tests for enhancement in actual estimators."""

    def test_volume_with_fortypcd(self, fia_db):
        """Test volume estimation with FORTYPCD grouping."""
        with FIA(fia_db) as db:
            db.clip_by_evalid(132301)
            result = volume(db, grp_by="FORTYPCD", totals=True)

        assert "FORTYPCD" in result.columns
        assert "FOREST_TYPE_GROUP" in result.columns
        assert len(result) > 0

        # Verify some expected forest type groups
        groups = result["FOREST_TYPE_GROUP"].unique().to_list()
        # Georgia should have pine and hardwood types
        assert any("Pine" in str(g) for g in groups if g is not None)

    def test_area_with_owngrpcd(self, fia_db):
        """Test area estimation with OWNGRPCD grouping."""
        with FIA(fia_db) as db:
            db.clip_by_evalid(132301)
            result = area(db, grp_by="OWNGRPCD", totals=True)

        assert "OWNGRPCD" in result.columns
        assert "OWNERSHIP_GROUP" in result.columns
        assert len(result) > 0

        # Verify ownership groups
        groups = result["OWNERSHIP_GROUP"].unique().to_list()
        assert "Private" in groups  # Georgia has lots of private forest

    def test_volume_with_both_groupings(self, fia_db):
        """Test volume with both FORTYPCD and OWNGRPCD."""
        with FIA(fia_db) as db:
            db.clip_by_evalid(132301)
            result = volume(db, grp_by=["FORTYPCD", "OWNGRPCD"], totals=True)

        assert "FORTYPCD" in result.columns
        assert "FOREST_TYPE_GROUP" in result.columns
        assert "OWNGRPCD" in result.columns
        assert "OWNERSHIP_GROUP" in result.columns

    def test_spcd_not_auto_enhanced(self, fia_db):
        """Test that SPCD is not auto-enhanced (requires DB access)."""
        with FIA(fia_db) as db:
            db.clip_by_evalid(132301)
            result = volume(db, by_species=True, totals=True)

        assert "SPCD" in result.columns
        # SPCD should NOT be auto-enhanced (no COMMON_NAME column)
        # Users should use join_species_names() for this
        assert "COMMON_NAME" not in result.columns

    def test_no_grp_by_no_enhancement(self, fia_db):
        """Test that without grp_by, no enhancement columns added."""
        with FIA(fia_db) as db:
            db.clip_by_evalid(132301)
            result = volume(db, totals=True)

        # No grouping columns should mean no enhancement columns
        assert "FOREST_TYPE_GROUP" not in result.columns
        assert "OWNERSHIP_GROUP" not in result.columns
