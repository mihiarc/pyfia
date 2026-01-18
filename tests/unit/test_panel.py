"""
Unit tests for the panel() function.

Tests creation of t1/t2 remeasurement panels at both condition
and tree levels for harvest analysis and change detection.
"""

import pytest
import polars as pl

from pyfia import FIA, panel


@pytest.fixture
def db_path(georgia_db_path):
    """Get database path for tests."""
    return str(georgia_db_path)


class TestPanelConditionLevel:
    """Tests for condition-level panels."""

    def test_basic_condition_panel(self, db_path):
        """Test basic condition panel creation."""
        with FIA(db_path) as db:
            result = panel(db, level="condition")

            # Should return a DataFrame with expected columns
            assert isinstance(result, pl.DataFrame)
            assert len(result) > 0

            # Check required columns exist
            required_cols = ["PLT_CN", "PREV_PLT_CN", "CONDID", "REMPER", "HARVEST"]
            for col in required_cols:
                assert col in result.columns, f"Missing column: {col}"

            # Check t1/t2 columns exist
            t1_cols = [c for c in result.columns if c.startswith("t1_")]
            t2_cols = [c for c in result.columns if c.startswith("t2_")]
            assert len(t1_cols) > 0, "No t1_ columns found"
            assert len(t2_cols) > 0, "No t2_ columns found"

    def test_harvest_detection(self, db_path):
        """Test harvest indicator is calculated."""
        with FIA(db_path) as db:
            result = panel(db, level="condition")

            # HARVEST column should be binary (0 or 1)
            assert "HARVEST" in result.columns
            assert result["HARVEST"].dtype in (pl.Int8, pl.Int64, pl.UInt8)
            assert result["HARVEST"].min() >= 0
            assert result["HARVEST"].max() <= 1

    def test_harvest_only_filter(self, db_path):
        """Test harvest_only filter returns only harvested conditions."""
        with FIA(db_path) as db:
            all_result = panel(db, level="condition")
            harvest_result = panel(db, level="condition", harvest_only=True)

            # harvest_only should return fewer rows
            assert len(harvest_result) <= len(all_result)

            # All rows should have HARVEST=1
            if len(harvest_result) > 0:
                assert harvest_result["HARVEST"].min() == 1

    def test_land_type_filter(self, db_path):
        """Test land_type filter works."""
        with FIA(db_path) as db:
            forest_result = panel(db, level="condition", land_type="forest")
            timber_result = panel(db, level="condition", land_type="timber")

            # Both should return results
            assert len(forest_result) > 0
            assert len(timber_result) > 0

            # Timber should be subset of forest (more restrictive)
            assert len(timber_result) <= len(forest_result)

    def test_remper_filter(self, db_path):
        """Test remeasurement period filtering."""
        with FIA(db_path) as db:
            # Filter to 5-10 year remeasurement periods
            result = panel(db, level="condition", min_remper=5, max_remper=10)

            if len(result) > 0:
                assert result["REMPER"].min() >= 5
                assert result["REMPER"].max() <= 10


class TestPanelTreeLevel:
    """Tests for tree-level panels."""

    def test_basic_tree_panel(self, db_path):
        """Test basic tree panel creation."""
        with FIA(db_path) as db:
            result = panel(db, level="tree")

            # Should return a DataFrame with expected columns
            assert isinstance(result, pl.DataFrame)
            assert len(result) > 0

            # Check required columns exist
            # Note: GRM-based panels use TRE_CN, not PREV_TRE_CN
            required_cols = ["PLT_CN", "PREV_PLT_CN", "TRE_CN", "TREE_FATE"]
            for col in required_cols:
                assert col in result.columns, f"Missing column: {col}"

    def test_tree_fate_calculation(self, db_path):
        """Test tree fate is calculated correctly."""
        with FIA(db_path) as db:
            result = panel(db, level="tree")

            # TREE_FATE should have expected values
            assert "TREE_FATE" in result.columns
            fate_values = result["TREE_FATE"].unique().to_list()

            # Should have at least some of these categories
            expected_fates = {"survivor", "mortality", "ingrowth", "cut", "other", "tracked", "unknown"}
            assert len(set(fate_values) & expected_fates) > 0

    def test_tree_type_filter(self, db_path):
        """Test tree_type filter works."""
        with FIA(db_path) as db:
            all_result = panel(db, level="tree", tree_type="all")
            live_result = panel(db, level="tree", tree_type="live")

            # Both should return results (if data exists)
            if len(all_result) > 0:
                # live should be subset or equal
                assert len(live_result) <= len(all_result)


class TestPanelValidation:
    """Tests for input validation."""

    def test_invalid_level(self, db_path):
        """Test invalid level raises error."""
        with FIA(db_path) as db:
            with pytest.raises(ValueError, match="Invalid level"):
                panel(db, level="invalid")

    def test_invalid_land_type(self, db_path):
        """Test invalid land_type raises error."""
        with FIA(db_path) as db:
            with pytest.raises(ValueError, match="Invalid land_type"):
                panel(db, level="condition", land_type="invalid")

    def test_invalid_tree_type(self, db_path):
        """Test invalid tree_type raises error."""
        with FIA(db_path) as db:
            with pytest.raises(ValueError, match="Invalid tree_type"):
                panel(db, level="tree", tree_type="invalid")

    def test_negative_min_remper(self, db_path):
        """Test negative min_remper raises error."""
        with FIA(db_path) as db:
            with pytest.raises(ValueError, match="min_remper must be non-negative"):
                panel(db, level="condition", min_remper=-1)

    def test_max_remper_less_than_min(self, db_path):
        """Test max_remper < min_remper raises error."""
        with FIA(db_path) as db:
            with pytest.raises(ValueError, match="max_remper.*must be >= min_remper"):
                panel(db, level="condition", min_remper=10, max_remper=5)

    def test_negative_min_invyr(self, db_path):
        """Test negative min_invyr raises error."""
        with FIA(db_path) as db:
            with pytest.raises(ValueError, match="min_invyr must be non-negative"):
                panel(db, level="condition", min_invyr=-1)


class TestPanelOutput:
    """Tests for output format."""

    def test_condition_column_ordering(self, db_path):
        """Test condition panel has logical column ordering."""
        with FIA(db_path) as db:
            result = panel(db, level="condition")

            # Priority columns should come first
            priority = ["PLT_CN", "PREV_PLT_CN", "CONDID", "STATECD"]
            for i, col in enumerate(priority):
                if col in result.columns:
                    assert result.columns.index(col) == i

    def test_t1_t2_interleaving(self, db_path):
        """Test t1/t2 columns are interleaved for easy comparison."""
        with FIA(db_path) as db:
            result = panel(db, level="condition")

            # Find pairs of t1/t2 columns
            t1_cols = [c for c in result.columns if c.startswith("t1_")]

            # For each t1 column, check if corresponding t2 column follows
            for t1_col in t1_cols:
                base = t1_col.replace("t1_", "")
                t2_col = f"t2_{base}"
                if t2_col in result.columns:
                    t1_idx = result.columns.index(t1_col)
                    t2_idx = result.columns.index(t2_col)
                    # t2 should immediately follow t1
                    assert t2_idx == t1_idx + 1, f"{t1_col} not followed by {t2_col}"
