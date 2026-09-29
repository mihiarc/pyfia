"""
Unit tests for the panel() function.

Tests creation of t1/t2 remeasurement panels at both condition
and tree levels for harvest analysis and change detection.
"""

import duckdb
import polars as pl
import pytest

from pyfia import FIA, panel


@pytest.fixture
def db_path(fiadb_fixture_path):
    """Path to the committed Alabama FIADB fixture."""
    return str(fiadb_fixture_path)


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
            expected_fates = {
                "survivor",
                "mortality",
                "ingrowth",
                "cut",
                "other",
                "tracked",
                "unknown",
            }
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

    @pytest.mark.parametrize("tree_type,design", [("live", "MICR"), ("al5", "SUBP")])
    def test_all_live_weights_follow_the_design(self, db_path, tree_type, design):
        """'live' reads the microplot all-live columns, 'al5' the subplot ones."""
        with FIA(db_path) as db:
            trees = panel(db, level="tree", tree_type=tree_type)
        with duckdb.connect(db_path, read_only=True) as con:
            grm = pl.from_arrow(
                con.sql(
                    "SELECT CAST(TRE_CN AS VARCHAR) AS TRE_CN, "
                    f"{design}_TPAGROW_UNADJ_AL_FOREST AS expected "
                    "FROM TREE_GRM_COMPONENT"
                ).arrow()
            )
        joined = trees.join(grm, on="TRE_CN")
        assert joined.height == trees.height > 0
        assert joined["TPAGROW_UNADJ"].equals(joined["expected"], check_names=False)


class TestPanelDomainsAndWeights:
    """Domains filter rows, and tree rows carry GRM weights (#136)."""

    def test_area_domain_filters_t2_conditions(self, db_path):
        with FIA(db_path) as db:
            everything = panel(db, level="condition")
        with FIA(db_path) as db:
            private = panel(db, level="condition", area_domain="OWNGRPCD == 40")

        expected = everything.filter(pl.col("t2_OWNGRPCD") == 40)
        assert 0 < private.height < everything.height
        assert sorted(private["PLT_CN"] + "_" + private["CONDID"].cast(str)) == sorted(
            expected["PLT_CN"] + "_" + expected["CONDID"].cast(str)
        )

    def test_area_domain_loads_referenced_columns(self, db_path):
        """A domain on a column outside the defaults still filters."""
        with FIA(db_path) as db:
            with_col = panel(db, level="condition", columns=["STDORGCD"])
        with FIA(db_path) as db:
            planted = panel(db, level="condition", area_domain="STDORGCD == 1")

        assert planted.height == with_col.filter(pl.col("STDORGCD") == 1).height > 0

    def test_tree_domain_filters_rows(self, db_path):
        with FIA(db_path) as db:
            everything = panel(db, level="tree")
        with FIA(db_path) as db:
            loblolly = panel(db, level="tree", tree_domain="SPCD == 131")

        assert loblolly["SPCD"].unique().to_list() == [131]
        assert loblolly.height == everything.filter(pl.col("SPCD") == 131).height
        assert "survivor" in loblolly["TREE_FATE"].to_list()

    def test_tree_domain_on_tree_table_column(self, db_path):
        """Columns only in TREE (TREECLCD) are joined for filtering, then dropped."""
        with FIA(db_path) as db:
            everything = panel(db, level="tree", tree_type="live")
        with FIA(db_path) as db:
            gs = panel(db, level="tree", tree_type="live", tree_domain="TREECLCD == 2")

        assert 0 < gs.height < everything.height
        assert "TREECLCD" not in gs.columns

    def test_grm_weights(self, db_path):
        with FIA(db_path) as db:
            trees = panel(db, level="tree")

        for fate in ("survivor", "mortality", "ingrowth", "cut", "diversion"):
            rows = trees.filter(pl.col("TREE_FATE") == fate)
            assert rows.height > 0
            assert rows["TPAGROW_UNADJ"].null_count() == 0, fate

        # Annual rates are TPAGROW / REMPER on their own rows
        for fate, col in (("cut", "TPAREMV_UNADJ"), ("mortality", "TPAMORT_UNADJ")):
            rows = trees.filter(pl.col("TREE_FATE") == fate)
            gap = (rows[col] * rows["REMPER"] - rows["TPAGROW_UNADJ"]).abs().max()
            assert gap < 1e-4, fate

        # TPA_UNADJ stays the removals rate
        assert trees["TPA_UNADJ"].equals(trees["TPAREMV_UNADJ"], check_names=False)

    def test_columns_carried_at_both_times(self, db_path):
        with FIA(db_path) as db:
            result = panel(db, level="condition", columns=["STDORGCD"])

        assert {"STDORGCD", "t1_STDORGCD", "t1_CONDPROP_UNADJ"} <= set(result.columns)

        with duckdb.connect(db_path, read_only=True) as con:
            t1 = con.execute(
                "SELECT CAST(PLT_CN AS VARCHAR) AS PLT_CN, CONDID, "
                "STDORGCD AS t1_STDORGCD_REF FROM COND"
            ).pl()
        checked = result.join(
            t1,
            left_on=["PREV_PLT_CN", "CONDID"],
            right_on=["PLT_CN", "CONDID"],
            how="inner",
        )
        assert checked.height > 0
        assert checked["t1_STDORGCD"].equals(
            checked["t1_STDORGCD_REF"], check_names=False
        )


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
