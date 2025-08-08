"""Tests for the common_joins module."""

import polars as pl
import pytest

from pyfia.filters.joins import (
    aggregate_tree_to_plot,
    apply_adjustment_factors,
    get_evalid_assignments,
    join_plot_metadata,
    join_plot_stratum,
    join_species_info,
    join_tree_condition,
)
from pyfia.filters.classification import assign_tree_basis

# Custom fixtures for specific join tests (smaller datasets)
@pytest.fixture
def joins_tree_data():
    """Create specific tree data for join testing."""
    return pl.DataFrame({
        "CN": ["1", "2", "3", "4", "5"],
        "PLT_CN": ["P1", "P1", "P2", "P2", "P3"],
        "CONDID": [1, 1, 1, 2, 1],
        "DIA": [3.5, 6.2, 12.5, 25.0, 4.8],
        "SPCD": [110, 121, 110, 202, 316],
        "TPA_UNADJ": [6.018, 1.234, 0.616, 0.308, 1.234],
    })


@pytest.fixture
def joins_condition_data():
    """Create specific condition data for join testing."""
    return pl.DataFrame({
        "PLT_CN": ["P1", "P1", "P2", "P2", "P3"],
        "CONDID": [1, 2, 1, 2, 1],
        "CONDPROP_UNADJ": [1.0, 0.0, 0.6, 0.4, 1.0],
        "COND_STATUS_CD": [1, 1, 1, 1, 2],
        "SITECLCD": [3, 7, 2, 4, 3],
        "RESERVCD": [0, 0, 0, 1, 0],
    })


@pytest.fixture
def joins_plot_data():
    """Create specific plot data for join testing."""
    return pl.DataFrame({
        "PLT_CN": ["P1", "P2", "P3"],
        "CN": ["P1", "P2", "P3"],
        "STATECD": [37, 37, 37],
        "INVYR": [2023, 2023, 2022],
        "PLOT": [1, 2, 3],
        "LAT": [35.5, 35.6, 35.7],
        "LON": [-78.5, -78.6, -78.7],
        "MACRO_BREAKPOINT_DIA": [20.0, 24.0, 0.0],
    })


class TestTreeConditionJoin:
    """Test tree-condition join function."""

    def test_basic_join(self, joins_tree_data, joins_condition_data):
        """Test basic tree-condition join."""
        result = join_tree_condition(joins_tree_data, joins_condition_data)

        # Check structure
        assert "CONDPROP_UNADJ" in result.columns
        assert len(result) == 5  # All trees should join

        # Check values preserved
        assert result["DIA"].to_list() == joins_tree_data["DIA"].to_list()

    def test_custom_columns(self, joins_tree_data, joins_condition_data):
        """Test join with custom column selection."""
        result = join_tree_condition(
            joins_tree_data,
            joins_condition_data,
            cond_columns=["PLT_CN", "CONDID", "SITECLCD", "RESERVCD"]
        )

        assert "SITECLCD" in result.columns
        assert "RESERVCD" in result.columns
        assert "CONDPROP_UNADJ" not in result.columns


class TestPlotStratumJoin:
    """Test plot-stratum join function."""

    def test_default_join(self, joins_plot_data, standard_ppsa_data, standard_stratum_data):
        """Test default plot-stratum join."""
        # Adapt PPSA PLT_CN values to match joins_plot_data (P1, P2, P3)
        ppsa_adj = standard_ppsa_data.with_columns(
            pl.col("PLT_CN").str.replace("^P00", "P")
        )
        # Keep only rows for P1..P3 to align with joins_plot_data
        ppsa_adj = ppsa_adj.filter(pl.col("PLT_CN").is_in(["P1", "P2", "P3"]))

        result = join_plot_stratum(
            joins_plot_data,
            ppsa_adj,
            standard_stratum_data
        )

        # Check structure
        assert "EXPNS" in result.columns
        assert "ADJ_FACTOR_SUBP" in result.columns
        assert "ADJ_FACTOR_MICR" not in result.columns  # Not requested by default
        assert len(result) == 3

    def test_all_adjustment_factors(self, joins_plot_data, standard_ppsa_data, standard_stratum_data):
        """Test join with all adjustment factors."""
        result = join_plot_stratum(
            joins_plot_data,
            standard_ppsa_data,
            standard_stratum_data,
            adj_factors=["MICR", "SUBP", "MACR"]
        )

        assert "ADJ_FACTOR_MICR" in result.columns
        assert "ADJ_FACTOR_SUBP" in result.columns
        assert "ADJ_FACTOR_MACR" in result.columns


class TestTreeBasisAssignment:
    """Test tree basis assignment function."""

    def test_simple_assignment(self, joins_tree_data):
        """Test simple MICR/SUBP assignment."""
        result = assign_tree_basis(joins_tree_data, include_macro=False)

        assert "TREE_BASIS" in result.columns

        # Check assignments
        expected = ["MICR", "SUBP", "SUBP", "SUBP", "MICR"]
        assert result["TREE_BASIS"].to_list() == expected

    def test_macro_assignment(self, joins_tree_data, joins_plot_data):
        """Test full assignment including macroplot."""
        result = assign_tree_basis(joins_tree_data, joins_plot_data, include_macro=True)

        # Tree with DIA=25.0 on plot P2 (MACRO_BREAKPOINT=24.0) should be MACR
        tree4 = result.filter(pl.col("CN") == "4")
        assert tree4["TREE_BASIS"][0] == "MACR"

        # Tree on P3 with MACRO_BREAKPOINT=0 should be SUBP
        tree5 = result.filter(pl.col("CN") == "5")
        assert tree5["TREE_BASIS"][0] == "MICR"  # DIA < 5.0


class TestAdjustmentFactors:
    """Test adjustment factor application."""

    def test_single_column_adjustment(self, joins_tree_data):
        """Test adjusting a single value column."""
        # Add tree basis and adjustment factors
        df = joins_tree_data.with_columns([
            pl.when(pl.col("DIA") < 5.0).then(pl.lit("MICR")).otherwise(pl.lit("SUBP")).alias("TREE_BASIS"),
            pl.lit(1.1).alias("ADJ_FACTOR_MICR"),
            pl.lit(1.0).alias("ADJ_FACTOR_SUBP"),
        ])

        result = apply_adjustment_factors(df, "TPA_UNADJ")

        assert "TPA_UNADJ_ADJ" in result.columns

        # Check MICR trees get 1.1x adjustment
        micr_trees = result.filter(pl.col("TREE_BASIS") == "MICR")
        for i in range(len(micr_trees)):
            assert micr_trees["TPA_UNADJ_ADJ"][i] == micr_trees["TPA_UNADJ"][i] * 1.1

    def test_multiple_columns(self, joins_tree_data):
        """Test adjusting multiple columns."""
        df = joins_tree_data.with_columns([
            pl.when(pl.col("DIA") < 5.0).then(pl.lit("MICR")).otherwise(pl.lit("SUBP")).alias("TREE_BASIS"),
            pl.lit(1.1).alias("ADJ_FACTOR_MICR"),
            pl.lit(1.0).alias("ADJ_FACTOR_SUBP"),
            (pl.col("TPA_UNADJ") * 2).alias("BAA_UNADJ"),  # Fake basal area
        ])

        result = apply_adjustment_factors(df, ["TPA_UNADJ", "BAA_UNADJ"])

        assert "TPA_UNADJ_ADJ" in result.columns
        assert "BAA_UNADJ_ADJ" in result.columns


class TestEvalidAssignments:
    """Test EVALID-based assignment filtering."""

    def test_evalid_filter(self, standard_ppsa_data):
        """Test filtering by EVALID."""
        # Create a small PPSA dataset tailored to expected counts
        sample_ppsa_df = pl.DataFrame({
            "PLT_CN": ["P1", "P2", "P3"],
            "STRATUM_CN": ["S1", "S1", "S2"],
            "EVALID": [372301, 372301, 372201],
        })
        ppsa = pl.LazyFrame(sample_ppsa_df)

        result = get_evalid_assignments(ppsa, evalid=372301)
        assert len(result) == 2  # Only P1 and P2

        result = get_evalid_assignments(ppsa, evalid=[372301, 372201])
        assert len(result) == 3  # All plots

    def test_plot_cn_filter(self, standard_ppsa_data):
        """Test filtering by plot CNs."""
        sample_ppsa_df = pl.DataFrame({
            "PLT_CN": ["P1", "P2", "P3"],
            "STRATUM_CN": ["S1", "S1", "S2"],
            "EVALID": [372301, 372301, 372201],
        })
        ppsa = pl.LazyFrame(sample_ppsa_df)

        result = get_evalid_assignments(ppsa, plot_cns=["P1", "P3"])
        assert len(result) == 2
        assert set(result["PLT_CN"]) == {"P1", "P3"}


class TestSpeciesJoin:
    """Test species reference join."""

    def test_species_join(self, joins_tree_data, standard_species_data):
        """Test joining species information."""
        result = join_species_info(joins_tree_data, standard_species_data)

        assert "COMMON_NAME" in result.columns
        assert "GENUS" in result.columns

        # Check specific species - we have 110 (twice) and 121 which are both Pinus
        pine_trees = result.filter(pl.col("GENUS") == "Pinus")
        assert len(pine_trees) == 3  # Trees 1, 2, and 3 have SPCD 110 or 121

        # Check that unmatched species have null values
        unknown_species = joins_tree_data.with_columns(pl.lit(999).alias("SPCD"))
        result_unknown = join_species_info(unknown_species, standard_species_data)
        assert result_unknown["COMMON_NAME"].null_count() == len(unknown_species)


class TestTreeToPlotAggregation:
    """Test tree-to-plot aggregation."""

    def test_basic_aggregation(self, joins_tree_data):
        """Test basic tree to plot aggregation."""
        # Add tree basis
        df = assign_tree_basis(joins_tree_data, include_macro=False)

        agg_cols = {
            "TPA_SUM": pl.col("TPA_UNADJ").sum(),
            "TREE_COUNT": pl.col("CN").count(),
        }

        result = aggregate_tree_to_plot(
            df,
            group_by=["PLT_CN"],
            agg_columns=agg_cols,
            adjustment_needed=True
        )

        # Should have PLT_CN and TREE_BASIS in grouping
        assert "PLT_CN" in result.columns
        assert "TREE_BASIS" in result.columns
        assert "TPA_SUM" in result.columns
        assert "TREE_COUNT" in result.columns

        # P1 should have 2 rows (MICR and SUBP trees)
        p1_data = result.filter(pl.col("PLT_CN") == "P1")
        assert len(p1_data) == 2


class TestPlotMetadataJoin:
    """Test plot metadata join."""

    def test_metadata_join(self, joins_tree_data, joins_plot_data):
        """Test joining plot metadata."""
        result = join_plot_metadata(joins_tree_data, joins_plot_data)

        assert "LAT" in result.columns
        assert "LON" in result.columns
        assert "INVYR" in result.columns

        # All trees should have metadata
        assert result["LAT"].null_count() == 0
