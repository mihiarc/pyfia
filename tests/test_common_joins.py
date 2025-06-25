"""Tests for the common_joins module."""

import pytest
import polars as pl
from pyfia.filters.joins import (
    join_tree_condition,
    join_plot_stratum,
    assign_tree_basis,
    apply_adjustment_factors,
    get_evalid_assignments,
    join_species_info,
    aggregate_tree_to_plot,
    join_plot_metadata,
)


@pytest.fixture
def sample_tree_df():
    """Create sample tree data."""
    return pl.DataFrame({
        "CN": ["1", "2", "3", "4", "5"],
        "PLT_CN": ["P1", "P1", "P2", "P2", "P3"],
        "CONDID": [1, 1, 1, 2, 1],
        "DIA": [3.5, 6.2, 12.5, 25.0, 4.8],
        "SPCD": [110, 121, 110, 202, 316],
        "TPA_UNADJ": [6.018, 1.234, 0.616, 0.308, 1.234],
    })


@pytest.fixture
def sample_cond_df():
    """Create sample condition data."""
    return pl.DataFrame({
        "PLT_CN": ["P1", "P1", "P2", "P2", "P3"],
        "CONDID": [1, 2, 1, 2, 1],
        "CONDPROP_UNADJ": [1.0, 0.0, 0.6, 0.4, 1.0],
        "COND_STATUS_CD": [1, 1, 1, 1, 2],
        "SITECLCD": [3, 7, 2, 4, 3],
        "RESERVCD": [0, 0, 0, 1, 0],
    })


@pytest.fixture
def sample_plot_df():
    """Create sample plot data."""
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


@pytest.fixture
def sample_ppsa_df():
    """Create sample POP_PLOT_STRATUM_ASSGN data."""
    return pl.DataFrame({
        "PLT_CN": ["P1", "P2", "P3"],
        "STRATUM_CN": ["S1", "S2", "S1"],
        "EVALID": [372301, 372301, 372201],
    })


@pytest.fixture
def sample_stratum_df():
    """Create sample POP_STRATUM data."""
    return pl.DataFrame({
        "CN": ["S1", "S2"],
        "EXPNS": [1234.5, 2345.6],
        "P2POINTCNT": [100, 150],
        "ADJ_FACTOR_MICR": [1.1, 1.05],
        "ADJ_FACTOR_SUBP": [1.0, 1.0],
        "ADJ_FACTOR_MACR": [0.95, 0.98],
    })


@pytest.fixture
def sample_species_df():
    """Create sample species reference data."""
    return pl.DataFrame({
        "SPCD": [110, 121, 202, 316],
        "COMMON_NAME": ["shortleaf pine", "loblolly pine", "black cherry", "red maple"],
        "GENUS": ["Pinus", "Pinus", "Prunus", "Acer"],
        "SPECIES": ["echinata", "taeda", "serotina", "rubrum"],
    })


class TestTreeConditionJoin:
    """Test tree-condition join function."""
    
    def test_basic_join(self, sample_tree_df, sample_cond_df):
        """Test basic tree-condition join."""
        result = join_tree_condition(sample_tree_df, sample_cond_df)
        
        # Check structure
        assert "CONDPROP_UNADJ" in result.columns
        assert len(result) == 5  # All trees should join
        
        # Check values preserved
        assert result["DIA"].to_list() == sample_tree_df["DIA"].to_list()
    
    def test_custom_columns(self, sample_tree_df, sample_cond_df):
        """Test join with custom column selection."""
        result = join_tree_condition(
            sample_tree_df, 
            sample_cond_df,
            cond_columns=["PLT_CN", "CONDID", "SITECLCD", "RESERVCD"]
        )
        
        assert "SITECLCD" in result.columns
        assert "RESERVCD" in result.columns
        assert "CONDPROP_UNADJ" not in result.columns


class TestPlotStratumJoin:
    """Test plot-stratum join function."""
    
    def test_default_join(self, sample_plot_df, sample_ppsa_df, sample_stratum_df):
        """Test default plot-stratum join."""
        result = join_plot_stratum(
            sample_plot_df, 
            sample_ppsa_df,
            sample_stratum_df
        )
        
        # Check structure
        assert "EXPNS" in result.columns
        assert "ADJ_FACTOR_SUBP" in result.columns
        assert "ADJ_FACTOR_MICR" not in result.columns  # Not requested by default
        assert len(result) == 3
    
    def test_all_adjustment_factors(self, sample_plot_df, sample_ppsa_df, sample_stratum_df):
        """Test join with all adjustment factors."""
        result = join_plot_stratum(
            sample_plot_df,
            sample_ppsa_df, 
            sample_stratum_df,
            adj_factors=["MICR", "SUBP", "MACR"]
        )
        
        assert "ADJ_FACTOR_MICR" in result.columns
        assert "ADJ_FACTOR_SUBP" in result.columns
        assert "ADJ_FACTOR_MACR" in result.columns


class TestTreeBasisAssignment:
    """Test tree basis assignment function."""
    
    def test_simple_assignment(self, sample_tree_df):
        """Test simple MICR/SUBP assignment."""
        result = assign_tree_basis(sample_tree_df, include_macro=False)
        
        assert "TREE_BASIS" in result.columns
        
        # Check assignments
        expected = ["MICR", "SUBP", "SUBP", "SUBP", "MICR"]
        assert result["TREE_BASIS"].to_list() == expected
    
    def test_macro_assignment(self, sample_tree_df, sample_plot_df):
        """Test full assignment including macroplot."""
        result = assign_tree_basis(sample_tree_df, sample_plot_df, include_macro=True)
        
        # Tree with DIA=25.0 on plot P2 (MACRO_BREAKPOINT=24.0) should be MACR
        tree4 = result.filter(pl.col("CN") == "4")
        assert tree4["TREE_BASIS"][0] == "MACR"
        
        # Tree on P3 with MACRO_BREAKPOINT=0 should be SUBP
        tree5 = result.filter(pl.col("CN") == "5")
        assert tree5["TREE_BASIS"][0] == "MICR"  # DIA < 5.0


class TestAdjustmentFactors:
    """Test adjustment factor application."""
    
    def test_single_column_adjustment(self, sample_tree_df):
        """Test adjusting a single value column."""
        # Add tree basis and adjustment factors
        df = sample_tree_df.with_columns([
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
    
    def test_multiple_columns(self, sample_tree_df):
        """Test adjusting multiple columns."""
        df = sample_tree_df.with_columns([
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
    
    def test_evalid_filter(self, sample_ppsa_df):
        """Test filtering by EVALID."""
        ppsa = pl.LazyFrame(sample_ppsa_df)
        
        result = get_evalid_assignments(ppsa, evalid=372301)
        assert len(result) == 2  # Only P1 and P2
        
        result = get_evalid_assignments(ppsa, evalid=[372301, 372201])
        assert len(result) == 3  # All plots
    
    def test_plot_cn_filter(self, sample_ppsa_df):
        """Test filtering by plot CNs."""
        ppsa = pl.LazyFrame(sample_ppsa_df)
        
        result = get_evalid_assignments(ppsa, plot_cns=["P1", "P3"])
        assert len(result) == 2
        assert set(result["PLT_CN"]) == {"P1", "P3"}


class TestSpeciesJoin:
    """Test species reference join."""
    
    def test_species_join(self, sample_tree_df, sample_species_df):
        """Test joining species information."""
        result = join_species_info(sample_tree_df, sample_species_df)
        
        assert "COMMON_NAME" in result.columns
        assert "GENUS" in result.columns
        
        # Check specific species - we have 110 (twice) and 121 which are both Pinus
        pine_trees = result.filter(pl.col("GENUS") == "Pinus")
        assert len(pine_trees) == 3  # Trees 1, 2, and 3 have SPCD 110 or 121
        
        # Check that unmatched species have null values
        unknown_species = sample_tree_df.with_columns(pl.lit(999).alias("SPCD"))
        result_unknown = join_species_info(unknown_species, sample_species_df)
        assert result_unknown["COMMON_NAME"].null_count() == len(unknown_species)


class TestTreeToPlotAggregation:
    """Test tree-to-plot aggregation."""
    
    def test_basic_aggregation(self, sample_tree_df):
        """Test basic tree to plot aggregation."""
        # Add tree basis
        df = assign_tree_basis(sample_tree_df, include_macro=False)
        
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
    
    def test_metadata_join(self, sample_tree_df, sample_plot_df):
        """Test joining plot metadata."""
        result = join_plot_metadata(sample_tree_df, sample_plot_df)
        
        assert "LAT" in result.columns
        assert "LON" in result.columns
        assert "INVYR" in result.columns
        
        # All trees should have metadata
        assert result["LAT"].null_count() == 0