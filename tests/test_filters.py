"""Tests for the filters module."""

import pytest
import polars as pl
from pyfia.filters.domain import (
    apply_tree_filters,
    apply_area_filters,
    apply_growing_stock_filter,
    apply_mortality_filters,
    apply_standard_filters,
    get_size_class_expr,
    parse_domain_expression,
    validate_filters,
)


@pytest.fixture
def sample_tree_df():
    """Create a sample tree dataframe for testing."""
    return pl.DataFrame({
        "STATUSCD": [1, 1, 2, 2, 1, 1],  # Live and dead trees
        "TREECLCD": [2, 2, 2, 1, 3, 2],  # Growing stock and other classes
        "AGENTCD": [0, 10, 0, 40, 0, 35],  # Damage codes
        "DIA": [10.5, 4.2, 15.3, 8.0, 25.0, 6.5],  # Diameter
        "SPCD": [110, 121, 110, 202, 316, 121],  # Species codes
        "COMPONENT": ["MORTALITY", "COMPONENT", "MORTALITY", "COMPONENT", "COMPONENT", "MORTALITY"],
    })


@pytest.fixture
def sample_cond_df():
    """Create a sample condition dataframe for testing."""
    return pl.DataFrame({
        "COND_STATUS_CD": [1, 1, 2, 1, 3],  # Forest and non-forest
        "SITECLCD": [3, 7, 3, 2, None],  # Site class
        "RESERVCD": [0, 0, 0, 1, 0],  # Reserved status
        "OWNGRPCD": [10, 20, 10, 30, 40],  # Ownership groups
        "FORTYPCD": [121, 401, 122, 703, 999],  # Forest type codes
    })


class TestTreeFilters:
    """Test tree filtering functions."""
    
    def test_live_trees_filter(self, sample_tree_df):
        """Test filtering for live trees."""
        result = apply_tree_filters(sample_tree_df, tree_type="live")
        assert len(result) == 4
        assert result["STATUSCD"].unique().to_list() == [1]
    
    def test_dead_trees_filter(self, sample_tree_df):
        """Test filtering for dead trees."""
        result = apply_tree_filters(sample_tree_df, tree_type="dead")
        assert len(result) == 2
        assert result["STATUSCD"].unique().to_list() == [2]
    
    def test_growing_stock_filter(self, sample_tree_df):
        """Test filtering for growing stock trees."""
        result = apply_tree_filters(sample_tree_df, tree_type="gs")
        # Should get live trees with TREECLCD=2 and AGENTCD<30
        assert len(result) == 2
        assert all(result["STATUSCD"] == 1)
        assert all(result["TREECLCD"] == 2)
        assert all(result["AGENTCD"] < 30)
    
    def test_tree_domain_filter(self, sample_tree_df):
        """Test custom tree domain filtering."""
        result = apply_tree_filters(sample_tree_df, tree_domain="DIA >= 10")
        assert len(result) == 3
        assert all(result["DIA"] >= 10)
    
    def test_combined_filters(self, sample_tree_df):
        """Test combining tree type and domain filters."""
        result = apply_tree_filters(
            sample_tree_df, 
            tree_type="live", 
            tree_domain="DIA >= 10"
        )
        assert len(result) == 2
        assert all(result["STATUSCD"] == 1)
        assert all(result["DIA"] >= 10)


class TestAreaFilters:
    """Test area filtering functions."""
    
    def test_forest_filter(self, sample_cond_df):
        """Test filtering for forest land."""
        result = apply_area_filters(sample_cond_df, land_type="forest")
        assert len(result) == 3
        assert all(result["COND_STATUS_CD"] == 1)
    
    def test_timber_filter(self, sample_cond_df):
        """Test filtering for timberland."""
        result = apply_area_filters(sample_cond_df, land_type="timber")
        # Forest + productive + unreserved
        assert len(result) == 1  # Only one row meets all criteria
        assert all(result["COND_STATUS_CD"] == 1)
        assert all(result["SITECLCD"].is_in([1, 2, 3, 4, 5, 6]))
        assert all(result["RESERVCD"] == 0)
    
    def test_area_domain_filter(self, sample_cond_df):
        """Test custom area domain filtering."""
        result = apply_area_filters(
            sample_cond_df, 
            land_type="all",  # Use all to bypass status filter
            area_domain="OWNGRPCD == 10"
        )
        assert len(result) == 2
        assert all(result["OWNGRPCD"] == 10)


class TestSpecializedFilters:
    """Test specialized filtering functions."""
    
    def test_growing_stock_types(self, sample_tree_df):
        """Test different growing stock filter types."""
        # Standard GS
        standard = apply_growing_stock_filter(sample_tree_df, gs_type="standard")
        assert len(standard) == 2
        
        # Merchantable (adds DIA >= 5.0)
        merch = apply_growing_stock_filter(sample_tree_df, gs_type="merchantable")
        assert len(merch) == 1
        assert all(merch["DIA"] >= 5.0)
        
        # Board foot (adds DIA >= 9.0)
        bf = apply_growing_stock_filter(sample_tree_df, gs_type="board_foot")
        assert len(bf) == 1
        assert all(bf["DIA"] >= 9.0)
    
    def test_mortality_filters(self, sample_tree_df):
        """Test mortality filtering."""
        # All mortality
        all_mort = apply_mortality_filters(sample_tree_df, tree_class="all")
        assert len(all_mort) == 3
        assert all(all_mort["COMPONENT"].str.contains("MORTALITY"))
        
        # Growing stock mortality
        gs_mort = apply_mortality_filters(sample_tree_df, tree_class="growing_stock")
        assert len(gs_mort) == 2
        assert all(gs_mort["TREECLCD"] == 2)
        assert all(gs_mort["AGENTCD"] < 30)


class TestUtilityFunctions:
    """Test utility functions."""
    
    def test_size_class_expression(self, sample_tree_df):
        """Test size class expression generation."""
        expr = get_size_class_expr()
        result = sample_tree_df.with_columns(expr)
        
        assert "sizeClass" in result.columns
        expected_classes = ["10.0-19.9", "1.0-4.9", "10.0-19.9", 
                           "5.0-9.9", "20.0-29.9", "5.0-9.9"]
        assert result["sizeClass"].to_list() == expected_classes
    
    def test_standard_filters(self, sample_tree_df, sample_cond_df):
        """Test applying standard filters to both dataframes."""
        tree_result, cond_result = apply_standard_filters(
            sample_tree_df,
            sample_cond_df,
            tree_type="live",
            land_type="timber"
        )
        
        assert len(tree_result) == 4  # Live trees
        assert len(cond_result) == 1   # Timberland (only 1 row meets all criteria)
    
    def test_validate_filters(self):
        """Test filter validation."""
        # Valid filters should not raise
        validate_filters(tree_type="live", land_type="forest", gs_type="standard")
        
        # Invalid filters should raise ValueError
        with pytest.raises(ValueError, match="Invalid tree_type"):
            validate_filters(tree_type="invalid")
        
        with pytest.raises(ValueError, match="Invalid land_type"):
            validate_filters(land_type="invalid")
        
        with pytest.raises(ValueError, match="Invalid gs_type"):
            validate_filters(gs_type="invalid")


class TestDomainParsing:
    """Test domain expression parsing."""
    
    def test_simple_domain(self, sample_tree_df):
        """Test simple domain expressions."""
        result = parse_domain_expression(
            sample_tree_df, 
            "DIA >= 10", 
            "tree"
        )
        assert len(result) == 3
    
    def test_complex_domain(self, sample_tree_df):
        """Test complex domain expressions."""
        result = parse_domain_expression(
            sample_tree_df,
            "SPCD == 110 and DIA > 10",
            "tree"
        )
        assert len(result) == 2  # Two trees match: DIA 10.5 and 15.3
        assert all(result["SPCD"] == 110)
        assert all(result["DIA"] > 10)
    
    def test_invalid_domain(self, sample_tree_df):
        """Test invalid domain expressions."""
        with pytest.raises(ValueError, match="Invalid tree domain expression"):
            parse_domain_expression(
                sample_tree_df,
                "INVALID SYNTAX",
                "tree"
            )