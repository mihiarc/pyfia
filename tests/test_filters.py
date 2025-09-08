"""Tests for the filters module."""

import polars as pl
import pytest

from pyfia.filtering.area.filters import apply_area_filters
from pyfia.filtering.tree.filters import apply_tree_filters
from pyfia.filtering.core.parser import DomainExpressionParser

# Note: Some functions may have been moved or renamed
# We'll need to update these imports as we find them

# Using standard centralized fixtures where appropriate
# Custom fixtures only for specific filter test cases


class TestTreeFilters:
    """Test tree filtering functions."""

    def test_live_trees_filter(self, standard_tree_data):
        """Test filtering for live trees."""
        result, _ = apply_tree_filters(standard_tree_data, tree_type="live")
        # Count live trees in the standard data (STATUSCD == 1)
        live_count = len(standard_tree_data.filter(pl.col("STATUSCD") == 1))
        assert len(result) == live_count
        assert result["STATUSCD"].unique().to_list() == [1]

    def test_dead_trees_filter(self, standard_tree_data):
        """Test filtering for dead trees."""
        result, _ = apply_tree_filters(standard_tree_data, tree_type="dead")
        # Count dead trees in the standard data (STATUSCD == 2)
        dead_count = len(standard_tree_data.filter(pl.col("STATUSCD") == 2))
        assert len(result) == dead_count
        assert result["STATUSCD"].unique().to_list() == [2]

    def test_growing_stock_filter(self, standard_tree_data):
        """Test filtering for growing stock trees."""
        result, _ = apply_tree_filters(standard_tree_data, tree_type="gs")
        # Growing stock: STATUSCD == 1 AND TREECLCD == 2
        gs_count = len(standard_tree_data.filter((pl.col("STATUSCD") == 1) & (pl.col("TREECLCD") == 2)))
        assert len(result) == gs_count
        assert all(result["STATUSCD"] == 1)
        assert all(result["TREECLCD"] == 2)

    def test_tree_domain_filter(self, standard_tree_data):
        """Test custom tree domain filtering."""
        result, _ = apply_tree_filters(standard_tree_data, tree_domain="DIA >= 10")
        # Trees with DIA >= 10: T001(10.5), T003(15.3), T004(12.0), T006(25.0)
        assert len(result) == 4
        assert all(result["DIA"] >= 10)

    def test_combined_filters(self, standard_tree_data):
        """Test combining tree type and domain filters."""
        result, _ = apply_tree_filters(
            standard_tree_data,
            tree_type="live",
            tree_domain="DIA >= 10"
        )
        # Live trees (STATUSCD==1) with DIA >= 10: T001(10.5), T004(12.0), T006(25.0)
        assert len(result) == 3  # Live trees with DIA >= 10
        assert all(result["STATUSCD"] == 1)
        assert all(result["DIA"] >= 10)


class TestAreaFilters:
    """Test area filtering functions."""

    def test_forest_filter(self, standard_condition_data):
        """Test filtering for forest conditions."""
        result, _ = apply_area_filters(standard_condition_data, land_type="forest")
        # First 3 conditions have COND_STATUS_CD == 1 (Forest)
        assert len(result) == 3
        assert all(result["COND_STATUS_CD"] == 1)

    def test_timber_filter(self, standard_condition_data):
        """Test filtering for timberland conditions."""
        result, _ = apply_area_filters(standard_condition_data, land_type="timber")
        # Timberland: forest (COND_STATUS_CD==1), unreserved (RESERVCD==0), productive (SITECLCD 1-6)
        # C001: forest=1, siteclcd=3, reservcd=0 ✓
        # C002: forest=1, siteclcd=3, reservcd=0 ✓
        # C003: forest=1, siteclcd=2, reservcd=0 ✓
        assert len(result) == 3
        assert all(result["COND_STATUS_CD"] == 1)
        assert all(result["RESERVCD"] == 0)
        assert all(result["SITECLCD"] >= 1)
        assert all(result["SITECLCD"] <= 6)

    def test_area_domain_filter(self, standard_condition_data):
        """Test custom area domain filtering."""
        result, _ = apply_area_filters(standard_condition_data, area_domain="OWNGRPCD == 10")
        assert len(result) == 2
        assert all(result["OWNGRPCD"] == 10)


class TestDomainParsing:
    """Test domain expression parsing."""

    def test_simple_expression(self):
        """Test parsing simple domain expressions."""
        expr = parse_domain_expression("DIA > 10", "TREE")
        assert expr is not None
        
        # Test with sample data
        df = pl.DataFrame({"DIA": [5.0, 10.0, 15.0]})
        result = df.filter(expr)
        assert len(result) == 1
        assert result["DIA"][0] == 15.0

    def test_compound_expression(self):
        """Test parsing compound domain expressions."""
        expr = parse_domain_expression("DIA > 10 AND SPCD == 110", "TREE")
        assert expr is not None
        
        # Test with sample data
        df = pl.DataFrame({
            "DIA": [5.0, 15.0, 20.0],
            "SPCD": [110, 110, 121]
        })
        result = df.filter(expr)
        assert len(result) == 1
        assert result["DIA"][0] == 15.0

    def test_invalid_expression(self):
        """Test handling of invalid expressions."""
        # Test with an expression that will actually fail parsing
        # Using empty string which is invalid for SQL expressions
        with pytest.raises(ValueError, match="Invalid"):
            parse_domain_expression("", "TREE")


class TestUtilityFunctions:
    """Test utility filtering functions."""

    def test_standard_filters(self, standard_tree_data, standard_condition_data):
        """Test standard filter combinations."""
        tree_result, cond_result, assumptions = apply_standard_filters(
            standard_tree_data,
            standard_condition_data,
            tree_type="live",
            land_type="forest",
            track_assumptions=True  # Need to explicitly enable assumption tracking
        )
        
        assert len(tree_result) == 5  # Live trees (STATUSCD==1)
        assert len(cond_result) == 3  # Forest conditions (COND_STATUS_CD==1)
        assert assumptions is not None

    def test_growing_stock_specific(self):
        """Test growing stock filter function."""
        df = pl.DataFrame({
            "STATUSCD": [1, 1, 2, 1],
            "TREECLCD": [2, 3, 2, 2],
            "DIA": [10.0, 12.0, 8.0, 5.0]
        })
        
        result = apply_growing_stock_filter(df)
        assert len(result) == 2  # Only live growing stock
        assert all(result["STATUSCD"] == 1)
        assert all(result["TREECLCD"] == 2)

    def test_mortality_filters(self):
        """Test mortality-specific filters."""
        df = pl.DataFrame({
            "COMPONENT": ["MORTALITY", "COMPONENT", "MORTALITY_ANNUAL", "GROWTH"],
            "STATUSCD": [2, 1, 2, 1],
            "DIA": [10.0, 12.0, 8.0, 5.0]
        })
        
        result = apply_mortality_filters(df)
        assert len(result) == 2  # Only mortality components
        assert all(result["COMPONENT"].str.contains("MORTALITY"))

    def test_size_class_expression(self):
        """Test size class calculation."""
        expr = get_size_class_expr()
        
        df = pl.DataFrame({
            "DIA": [4.5, 6.0, 12.0, 18.0, 25.0]
        })
        
        result = df.with_columns(SIZE_CLASS=expr)
        expected = ["Small", "Small", "Medium", "Large", "Large"]
        assert result["SIZE_CLASS"].to_list() == expected

    def test_validate_filters(self):
        """Test filter validation."""
        # Valid combinations
        validate_filters(tree_type="live", land_type="forest")
        validate_filters(tree_type="gs", land_type="timber")
        
        # Invalid tree type
        with pytest.raises(ValueError):
            validate_filters(tree_type="invalid", land_type="forest")
        
        # Invalid land type
        with pytest.raises(ValueError):
            validate_filters(tree_type="live", land_type="invalid")