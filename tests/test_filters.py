"""Tests for the filters module."""

import polars as pl
import pytest

from pyfia.filters.domain import (
    apply_area_filters,
    apply_growing_stock_filter,
    apply_mortality_filters,
    apply_standard_filters,
    apply_tree_filters,
    get_size_class_expr,
    parse_domain_expression,
    validate_filters,
)

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
        assert len(result) == 3
        assert all(result["DIA"] >= 10)

    def test_combined_filters(self, standard_tree_data):
        """Test combining tree type and domain filters."""
        result, _ = apply_tree_filters(
            standard_tree_data,
            tree_type="live",
            tree_domain="DIA >= 10"
        )
        assert len(result) == 2  # Live trees with DIA >= 10
        assert all(result["STATUSCD"] == 1)
        assert all(result["DIA"] >= 10)


class TestAreaFilters:
    """Test area filtering functions."""

    def test_forest_filter(self, standard_condition_data):
        """Test filtering for forest conditions."""
        result, _ = apply_area_filters(standard_condition_data, land_type="forest")
        assert len(result) == 2
        assert all(result["COND_STATUS_CD"] == 1)

    def test_timber_filter(self, standard_condition_data):
        """Test filtering for timberland conditions."""
        result, _ = apply_area_filters(standard_condition_data, land_type="timber")
        # Timberland: forest, unreserved, productive
        assert len(result) == 1
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
        with pytest.raises(ValueError):
            parse_domain_expression("INVALID_COL > 10", "TREE")


class TestUtilityFunctions:
    """Test utility filtering functions."""

    def test_standard_filters(self, standard_tree_data, standard_condition_data):
        """Test standard filter combinations."""
        tree_result, cond_result, assumptions = apply_standard_filters(
            standard_tree_data,
            standard_condition_data,
            tree_type="live",
            land_type="forest"
        )
        
        assert len(tree_result) == 4  # Live trees
        assert len(cond_result) == 2  # Forest conditions
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