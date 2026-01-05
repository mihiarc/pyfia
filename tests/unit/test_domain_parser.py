"""Tests for DomainExpressionParser."""

import pytest
import polars as pl

from pyfia.filtering.parser import DomainExpressionParser


class TestDomainExpressionParserParse:
    """Tests for DomainExpressionParser.parse() method."""

    def test_parse_simple_comparison(self):
        """Test parsing simple comparison expressions."""
        expr = DomainExpressionParser.parse("DIA >= 10.0", "tree")
        assert expr is not None
        # Verify it's a valid Polars expression by using it
        df = pl.DataFrame({"DIA": [5.0, 10.0, 15.0]})
        result = df.filter(expr)
        assert len(result) == 2
        assert result["DIA"].to_list() == [10.0, 15.0]

    def test_parse_equality(self):
        """Test parsing equality expressions."""
        expr = DomainExpressionParser.parse("STATUSCD = 1", "tree")
        df = pl.DataFrame({"STATUSCD": [1, 2, 1, 3]})
        result = df.filter(expr)
        assert len(result) == 2

    def test_parse_double_equals(self):
        """Test parsing double equals expressions."""
        expr = DomainExpressionParser.parse("STATUSCD == 1", "tree")
        df = pl.DataFrame({"STATUSCD": [1, 2, 1, 3]})
        result = df.filter(expr)
        assert len(result) == 2

    def test_parse_and_expression(self):
        """Test parsing AND expressions."""
        expr = DomainExpressionParser.parse("DIA >= 10.0 AND STATUSCD = 1", "tree")
        df = pl.DataFrame({
            "DIA": [5.0, 10.0, 15.0, 20.0],
            "STATUSCD": [1, 1, 2, 1]
        })
        result = df.filter(expr)
        assert len(result) == 2
        assert result["DIA"].to_list() == [10.0, 20.0]

    def test_parse_or_expression(self):
        """Test parsing OR expressions."""
        expr = DomainExpressionParser.parse("DIA < 5.0 OR DIA > 15.0", "tree")
        df = pl.DataFrame({"DIA": [3.0, 10.0, 20.0]})
        result = df.filter(expr)
        assert len(result) == 2
        assert result["DIA"].to_list() == [3.0, 20.0]

    def test_parse_in_expression(self):
        """Test parsing IN expressions."""
        expr = DomainExpressionParser.parse("STATUSCD IN (1, 2)", "tree")
        df = pl.DataFrame({"STATUSCD": [1, 2, 3, 4]})
        result = df.filter(expr)
        assert len(result) == 2

    def test_parse_not_in_expression(self):
        """Test parsing NOT IN expressions."""
        expr = DomainExpressionParser.parse("STATUSCD NOT IN (1, 2)", "tree")
        df = pl.DataFrame({"STATUSCD": [1, 2, 3, 4]})
        result = df.filter(expr)
        assert len(result) == 2
        assert result["STATUSCD"].to_list() == [3, 4]

    def test_parse_between_expression(self):
        """Test parsing BETWEEN expressions."""
        expr = DomainExpressionParser.parse("DIA BETWEEN 5.0 AND 15.0", "tree")
        df = pl.DataFrame({"DIA": [3.0, 5.0, 10.0, 15.0, 20.0]})
        result = df.filter(expr)
        assert len(result) == 3

    def test_parse_is_null(self):
        """Test parsing IS NULL expressions."""
        expr = DomainExpressionParser.parse("DIA IS NULL", "tree")
        df = pl.DataFrame({"DIA": [1.0, None, 3.0, None]})
        result = df.filter(expr)
        assert len(result) == 2

    def test_parse_is_not_null(self):
        """Test parsing IS NOT NULL expressions."""
        expr = DomainExpressionParser.parse("DIA IS NOT NULL", "tree")
        df = pl.DataFrame({"DIA": [1.0, None, 3.0, None]})
        result = df.filter(expr)
        assert len(result) == 2

    def test_parse_empty_expression_raises(self):
        """Test that empty expressions raise ValueError."""
        with pytest.raises(ValueError, match="empty expression"):
            DomainExpressionParser.parse("", "tree")

    def test_parse_whitespace_only_raises(self):
        """Test that whitespace-only expressions raise ValueError."""
        with pytest.raises(ValueError, match="empty expression"):
            DomainExpressionParser.parse("   ", "tree")

    def test_parse_invalid_expression_raises(self):
        """Test that invalid expressions raise ValueError."""
        with pytest.raises(ValueError, match="Invalid tree domain expression"):
            DomainExpressionParser.parse("DIA >>= 10", "tree")

    def test_parse_domain_type_in_error_message(self):
        """Test that domain type appears in error messages."""
        # Use genuinely invalid SQL syntax (unmatched parenthesis)
        with pytest.raises(ValueError, match="Invalid area domain expression"):
            DomainExpressionParser.parse("DIA >= ((10", "area")


class TestDomainExpressionParserApplyToDataframe:
    """Tests for DomainExpressionParser.apply_to_dataframe() method."""

    def test_apply_to_dataframe(self):
        """Test applying expression to DataFrame."""
        df = pl.DataFrame({"DIA": [5.0, 10.0, 15.0]})
        result = DomainExpressionParser.apply_to_dataframe(df, "DIA >= 10.0", "tree")
        assert isinstance(result, pl.DataFrame)
        assert len(result) == 2

    def test_apply_to_lazyframe(self):
        """Test applying expression to LazyFrame."""
        lf = pl.DataFrame({"DIA": [5.0, 10.0, 15.0]}).lazy()
        result = DomainExpressionParser.apply_to_dataframe(lf, "DIA >= 10.0", "tree")
        assert isinstance(result, pl.LazyFrame)
        collected = result.collect()
        assert len(collected) == 2

    def test_apply_preserves_frame_type(self):
        """Test that apply preserves DataFrame/LazyFrame type."""
        df = pl.DataFrame({"DIA": [5.0, 10.0, 15.0]})
        lf = df.lazy()

        df_result = DomainExpressionParser.apply_to_dataframe(df, "DIA >= 10.0")
        lf_result = DomainExpressionParser.apply_to_dataframe(lf, "DIA >= 10.0")

        assert type(df_result) is pl.DataFrame
        assert type(lf_result) is pl.LazyFrame

    def test_apply_to_frame_alias(self):
        """Test that apply_to_frame is an alias for apply_to_dataframe."""
        assert DomainExpressionParser.apply_to_frame == DomainExpressionParser.apply_to_dataframe


class TestDomainExpressionParserCreateIndicator:
    """Tests for DomainExpressionParser.create_indicator() method."""

    def test_create_indicator_basic(self):
        """Test creating basic indicator column."""
        indicator_expr = DomainExpressionParser.create_indicator(
            "DIA >= 10.0", "tree", "large_tree"
        )
        df = pl.DataFrame({"DIA": [5.0, 10.0, 15.0]})
        result = df.with_columns(indicator_expr)
        assert "large_tree" in result.columns
        assert result["large_tree"].to_list() == [0, 1, 1]

    def test_create_indicator_complex_expression(self):
        """Test creating indicator with complex expression."""
        indicator_expr = DomainExpressionParser.create_indicator(
            "DIA >= 10.0 AND STATUSCD = 1", "tree", "live_large"
        )
        df = pl.DataFrame({
            "DIA": [5.0, 10.0, 15.0, 20.0],
            "STATUSCD": [1, 1, 2, 1]
        })
        result = df.with_columns(indicator_expr)
        assert result["live_large"].to_list() == [0, 1, 0, 1]

    def test_create_indicator_default_name(self):
        """Test creating indicator with default name."""
        indicator_expr = DomainExpressionParser.create_indicator("DIA >= 10.0")
        df = pl.DataFrame({"DIA": [5.0, 10.0, 15.0]})
        result = df.with_columns(indicator_expr)
        assert "indicator" in result.columns


class TestDomainExpressionParserValidateExpression:
    """Tests for DomainExpressionParser.validate_expression() method."""

    def test_validate_valid_expression(self):
        """Test validating a valid expression."""
        is_valid, error = DomainExpressionParser.validate_expression("DIA >= 10.0")
        assert is_valid is True
        assert error is None

    def test_validate_invalid_expression(self):
        """Test validating an invalid expression."""
        is_valid, error = DomainExpressionParser.validate_expression("DIA >>= 10.0")
        assert is_valid is False
        assert error is not None
        assert "Invalid" in error

    def test_validate_with_available_columns_valid(self):
        """Test validation with available columns that exist."""
        is_valid, error = DomainExpressionParser.validate_expression(
            "DIA >= 10.0", "tree", available_columns=["DIA", "HT", "STATUSCD"]
        )
        assert is_valid is True
        assert error is None

    def test_validate_with_available_columns_missing(self):
        """Test validation with missing column reference."""
        is_valid, error = DomainExpressionParser.validate_expression(
            "DIA >= 10.0", "tree", available_columns=["HT", "STATUSCD"]
        )
        assert is_valid is False
        assert "DIA" in error

    def test_validate_ignores_sql_keywords(self):
        """Test that SQL keywords are not flagged as missing columns."""
        is_valid, error = DomainExpressionParser.validate_expression(
            "DIA >= 10.0 AND STATUSCD = 1", "tree", available_columns=["DIA", "STATUSCD"]
        )
        assert is_valid is True
        assert error is None


class TestDomainExpressionParserCombineExpressions:
    """Tests for DomainExpressionParser.combine_expressions() method."""

    def test_combine_and(self):
        """Test combining expressions with AND."""
        combined = DomainExpressionParser.combine_expressions(
            ["DIA >= 10.0", "STATUSCD = 1"], "AND", "tree"
        )
        df = pl.DataFrame({
            "DIA": [5.0, 10.0, 15.0, 20.0],
            "STATUSCD": [1, 1, 2, 1]
        })
        result = df.filter(combined)
        assert len(result) == 2
        assert result["DIA"].to_list() == [10.0, 20.0]

    def test_combine_or(self):
        """Test combining expressions with OR."""
        combined = DomainExpressionParser.combine_expressions(
            ["DIA < 5.0", "DIA > 15.0"], "OR", "tree"
        )
        df = pl.DataFrame({"DIA": [3.0, 10.0, 20.0]})
        result = df.filter(combined)
        assert len(result) == 2

    def test_combine_case_insensitive_operator(self):
        """Test that operator is case insensitive."""
        combined_lower = DomainExpressionParser.combine_expressions(
            ["DIA >= 10.0", "STATUSCD = 1"], "and"
        )
        combined_upper = DomainExpressionParser.combine_expressions(
            ["DIA >= 10.0", "STATUSCD = 1"], "AND"
        )
        df = pl.DataFrame({
            "DIA": [5.0, 10.0, 15.0],
            "STATUSCD": [1, 1, 2]
        })
        assert len(df.filter(combined_lower)) == len(df.filter(combined_upper))

    def test_combine_multiple_expressions(self):
        """Test combining more than two expressions."""
        combined = DomainExpressionParser.combine_expressions(
            ["DIA >= 5.0", "DIA <= 15.0", "STATUSCD = 1"], "AND"
        )
        df = pl.DataFrame({
            "DIA": [3.0, 5.0, 10.0, 15.0, 20.0],
            "STATUSCD": [1, 1, 1, 2, 1]
        })
        result = df.filter(combined)
        assert len(result) == 2
        assert result["DIA"].to_list() == [5.0, 10.0]

    def test_combine_empty_list_raises(self):
        """Test that empty expression list raises ValueError."""
        with pytest.raises(ValueError, match="No expressions provided"):
            DomainExpressionParser.combine_expressions([])

    def test_combine_invalid_operator_raises(self):
        """Test that invalid operator raises ValueError."""
        with pytest.raises(ValueError, match="Unsupported operator"):
            DomainExpressionParser.combine_expressions(
                ["DIA >= 10.0"], "XOR"
            )

    def test_combine_single_expression(self):
        """Test combining single expression returns equivalent expression."""
        combined = DomainExpressionParser.combine_expressions(["DIA >= 10.0"])
        df = pl.DataFrame({"DIA": [5.0, 10.0, 15.0]})
        result = df.filter(combined)
        assert len(result) == 2


class TestDomainExpressionParserEdgeCases:
    """Test edge cases and special scenarios."""

    def test_expression_with_string_values(self):
        """Test expression with string comparisons."""
        expr = DomainExpressionParser.parse("SPECIES = 'oak'", "tree")
        df = pl.DataFrame({"SPECIES": ["oak", "pine", "oak", "maple"]})
        result = df.filter(expr)
        assert len(result) == 2

    def test_expression_with_negative_numbers(self):
        """Test expression with negative numbers."""
        expr = DomainExpressionParser.parse("VALUE > -10", "tree")
        df = pl.DataFrame({"VALUE": [-20, -5, 0, 10]})
        result = df.filter(expr)
        assert len(result) == 3

    def test_expression_with_decimal_numbers(self):
        """Test expression with decimal precision."""
        expr = DomainExpressionParser.parse("DIA >= 10.125", "tree")
        df = pl.DataFrame({"DIA": [10.0, 10.125, 10.5]})
        result = df.filter(expr)
        assert len(result) == 2

    def test_complex_nested_expression(self):
        """Test complex nested expressions."""
        expr = DomainExpressionParser.parse(
            "(DIA >= 10.0 AND STATUSCD = 1) OR (DIA >= 20.0 AND STATUSCD = 2)",
            "tree"
        )
        df = pl.DataFrame({
            "DIA": [5.0, 10.0, 15.0, 20.0, 25.0],
            "STATUSCD": [1, 1, 2, 2, 1]
        })
        result = df.filter(expr)
        assert len(result) == 3  # 10.0/1, 20.0/2, 25.0/1
