"""
Tests for filtering/utils/validation.py module.

This module provides centralized column validation utilities for pyFIA,
including the ColumnValidator class and convenience functions.
"""

import polars as pl
import pytest

from pyfia.filtering.utils.validation import (
    ColumnValidator,
    check_columns,
    ensure_columns,
    validate_columns,
)


class TestColumnValidatorValidateColumns:
    """Tests for ColumnValidator.validate_columns method."""

    def test_valid_columns_list(self):
        """Test validation with list of columns that exist."""
        df = pl.DataFrame({"A": [1], "B": [2], "C": [3]})
        is_valid, missing = ColumnValidator.validate_columns(
            df, required_columns=["A", "B"]
        )
        assert is_valid is True
        assert missing == []

    def test_valid_columns_string(self):
        """Test validation with single column name as string."""
        df = pl.DataFrame({"A": [1], "B": [2]})
        is_valid, missing = ColumnValidator.validate_columns(
            df, required_columns="A"
        )
        assert is_valid is True
        assert missing == []

    def test_missing_columns_raises(self):
        """Test that missing columns raises ValueError by default."""
        df = pl.DataFrame({"A": [1]})
        with pytest.raises(ValueError, match="Missing required columns"):
            ColumnValidator.validate_columns(df, required_columns=["A", "B", "C"])

    def test_missing_columns_no_raise(self):
        """Test missing columns with raise_on_missing=False."""
        df = pl.DataFrame({"A": [1]})
        is_valid, missing = ColumnValidator.validate_columns(
            df, required_columns=["A", "B", "C"], raise_on_missing=False
        )
        assert is_valid is False
        assert set(missing) == {"B", "C"}

    def test_with_context(self):
        """Test error message includes context."""
        df = pl.DataFrame({"A": [1]})
        with pytest.raises(ValueError, match="for tree volume calculation"):
            ColumnValidator.validate_columns(
                df,
                required_columns=["B"],
                context="tree volume calculation",
            )

    def test_include_available_columns(self):
        """Test error message includes available columns."""
        df = pl.DataFrame({"A": [1], "B": [2]})
        with pytest.raises(ValueError, match="Available columns:"):
            ColumnValidator.validate_columns(
                df,
                required_columns=["C"],
                include_available=True,
            )

    def test_exclude_available_columns(self):
        """Test error message excludes available columns when disabled."""
        df = pl.DataFrame({"A": [1]})
        try:
            ColumnValidator.validate_columns(
                df,
                required_columns=["B"],
                include_available=False,
            )
        except ValueError as e:
            assert "Available columns" not in str(e)

    def test_predefined_column_set(self):
        """Test validation with predefined column set."""
        df = pl.DataFrame({"CN": [1], "PLT_CN": [2], "STATUSCD": [1]})
        is_valid, missing = ColumnValidator.validate_columns(
            df, column_set="tree_basic"
        )
        assert is_valid is True
        assert missing == []

    def test_predefined_column_set_missing(self):
        """Test predefined column set with missing columns."""
        df = pl.DataFrame({"CN": [1]})
        is_valid, missing = ColumnValidator.validate_columns(
            df, column_set="tree_basic", raise_on_missing=False
        )
        assert is_valid is False
        assert "PLT_CN" in missing
        assert "STATUSCD" in missing

    def test_unknown_column_set_raises(self):
        """Test that unknown column set raises ValueError."""
        df = pl.DataFrame({"A": [1]})
        with pytest.raises(ValueError, match="Unknown column set"):
            ColumnValidator.validate_columns(df, column_set="nonexistent_set")

    def test_no_columns_specified_raises(self):
        """Test that no columns specified raises ValueError."""
        df = pl.DataFrame({"A": [1]})
        with pytest.raises(ValueError, match="Either required_columns or column_set must be specified"):
            ColumnValidator.validate_columns(df)


class TestColumnValidatorValidateOneOf:
    """Tests for ColumnValidator.validate_one_of method."""

    def test_first_column_exists(self):
        """Test when first column in group exists."""
        df = pl.DataFrame({"PLT_CN": [1], "OTHER": [2]})
        is_valid, found = ColumnValidator.validate_one_of(
            df, [["PLT_CN", "CN"]]
        )
        assert is_valid is True
        assert found == ["PLT_CN"]

    def test_second_column_exists(self):
        """Test when second column in group exists."""
        df = pl.DataFrame({"CN": [1], "OTHER": [2]})
        is_valid, found = ColumnValidator.validate_one_of(
            df, [["PLT_CN", "CN"]]
        )
        assert is_valid is True
        assert found == ["CN"]

    def test_multiple_groups_all_found(self):
        """Test multiple groups where all have at least one match."""
        df = pl.DataFrame({"PLT_CN": [1], "DIA": [10.0], "SPCD": [131]})
        is_valid, found = ColumnValidator.validate_one_of(
            df, [["PLT_CN", "CN"], ["DIA", "DIAMETER"], ["SPCD"]]
        )
        assert is_valid is True
        assert "PLT_CN" in found
        assert "DIA" in found
        assert "SPCD" in found

    def test_group_missing_raises(self):
        """Test missing group raises ValueError by default."""
        df = pl.DataFrame({"OTHER": [1]})
        with pytest.raises(ValueError, match="Missing required columns"):
            ColumnValidator.validate_one_of(df, [["PLT_CN", "CN"]])

    def test_group_missing_no_raise(self):
        """Test missing group with raise_on_missing=False."""
        df = pl.DataFrame({"OTHER": [1]})
        is_valid, found = ColumnValidator.validate_one_of(
            df, [["PLT_CN", "CN"]], raise_on_missing=False
        )
        assert is_valid is False
        assert found == []

    def test_with_context(self):
        """Test error message includes context."""
        df = pl.DataFrame({"OTHER": [1]})
        with pytest.raises(ValueError, match="for plot identification"):
            ColumnValidator.validate_one_of(
                df,
                [["PLT_CN", "CN"]],
                context="plot identification",
            )

    def test_multiple_groups_partial_match(self):
        """Test when some groups match and some don't."""
        df = pl.DataFrame({"PLT_CN": [1]})
        is_valid, found = ColumnValidator.validate_one_of(
            df,
            [["PLT_CN", "CN"], ["DIA", "DIAMETER"]],
            raise_on_missing=False,
        )
        assert is_valid is False
        assert found == ["PLT_CN"]


class TestColumnValidatorEnsureColumns:
    """Tests for ColumnValidator.ensure_columns method."""

    def test_column_exists(self):
        """Test when column already exists - no change."""
        df = pl.DataFrame({"A": [1, 2, 3]})
        result = ColumnValidator.ensure_columns(df, ["A"])
        assert result.equals(df)

    def test_add_missing_column_list(self):
        """Test adding missing column from list."""
        df = pl.DataFrame({"A": [1, 2, 3]})
        result = ColumnValidator.ensure_columns(df, ["A", "B"])
        assert "B" in result.columns
        assert result["B"].to_list() == [None, None, None]

    def test_add_missing_column_with_fill(self):
        """Test adding missing column with fill value."""
        df = pl.DataFrame({"A": [1, 2, 3]})
        result = ColumnValidator.ensure_columns(df, ["B"], fill_value=0)
        assert result["B"].to_list() == [0, 0, 0]

    def test_add_column_with_dtype(self):
        """Test adding column with specific dtype."""
        df = pl.DataFrame({"A": [1, 2, 3]})
        result = ColumnValidator.ensure_columns(
            df, {"B": pl.Boolean}, fill_value=False
        )
        assert "B" in result.columns
        assert result["B"].dtype == pl.Boolean
        assert result["B"].to_list() == [False, False, False]

    def test_add_multiple_columns_with_dict_fill(self):
        """Test adding multiple columns with different fill values."""
        df = pl.DataFrame({"A": [1, 2]})
        result = ColumnValidator.ensure_columns(
            df,
            {"B": pl.Int64, "C": pl.Utf8},
            fill_value={"B": 0, "C": "default"},
        )
        assert result["B"].to_list() == [0, 0]
        assert result["C"].to_list() == ["default", "default"]

    def test_add_column_no_dtype(self):
        """Test adding column without dtype specified."""
        df = pl.DataFrame({"A": [1, 2, 3]})
        result = ColumnValidator.ensure_columns(df, {"B": None}, fill_value="test")
        assert "B" in result.columns
        assert result["B"].to_list() == ["test", "test", "test"]

    def test_partial_columns_exist(self):
        """Test when some columns exist and some don't."""
        df = pl.DataFrame({"A": [1, 2], "B": [3, 4]})
        result = ColumnValidator.ensure_columns(df, ["A", "B", "C"], fill_value=0)
        assert result["A"].to_list() == [1, 2]  # Unchanged
        assert result["B"].to_list() == [3, 4]  # Unchanged
        assert result["C"].to_list() == [0, 0]  # Added


class TestColumnValidatorGetMissingColumns:
    """Tests for ColumnValidator.get_missing_columns method."""

    def test_no_missing_columns(self):
        """Test when all columns exist."""
        df = pl.DataFrame({"A": [1], "B": [2]})
        missing = ColumnValidator.get_missing_columns(df, required_columns=["A", "B"])
        assert missing == []

    def test_some_missing_columns(self):
        """Test when some columns are missing."""
        df = pl.DataFrame({"A": [1]})
        missing = ColumnValidator.get_missing_columns(df, required_columns=["A", "B", "C"])
        assert set(missing) == {"B", "C"}

    def test_with_column_set(self):
        """Test with predefined column set."""
        df = pl.DataFrame({"CN": [1]})
        missing = ColumnValidator.get_missing_columns(df, column_set="tree_basic")
        assert "PLT_CN" in missing
        assert "STATUSCD" in missing

    def test_with_string_column(self):
        """Test with single column name as string."""
        df = pl.DataFrame({"A": [1]})
        missing = ColumnValidator.get_missing_columns(df, required_columns="B")
        assert missing == ["B"]


class TestColumnValidatorHasColumns:
    """Tests for ColumnValidator.has_columns method."""

    def test_all_columns_present(self):
        """Test when all columns are present."""
        df = pl.DataFrame({"A": [1], "B": [2], "C": [3]})
        assert ColumnValidator.has_columns(df, required_columns=["A", "B"]) is True

    def test_columns_missing(self):
        """Test when columns are missing."""
        df = pl.DataFrame({"A": [1]})
        assert ColumnValidator.has_columns(df, required_columns=["A", "B"]) is False

    def test_with_column_set(self):
        """Test with predefined column set."""
        df = pl.DataFrame({"CN": [1], "PLT_CN": [2], "STATUSCD": [1]})
        assert ColumnValidator.has_columns(df, column_set="tree_basic") is True

    def test_with_column_set_missing(self):
        """Test with predefined column set when columns missing."""
        df = pl.DataFrame({"CN": [1]})
        assert ColumnValidator.has_columns(df, column_set="tree_basic") is False


class TestColumnValidatorPrivateMethods:
    """Tests for ColumnValidator private helper methods (via public interface)."""

    def test_get_columns_to_check_with_list(self):
        """Test _get_columns_to_check with list input."""
        df = pl.DataFrame({"A": [1], "B": [2]})
        is_valid, _ = ColumnValidator.validate_columns(
            df, required_columns=["A", "B"]
        )
        assert is_valid is True

    def test_get_columns_to_check_with_string(self):
        """Test _get_columns_to_check with string input."""
        df = pl.DataFrame({"DIA": [10.0]})
        is_valid, _ = ColumnValidator.validate_columns(
            df, required_columns="DIA"
        )
        assert is_valid is True

    def test_build_error_message_with_context(self):
        """Test _build_error_message includes context."""
        df = pl.DataFrame({"A": [1]})
        try:
            ColumnValidator.validate_columns(
                df, required_columns=["B"], context="test context"
            )
        except ValueError as e:
            assert "test context" in str(e)
            assert "B" in str(e)

    def test_build_error_message_without_context(self):
        """Test _build_error_message without context."""
        df = pl.DataFrame({"A": [1]})
        try:
            ColumnValidator.validate_columns(df, required_columns=["B"])
        except ValueError as e:
            assert "Missing required columns" in str(e)
            assert "B" in str(e)


class TestColumnSets:
    """Tests for predefined COLUMN_SETS."""

    def test_tree_basic_set(self):
        """Test tree_basic column set."""
        expected = ["CN", "PLT_CN", "STATUSCD"]
        assert ColumnValidator.COLUMN_SETS["tree_basic"] == expected

    def test_tree_diameter_set(self):
        """Test tree_diameter column set."""
        expected = ["DIA"]
        assert ColumnValidator.COLUMN_SETS["tree_diameter"] == expected

    def test_cond_basic_set(self):
        """Test cond_basic column set."""
        expected = ["PLT_CN", "CONDID", "COND_STATUS_CD"]
        assert ColumnValidator.COLUMN_SETS["cond_basic"] == expected

    def test_all_sets_are_lists(self):
        """Test all column sets are lists of strings."""
        for set_name, columns in ColumnValidator.COLUMN_SETS.items():
            assert isinstance(columns, list), f"{set_name} is not a list"
            for col in columns:
                assert isinstance(col, str), f"{set_name} contains non-string: {col}"


class TestConvenienceFunctions:
    """Tests for convenience wrapper functions."""

    def test_validate_columns_raises(self):
        """Test validate_columns convenience function raises on missing."""
        df = pl.DataFrame({"A": [1]})
        with pytest.raises(ValueError, match="Missing required columns"):
            validate_columns(df, required_columns=["B"])

    def test_validate_columns_passes(self):
        """Test validate_columns convenience function with valid columns."""
        df = pl.DataFrame({"A": [1], "B": [2]})
        # Should not raise
        validate_columns(df, required_columns=["A", "B"])

    def test_validate_columns_with_context(self):
        """Test validate_columns with context."""
        df = pl.DataFrame({"A": [1]})
        with pytest.raises(ValueError, match="for testing"):
            validate_columns(df, required_columns=["B"], context="testing")

    def test_validate_columns_with_column_set(self):
        """Test validate_columns with column set."""
        df = pl.DataFrame({"DIA": [10.0]})
        validate_columns(df, column_set="tree_diameter")

    def test_check_columns_returns_tuple(self):
        """Test check_columns returns tuple without raising."""
        df = pl.DataFrame({"A": [1]})
        is_valid, missing = check_columns(df, required_columns=["A", "B"])
        assert is_valid is False
        assert missing == ["B"]

    def test_check_columns_all_present(self):
        """Test check_columns when all columns present."""
        df = pl.DataFrame({"A": [1], "B": [2]})
        is_valid, missing = check_columns(df, required_columns=["A", "B"])
        assert is_valid is True
        assert missing == []

    def test_check_columns_with_column_set(self):
        """Test check_columns with column set."""
        df = pl.DataFrame({"SPCD": [131]})
        is_valid, missing = check_columns(df, column_set="tree_species")
        assert is_valid is True
        assert missing == []

    def test_ensure_columns_function(self):
        """Test ensure_columns convenience function."""
        df = pl.DataFrame({"A": [1, 2, 3]})
        result = ensure_columns(df, ["A", "B"], fill_value=0)
        assert "B" in result.columns
        assert result["B"].to_list() == [0, 0, 0]

    def test_ensure_columns_with_dict(self):
        """Test ensure_columns with dict of columns and types."""
        df = pl.DataFrame({"A": [1, 2]})
        result = ensure_columns(df, {"B": pl.Int64}, fill_value=99)
        assert result["B"].dtype == pl.Int64
        assert result["B"].to_list() == [99, 99]
