"""
Centralized column validation utilities for pyFIA.

This module provides a unified approach to column validation across the codebase,
eliminating duplication and ensuring consistent error messages.
"""

from typing import Dict, List, Optional, Union

import polars as pl


class ColumnValidator:
    """
    Centralized column validation with consistent error handling.

    This class provides a single source of truth for column validation logic,
    replacing the scattered validation patterns throughout the codebase.
    """

    # Predefined column sets for common validation scenarios
    COLUMN_SETS: Dict[str, List[str]] = {
        # Tree-related columns
        "tree_basic": ["CN", "PLT_CN", "STATUSCD"],
        "tree_diameter": ["DIA"],
        "tree_expansion": ["TPA_UNADJ"],
        "tree_species": ["SPCD"],
        "tree_biomass": ["DRYBIO_AG", "DRYBIO_BG"],
        # Condition-related columns
        "cond_basic": ["PLT_CN", "CONDID", "COND_STATUS_CD"],
        "cond_land": ["COND_STATUS_CD", "SITECLCD", "RESERVCD"],
        "cond_forest": ["FORTYPCD", "OWNGRPCD"],
        # Plot-related columns
        "plot_basic": ["CN", "STATECD", "PLOT"],
        "plot_location": ["LAT", "LON"],
        # Adjustment factor columns
        "adjustment_basic": ["DIA", "MACRO_BREAKPOINT_DIA", "EXPNS"],
        "adjustment_factors": ["ADJ_FACTOR_MICR", "ADJ_FACTOR_SUBP", "ADJ_FACTOR_MACR"],
        # Stratification columns
        "stratification": ["STRATUM_CN", "EVALID", "EXPNS"],
        # Grouping columns
        "size_grouping": ["DIA"],
        "species_grouping": ["SPCD"],
        "forest_grouping": ["FORTYPCD"],
        "ownership_grouping": ["OWNGRPCD"],
    }

    @classmethod
    def validate_columns(
        cls,
        df: pl.DataFrame,
        required_columns: Optional[Union[List[str], str]] = None,
        column_set: Optional[str] = None,
        context: Optional[str] = None,
        raise_on_missing: bool = True,
        include_available: bool = True,
    ) -> tuple[bool, List[str]]:
        """
        Validate that required columns exist in a DataFrame.

        Parameters
        ----------
        df : pl.DataFrame
            DataFrame to validate
        required_columns : List[str] or str, optional
            List of required column names or a single column name
        column_set : str, optional
            Name of a predefined column set from COLUMN_SETS
        context : str, optional
            Context for error message (e.g., "adjustment factors", "tree filtering")
        raise_on_missing : bool, default True
            Whether to raise an exception if columns are missing
        include_available : bool, default True
            Whether to include available columns in error message

        Returns
        -------
        tuple[bool, List[str]]
            (validation_passed, list_of_missing_columns)

        Raises
        ------
        ValueError
            If raise_on_missing=True and required columns are missing

        Examples
        --------
        >>> # Use predefined column set
        >>> ColumnValidator.validate_columns(df, column_set="tree_basic")

        >>> # Custom columns with context
        >>> ColumnValidator.validate_columns(
        ...     df,
        ...     required_columns=["DIA", "TPA_UNADJ"],
        ...     context="tree volume calculation"
        ... )

        >>> # Check without raising exception
        >>> is_valid, missing = ColumnValidator.validate_columns(
        ...     df,
        ...     required_columns=["SPCD"],
        ...     raise_on_missing=False
        ... )
        """
        # Determine which columns to check
        columns_to_check = cls._get_columns_to_check(required_columns, column_set)

        # Find missing columns
        missing_columns = cls._find_missing_columns(df, columns_to_check)

        # Handle validation result
        if missing_columns and raise_on_missing:
            error_msg = cls._build_error_message(
                missing_columns, context, df.columns if include_available else None
            )
            raise ValueError(error_msg)

        return len(missing_columns) == 0, missing_columns

    @classmethod
    def validate_one_of(
        cls,
        df: pl.DataFrame,
        column_groups: List[List[str]],
        context: Optional[str] = None,
        raise_on_missing: bool = True,
    ) -> tuple[bool, List[str]]:
        """
        Validate that at least one column from each group exists.

        Parameters
        ----------
        df : pl.DataFrame
            DataFrame to validate
        column_groups : List[List[str]]
            List of column groups where at least one from each group must exist
        context : str, optional
            Context for error message
        raise_on_missing : bool, default True
            Whether to raise an exception if validation fails

        Returns
        -------
        tuple[bool, List[str]]
            (validation_passed, list_of_available_columns_used)

        Examples
        --------
        >>> # Ensure we have either PLT_CN or CN for joining
        >>> ColumnValidator.validate_one_of(
        ...     df,
        ...     [["PLT_CN", "CN"]],
        ...     context="plot identification"
        ... )
        """
        available_columns = []
        missing_groups = []

        for group in column_groups:
            found = False
            for col in group:
                if col in df.columns:
                    available_columns.append(col)
                    found = True
                    break
            if not found:
                missing_groups.append(group)

        if missing_groups and raise_on_missing:
            error_msg = "Missing required columns"
            if context:
                error_msg += f" for {context}"
            error_msg += f". Need at least one from each group: {missing_groups}"
            raise ValueError(error_msg)

        return len(missing_groups) == 0, available_columns

    @classmethod
    def ensure_columns(
        cls,
        df: pl.DataFrame,
        columns: Union[List[str], Dict[str, pl.DataType]],
        fill_value=None,
        context: Optional[str] = None,
    ) -> pl.DataFrame:
        """
        Ensure columns exist in DataFrame, adding them if missing.

        Parameters
        ----------
        df : pl.DataFrame
            DataFrame to modify
        columns : List[str] or Dict[str, pl.DataType]
            Columns to ensure exist (with optional datatypes)
        fill_value : Any, default None
            Value to fill for missing columns
        context : str, optional
            Context for logging/debugging

        Returns
        -------
        pl.DataFrame
            DataFrame with all required columns

        Examples
        --------
        >>> # Ensure columns exist with default values
        >>> df = ColumnValidator.ensure_columns(
        ...     df,
        ...     {"PROCESSED": pl.Boolean, "NOTES": pl.Utf8},
        ...     fill_value={"PROCESSED": False, "NOTES": ""}
        ... )
        """
        if isinstance(columns, list):
            columns = {col: None for col in columns}  # type: ignore[misc]

        for col_name, dtype in columns.items():
            if col_name not in df.columns:
                if isinstance(fill_value, dict):
                    value = fill_value.get(col_name, None)
                else:
                    value = fill_value

                if dtype:
                    df = df.with_columns(pl.lit(value).cast(dtype).alias(col_name))
                else:
                    df = df.with_columns(pl.lit(value).alias(col_name))

        return df

    @classmethod
    def get_missing_columns(
        cls,
        df: pl.DataFrame,
        required_columns: Optional[Union[List[str], str]] = None,
        column_set: Optional[str] = None,
    ) -> List[str]:
        """
        Get list of missing columns without raising an exception.

        Parameters
        ----------
        df : pl.DataFrame
            DataFrame to check
        required_columns : List[str] or str, optional
            Required column names
        column_set : str, optional
            Name of a predefined column set

        Returns
        -------
        List[str]
            List of missing column names
        """
        columns_to_check = cls._get_columns_to_check(required_columns, column_set)
        return cls._find_missing_columns(df, columns_to_check)

    @classmethod
    def has_columns(
        cls,
        df: pl.DataFrame,
        required_columns: Optional[Union[List[str], str]] = None,
        column_set: Optional[str] = None,
    ) -> bool:
        """
        Check if DataFrame has all required columns.

        Parameters
        ----------
        df : pl.DataFrame
            DataFrame to check
        required_columns : List[str] or str, optional
            Required column names
        column_set : str, optional
            Name of a predefined column set

        Returns
        -------
        bool
            True if all columns are present
        """
        is_valid, _ = cls.validate_columns(
            df,
            required_columns=required_columns,
            column_set=column_set,
            raise_on_missing=False,
        )
        return is_valid

    # === Private Helper Methods ===

    @classmethod
    def _get_columns_to_check(
        cls,
        required_columns: Optional[Union[List[str], str]],
        column_set: Optional[str],
    ) -> List[str]:
        """Get the list of columns to check based on inputs."""
        if column_set:
            if column_set not in cls.COLUMN_SETS:
                raise ValueError(
                    f"Unknown column set: '{column_set}'. "
                    f"Available sets: {list(cls.COLUMN_SETS.keys())}"
                )
            columns_to_check = cls.COLUMN_SETS[column_set]
        elif required_columns:
            if isinstance(required_columns, str):
                columns_to_check = [required_columns]
            else:
                columns_to_check = list(required_columns)
        else:
            raise ValueError("Either required_columns or column_set must be specified")

        return columns_to_check

    @classmethod
    def _find_missing_columns(
        cls,
        df: pl.DataFrame,
        columns_to_check: List[str],
    ) -> List[str]:
        """Find which columns are missing from the DataFrame."""
        df_columns = set(df.columns)
        return [col for col in columns_to_check if col not in df_columns]

    @classmethod
    def _build_error_message(
        cls,
        missing_columns: List[str],
        context: Optional[str],
        available_columns: Optional[List[str]],
    ) -> str:
        """Build a consistent error message for missing columns."""
        error_msg = "Missing required columns"

        if context:
            error_msg += f" for {context}"

        error_msg += f": {missing_columns}"

        if available_columns:
            error_msg += f". Available columns: {available_columns}"

        return error_msg


# Convenience functions for backward compatibility and ease of use


def validate_columns(
    df: pl.DataFrame,
    required_columns: Optional[Union[List[str], str]] = None,
    column_set: Optional[str] = None,
    context: Optional[str] = None,
) -> None:
    """
    Validate columns and raise an error if any are missing.

    This is a convenience wrapper around ColumnValidator.validate_columns
    that always raises on missing columns.
    """
    ColumnValidator.validate_columns(
        df,
        required_columns=required_columns,
        column_set=column_set,
        context=context,
        raise_on_missing=True,
    )


def check_columns(
    df: pl.DataFrame,
    required_columns: Optional[Union[List[str], str]] = None,
    column_set: Optional[str] = None,
) -> tuple[bool, List[str]]:
    """
    Check if columns exist without raising an error.

    Returns
    -------
    tuple[bool, List[str]]
        (all_present, missing_columns)
    """
    return ColumnValidator.validate_columns(
        df,
        required_columns=required_columns,
        column_set=column_set,
        raise_on_missing=False,
    )


def ensure_columns(
    df: pl.DataFrame,
    columns: Union[List[str], Dict[str, pl.DataType]],
    fill_value=None,
) -> pl.DataFrame:
    """
    Ensure columns exist, adding them if necessary.

    This is a convenience wrapper around ColumnValidator.ensure_columns.
    """
    return ColumnValidator.ensure_columns(df, columns, fill_value)
