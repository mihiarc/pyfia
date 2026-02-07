"""
Utility functions for FIA data filtering, validation, classification, and grouping.

This module consolidates all utility functionality including:
- Column validation
- Tree and condition classification
- Grouping functions for analysis
"""

from __future__ import annotations

from typing import Literal

import polars as pl

from ..constants.plot_design import (
    DESCRIPTIVE_SIZE_CLASSES,
    STANDARD_SIZE_CLASSES,
    DiameterBreakpoints,
    PlotBasis,
)
from ..constants.status_codes import (
    LandStatus,
    ReserveStatus,
    SiteClass,
)

# =============================================================================
# Column Validation
# =============================================================================


class ColumnValidator:
    """
    Centralized column validation with consistent error handling.

    This class provides a single source of truth for column validation logic,
    replacing the scattered validation patterns throughout the codebase.
    """

    # Predefined column sets for common validation scenarios
    COLUMN_SETS: dict[str, list[str]] = {
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
        required_columns: list[str] | str | None = None,
        column_set: str | None = None,
        context: str | None = None,
        raise_on_missing: bool = True,
        include_available: bool = True,
    ) -> tuple[bool, list[str]]:
        """
        Validate that required columns exist in a DataFrame.

        Parameters
        ----------
        df : pl.DataFrame
            DataFrame to validate
        required_columns : list[str] or str, optional
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
        tuple[bool, list[str]]
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
        column_groups: list[list[str]],
        context: str | None = None,
        raise_on_missing: bool = True,
    ) -> tuple[bool, list[str]]:
        """
        Validate that at least one column from each group exists.

        Parameters
        ----------
        df : pl.DataFrame
            DataFrame to validate
        column_groups : list[list[str]]
            List of column groups where at least one from each group must exist
        context : str, optional
            Context for error message
        raise_on_missing : bool, default True
            Whether to raise an exception if validation fails

        Returns
        -------
        tuple[bool, list[str]]
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
        columns: list[str] | dict[str, pl.DataType | None],
        fill_value=None,
        context: str | None = None,
    ) -> pl.DataFrame:
        """
        Ensure columns exist in DataFrame, adding them if missing.

        Parameters
        ----------
        df : pl.DataFrame
            DataFrame to modify
        columns : list[str] or dict[str, pl.DataType]
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
            columns = {col: None for col in columns}

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
        required_columns: list[str] | str | None = None,
        column_set: str | None = None,
    ) -> list[str]:
        """
        Get list of missing columns without raising an exception.

        Parameters
        ----------
        df : pl.DataFrame
            DataFrame to check
        required_columns : list[str] or str, optional
            Required column names
        column_set : str, optional
            Name of a predefined column set

        Returns
        -------
        list[str]
            List of missing column names
        """
        columns_to_check = cls._get_columns_to_check(required_columns, column_set)
        return cls._find_missing_columns(df, columns_to_check)

    @classmethod
    def has_columns(
        cls,
        df: pl.DataFrame,
        required_columns: list[str] | str | None = None,
        column_set: str | None = None,
    ) -> bool:
        """
        Check if DataFrame has all required columns.

        Parameters
        ----------
        df : pl.DataFrame
            DataFrame to check
        required_columns : list[str] or str, optional
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
        required_columns: list[str] | str | None,
        column_set: str | None,
    ) -> list[str]:
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
        columns_to_check: list[str],
    ) -> list[str]:
        """Find which columns are missing from the DataFrame."""
        df_columns = set(df.columns)
        return [col for col in columns_to_check if col not in df_columns]

    @classmethod
    def _build_error_message(
        cls,
        missing_columns: list[str],
        context: str | None,
        available_columns: list[str] | None,
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
    required_columns: list[str] | str | None = None,
    column_set: str | None = None,
    context: str | None = None,
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
    required_columns: list[str] | str | None = None,
    column_set: str | None = None,
) -> tuple[bool, list[str]]:
    """
    Check if columns exist without raising an error.

    Returns
    -------
    tuple[bool, list[str]]
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
    columns: list[str] | dict[str, pl.DataType | None],
    fill_value=None,
) -> pl.DataFrame:
    """
    Ensure columns exist, adding them if necessary.

    This is a convenience wrapper around ColumnValidator.ensure_columns.
    """
    return ColumnValidator.ensure_columns(df, columns, fill_value)


# =============================================================================
# Classification Functions
# =============================================================================


def assign_tree_basis(
    tree_df: pl.DataFrame,
    plot_df: pl.DataFrame | None = None,
    include_macro: bool = True,
    dia_column: str = "DIA",
    macro_breakpoint_column: str = "MACRO_BREAKPOINT_DIA",
    output_column: str = "TREE_BASIS",
) -> pl.DataFrame:
    """
    Assign TREE_BASIS based on tree diameter and plot design.

    Trees are assigned to measurement plots based on their diameter:
    - MICR: Trees 1.0-4.9" DBH (microplot)
    - SUBP: Trees 5.0"+ DBH (subplot)
    - MACR: Large trees based on MACRO_BREAKPOINT_DIA (macroplot)

    Parameters
    ----------
    tree_df : pl.DataFrame
        Tree dataframe with DIA column
    plot_df : pl.DataFrame, optional
        Plot dataframe with MACRO_BREAKPOINT_DIA. Required if include_macro=True
    include_macro : bool, default True
        Whether to check for macroplot assignment
    dia_column : str, default "DIA"
        Column containing tree diameter
    macro_breakpoint_column : str, default "MACRO_BREAKPOINT_DIA"
        Column containing macroplot breakpoint diameter
    output_column : str, default "TREE_BASIS"
        Name for output column

    Returns
    -------
    pl.DataFrame
        Tree dataframe with TREE_BASIS column added

    Examples
    --------
    >>> # Basic tree basis assignment
    >>> trees_with_basis = assign_tree_basis(trees, plots)

    >>> # Simplified assignment (no macroplot)
    >>> trees_simple = assign_tree_basis(trees, include_macro=False)
    """
    if include_macro and plot_df is not None:
        # Join with plot to get MACRO_BREAKPOINT_DIA if not already present
        if macro_breakpoint_column not in tree_df.columns:
            # Support plot tables that expose plot key as either PLT_CN or CN
            right_key = "PLT_CN" if "PLT_CN" in plot_df.columns else "CN"
            tree_df = tree_df.join(
                plot_df.select([right_key, macro_breakpoint_column]),
                left_on="PLT_CN",
                right_on=right_key,
                how="left",
            )

        # Full tree basis assignment with macroplot logic
        tree_basis_expr = (
            pl.when(pl.col(dia_column).is_null())
            .then(None)
            .when(pl.col(dia_column) < DiameterBreakpoints.MICROPLOT_MAX_DIA)
            .then(pl.lit(PlotBasis.MICROPLOT))
            .when(pl.col(macro_breakpoint_column) <= 0)
            .then(pl.lit(PlotBasis.SUBPLOT))
            .when(pl.col(macro_breakpoint_column).is_null())
            .then(pl.lit(PlotBasis.SUBPLOT))
            .when(pl.col(dia_column) < pl.col(macro_breakpoint_column))
            .then(pl.lit(PlotBasis.SUBPLOT))
            .otherwise(pl.lit(PlotBasis.MACROPLOT))
            .alias(output_column)
        )
    else:
        # Simplified assignment (just MICR/SUBP)
        tree_basis_expr = (
            pl.when(pl.col(dia_column) < DiameterBreakpoints.MICROPLOT_MAX_DIA)
            .then(pl.lit(PlotBasis.MICROPLOT))
            .otherwise(pl.lit(PlotBasis.SUBPLOT))
            .alias(output_column)
        )

    return tree_df.with_columns(tree_basis_expr)


def assign_size_class(
    tree_df: pl.DataFrame,
    dia_column: str = "DIA",
    output_column: str = "SIZE_CLASS",
    class_system: str = "standard",
) -> pl.DataFrame:
    """
    Assign size class based on tree diameter.

    Parameters
    ----------
    tree_df : pl.DataFrame
        Tree dataframe with diameter column
    dia_column : str, default "DIA"
        Column containing tree diameter
    output_column : str, default "SIZE_CLASS"
        Name for output column
    class_system : str, default "standard"
        Size class system to use:
        - "standard": Saplings (<5"), Small (5-9.9"), Medium (10-19.9"), Large (20"+)
        - "detailed": More granular classes
        - "simple": Small (<10"), Large (10"+)

    Returns
    -------
    pl.DataFrame
        Tree dataframe with size class column added

    Examples
    --------
    >>> # Standard size classes
    >>> trees_with_size = assign_size_class(trees)

    >>> # Simple size classes
    >>> trees_simple = assign_size_class(trees, class_system="simple")
    """
    if class_system == "standard":
        size_expr = (
            pl.when(pl.col(dia_column) < 5.0)
            .then(pl.lit("Saplings"))
            .when(pl.col(dia_column) < 10.0)
            .then(pl.lit("Small"))
            .when(pl.col(dia_column) < 20.0)
            .then(pl.lit("Medium"))
            .otherwise(pl.lit("Large"))
            .alias(output_column)
        )
    elif class_system == "detailed":
        size_expr = (
            pl.when(pl.col(dia_column) < 1.0)
            .then(pl.lit("Seedlings"))
            .when(pl.col(dia_column) < 5.0)
            .then(pl.lit("Saplings"))
            .when(pl.col(dia_column) < 10.0)
            .then(pl.lit("Small"))
            .when(pl.col(dia_column) < 15.0)
            .then(pl.lit("Medium"))
            .when(pl.col(dia_column) < 25.0)
            .then(pl.lit("Large"))
            .otherwise(pl.lit("Very Large"))
            .alias(output_column)
        )
    elif class_system == "simple":
        size_expr = (
            pl.when(pl.col(dia_column) < 10.0)
            .then(pl.lit("Small"))
            .otherwise(pl.lit("Large"))
            .alias(output_column)
        )
    else:
        raise ValueError(f"Unknown class_system: {class_system}")

    return tree_df.with_columns(size_expr)


def assign_forest_type_group(
    cond_df: pl.DataFrame,
    fortypcd_column: str = "FORTYPCD",
    output_column: str = "FOREST_TYPE_GROUP",
) -> pl.DataFrame:
    """
    Assign forest type groups based on forest type codes.

    .. deprecated::
        Use `add_forest_type_group` instead for more accurate
        western forest type handling.

    Groups forest types into major categories following FIA classification.

    Parameters
    ----------
    cond_df : pl.DataFrame
        Condition dataframe with forest type codes
    fortypcd_column : str, default "FORTYPCD"
        Column containing forest type codes
    output_column : str, default "FOREST_TYPE_GROUP"
        Name for output column

    Returns
    -------
    pl.DataFrame
        Condition dataframe with forest type group column added

    Examples
    --------
    >>> # Add forest type groups
    >>> conds_with_groups = assign_forest_type_group(conditions)
    """
    import warnings

    warnings.warn(
        "assign_forest_type_group is deprecated. Use add_forest_type_group instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    return add_forest_type_group(
        cond_df,
        fortypcd_col=fortypcd_column,
        output_col=output_column,
    )


def assign_species_group(
    tree_df: pl.DataFrame,
    species_df: pl.DataFrame,
    spcd_column: str = "SPCD",
    grouping_system: str = "major_species",
    output_column: str = "SPECIES_GROUP",
) -> pl.DataFrame:
    """
    Assign species groups based on species codes.

    Parameters
    ----------
    tree_df : pl.DataFrame
        Tree dataframe with species codes
    species_df : pl.DataFrame
        Species reference dataframe
    spcd_column : str, default "SPCD"
        Column containing species codes
    grouping_system : str, default "major_species"
        Grouping system to use:
        - "major_species": Major commercial species groups
        - "genus": Group by genus
        - "family": Group by family
    output_column : str, default "SPECIES_GROUP"
        Name for output column

    Returns
    -------
    pl.DataFrame
        Tree dataframe with species group column added
    """
    if grouping_system == "major_species":
        # Create major species groups based on common FIA groupings
        species_groups = species_df.with_columns(
            pl.when(pl.col("SPCD").is_in([131, 132, 133]))  # Pines
            .then(pl.lit("Southern Pines"))
            .when(pl.col("SPCD").is_in([316, 318, 319]))  # Maples
            .then(pl.lit("Maples"))
            .when(pl.col("SPCD").is_in([800, 801, 802, 803, 804]))  # Oaks
            .then(pl.lit("Oaks"))
            .when(pl.col("GENUS") == "Quercus")
            .then(pl.lit("Oaks"))
            .when(pl.col("GENUS") == "Pinus")
            .then(pl.lit("Pines"))
            .when(pl.col("GENUS") == "Acer")
            .then(pl.lit("Maples"))
            .otherwise(pl.col("GENUS"))
            .alias(output_column)
        )
    elif grouping_system == "genus":
        species_groups = species_df.select(
            [spcd_column, pl.col("GENUS").alias(output_column)]
        )
    elif grouping_system == "family":
        species_groups = species_df.select(
            [spcd_column, pl.col("FAMILY").alias(output_column)]
        )
    else:
        raise ValueError(f"Unknown grouping_system: {grouping_system}")

    return tree_df.join(
        species_groups.select([spcd_column, output_column]), on=spcd_column, how="left"
    )


def validate_classification_columns(
    df: pl.DataFrame,
    classification_type: str,
    required_columns: list[str] | None = None,
) -> bool:
    """
    Validate that DataFrame has required columns for classification operations.

    Parameters
    ----------
    df : pl.DataFrame
        DataFrame to validate
    classification_type : str
        Type of classification: "tree_basis", "size_class", "prop_basis", etc.
    required_columns : list[str], optional
        List of required columns. If None, uses defaults for classification type.

    Returns
    -------
    bool
        True if all required columns are present

    Raises
    ------
    ValueError
        If required columns are missing
    """
    if required_columns is None:
        if classification_type == "tree_basis":
            required_columns = ["DIA"]
        elif classification_type == "size_class":
            required_columns = ["DIA"]
        elif classification_type == "prop_basis":
            required_columns = ["MACRO_BREAKPOINT_DIA"]
        elif classification_type == "forest_type":
            required_columns = ["FORTYPCD"]
        elif classification_type == "land_use":
            required_columns = ["COND_STATUS_CD", "RESERVCD"]
        elif classification_type == "species_group":
            required_columns = ["SPCD"]
        else:
            raise ValueError(f"Unknown classification_type: {classification_type}")

    missing_columns = [col for col in required_columns if col not in df.columns]

    if missing_columns:
        raise ValueError(
            f"Missing required columns for {classification_type}: {missing_columns}"
        )

    return True


# =============================================================================
# Grouping Functions
# =============================================================================


def setup_grouping_columns(
    df: pl.DataFrame,
    grp_by: str | list[str] | None = None,
    by_species: bool = False,
    by_size_class: bool = False,
    by_land_type: bool = False,
    size_class_type: Literal["standard", "descriptive"] = "standard",
    dia_col: str = "DIA",
) -> tuple[pl.DataFrame, list[str]]:
    """
    Set up grouping columns for FIA estimation.

    This function prepares the dataframe with necessary grouping columns
    and returns both the modified dataframe and the list of columns to group by.

    Parameters
    ----------
    df : pl.DataFrame
        Input dataframe
    grp_by : str or list[str], optional
        Custom column(s) to group by
    by_species : bool, default False
        Whether to group by species (SPCD)
    by_size_class : bool, default False
        Whether to group by diameter size class
    by_land_type : bool, default False
        Whether to group by land type (for area estimation)
    size_class_type : {"standard", "descriptive"}, default "standard"
        Type of size class labels to use
    dia_col : str, default "DIA"
        Name of diameter column to use for size classes

    Returns
    -------
    tuple[pl.DataFrame, list[str]]
        Modified dataframe with grouping columns added, and list of column names to group by
    """
    group_cols = []

    # Handle custom grouping columns
    if grp_by is not None:
        if isinstance(grp_by, str):
            group_cols = [grp_by]
        else:
            group_cols = list(grp_by)

    # Add species grouping
    if by_species:
        ColumnValidator.validate_columns(
            df,
            required_columns="SPCD",
            context="species grouping",
            raise_on_missing=True,
        )
        group_cols.append("SPCD")

    # Add size class grouping
    if by_size_class:
        ColumnValidator.validate_columns(
            df,
            required_columns=dia_col,
            context="size class grouping",
            raise_on_missing=True,
        )

        # Add size class column (standardize to UPPER_SNAKE_CASE)
        size_class_expr = create_size_class_expr(dia_col, size_class_type)
        df = df.with_columns(size_class_expr)
        group_cols.append("SIZE_CLASS")

    # Add land type grouping (for area estimation)
    if by_land_type:
        ColumnValidator.validate_columns(
            df,
            required_columns="LAND_TYPE",
            context="land type grouping (run add_land_type_column() first)",
            raise_on_missing=True,
        )
        group_cols.append("LAND_TYPE")

    # Remove duplicates while preserving order
    group_cols = list(dict.fromkeys(group_cols))

    return df, group_cols


def create_size_class_expr(
    dia_col: str = "DIA",
    size_class_type: Literal["standard", "descriptive", "market"] = "standard",
    spcd_col: str = "SPCD",
) -> pl.Expr:
    """
    Create a Polars expression for diameter size classes.

    Parameters
    ----------
    dia_col : str, default "DIA"
        Name of diameter column
    size_class_type : {"standard", "descriptive", "market"}, default "standard"
        Type of size class labels to use:
        - "standard": Numeric ranges (1.0-4.9, 5.0-9.9, etc.)
        - "descriptive": Text labels (Saplings, Small, etc.)
        - "market": Timber market categories (Pulpwood, Chip-n-Saw, Sawtimber)
    spcd_col : str, default "SPCD"
        Name of species code column. Used for "market" type to differentiate
        pine/softwood (SPCD < 300) from hardwood (SPCD >= 300).

    Returns
    -------
    pl.Expr
        Expression that creates 'SIZE_CLASS' column based on diameter

    Notes
    -----
    Market size classes are based on TimberMart-South categories:

    **Pre-merchantable (all species):**
    - Pre-merchantable: 1.0" to 4.9" DBH

    **Pine/Softwood (SPCD < 300):**
    - Pulpwood: 5.0" to 8.9" DBH
    - Chip-n-Saw: 9.0" to 11.9" DBH
    - Sawtimber: 12.0"+ DBH

    **Hardwood (SPCD >= 300):**
    - Pulpwood: 5.0" to 10.9" DBH
    - Sawtimber: 11.0"+ DBH (no Chip-n-Saw category)

    To include pre-merchantable trees in mortality estimation, use
    ``tree_type="live"`` or ``tree_type="al"`` (all live trees) instead
    of the default ``tree_type="gs"`` (growing stock, which excludes < 5" DBH).
    """
    if size_class_type == "standard":
        return (
            pl.when(pl.col(dia_col) < DiameterBreakpoints.MICROPLOT_MAX_DIA)
            .then(pl.lit("1.0-4.9"))
            .when(pl.col(dia_col) < 10.0)
            .then(pl.lit("5.0-9.9"))
            .when(pl.col(dia_col) < 20.0)
            .then(pl.lit("10.0-19.9"))
            .when(pl.col(dia_col) < 30.0)
            .then(pl.lit("20.0-29.9"))
            .otherwise(pl.lit("30.0+"))
            .alias("SIZE_CLASS")
        )
    elif size_class_type == "descriptive":
        return (
            pl.when(pl.col(dia_col) < DiameterBreakpoints.MICROPLOT_MAX_DIA)
            .then(pl.lit("Saplings"))
            .when(pl.col(dia_col) < 10.0)
            .then(pl.lit("Small"))
            .when(pl.col(dia_col) < 20.0)
            .then(pl.lit("Medium"))
            .otherwise(pl.lit("Large"))
            .alias("SIZE_CLASS")
        )
    elif size_class_type == "market":
        # Species-aware timber market size classes with pre-merchantable support
        # Pre-merchantable: < 5.0" DBH (both softwood and hardwood)
        # Pine/Softwood (SPCD < 300): Pulpwood (5-8.9"), Chip-n-Saw (9-11.9"), Sawtimber (12"+)
        # Hardwood (SPCD >= 300): Pulpwood (5-10.9"), Sawtimber (11"+) (no CNS)
        return (
            pl.when(pl.col(dia_col) < 5.0)
            .then(pl.lit("Pre-merchantable"))
            .when(pl.col(spcd_col) < 300)  # Pine/Softwood
            .then(
                pl.when(pl.col(dia_col) < 9.0)
                .then(pl.lit("Pulpwood"))
                .when(pl.col(dia_col) < 12.0)
                .then(pl.lit("Chip-n-Saw"))
                .otherwise(pl.lit("Sawtimber"))
            )
            .otherwise(  # Hardwood
                pl.when(pl.col(dia_col) < 11.0)
                .then(pl.lit("Pulpwood"))
                .otherwise(pl.lit("Sawtimber"))
            )
            .alias("SIZE_CLASS")
        )
    else:
        raise ValueError(f"Invalid size_class_type: {size_class_type}")


def add_land_type_column(df: pl.DataFrame) -> pl.DataFrame:
    """
    Add land type category column for area estimation grouping.

    Creates a 'LAND_TYPE' column based on COND_STATUS_CD and other attributes.

    Parameters
    ----------
    df : pl.DataFrame
        Condition dataframe with COND_STATUS_CD, SITECLCD, and RESERVCD columns

    Returns
    -------
    pl.DataFrame
        Dataframe with 'LAND_TYPE' column added
    """
    required_cols = ["COND_STATUS_CD", "SITECLCD", "RESERVCD"]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    land_type_expr = (
        pl.when(pl.col("COND_STATUS_CD") != LandStatus.FOREST)
        .then(
            pl.when(pl.col("COND_STATUS_CD") == LandStatus.NONFOREST)
            .then(pl.lit("Non-forest"))
            .when(pl.col("COND_STATUS_CD") == LandStatus.WATER)
            .then(pl.lit("Water"))
            .otherwise(pl.lit("Other"))
        )
        .otherwise(
            # Forest land - check if timber
            pl.when(
                (pl.col("SITECLCD").is_in(SiteClass.PRODUCTIVE_CLASSES))
                & (pl.col("RESERVCD") == ReserveStatus.NOT_RESERVED)
            )
            .then(pl.lit("Timber"))
            .otherwise(pl.lit("Non-timber forest"))
        )
        .alias("LAND_TYPE")
    )

    return df.with_columns(land_type_expr)


def prepare_plot_groups(
    base_groups: list[str],
    additional_groups: list[str] | None = None,
    always_include: list[str] | None = None,
) -> list[str]:
    """
    Prepare final grouping columns for plot-level aggregation.

    This function combines base grouping columns with additional groups
    and ensures certain columns are always included (like PLT_CN).

    Parameters
    ----------
    base_groups : list[str]
        Base grouping columns from setup_grouping_columns
    additional_groups : list[str], optional
        Additional columns to include in grouping
    always_include : list[str], optional
        Columns that should always be included (default: ["PLT_CN"])

    Returns
    -------
    list[str]
        Final list of grouping columns
    """
    if always_include is None:
        always_include = ["PLT_CN"]

    # Start with always_include columns
    final_groups = list(always_include)

    # Add base groups
    final_groups.extend(base_groups)

    # Add additional groups if provided
    if additional_groups:
        final_groups.extend(additional_groups)

    # Remove duplicates while preserving order
    final_groups = list(dict.fromkeys(final_groups))

    return final_groups


def add_species_info(
    df: pl.DataFrame,
    species_df: pl.DataFrame | None = None,
    include_common_name: bool = True,
    include_genus: bool = False,
) -> pl.DataFrame:
    """
    Add species information for grouping and display.

    Parameters
    ----------
    df : pl.DataFrame
        Dataframe with SPCD column
    species_df : pl.DataFrame, optional
        REF_SPECIES dataframe. If None, only SPCD is used
    include_common_name : bool, default True
        Whether to include COMMON_NAME column
    include_genus : bool, default False
        Whether to include GENUS column

    Returns
    -------
    pl.DataFrame
        Dataframe with species information added
    """
    if "SPCD" not in df.columns:
        raise ValueError("SPCD column not found in dataframe")

    if species_df is None:
        return df

    # Select columns to join
    join_cols = ["SPCD"]
    if include_common_name:
        join_cols.append("COMMON_NAME")
    if include_genus:
        join_cols.append("GENUS")

    # Join species info
    return df.join(
        species_df.select(join_cols),
        on="SPCD",
        how="left",
    )


def validate_grouping_columns(
    df: pl.DataFrame,
    required_groups: list[str],
) -> None:
    """
    Validate that required grouping columns exist in dataframe.

    Parameters
    ----------
    df : pl.DataFrame
        Dataframe to validate
    required_groups : list[str]
        List of required column names

    Raises
    ------
    ValueError
        If any required columns are missing
    """
    ColumnValidator.validate_columns(
        df,
        required_columns=required_groups,
        context="grouping",
        raise_on_missing=True,
        include_available=True,  # Include available columns in error message
    )


def get_size_class_bounds(
    size_class_type: Literal["standard", "descriptive"] = "standard",
) -> dict[str, tuple[float, float]]:
    """
    Get the diameter bounds for each size class.

    Parameters
    ----------
    size_class_type : {"standard", "descriptive"}, default "standard"
        Type of size class definitions to return

    Returns
    -------
    dict[str, tuple[float, float]]
        Dictionary mapping size class labels to (min, max) diameter bounds
    """
    if size_class_type == "standard":
        return STANDARD_SIZE_CLASSES.copy()
    elif size_class_type == "descriptive":
        return DESCRIPTIVE_SIZE_CLASSES.copy()
    else:
        raise ValueError(f"Invalid size_class_type: {size_class_type}")


def get_forest_type_group(fortypcd: int | None) -> str:
    """
    Map forest type code (FORTYPCD) to forest type group name.

    Groups forest types into major categories following FIA classification
    with special handling for common western forest types.

    Parameters
    ----------
    fortypcd : int or None
        Forest type code from COND table

    Returns
    -------
    str
        Forest type group name

    Examples
    --------
    >>> get_forest_type_group(200)
    'Douglas-fir'
    >>> get_forest_type_group(221)
    'Ponderosa Pine'
    >>> get_forest_type_group(None)
    'Unknown'
    """
    if fortypcd is None:
        return "Unknown"
    elif 100 <= fortypcd <= 199:
        return "White/Red/Jack Pine"
    elif 200 <= fortypcd <= 299:
        if fortypcd == 200:
            return "Douglas-fir"
        elif fortypcd in [220, 221, 222]:
            return "Ponderosa Pine"
        elif fortypcd == 240:
            return "Western White Pine"
        elif fortypcd in [260, 261, 262, 263, 264, 265]:
            return "Fir/Spruce/Mountain Hemlock"
        elif fortypcd == 280:
            return "Lodgepole Pine"
        else:
            return "Spruce/Fir"
    elif 300 <= fortypcd <= 399:
        if fortypcd in [300, 301, 302, 303, 304, 305]:
            return "Hemlock/Sitka Spruce"
        elif fortypcd == 370:
            return "California Mixed Conifer"
        else:
            return "Longleaf/Slash Pine"
    elif 400 <= fortypcd <= 499:
        return "Oak/Pine"
    elif 500 <= fortypcd <= 599:
        return "Oak/Hickory"
    elif 600 <= fortypcd <= 699:
        return "Oak/Gum/Cypress"
    elif 700 <= fortypcd <= 799:
        return "Elm/Ash/Cottonwood"
    elif 800 <= fortypcd <= 899:
        return "Maple/Beech/Birch"
    elif 900 <= fortypcd <= 999:
        if 900 <= fortypcd <= 909:
            return "Aspen/Birch"
        elif 910 <= fortypcd <= 919:
            return "Alder/Maple"
        elif 920 <= fortypcd <= 929:
            return "Western Oak"
        elif 940 <= fortypcd <= 949:
            return "Tanoak/Laurel"
        elif 950 <= fortypcd <= 959:
            return "Other Western Hardwoods"
        elif 960 <= fortypcd <= 969:
            return "Tropical Hardwoods"
        elif 970 <= fortypcd <= 979:
            return "Exotic Hardwoods"
        elif 980 <= fortypcd <= 989:
            return "Woodland Hardwoods"
        elif 990 <= fortypcd <= 998:
            return "Exotic Softwoods"
        elif fortypcd == 999:
            return "Nonstocked"
        else:
            return "Other Hardwoods"
    else:
        return "Other"


def add_forest_type_group(
    df: pl.DataFrame,
    fortypcd_col: str = "FORTYPCD",
    output_col: str = "FOREST_TYPE_GROUP",
) -> pl.DataFrame:
    """
    Add forest type group column to a dataframe containing FORTYPCD.

    Parameters
    ----------
    df : pl.DataFrame
        DataFrame containing forest type codes
    fortypcd_col : str, default "FORTYPCD"
        Name of column containing forest type codes
    output_col : str, default "FOREST_TYPE_GROUP"
        Name for the output column

    Returns
    -------
    pl.DataFrame
        DataFrame with forest type group column added

    Examples
    --------
    >>> cond_with_groups = add_forest_type_group(cond_df)
    >>> # Group by forest type for analysis
    >>> by_forest_type = cond_with_groups.group_by("FOREST_TYPE_GROUP").agg(...)
    """
    return df.with_columns(
        pl.col(fortypcd_col)
        .map_elements(get_forest_type_group, return_dtype=pl.Utf8)
        .alias(output_col)
    )


def get_ownership_group_name(owngrpcd: int | None) -> str:
    """
    Map ownership group code to descriptive name.

    Parameters
    ----------
    owngrpcd : int or None
        Ownership group code from FIA

    Returns
    -------
    str
        Ownership group name

    Examples
    --------
    >>> get_ownership_group_name(10)
    'Forest Service'
    >>> get_ownership_group_name(40)
    'Private'
    """
    ownership_names = {
        10: "Forest Service",
        20: "Other Federal",
        30: "State and Local Government",
        40: "Private",
    }
    if owngrpcd is None:
        return "Unknown (Code None)"
    return ownership_names.get(owngrpcd, f"Unknown (Code {owngrpcd})")


def add_ownership_group_name(
    df: pl.DataFrame,
    owngrpcd_col: str = "OWNGRPCD",
    output_col: str = "OWNERSHIP_GROUP",
) -> pl.DataFrame:
    """
    Add ownership group name column to a dataframe containing OWNGRPCD.

    Parameters
    ----------
    df : pl.DataFrame
        DataFrame containing ownership group codes
    owngrpcd_col : str, default "OWNGRPCD"
        Name of column containing ownership group codes
    output_col : str, default "OWNERSHIP_GROUP"
        Name for the output column

    Returns
    -------
    pl.DataFrame
        DataFrame with ownership group name column added
    """
    return df.with_columns(
        pl.col(owngrpcd_col)
        .map_elements(get_ownership_group_name, return_dtype=pl.Utf8)
        .alias(output_col)
    )


def get_forest_type_group_code(fortypcd: int | None) -> int | None:
    """
    Map forest type code (FORTYPCD) to forest type group code (FORTYPGRP).

    This provides the numeric group code that corresponds to forest type
    groupings used in FIA reference tables.

    Parameters
    ----------
    fortypcd : int or None
        Forest type code from COND table

    Returns
    -------
    int or None
        Forest type group code

    Examples
    --------
    >>> get_forest_type_group_code(200)  # Douglas-fir
    200
    >>> get_forest_type_group_code(221)  # Ponderosa Pine
    220
    """
    if fortypcd is None:
        return None

    # Map specific codes to their group codes
    # Based on FIA forest type groupings
    group_mappings = {
        # Douglas-fir group
        200: 200,
        201: 200,
        202: 200,
        203: 200,
        # Ponderosa Pine group
        220: 220,
        221: 220,
        222: 220,
        # Western White Pine
        240: 240,
        241: 240,
        # Fir/Spruce/Mountain Hemlock group
        260: 260,
        261: 260,
        262: 260,
        263: 260,
        264: 260,
        265: 260,
        # Lodgepole Pine
        280: 280,
        281: 280,
        # Hemlock/Sitka Spruce
        300: 300,
        301: 300,
        302: 300,
        303: 300,
        304: 300,
        305: 300,
        # California Mixed Conifer
        370: 370,
        371: 370,
        # Alder/Maple
        910: 910,
        911: 910,
        912: 910,
        913: 910,
        914: 910,
        915: 910,
        # Western Oak
        920: 920,
        921: 920,
        922: 920,
        923: 920,
        924: 920,
        # Tanoak/Laurel
        940: 940,
        941: 940,
        942: 940,
        # Other Western Hardwoods
        950: 950,
        951: 950,
        952: 950,
        # Nonstocked
        999: 999,
    }

    # Check if specific mapping exists
    if fortypcd in group_mappings:
        return group_mappings[fortypcd]

    # Otherwise, use the hundred's place as the group
    # This works for most eastern forest types
    return (fortypcd // 100) * 100


def add_forest_type_group_code(
    df: pl.DataFrame, fortypcd_col: str = "FORTYPCD", output_col: str = "FORTYPGRP"
) -> pl.DataFrame:
    """
    Add forest type group code column to a dataframe containing FORTYPCD.

    This creates the FORTYPGRP column that can be used for grouping
    in area() and other estimation functions.

    Parameters
    ----------
    df : pl.DataFrame
        DataFrame containing forest type codes
    fortypcd_col : str, default "FORTYPCD"
        Name of column containing forest type codes
    output_col : str, default "FORTYPGRP"
        Name for the output column

    Returns
    -------
    pl.DataFrame
        DataFrame with forest type group code column added

    Examples
    --------
    >>> # Add FORTYPGRP before using area() function
    >>> cond_with_grp = add_forest_type_group_code(cond_df)
    >>> results = area(db, grp_by=["FORTYPGRP"])
    """
    return df.with_columns(
        pl.col(fortypcd_col)
        .map_elements(get_forest_type_group_code, return_dtype=pl.Int32)
        .alias(output_col)
    )


def auto_enhance_grouping_data(
    data_df: pl.DataFrame,
    group_cols: list[str],
    preserve_reference_columns: bool = True,
) -> tuple[pl.DataFrame, list[str]]:
    """
    Automatically enhance grouping data with reference information.

    This function intelligently adds enhanced columns for common FIA grouping
    variables to make output more interpretable while preserving original
    columns for reference.

    Parameters
    ----------
    data_df : pl.DataFrame
        Input dataframe to enhance
    group_cols : list[str]
        List of grouping columns to potentially enhance
    preserve_reference_columns : bool, default True
        Whether to preserve original columns alongside enhanced ones

    Returns
    -------
    tuple[pl.DataFrame, list[str]]
        Enhanced dataframe and updated list of grouping columns

    Examples
    --------
    >>> # Enhance data with forest type group names
    >>> enhanced_df, enhanced_cols = auto_enhance_grouping_data(
    ...     cond_df, ["FORTYPCD", "OWNGRPCD"]
    ... )
    >>> # Now has FORTYPCD + FOREST_TYPE_GROUP, OWNGRPCD + OWNERSHIP_GROUP
    """
    enhanced_df = data_df
    enhanced_group_cols = group_cols.copy()

    # Track columns that were enhanced for reference preservation
    enhanced_mappings = {}

    # Enhance FORTYPCD with forest type groups
    if "FORTYPCD" in group_cols and "FORTYPCD" in enhanced_df.columns:
        # Add forest type group code (FORTYPGRP) for grouping
        enhanced_df = add_forest_type_group_code(enhanced_df)
        enhanced_mappings["FORTYPCD"] = "FORTYPGRP"

        # Also add descriptive name for better output readability
        enhanced_df = add_forest_type_group(enhanced_df)

        # Replace FORTYPCD with FORTYPGRP in grouping columns if not preserving references
        if not preserve_reference_columns:
            enhanced_group_cols = [
                "FORTYPGRP" if col == "FORTYPCD" else col for col in enhanced_group_cols
            ]
        else:
            # Add FORTYPGRP to grouping columns alongside FORTYPCD (if not already there)
            if "FORTYPGRP" not in enhanced_group_cols:
                idx = enhanced_group_cols.index("FORTYPCD")
                enhanced_group_cols.insert(idx + 1, "FORTYPGRP")

    # Enhance OWNGRPCD with ownership group names
    if "OWNGRPCD" in group_cols and "OWNGRPCD" in enhanced_df.columns:
        enhanced_df = add_ownership_group_name(enhanced_df)
        enhanced_mappings["OWNGRPCD"] = "OWNERSHIP_GROUP"

        if not preserve_reference_columns:
            enhanced_group_cols = [
                "OWNERSHIP_GROUP" if col == "OWNGRPCD" else col
                for col in enhanced_group_cols
            ]
        else:
            # Add OWNERSHIP_GROUP alongside OWNGRPCD
            if "OWNGRPCD" in enhanced_group_cols:
                idx = enhanced_group_cols.index("OWNGRPCD")
                enhanced_group_cols.insert(idx + 1, "OWNERSHIP_GROUP")

    # Enhance SPCD with species information if available
    # Note: This would require species reference table, which may not always be available
    # For now, we just preserve SPCD as-is but could be extended later

    return enhanced_df, enhanced_group_cols
