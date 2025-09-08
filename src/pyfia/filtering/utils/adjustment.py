"""
Adjustment factor filters for FIA estimation.

This module provides standardized functions for applying FIA adjustment factors
based on tree diameter classes and plot design specifications.
"""

from typing import List, Optional

import polars as pl

from ...constants.constants import (
    DiameterBreakpoints,
)
def apply_adjustment_factors(
    df: pl.DataFrame,
    value_columns: "str | List[str]",
    basis_column: str = "TREE_BASIS",
    adj_factor_columns: Optional[dict] = None,
) -> pl.DataFrame:
    """
    Apply adjustment factors based on basis to one or more value columns.

    This is a convenience wrapper expected by estimation modules.

    Parameters
    ----------
    df : pl.DataFrame
        Input dataframe containing value columns and adjustment factor columns
    value_columns : str | List[str]
        Column name or list of column names to adjust
    basis_column : str, default "TREE_BASIS"
        Column containing basis values (e.g., MICR/SUBP/MACR)
    adj_factor_columns : dict, optional
        Mapping from basis value to adjustment factor column name

    Returns
    -------
    pl.DataFrame
        Dataframe with new columns suffixed with _ADJ for each value column
    """
    if isinstance(value_columns, str):
        value_columns = [value_columns]

    if adj_factor_columns is None:
        adj_factor_columns = {
            "MICR": "ADJ_FACTOR_MICR",
            "SUBP": "ADJ_FACTOR_SUBP",
            "MACR": "ADJ_FACTOR_MACR",
        }

    adjusted_exprs: List[pl.Expr] = []
    for col in value_columns:
        conditional_expr = None
        for basis_value, factor_col in adj_factor_columns.items():
            if factor_col in df.columns:
                condition = pl.col(basis_column) == basis_value
                # Ensure numeric types are float to avoid decimal type issues
                value_expr = pl.col(col).cast(pl.Float64) * pl.col(factor_col).cast(pl.Float64)
                if conditional_expr is None:
                    conditional_expr = pl.when(condition).then(value_expr)
                else:
                    conditional_expr = conditional_expr.when(condition).then(value_expr)

        if conditional_expr is None:
            adjusted_exprs.append(pl.col(col).alias(f"{col}_ADJ"))
        else:
            adjusted_exprs.append(
                conditional_expr.otherwise(pl.col(col).cast(pl.Float64)).alias(f"{col}_ADJ")
            )

    return df.with_columns(adjusted_exprs)



def apply_tree_adjustment_factors(
    df: pl.DataFrame,
    value_column: str = "TPA_UNADJ",
    dia_column: str = "DIA",
    macro_breakpoint_column: str = "MACRO_BREAKPOINT_DIA",
    micr_factor_column: str = "ADJ_FACTOR_MICR",
    subp_factor_column: str = "ADJ_FACTOR_SUBP",
    macr_factor_column: str = "ADJ_FACTOR_MACR",
    output_column: Optional[str] = None,
) -> pl.DataFrame:
    """
    Apply FIA adjustment factors using EVALIDator methodology.

    This implements the standard FIA adjustment factor logic:
    - DIA IS NULL → ADJ_FACTOR_SUBP
    - DIA < 5.0 → ADJ_FACTOR_MICR
    - DIA < MACRO_BREAKPOINT_DIA → ADJ_FACTOR_SUBP
    - DIA >= MACRO_BREAKPOINT_DIA → ADJ_FACTOR_MACR

    Parameters
    ----------
    df : pl.DataFrame
        DataFrame containing tree data with diameter and adjustment factors
    value_column : str, default "TPA_UNADJ"
        Column containing values to adjust
    dia_column : str, default "DIA"
        Column containing tree diameter
    macro_breakpoint_column : str, default "MACRO_BREAKPOINT_DIA"
        Column containing macroplot breakpoint diameter
    micr_factor_column : str, default "ADJ_FACTOR_MICR"
        Column containing microplot adjustment factor
    subp_factor_column : str, default "ADJ_FACTOR_SUBP"
        Column containing subplot adjustment factor
    macr_factor_column : str, default "ADJ_FACTOR_MACR"
        Column containing macroplot adjustment factor
    output_column : str, optional
        Name for output column. If None, uses f"{value_column}_ADJ"

    Returns
    -------
    pl.DataFrame
        DataFrame with adjustment factor applied

    Examples
    --------
    >>> # Apply adjustment factors to TPA
    >>> df_adj = apply_tree_adjustment_factors(df, "TPA_UNADJ")

    >>> # Apply to biomass values
    >>> df_bio = apply_tree_adjustment_factors(df, "DRYBIO_AG", output_column="BIO_ADJ")
    """
    if output_column is None:
        output_column = f"{value_column}_ADJ"

    # EVALIDator adjustment factor logic
    adjustment_expr = (
        pl.when(pl.col(dia_column).is_null())
        .then(pl.col(subp_factor_column))
        .when(pl.col(dia_column) < DiameterBreakpoints.MICROPLOT_MAX_DIA)
        .then(pl.col(micr_factor_column))
        .when(pl.col(dia_column) < pl.coalesce(pl.col(macro_breakpoint_column), pl.lit(9999.0)))
        .then(pl.col(subp_factor_column))
        .otherwise(pl.col(macr_factor_column))
    )

    return df.with_columns(
        (pl.col(value_column) * adjustment_expr).alias(output_column)
    )


def calculate_expanded_estimate(
    df: pl.DataFrame,
    adjusted_value_column: str,
    expansion_column: str = "EXPNS",
    output_column: Optional[str] = None,
) -> pl.DataFrame:
    """
    Calculate population-expanded estimates.

    Multiplies adjusted values by expansion factors to get population totals.

    Parameters
    ----------
    df : pl.DataFrame
        DataFrame with adjusted values and expansion factors
    adjusted_value_column : str
        Column containing adjusted values
    expansion_column : str, default "EXPNS"
        Column containing expansion factors
    output_column : str, optional
        Name for output column. If None, uses f"{adjusted_value_column}_EXPANDED"

    Returns
    -------
    pl.DataFrame
        DataFrame with expanded estimates
    """
    if output_column is None:
        output_column = f"{adjusted_value_column}_EXPANDED"

    return df.with_columns(
        (pl.col(adjusted_value_column) * pl.col(expansion_column)).alias(output_column)
    )


def apply_tree_expansion_full(
    df: pl.DataFrame,
    value_column: str = "TPA_UNADJ",
    dia_column: str = "DIA",
    expansion_column: str = "EXPNS",
    macro_breakpoint_column: str = "MACRO_BREAKPOINT_DIA",
    micr_factor_column: str = "ADJ_FACTOR_MICR",
    subp_factor_column: str = "ADJ_FACTOR_SUBP",
    macr_factor_column: str = "ADJ_FACTOR_MACR",
    output_column: Optional[str] = None,
) -> pl.DataFrame:
    """
    Apply full FIA expansion in one step: VALUE * ADJUSTMENT_FACTOR * EXPNS.

    This is the complete EVALIDator formula for population estimates.

    Parameters
    ----------
    df : pl.DataFrame
        DataFrame containing all required columns
    value_column : str, default "TPA_UNADJ"
        Column containing base values (e.g., TPA_UNADJ, DRYBIO_AG)
    dia_column : str, default "DIA"
        Column containing tree diameter
    expansion_column : str, default "EXPNS"
        Column containing expansion factors
    macro_breakpoint_column : str, default "MACRO_BREAKPOINT_DIA"
        Column containing macroplot breakpoint diameter
    micr_factor_column : str, default "ADJ_FACTOR_MICR"
        Column containing microplot adjustment factor
    subp_factor_column : str, default "ADJ_FACTOR_SUBP"
        Column containing subplot adjustment factor
    macr_factor_column : str, default "ADJ_FACTOR_MACR"
        Column containing macroplot adjustment factor
    output_column : str, optional
        Name for output column. If None, uses f"{value_column}_EXPANDED"

    Returns
    -------
    pl.DataFrame
        DataFrame with fully expanded population estimates

    Examples
    --------
    >>> # Calculate expanded tree counts
    >>> df_trees = apply_tree_expansion_full(df, "TPA_UNADJ", output_column="TREE_COUNT")

    >>> # Calculate expanded biomass
    >>> df_bio = apply_tree_expansion_full(df, "DRYBIO_AG", output_column="BIOMASS_TOTAL")
    """
    if output_column is None:
        output_column = f"{value_column}_EXPANDED"

    # Complete EVALIDator expansion formula
    expansion_expr = (
        pl.col(value_column) *
        (
            pl.when(pl.col(dia_column).is_null())
            .then(pl.col(subp_factor_column))
            .when(pl.col(dia_column) < DiameterBreakpoints.MICROPLOT_MAX_DIA)
            .then(pl.col(micr_factor_column))
            .when(pl.col(dia_column) < pl.coalesce(pl.col(macro_breakpoint_column), pl.lit(9999.0)))
            .then(pl.col(subp_factor_column))
            .otherwise(pl.col(macr_factor_column))
        ) *
        pl.col(expansion_column)
    )

    return df.with_columns(expansion_expr.alias(output_column))


def get_adjustment_factor_column(
    df: pl.DataFrame,
    dia_column: str = "DIA",
    macro_breakpoint_column: str = "MACRO_BREAKPOINT_DIA",
    micr_factor_column: str = "ADJ_FACTOR_MICR",
    subp_factor_column: str = "ADJ_FACTOR_SUBP",
    macr_factor_column: str = "ADJ_FACTOR_MACR",
    output_column: str = "ADJ_FACTOR",
) -> pl.DataFrame:
    """
    Create a column with the appropriate adjustment factor for each tree.

    Useful when you need the adjustment factor itself rather than applying it.

    Parameters
    ----------
    df : pl.DataFrame
        DataFrame containing tree data
    dia_column : str, default "DIA"
        Column containing tree diameter
    macro_breakpoint_column : str, default "MACRO_BREAKPOINT_DIA"
        Column containing macroplot breakpoint diameter
    micr_factor_column : str, default "ADJ_FACTOR_MICR"
        Column containing microplot adjustment factor
    subp_factor_column : str, default "ADJ_FACTOR_SUBP"
        Column containing subplot adjustment factor
    macr_factor_column : str, default "ADJ_FACTOR_MACR"
        Column containing macroplot adjustment factor
    output_column : str, default "ADJ_FACTOR"
        Name for output column

    Returns
    -------
    pl.DataFrame
        DataFrame with adjustment factor column added
    """
    adjustment_factor_expr = (
        pl.when(pl.col(dia_column).is_null())
        .then(pl.col(subp_factor_column))
        .when(pl.col(dia_column) < DiameterBreakpoints.MICROPLOT_MAX_DIA)
        .then(pl.col(micr_factor_column))
        .when(pl.col(dia_column) < pl.coalesce(pl.col(macro_breakpoint_column), pl.lit(9999.0)))
        .then(pl.col(subp_factor_column))
        .otherwise(pl.col(macr_factor_column))
    )

    return df.with_columns(adjustment_factor_expr.alias(output_column))


from .validation import ColumnValidator


def validate_adjustment_columns(
    df: pl.DataFrame,
    required_columns: Optional[List[str]] = None,
) -> bool:
    """
    Validate that DataFrame has required columns for adjustment factor operations.

    Parameters
    ----------
    df : pl.DataFrame
        DataFrame to validate
    required_columns : List[str], optional
        List of required columns. If None, uses standard set.

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
        # Use both predefined sets for full adjustment validation
        is_valid_basic, _ = ColumnValidator.validate_columns(
            df,
            column_set="adjustment_basic",
            context="adjustment factors",
            raise_on_missing=True
        )
        is_valid_factors, _ = ColumnValidator.validate_columns(
            df,
            column_set="adjustment_factors",
            context="adjustment factors",
            raise_on_missing=True
        )
        return is_valid_basic and is_valid_factors
    else:
        is_valid, _ = ColumnValidator.validate_columns(
            df,
            required_columns=required_columns,
            context="adjustment factors",
            raise_on_missing=True
        )
        return is_valid
