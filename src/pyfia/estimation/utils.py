"""
Common utilities for design-based estimation in pyFIA.

This module provides shared functions for FIA population estimation
following Bechtold & Patterson (2005) procedures.
"""

from typing import Dict, List, Optional, Union

import numpy as np
import polars as pl

from ..filters.domain_parser import DomainExpressionParser


def merge_estimation_data(data: Dict[str, pl.DataFrame]) -> pl.DataFrame:
    """
    Merge FIA tables for estimation procedures.

    This function joins PLOT, COND, and population tables to create
    a unified dataframe for estimation.

    Args:
        data: Dictionary with FIA tables from data reader

    Returns:
        Merged dataframe ready for estimation
    """
    # Start with plots
    result = data["plot"].clone()

    # Add stratum assignments
    if (
        "pop_plot_stratum_assgn" in data
        and not data["pop_plot_stratum_assgn"].is_empty()
    ):
        result = result.join(
            data["pop_plot_stratum_assgn"].select(["PLT_CN", "STRATUM_CN", "EVALID"]),
            left_on="CN",
            right_on="PLT_CN",
            how="left",
        )

    # Add stratum info (expansion factors)
    if "pop_stratum" in data and not data["pop_stratum"].is_empty():
        result = result.join(
            data["pop_stratum"].select(
                ["CN", "EXPNS", "ADJ_FACTOR_SUBP", "ADJ_FACTOR_MICR", "ADJ_FACTOR_MACR"]
            ),
            left_on="STRATUM_CN",
            right_on="CN",
            how="left",
            suffix="_STRATUM",
        )

    # Add estimation unit info
    if "pop_estn_unit" in data and not data["pop_estn_unit"].is_empty():
        # First need to link through pop_stratum
        if "ESTN_UNIT_CN" in data["pop_stratum"].columns:
            estn_unit_link = data["pop_stratum"].select(["CN", "ESTN_UNIT_CN"])
            result = result.join(
                estn_unit_link,
                left_on="STRATUM_CN",
                right_on="CN",
                how="left",
                suffix="_LINK",
            )

            result = result.join(
                data["pop_estn_unit"].select(["CN", "AREA_USED"]),
                left_on="ESTN_UNIT_CN",
                right_on="CN",
                how="left",
                suffix="_ESTN",
            )

    return result


def calculate_adjustment_factors(
    data: pl.DataFrame, condition: Optional[str] = None
) -> pl.DataFrame:
    """
    Calculate plot-level adjustment factors.

    FIA uses adjustment factors to account for different plot designs
    and partially forested conditions.

    Args:
        data: Plot data with condition information
        condition: Optional condition filter (e.g., "COND_STATUS_CD == 1")

    Returns:
        Data with adjustment factors added
    """
    # Base adjustment factor (usually 1.0 for standard plots)
    data = data.with_columns(
        pl.when(pl.col("DESIGNCD") == 1)
        .then(1.0)
        .otherwise(1.0)  # Adjust for other designs as needed
        .alias("PLOT_ADJ_FACTOR")
    )

    # Condition proportion adjustment
    if condition:
        # This would typically involve SUBP_COND_PROP calculations
        # For now, simplified version
        data = data.with_columns(pl.lit(1.0).alias("COND_ADJ_FACTOR"))
    else:
        data = data.with_columns(pl.lit(1.0).alias("COND_ADJ_FACTOR"))

    # Combined adjustment
    data = data.with_columns(
        (pl.col("PLOT_ADJ_FACTOR") * pl.col("COND_ADJ_FACTOR")).alias("ADJ_FACTOR")
    )

    return data




def apply_domain_filter(
    data: pl.DataFrame,
    tree_domain: Optional[str] = None,
    area_domain: Optional[str] = None,
) -> pl.DataFrame:
    """
    Apply domain (subset) filters to data.

    Args:
        data: Input dataframe
        tree_domain: Tree-level domain expression
        area_domain: Area-level domain expression

    Returns:
        Filtered dataframe
    """
    result = data.clone()

    # Apply SQL-like expressions if provided
    if tree_domain:
        result = DomainExpressionParser.apply_to_dataframe(result, tree_domain, "tree")

    if area_domain:
        result = DomainExpressionParser.apply_to_dataframe(result, area_domain, "area")

    return result




def summarize_by_groups(
    data: pl.DataFrame, response_col: str, group_cols: List[str], agg_func: str = "sum"
) -> pl.DataFrame:
    """
    Summarize data by grouping variables.

    Args:
        data: Input data
        response_col: Column to summarize
        group_cols: Columns to group by
        agg_func: Aggregation function ('sum', 'mean', etc.)

    Returns:
        Summarized dataframe
    """
    # Build aggregation expression
    if agg_func == "sum":
        agg_expr = pl.col(response_col).sum()
    elif agg_func == "mean":
        agg_expr = pl.col(response_col).mean()
    elif agg_func == "count":
        agg_expr = pl.count()
    else:
        raise ValueError(f"Unknown aggregation function: {agg_func}")

    # Add other useful statistics
    result = data.group_by(group_cols).agg(
        [
            agg_expr.alias(f"{response_col}_{agg_func}"),
            pl.len().alias("n_obs"),
            pl.col(response_col).std().alias(f"{response_col}_std"),
            pl.col(response_col).min().alias(f"{response_col}_min"),
            pl.col(response_col).max().alias(f"{response_col}_max"),
        ]
    )

    return result


def cv(
    estimate: Union[float, pl.Expr], variance: Union[float, pl.Expr]
) -> Union[float, pl.Expr]:
    """Calculate coefficient of variation as percentage."""
    if isinstance(estimate, (int, float)):
        if estimate == 0:
            return 0.0
        return (variance**0.5) / estimate * 100
    else:
        # Polars expression
        return (
            pl.when(estimate == 0).then(0.0).otherwise((variance**0.5) / estimate * 100)
        )


def ratio_var(
    numerator: Union[float, pl.Expr],
    denominator: Union[float, pl.Expr],
    var_num: Union[float, pl.Expr],
    var_den: Union[float, pl.Expr],
    covar: Union[float, pl.Expr],
) -> Union[float, pl.Expr]:
    """
    Calculate variance of a ratio using the delta method.

    For ratio R = Y/X, the variance is:
    Var(R) = (1/X²) * [Var(Y) + R² * Var(X) - 2 * R * Cov(Y,X)]

    Parameters
    ----------
    numerator : float or pl.Expr
        The numerator value (Y)
    denominator : float or pl.Expr
        The denominator value (X)
    var_num : float or pl.Expr
        Variance of the numerator
    var_den : float or pl.Expr
        Variance of the denominator
    covar : float or pl.Expr
        Covariance between numerator and denominator

    Returns
    -------
    float or pl.Expr
        Variance of the ratio
    """
    if isinstance(numerator, (int, float)):
        if denominator == 0:
            return 0.0
        ratio = numerator / denominator
        return (1 / denominator**2) * (var_num + ratio**2 * var_den - 2 * ratio * covar)
    else:
        # Polars expression
        ratio = numerator / denominator
        return (
            pl.when(denominator == 0)
            .then(0.0)
            .otherwise(
                (1 / denominator**2)
                * (var_num + ratio**2 * var_den - 2 * ratio * covar)
            )
        )
