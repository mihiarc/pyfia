"""
Variance calculation functions for FIA estimation.

This module provides shared variance calculation functions used across
all estimation modules, implementing variance formulas from Bechtold &
Patterson (2005), Chapter 4 (pp. 53-77).

The domain total variance formula is implemented:

V(Ŷ) = Σ_h W_h² × s²_yh × n_h

Where:
- W_h is the stratum expansion factor (EXPNS, acres per plot)
- s²_yh is the sample variance within stratum h (with ddof=1)
- n_h is the number of plots in stratum h

This is algebraically equivalent to the textbook form in Bechtold &
Patterson (2005): V(Ŷ_d) = Σ_h (N_h² / n_h) × s²_yh, because
W_h = N_h / n_h (EXPNS = total stratum acres / plots in stratum),
so W_h² × n_h = (N_h/n_h)² × n_h = N_h²/n_h.

This is the variance formula used by EVALIDator for tree-based estimates
(volume, biomass, TPA, GRM) and produces SE estimates within 1-3% of
EVALIDator output.

Key implementation requirements:
- Include ALL plots (even with zero values) in variance calculations
- Exclude single-plot strata (variance undefined with n=1)
- Use ddof=1 for sample variance calculation

Statistical methodology references:
- Domain indicator function: Eq. 4.1, p. 47 (Φ_hid for condition attributes)
- Adjustment factors: Eq. 4.2, p. 49 (1/p_mh for non-sampled plots)
- Tree attribute estimation: Eq. 4.8, p. 53 (y_hid)
- Post-stratified variance: Section 4.2, pp. 55-60

Reference:
    Bechtold, W.A.; Patterson, P.L., eds. 2005. The Enhanced Forest
    Inventory and Analysis Program - National Sampling Design and
    Estimation Procedures. Gen. Tech. Rep. SRS-80. Asheville, NC:
    U.S. Department of Agriculture, Forest Service, Southern Research
    Station. 85 p. https://doi.org/10.2737/SRS-GTR-80
"""

from typing import Dict, List, Tuple

import polars as pl

from .constants import Z_SCORE_90, Z_SCORE_95, Z_SCORE_99


def calculate_grouped_domain_total_variance(
    plot_data: pl.DataFrame,
    group_cols: List[str],
    y_col: str,
    x_col: str = "x_i",
    stratum_col: str = "STRATUM_CN",
    weight_col: str = "EXPNS",
) -> pl.DataFrame:
    """Calculate domain total variance for multiple groups in a single pass.

    This is a vectorized version of calculate_domain_total_variance that
    computes variance for all groups simultaneously using Polars group_by
    operations, avoiding the N+1 query pattern of iterating through groups.

    Implements the stratified domain total variance formula:
    V(Ŷ) = Σ_h w_h² × s²_yh × n_h

    Parameters
    ----------
    plot_data : pl.DataFrame
        Plot-level data with group columns, stratum assignment, and values.
        Must contain PLT_CN, y_col, stratum_col, weight_col, and group_cols.
    group_cols : List[str]
        Columns to group results by (e.g., ["SPCD"] for by-species)
    y_col : str
        Column name for Y values (the metric being estimated)
    x_col : str, default 'x_i'
        Column name for X values (area proportion, for per-acre SE calculation)
    stratum_col : str, default 'STRATUM_CN'
        Column name for stratum assignment
    weight_col : str, default 'EXPNS'
        Column name for stratum weights (expansion factors)

    Returns
    -------
    pl.DataFrame
        DataFrame with group columns and variance statistics:
        - se_acre: Standard error of per-acre estimate
        - se_total: Standard error of total estimate
        - variance_acre: Variance of per-acre estimate
        - variance_total: Variance of total estimate
    """
    # Ensure we have the stratum column
    if stratum_col not in plot_data.columns:
        plot_data = plot_data.with_columns(pl.lit(1).alias("_STRATUM"))
        stratum_col = "_STRATUM"

    # Filter to valid group columns that exist in data
    valid_group_cols = [c for c in group_cols if c in plot_data.columns]

    if not valid_group_cols:
        # No grouping - fall back to scalar calculation
        var_stats = calculate_domain_total_variance(
            plot_data, y_col, stratum_col, weight_col
        )
        # Calculate per-acre SE
        total_x = (
            (plot_data[weight_col] * plot_data[x_col]).sum()
            if x_col in plot_data.columns
            else 1.0
        )
        se_acre = var_stats["se_total"] / total_x if total_x > 0 else 0.0
        return pl.DataFrame(
            {
                "se_acre": [se_acre],
                "se_total": [var_stats["se_total"]],
                "variance_acre": [se_acre**2],
                "variance_total": [var_stats["variance_total"]],
            }
        )

    # Step 1: Calculate stratum-level statistics per group
    # Group by (group_cols + stratum) to get stratum stats within each group
    stratum_group_cols = valid_group_cols + [stratum_col]

    strata_stats = plot_data.group_by(stratum_group_cols).agg(
        [
            pl.count("PLT_CN").alias("n_h"),
            pl.mean(y_col).alias("ybar_h"),
            pl.var(y_col, ddof=1).alias("s2_yh"),
            pl.first(weight_col).cast(pl.Float64).alias("w_h"),
            # For per-acre calculation, need total area
            (pl.col(weight_col).first() * pl.col(x_col).sum()).alias("stratum_area")
            if x_col in plot_data.columns
            else pl.lit(0.0).alias("stratum_area"),
        ]
    )

    # Handle null variances (single observation or all same values)
    strata_stats = strata_stats.with_columns(
        [
            pl.when(pl.col("s2_yh").is_null())
            .then(0.0)
            .otherwise(pl.col("s2_yh"))
            .cast(pl.Float64)
            .alias("s2_yh"),
            pl.col("ybar_h").fill_null(0.0).cast(pl.Float64).alias("ybar_h"),
        ]
    )

    # Step 2: Calculate variance component per stratum (only for n_h > 1)
    # v_h = w_h² × s²_yh × n_h
    strata_stats = strata_stats.with_columns(
        [
            pl.when(pl.col("n_h") > 1)
            .then(pl.col("w_h") ** 2 * pl.col("s2_yh") * pl.col("n_h"))
            .otherwise(0.0)
            .alias("v_h"),
            # Total Y contribution from this stratum
            (pl.col("ybar_h") * pl.col("w_h") * pl.col("n_h")).alias("total_y_h"),
        ]
    )

    # Step 3: Aggregate variance components by group
    variance_by_group = strata_stats.group_by(valid_group_cols).agg(
        [
            pl.sum("v_h").alias("variance_total"),
            pl.sum("total_y_h").alias("total_y"),
            pl.sum("stratum_area").alias("total_area"),
            pl.sum("n_h").alias("n_plots"),
        ]
    )

    # Step 4: Calculate SE and per-acre variance
    variance_by_group = (
        variance_by_group.with_columns(
            [
                # Clamp negative variances to 0
                pl.when(pl.col("variance_total") < 0)
                .then(0.0)
                .otherwise(pl.col("variance_total"))
                .alias("variance_total"),
            ]
        )
        .with_columns(
            [
                # SE total = sqrt(variance_total)
                pl.col("variance_total").sqrt().alias("se_total"),
                # SE acre = SE_total / total_area
                pl.when(pl.col("total_area") > 0)
                .then(pl.col("variance_total").sqrt() / pl.col("total_area"))
                .otherwise(0.0)
                .alias("se_acre"),
            ]
        )
        .with_columns(
            [
                # Variance acre = SE_acre²
                (pl.col("se_acre") ** 2).alias("variance_acre"),
            ]
        )
    )

    # Select only the columns we need
    result_cols = valid_group_cols + [
        "se_acre",
        "se_total",
        "variance_acre",
        "variance_total",
    ]
    return variance_by_group.select(result_cols)


def calculate_domain_total_variance(
    plot_data: pl.DataFrame,
    y_col: str,
    stratum_col: str = "STRATUM_CN",
    weight_col: str = "EXPNS",
) -> Dict[str, float]:
    """Calculate variance for domain total estimation.

    Implements the stratified domain total variance formula from
    Bechtold & Patterson (2005), which is used by EVALIDator for
    tree-based attributes (volume, biomass, tree count, etc.):

    V(Ŷ) = Σ_h w_h² × s²_yh × n_h

    Where:
    - Y is the attribute of interest (volume, biomass, tree count, etc.)
    - s²_yh is the sample variance of Y in stratum h (with ddof=1)
    - w_h is the stratum weight (EXPNS = acres/plot in stratum h)
    - n_h is the number of plots in stratum h

    This is the standard FIA variance formula for domain totals, which
    does NOT include ratio adjustment terms. EVALIDator uses this formula
    for tree-based estimates because the domain total is calculated
    directly from plot-level expanded values.

    Note: This differs from `calculate_ratio_variance` which includes
    covariance terms for ratio-of-means estimation. For tree attributes
    where Y already incorporates expansion factors, the simpler domain
    total variance is appropriate and matches EVALIDator output.

    Parameters
    ----------
    plot_data : pl.DataFrame
        Plot-level data with columns for Y values, stratum assignment,
        and weights. Must contain at minimum:
        - PLT_CN: Plot identifier
        - y_col: Attribute values (expanded to per-acre or total)
        - stratum_col: Stratum assignment
        - weight_col: Expansion factors
    y_col : str
        Column name for Y values
    stratum_col : str, default 'STRATUM_CN'
        Column name for stratum assignment
    weight_col : str, default 'EXPNS'
        Column name for stratum weights (expansion factors)

    Returns
    -------
    dict
        Dictionary with keys:
        - variance_total: Variance of total estimate
        - se_total: Standard error of total estimate
        - total_y: Total Y value
        - n_strata: Number of strata
        - n_plots: Total number of plots

    Notes
    -----
    This function properly handles:
    - Single-plot strata (excluded from variance calculation)
    - Null variances (treated as 0)
    - Missing stratification (treated as single stratum)

    References
    ----------
    Bechtold, W.A.; Patterson, P.L., eds. 2005. The Enhanced Forest
    Inventory and Analysis Program - National Sampling Design and
    Estimation Procedures. Gen. Tech. Rep. SRS-80. Asheville, NC:
    U.S. Department of Agriculture, Forest Service, Southern Research
    Station. 85 p. https://doi.org/10.2737/SRS-GTR-80
    """
    # Determine stratification columns
    if stratum_col not in plot_data.columns:
        # No stratification, treat as single stratum
        plot_data = plot_data.with_columns(pl.lit(1).alias("_STRATUM"))
        stratum_col = "_STRATUM"

    # Calculate stratum-level statistics
    strata_stats = plot_data.group_by(stratum_col).agg(
        [
            pl.count("PLT_CN").alias("n_h"),
            pl.mean(y_col).alias("ybar_h"),
            pl.var(y_col, ddof=1).alias("s2_yh"),
            pl.first(weight_col).cast(pl.Float64).alias("w_h"),
        ]
    )

    # Handle null variances (single observation or all same values)
    strata_stats = strata_stats.with_columns(
        [
            pl.when(pl.col("s2_yh").is_null())
            .then(0.0)
            .otherwise(pl.col("s2_yh"))
            .cast(pl.Float64)
            .alias("s2_yh"),
            pl.col("ybar_h").cast(pl.Float64).alias("ybar_h"),
        ]
    )

    # Calculate population total using expansion factors
    # Total Y = Σ_h (ybar_h × w_h × n_h)
    total_y = (strata_stats["ybar_h"] * strata_stats["w_h"] * strata_stats["n_h"]).sum()

    # Filter out single-plot strata (variance undefined with n=1)
    strata_with_variance = strata_stats.filter(pl.col("n_h") > 1)

    # Calculate variance components only for strata with n > 1
    # V(Ŷ) = Σ_h w_h² × s²_yh × n_h
    variance_components = strata_with_variance.with_columns(
        [(pl.col("w_h") ** 2 * pl.col("s2_yh") * pl.col("n_h")).alias("v_h")]
    )

    # Sum variance components, handling NaN values
    variance_total = variance_components["v_h"].drop_nans().sum()
    if variance_total is None or variance_total < 0:
        variance_total = 0.0

    # Standard error
    se_total = variance_total**0.5

    return {
        "variance_total": variance_total,
        "se_total": se_total,
        "total_y": total_y,
        "n_strata": len(strata_stats),
        "n_plots": int(strata_stats["n_h"].sum()),
    }


# =============================================================================
# Utility functions salvaged from statistics.py
# =============================================================================


def safe_divide(
    numerator: pl.Expr, denominator: pl.Expr, default: float = 0.0
) -> pl.Expr:
    """
    Safe division that handles zero denominators.

    Parameters
    ----------
    numerator : pl.Expr
        Numerator expression
    denominator : pl.Expr
        Denominator expression
    default : float
        Default value when denominator is zero

    Returns
    -------
    pl.Expr
        Safe division expression
    """
    return pl.when(denominator != 0).then(numerator / denominator).otherwise(default)


def safe_sqrt(expr: pl.Expr, default: float = 0.0) -> pl.Expr:
    """
    Safe square root that handles negative values.

    Parameters
    ----------
    expr : pl.Expr
        Expression to take square root of
    default : float
        Default value for negative inputs

    Returns
    -------
    pl.Expr
        Safe square root expression
    """
    return pl.when(expr >= 0).then(expr.sqrt()).otherwise(default)


def calculate_confidence_interval(
    estimate: float, se: float, confidence: float = 0.95
) -> Tuple[float, float]:
    """
    Calculate confidence interval using normal approximation.

    Parameters
    ----------
    estimate : float
        Point estimate
    se : float
        Standard error
    confidence : float
        Confidence level (default 0.95 for 95% CI)

    Returns
    -------
    Tuple[float, float]
        Lower and upper bounds of confidence interval
    """
    if confidence == 0.95:
        z = Z_SCORE_95
    elif confidence == 0.90:
        z = Z_SCORE_90
    elif confidence == 0.99:
        z = Z_SCORE_99
    else:
        # For other confidence levels, would need scipy.stats
        z = Z_SCORE_95  # Default to 95%

    lower = estimate - z * se
    upper = estimate + z * se

    return lower, upper


def calculate_cv(estimate: float, se: float) -> float:
    """
    Calculate coefficient of variation as percentage.

    Parameters
    ----------
    estimate : float
        Point estimate
    se : float
        Standard error

    Returns
    -------
    float
        Coefficient of variation as percentage
    """
    if estimate != 0:
        return 100 * se / abs(estimate)
    return 0.0


def apply_finite_population_correction(
    variance: float, n_sampled: int, n_total: int
) -> float:
    """
    Apply finite population correction factor.

    Parameters
    ----------
    variance : float
        Uncorrected variance
    n_sampled : int
        Number of sampled units
    n_total : int
        Total population size

    Returns
    -------
    float
        Corrected variance
    """
    if n_total > n_sampled:
        fpc = (n_total - n_sampled) / n_total
        return variance * fpc
    return variance
