"""
Variance calculation functions for FIA estimation.

This module provides shared variance calculation functions used across
all estimation modules, implementing variance formulas from Bechtold &
Patterson (2005), Chapter 4 (pp. 53-77).

Two variance functions are provided:

1. **calculate_domain_total_variance**: Stratified domain total variance
   V(Ŷ) = Σ_h w_h² × s²_yh × n_h

   Used by EVALIDator for tree-based estimates (volume, biomass, TPA, GRM).
   This is the PRIMARY variance method used in pyFIA and produces SE estimates
   within 1-3% of EVALIDator output.

2. **calculate_ratio_variance**: Ratio-of-means variance with covariance
   V(R) = (1/X̄²) × Σ_h w_h² × [s²_yh + R² × s²_xh - 2R × s_yxh] × n_h

   Includes covariance terms between numerator and denominator. Available for
   cases where explicit ratio estimation is needed.

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

from typing import Dict, List

import polars as pl


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
        var_stats = calculate_domain_total_variance(plot_data, y_col, stratum_col, weight_col)
        # Calculate per-acre SE
        total_x = (plot_data[weight_col] * plot_data[x_col]).sum() if x_col in plot_data.columns else 1.0
        se_acre = var_stats["se_total"] / total_x if total_x > 0 else 0.0
        return pl.DataFrame({
            "se_acre": [se_acre],
            "se_total": [var_stats["se_total"]],
            "variance_acre": [se_acre ** 2],
            "variance_total": [var_stats["variance_total"]],
        })

    # Step 1: Calculate stratum-level statistics per group
    # Group by (group_cols + stratum) to get stratum stats within each group
    stratum_group_cols = valid_group_cols + [stratum_col]

    strata_stats = plot_data.group_by(stratum_group_cols).agg([
        pl.count("PLT_CN").alias("n_h"),
        pl.mean(y_col).alias("ybar_h"),
        pl.var(y_col, ddof=1).alias("s2_yh"),
        pl.first(weight_col).cast(pl.Float64).alias("w_h"),
        # For per-acre calculation, need total area
        (pl.col(weight_col).first() * pl.col(x_col).sum()).alias("stratum_area") if x_col in plot_data.columns else pl.lit(0.0).alias("stratum_area"),
    ])

    # Handle null variances (single observation or all same values)
    strata_stats = strata_stats.with_columns([
        pl.when(pl.col("s2_yh").is_null())
        .then(0.0)
        .otherwise(pl.col("s2_yh"))
        .cast(pl.Float64)
        .alias("s2_yh"),
        pl.col("ybar_h").fill_null(0.0).cast(pl.Float64).alias("ybar_h"),
    ])

    # Step 2: Calculate variance component per stratum (only for n_h > 1)
    # v_h = w_h² × s²_yh × n_h
    strata_stats = strata_stats.with_columns([
        pl.when(pl.col("n_h") > 1)
        .then(pl.col("w_h") ** 2 * pl.col("s2_yh") * pl.col("n_h"))
        .otherwise(0.0)
        .alias("v_h"),
        # Total Y contribution from this stratum
        (pl.col("ybar_h") * pl.col("w_h") * pl.col("n_h")).alias("total_y_h"),
    ])

    # Step 3: Aggregate variance components by group
    variance_by_group = strata_stats.group_by(valid_group_cols).agg([
        pl.sum("v_h").alias("variance_total"),
        pl.sum("total_y_h").alias("total_y"),
        pl.sum("stratum_area").alias("total_area"),
        pl.sum("n_h").alias("n_plots"),
    ])

    # Step 4: Calculate SE and per-acre variance
    variance_by_group = variance_by_group.with_columns([
        # Clamp negative variances to 0
        pl.when(pl.col("variance_total") < 0)
        .then(0.0)
        .otherwise(pl.col("variance_total"))
        .alias("variance_total"),
    ]).with_columns([
        # SE total = sqrt(variance_total)
        pl.col("variance_total").sqrt().alias("se_total"),
        # SE acre = SE_total / total_area
        pl.when(pl.col("total_area") > 0)
        .then(pl.col("variance_total").sqrt() / pl.col("total_area"))
        .otherwise(0.0)
        .alias("se_acre"),
    ]).with_columns([
        # Variance acre = SE_acre²
        (pl.col("se_acre") ** 2).alias("variance_acre"),
    ])

    # Select only the columns we need
    result_cols = valid_group_cols + ["se_acre", "se_total", "variance_acre", "variance_total"]
    return variance_by_group.select(result_cols)


def calculate_grouped_ratio_variance(
    plot_data: pl.DataFrame,
    group_cols: List[str],
    y_col: str,
    x_col: str = "x_i",
    stratum_col: str = "STRATUM_CN",
    weight_col: str = "EXPNS",
) -> pl.DataFrame:
    """Calculate ratio-of-means variance for multiple groups in a single pass.

    This is a vectorized version of calculate_ratio_variance that computes
    variance for all groups simultaneously, avoiding the N+1 query pattern.

    Implements the stratified ratio-of-means variance formula:
    V(R) = (1/X̄²) × Σ_h w_h² × [s²_yh + R² × s²_xh - 2R × s_yxh] × n_h

    Parameters
    ----------
    plot_data : pl.DataFrame
        Plot-level data with group columns, stratum assignment, and values.
    group_cols : List[str]
        Columns to group results by
    y_col : str
        Column name for Y values (numerator)
    x_col : str, default 'x_i'
        Column name for X values (denominator/area)
    stratum_col : str, default 'STRATUM_CN'
        Column name for stratum assignment
    weight_col : str, default 'EXPNS'
        Column name for stratum weights

    Returns
    -------
    pl.DataFrame
        DataFrame with group columns and variance statistics
    """
    # Ensure we have the stratum column
    if stratum_col not in plot_data.columns:
        plot_data = plot_data.with_columns(pl.lit(1).alias("_STRATUM"))
        stratum_col = "_STRATUM"

    # Filter to valid group columns
    valid_group_cols = [c for c in group_cols if c in plot_data.columns]

    if not valid_group_cols:
        # No grouping - fall back to scalar calculation
        var_stats = calculate_ratio_variance(plot_data, y_col, x_col, stratum_col, weight_col)
        return pl.DataFrame({
            "se_acre": [var_stats["se_acre"]],
            "se_total": [var_stats["se_total"]],
            "variance_acre": [var_stats["variance_acre"]],
            "variance_total": [var_stats["variance_total"]],
        })

    # Step 1: Calculate stratum-level statistics per group
    stratum_group_cols = valid_group_cols + [stratum_col]

    strata_stats = plot_data.group_by(stratum_group_cols).agg([
        pl.count("PLT_CN").alias("n_h"),
        pl.mean(y_col).alias("ybar_h"),
        pl.mean(x_col).alias("xbar_h"),
        pl.var(y_col, ddof=1).alias("s2_yh"),
        pl.var(x_col, ddof=1).alias("s2_xh"),
        pl.first(weight_col).cast(pl.Float64).alias("w_h"),
        # Covariance calculation
        (
            ((pl.col(y_col) - pl.col(y_col).mean()) * (pl.col(x_col) - pl.col(x_col).mean())).sum()
            / (pl.len() - 1)
        ).alias("cov_yxh"),
    ])

    # Handle null values
    strata_stats = strata_stats.with_columns([
        pl.col("s2_yh").fill_null(0.0).cast(pl.Float64).alias("s2_yh"),
        pl.col("s2_xh").fill_null(0.0).cast(pl.Float64).alias("s2_xh"),
        pl.col("cov_yxh").fill_null(0.0).cast(pl.Float64).alias("cov_yxh"),
        pl.col("ybar_h").fill_null(0.0).cast(pl.Float64).alias("ybar_h"),
        pl.col("xbar_h").fill_null(0.0).cast(pl.Float64).alias("xbar_h"),
    ])

    # Calculate stratum contributions to totals
    strata_stats = strata_stats.with_columns([
        (pl.col("ybar_h") * pl.col("w_h") * pl.col("n_h")).alias("total_y_h"),
        (pl.col("xbar_h") * pl.col("w_h") * pl.col("n_h")).alias("total_x_h"),
    ])

    # Step 2: First aggregation to get total_y and total_x per group (needed for ratio R)
    group_totals = strata_stats.group_by(valid_group_cols).agg([
        pl.sum("total_y_h").alias("total_y"),
        pl.sum("total_x_h").alias("total_x"),
    ])

    # Join back to get R per stratum row
    strata_with_ratio = strata_stats.join(group_totals, on=valid_group_cols, how="left")

    # Calculate ratio R = total_y / total_x per group
    strata_with_ratio = strata_with_ratio.with_columns([
        pl.when(pl.col("total_x") > 0)
        .then(pl.col("total_y") / pl.col("total_x"))
        .otherwise(0.0)
        .alias("R"),
    ])

    # Step 3: Calculate variance component per stratum (only for n_h > 1)
    # v_h = w_h² × (s²_yh + R² × s²_xh - 2R × cov_yxh) × n_h
    strata_with_ratio = strata_with_ratio.with_columns([
        pl.when(pl.col("n_h") > 1)
        .then(
            pl.col("w_h") ** 2
            * (
                pl.col("s2_yh")
                + pl.col("R") ** 2 * pl.col("s2_xh")
                - 2 * pl.col("R") * pl.col("cov_yxh")
            )
            * pl.col("n_h")
        )
        .otherwise(0.0)
        .alias("v_h"),
    ])

    # Step 4: Aggregate variance components by group
    variance_by_group = strata_with_ratio.group_by(valid_group_cols).agg([
        pl.sum("v_h").alias("variance_numerator"),
        pl.first("total_x").alias("total_x"),
        pl.first("total_y").alias("total_y"),
        pl.first("R").alias("ratio"),
    ])

    # Step 5: Convert to variance of ratio and calculate SEs
    variance_by_group = variance_by_group.with_columns([
        # Clamp negative to 0
        pl.when(pl.col("variance_numerator") < 0)
        .then(0.0)
        .otherwise(pl.col("variance_numerator"))
        .alias("variance_numerator"),
    ]).with_columns([
        # Variance of ratio = V(Y-RX) / X²
        pl.when(pl.col("total_x") > 0)
        .then(pl.col("variance_numerator") / (pl.col("total_x") ** 2))
        .otherwise(0.0)
        .alias("variance_acre"),
    ]).with_columns([
        # SE acre = sqrt(variance_acre)
        pl.col("variance_acre").sqrt().alias("se_acre"),
        # SE total = SE_acre × total_x
        (pl.col("variance_acre").sqrt() * pl.col("total_x")).alias("se_total"),
    ]).with_columns([
        # Variance total = SE_total²
        (pl.col("se_total") ** 2).alias("variance_total"),
    ])

    # Select only the columns we need
    result_cols = valid_group_cols + ["se_acre", "se_total", "variance_acre", "variance_total"]
    return variance_by_group.select(result_cols)


def calculate_ratio_variance(
    plot_data: pl.DataFrame,
    y_col: str,
    x_col: str = "x_i",
    stratum_col: str = "STRATUM_CN",
    weight_col: str = "EXPNS",
) -> Dict[str, float]:
    """Calculate variance for ratio-of-means estimator.

    Implements the stratified ratio-of-means variance formula from
    Bechtold & Patterson (2005):

    V(R) = (1/X̄²) × Σ_h w_h² × [s²_yh + R² × s²_xh - 2R × s_yxh] × n_h

    Where:
    - Y is the numerator (volume, biomass, tree count, etc.)
    - X is the denominator (area proportion, from CONDPROP_UNADJ)
    - R is the ratio estimate (Y/X)
    - s_yxh is the covariance between Y and X in stratum h
    - w_h is the stratum weight (EXPNS = acres/plot in stratum h)
    - n_h is the number of plots in stratum h

    Note: The multiplication by n_h (not division) is correct for FIA's
    domain total estimation because EXPNS already incorporates 1/n_h.

    Parameters
    ----------
    plot_data : pl.DataFrame
        Plot-level data with columns for Y values, X values (area),
        stratum assignment, and weights. Must contain at minimum:
        - PLT_CN: Plot identifier
        - y_col: Numerator values
        - x_col: Denominator values (typically area proportion)
        - stratum_col: Stratum assignment
        - weight_col: Expansion factors
    y_col : str
        Column name for Y values (numerator)
    x_col : str, default 'x_i'
        Column name for X values (denominator/area)
    stratum_col : str, default 'STRATUM_CN'
        Column name for stratum assignment
    weight_col : str, default 'EXPNS'
        Column name for stratum weights (expansion factors)

    Returns
    -------
    dict
        Dictionary with keys:
        - variance_acre: Variance of per-acre estimate
        - variance_total: Variance of total estimate
        - se_acre: Standard error of per-acre estimate
        - se_total: Standard error of total estimate
        - ratio: The ratio estimate R
        - total_y: Total Y value
        - total_x: Total X value (area)

    Notes
    -----
    This function properly handles:
    - Single-plot strata (excluded from variance calculation)
    - Null variances (treated as 0)
    - Missing stratification (treated as single stratum)
    - Negative variance components (clamped to 0)

    The formula accounts for covariance between numerator and denominator,
    which is essential for ratio estimation where both depend on the same
    sample plots.

    References
    ----------
    Bechtold, W.A.; Patterson, P.L., eds. 2005. The Enhanced Forest
    Inventory and Analysis Program - National Sampling Design and
    Estimation Procedures. Gen. Tech. Rep. SRS-80. Asheville, NC:
    U.S. Department of Agriculture, Forest Service, Southern Research
    Station. 85 p. https://doi.org/10.2737/SRS-GTR-80

    Specific equations implemented:
    - Domain indicator (Φ_hid): Eq. 4.1, p. 47
    - Adjustment factor (1/p_mh): Eq. 4.2, p. 49
    - Tree attributes (y_hid): Eq. 4.8, p. 53
    - Stratified variance: Section 4.2, pp. 55-60

    See also:
    - Scott, C.T. et al. 2005. Sample-based estimators used by the
      Forest Inventory and Analysis national information management
      system. Gen. Tech. Rep. SRS-80, Chapter 4, pp. 53-77.
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
            pl.mean(x_col).alias("xbar_h"),
            pl.var(y_col, ddof=1).alias("s2_yh"),
            pl.var(x_col, ddof=1).alias("s2_xh"),
            pl.first(weight_col).cast(pl.Float64).alias("w_h"),
            # Calculate covariance: Cov(Y,X) = E[(Y-E[Y])(X-E[X])]
            # Sample covariance with ddof=1: sum((y-ybar)(x-xbar))/(n-1)
            (
                (
                    (pl.col(y_col) - pl.col(y_col).mean())
                    * (pl.col(x_col) - pl.col(x_col).mean())
                ).sum()
                / (pl.len() - 1)
            ).alias("cov_yxh"),
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
            pl.when(pl.col("s2_xh").is_null())
            .then(0.0)
            .otherwise(pl.col("s2_xh"))
            .cast(pl.Float64)
            .alias("s2_xh"),
            pl.when(pl.col("cov_yxh").is_null())
            .then(0.0)
            .otherwise(pl.col("cov_yxh"))
            .cast(pl.Float64)
            .alias("cov_yxh"),
            pl.col("xbar_h").cast(pl.Float64).alias("xbar_h"),
            pl.col("ybar_h").cast(pl.Float64).alias("ybar_h"),
        ]
    )

    # Calculate population totals using expansion factors
    # Total Y = Σ_h (ybar_h × w_h × n_h)
    # Total X = Σ_h (xbar_h × w_h × n_h)
    total_y = (strata_stats["ybar_h"] * strata_stats["w_h"] * strata_stats["n_h"]).sum()
    total_x = (strata_stats["xbar_h"] * strata_stats["w_h"] * strata_stats["n_h"]).sum()

    # Calculate ratio estimate
    ratio = total_y / total_x if total_x > 0 else 0

    # Filter out single-plot strata (variance undefined with n=1)
    strata_with_variance = strata_stats.filter(pl.col("n_h") > 1)

    # Calculate variance components only for strata with n > 1
    # V(Y - RX) = Σ_h w_h² × (s²_yh + R² × s²_xh - 2R × cov_yxh) × n_h
    variance_components = strata_with_variance.with_columns(
        [
            (
                pl.col("w_h") ** 2
                * (
                    pl.col("s2_yh")
                    + ratio**2 * pl.col("s2_xh")
                    - 2 * ratio * pl.col("cov_yxh")
                )
                * pl.col("n_h")
            ).alias("v_h")
        ]
    )

    # Sum variance components, handling NaN values
    variance_of_numerator = variance_components["v_h"].drop_nans().sum()
    if variance_of_numerator is None or variance_of_numerator < 0:
        variance_of_numerator = 0.0

    # Convert to variance of the ratio by dividing by X̄²
    # This gives us Var(R) where R = Y/X
    variance_of_ratio = variance_of_numerator / (total_x**2) if total_x > 0 else 0.0

    # Standard errors
    se_acre = variance_of_ratio**0.5
    # For total, multiply SE of ratio by total area
    se_total = se_acre * total_x if total_x > 0 else 0

    return {
        "variance_acre": variance_of_ratio,
        "variance_total": (se_total**2) if se_total > 0 else 0,
        "se_acre": se_acre,
        "se_total": se_total,
        "ratio": ratio,
        "total_y": total_y,
        "total_x": total_x,
    }


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
