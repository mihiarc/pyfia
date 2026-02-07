"""
Variance calculation functions for FIA estimation.

This module provides shared variance calculation functions used across
all estimation modules, implementing variance formulas from Bechtold &
Patterson (2005), Chapter 4 (pp. 53-77).

Domain Total Variance (V(Y)):
-----------------------------

The exact post-stratified domain total variance formula is implemented:

For each estimation unit EU:

    V_EU = V1 + V2

    V1 = (A²/n) × Σ_h W_h × s²_yh / n_h     (within-stratum term)
    V2 = (A²/n²) × Σ_h (1 - W_h) × s²_yh / n_h  (post-stratification correction)

    V_total = Σ_EU V_EU

Where:
- A = AREA_USED (total area of the estimation unit in acres)
- n = total number of phase 2 plots in the estimation unit (Σ_h n_h)
- W_h = STRATUM_WGT = P1POINTCNT / P1PNTCNT_EU (phase 1 stratum weight)
- s²_yh = sample variance within stratum h (with ddof=1)
- n_h = P2POINTCNT = number of phase 2 plots in stratum h

The V2 term captures uncertainty from estimating stratum weights from
the sample. It is zero under proportional allocation (W_h = n_h/n)
and small when allocation is approximately proportional. This matches
rFIA's unitVar() implementation.

Ratio-of-Means Variance (V(R)):
-------------------------------

For per-acre estimates R = Y/X, the ratio-of-means variance from
Section 4.2 is used:

    V(R) = (1/X²) × [V(Y) + R² × V(X) - 2R × Cov(Y,X)]

Where V(Y), V(X), and Cov(Y,X) are all computed using the same
V1+V2 post-stratified formula applied to the Y, X, and cross terms.

Since Y and X are estimated from the same sample plots, their
covariance is typically positive (more forest area → more volume),
which means the ratio variance is less than the simplified
V(Y)/X² formula. The old formula se_acre = se_total/total_area
treated the denominator as known, overestimating per-acre SE.

Fallback formulas:
- V(Y) = Σ_h EXPNS² × s²_yh × n_h (when B&P columns absent)
- V(R) = (1/X²) × Σ_h [w²_h × n_h × (s²_y - 2R×cov_yx + R²×s²_x)]

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

from __future__ import annotations

import polars as pl

from .constants import Z_SCORE_90, Z_SCORE_95, Z_SCORE_99


def calculate_grouped_domain_total_variance(
    plot_data: pl.DataFrame,
    group_cols: list[str],
    y_col: str,
    x_col: str = "x_i",
    stratum_col: str = "STRATUM_CN",
    weight_col: str = "EXPNS",
    estn_unit_col: str = "ESTN_UNIT_CN",
    stratum_wgt_col: str = "STRATUM_WGT",
    area_used_col: str = "AREA_USED",
    p2pointcnt_col: str = "P2POINTCNT",
) -> pl.DataFrame:
    """Calculate domain total variance for multiple groups in a single pass.

    This is a vectorized version of calculate_domain_total_variance that
    computes variance for all groups simultaneously using Polars group_by
    operations, avoiding the N+1 query pattern of iterating through groups.

    Implements the exact Bechtold & Patterson (2005) post-stratified
    variance formula (V1 + V2) when B&P columns are available, falling
    back to the simplified formula otherwise.

    Parameters
    ----------
    plot_data : pl.DataFrame
        Plot-level data with group columns, stratum assignment, and values.
        Must contain PLT_CN, y_col, stratum_col, weight_col, and group_cols.
    group_cols : list[str]
        Columns to group results by (e.g., ["SPCD"] for by-species)
    y_col : str
        Column name for Y values (the metric being estimated)
    x_col : str, default 'x_i'
        Column name for X values (area proportion, for per-acre SE calculation)
    stratum_col : str, default 'STRATUM_CN'
        Column name for stratum assignment
    weight_col : str, default 'EXPNS'
        Column name for stratum weights (expansion factors)
    estn_unit_col : str, default 'ESTN_UNIT_CN'
        Column name for estimation unit identifier
    stratum_wgt_col : str, default 'STRATUM_WGT'
        Column name for phase 1 stratum weight
    area_used_col : str, default 'AREA_USED'
        Column name for total area of estimation unit
    p2pointcnt_col : str, default 'P2POINTCNT'
        Column name for number of phase 2 plots in stratum

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
        # No grouping - fall back to scalar calculation with ratio variance
        if x_col in plot_data.columns:
            ratio_stats = calculate_ratio_of_means_variance(
                plot_data,
                y_col,
                x_col,
                stratum_col,
                weight_col,
                estn_unit_col,
                stratum_wgt_col,
                area_used_col,
                p2pointcnt_col,
            )
            return pl.DataFrame(
                {
                    "se_acre": [ratio_stats["se_ratio"]],
                    "se_total": [ratio_stats["se_total"]],
                    "variance_acre": [ratio_stats["variance_ratio"]],
                    "variance_total": [ratio_stats["variance_total"]],
                }
            )
        else:
            var_stats = calculate_domain_total_variance(
                plot_data,
                y_col,
                stratum_col,
                weight_col,
                estn_unit_col,
                stratum_wgt_col,
                area_used_col,
                p2pointcnt_col,
            )
            return pl.DataFrame(
                {
                    "se_acre": [0.0],
                    "se_total": [var_stats["se_total"]],
                    "variance_acre": [0.0],
                    "variance_total": [var_stats["variance_total"]],
                }
            )

    # Check if exact B&P columns are available
    has_bp_cols = all(
        col in plot_data.columns
        for col in [estn_unit_col, stratum_wgt_col, area_used_col, p2pointcnt_col]
    )

    if has_bp_cols:
        return _calculate_grouped_exact_bp_variance(
            plot_data,
            valid_group_cols,
            y_col,
            x_col,
            stratum_col,
            weight_col,
            estn_unit_col,
            stratum_wgt_col,
            area_used_col,
            p2pointcnt_col,
        )

    # Fallback: simplified formula
    return _calculate_grouped_simplified_variance(
        plot_data,
        valid_group_cols,
        y_col,
        x_col,
        stratum_col,
        weight_col,
    )


def _calculate_grouped_exact_bp_variance(
    plot_data: pl.DataFrame,
    valid_group_cols: list[str],
    y_col: str,
    x_col: str,
    stratum_col: str,
    weight_col: str,
    estn_unit_col: str,
    stratum_wgt_col: str,
    area_used_col: str,
    p2pointcnt_col: str,
) -> pl.DataFrame:
    """Exact B&P grouped variance with V1 + V2 per estimation unit.

    Computes V(Y), V(X), and Cov(Y,X) per group and uses the ratio-of-means
    formula for per-acre SE: V(R) = (1/X^2) * [V(Y) + R^2*V(X) - 2*R*Cov(Y,X)]
    """
    # Group by (group_cols + EU + stratum) to get stratum stats
    stratum_group_cols = valid_group_cols + [estn_unit_col, stratum_col]

    agg_exprs = [
        pl.count("PLT_CN").alias("n_h_actual"),
        pl.mean(y_col).alias("ybar_h"),
        pl.var(y_col, ddof=1).alias("s2_yh"),
        pl.first(weight_col).cast(pl.Float64).alias("w_h"),
        pl.first(stratum_wgt_col).cast(pl.Float64).alias("W_h"),
        pl.first(area_used_col).cast(pl.Float64).alias("A"),
        pl.first(p2pointcnt_col).cast(pl.Float64).alias("n_h_design"),
    ]

    # Add X-related stats for ratio variance
    if x_col in plot_data.columns:
        agg_exprs.extend(
            [
                pl.mean(x_col).alias("xbar_h"),
                pl.var(x_col, ddof=1).alias("s2_xh"),
                pl.cov(y_col, x_col, ddof=1).alias("cov_yxh"),
            ]
        )

    strata_stats = plot_data.group_by(stratum_group_cols).agg(agg_exprs)

    # Handle null variances/covariances
    fill_exprs = [
        pl.col("s2_yh").fill_null(0.0).cast(pl.Float64).alias("s2_yh"),
        pl.col("ybar_h").fill_null(0.0).cast(pl.Float64).alias("ybar_h"),
    ]
    if x_col in plot_data.columns:
        fill_exprs.extend(
            [
                pl.col("s2_xh").fill_null(0.0).cast(pl.Float64).alias("s2_xh"),
                pl.col("cov_yxh").fill_null(0.0).cast(pl.Float64).alias("cov_yxh"),
                pl.col("xbar_h").fill_null(0.0).cast(pl.Float64).alias("xbar_h"),
            ]
        )
    strata_stats = strata_stats.with_columns(fill_exprs)

    # Calculate n (total plots per EU per group)
    eu_group_cols = valid_group_cols + [estn_unit_col]
    eu_totals = strata_stats.group_by(eu_group_cols).agg(
        pl.sum("n_h_actual").alias("n_eu")
    )
    strata_stats = strata_stats.join(eu_totals, on=eu_group_cols, how="left")

    # v_yh, v_xh, c_yxh = s²/n_h for strata with n_h > 1
    v_exprs = [
        pl.when(pl.col("n_h_actual") > 1)
        .then(pl.col("s2_yh") / pl.col("n_h_design"))
        .otherwise(0.0)
        .alias("v_yh"),
    ]
    if x_col in plot_data.columns:
        v_exprs.extend(
            [
                pl.when(pl.col("n_h_actual") > 1)
                .then(pl.col("s2_xh") / pl.col("n_h_design"))
                .otherwise(0.0)
                .alias("v_xh"),
                pl.when(pl.col("n_h_actual") > 1)
                .then(pl.col("cov_yxh") / pl.col("n_h_design"))
                .otherwise(0.0)
                .alias("c_yxh"),
            ]
        )
    strata_stats = strata_stats.with_columns(v_exprs)

    # V1 and V2 components for Y, X, and Cov
    comp_exprs = [
        (pl.col("W_h") * pl.col("v_yh")).alias("v1_y"),
        ((1.0 - pl.col("W_h")) * pl.col("v_yh")).alias("v2_y"),
    ]
    if x_col in plot_data.columns:
        comp_exprs.extend(
            [
                (pl.col("W_h") * pl.col("v_xh")).alias("v1_x"),
                ((1.0 - pl.col("W_h")) * pl.col("v_xh")).alias("v2_x"),
                (pl.col("W_h") * pl.col("c_yxh")).alias("v1_cov"),
                ((1.0 - pl.col("W_h")) * pl.col("c_yxh")).alias("v2_cov"),
            ]
        )
    strata_stats = strata_stats.with_columns(comp_exprs)

    # Aggregate to EU level per group
    eu_agg_exprs = [
        pl.sum("v1_y").alias("sum_v1_y"),
        pl.sum("v2_y").alias("sum_v2_y"),
        pl.first("A").alias("A"),
        pl.first("n_eu").alias("n"),
    ]
    if x_col in plot_data.columns:
        eu_agg_exprs.extend(
            [
                pl.sum("v1_x").alias("sum_v1_x"),
                pl.sum("v2_x").alias("sum_v2_x"),
                pl.sum("v1_cov").alias("sum_v1_cov"),
                pl.sum("v2_cov").alias("sum_v2_cov"),
            ]
        )

    eu_variance = strata_stats.group_by(eu_group_cols).agg(eu_agg_exprs)

    # V_EU = (A²/n) × sum_v1 + (A²/n²) × sum_v2 for Y
    eu_v_exprs = [
        (
            (pl.col("A") ** 2 / pl.col("n")) * pl.col("sum_v1_y")
            + (pl.col("A") ** 2 / pl.col("n") ** 2) * pl.col("sum_v2_y")
        ).alias("V_y_EU"),
    ]
    if x_col in plot_data.columns:
        eu_v_exprs.extend(
            [
                (
                    (pl.col("A") ** 2 / pl.col("n")) * pl.col("sum_v1_x")
                    + (pl.col("A") ** 2 / pl.col("n") ** 2) * pl.col("sum_v2_x")
                ).alias("V_x_EU"),
                (
                    (pl.col("A") ** 2 / pl.col("n")) * pl.col("sum_v1_cov")
                    + (pl.col("A") ** 2 / pl.col("n") ** 2) * pl.col("sum_v2_cov")
                ).alias("Cov_EU"),
            ]
        )
    eu_variance = eu_variance.with_columns(eu_v_exprs)

    # Sum across EUs per group - need total_y and total_x per group too
    # Compute total_y and total_x from strata_stats
    totals_by_group = strata_stats.group_by(valid_group_cols).agg(
        [
            (pl.col("ybar_h") * pl.col("w_h") * pl.col("n_h_actual"))
            .sum()
            .alias("total_y"),
        ]
        + (
            [
                (pl.col("xbar_h") * pl.col("w_h") * pl.col("n_h_actual"))
                .sum()
                .alias("total_x")
            ]
            if x_col in plot_data.columns
            else []
        )
    )

    group_agg_exprs = [
        pl.sum("V_y_EU").alias("variance_total"),
    ]
    if x_col in plot_data.columns:
        group_agg_exprs.extend(
            [
                pl.sum("V_x_EU").alias("variance_x"),
                pl.sum("Cov_EU").alias("covariance"),
            ]
        )

    variance_by_group = eu_variance.group_by(valid_group_cols).agg(group_agg_exprs)

    # Join totals
    variance_by_group = variance_by_group.join(
        totals_by_group, on=valid_group_cols, how="left"
    )

    # Clamp variance_total and compute SE
    variance_by_group = variance_by_group.with_columns(
        [
            pl.when(pl.col("variance_total") < 0)
            .then(0.0)
            .otherwise(pl.col("variance_total"))
            .alias("variance_total"),
        ]
    ).with_columns(
        [
            pl.col("variance_total").sqrt().alias("se_total"),
        ]
    )

    # Compute ratio variance for per-acre SE
    if x_col in plot_data.columns:
        variance_by_group = (
            variance_by_group.with_columns(
                [
                    pl.when(pl.col("total_x") > 0)
                    .then(pl.col("total_y") / pl.col("total_x"))
                    .otherwise(0.0)
                    .alias("ratio"),
                ]
            )
            .with_columns(
                [
                    pl.when(pl.col("total_x") > 0)
                    .then(
                        (1.0 / pl.col("total_x") ** 2)
                        * (
                            pl.col("variance_total")
                            + pl.col("ratio") ** 2 * pl.col("variance_x")
                            - 2.0 * pl.col("ratio") * pl.col("covariance")
                        )
                    )
                    .otherwise(0.0)
                    .alias("variance_acre"),
                ]
            )
            .with_columns(
                [
                    # Clamp to zero if negative (numerical precision)
                    pl.when(pl.col("variance_acre") < 0)
                    .then(0.0)
                    .otherwise(pl.col("variance_acre"))
                    .alias("variance_acre"),
                ]
            )
            .with_columns(
                [
                    pl.col("variance_acre").sqrt().alias("se_acre"),
                ]
            )
        )
    else:
        variance_by_group = variance_by_group.with_columns(
            [
                pl.lit(0.0).alias("se_acre"),
                pl.lit(0.0).alias("variance_acre"),
            ]
        )

    result_cols = valid_group_cols + [
        "se_acre",
        "se_total",
        "variance_acre",
        "variance_total",
    ]
    return variance_by_group.select(result_cols)


def _calculate_grouped_simplified_variance(
    plot_data: pl.DataFrame,
    valid_group_cols: list[str],
    y_col: str,
    x_col: str,
    stratum_col: str,
    weight_col: str,
) -> pl.DataFrame:
    """Simplified grouped variance formula with ratio-of-means for per-acre SE.

    V(Y) = Σ_h W_h² × s²_yh × n_h
    V(R) = (1/X^2) * Σ_h [w_h^2 * n_h * (s2_y - 2*R*cov_yx + R^2*s2_x)]
    """
    stratum_group_cols = valid_group_cols + [stratum_col]

    agg_exprs = [
        pl.count("PLT_CN").alias("n_h"),
        pl.mean(y_col).alias("ybar_h"),
        pl.var(y_col, ddof=1).alias("s2_yh"),
        pl.first(weight_col).cast(pl.Float64).alias("w_h"),
    ]

    if x_col in plot_data.columns:
        agg_exprs.extend(
            [
                pl.mean(x_col).alias("xbar_h"),
                pl.var(x_col, ddof=1).alias("s2_xh"),
                pl.cov(y_col, x_col, ddof=1).alias("cov_yxh"),
            ]
        )

    strata_stats = plot_data.group_by(stratum_group_cols).agg(agg_exprs)

    fill_exprs = [
        pl.col("s2_yh").fill_null(0.0).cast(pl.Float64).alias("s2_yh"),
        pl.col("ybar_h").fill_null(0.0).cast(pl.Float64).alias("ybar_h"),
    ]
    if x_col in plot_data.columns:
        fill_exprs.extend(
            [
                pl.col("s2_xh").fill_null(0.0).cast(pl.Float64).alias("s2_xh"),
                pl.col("cov_yxh").fill_null(0.0).cast(pl.Float64).alias("cov_yxh"),
                pl.col("xbar_h").fill_null(0.0).cast(pl.Float64).alias("xbar_h"),
            ]
        )
    strata_stats = strata_stats.with_columns(fill_exprs)

    # Variance of Y total and totals
    strata_stats = strata_stats.with_columns(
        [
            pl.when(pl.col("n_h") > 1)
            .then(pl.col("w_h") ** 2 * pl.col("s2_yh") * pl.col("n_h"))
            .otherwise(0.0)
            .alias("v_y_h"),
            (pl.col("ybar_h") * pl.col("w_h") * pl.col("n_h")).alias("total_y_h"),
        ]
        + (
            [(pl.col("xbar_h") * pl.col("w_h") * pl.col("n_h")).alias("total_x_h")]
            if x_col in plot_data.columns
            else []
        )
    )

    group_agg_exprs = [
        pl.sum("v_y_h").alias("variance_total"),
        pl.sum("total_y_h").alias("total_y"),
        pl.sum("n_h").alias("n_plots"),
    ]
    if x_col in plot_data.columns:
        group_agg_exprs.append(pl.sum("total_x_h").alias("total_x"))

    variance_by_group = strata_stats.group_by(valid_group_cols).agg(group_agg_exprs)

    # Clamp and compute SE total
    variance_by_group = variance_by_group.with_columns(
        [
            pl.when(pl.col("variance_total") < 0)
            .then(0.0)
            .otherwise(pl.col("variance_total"))
            .alias("variance_total"),
        ]
    ).with_columns(
        [
            pl.col("variance_total").sqrt().alias("se_total"),
        ]
    )

    # Compute ratio variance for per-acre SE
    if x_col in plot_data.columns:
        # Need to compute ratio per group for the simplified ratio variance formula
        variance_by_group = variance_by_group.with_columns(
            [
                pl.when(pl.col("total_x") > 0)
                .then(pl.col("total_y") / pl.col("total_x"))
                .otherwise(0.0)
                .alias("ratio"),
            ]
        )

        # Compute ratio variance per stratum: w_h^2 * n_h * (s2_y - 2R*cov + R^2*s2_x)
        # We need the ratio per group joined back to strata_stats
        strata_with_ratio = strata_stats.join(
            variance_by_group.select(valid_group_cols + ["ratio", "total_x"]),
            on=valid_group_cols,
            how="left",
        )

        strata_with_ratio = strata_with_ratio.with_columns(
            [
                pl.when(pl.col("n_h") > 1)
                .then(
                    pl.col("w_h") ** 2
                    * pl.col("n_h")
                    * (
                        pl.col("s2_yh")
                        - 2.0 * pl.col("ratio") * pl.col("cov_yxh")
                        + pl.col("ratio") ** 2 * pl.col("s2_xh")
                    )
                )
                .otherwise(0.0)
                .alias("v_ratio_h"),
            ]
        )

        ratio_var_by_group = strata_with_ratio.group_by(valid_group_cols).agg(
            [
                pl.sum("v_ratio_h").alias("total_ratio_var"),
                pl.first("total_x").alias("total_x"),
            ]
        )

        ratio_var_by_group = (
            ratio_var_by_group.with_columns(
                [
                    pl.when(pl.col("total_x") > 0)
                    .then(pl.col("total_ratio_var") / pl.col("total_x") ** 2)
                    .otherwise(0.0)
                    .alias("variance_acre"),
                ]
            )
            .with_columns(
                [
                    pl.when(pl.col("variance_acre") < 0)
                    .then(0.0)
                    .otherwise(pl.col("variance_acre"))
                    .alias("variance_acre"),
                ]
            )
            .with_columns(
                [
                    pl.col("variance_acre").sqrt().alias("se_acre"),
                ]
            )
        )

        variance_by_group = variance_by_group.join(
            ratio_var_by_group.select(valid_group_cols + ["variance_acre", "se_acre"]),
            on=valid_group_cols,
            how="left",
        )
    else:
        variance_by_group = variance_by_group.with_columns(
            [
                pl.lit(0.0).alias("se_acre"),
                pl.lit(0.0).alias("variance_acre"),
            ]
        )

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
    estn_unit_col: str = "ESTN_UNIT_CN",
    stratum_wgt_col: str = "STRATUM_WGT",
    area_used_col: str = "AREA_USED",
    p2pointcnt_col: str = "P2POINTCNT",
) -> dict[str, float]:
    """Calculate variance for domain total estimation.

    Implements the exact post-stratified variance formula from Bechtold &
    Patterson (2005), Section 4.2, pp. 55-60, as used by rFIA's unitVar().

    For each estimation unit EU:

        n = Σ_h n_h  (total phase 2 plots in EU)
        A = AREA_USED (total area of the estimation unit)

        For each stratum h with n_h > 1:
            v_h = s²_yh / n_h  (variance of stratum mean)

        V1 = (A²/n) × Σ_h W_h × v_h       (main within-stratum term)
        V2 = (A²/n²) × Σ_h (1 - W_h) × v_h  (post-stratification correction)
        V_EU = V1 + V2

    V_total = Σ_EU V_EU

    Where:
    - A = AREA_USED (total area of the estimation unit in acres)
    - n = total number of phase 2 plots in the estimation unit
    - W_h = STRATUM_WGT = P1POINTCNT / P1PNTCNT_EU (phase 1 stratum weight)
    - s²_yh = sample variance within stratum h (ddof=1)
    - n_h = P2POINTCNT = number of phase 2 plots in stratum h

    Parameters
    ----------
    plot_data : pl.DataFrame
        Plot-level data with columns for Y values, stratum assignment,
        and weights. Must contain at minimum:
        - PLT_CN: Plot identifier
        - y_col: Attribute values (expanded to per-acre or total)
        - stratum_col: Stratum assignment
        - weight_col: Expansion factors
        - estn_unit_col: Estimation unit identifier
        - stratum_wgt_col: Phase 1 stratum weight (W_h)
        - area_used_col: Total area of the estimation unit (acres)
        - p2pointcnt_col: Number of phase 2 plots in stratum
    y_col : str
        Column name for Y values
    stratum_col : str, default 'STRATUM_CN'
        Column name for stratum assignment
    weight_col : str, default 'EXPNS'
        Column name for stratum weights (expansion factors)
    estn_unit_col : str, default 'ESTN_UNIT_CN'
        Column name for estimation unit identifier
    stratum_wgt_col : str, default 'STRATUM_WGT'
        Column name for phase 1 stratum weight
    area_used_col : str, default 'AREA_USED'
        Column name for total area of estimation unit
    p2pointcnt_col : str, default 'P2POINTCNT'
        Column name for number of phase 2 plots in stratum

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
    - Multiple estimation units (variance computed per-EU then summed)

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
        plot_data = plot_data.with_columns(pl.lit(1).alias("_STRATUM"))
        stratum_col = "_STRATUM"

    # Check if exact B&P columns are available
    has_bp_cols = all(
        col in plot_data.columns
        for col in [estn_unit_col, stratum_wgt_col, area_used_col, p2pointcnt_col]
    )

    if has_bp_cols:
        return _calculate_exact_bp_variance(
            plot_data,
            y_col,
            stratum_col,
            weight_col,
            estn_unit_col,
            stratum_wgt_col,
            area_used_col,
            p2pointcnt_col,
        )

    # Fallback: simplified formula for backward compatibility (e.g., area estimator)
    return _calculate_simplified_variance(
        plot_data,
        y_col,
        stratum_col,
        weight_col,
    )


def _calculate_exact_bp_variance(
    plot_data: pl.DataFrame,
    y_col: str,
    stratum_col: str,
    weight_col: str,
    estn_unit_col: str,
    stratum_wgt_col: str,
    area_used_col: str,
    p2pointcnt_col: str,
) -> dict[str, float]:
    """Exact Bechtold & Patterson post-stratified variance (V1 + V2).

    Computes variance per estimation unit then sums across EUs.
    """
    # Calculate stratum-level statistics within each EU
    strata_stats = plot_data.group_by([estn_unit_col, stratum_col]).agg(
        [
            pl.count("PLT_CN").alias("n_h_actual"),
            pl.mean(y_col).alias("ybar_h"),
            pl.var(y_col, ddof=1).alias("s2_yh"),
            pl.first(weight_col).cast(pl.Float64).alias("w_h"),
            pl.first(stratum_wgt_col).cast(pl.Float64).alias("W_h"),
            pl.first(area_used_col).cast(pl.Float64).alias("A"),
            pl.first(p2pointcnt_col).cast(pl.Float64).alias("n_h_design"),
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

    # Calculate total Y using expansion factors
    total_y = (
        strata_stats["ybar_h"] * strata_stats["w_h"] * strata_stats["n_h_actual"]
    ).sum()

    n_strata = len(strata_stats)
    n_plots = int(strata_stats["n_h_actual"].sum())

    # Calculate n (total plots per EU) for exact formula
    eu_totals = strata_stats.group_by(estn_unit_col).agg(
        pl.sum("n_h_actual").alias("n_eu")
    )
    strata_stats = strata_stats.join(eu_totals, on=estn_unit_col, how="left")

    # B&P post-stratified variance uses s²_h directly (NOT s²_h/n_h).
    # The formula V(ȳ_ps) = (1/n)Σ W_h s²_h + (1/n²)Σ (1-W_h) s²_h
    # already accounts for sample size through the A²/n and A²/n² terms.
    strata_stats = strata_stats.with_columns(
        [
            pl.when(pl.col("n_h_actual") > 1)
            .then(pl.col("s2_yh"))
            .otherwise(0.0)
            .alias("v_h"),
        ]
    )

    # Calculate V1 and V2 components per stratum
    # V1_component = W_h × v_h
    # V2_component = (1 - W_h) × v_h
    strata_stats = strata_stats.with_columns(
        [
            (pl.col("W_h") * pl.col("v_h")).alias("v1_component"),
            ((1.0 - pl.col("W_h")) * pl.col("v_h")).alias("v2_component"),
        ]
    )

    # Aggregate to EU level
    eu_variance = strata_stats.group_by(estn_unit_col).agg(
        [
            pl.sum("v1_component").alias("sum_v1"),
            pl.sum("v2_component").alias("sum_v2"),
            pl.first("A").alias("A"),
            pl.first("n_eu").alias("n"),
        ]
    )

    # Calculate V_EU = (A²/n) × sum_v1 + (A²/n²) × sum_v2
    eu_variance = eu_variance.with_columns(
        [
            (
                (pl.col("A") ** 2 / pl.col("n")) * pl.col("sum_v1")
                + (pl.col("A") ** 2 / pl.col("n") ** 2) * pl.col("sum_v2")
            ).alias("V_EU"),
        ]
    )

    # Sum across estimation units
    variance_total = eu_variance["V_EU"].drop_nans().sum()
    if variance_total is None or variance_total < 0:
        variance_total = 0.0

    se_total = variance_total**0.5

    return {
        "variance_total": variance_total,
        "se_total": se_total,
        "total_y": total_y,
        "n_strata": n_strata,
        "n_plots": n_plots,
    }


def _calculate_simplified_variance(
    plot_data: pl.DataFrame,
    y_col: str,
    stratum_col: str,
    weight_col: str,
) -> dict[str, float]:
    """Simplified variance formula V = Σ_h W_h² × s²_yh × n_h.

    Used as fallback when exact B&P columns are not available
    (e.g., area estimator which has its own variance path).
    """
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

    # Calculate population total
    total_y = (strata_stats["ybar_h"] * strata_stats["w_h"] * strata_stats["n_h"]).sum()

    # Filter out single-plot strata
    strata_with_variance = strata_stats.filter(pl.col("n_h") > 1)

    # V(Ŷ) = Σ_h w_h² × s²_yh × n_h
    variance_components = strata_with_variance.with_columns(
        [(pl.col("w_h") ** 2 * pl.col("s2_yh") * pl.col("n_h")).alias("v_h")]
    )

    variance_total = variance_components["v_h"].drop_nans().sum()
    if variance_total is None or variance_total < 0:
        variance_total = 0.0

    se_total = variance_total**0.5

    return {
        "variance_total": variance_total,
        "se_total": se_total,
        "total_y": total_y,
        "n_strata": len(strata_stats),
        "n_plots": int(strata_stats["n_h"].sum()),
    }


def calculate_ratio_of_means_variance(
    plot_data: pl.DataFrame,
    y_col: str,
    x_col: str = "x_i",
    stratum_col: str = "STRATUM_CN",
    weight_col: str = "EXPNS",
    estn_unit_col: str = "ESTN_UNIT_CN",
    stratum_wgt_col: str = "STRATUM_WGT",
    area_used_col: str = "AREA_USED",
    p2pointcnt_col: str = "P2POINTCNT",
) -> dict[str, float]:
    """Calculate variance for ratio-of-means estimation (per-acre estimates).

    Implements the ratio-of-means variance formula from Bechtold & Patterson
    (2005), Section 4.2:

        V(R) = (1/X^2) * [V(Y) + R^2 * V(X) - 2*R*Cov(Y,X)]

    Where R = Y/X is the ratio (per-acre estimate), Y is the tree attribute
    total, X is the total area, and V(Y), V(X), Cov(Y,X) are computed using
    the exact post-stratified B&P formula or simplified fallback.

    Since Y and X are estimated from the same sample plots, their covariance
    is typically positive (more forest area -> more volume), which means
    the ratio variance is typically less than se_total^2 / total_x^2.

    Parameters
    ----------
    plot_data : pl.DataFrame
        Plot-level data with y_col and x_col values, plus stratification.
    y_col : str
        Column name for Y values (tree attribute per plot)
    x_col : str, default 'x_i'
        Column name for X values (area proportion per plot)
    stratum_col : str, default 'STRATUM_CN'
        Column name for stratum assignment
    weight_col : str, default 'EXPNS'
        Column name for stratum weights (expansion factors)
    estn_unit_col : str, default 'ESTN_UNIT_CN'
        Column name for estimation unit identifier
    stratum_wgt_col : str, default 'STRATUM_WGT'
        Column name for phase 1 stratum weight
    area_used_col : str, default 'AREA_USED'
        Column name for total area of estimation unit
    p2pointcnt_col : str, default 'P2POINTCNT'
        Column name for number of phase 2 plots in stratum

    Returns
    -------
    dict
        Dictionary with keys:
        - variance_total: Variance of total Y estimate V(Y)
        - se_total: Standard error of total Y estimate
        - variance_ratio: Variance of ratio estimate V(R)
        - se_ratio: Standard error of ratio estimate
        - total_y: Estimated total Y
        - total_x: Estimated total X (area)
        - ratio: Estimated ratio R = Y/X
    """
    if stratum_col not in plot_data.columns:
        plot_data = plot_data.with_columns(pl.lit(1).alias("_STRATUM"))
        stratum_col = "_STRATUM"

    has_bp_cols = all(
        col in plot_data.columns
        for col in [estn_unit_col, stratum_wgt_col, area_used_col, p2pointcnt_col]
    )

    if has_bp_cols:
        return _calculate_exact_bp_ratio_variance(
            plot_data,
            y_col,
            x_col,
            stratum_col,
            weight_col,
            estn_unit_col,
            stratum_wgt_col,
            area_used_col,
            p2pointcnt_col,
        )

    return _calculate_simplified_ratio_variance(
        plot_data,
        y_col,
        x_col,
        stratum_col,
        weight_col,
    )


def _calculate_exact_bp_ratio_variance(
    plot_data: pl.DataFrame,
    y_col: str,
    x_col: str,
    stratum_col: str,
    weight_col: str,
    estn_unit_col: str,
    stratum_wgt_col: str,
    area_used_col: str,
    p2pointcnt_col: str,
) -> dict[str, float]:
    """Exact B&P ratio-of-means variance with V1+V2 per estimation unit.

    Computes V(Y), V(X), and Cov(Y,X) using the exact post-stratified
    formula, then applies the ratio variance formula:

        V(R) = (1/X^2) * [V(Y) + R^2 * V(X) - 2*R*Cov(Y,X)]
    """
    # Stratum-level statistics for Y, X, and their covariance
    strata_stats = plot_data.group_by([estn_unit_col, stratum_col]).agg(
        [
            pl.count("PLT_CN").alias("n_h_actual"),
            pl.mean(y_col).alias("ybar_h"),
            pl.mean(x_col).alias("xbar_h"),
            pl.var(y_col, ddof=1).alias("s2_yh"),
            pl.var(x_col, ddof=1).alias("s2_xh"),
            pl.cov(y_col, x_col, ddof=1).alias("cov_yxh"),
            pl.first(weight_col).cast(pl.Float64).alias("w_h"),
            pl.first(stratum_wgt_col).cast(pl.Float64).alias("W_h"),
            pl.first(area_used_col).cast(pl.Float64).alias("A"),
            pl.first(p2pointcnt_col).cast(pl.Float64).alias("n_h_design"),
        ]
    )

    # Handle null variances/covariances
    strata_stats = strata_stats.with_columns(
        [
            pl.col("s2_yh").fill_null(0.0).cast(pl.Float64).alias("s2_yh"),
            pl.col("s2_xh").fill_null(0.0).cast(pl.Float64).alias("s2_xh"),
            pl.col("cov_yxh").fill_null(0.0).cast(pl.Float64).alias("cov_yxh"),
            pl.col("ybar_h").fill_null(0.0).cast(pl.Float64).alias("ybar_h"),
            pl.col("xbar_h").fill_null(0.0).cast(pl.Float64).alias("xbar_h"),
        ]
    )

    # Total Y and X using expansion factors
    total_y = (
        strata_stats["ybar_h"] * strata_stats["w_h"] * strata_stats["n_h_actual"]
    ).sum()
    total_x = (
        strata_stats["xbar_h"] * strata_stats["w_h"] * strata_stats["n_h_actual"]
    ).sum()

    # n per EU
    eu_totals = strata_stats.group_by(estn_unit_col).agg(
        pl.sum("n_h_actual").alias("n_eu")
    )
    strata_stats = strata_stats.join(eu_totals, on=estn_unit_col, how="left")

    # B&P post-stratified variance uses s²_h directly (NOT s²_h/n_h).
    # The formula V(ȳ_ps) = (1/n)Σ W_h s²_h + (1/n²)Σ (1-W_h) s²_h
    # already accounts for sample size through the A²/n and A²/n² terms.
    strata_stats = strata_stats.with_columns(
        [
            pl.when(pl.col("n_h_actual") > 1)
            .then(pl.col("s2_yh"))
            .otherwise(0.0)
            .alias("v_yh"),
            pl.when(pl.col("n_h_actual") > 1)
            .then(pl.col("s2_xh"))
            .otherwise(0.0)
            .alias("v_xh"),
            pl.when(pl.col("n_h_actual") > 1)
            .then(pl.col("cov_yxh"))
            .otherwise(0.0)
            .alias("c_yxh"),
        ]
    )

    # V1 and V2 components for Y, X, and Cov
    strata_stats = strata_stats.with_columns(
        [
            # Y variance components
            (pl.col("W_h") * pl.col("v_yh")).alias("v1_y"),
            ((1.0 - pl.col("W_h")) * pl.col("v_yh")).alias("v2_y"),
            # X variance components
            (pl.col("W_h") * pl.col("v_xh")).alias("v1_x"),
            ((1.0 - pl.col("W_h")) * pl.col("v_xh")).alias("v2_x"),
            # Covariance components
            (pl.col("W_h") * pl.col("c_yxh")).alias("v1_cov"),
            ((1.0 - pl.col("W_h")) * pl.col("c_yxh")).alias("v2_cov"),
        ]
    )

    # Aggregate to EU level
    eu_variance = strata_stats.group_by(estn_unit_col).agg(
        [
            pl.sum("v1_y").alias("sum_v1_y"),
            pl.sum("v2_y").alias("sum_v2_y"),
            pl.sum("v1_x").alias("sum_v1_x"),
            pl.sum("v2_x").alias("sum_v2_x"),
            pl.sum("v1_cov").alias("sum_v1_cov"),
            pl.sum("v2_cov").alias("sum_v2_cov"),
            pl.first("A").alias("A"),
            pl.first("n_eu").alias("n"),
        ]
    )

    # V_EU = (A²/n) * sum_v1 + (A²/n²) * sum_v2 for Y, X, and Cov
    eu_variance = eu_variance.with_columns(
        [
            (
                (pl.col("A") ** 2 / pl.col("n")) * pl.col("sum_v1_y")
                + (pl.col("A") ** 2 / pl.col("n") ** 2) * pl.col("sum_v2_y")
            ).alias("V_y_EU"),
            (
                (pl.col("A") ** 2 / pl.col("n")) * pl.col("sum_v1_x")
                + (pl.col("A") ** 2 / pl.col("n") ** 2) * pl.col("sum_v2_x")
            ).alias("V_x_EU"),
            (
                (pl.col("A") ** 2 / pl.col("n")) * pl.col("sum_v1_cov")
                + (pl.col("A") ** 2 / pl.col("n") ** 2) * pl.col("sum_v2_cov")
            ).alias("Cov_EU"),
        ]
    )

    # Sum across EUs
    variance_y = eu_variance["V_y_EU"].drop_nans().sum()
    variance_x = eu_variance["V_x_EU"].drop_nans().sum()
    covariance = eu_variance["Cov_EU"].drop_nans().sum()

    if variance_y is None or variance_y < 0:
        variance_y = 0.0

    se_total = variance_y**0.5

    # Ratio variance: V(R) = (1/X^2) * [V(Y) + R^2*V(X) - 2*R*Cov(Y,X)]
    if total_x is not None and total_x > 0:
        ratio = total_y / total_x
        variance_ratio = (1.0 / total_x**2) * (
            variance_y + ratio**2 * variance_x - 2.0 * ratio * covariance
        )
        # Clamp to zero if negative (numerical precision)
        if variance_ratio < 0:
            variance_ratio = 0.0
        se_ratio = variance_ratio**0.5
    else:
        ratio = 0.0
        variance_ratio = 0.0
        se_ratio = 0.0

    return {
        "variance_total": variance_y,
        "se_total": se_total,
        "variance_ratio": variance_ratio,
        "se_ratio": se_ratio,
        "total_y": total_y,
        "total_x": total_x,
        "ratio": ratio,
    }


def _calculate_simplified_ratio_variance(
    plot_data: pl.DataFrame,
    y_col: str,
    x_col: str,
    stratum_col: str,
    weight_col: str,
) -> dict[str, float]:
    """Simplified ratio-of-means variance (no B&P columns).

    Uses the per-stratum formula:
        V(R) = (1/X^2) * Σ_h [w_h^2 * n_h * (s2_y - 2*R*cov_yx + R^2*s2_x)]
    """
    strata_stats = plot_data.group_by(stratum_col).agg(
        [
            pl.count("PLT_CN").alias("n_h"),
            pl.mean(y_col).alias("ybar_h"),
            pl.mean(x_col).alias("xbar_h"),
            pl.var(y_col, ddof=1).alias("s2_y"),
            pl.var(x_col, ddof=1).alias("s2_x"),
            pl.cov(y_col, x_col, ddof=1).alias("cov_yx"),
            pl.first(weight_col).cast(pl.Float64).alias("w_h"),
        ]
    )

    strata_stats = strata_stats.with_columns(
        [
            pl.col("s2_y").fill_null(0.0).cast(pl.Float64).alias("s2_y"),
            pl.col("s2_x").fill_null(0.0).cast(pl.Float64).alias("s2_x"),
            pl.col("cov_yx").fill_null(0.0).cast(pl.Float64).alias("cov_yx"),
            pl.col("ybar_h").fill_null(0.0).cast(pl.Float64).alias("ybar_h"),
            pl.col("xbar_h").fill_null(0.0).cast(pl.Float64).alias("xbar_h"),
        ]
    )

    # Total Y and X
    total_y = (strata_stats["ybar_h"] * strata_stats["w_h"] * strata_stats["n_h"]).sum()
    total_x = (strata_stats["xbar_h"] * strata_stats["w_h"] * strata_stats["n_h"]).sum()

    # Variance of Y total (simplified formula)
    strata_with_variance = strata_stats.filter(pl.col("n_h") > 1)
    variance_y_components = strata_with_variance.with_columns(
        [(pl.col("w_h") ** 2 * pl.col("s2_y") * pl.col("n_h")).alias("v_y_h")]
    )
    variance_y = variance_y_components["v_y_h"].drop_nans().sum()
    if variance_y is None or variance_y < 0:
        variance_y = 0.0

    se_total = variance_y**0.5

    # Ratio variance using covariance
    if total_x is not None and total_x > 0:
        ratio = total_y / total_x

        # V(total_ratio) = Σ_h [w_h^2 * n_h * (s2_y - 2*R*cov_yx + R^2*s2_x)]
        ratio_var_components = strata_with_variance.with_columns(
            [
                (
                    pl.col("w_h") ** 2
                    * pl.col("n_h")
                    * (
                        pl.col("s2_y")
                        - 2.0 * ratio * pl.col("cov_yx")
                        + ratio**2 * pl.col("s2_x")
                    )
                ).alias("v_ratio_h")
            ]
        )
        total_ratio_variance = ratio_var_components["v_ratio_h"].drop_nans().sum()
        if total_ratio_variance is None or total_ratio_variance < 0:
            total_ratio_variance = 0.0

        variance_ratio = total_ratio_variance / total_x**2
        if variance_ratio < 0:
            variance_ratio = 0.0
        se_ratio = variance_ratio**0.5
    else:
        ratio = 0.0
        variance_ratio = 0.0
        se_ratio = 0.0

    return {
        "variance_total": variance_y,
        "se_total": se_total,
        "variance_ratio": variance_ratio,
        "se_ratio": se_ratio,
        "total_y": total_y,
        "total_x": total_x,
        "ratio": ratio,
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
) -> tuple[float, float]:
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
    tuple[float, float]
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
