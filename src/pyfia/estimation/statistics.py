"""
Statistical calculations for FIA estimation.

Combines variance calculation and statistical expressions into a single module
without unnecessary abstraction layers.

Statistical methodology follows Bechtold & Patterson (2005), Gen. Tech. Rep. SRS-80:
- Ratio-of-means estimator: Chapter 4, Section 4.2 (pp. 55-60)
- Domain estimation: Eq. 4.1, p. 47 (domain indicator function)
- Post-stratified variance: Section 4.2

Reference: https://doi.org/10.2737/SRS-GTR-80
"""

from typing import List, Optional, Tuple

import polars as pl


def calculate_ratio_of_means_variance(
    data: pl.DataFrame,
    response_col: str,
    area_col: str = "AREA_USED",
    strata_col: str = "ESTN_UNIT",
    plot_col: str = "PLT_CN",
    weight_col: str = "EXPNS",
) -> pl.DataFrame:
    """
    Calculate variance using ratio-of-means estimator.

    This is the standard FIA variance calculation following
    Bechtold & Patterson (2005), Chapter 4, Section 4.2 (pp. 55-60).

    Implements post-stratified ratio estimation where EXPNS (expansion factor)
    already incorporates the inverse of sample size (1/n_h). See Eq. 4.8 (p. 53)
    for tree attribute estimation and Section 4.2 for variance formulas.

    Parameters
    ----------
    data : pl.DataFrame
        Data with response values and stratification
    response_col : str
        Column containing response values (e.g., volume, biomass)
    area_col : str
        Column containing area values
    strata_col : str
        Column identifying strata
    plot_col : str
        Column identifying plots
    weight_col : str
        Column containing expansion factors

    Returns
    -------
    pl.DataFrame
        Data with variance estimates added
    """
    # Calculate stratum-level statistics
    strata_stats = data.group_by(strata_col).agg(
        [
            pl.count(plot_col).alias("n_h"),  # plots in stratum
            pl.mean(response_col).alias("ybar_h"),  # mean response
            pl.mean(area_col).alias("abar_h"),  # mean area
            pl.std(response_col, ddof=1).alias("s_y_h"),  # std response
            pl.std(area_col, ddof=1).alias("s_a_h"),  # std area
            # Covariance between response and area
            (
                (pl.col(response_col) - pl.mean(response_col))
                * (pl.col(area_col) - pl.mean(area_col))
            )
            .mean()
            .alias("s_ya_h"),
            pl.first(weight_col).alias("w_h"),  # weight (EXPNS)
        ]
    )

    # Calculate population totals
    pop_totals = strata_stats.select(
        [
            (pl.col("ybar_h") * pl.col("w_h")).sum().alias("Y_total"),
            (pl.col("abar_h") * pl.col("w_h")).sum().alias("A_total"),
            pl.col("w_h").sum().alias("W_total"),
        ]
    )

    # Get scalars (uppercase follows Bechtold & Patterson notation)
    Y_total = pop_totals["Y_total"][0]  # noqa: N806
    A_total = pop_totals["A_total"][0]  # noqa: N806

    # Calculate ratio estimate
    R = Y_total / A_total if A_total > 0 else 0  # noqa: N806

    # Calculate variance components for each stratum
    variance_components = strata_stats.with_columns(
        [
            # Variance of ratio estimator
            (
                (pl.col("w_h") ** 2)
                * (1 - 1 / pl.col("n_h"))
                / pl.col("n_h")
                * (
                    pl.col("s_y_h") ** 2
                    + (R**2) * pl.col("s_a_h") ** 2
                    - 2 * R * pl.col("s_ya_h")
                )
            ).alias("var_h")
        ]
    )

    # Total variance
    total_variance = variance_components["var_h"].sum()

    # Standard error
    se = (total_variance / (A_total**2)) ** 0.5 if A_total > 0 else 0

    # Add to results
    results = pl.DataFrame(
        {
            "ESTIMATE": [R],
            "VARIANCE": [total_variance],
            "SE": [se],
            "SE_PERCENT": [100 * se / R if R > 0 else 0],
        }
    )

    return results


def calculate_post_stratified_variance(
    data: pl.DataFrame,
    response_col: str,
    ps_col: str = "POST_STRATUM",
    weight_col: str = "ADJ_FACTOR",
) -> float:
    """
    Calculate variance with post-stratification.

    Parameters
    ----------
    data : pl.DataFrame
        Data with post-stratification assignments
    response_col : str
        Column with response values
    ps_col : str
        Post-stratum identifier column
    weight_col : str
        Adjustment factor column

    Returns
    -------
    float
        Variance estimate
    """
    # Group by post-stratum
    ps_stats = data.group_by(ps_col).agg(
        [
            pl.count().alias("n_ps"),
            pl.mean(response_col).alias("mean_ps"),
            pl.var(response_col, ddof=1).alias("var_ps"),
            pl.first(weight_col).alias("weight_ps"),
        ]
    )

    # Calculate weighted variance
    weighted_var = ps_stats.select(
        [(pl.col("weight_ps") ** 2 * pl.col("var_ps") / pl.col("n_ps")).sum()]
    ).item()

    return float(weighted_var) if weighted_var is not None else 0.0


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
    Calculate confidence interval.

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
    # Use normal approximation (could use t-distribution for small samples)
    if confidence == 0.95:
        z = 1.96
    elif confidence == 0.90:
        z = 1.645
    elif confidence == 0.99:
        z = 2.576
    else:
        # For other confidence levels, would need scipy.stats
        z = 1.96  # Default to 95%

    lower = estimate - z * se
    upper = estimate + z * se

    return lower, upper


def calculate_cv(estimate: float, se: float) -> float:
    """
    Calculate coefficient of variation.

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


def calculate_domain_variance(
    data: pl.DataFrame, domain_col: str, response_col: str, weight_col: str = "EXPNS"
) -> pl.DataFrame:
    """
    Calculate variance for domain estimation.

    Parameters
    ----------
    data : pl.DataFrame
        Data with domain indicators
    domain_col : str
        Column indicating domain membership (0/1)
    response_col : str
        Response variable column
    weight_col : str
        Expansion factor column

    Returns
    -------
    pl.DataFrame
        Variance estimates by domain
    """
    # Filter to domain
    domain_data = data.filter(pl.col(domain_col) == 1)

    # Calculate domain statistics using select with aggregation expressions
    domain_stats = domain_data.select(
        [
            pl.count().alias("n_domain"),
            (pl.col(response_col) * pl.col(weight_col)).sum().alias("total_domain"),
            ((pl.col(response_col) * pl.col(weight_col)) ** 2).sum().alias("sum_sq"),
            pl.col(weight_col).sum().alias("total_weight"),
        ]
    )

    n = domain_stats["n_domain"][0]
    if n > 1:
        # Calculate variance using standard formula
        mean = domain_stats["total_domain"][0] / domain_stats["total_weight"][0]
        variance = (domain_stats["sum_sq"][0] - n * mean**2) / (n - 1)
    else:
        variance = 0

    return pl.DataFrame(
        {
            "DOMAIN": [domain_col],
            "ESTIMATE": [domain_stats["total_domain"][0]],
            "VARIANCE": [variance],
            "SE": [variance**0.5],
        }
    )


class VarianceCalculator:
    """
    Simple variance calculator for FIA estimates.

    This replaces the complex variance calculation system with a
    straightforward implementation.
    """

    def __init__(self, method: str = "ratio_of_means"):
        """
        Initialize calculator.

        Parameters
        ----------
        method : str
            Variance calculation method
        """
        self.method = method

    def calculate(
        self,
        data: pl.DataFrame,
        response_col: str,
        group_cols: Optional[List[str]] = None,
    ) -> pl.DataFrame:
        """
        Calculate variance for grouped estimates.

        Parameters
        ----------
        data : pl.DataFrame
            Input data with stratification
        response_col : str
            Response variable column
        group_cols : Optional[List[str]]
            Grouping columns

        Returns
        -------
        pl.DataFrame
            Results with variance estimates
        """
        if group_cols:
            # Calculate variance for each group
            results = []
            for group in data.partition_by(group_cols):
                group_result = self._calculate_single_group(group, response_col)
                # Add group identifiers
                for col in group_cols:
                    group_result = group_result.with_columns(
                        pl.lit(group[col][0]).alias(col)
                    )
                results.append(group_result)

            return pl.concat(results)
        else:
            # Calculate for entire dataset
            return self._calculate_single_group(data, response_col)

    def _calculate_single_group(
        self, data: pl.DataFrame, response_col: str
    ) -> pl.DataFrame:
        """Calculate variance for a single group."""
        if self.method == "ratio_of_means":
            return calculate_ratio_of_means_variance(
                data,
                response_col,
                area_col="AREA_USED"
                if "AREA_USED" in data.columns
                else "CONDPROP_UNADJ",
            )
        elif self.method == "post_stratified":
            variance = calculate_post_stratified_variance(data, response_col)
            estimate = data[response_col].sum()
            se = variance**0.5
            return pl.DataFrame(
                {"ESTIMATE": [estimate], "VARIANCE": [variance], "SE": [se]}
            )
        else:
            # Simple variance calculation
            mean_val = data[response_col].mean()
            var_val = data[response_col].var(ddof=1)
            est = float(mean_val) if mean_val is not None else 0.0  # type: ignore[arg-type]
            var = float(var_val) if var_val is not None else 0.0  # type: ignore[arg-type]
            se = var**0.5 if var else 0
            return pl.DataFrame({"ESTIMATE": [est], "VARIANCE": [var], "SE": [se]})
