"""
Aggregation functions for FIA two-stage estimation methodology.

This module provides pure functions for FIA's design-based estimation aggregation,
following Bechtold & Patterson (2005) methodology. The two-stage aggregation is
the core of FIA estimation:

Stage 1: Aggregate metrics to plot-condition level
    - Each condition's area proportion (CONDPROP_UNADJ) is counted exactly once
    - Trees within a condition are summed together

Stage 2: Apply expansion factors for population estimates
    - Condition-level values are expanded using stratification factors (EXPNS)
    - Per-acre estimates = sum(metric x EXPNS) / sum(CONDPROP_UNADJ x EXPNS)

These functions are stateless and operate on Polars DataFrames/LazyFrames,
making them easy to test and reuse across different estimators.
"""

from typing import Dict, List, Tuple

import polars as pl


def aggregate_to_condition_level(
    data_with_strat: pl.LazyFrame,
    metric_mappings: Dict[str, str],
    group_cols: List[str],
    available_cols: List[str],
) -> Tuple[pl.LazyFrame, List[str]]:
    """
    Stage 1: Aggregate metrics to plot-condition level.

    Each condition's area proportion (CONDPROP_UNADJ) is counted exactly once.
    Trees within a condition are summed together.

    Parameters
    ----------
    data_with_strat : pl.LazyFrame
        Data with stratification columns joined (must include PLT_CN, CONDID,
        STRATUM_CN, EXPNS, CONDPROP_UNADJ)
    metric_mappings : Dict[str, str]
        Mapping of adjusted metrics to condition-level aggregates, e.g.:
        {"VOLUME_ADJ": "CONDITION_VOLUME"} for volume estimation
        {"TPA_ADJ": "CONDITION_TPA", "BAA_ADJ": "CONDITION_BAA"} for TPA estimation
    group_cols : List[str]
        User-specified grouping columns (e.g., SPCD, FORTYPCD)
    available_cols : List[str]
        Available columns in the data (from collect_schema().names())

    Returns
    -------
    tuple[pl.LazyFrame, List[str]]
        Condition-level aggregated data and the grouping columns used

    Examples
    --------
    >>> metric_mappings = {"VOLUME_ADJ": "CONDITION_VOLUME"}
    >>> cond_agg, cond_cols = aggregate_to_condition_level(
    ...     data_with_strat, metric_mappings, ["SPCD"], available_cols
    ... )
    """
    # Define condition-level grouping columns (always needed)
    condition_group_cols = [
        "PLT_CN",
        "CONDID",
        "STRATUM_CN",
        "EXPNS",
        "CONDPROP_UNADJ",
    ]

    # Add user-specified grouping columns if they exist at condition level
    if group_cols:
        for col in group_cols:
            if col in available_cols and col not in condition_group_cols:
                condition_group_cols.append(col)

    # Build aggregation expressions
    agg_exprs = []
    for adj_col, cond_col in metric_mappings.items():
        agg_exprs.append(pl.col(adj_col).sum().alias(cond_col))
    # Add tree count for diagnostics
    agg_exprs.append(pl.len().alias("TREES_PER_CONDITION"))

    # Aggregate at condition level
    condition_agg = data_with_strat.group_by(condition_group_cols).agg(agg_exprs)

    return condition_agg, condition_group_cols


def aggregate_to_population_level(
    condition_agg: pl.LazyFrame,
    metric_mappings: Dict[str, str],
    group_cols: List[str],
    condition_group_cols: List[str],
) -> pl.LazyFrame:
    """
    Stage 2: Apply expansion factors and calculate population estimates.

    Condition-level values are expanded using stratification factors (EXPNS).

    Parameters
    ----------
    condition_agg : pl.LazyFrame
        Condition-level aggregated data from aggregate_to_condition_level
    metric_mappings : Dict[str, str]
        Mapping of adjusted metrics to condition-level aggregates
    group_cols : List[str]
        User-specified grouping columns
    condition_group_cols : List[str]
        Columns used in condition-level grouping

    Returns
    -------
    pl.LazyFrame
        Population-level aggregated results with columns:
        - {METRIC}_NUM: Numerator for ratio calculation
        - {METRIC}_TOTAL: Total expanded estimate
        - AREA_TOTAL: Total expanded area (denominator)
        - N_PLOTS: Number of unique plots
        - N_TREES: Total tree count
        - N_CONDITIONS: Number of conditions

    Examples
    --------
    >>> pop_results = aggregate_to_population_level(
    ...     condition_agg,
    ...     {"VOLUME_ADJ": "CONDITION_VOLUME"},
    ...     ["SPCD"],
    ...     condition_group_cols
    ... )
    """
    # Build final aggregation expressions
    final_agg_exprs = []

    # For each metric, create numerator and total calculations
    for adj_col, cond_col in metric_mappings.items():
        metric_name = cond_col.replace("CONDITION_", "")

        # Numerator: sum(metric x EXPNS)
        final_agg_exprs.append(
            (pl.col(cond_col) * pl.col("EXPNS")).sum().alias(f"{metric_name}_NUM")
        )

        # Total: sum(metric x EXPNS) - same as numerator but kept for clarity
        final_agg_exprs.append(
            (pl.col(cond_col) * pl.col("EXPNS")).sum().alias(f"{metric_name}_TOTAL")
        )

    # Denominator: sum(CONDPROP_UNADJ x EXPNS) - shared across all metrics
    final_agg_exprs.append(
        (pl.col("CONDPROP_UNADJ") * pl.col("EXPNS")).sum().alias("AREA_TOTAL")
    )

    # Diagnostic counts
    final_agg_exprs.extend(
        [
            pl.n_unique("PLT_CN").alias("N_PLOTS"),
            pl.col("TREES_PER_CONDITION").sum().alias("N_TREES"),
            pl.len().alias("N_CONDITIONS"),
        ]
    )

    # Apply final aggregation based on grouping
    if group_cols:
        # Filter to valid grouping columns at condition level
        final_group_cols = [col for col in group_cols if col in condition_group_cols]

        if final_group_cols:
            return condition_agg.group_by(final_group_cols).agg(final_agg_exprs)
        else:
            # No valid grouping columns, aggregate all
            return condition_agg.select(final_agg_exprs)
    else:
        # No grouping specified, aggregate all
        return condition_agg.select(final_agg_exprs)


def compute_per_acre_values(
    results_df: pl.DataFrame,
    metric_mappings: Dict[str, str],
) -> pl.DataFrame:
    """
    Calculate per-acre values using ratio-of-means and clean up intermediate columns.

    Per-acre estimates = sum(metric x EXPNS) / sum(CONDPROP_UNADJ x EXPNS)

    Parameters
    ----------
    results_df : pl.DataFrame
        Population-level results from aggregate_to_population_level with
        numerator, total, and area columns
    metric_mappings : Dict[str, str]
        Mapping of adjusted metrics to condition-level aggregates

    Returns
    -------
    pl.DataFrame
        Results with per-acre values calculated and intermediate columns removed.
        For each metric in metric_mappings, adds:
        - {METRIC}_ACRE: Per-acre estimate
        Retains:
        - {METRIC}_TOTAL: Total expanded estimate
        - N_PLOTS, N_TREES

    Examples
    --------
    >>> results = compute_per_acre_values(
    ...     pop_results,
    ...     {"VOLUME_ADJ": "CONDITION_VOLUME"}
    ... )
    >>> # Results will have VOLUME_ACRE and VOLUME_TOTAL columns
    """
    # Calculate per-acre values
    per_acre_exprs = []
    for adj_col, cond_col in metric_mappings.items():
        metric_name = cond_col.replace("CONDITION_", "")

        # Per-acre = numerator / denominator with division-by-zero protection
        per_acre_exprs.append(
            pl.when(pl.col("AREA_TOTAL") > 0)
            .then(pl.col(f"{metric_name}_NUM") / pl.col("AREA_TOTAL"))
            .otherwise(0.0)
            .alias(f"{metric_name}_ACRE")
        )

    results_df = results_df.with_columns(per_acre_exprs)

    # Clean up intermediate columns (keep totals and per-acre values)
    cols_to_drop = ["N_CONDITIONS", "AREA_TOTAL"]
    for adj_col, cond_col in metric_mappings.items():
        metric_name = cond_col.replace("CONDITION_", "")
        cols_to_drop.append(f"{metric_name}_NUM")

    # Only drop columns that exist
    cols_to_drop = [col for col in cols_to_drop if col in results_df.columns]
    if cols_to_drop:
        results_df = results_df.drop(cols_to_drop)

    return results_df


def apply_two_stage_aggregation(
    data_with_strat: pl.LazyFrame,
    metric_mappings: Dict[str, str],
    group_cols: List[str],
    use_grm_adjustment: bool = False,
) -> pl.DataFrame:
    """
    Apply FIA's two-stage aggregation methodology for statistically valid estimates.

    This function implements the critical two-stage aggregation pattern that is
    required for all FIA per-acre estimates. It combines:
    1. Condition-level aggregation (aggregate_to_condition_level)
    2. Population-level expansion (aggregate_to_population_level)
    3. Per-acre calculation (compute_per_acre_values)

    Parameters
    ----------
    data_with_strat : pl.LazyFrame
        Data with stratification columns joined. Required columns:
        - PLT_CN: Plot control number
        - CONDID: Condition identifier
        - STRATUM_CN: Stratum control number
        - EXPNS: Expansion factor (acres per plot)
        - CONDPROP_UNADJ: Unadjusted condition proportion
        - Plus all columns in metric_mappings keys
    metric_mappings : Dict[str, str]
        Mapping of adjusted metrics to condition-level aggregates, e.g.:
        - {"VOLUME_ADJ": "CONDITION_VOLUME"} for volume estimation
        - {"TPA_ADJ": "CONDITION_TPA", "BAA_ADJ": "CONDITION_BAA"} for TPA
        - {"BIOMASS_ADJ": "CONDITION_BIOMASS"} for biomass
    group_cols : List[str]
        User-specified grouping columns (e.g., ["SPCD"], ["FORTYPCD", "OWNGRPCD"])
    use_grm_adjustment : bool, default False
        If True, use SUBPTYP_GRM for adjustment factors (mortality/growth/removals).
        If False, use standard DIA-based adjustments (volume/biomass/tpa).
        Note: This parameter is currently unused but reserved for future GRM
        adjustments in the aggregation logic.

    Returns
    -------
    pl.DataFrame
        Aggregated results with:
        - Per-acre estimates: {METRIC}_ACRE for each metric in mappings
        - Total estimates: {METRIC}_TOTAL for each metric in mappings
        - Diagnostic columns: N_PLOTS, N_TREES

    Notes
    -----
    Stage 1: Aggregate metrics to plot-condition level
        - Each condition's area proportion (CONDPROP_UNADJ) is counted exactly once
        - Trees within a condition are summed together

    Stage 2: Apply expansion factors and calculate ratio-of-means
        - Condition-level values are expanded using stratification factors (EXPNS)
        - Per-acre estimates = sum(metric x EXPNS) / sum(CONDPROP_UNADJ x EXPNS)

    This method eliminates ~400-600 lines of duplicated code across 6 estimators
    while ensuring consistent, correct results that match EVALIDator.

    Examples
    --------
    Volume estimation:

    >>> metric_mappings = {"VOLUME_ADJ": "CONDITION_VOLUME"}
    >>> results = apply_two_stage_aggregation(
    ...     data_with_strat=data_with_strat,
    ...     metric_mappings=metric_mappings,
    ...     group_cols=["SPCD"],
    ... )
    >>> # Results will have VOLUME_ACRE, VOLUME_TOTAL, N_PLOTS, N_TREES

    Multiple metrics (TPA with basal area):

    >>> metric_mappings = {
    ...     "TPA_ADJ": "CONDITION_TPA",
    ...     "BAA_ADJ": "CONDITION_BAA"
    ... }
    >>> results = apply_two_stage_aggregation(
    ...     data_with_strat=data_with_strat,
    ...     metric_mappings=metric_mappings,
    ...     group_cols=["FORTYPCD", "OWNGRPCD"],
    ... )
    >>> # Results will have TPA_ACRE, TPA_TOTAL, BAA_ACRE, BAA_TOTAL, etc.
    """
    # Cache schema once at the beginning to avoid repeated collection
    available_cols = data_with_strat.collect_schema().names()

    # Stage 1: Aggregate to condition level
    condition_agg, condition_group_cols = aggregate_to_condition_level(
        data_with_strat, metric_mappings, group_cols, available_cols
    )

    # Stage 2: Aggregate to population level
    results = aggregate_to_population_level(
        condition_agg, metric_mappings, group_cols, condition_group_cols
    )

    # Collect and compute per-acre values
    results_df: pl.DataFrame = results.collect()
    results_df = compute_per_acre_values(results_df, metric_mappings)

    return results_df


__all__ = [
    "aggregate_to_condition_level",
    "aggregate_to_population_level",
    "compute_per_acre_values",
    "apply_two_stage_aggregation",
]
