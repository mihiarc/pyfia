"""
Aggregation logic for FIA estimation.

Simple aggregation functions without complex abstractions like
UnifiedAggregationWorkflow, EstimationType enums, or multiple strategy patterns.
"""

from typing import Dict, List, Optional, Tuple

import polars as pl


def aggregate_to_population(
    data: pl.LazyFrame,
    response_cols: List[str],
    group_cols: Optional[List[str]] = None,
    weight_col: str = "EXPNS",
    adjustment_col: Optional[str] = None
) -> pl.DataFrame:
    """
    Aggregate tree/plot data to population estimates.
    
    Parameters
    ----------
    data : pl.LazyFrame
        Input data with response values and weights
    response_cols : List[str]
        Columns to aggregate (e.g., ["VOLUME", "BIOMASS"])
    group_cols : Optional[List[str]]
        Columns to group by
    weight_col : str
        Expansion factor column
    adjustment_col : Optional[str]
        Adjustment factor column (e.g., "ADJ_FACTOR_SUBP")
        
    Returns
    -------
    pl.DataFrame
        Aggregated population estimates
    """
    # Apply adjustment factor if provided
    if adjustment_col and adjustment_col in data.columns:
        for col in response_cols:
            data = data.with_columns([
                (pl.col(col) * pl.col(adjustment_col)).alias(col)
            ])
    
    # Calculate weighted totals
    agg_exprs = []
    for col in response_cols:
        # Population total
        agg_exprs.append(
            (pl.col(col) * pl.col(weight_col)).sum().alias(f"{col}_TOTAL")
        )
        # Per-acre value (ratio of means)
        agg_exprs.append(
            (pl.col(col) * pl.col(weight_col)).sum().alias(f"{col}_NUM")
        )
    
    # Add area total for ratio calculation
    agg_exprs.append(
        pl.col(weight_col).sum().alias("AREA_TOTAL")
    )
    
    # Add plot count
    agg_exprs.append(
        pl.count("PLT_CN").alias("N_PLOTS")
    )
    
    # Perform aggregation
    if group_cols:
        results = data.group_by(group_cols).agg(agg_exprs)
    else:
        results = data.select(agg_exprs)
    
    # Collect results
    results = results.collect()
    
    # Calculate per-acre values (ratio of means)
    for col in response_cols:
        results = results.with_columns([
            (pl.col(f"{col}_NUM") / pl.col("AREA_TOTAL")).alias(f"{col}_ACRE")
        ])
        # Clean up intermediate column
        results = results.drop(f"{col}_NUM")
    
    return results


def aggregate_by_domain(
    data: pl.DataFrame,
    domain_indicators: Dict[str, pl.Expr],
    response_col: str,
    weight_col: str = "EXPNS"
) -> pl.DataFrame:
    """
    Aggregate by domain (subpopulation).
    
    Parameters
    ----------
    data : pl.DataFrame
        Input data
    domain_indicators : Dict[str, pl.Expr]
        Domain name to indicator expression mapping
    response_col : str
        Response variable to aggregate
    weight_col : str
        Expansion factor
        
    Returns
    -------
    pl.DataFrame
        Aggregated results by domain
    """
    results = []
    
    for domain_name, indicator_expr in domain_indicators.items():
        # Apply domain indicator
        domain_data = data.with_columns([
            indicator_expr.alias("IN_DOMAIN")
        ]).filter(pl.col("IN_DOMAIN") == 1)
        
        # Aggregate domain
        domain_result = domain_data.agg([
            (pl.col(response_col) * pl.col(weight_col)).sum().alias("TOTAL"),
            pl.col(weight_col).sum().alias("AREA"),
            pl.count().alias("N_PLOTS")
        ])
        
        # Calculate per-acre
        domain_result = domain_result.with_columns([
            pl.lit(domain_name).alias("DOMAIN"),
            (pl.col("TOTAL") / pl.col("AREA")).alias("PER_ACRE")
        ])
        
        results.append(domain_result)
    
    return pl.concat(results)


def aggregate_plot_level(
    data: pl.LazyFrame,
    response_cols: List[str],
    plot_col: str = "PLT_CN",
    weight_col: str = "TPA_UNADJ"
) -> pl.DataFrame:
    """
    Aggregate tree data to plot level.
    
    Parameters
    ----------
    data : pl.LazyFrame
        Tree-level data
    response_cols : List[str]
        Columns to aggregate
    plot_col : str
        Plot identifier column
    weight_col : str
        Trees per acre factor
        
    Returns
    -------
    pl.DataFrame
        Plot-level aggregates
    """
    agg_exprs = []
    
    for col in response_cols:
        # Sum weighted values per plot
        agg_exprs.append(
            (pl.col(col) * pl.col(weight_col)).sum().alias(f"{col}_PLOT")
        )
    
    # Add tree count
    agg_exprs.append(
        pl.count().alias("N_TREES")
    )
    
    # Aggregate by plot
    plot_data = data.group_by(plot_col).agg(agg_exprs)
    
    return plot_data.collect()


def calculate_proportions(
    data: pl.DataFrame,
    value_col: str,
    total_col: str,
    group_cols: Optional[List[str]] = None
) -> pl.DataFrame:
    """
    Calculate proportions within groups.
    
    Parameters
    ----------
    data : pl.DataFrame
        Input data
    value_col : str
        Numerator column
    total_col : str
        Denominator column
    group_cols : Optional[List[str]]
        Columns defining groups
        
    Returns
    -------
    pl.DataFrame
        Data with proportions added
    """
    if group_cols:
        # Calculate proportions within each group
        totals = data.group_by(group_cols).agg([
            pl.col(total_col).sum().alias("GROUP_TOTAL")
        ])
        
        data = data.join(totals, on=group_cols, how="left")
        data = data.with_columns([
            (pl.col(value_col) / pl.col("GROUP_TOTAL")).alias("PROPORTION")
        ])
        data = data.drop("GROUP_TOTAL")
    else:
        # Calculate overall proportion
        total = data[total_col].sum()
        data = data.with_columns([
            (pl.col(value_col) / total).alias("PROPORTION")
        ])
    
    return data


def merge_stratification(
    data: pl.LazyFrame,
    ppsa: pl.LazyFrame,
    pop_stratum: pl.LazyFrame,
    plot_col: str = "PLT_CN"
) -> pl.LazyFrame:
    """
    Merge stratification tables with data.
    
    Simple join operation without complex optimization strategies.
    
    Parameters
    ----------
    data : pl.LazyFrame
        Main data (tree, plot, or condition)
    ppsa : pl.LazyFrame
        POP_PLOT_STRATUM_ASSGN table
    pop_stratum : pl.LazyFrame
        POP_STRATUM table
    plot_col : str
        Plot identifier column
        
    Returns
    -------
    pl.LazyFrame
        Data with stratification merged
    """
    # Select needed columns from pop_stratum
    pop_stratum_selected = pop_stratum.select([
        pl.col("CN").alias("STRATUM_CN"),
        "EXPNS",
        "ADJ_FACTOR_MICR",
        "ADJ_FACTOR_SUBP",
        "ADJ_FACTOR_MACR",
        "ESTN_UNIT"
    ])
    
    # Join PPSA with POP_STRATUM
    strat = ppsa.join(
        pop_stratum_selected,
        on="STRATUM_CN",
        how="inner"
    )
    
    # Join with main data
    result = data.join(
        strat,
        on=plot_col,
        how="inner"
    )
    
    return result


# Import expansion functions from dedicated tree_expansion module
from .tree_expansion import (
    apply_tree_adjustment_factors, 
    apply_area_adjustment_factors
)


def expand_to_population(
    estimate: float,
    area_total: float,
    unit: str = "acres"
) -> float:
    """
    Expand per-unit estimate to population total.
    
    Parameters
    ----------
    estimate : float
        Per-unit estimate (e.g., per acre)
    area_total : float
        Total area
    unit : str
        Unit of estimate
        
    Returns
    -------
    float
        Population total
    """
    if unit == "acres":
        return estimate * area_total
    elif unit == "hectares":
        return estimate * area_total * 2.47105  # acres to hectares
    else:
        return estimate * area_total


def combine_estimation_results(
    per_acre: pl.DataFrame,
    totals: Optional[pl.DataFrame] = None,
    variance: Optional[pl.DataFrame] = None
) -> pl.DataFrame:
    """
    Combine per-acre estimates, totals, and variance.
    
    Parameters
    ----------
    per_acre : pl.DataFrame
        Per-acre estimates
    totals : Optional[pl.DataFrame]
        Population totals
    variance : Optional[pl.DataFrame]
        Variance estimates
        
    Returns
    -------
    pl.DataFrame
        Combined results
    """
    result = per_acre
    
    # Add totals if provided
    if totals is not None:
        # Join on common columns (group columns)
        join_cols = [col for col in per_acre.columns if col in totals.columns 
                     and not col.endswith("_ACRE") and not col.endswith("_TOTAL")]
        if join_cols:
            result = result.join(totals, on=join_cols, how="left")
        else:
            # No common columns, just concatenate horizontally
            for col in totals.columns:
                if col not in result.columns:
                    result = result.with_columns(totals[col])
    
    # Add variance if provided
    if variance is not None:
        se_cols = [col for col in variance.columns if col.startswith("SE")]
        var_cols = [col for col in variance.columns if col.startswith("VAR")]
        
        for col in se_cols + var_cols:
            if col not in result.columns:
                result = result.with_columns(variance[col])
    
    return result