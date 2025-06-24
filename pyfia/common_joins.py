"""
Common table join functions for FIA data analysis.

This module consolidates frequently used join patterns across different
FIA estimators to reduce code duplication and ensure consistency.
"""

from typing import Optional, List, Union, Tuple
import polars as pl

from .constants import (
    PlotBasis,
    DiameterBreakpoints,
)


def join_tree_condition(
    tree_df: pl.DataFrame,
    cond_df: pl.DataFrame,
    cond_columns: Optional[List[str]] = None,
) -> pl.DataFrame:
    """
    Join tree and condition tables on PLT_CN and CONDID.
    
    This is the standard join for associating trees with their plot conditions.
    
    Parameters
    ----------
    tree_df : pl.DataFrame
        Tree dataframe containing PLT_CN and CONDID
    cond_df : pl.DataFrame
        Condition dataframe
    cond_columns : List[str], optional
        Specific columns to select from condition table.
        If None, defaults to ["PLT_CN", "CONDID", "CONDPROP_UNADJ"]
    
    Returns
    -------
    pl.DataFrame
        Joined tree-condition dataframe
    """
    if cond_columns is None:
        cond_columns = ["PLT_CN", "CONDID", "CONDPROP_UNADJ"]
    
    # Ensure join keys are always included
    join_keys = ["PLT_CN", "CONDID"]
    cond_columns = list(set(cond_columns) | set(join_keys))
    
    return tree_df.join(
        cond_df.select(cond_columns),
        on=join_keys,
        how="inner",
    )


def join_plot_stratum(
    plot_df: pl.DataFrame,
    ppsa_df: pl.DataFrame,
    stratum_df: pl.DataFrame,
    adj_factors: Optional[List[str]] = None,
    stratum_columns: Optional[List[str]] = None,
) -> pl.DataFrame:
    """
    Perform the standard plot → assignment → stratum join sequence.
    
    This three-way join associates plots with their stratification data
    through the POP_PLOT_STRATUM_ASSGN table.
    
    Parameters
    ----------
    plot_df : pl.DataFrame
        Plot dataframe containing PLT_CN
    ppsa_df : pl.DataFrame
        POP_PLOT_STRATUM_ASSGN dataframe
    stratum_df : pl.DataFrame
        POP_STRATUM dataframe
    adj_factors : List[str], optional
        Which adjustment factors to include. Options: ["MICR", "SUBP", "MACR"]
        If None, includes only SUBP
    stratum_columns : List[str], optional
        Additional columns to select from stratum table
    
    Returns
    -------
    pl.DataFrame
        Joined dataframe with stratification data
    """
    # Default adjustment factors
    if adj_factors is None:
        adj_factors = ["SUBP"]
    
    # Build adjustment factor column names
    adj_cols = [f"ADJ_FACTOR_{factor}" for factor in adj_factors]
    
    # Default stratum columns
    base_stratum_cols = ["CN", "EXPNS", "P2POINTCNT"] + adj_cols
    
    if stratum_columns:
        stratum_cols = list(set(base_stratum_cols + stratum_columns))
    else:
        stratum_cols = base_stratum_cols
    
    # Perform the three-way join
    return (
        plot_df.join(
            ppsa_df.select(["PLT_CN", "STRATUM_CN"]),
            on="PLT_CN",
            how="inner",
        )
        .join(
            stratum_df.select(stratum_cols),
            left_on="STRATUM_CN",
            right_on="CN",
            how="inner",
        )
    )


def assign_tree_basis(
    tree_df: pl.DataFrame,
    plot_df: Optional[pl.DataFrame] = None,
    include_macro: bool = True,
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
    
    Returns
    -------
    pl.DataFrame
        Tree dataframe with TREE_BASIS column added
    """
    if include_macro and plot_df is not None:
        # Join with plot to get MACRO_BREAKPOINT_DIA
        if "MACRO_BREAKPOINT_DIA" not in tree_df.columns:
            tree_df = tree_df.join(
                plot_df.select(["PLT_CN", "MACRO_BREAKPOINT_DIA"]),
                on="PLT_CN",
                how="left",
            )
        
        # Full tree basis assignment with macroplot logic
        tree_basis_expr = (
            pl.when(pl.col("DIA").is_null())
            .then(None)
            .when(pl.col("DIA") < DiameterBreakpoints.MICROPLOT_MAX_DIA)
            .then(pl.lit(PlotBasis.MICROPLOT))
            .when(pl.col("MACRO_BREAKPOINT_DIA") <= 0)
            .then(pl.lit(PlotBasis.SUBPLOT))
            .when(pl.col("MACRO_BREAKPOINT_DIA").is_null())
            .then(pl.lit(PlotBasis.SUBPLOT))
            .when(pl.col("DIA") < pl.col("MACRO_BREAKPOINT_DIA"))
            .then(pl.lit(PlotBasis.SUBPLOT))
            .otherwise(pl.lit(PlotBasis.MACROPLOT))
            .alias("TREE_BASIS")
        )
    else:
        # Simplified assignment (just MICR/SUBP)
        tree_basis_expr = (
            pl.when(pl.col("DIA") < DiameterBreakpoints.MICROPLOT_MAX_DIA)
            .then(pl.lit(PlotBasis.MICROPLOT))
            .otherwise(pl.lit(PlotBasis.SUBPLOT))
            .alias("TREE_BASIS")
        )
    
    return tree_df.with_columns(tree_basis_expr)


def apply_adjustment_factors(
    df: pl.DataFrame,
    value_columns: Union[str, List[str]],
    basis_column: str = "TREE_BASIS",
    adj_factor_columns: Optional[dict] = None,
) -> pl.DataFrame:
    """
    Apply adjustment factors based on tree/prop basis.
    
    This handles the common pattern of multiplying values by the appropriate
    adjustment factor based on plot design (MICR, SUBP, or MACR).
    
    Parameters
    ----------
    df : pl.DataFrame
        Dataframe with values to adjust
    value_columns : str or List[str]
        Column(s) containing values to adjust
    basis_column : str, default "TREE_BASIS"
        Column containing basis assignment (MICR, SUBP, MACR)
    adj_factor_columns : dict, optional
        Mapping of basis values to adjustment factor columns.
        Defaults to standard FIA naming convention.
    
    Returns
    -------
    pl.DataFrame
        Dataframe with adjusted value columns added (with "_ADJ" suffix)
    """
    if isinstance(value_columns, str):
        value_columns = [value_columns]
    
    # Default adjustment factor mapping
    if adj_factor_columns is None:
        adj_factor_columns = {
            PlotBasis.MICROPLOT: "ADJ_FACTOR_MICR",
            PlotBasis.SUBPLOT: "ADJ_FACTOR_SUBP", 
            PlotBasis.MACROPLOT: "ADJ_FACTOR_MACR",
        }
    
    # Create adjusted columns
    adjusted_exprs = []
    for col in value_columns:
        # Build conditional expression for this column
        # Check which adjustment columns actually exist
        existing_adj_cols = [adj_col for adj_col in adj_factor_columns.values() 
                            if adj_col in df.columns]
        
        if not existing_adj_cols:
            # No adjustment factors found, just copy the column
            adjusted_exprs.append(pl.col(col).alias(f"{col}_ADJ"))
            continue
        
        expr = None
        
        # Apply each adjustment factor conditionally
        for basis, adj_col in adj_factor_columns.items():
            if adj_col in df.columns:  # Only use if column exists
                if expr is None:
                    expr = pl.when(pl.col(basis_column) == basis).then(
                        pl.col(col) * pl.col(adj_col)
                    )
                else:
                    expr = expr.when(pl.col(basis_column) == basis).then(
                        pl.col(col) * pl.col(adj_col)
                    )
        
        # Default to original value if no match
        if expr is not None:
            expr = expr.otherwise(pl.col(col))
            adjusted_exprs.append(expr.alias(f"{col}_ADJ"))
        else:
            adjusted_exprs.append(pl.col(col).alias(f"{col}_ADJ"))
    
    return df.with_columns(adjusted_exprs)


def get_evalid_assignments(
    ppsa_table: pl.LazyFrame,
    evalid: Optional[Union[int, List[int]]] = None,
    plot_cns: Optional[List[str]] = None,
) -> pl.DataFrame:
    """
    Filter POP_PLOT_STRATUM_ASSGN by EVALID or plot CNs.
    
    This standardizes the common pattern of filtering assignments
    based on evaluation or specific plots.
    
    Parameters
    ----------
    ppsa_table : pl.LazyFrame
        POP_PLOT_STRATUM_ASSGN lazy frame
    evalid : int or List[int], optional
        Evaluation ID(s) to filter by
    plot_cns : List[str], optional
        Plot CNs to filter by (used if evalid is None)
    
    Returns
    -------
    pl.DataFrame
        Filtered assignment dataframe
    """
    if evalid is not None:
        # Convert single evalid to list
        if isinstance(evalid, int):
            evalid = [evalid]
        return ppsa_table.filter(pl.col("EVALID").is_in(evalid)).collect()
    elif plot_cns is not None:
        return ppsa_table.filter(pl.col("PLT_CN").is_in(plot_cns)).collect()
    else:
        # No filtering - return all
        return ppsa_table.collect()


def join_species_info(
    df: pl.DataFrame,
    species_df: pl.DataFrame,
    species_columns: Optional[List[str]] = None,
) -> pl.DataFrame:
    """
    Join species reference information.
    
    Parameters
    ----------
    df : pl.DataFrame
        Dataframe with SPCD column
    species_df : pl.DataFrame
        REF_SPECIES dataframe
    species_columns : List[str], optional
        Columns to include from species table.
        Defaults to ["SPCD", "COMMON_NAME", "GENUS", "SPECIES"]
    
    Returns
    -------
    pl.DataFrame
        Dataframe with species information joined
    """
    if species_columns is None:
        species_columns = ["SPCD", "COMMON_NAME", "GENUS", "SPECIES"]
    
    # Ensure SPCD is always included
    if "SPCD" not in species_columns:
        species_columns = ["SPCD"] + species_columns
    
    return df.join(
        species_df.select(species_columns),
        on="SPCD",
        how="left",
    )


def aggregate_tree_to_plot(
    tree_df: pl.DataFrame,
    group_by: List[str],
    agg_columns: dict,
    adjustment_needed: bool = True,
) -> pl.DataFrame:
    """
    Aggregate tree-level data to plot level with proper adjustment.
    
    This handles the common pattern of summing tree values to plot level
    while maintaining proper grouping and adjustment factor application.
    
    Parameters
    ----------
    tree_df : pl.DataFrame
        Tree-level dataframe
    group_by : List[str]
        Columns to group by (typically includes PLT_CN)
    agg_columns : dict
        Mapping of column names to aggregation expressions
    adjustment_needed : bool, default True
        Whether to group by TREE_BASIS for adjustment factor application
    
    Returns
    -------
    pl.DataFrame
        Plot-level aggregated dataframe
    """
    # Add TREE_BASIS to grouping if adjustments needed
    if adjustment_needed and "TREE_BASIS" not in group_by:
        group_by = group_by + ["TREE_BASIS"]
    
    # Perform aggregation
    plot_df = tree_df.group_by(group_by).agg(
        [expr.alias(name) for name, expr in agg_columns.items()]
    )
    
    return plot_df


def join_plot_metadata(
    df: pl.DataFrame,
    plot_df: pl.DataFrame,
    plot_columns: Optional[List[str]] = None,
) -> pl.DataFrame:
    """
    Join plot-level metadata.
    
    Parameters
    ----------
    df : pl.DataFrame
        Dataframe with PLT_CN
    plot_df : pl.DataFrame
        PLOT dataframe
    plot_columns : List[str], optional
        Columns to include from plot table
    
    Returns
    -------
    pl.DataFrame
        Dataframe with plot metadata joined
    """
    if plot_columns is None:
        plot_columns = ["PLT_CN", "STATECD", "INVYR", "PLOT", 
                       "LAT", "LON"]
    
    # Ensure PLT_CN is included
    if "PLT_CN" not in plot_columns:
        plot_columns = ["PLT_CN"] + plot_columns
    
    return df.join(
        plot_df.select(plot_columns),
        on="PLT_CN", 
        how="left",
    )