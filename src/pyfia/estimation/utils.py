"""
Utility functions for FIA estimation.

Simple utilities for common operations.
"""

from typing import Dict, List, Optional, Tuple, Union

import polars as pl


def join_tables(
    left: Union[pl.DataFrame, pl.LazyFrame],
    right: Union[pl.DataFrame, pl.LazyFrame],
    on: Union[str, List[str]],
    how: str = "inner",
    suffix: str = "_right"
) -> Union[pl.DataFrame, pl.LazyFrame]:
    """
    Simple join operation without complex optimization strategies.
    
    Parameters
    ----------
    left : Union[pl.DataFrame, pl.LazyFrame]
        Left table
    right : Union[pl.DataFrame, pl.LazyFrame]
        Right table
    on : Union[str, List[str]]
        Join columns
    how : str
        Join type: "inner", "left", "right", "outer"
    suffix : str
        Suffix for duplicate columns
        
    Returns
    -------
    Union[pl.DataFrame, pl.LazyFrame]
        Joined result
    """
    # Simple join without overthinking it
    return left.join(right, on=on, how=how, suffix=suffix)


def format_output_columns(
    df: pl.DataFrame,
    estimation_type: str,
    include_se: bool = True,
    include_cv: bool = False
) -> pl.DataFrame:
    """
    Format output columns to standard structure.
    
    Parameters
    ----------
    df : pl.DataFrame
        Results dataframe
    estimation_type : str
        Type of estimation (for column naming)
    include_se : bool
        Include standard error columns
    include_cv : bool
        Include coefficient of variation
        
    Returns
    -------
    pl.DataFrame
        Formatted dataframe
    """
    # Standard column mappings by estimation type
    column_maps = {
        "volume": {
            "VOLUME_ACRE": "VOL_ACRE",
            "VOLUME_TOTAL": "VOL_TOTAL",
        },
        "biomass": {
            "BIOMASS_ACRE": "BIO_ACRE",
            "BIOMASS_TOTAL": "BIO_TOTAL",
            "CARBON_ACRE": "CARB_ACRE",
        },
        "tpa": {
            "TPA": "TPA",
            "BAA": "BAA",
        },
        "area": {
            "AREA_TOTAL": "AREA",
            "AREA_PERCENT": "AREA_PCT",
        },
        "mortality": {
            "MORTALITY_ACRE": "MORT_ACRE",
            "MORTALITY_TOTAL": "MORT_TOTAL",
        },
        "growth": {
            "GROWTH_ACRE": "GROWTH_ACRE",  # Fixed: was incorrectly "GROW_ACRE"
            "GROWTH_TOTAL": "GROWTH_TOTAL",
        }
    }
    
    # Apply column mappings if available
    if estimation_type in column_maps:
        rename_dict = {}
        for old_name, new_name in column_maps[estimation_type].items():
            if old_name in df.columns:
                rename_dict[old_name] = new_name
        
        if rename_dict:
            df = df.rename(rename_dict)
    
    # Add CV if requested
    if include_cv:
        # Find estimate and SE columns
        est_cols = [col for col in df.columns if col.endswith("_ACRE") or col.endswith("_TOTAL")]
        se_cols = [col for col in df.columns if col.endswith("_SE")]
        
        for est_col in est_cols:
            se_col = f"{est_col}_SE"
            if se_col in se_cols:
                cv_col = f"{est_col}_CV"
                df = df.with_columns([
                    (100 * pl.col(se_col) / pl.col(est_col).abs())
                    .fill_null(0)
                    .alias(cv_col)
                ])
    
    # Order columns consistently
    priority_cols = ["YEAR", "EVALID", "STATECD", "PLOT", "SPCD"]
    estimate_cols = [col for col in df.columns if col.endswith(("_ACRE", "_TOTAL", "_PCT"))]
    se_cols = [col for col in df.columns if col.endswith("_SE")]
    cv_cols = [col for col in df.columns if col.endswith("_CV")]
    meta_cols = ["N_PLOTS", "N_TREES", "AREA"]
    
    # Build ordered column list
    ordered = []
    for col in priority_cols:
        if col in df.columns:
            ordered.append(col)
    
    for col in estimate_cols:
        if col not in ordered:
            ordered.append(col)
    
    for col in se_cols:
        if col not in ordered:
            ordered.append(col)
    
    for col in cv_cols:
        if col not in ordered:
            ordered.append(col)
    
    for col in meta_cols:
        if col in df.columns and col not in ordered:
            ordered.append(col)
    
    # Add any remaining columns
    for col in df.columns:
        if col not in ordered:
            ordered.append(col)
    
    return df.select(ordered)


def get_evalid_info(evalid: Union[int, str]) -> Dict[str, Union[int, str]]:
    """
    Parse EVALID into components.
    
    Parameters
    ----------
    evalid : Union[int, str]
        6-digit EVALID code
        
    Returns
    -------
    Dict[str, Union[int, str]]
        Dictionary with state, year, and type components
    """
    evalid_str = str(evalid).zfill(6)
    
    return {
        "state": int(evalid_str[:2]),
        "year": int(evalid_str[2:4]),
        "type": evalid_str[4:6],
        "full": evalid_str
    }


def filter_most_recent_evalid(
    evalids: List[Union[int, str]],
    eval_type: Optional[str] = None
) -> List[str]:
    """
    Filter to most recent EVALIDs by state.
    
    Parameters
    ----------
    evalids : List[Union[int, str]]
        List of EVALID codes
    eval_type : Optional[str]
        Specific evaluation type to filter for
        
    Returns
    -------
    List[str]
        Most recent EVALIDs
    """
    # Parse all evalids
    parsed = [get_evalid_info(e) for e in evalids]
    
    # Filter by type if specified
    if eval_type:
        type_map = {
            "EXPALL": "00",
            "EXPVOL": "01",
            "EXPCHNG": "03",
            "EXPDWM": "07",
            "EXPINV": "09"
        }
        target_type = type_map.get(eval_type, eval_type)
        parsed = [e for e in parsed if e["type"] == target_type]
    
    # Group by state and find most recent
    by_state = {}
    for evalid in parsed:
        state = evalid["state"]
        if state not in by_state or evalid["year"] > by_state[state]["year"]:
            by_state[state] = evalid
    
    return [e["full"] for e in by_state.values()]


def check_required_columns(
    df: Union[pl.DataFrame, pl.LazyFrame],
    required: List[str],
    context: str = ""
) -> None:
    """
    Check if required columns exist.
    
    Simple validation without complex error messages.
    
    Parameters
    ----------
    df : Union[pl.DataFrame, pl.LazyFrame]
        Dataframe to check
    required : List[str]
        Required column names
    context : str
        Context for error message
        
    Raises
    ------
    ValueError
        If columns are missing
    """
    if isinstance(df, pl.LazyFrame):
        columns = df.columns
    else:
        columns = df.columns
    
    missing = [col for col in required if col not in columns]
    
    if missing:
        msg = f"Missing required columns: {missing}"
        if context:
            msg = f"{context}: {msg}"
        raise ValueError(msg)


# Import tree expansion functions from dedicated module
from .tree_expansion import (
    calculate_expanded_trees,
    get_tree_adjustment_sql,
    get_area_adjustment_sql
)


def create_domain_filter(
    domain_str: str,
    table_prefix: Optional[str] = None
) -> pl.Expr:
    """
    Convert SQL-like domain string to Polars expression.
    
    Parameters
    ----------
    domain_str : str
        SQL-like condition (e.g., "DIA > 10 AND STATUSCD == 1")
    table_prefix : Optional[str]
        Prefix for column names
        
    Returns
    -------
    pl.Expr
        Polars filter expression
    """
    # Simple conversion of common patterns
    # This would need more sophisticated parsing for complex expressions
    
    # Replace SQL operators
    domain_str = domain_str.replace(" AND ", " & ")
    domain_str = domain_str.replace(" OR ", " | ")
    domain_str = domain_str.replace(" NOT ", " ~ ")
    domain_str = domain_str.replace("=", "==")
    domain_str = domain_str.replace("<>", "!=")
    
    # Add pl.col() around column names
    # This is a simplified approach - real implementation would need proper parsing
    import re
    
    # Find all potential column names (uppercase words)
    columns = re.findall(r'\b[A-Z_]+\b', domain_str)
    
    for col in set(columns):
        if col not in ["AND", "OR", "NOT", "NULL", "TRUE", "FALSE"]:
            if table_prefix:
                domain_str = domain_str.replace(col, f'pl.col("{table_prefix}.{col}")')
            else:
                domain_str = domain_str.replace(col, f'pl.col("{col}")')
    
    # Handle IS NULL / IS NOT NULL
    domain_str = domain_str.replace(" IS NULL", ".is_null()")
    domain_str = domain_str.replace(" IS NOT NULL", ".is_not_null()")
    
    # Evaluate the expression
    try:
        return eval(domain_str)
    except:
        # If parsing fails, return a simple true condition
        return pl.lit(True)


def add_year_column(
    df: pl.DataFrame,
    year_col: str = "INVYR",
    default_year: int = 2023
) -> pl.DataFrame:
    """
    Add year column for output.
    
    Parameters
    ----------
    df : pl.DataFrame
        Input dataframe
    year_col : str
        Column containing inventory year
    default_year : int
        Default year if not found
        
    Returns
    -------
    pl.DataFrame
        Dataframe with YEAR column added
    """
    if year_col in df.columns:
        # Use max year from data
        year = df[year_col].max()
        if year is None:
            year = default_year
    else:
        year = default_year
    
    return df.with_columns([
        pl.lit(year).alias("YEAR")
    ])


def combine_group_results(
    results: List[pl.DataFrame],
    group_cols: List[str]
) -> pl.DataFrame:
    """
    Combine results from grouped calculations.
    
    Parameters
    ----------
    results : List[pl.DataFrame]
        List of result dataframes
    group_cols : List[str]
        Grouping columns
        
    Returns
    -------
    pl.DataFrame
        Combined results
    """
    if not results:
        return pl.DataFrame()
    
    if len(results) == 1:
        return results[0]
    
    # Concatenate all results
    combined = pl.concat(results, how="vertical")
    
    # Sort by group columns
    if group_cols:
        combined = combined.sort(group_cols)
    
    return combined