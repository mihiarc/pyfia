"""
Tree filtering functions for FIA estimation.

This module provides tree-level filtering logic used across all estimation modules,
including tree type filtering (live/dead/growing stock) and custom domain filters.

All functions support both eager DataFrames and lazy LazyFrames for memory efficiency.
"""

from typing import Optional, TypeVar

import polars as pl

from ...constants.plot_design import DiameterBreakpoints
from ...constants.status_codes import TreeClass, TreeStatus
from ..core.parser import DomainExpressionParser

# Type variable for DataFrame/LazyFrame operations
FrameType = TypeVar("FrameType", pl.DataFrame, pl.LazyFrame)


def apply_tree_filters(
    tree_df: FrameType,
    tree_type: str = "all",
    tree_domain: Optional[str] = None,
    require_volume: bool = False,
    require_diameter_thresholds: bool = False,
) -> FrameType:
    """
    Apply tree type and domain filters following FIA methodology.

    This function provides consistent tree filtering across all estimation modules.
    It handles tree status filtering (live/dead/growing stock/all), applies optional
    user-defined domains, and ensures data validity for estimation.

    Supports both eager DataFrames and lazy LazyFrames for memory-efficient
    processing of large datasets.

    Parameters
    ----------
    tree_df : pl.DataFrame or pl.LazyFrame
        Tree dataframe or lazyframe to filter
    tree_type : str, default "all"
        Type of trees to include:
        - "live": Live trees only (STATUSCD == 1)
        - "dead": Dead trees only (STATUSCD == 2)
        - "gs": Growing stock trees (TREECLCD == 2)
        - "all": All trees with valid measurements
    tree_domain : Optional[str], default None
        SQL-like expression for additional filtering (e.g., "DIA >= 10.0")
    require_volume : bool, default False
        If True, require valid volume data (VOLCFGRS not null).
        Used by volume estimation module.
    require_diameter_thresholds : bool, default False
        If True, apply FIA standard diameter thresholds based on tree type.
        Used by tpa estimation module.

    Returns
    -------
    pl.DataFrame or pl.LazyFrame
        Filtered tree dataframe/lazyframe (same type as input)

    Examples
    --------
    >>> # Filter for live trees
    >>> filtered = apply_tree_filters(tree_df, tree_type="live")

    >>> # Filter for large trees with volume data
    >>> filtered = apply_tree_filters(
    ...     tree_df,
    ...     tree_type="live",
    ...     tree_domain="DIA >= 20.0",
    ...     require_volume=True
    ... )

    >>> # Works with LazyFrames too (memory efficient)
    >>> filtered_lazy = apply_tree_filters(tree_lazy, tree_type="gs")
    """
    # Get column names (works for both DataFrame and LazyFrame)
    if isinstance(tree_df, pl.LazyFrame):
        columns = tree_df.collect_schema().names()
    else:
        columns = tree_df.columns

    # Tree type filters
    if tree_type == "live":
        if require_diameter_thresholds:
            # TPA module specific: live trees >= 1.0" DBH
            tree_df = tree_df.filter(
                (pl.col("STATUSCD") == TreeStatus.LIVE)
                & (pl.col("DIA").is_not_null())
                & (pl.col("DIA") >= DiameterBreakpoints.MIN_DBH)
            )
        else:
            # Standard live tree filter
            tree_df = tree_df.filter(pl.col("STATUSCD") == TreeStatus.LIVE)
    elif tree_type == "dead":
        if require_diameter_thresholds:
            # TPA module specific: dead trees >= 5.0" DBH
            tree_df = tree_df.filter(
                (pl.col("STATUSCD") == TreeStatus.DEAD)
                & (pl.col("DIA").is_not_null())
                & (pl.col("DIA") >= DiameterBreakpoints.SUBPLOT_MIN_DIA)
            )
        else:
            # Standard dead tree filter
            tree_df = tree_df.filter(pl.col("STATUSCD") == TreeStatus.DEAD)
    elif tree_type == "gs":  # Growing stock
        if require_diameter_thresholds:
            # TPA module specific: growing stock with diameter threshold
            tree_df = tree_df.filter(
                (pl.col("TREECLCD") == TreeClass.GROWING_STOCK)
                & (pl.col("DIA").is_not_null())
                & (pl.col("DIA") >= DiameterBreakpoints.MIN_DBH)
            )
        else:
            # Standard growing stock filter (for volume/biomass)
            tree_df = tree_df.filter(
                pl.col("STATUSCD").is_in([TreeStatus.LIVE, TreeStatus.DEAD])
            )
    # "all" includes everything with valid measurements

    # Filter for valid data required by all modules
    # If DIA not present (e.g., minimal projections for performance), skip DIA validation
    if "DIA" in columns:
        tree_df = tree_df.filter(
            (pl.col("DIA").is_not_null()) & (pl.col("TPA_UNADJ") > 0)
        )
    else:
        tree_df = tree_df.filter(pl.col("TPA_UNADJ") > 0)

    # Additional filter for volume estimation
    if require_volume:
        tree_df = tree_df.filter(
            pl.col("VOLCFGRS").is_not_null()  # At least gross volume required
        )

    # Apply user-defined tree domain
    if tree_domain:
        tree_df = DomainExpressionParser.apply_to_dataframe(
            tree_df, tree_domain, "tree"
        )

    return tree_df
