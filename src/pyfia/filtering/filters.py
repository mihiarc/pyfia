"""
Filtering functions for FIA estimation.

This module provides all filtering logic used across estimation modules,
including tree-level, area/condition-level, and plot-level filtering.

All functions support both eager DataFrames and lazy LazyFrames for memory efficiency.
"""

from __future__ import annotations

from typing import TypeVar

import polars as pl

from ..constants.plot_design import DiameterBreakpoints
from ..constants.status_codes import (
    LandStatus,
    ReserveStatus,
    SiteClass,
    TreeClass,
    TreeStatus,
)
from .parser import DomainExpressionParser

# Type variable for DataFrame/LazyFrame operations
FrameType = TypeVar("FrameType", pl.DataFrame, pl.LazyFrame)

# =============================================================================
# Tree Filtering
# =============================================================================


def apply_tree_filters(
    tree_df: FrameType,
    tree_type: str = "all",
    tree_domain: str | None = None,
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
    tree_domain : str | None, default None
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


# =============================================================================
# Area/Condition Filtering
# =============================================================================


def apply_area_filters(
    cond_df: FrameType,
    land_type: str = "all",
    area_domain: str | None = None,
    area_estimation_mode: bool = False,
) -> FrameType:
    """
    Apply land type and area domain filters for condition data.

    This function provides consistent area/condition filtering across all
    estimation modules. It handles land type filtering (forest/timber/all)
    and applies optional user-defined area domains.

    Supports both eager DataFrames and lazy LazyFrames for memory-efficient
    processing of large datasets.

    Parameters
    ----------
    cond_df : pl.DataFrame or pl.LazyFrame
        Condition dataframe or lazyframe to filter
    land_type : str, default "all"
        Type of land to include:
        - "forest": Forest land only (COND_STATUS_CD == 1)
        - "timber": Productive, unreserved forest land
        - "all": All conditions
    area_domain : str | None, default None
        SQL-like expression for additional filtering
    area_estimation_mode : bool, default False
        If True, skip land type filtering (used by area estimation module
        where land type is handled through indicators instead)

    Returns
    -------
    pl.DataFrame or pl.LazyFrame
        Filtered condition dataframe/lazyframe (same type as input)

    Examples
    --------
    >>> # Filter for forest land
    >>> filtered = apply_area_filters(cond_df, land_type="forest")

    >>> # Filter for timber land with custom domain
    >>> filtered = apply_area_filters(
    ...     cond_df,
    ...     land_type="timber",
    ...     area_domain="OWNGRPCD == 40"  # Private land
    ... )

    >>> # Works with LazyFrames too (memory efficient)
    >>> filtered_lazy = apply_area_filters(cond_lazy, land_type="forest")
    """
    # In area estimation mode, we don't filter by land type here
    # (it's handled through domain indicators instead)
    if not area_estimation_mode:
        # Land type domain filtering
        if land_type == "forest":
            cond_df = cond_df.filter(pl.col("COND_STATUS_CD") == LandStatus.FOREST)
        elif land_type == "timber":
            cond_df = cond_df.filter(
                (pl.col("COND_STATUS_CD") == LandStatus.FOREST)
                & (pl.col("SITECLCD").is_in(SiteClass.PRODUCTIVE_CLASSES))
                & (pl.col("RESERVCD") == ReserveStatus.NOT_RESERVED)
            )
        # "all" includes everything

    # Apply user-defined area domain
    # In area estimation mode, area domain is handled through domain indicators
    if area_domain and not area_estimation_mode:
        cond_df = DomainExpressionParser.apply_to_dataframe(
            cond_df, area_domain, "area"
        )

    return cond_df


# =============================================================================
# Plot Filtering
# =============================================================================


def apply_plot_filters(
    plot_df: FrameType,
    plot_domain: str | None = None,
) -> FrameType:
    """
    Apply plot domain filters for plot data.

    This function provides consistent plot-level filtering across all
    estimation modules. It handles user-defined plot domains for filtering
    by PLOT table attributes like COUNTYCD, UNITCD, geographic coordinates, etc.

    Supports both eager DataFrames and lazy LazyFrames for memory-efficient
    processing of large datasets.

    Parameters
    ----------
    plot_df : pl.DataFrame or pl.LazyFrame
        Plot dataframe or lazyframe to filter
    plot_domain : str | None, default None
        SQL-like expression for plot-level filtering.
        Common PLOT columns include:

        **Location:**
        - COUNTYCD: County FIPS code
        - UNITCD: Survey unit code
        - STATECD: State FIPS code

        **Geographic:**
        - LAT: Latitude (decimal degrees)
        - LON: Longitude (decimal degrees)
        - ELEV: Elevation (feet)

        **Plot attributes:**
        - PLOT: Plot number
        - SUBP: Subplot number
        - INVYR: Inventory year
        - MEASYEAR: Measurement year
        - MEASMON: Measurement month
        - MEASDAY: Measurement day

        **Design:**
        - DESIGNCD: Plot design code
        - KINDCD: Kind of plot code
        - INTENSITY: Sample intensity code

    Returns
    -------
    pl.DataFrame or pl.LazyFrame
        Filtered plot dataframe/lazyframe (same type as input)

    Examples
    --------
    >>> # Filter for specific county
    >>> filtered = apply_plot_filters(plot_df, plot_domain="COUNTYCD == 183")

    >>> # Filter for multiple counties
    >>> filtered = apply_plot_filters(
    ...     plot_df,
    ...     plot_domain="COUNTYCD IN (183, 185, 187)"
    ... )

    >>> # Filter by geographic location
    >>> filtered = apply_plot_filters(
    ...     plot_df,
    ...     plot_domain="LAT >= 35.0 AND LAT <= 36.0 AND LON >= -80.0 AND LON <= -79.0"
    ... )

    >>> # Filter by elevation
    >>> filtered = apply_plot_filters(
    ...     plot_df,
    ...     plot_domain="ELEV > 2000"
    ... )

    >>> # Works with LazyFrames too (memory efficient)
    >>> filtered_lazy = apply_plot_filters(plot_lazy, plot_domain="COUNTYCD == 183")
    """
    # Apply user-defined plot domain
    if plot_domain:
        plot_df = DomainExpressionParser.apply_to_dataframe(
            plot_df, plot_domain, "plot"
        )

    return plot_df
