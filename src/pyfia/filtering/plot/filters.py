"""
Plot-level filtering functions for FIA estimation.

This module provides plot-level filtering logic used across all estimation
modules, enabling filtering by PLOT table attributes like COUNTYCD, UNITCD,
LAT, LON, ELEV, etc.

All functions support both eager DataFrames and lazy LazyFrames for memory efficiency.
"""

from typing import Optional, TypeVar

import polars as pl

from ..core.parser import DomainExpressionParser

# Type variable for DataFrame/LazyFrame operations
FrameType = TypeVar("FrameType", pl.DataFrame, pl.LazyFrame)


def apply_plot_filters(
    plot_df: FrameType,
    plot_domain: Optional[str] = None,
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
    plot_domain : Optional[str], default None
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
