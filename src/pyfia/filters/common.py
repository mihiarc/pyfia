"""
Common filter functions for FIA estimation modules.

This module consolidates duplicated filter functions used across
multiple estimation modules (volume, biomass, tpa, area, mortality, growth).
These functions provide consistent filtering logic for tree data, area/condition
data, and grouping column setup.
"""

from typing import List, Optional, Union

import polars as pl

from pyfia.constants import (
    DiameterBreakpoints,
    LandStatus,
    ReserveStatus,
    SiteClass,
    TreeClass,
    TreeStatus,
)


def apply_tree_filters_common(
    tree_df: pl.DataFrame,
    tree_type: str = "all",
    tree_domain: Optional[str] = None,
    require_volume: bool = False,
    require_diameter_thresholds: bool = False,
) -> pl.DataFrame:
    """
    Apply tree type and domain filters following rFIA methodology.

    This function provides consistent tree filtering across all estimation modules.
    It handles tree status filtering (live/dead/growing stock/all), applies optional
    user-defined domains, and ensures data validity for estimation.

    Parameters
    ----------
    tree_df : pl.DataFrame
        Tree dataframe to filter
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
    pl.DataFrame
        Filtered tree dataframe

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
    """
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
    tree_df = tree_df.filter(
        (pl.col("DIA").is_not_null()) & (pl.col("TPA_UNADJ") > 0)
    )

    # Additional filter for volume estimation
    if require_volume:
        tree_df = tree_df.filter(
            pl.col("VOLCFGRS").is_not_null()  # At least gross volume required
        )

    # Apply user-defined tree domain
    if tree_domain:
        try:
            tree_df = tree_df.filter(pl.sql_expr(tree_domain))
        except Exception as exc:
            # Normalize parsing errors to ValueError for consistent error handling in tests
            raise ValueError(f"Invalid tree_domain expression: {tree_domain}") from exc

    return tree_df


def apply_area_filters_common(
    cond_df: pl.DataFrame,
    land_type: str = "all",
    area_domain: Optional[str] = None,
    area_estimation_mode: bool = False,
) -> pl.DataFrame:
    """
    Apply land type and area domain filters for condition data.

    This function provides consistent area/condition filtering across all
    estimation modules. It handles land type filtering (forest/timber/all)
    and applies optional user-defined area domains.

    Parameters
    ----------
    cond_df : pl.DataFrame
        Condition dataframe to filter
    land_type : str, default "all"
        Type of land to include:
        - "forest": Forest land only (COND_STATUS_CD == 1)
        - "timber": Productive, unreserved forest land
        - "all": All conditions
    area_domain : Optional[str], default None
        SQL-like expression for additional filtering
    area_estimation_mode : bool, default False
        If True, skip land type filtering (used by area estimation module
        where land type is handled through indicators instead)

    Returns
    -------
    pl.DataFrame
        Filtered condition dataframe

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
        try:
            cond_df = cond_df.filter(pl.sql_expr(area_domain))
        except Exception as exc:
            raise ValueError(f"Invalid area_domain expression: {area_domain}") from exc

    return cond_df


def setup_grouping_columns_common(
    data_df: pl.DataFrame,
    grp_by: Optional[Union[str, List[str]]] = None,
    by_species: bool = False,
    by_size_class: bool = False,
    return_dataframe: bool = True,
) -> Union[tuple[pl.DataFrame, List[str]], List[str]]:
    """
    Set up grouping columns for estimation aggregation.

    This function handles the configuration of grouping columns used in
    estimation functions. It processes user-specified grouping columns,
    adds species grouping if requested, and creates size class categories
    based on tree diameter.

    Parameters
    ----------
    data_df : pl.DataFrame
        Dataframe to add grouping columns to (typically tree_cond joined data)
    grp_by : Optional[Union[str, List[str]]], default None
        Column name(s) to group by in addition to species/size class
    by_species : bool, default False
        If True, add SPCD (species code) to grouping columns
    by_size_class : bool, default False
        If True, create size class column based on diameter and add to grouping
    return_dataframe : bool, default True
        If True, return tuple of (modified_dataframe, group_columns).
        If False, return only group_columns list (for compatibility with
        biomass module pattern).

    Returns
    -------
    Union[tuple[pl.DataFrame, List[str]], List[str]]
        If return_dataframe=True: (modified dataframe, list of grouping columns)
        If return_dataframe=False: list of grouping columns only

    Notes
    -----
    Size classes follow FIA standards:
    - 1.0-4.9": Saplings
    - 5.0-9.9": Pole timber
    - 10.0-19.9": Small sawtimber
    - 20.0-29.9": Large sawtimber
    - 30.0+": Very large trees

    Examples
    --------
    >>> # Group by species and size class
    >>> df, groups = setup_grouping_columns(
    ...     tree_cond,
    ...     by_species=True,
    ...     by_size_class=True
    ... )

    >>> # Custom grouping with forest type
    >>> df, groups = setup_grouping_columns(
    ...     tree_cond,
    ...     grp_by="FORTYPCD",
    ...     by_species=True
    ... )
    """
    group_cols = []

    # Process user-specified grouping columns
    if grp_by:
        if isinstance(grp_by, str):
            group_cols = [grp_by]
        else:
            group_cols = list(grp_by)

    # Add species grouping
    if by_species:
        group_cols.append("SPCD")

    # Add size class grouping
    if by_size_class:
        # Add size class based on diameter (following FIA standards)
        data_df = data_df.with_columns(
            pl.when(pl.col("DIA") < 5.0)
            .then(pl.lit("1.0-4.9"))
            .when(pl.col("DIA") < 10.0)
            .then(pl.lit("5.0-9.9"))
            .when(pl.col("DIA") < 20.0)
            .then(pl.lit("10.0-19.9"))
            .when(pl.col("DIA") < 30.0)
            .then(pl.lit("20.0-29.9"))
            .otherwise(pl.lit("30.0+"))
            .alias("sizeClass")
        )
        group_cols.append("sizeClass")

    # Return based on requested format
    if return_dataframe:
        return data_df, group_cols
    else:
        return group_cols
