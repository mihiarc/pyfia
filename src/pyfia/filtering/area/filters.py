"""
Area and condition filtering functions for FIA estimation.

This module provides area/condition-level filtering logic used across all estimation
modules, including land type filtering (forest/timber) and custom area domains.
"""

from typing import Optional

import polars as pl

from ...constants.status_codes import LandStatus, ReserveStatus, SiteClass
from ..core.parser import DomainExpressionParser


def apply_area_filters(
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
        cond_df = DomainExpressionParser.apply_to_dataframe(
            cond_df, area_domain, "area"
        )

    return cond_df
