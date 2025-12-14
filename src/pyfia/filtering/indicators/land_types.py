"""
Land type classification functions for FIA area estimation.

Simple, direct functions for land type classification without unnecessary
abstraction layers or design patterns.
"""

from enum import Enum

import polars as pl

from ...constants.status_codes import (
    LandStatus,
    ReserveStatus,
    SiteClass,
)


class LandTypeCategory(str, Enum):
    """Standard FIA land type categories."""

    TIMBER = "Timber"
    NON_TIMBER_FOREST = "Non-Timber Forest"
    NON_FOREST = "Non-Forest"
    WATER = "Water"
    OTHER = "Other"


def get_land_domain_indicator(land_type: str) -> pl.Expr:
    """
    Get the domain indicator expression for a land type.

    Parameters
    ----------
    land_type : str
        Type of land ("forest", "timber", or "all")

    Returns
    -------
    pl.Expr
        Polars expression for the land domain indicator

    Examples
    --------
    >>> expr = get_land_domain_indicator("forest")
    >>> df = df.with_columns(expr.cast(pl.Int32).alias("landD"))
    """
    if land_type == "forest":
        return pl.col("COND_STATUS_CD") == LandStatus.FOREST
    elif land_type == "timber":
        return (
            (pl.col("COND_STATUS_CD") == LandStatus.FOREST)
            & pl.col("SITECLCD").is_in(SiteClass.PRODUCTIVE_CLASSES)
            & (pl.col("RESERVCD") == ReserveStatus.NOT_RESERVED)
        )
    else:  # "all"
        return pl.lit(True)


def add_land_type_categories(df: pl.DataFrame) -> pl.DataFrame:
    """
    Add comprehensive land type categories for grouping analysis.

    Adds the standard FIA land type categories used in by_land_type analysis.

    Parameters
    ----------
    df : pl.DataFrame
        DataFrame with condition status and site class data

    Returns
    -------
    pl.DataFrame
        DataFrame with LAND_TYPE column added

    Examples
    --------
    >>> df_with_categories = add_land_type_categories(cond_df)
    >>> # Now has LAND_TYPE column with values like "Timber", "Non-Timber Forest", etc.
    """
    return df.with_columns(
        pl.when(
            (pl.col("COND_STATUS_CD") == LandStatus.FOREST)
            & pl.col("SITECLCD").is_in(SiteClass.PRODUCTIVE_CLASSES)
            & (pl.col("RESERVCD") == ReserveStatus.NOT_RESERVED)
        )
        .then(pl.lit(LandTypeCategory.TIMBER))
        .when(pl.col("COND_STATUS_CD") == LandStatus.FOREST)
        .then(pl.lit(LandTypeCategory.NON_TIMBER_FOREST))
        .when(pl.col("COND_STATUS_CD") == LandStatus.NONFOREST)
        .then(pl.lit(LandTypeCategory.NON_FOREST))
        .when(
            pl.col("COND_STATUS_CD").is_in([LandStatus.WATER, LandStatus.CENSUS_WATER])
        )
        .then(pl.lit(LandTypeCategory.WATER))
        .otherwise(pl.lit(LandTypeCategory.OTHER))
        .alias("LAND_TYPE")
    )


def classify_land_types(
    df: pl.DataFrame, land_type: str = "forest", by_land_type: bool = False
) -> pl.DataFrame:
    """
    Apply land type classification to a dataframe.

    This is the main entry point for land type classification, handling
    both regular and by_land_type analysis modes.

    Parameters
    ----------
    df : pl.DataFrame
        DataFrame with condition data
    land_type : str, default "forest"
        Type of land classification ("forest", "timber", or "all")
    by_land_type : bool, default False
        Whether to add land type categories for grouping

    Returns
    -------
    pl.DataFrame
        DataFrame with land type classifications and/or indicators added

    Examples
    --------
    >>> # Regular forest land analysis
    >>> df = classify_land_types(cond_df, "forest")
    >>>
    >>> # By land type analysis with categories
    >>> df = classify_land_types(cond_df, "forest", by_land_type=True)
    """
    # Add land domain indicator
    land_expr = get_land_domain_indicator(land_type)
    df = df.with_columns(land_expr.cast(pl.Int32).alias("landD"))

    # Add land type categories if doing by_land_type analysis
    if by_land_type:
        df = add_land_type_categories(df)

    return df
