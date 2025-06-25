"""
Consolidated grouping functions for FIA data analysis.

This module contains all grouping-related logic used across different
FIA estimators, including size classes, species grouping, and custom
grouping configurations.
"""

from typing import Optional, Union, List, Dict, Literal
import polars as pl

from ..constants.constants import (
    DiameterBreakpoints,
    STANDARD_SIZE_CLASSES,
    DESCRIPTIVE_SIZE_CLASSES,
    LandStatus,
    SiteClass,
    ReserveStatus,
)


# Size classes are now imported from constants module


def setup_grouping_columns(
    df: pl.DataFrame,
    grp_by: Optional[Union[str, List[str]]] = None,
    by_species: bool = False,
    by_size_class: bool = False,
    by_land_type: bool = False,
    size_class_type: Literal["standard", "descriptive"] = "standard",
    dia_col: str = "DIA",
) -> tuple[pl.DataFrame, List[str]]:
    """
    Set up grouping columns for FIA estimation.
    
    This function prepares the dataframe with necessary grouping columns
    and returns both the modified dataframe and the list of columns to group by.
    
    Parameters
    ----------
    df : pl.DataFrame
        Input dataframe
    grp_by : str or List[str], optional
        Custom column(s) to group by
    by_species : bool, default False
        Whether to group by species (SPCD)
    by_size_class : bool, default False
        Whether to group by diameter size class
    by_land_type : bool, default False
        Whether to group by land type (for area estimation)
    size_class_type : {"standard", "descriptive"}, default "standard"
        Type of size class labels to use
    dia_col : str, default "DIA"
        Name of diameter column to use for size classes
    
    Returns
    -------
    tuple[pl.DataFrame, List[str]]
        Modified dataframe with grouping columns added, and list of column names to group by
    """
    group_cols = []
    
    # Handle custom grouping columns
    if grp_by is not None:
        if isinstance(grp_by, str):
            group_cols = [grp_by]
        else:
            group_cols = list(grp_by)
    
    # Add species grouping
    if by_species:
        if "SPCD" not in df.columns:
            raise ValueError("SPCD column not found in dataframe for species grouping")
        group_cols.append("SPCD")
    
    # Add size class grouping
    if by_size_class:
        if dia_col not in df.columns:
            raise ValueError(f"{dia_col} column not found in dataframe for size class grouping")
        
        # Add size class column
        size_class_expr = create_size_class_expr(dia_col, size_class_type)
        df = df.with_columns(size_class_expr)
        group_cols.append("sizeClass")
    
    # Add land type grouping (for area estimation)
    if by_land_type:
        if "landType" not in df.columns:
            raise ValueError("landType column not found. Run add_land_type_column() first")
        group_cols.append("landType")
    
    # Remove duplicates while preserving order
    seen = set()
    group_cols = [x for x in group_cols if not (x in seen or seen.add(x))]
    
    return df, group_cols


def create_size_class_expr(
    dia_col: str = "DIA",
    size_class_type: Literal["standard", "descriptive"] = "standard",
) -> pl.Expr:
    """
    Create a Polars expression for diameter size classes.
    
    Parameters
    ----------
    dia_col : str, default "DIA"
        Name of diameter column
    size_class_type : {"standard", "descriptive"}, default "standard"
        Type of size class labels to use:
        - "standard": Numeric ranges (1.0-4.9, 5.0-9.9, etc.)
        - "descriptive": Text labels (Saplings, Small, etc.)
    
    Returns
    -------
    pl.Expr
        Expression that creates 'sizeClass' column based on diameter
    """
    if size_class_type == "standard":
        return (
            pl.when(pl.col(dia_col) < DiameterBreakpoints.MICROPLOT_MAX_DIA).then(pl.lit("1.0-4.9"))
            .when(pl.col(dia_col) < 10.0).then(pl.lit("5.0-9.9"))
            .when(pl.col(dia_col) < 20.0).then(pl.lit("10.0-19.9"))
            .when(pl.col(dia_col) < 30.0).then(pl.lit("20.0-29.9"))
            .otherwise(pl.lit("30.0+"))
            .alias("sizeClass")
        )
    elif size_class_type == "descriptive":
        return (
            pl.when(pl.col(dia_col) < DiameterBreakpoints.MICROPLOT_MAX_DIA).then(pl.lit("Saplings"))
            .when(pl.col(dia_col) < 10.0).then(pl.lit("Small"))
            .when(pl.col(dia_col) < 20.0).then(pl.lit("Medium"))
            .otherwise(pl.lit("Large"))
            .alias("sizeClass")
        )
    else:
        raise ValueError(f"Invalid size_class_type: {size_class_type}")


def add_land_type_column(df: pl.DataFrame) -> pl.DataFrame:
    """
    Add land type category column for area estimation grouping.
    
    Creates a 'landType' column based on COND_STATUS_CD and other attributes.
    
    Parameters
    ----------
    df : pl.DataFrame
        Condition dataframe with COND_STATUS_CD, SITECLCD, and RESERVCD columns
    
    Returns
    -------
    pl.DataFrame
        Dataframe with 'landType' column added
    """
    required_cols = ["COND_STATUS_CD", "SITECLCD", "RESERVCD"]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")
    
    land_type_expr = (
        pl.when(pl.col("COND_STATUS_CD") != LandStatus.FOREST)
        .then(
            pl.when(pl.col("COND_STATUS_CD") == LandStatus.NONFOREST).then(pl.lit("Non-forest"))
            .when(pl.col("COND_STATUS_CD") == LandStatus.WATER).then(pl.lit("Water"))
            .otherwise(pl.lit("Other"))
        )
        .otherwise(
            # Forest land - check if timber
            pl.when(
                (pl.col("SITECLCD").is_in(SiteClass.PRODUCTIVE_CLASSES))
                & (pl.col("RESERVCD") == ReserveStatus.NOT_RESERVED)
            )
            .then(pl.lit("Timber"))
            .otherwise(pl.lit("Non-timber forest"))
        )
        .alias("landType")
    )
    
    return df.with_columns(land_type_expr)


def prepare_plot_groups(
    base_groups: List[str],
    additional_groups: Optional[List[str]] = None,
    always_include: Optional[List[str]] = None,
) -> List[str]:
    """
    Prepare final grouping columns for plot-level aggregation.
    
    This function combines base grouping columns with additional groups
    and ensures certain columns are always included (like PLT_CN).
    
    Parameters
    ----------
    base_groups : List[str]
        Base grouping columns from setup_grouping_columns
    additional_groups : List[str], optional
        Additional columns to include in grouping
    always_include : List[str], optional
        Columns that should always be included (default: ["PLT_CN"])
    
    Returns
    -------
    List[str]
        Final list of grouping columns
    """
    if always_include is None:
        always_include = ["PLT_CN"]
    
    # Start with always_include columns
    final_groups = list(always_include)
    
    # Add base groups
    final_groups.extend(base_groups)
    
    # Add additional groups if provided
    if additional_groups:
        final_groups.extend(additional_groups)
    
    # Remove duplicates while preserving order
    seen = set()
    final_groups = [x for x in final_groups if not (x in seen or seen.add(x))]
    
    return final_groups


def add_species_info(
    df: pl.DataFrame,
    species_df: Optional[pl.DataFrame] = None,
    include_common_name: bool = True,
    include_genus: bool = False,
) -> pl.DataFrame:
    """
    Add species information for grouping and display.
    
    Parameters
    ----------
    df : pl.DataFrame
        Dataframe with SPCD column
    species_df : pl.DataFrame, optional
        REF_SPECIES dataframe. If None, only SPCD is used
    include_common_name : bool, default True
        Whether to include COMMON_NAME column
    include_genus : bool, default False
        Whether to include GENUS column
    
    Returns
    -------
    pl.DataFrame
        Dataframe with species information added
    """
    if "SPCD" not in df.columns:
        raise ValueError("SPCD column not found in dataframe")
    
    if species_df is None:
        return df
    
    # Select columns to join
    join_cols = ["SPCD"]
    if include_common_name:
        join_cols.append("COMMON_NAME")
    if include_genus:
        join_cols.append("GENUS")
    
    # Join species info
    return df.join(
        species_df.select(join_cols),
        on="SPCD",
        how="left",
    )


# standardize_group_names function removed - no longer needed
# All modules now use consistent snake_case naming


def validate_grouping_columns(
    df: pl.DataFrame,
    required_groups: List[str],
) -> None:
    """
    Validate that required grouping columns exist in dataframe.
    
    Parameters
    ----------
    df : pl.DataFrame
        Dataframe to validate
    required_groups : List[str]
        List of required column names
    
    Raises
    ------
    ValueError
        If any required columns are missing
    """
    missing_cols = [col for col in required_groups if col not in df.columns]
    if missing_cols:
        raise ValueError(
            f"Missing required grouping columns: {missing_cols}. "
            f"Available columns: {df.columns}"
        )


def get_size_class_bounds(
    size_class_type: Literal["standard", "descriptive"] = "standard"
) -> Dict[str, tuple[float, float]]:
    """
    Get the diameter bounds for each size class.
    
    Parameters
    ----------
    size_class_type : {"standard", "descriptive"}, default "standard"
        Type of size class definitions to return
    
    Returns
    -------
    Dict[str, tuple[float, float]]
        Dictionary mapping size class labels to (min, max) diameter bounds
    """
    if size_class_type == "standard":
        return STANDARD_SIZE_CLASSES.copy()
    elif size_class_type == "descriptive":
        return DESCRIPTIVE_SIZE_CLASSES.copy()
    else:
        raise ValueError(f"Invalid size_class_type: {size_class_type}")