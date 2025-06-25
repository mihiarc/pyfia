"""
Consolidated filtering functions for FIA data analysis.

This module contains all tree and area filtering logic used across
different FIA estimators to reduce code duplication and ensure
consistent filtering methodology.
"""

from typing import Optional, Tuple

import polars as pl

from ..constants.constants import (
    TreeStatus,
    TreeClass,
    LandStatus,
    SiteClass,
    ReserveStatus,
)


def apply_tree_filters(
    tree_df: pl.DataFrame,
    tree_type: str = "all",
    tree_domain: Optional[str] = None,
) -> pl.DataFrame:
    """
    Apply tree type and domain filters following FIA methodology.
    
    Parameters
    ----------
    tree_df : pl.DataFrame
        Tree dataframe to filter
    tree_type : str, default "all"
        Type of trees to include:
        - "all": All trees
        - "live": Live trees only (STATUSCD == TreeStatus.LIVE)
        - "dead": Dead trees only (STATUSCD == TreeStatus.DEAD)
        - "gs": Growing stock trees (live, sound, commercial species)
        - "live_gs": Live growing stock
        - "dead_gs": Dead growing stock
    tree_domain : str, optional
        Additional filter expression (e.g., "DIA >= 5")
    
    Returns
    -------
    pl.DataFrame
        Filtered tree dataframe
    """
    # Apply tree type filters
    if tree_type == "live":
        tree_df = tree_df.filter(pl.col("STATUSCD") == TreeStatus.LIVE)
    elif tree_type == "dead":
        tree_df = tree_df.filter(pl.col("STATUSCD") == TreeStatus.DEAD)
    elif tree_type == "gs":
        # Growing stock: live, sound, commercial species
        tree_df = tree_df.filter(
            (pl.col("STATUSCD") == TreeStatus.LIVE)  # Live
            & (pl.col("TREECLCD") == TreeClass.GROWING_STOCK)  # Growing stock
            & (pl.col("AGENTCD") < 30)  # No severe damage
        )
    elif tree_type == "live_gs":
        tree_df = tree_df.filter(
            (pl.col("STATUSCD") == TreeStatus.LIVE)
            & (pl.col("TREECLCD") == TreeClass.GROWING_STOCK)
            & (pl.col("AGENTCD") < 30)
        )
    elif tree_type == "dead_gs":
        tree_df = tree_df.filter(
            (pl.col("STATUSCD") == TreeStatus.DEAD)
            & (pl.col("TREECLCD") == TreeClass.GROWING_STOCK)
            & (pl.col("AGENTCD") < 30)
        )
    elif tree_type != "all":
        raise ValueError(f"Invalid tree_type: {tree_type}")
    
    # Apply custom domain filter if provided
    if tree_domain:
        tree_df = parse_domain_expression(tree_df, tree_domain, "tree")
    
    return tree_df


def apply_area_filters(
    cond_df: pl.DataFrame,
    land_type: str = "forest",
    area_domain: Optional[str] = None,
) -> pl.DataFrame:
    """
    Apply land type and area domain filters.
    
    Parameters
    ----------
    cond_df : pl.DataFrame
        Condition dataframe to filter
    land_type : str, default "forest"
        Type of land to include:
        - "forest": Forest land (COND_STATUS_CD == LandStatus.FOREST)
        - "timber": Timberland (forest + productive + unreserved)
        - "all": All conditions
    area_domain : str, optional
        Additional filter expression (e.g., "OWNGRPCD == 10")
    
    Returns
    -------
    pl.DataFrame
        Filtered condition dataframe
    """
    # Apply land type filters
    if land_type == "forest":
        cond_df = cond_df.filter(pl.col("COND_STATUS_CD") == LandStatus.FOREST)
    elif land_type == "timber":
        # Timberland: forest + productive + unreserved
        cond_df = cond_df.filter(
            (pl.col("COND_STATUS_CD") == LandStatus.FOREST)  # Forest
            & (pl.col("SITECLCD").is_in(SiteClass.PRODUCTIVE_CLASSES))  # Productive
            & (pl.col("RESERVCD") == ReserveStatus.NOT_RESERVED)  # Not reserved
        )
    elif land_type != "all":
        raise ValueError(f"Invalid land_type: {land_type}")
    
    # Apply custom domain filter if provided
    if area_domain:
        cond_df = parse_domain_expression(cond_df, area_domain, "area")
    
    return cond_df


def parse_domain_expression(
    df: pl.DataFrame,
    domain: str,
    domain_type: str = "tree",
) -> pl.DataFrame:
    """
    Parse and apply custom domain expressions safely.
    
    Parameters
    ----------
    df : pl.DataFrame
        Dataframe to filter
    domain : str
        Filter expression to parse
    domain_type : str, default "tree"
        Type of domain ("tree" or "area") for validation
    
    Returns
    -------
    pl.DataFrame
        Filtered dataframe
    """
    try:
        # First try as a Polars expression
        # Replace common SQL operators with Polars equivalents
        polars_expr = domain.replace(" and ", " & ").replace(" or ", " | ")
        
        # Create a namespace with column references
        namespace = {col: pl.col(col) for col in df.columns}
        
        # Evaluate the expression
        filter_expr = eval(polars_expr, {"pl": pl}, namespace)
        return df.filter(filter_expr)
    except Exception:
        # Fallback: try as raw SQL expression
        try:
            return df.sql(f"SELECT * FROM self WHERE {domain}")
        except Exception as e:
            raise ValueError(f"Invalid {domain_type} domain expression: {domain}") from e


def apply_growing_stock_filter(
    tree_df: pl.DataFrame,
    gs_type: str = "standard",
) -> pl.DataFrame:
    """
    Apply growing stock filters per FIA definitions.
    
    Parameters
    ----------
    tree_df : pl.DataFrame
        Tree dataframe to filter
    gs_type : str, default "standard"
        Type of growing stock definition:
        - "standard": Standard GS definition
        - "merchantable": For merchantable volume
        - "board_foot": For board foot volume
    
    Returns
    -------
    pl.DataFrame
        Filtered tree dataframe
    """
    # Base growing stock filter
    gs_filter = (
        (pl.col("STATUSCD") == TreeStatus.LIVE)  # Live
        & (pl.col("TREECLCD") == TreeClass.GROWING_STOCK)  # Growing stock class
        & (pl.col("AGENTCD") < 30)  # No severe damage
    )
    
    if gs_type == "merchantable":
        # Additional filters for merchantable volume
        gs_filter = gs_filter & (pl.col("DIA") >= 5.0)
    elif gs_type == "board_foot":
        # Board foot requires larger diameter
        gs_filter = gs_filter & (pl.col("DIA") >= 9.0)
    elif gs_type != "standard":
        raise ValueError(f"Invalid gs_type: {gs_type}")
    
    return tree_df.filter(gs_filter)


def apply_mortality_filters(
    tree_df: pl.DataFrame,
    tree_class: str = "all",
) -> pl.DataFrame:
    """
    Apply mortality-specific filters.
    
    Parameters
    ----------
    tree_df : pl.DataFrame
        Tree dataframe to filter
    tree_class : str, default "all"
        Tree classification:
        - "all": All mortality trees
        - "growing_stock": Only growing stock mortality
    
    Returns
    -------
    pl.DataFrame
        Filtered tree dataframe
    """
    # Base mortality filter - trees with mortality component
    mort_df = tree_df.filter(pl.col("COMPONENT").str.contains("MORTALITY"))
    
    if tree_class == "growing_stock":
        # Apply growing stock filters to mortality trees
        mort_df = mort_df.filter(
            (pl.col("TREECLCD") == TreeClass.GROWING_STOCK)  # Growing stock class
            & (pl.col("AGENTCD") < 30)  # No severe damage at time of death
        )
    elif tree_class != "all":
        raise ValueError(f"Invalid tree_class: {tree_class}")
    
    return mort_df


def apply_standard_filters(
    tree_df: pl.DataFrame,
    cond_df: pl.DataFrame,
    tree_type: str = "all",
    land_type: str = "forest",
    tree_domain: Optional[str] = None,
    area_domain: Optional[str] = None,
) -> Tuple[pl.DataFrame, pl.DataFrame]:
    """
    Apply standard tree and area filters together.
    
    This is a convenience function that applies both tree and area
    filters in one call, returning both filtered dataframes.
    
    Parameters
    ----------
    tree_df : pl.DataFrame
        Tree dataframe to filter
    cond_df : pl.DataFrame
        Condition dataframe to filter
    tree_type : str, default "all"
        Type of trees to include
    land_type : str, default "forest"
        Type of land to include
    tree_domain : str, optional
        Additional tree filter expression
    area_domain : str, optional
        Additional area filter expression
    
    Returns
    -------
    tuple[pl.DataFrame, pl.DataFrame]
        Filtered (tree_df, cond_df) tuple
    """
    # Apply tree filters
    filtered_trees = apply_tree_filters(tree_df, tree_type, tree_domain)
    
    # Apply area filters
    filtered_conds = apply_area_filters(cond_df, land_type, area_domain)
    
    return filtered_trees, filtered_conds


def get_size_class_expr() -> pl.Expr:
    """
    Get Polars expression for FIA standard size classes.
    
    Returns
    -------
    pl.Expr
        Expression that creates 'sizeClass' column based on DIA
    """
    return (
        pl.when(pl.col("DIA") < 5.0).then(pl.lit("1.0-4.9"))
        .when(pl.col("DIA") < 10.0).then(pl.lit("5.0-9.9"))
        .when(pl.col("DIA") < 20.0).then(pl.lit("10.0-19.9"))
        .when(pl.col("DIA") < 30.0).then(pl.lit("20.0-29.9"))
        .otherwise(pl.lit("30.0+"))
        .alias("sizeClass")
    )


def validate_filters(
    tree_type: str = "all",
    land_type: str = "forest",
    gs_type: str = "standard",
) -> None:
    """
    Validate filter type parameters.
    
    Parameters
    ----------
    tree_type : str
        Tree type to validate
    land_type : str
        Land type to validate
    gs_type : str
        Growing stock type to validate
    
    Raises
    ------
    ValueError
        If any filter type is invalid
    """
    valid_tree_types = {"all", "live", "dead", "gs", "live_gs", "dead_gs"}
    valid_land_types = {"all", "forest", "timber"}
    valid_gs_types = {"standard", "merchantable", "board_foot"}
    
    if tree_type not in valid_tree_types:
        raise ValueError(
            f"Invalid tree_type: {tree_type}. "
            f"Valid options: {', '.join(valid_tree_types)}"
        )
    
    if land_type not in valid_land_types:
        raise ValueError(
            f"Invalid land_type: {land_type}. "
            f"Valid options: {', '.join(valid_land_types)}"
        )
    
    if gs_type not in valid_gs_types:
        raise ValueError(
            f"Invalid gs_type: {gs_type}. "
            f"Valid options: {', '.join(valid_gs_types)}"
        )