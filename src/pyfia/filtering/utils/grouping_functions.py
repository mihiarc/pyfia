"""
Consolidated grouping functions for FIA data analysis.

This module contains all grouping-related logic used across different
FIA estimators, including size classes, species grouping, and custom
grouping configurations.
"""

from typing import Dict, List, Literal, Optional, Union

import polars as pl

from ...constants.plot_design import (
    DESCRIPTIVE_SIZE_CLASSES,
    STANDARD_SIZE_CLASSES,
    DiameterBreakpoints,
)
from ...constants.status_codes import (
    LandStatus,
    ReserveStatus,
    SiteClass,
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
        ColumnValidator.validate_columns(
            df,
            required_columns="SPCD",
            context="species grouping",
            raise_on_missing=True,
        )
        group_cols.append("SPCD")

    # Add size class grouping
    if by_size_class:
        ColumnValidator.validate_columns(
            df,
            required_columns=dia_col,
            context="size class grouping",
            raise_on_missing=True,
        )

        # Add size class column (standardize to UPPER_SNAKE_CASE)
        size_class_expr = create_size_class_expr(dia_col, size_class_type)
        df = df.with_columns(size_class_expr)
        group_cols.append("SIZE_CLASS")

    # Add land type grouping (for area estimation)
    if by_land_type:
        ColumnValidator.validate_columns(
            df,
            required_columns="landType",
            context="land type grouping (run add_land_type_column() first)",
            raise_on_missing=True,
        )
        group_cols.append("landType")

    # Remove duplicates while preserving order
    seen: set[str] = set()
    group_cols = [x for x in group_cols if not (x in seen or seen.add(x))]  # type: ignore[func-returns-value]

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
            pl.when(pl.col(dia_col) < DiameterBreakpoints.MICROPLOT_MAX_DIA)
            .then(pl.lit("1.0-4.9"))
            .when(pl.col(dia_col) < 10.0)
            .then(pl.lit("5.0-9.9"))
            .when(pl.col(dia_col) < 20.0)
            .then(pl.lit("10.0-19.9"))
            .when(pl.col(dia_col) < 30.0)
            .then(pl.lit("20.0-29.9"))
            .otherwise(pl.lit("30.0+"))
            .alias("SIZE_CLASS")
        )
    elif size_class_type == "descriptive":
        return (
            pl.when(pl.col(dia_col) < DiameterBreakpoints.MICROPLOT_MAX_DIA)
            .then(pl.lit("Saplings"))
            .when(pl.col(dia_col) < 10.0)
            .then(pl.lit("Small"))
            .when(pl.col(dia_col) < 20.0)
            .then(pl.lit("Medium"))
            .otherwise(pl.lit("Large"))
            .alias("SIZE_CLASS")
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
            pl.when(pl.col("COND_STATUS_CD") == LandStatus.NONFOREST)
            .then(pl.lit("Non-forest"))
            .when(pl.col("COND_STATUS_CD") == LandStatus.WATER)
            .then(pl.lit("Water"))
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
    seen: set[str] = set()
    final_groups = [x for x in final_groups if not (x in seen or seen.add(x))]  # type: ignore[func-returns-value]

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


from .validation import ColumnValidator


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
    ColumnValidator.validate_columns(
        df,
        required_columns=required_groups,
        context="grouping",
        raise_on_missing=True,
        include_available=True,  # Include available columns in error message
    )


def get_size_class_bounds(
    size_class_type: Literal["standard", "descriptive"] = "standard",
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


def get_forest_type_group(fortypcd: Optional[int]) -> str:
    """
    Map forest type code (FORTYPCD) to forest type group name.

    Groups forest types into major categories following FIA classification
    with special handling for common western forest types.

    Parameters
    ----------
    fortypcd : int or None
        Forest type code from COND table

    Returns
    -------
    str
        Forest type group name

    Examples
    --------
    >>> get_forest_type_group(200)
    'Douglas-fir'
    >>> get_forest_type_group(221)
    'Ponderosa Pine'
    >>> get_forest_type_group(None)
    'Unknown'
    """
    if fortypcd is None:
        return "Unknown"
    elif 100 <= fortypcd <= 199:
        return "White/Red/Jack Pine"
    elif 200 <= fortypcd <= 299:
        if fortypcd == 200:
            return "Douglas-fir"
        elif fortypcd in [220, 221, 222]:
            return "Ponderosa Pine"
        elif fortypcd == 240:
            return "Western White Pine"
        elif fortypcd in [260, 261, 262, 263, 264, 265]:
            return "Fir/Spruce/Mountain Hemlock"
        elif fortypcd == 280:
            return "Lodgepole Pine"
        else:
            return "Spruce/Fir"
    elif 300 <= fortypcd <= 399:
        if fortypcd in [300, 301, 302, 303, 304, 305]:
            return "Hemlock/Sitka Spruce"
        elif fortypcd == 370:
            return "California Mixed Conifer"
        else:
            return "Longleaf/Slash Pine"
    elif 400 <= fortypcd <= 499:
        return "Oak/Pine"
    elif 500 <= fortypcd <= 599:
        return "Oak/Hickory"
    elif 600 <= fortypcd <= 699:
        return "Oak/Gum/Cypress"
    elif 700 <= fortypcd <= 799:
        return "Elm/Ash/Cottonwood"
    elif 800 <= fortypcd <= 899:
        return "Maple/Beech/Birch"
    elif 900 <= fortypcd <= 999:
        if 900 <= fortypcd <= 909:
            return "Aspen/Birch"
        elif 910 <= fortypcd <= 919:
            return "Alder/Maple"
        elif 920 <= fortypcd <= 929:
            return "Western Oak"
        elif 940 <= fortypcd <= 949:
            return "Tanoak/Laurel"
        elif 950 <= fortypcd <= 959:
            return "Other Western Hardwoods"
        elif 960 <= fortypcd <= 969:
            return "Tropical Hardwoods"
        elif 970 <= fortypcd <= 979:
            return "Exotic Hardwoods"
        elif 980 <= fortypcd <= 989:
            return "Woodland Hardwoods"
        elif 990 <= fortypcd <= 998:
            return "Exotic Softwoods"
        elif fortypcd == 999:
            return "Nonstocked"
        else:
            return "Other Hardwoods"
    else:
        return "Other"


def add_forest_type_group(
    df: pl.DataFrame,
    fortypcd_col: str = "FORTYPCD",
    output_col: str = "FOREST_TYPE_GROUP",
) -> pl.DataFrame:
    """
    Add forest type group column to a dataframe containing FORTYPCD.

    Parameters
    ----------
    df : pl.DataFrame
        DataFrame containing forest type codes
    fortypcd_col : str, default "FORTYPCD"
        Name of column containing forest type codes
    output_col : str, default "FOREST_TYPE_GROUP"
        Name for the output column

    Returns
    -------
    pl.DataFrame
        DataFrame with forest type group column added

    Examples
    --------
    >>> cond_with_groups = add_forest_type_group(cond_df)
    >>> # Group by forest type for analysis
    >>> by_forest_type = cond_with_groups.group_by("FOREST_TYPE_GROUP").agg(...)
    """
    return df.with_columns(
        pl.col(fortypcd_col)
        .map_elements(get_forest_type_group, return_dtype=pl.Utf8)
        .alias(output_col)
    )


def get_ownership_group_name(owngrpcd: Optional[int]) -> str:
    """
    Map ownership group code to descriptive name.

    Parameters
    ----------
    owngrpcd : int or None
        Ownership group code from FIA

    Returns
    -------
    str
        Ownership group name

    Examples
    --------
    >>> get_ownership_group_name(10)
    'Forest Service'
    >>> get_ownership_group_name(40)
    'Private'
    """
    ownership_names = {
        10: "Forest Service",
        20: "Other Federal",
        30: "State and Local Government",
        40: "Private",
    }
    if owngrpcd is None:
        return "Unknown (Code None)"
    return ownership_names.get(owngrpcd, f"Unknown (Code {owngrpcd})")


def add_ownership_group_name(
    df: pl.DataFrame,
    owngrpcd_col: str = "OWNGRPCD",
    output_col: str = "OWNERSHIP_GROUP",
) -> pl.DataFrame:
    """
    Add ownership group name column to a dataframe containing OWNGRPCD.

    Parameters
    ----------
    df : pl.DataFrame
        DataFrame containing ownership group codes
    owngrpcd_col : str, default "OWNGRPCD"
        Name of column containing ownership group codes
    output_col : str, default "OWNERSHIP_GROUP"
        Name for the output column

    Returns
    -------
    pl.DataFrame
        DataFrame with ownership group name column added
    """
    return df.with_columns(
        pl.col(owngrpcd_col)
        .map_elements(get_ownership_group_name, return_dtype=pl.Utf8)
        .alias(output_col)
    )


def get_forest_type_group_code(fortypcd: Optional[int]) -> Optional[int]:
    """
    Map forest type code (FORTYPCD) to forest type group code (FORTYPGRP).

    This provides the numeric group code that corresponds to forest type
    groupings used in FIA reference tables.

    Parameters
    ----------
    fortypcd : int or None
        Forest type code from COND table

    Returns
    -------
    int or None
        Forest type group code

    Examples
    --------
    >>> get_forest_type_group_code(200)  # Douglas-fir
    200
    >>> get_forest_type_group_code(221)  # Ponderosa Pine
    220
    """
    if fortypcd is None:
        return None

    # Map specific codes to their group codes
    # Based on FIA forest type groupings
    group_mappings = {
        # Douglas-fir group
        200: 200,
        201: 200,
        202: 200,
        203: 200,
        # Ponderosa Pine group
        220: 220,
        221: 220,
        222: 220,
        # Western White Pine
        240: 240,
        241: 240,
        # Fir/Spruce/Mountain Hemlock group
        260: 260,
        261: 260,
        262: 260,
        263: 260,
        264: 260,
        265: 260,
        # Lodgepole Pine
        280: 280,
        281: 280,
        # Hemlock/Sitka Spruce
        300: 300,
        301: 300,
        302: 300,
        303: 300,
        304: 300,
        305: 300,
        # California Mixed Conifer
        370: 370,
        371: 370,
        # Alder/Maple
        910: 910,
        911: 910,
        912: 910,
        913: 910,
        914: 910,
        915: 910,
        # Western Oak
        920: 920,
        921: 920,
        922: 920,
        923: 920,
        924: 920,
        # Tanoak/Laurel
        940: 940,
        941: 940,
        942: 940,
        # Other Western Hardwoods
        950: 950,
        951: 950,
        952: 950,
        # Nonstocked
        999: 999,
    }

    # Check if specific mapping exists
    if fortypcd in group_mappings:
        return group_mappings[fortypcd]

    # Otherwise, use the hundred's place as the group
    # This works for most eastern forest types
    return (fortypcd // 100) * 100


def add_forest_type_group_code(
    df: pl.DataFrame, fortypcd_col: str = "FORTYPCD", output_col: str = "FORTYPGRP"
) -> pl.DataFrame:
    """
    Add forest type group code column to a dataframe containing FORTYPCD.

    This creates the FORTYPGRP column that can be used for grouping
    in area() and other estimation functions.

    Parameters
    ----------
    df : pl.DataFrame
        DataFrame containing forest type codes
    fortypcd_col : str, default "FORTYPCD"
        Name of column containing forest type codes
    output_col : str, default "FORTYPGRP"
        Name for the output column

    Returns
    -------
    pl.DataFrame
        DataFrame with forest type group code column added

    Examples
    --------
    >>> # Add FORTYPGRP before using area() function
    >>> cond_with_grp = add_forest_type_group_code(cond_df)
    >>> results = area(db, grp_by=["FORTYPGRP"])
    """
    return df.with_columns(
        pl.col(fortypcd_col)
        .map_elements(get_forest_type_group_code, return_dtype=pl.Int32)
        .alias(output_col)
    )


def auto_enhance_grouping_data(
    data_df: pl.DataFrame,
    group_cols: List[str],
    preserve_reference_columns: bool = True,
) -> tuple[pl.DataFrame, List[str]]:
    """
    Automatically enhance grouping data with reference information.

    This function intelligently adds enhanced columns for common FIA grouping
    variables to make output more interpretable while preserving original
    columns for reference.

    Parameters
    ----------
    data_df : pl.DataFrame
        Input dataframe to enhance
    group_cols : List[str]
        List of grouping columns to potentially enhance
    preserve_reference_columns : bool, default True
        Whether to preserve original columns alongside enhanced ones

    Returns
    -------
    tuple[pl.DataFrame, List[str]]
        Enhanced dataframe and updated list of grouping columns

    Examples
    --------
    >>> # Enhance data with forest type group names
    >>> enhanced_df, enhanced_cols = auto_enhance_grouping_data(
    ...     cond_df, ["FORTYPCD", "OWNGRPCD"]
    ... )
    >>> # Now has FORTYPCD + FOREST_TYPE_GROUP, OWNGRPCD + OWNERSHIP_GROUP
    """
    enhanced_df = data_df
    enhanced_group_cols = group_cols.copy()

    # Track columns that were enhanced for reference preservation
    enhanced_mappings = {}

    # Enhance FORTYPCD with forest type groups
    if "FORTYPCD" in group_cols and "FORTYPCD" in enhanced_df.columns:
        # Add forest type group code (FORTYPGRP) for grouping
        enhanced_df = add_forest_type_group_code(enhanced_df)
        enhanced_mappings["FORTYPCD"] = "FORTYPGRP"

        # Also add descriptive name for better output readability
        enhanced_df = add_forest_type_group(enhanced_df)

        # Replace FORTYPCD with FORTYPGRP in grouping columns if not preserving references
        if not preserve_reference_columns:
            enhanced_group_cols = [
                "FORTYPGRP" if col == "FORTYPCD" else col for col in enhanced_group_cols
            ]
        else:
            # Add FORTYPGRP to grouping columns alongside FORTYPCD (if not already there)
            if "FORTYPGRP" not in enhanced_group_cols:
                idx = enhanced_group_cols.index("FORTYPCD")
                enhanced_group_cols.insert(idx + 1, "FORTYPGRP")

    # Enhance OWNGRPCD with ownership group names
    if "OWNGRPCD" in group_cols and "OWNGRPCD" in enhanced_df.columns:
        enhanced_df = add_ownership_group_name(enhanced_df)
        enhanced_mappings["OWNGRPCD"] = "OWNERSHIP_GROUP"

        if not preserve_reference_columns:
            enhanced_group_cols = [
                "OWNERSHIP_GROUP" if col == "OWNGRPCD" else col
                for col in enhanced_group_cols
            ]
        else:
            # Add OWNERSHIP_GROUP alongside OWNGRPCD
            if "OWNGRPCD" in enhanced_group_cols:
                idx = enhanced_group_cols.index("OWNGRPCD")
                enhanced_group_cols.insert(idx + 1, "OWNERSHIP_GROUP")

    # Enhance SPCD with species information if available
    # Note: This would require species reference table, which may not always be available
    # For now, we just preserve SPCD as-is but could be extended later

    return enhanced_df, enhanced_group_cols
