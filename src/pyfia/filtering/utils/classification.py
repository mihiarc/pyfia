"""
Classification filters for FIA data.

This module provides functions for classifying trees and plots based on
FIA specifications such as tree basis assignment, size classes, and
forest type groupings.
"""

from typing import List, Optional

import polars as pl

from ...constants.plot_design import DiameterBreakpoints, PlotBasis


def assign_tree_basis(
    tree_df: pl.DataFrame,
    plot_df: Optional[pl.DataFrame] = None,
    include_macro: bool = True,
    dia_column: str = "DIA",
    macro_breakpoint_column: str = "MACRO_BREAKPOINT_DIA",
    output_column: str = "TREE_BASIS",
) -> pl.DataFrame:
    """
    Assign TREE_BASIS based on tree diameter and plot design.

    Trees are assigned to measurement plots based on their diameter:
    - MICR: Trees 1.0-4.9" DBH (microplot)
    - SUBP: Trees 5.0"+ DBH (subplot)
    - MACR: Large trees based on MACRO_BREAKPOINT_DIA (macroplot)

    Parameters
    ----------
    tree_df : pl.DataFrame
        Tree dataframe with DIA column
    plot_df : pl.DataFrame, optional
        Plot dataframe with MACRO_BREAKPOINT_DIA. Required if include_macro=True
    include_macro : bool, default True
        Whether to check for macroplot assignment
    dia_column : str, default "DIA"
        Column containing tree diameter
    macro_breakpoint_column : str, default "MACRO_BREAKPOINT_DIA"
        Column containing macroplot breakpoint diameter
    output_column : str, default "TREE_BASIS"
        Name for output column

    Returns
    -------
    pl.DataFrame
        Tree dataframe with TREE_BASIS column added

    Examples
    --------
    >>> # Basic tree basis assignment
    >>> trees_with_basis = assign_tree_basis(trees, plots)

    >>> # Simplified assignment (no macroplot)
    >>> trees_simple = assign_tree_basis(trees, include_macro=False)
    """
    if include_macro and plot_df is not None:
        # Join with plot to get MACRO_BREAKPOINT_DIA if not already present
        if macro_breakpoint_column not in tree_df.columns:
            # Support plot tables that expose plot key as either PLT_CN or CN
            right_key = "PLT_CN" if "PLT_CN" in plot_df.columns else "CN"
            tree_df = tree_df.join(
                plot_df.select([right_key, macro_breakpoint_column]),
                left_on="PLT_CN",
                right_on=right_key,
                how="left",
            )

        # Full tree basis assignment with macroplot logic
        tree_basis_expr = (
            pl.when(pl.col(dia_column).is_null())
            .then(None)
            .when(pl.col(dia_column) < DiameterBreakpoints.MICROPLOT_MAX_DIA)
            .then(pl.lit(PlotBasis.MICROPLOT))
            .when(pl.col(macro_breakpoint_column) <= 0)
            .then(pl.lit(PlotBasis.SUBPLOT))
            .when(pl.col(macro_breakpoint_column).is_null())
            .then(pl.lit(PlotBasis.SUBPLOT))
            .when(pl.col(dia_column) < pl.col(macro_breakpoint_column))
            .then(pl.lit(PlotBasis.SUBPLOT))
            .otherwise(pl.lit(PlotBasis.MACROPLOT))
            .alias(output_column)
        )
    else:
        # Simplified assignment (just MICR/SUBP)
        tree_basis_expr = (
            pl.when(pl.col(dia_column) < DiameterBreakpoints.MICROPLOT_MAX_DIA)
            .then(pl.lit(PlotBasis.MICROPLOT))
            .otherwise(pl.lit(PlotBasis.SUBPLOT))
            .alias(output_column)
        )

    return tree_df.with_columns(tree_basis_expr)


def assign_size_class(
    tree_df: pl.DataFrame,
    dia_column: str = "DIA",
    output_column: str = "SIZE_CLASS",
    class_system: str = "standard",
) -> pl.DataFrame:
    """
    Assign size class based on tree diameter.

    Parameters
    ----------
    tree_df : pl.DataFrame
        Tree dataframe with diameter column
    dia_column : str, default "DIA"
        Column containing tree diameter
    output_column : str, default "SIZE_CLASS"
        Name for output column
    class_system : str, default "standard"
        Size class system to use:
        - "standard": Saplings (<5"), Small (5-9.9"), Medium (10-19.9"), Large (20"+)
        - "detailed": More granular classes
        - "simple": Small (<10"), Large (10"+)

    Returns
    -------
    pl.DataFrame
        Tree dataframe with size class column added

    Examples
    --------
    >>> # Standard size classes
    >>> trees_with_size = assign_size_class(trees)

    >>> # Simple size classes
    >>> trees_simple = assign_size_class(trees, class_system="simple")
    """
    if class_system == "standard":
        size_expr = (
            pl.when(pl.col(dia_column) < 5.0)
            .then(pl.lit("Saplings"))
            .when(pl.col(dia_column) < 10.0)
            .then(pl.lit("Small"))
            .when(pl.col(dia_column) < 20.0)
            .then(pl.lit("Medium"))
            .otherwise(pl.lit("Large"))
            .alias(output_column)
        )
    elif class_system == "detailed":
        size_expr = (
            pl.when(pl.col(dia_column) < 1.0)
            .then(pl.lit("Seedlings"))
            .when(pl.col(dia_column) < 5.0)
            .then(pl.lit("Saplings"))
            .when(pl.col(dia_column) < 10.0)
            .then(pl.lit("Small"))
            .when(pl.col(dia_column) < 15.0)
            .then(pl.lit("Medium"))
            .when(pl.col(dia_column) < 25.0)
            .then(pl.lit("Large"))
            .otherwise(pl.lit("Very Large"))
            .alias(output_column)
        )
    elif class_system == "simple":
        size_expr = (
            pl.when(pl.col(dia_column) < 10.0)
            .then(pl.lit("Small"))
            .otherwise(pl.lit("Large"))
            .alias(output_column)
        )
    else:
        raise ValueError(f"Unknown class_system: {class_system}")

    return tree_df.with_columns(size_expr)


def assign_prop_basis(
    cond_df: pl.DataFrame,
    macro_breakpoint_column: str = "MACRO_BREAKPOINT_DIA",
    output_column: str = "PROP_BASIS",
) -> pl.DataFrame:
    """
    Assign PROP_BASIS for condition area calculations.

    Determines whether condition area should use subplot or macroplot
    adjustment factors based on plot design.

    Parameters
    ----------
    cond_df : pl.DataFrame
        Condition dataframe
    macro_breakpoint_column : str, default "MACRO_BREAKPOINT_DIA"
        Column containing macroplot breakpoint diameter
    output_column : str, default "PROP_BASIS"
        Name for output column

    Returns
    -------
    pl.DataFrame
        Condition dataframe with PROP_BASIS column added
    """
    prop_basis_expr = (
        pl.when(pl.col(macro_breakpoint_column) > 0)
        .then(pl.lit(PlotBasis.MACROPLOT))
        .otherwise(pl.lit(PlotBasis.SUBPLOT))
        .alias(output_column)
    )

    return cond_df.with_columns(prop_basis_expr)


def assign_forest_type_group(
    cond_df: pl.DataFrame,
    fortypcd_column: str = "FORTYPCD",
    output_column: str = "FOREST_TYPE_GROUP",
) -> pl.DataFrame:
    """
    Assign forest type groups based on forest type codes.

    Groups forest types into major categories following FIA classification.

    Parameters
    ----------
    cond_df : pl.DataFrame
        Condition dataframe with forest type codes
    fortypcd_column : str, default "FORTYPCD"
        Column containing forest type codes
    output_column : str, default "FOREST_TYPE_GROUP"
        Name for output column

    Returns
    -------
    pl.DataFrame
        Condition dataframe with forest type group column added

    Examples
    --------
    >>> # Add forest type groups
    >>> conds_with_groups = assign_forest_type_group(conditions)
    """
    # Major forest type groupings based on FIA codes
    forest_type_expr = (
        pl.when(pl.col(fortypcd_column).is_between(100, 199))
        .then(pl.lit("White/Red/Jack Pine"))
        .when(pl.col(fortypcd_column).is_between(200, 299))
        .then(pl.lit("Spruce/Fir"))
        .when(pl.col(fortypcd_column).is_between(300, 399))
        .then(pl.lit("Longleaf/Slash Pine"))
        .when(pl.col(fortypcd_column).is_between(400, 499))
        .then(pl.lit("Loblolly/Shortleaf Pine"))
        .when(pl.col(fortypcd_column).is_between(500, 599))
        .then(pl.lit("Oak/Pine"))
        .when(pl.col(fortypcd_column).is_between(600, 699))
        .then(pl.lit("Oak/Hickory"))
        .when(pl.col(fortypcd_column).is_between(700, 799))
        .then(pl.lit("Oak/Gum/Cypress"))
        .when(pl.col(fortypcd_column).is_between(800, 899))
        .then(pl.lit("Elm/Ash/Cottonwood"))
        .when(pl.col(fortypcd_column).is_between(900, 999))
        .then(pl.lit("Maple/Beech/Birch"))
        .otherwise(pl.lit("Other/Unknown"))
        .alias(output_column)
    )

    return cond_df.with_columns(forest_type_expr)


def assign_land_use_class(
    cond_df: pl.DataFrame,
    cond_status_column: str = "COND_STATUS_CD",
    reserve_column: str = "RESERVCD",
    output_column: str = "LAND_USE_CLASS",
) -> pl.DataFrame:
    """
    Assign land use classes based on condition status and reserve codes.

    Parameters
    ----------
    cond_df : pl.DataFrame
        Condition dataframe
    cond_status_column : str, default "COND_STATUS_CD"
        Column containing condition status codes
    reserve_column : str, default "RESERVCD"
        Column containing reserve codes
    output_column : str, default "LAND_USE_CLASS"
        Name for output column

    Returns
    -------
    pl.DataFrame
        Condition dataframe with land use class column added
    """
    land_use_expr = (
        pl.when(pl.col(cond_status_column) == 1)
        .then(
            pl.when(pl.col(reserve_column) == 0)
            .then(pl.lit("Timberland"))
            .otherwise(pl.lit("Reserved Forest"))
        )
        .when(pl.col(cond_status_column) == 2)
        .then(pl.lit("Other Forest"))
        .when(pl.col(cond_status_column) == 3)
        .then(pl.lit("Non-forest"))
        .otherwise(pl.lit("Other/Unknown"))
        .alias(output_column)
    )

    return cond_df.with_columns(land_use_expr)


def assign_species_group(
    tree_df: pl.DataFrame,
    species_df: pl.DataFrame,
    spcd_column: str = "SPCD",
    grouping_system: str = "major_species",
    output_column: str = "SPECIES_GROUP",
) -> pl.DataFrame:
    """
    Assign species groups based on species codes.

    Parameters
    ----------
    tree_df : pl.DataFrame
        Tree dataframe with species codes
    species_df : pl.DataFrame
        Species reference dataframe
    spcd_column : str, default "SPCD"
        Column containing species codes
    grouping_system : str, default "major_species"
        Grouping system to use:
        - "major_species": Major commercial species groups
        - "genus": Group by genus
        - "family": Group by family
    output_column : str, default "SPECIES_GROUP"
        Name for output column

    Returns
    -------
    pl.DataFrame
        Tree dataframe with species group column added
    """
    if grouping_system == "major_species":
        # Create major species groups based on common FIA groupings
        species_groups = species_df.with_columns(
            pl.when(pl.col("SPCD").is_in([131, 132, 133]))  # Pines
            .then(pl.lit("Southern Pines"))
            .when(pl.col("SPCD").is_in([316, 318, 319]))  # Maples
            .then(pl.lit("Maples"))
            .when(pl.col("SPCD").is_in([800, 801, 802, 803, 804]))  # Oaks
            .then(pl.lit("Oaks"))
            .when(pl.col("GENUS") == "Quercus")
            .then(pl.lit("Oaks"))
            .when(pl.col("GENUS") == "Pinus")
            .then(pl.lit("Pines"))
            .when(pl.col("GENUS") == "Acer")
            .then(pl.lit("Maples"))
            .otherwise(pl.col("GENUS"))
            .alias(output_column)
        )
    elif grouping_system == "genus":
        species_groups = species_df.select(
            [spcd_column, pl.col("GENUS").alias(output_column)]
        )
    elif grouping_system == "family":
        species_groups = species_df.select(
            [spcd_column, pl.col("FAMILY").alias(output_column)]
        )
    else:
        raise ValueError(f"Unknown grouping_system: {grouping_system}")

    return tree_df.join(
        species_groups.select([spcd_column, output_column]), on=spcd_column, how="left"
    )


def validate_classification_columns(
    df: pl.DataFrame,
    classification_type: str,
    required_columns: Optional[List[str]] = None,
) -> bool:
    """
    Validate that DataFrame has required columns for classification operations.

    Parameters
    ----------
    df : pl.DataFrame
        DataFrame to validate
    classification_type : str
        Type of classification: "tree_basis", "size_class", "prop_basis", etc.
    required_columns : List[str], optional
        List of required columns. If None, uses defaults for classification type.

    Returns
    -------
    bool
        True if all required columns are present

    Raises
    ------
    ValueError
        If required columns are missing
    """
    if required_columns is None:
        if classification_type == "tree_basis":
            required_columns = ["DIA"]
        elif classification_type == "size_class":
            required_columns = ["DIA"]
        elif classification_type == "prop_basis":
            required_columns = ["MACRO_BREAKPOINT_DIA"]
        elif classification_type == "forest_type":
            required_columns = ["FORTYPCD"]
        elif classification_type == "land_use":
            required_columns = ["COND_STATUS_CD", "RESERVCD"]
        elif classification_type == "species_group":
            required_columns = ["SPCD"]
        else:
            raise ValueError(f"Unknown classification_type: {classification_type}")

    missing_columns = [col for col in required_columns if col not in df.columns]

    if missing_columns:
        raise ValueError(
            f"Missing required columns for {classification_type}: {missing_columns}"
        )

    return True
