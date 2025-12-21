"""
Tree expansion factor calculation for FIA data.

This module contains the CRITICAL logic for calculating tree expansion factors
based on the FIA nested plot design. This logic is essential for accurate
tree counts, volume, biomass, carbon, and all per-acre estimates.

The FIA uses a nested plot design with different sampling intensities:
- Microplot (6.8 ft radius): All trees 1.0" - 4.9" DBH
- Subplot (24.0 ft radius): All trees 5.0"+ DBH
- Macroplot (58.9 ft radius): Large trees above a regional breakpoint diameter

Reference:
    Bechtold, W.A.; Patterson, P.L., eds. 2005. The Enhanced Forest Inventory
    and Analysis Program - National Sampling Design and Estimation Procedures.
    Gen. Tech. Rep. SRS-80. https://doi.org/10.2737/SRS-GTR-80

    Key sections:
    - Chapter 3, pp. 27-52: Plot design and nested sampling structure
    - Section 3.4.3, pp. 40-42: Nonsampled plots and plot replacement
    - Eq. 4.2, p. 49: Adjustment factor (1/p_mh) for non-sampled plots
    - Eq. 4.8, p. 53: Tree attribute estimation (y_hid)

CRITICAL: This module must be used for ALL tree-based calculations to ensure
accurate expansion from sample plots to population estimates.
"""

from typing import Optional, Union

import polars as pl


def get_adjustment_factor_expr(
    size_col: str = "DIA",
    macro_breakpoint_col: str = "MACRO_BREAKPOINT_DIA",
    adj_factor_micr_col: str = "ADJ_FACTOR_MICR",
    adj_factor_subp_col: str = "ADJ_FACTOR_SUBP",
    adj_factor_macr_col: str = "ADJ_FACTOR_MACR",
) -> pl.Expr:
    """
    Get Polars expression for tree adjustment factor selection.

    This is the CORE LOGIC for FIA tree expansion. It determines which
    adjustment factor to use based on tree diameter and plot design.

    Parameters
    ----------
    size_col : str
        Column name for tree diameter (DBH in inches)
    macro_breakpoint_col : str
        Column name for macroplot breakpoint diameter
    adj_factor_micr_col : str
        Column name for microplot adjustment factor
    adj_factor_subp_col : str
        Column name for subplot adjustment factor
    adj_factor_macr_col : str
        Column name for macroplot adjustment factor

    Returns
    -------
    pl.Expr
        Polars expression that selects the appropriate adjustment factor

    Notes
    -----
    The logic is:
    1. NULL diameter → subplot factor (default)
    2. Diameter < 5.0" → microplot factor
    3. 5.0" ≤ diameter < macro_breakpoint → subplot factor
    4. Diameter ≥ macro_breakpoint → macroplot factor

    NULL or missing MACRO_BREAKPOINT_DIA is treated as 9999 (no macroplot).
    """
    # Cast macro_breakpoint to Float64 to handle cases where it's stored as String
    macro_breakpoint_expr = (
        pl.col(macro_breakpoint_col).cast(pl.Float64, strict=False).fill_null(9999.0)
    )

    return (
        pl.when(pl.col(size_col).is_null())
        .then(pl.col(adj_factor_subp_col))  # NULL diameter uses subplot
        .when(pl.col(size_col) < 5.0)
        .then(pl.col(adj_factor_micr_col))  # Microplot for small trees
        .when(pl.col(size_col) < macro_breakpoint_expr)
        .then(pl.col(adj_factor_subp_col))  # Subplot for medium trees
        .otherwise(pl.col(adj_factor_macr_col))  # Macroplot for large trees
    )


def apply_tree_adjustment_factors(
    data: Union[pl.DataFrame, pl.LazyFrame],
    size_col: str = "DIA",
    macro_breakpoint_col: str = "MACRO_BREAKPOINT_DIA",
    output_col: str = "ADJ_FACTOR",
) -> Union[pl.DataFrame, pl.LazyFrame]:
    """
    Apply tree adjustment factors to a dataframe.

    This function adds the appropriate adjustment factor column based on
    tree diameter and plot design. This is REQUIRED for accurate tree expansion.

    Parameters
    ----------
    data : Union[pl.DataFrame, pl.LazyFrame]
        Data containing tree diameters and adjustment factor columns
    size_col : str
        Column name for tree diameter (DBH in inches)
    macro_breakpoint_col : str
        Column name for macroplot breakpoint diameter
    output_col : str
        Name for the output adjustment factor column

    Returns
    -------
    Union[pl.DataFrame, pl.LazyFrame]
        Data with adjustment factor column added

    Raises
    ------
    ValueError
        If required columns are missing

    Example
    -------
    >>> # Apply adjustment factors to tree data
    >>> tree_data = apply_tree_adjustment_factors(
    ...     tree_data,
    ...     size_col="DIA",
    ...     macro_breakpoint_col="MACRO_BREAKPOINT_DIA"
    ... )
    >>> # Now calculate expanded values
    >>> tree_data = tree_data.with_columns(
    ...     expanded_tpa=(pl.col("TPA_UNADJ") * pl.col("ADJ_FACTOR") * pl.col("EXPNS"))
    ... )
    """
    # Validate required columns
    required_cols = [size_col, "ADJ_FACTOR_MICR", "ADJ_FACTOR_SUBP", "ADJ_FACTOR_MACR"]

    # Check for missing columns using lazy frame schema
    if isinstance(data, pl.LazyFrame):
        available_cols = data.collect_schema().names()
    else:
        available_cols = data.columns
    missing_cols = [col for col in required_cols if col not in available_cols]
    if missing_cols:
        raise ValueError(
            f"Missing required columns for tree adjustment: {missing_cols}\n"
            f"These columns must be present from joining TREE, POP_STRATUM, and PLOT tables."
        )

    # Warn if MACRO_BREAKPOINT_DIA is missing
    if macro_breakpoint_col not in available_cols:
        import warnings

        warnings.warn(
            f"Column '{macro_breakpoint_col}' not found. "
            "Large tree estimates may be incorrect. "
            "Join with PLOT table to include macroplot breakpoint diameter.",
            UserWarning,
        )
        # Add a column with default value
        data = data.with_columns(pl.lit(9999).alias(macro_breakpoint_col))

    # Apply adjustment factor logic
    adj_expr = get_adjustment_factor_expr(
        size_col=size_col, macro_breakpoint_col=macro_breakpoint_col
    ).alias(output_col)

    return data.with_columns(adj_expr)


def get_tree_adjustment_sql(
    tree_alias: str = "TREE",
    plot_alias: str = "PLOT",
    stratum_alias: str = "POP_STRATUM",
) -> str:
    """
    Get SQL CASE expression for tree adjustment factor selection.

    This returns the EXACT SQL logic needed for proper tree expansion
    in direct SQL queries. This must be used for ALL tree-based SQL queries.

    Parameters
    ----------
    tree_alias : str
        Table alias for TREE table
    plot_alias : str
        Table alias for PLOT table
    stratum_alias : str
        Table alias for POP_STRATUM table

    Returns
    -------
    str
        SQL CASE expression for adjustment factor

    Example
    -------
    >>> sql = f'''
    ... SELECT SUM(
    ...     TREE.TPA_UNADJ * {get_tree_adjustment_sql()} * POP_STRATUM.EXPNS
    ... ) as total_trees
    ... FROM POP_STRATUM
    ... JOIN POP_PLOT_STRATUM_ASSGN ON ...
    ... JOIN PLOT ON ...
    ... JOIN TREE ON ...
    ... WHERE TREE.STATUSCD = 1
    ... '''

    Notes
    -----
    The SQL query must include these tables/columns:
    - TREE.DIA: Tree diameter
    - PLOT.MACRO_BREAKPOINT_DIA: Macro breakpoint (can be NULL)
    - POP_STRATUM.ADJ_FACTOR_MICR: Microplot adjustment
    - POP_STRATUM.ADJ_FACTOR_SUBP: Subplot adjustment
    - POP_STRATUM.ADJ_FACTOR_MACR: Macroplot adjustment
    """
    return f"""
    CASE
        WHEN {tree_alias}.DIA IS NULL THEN {stratum_alias}.ADJ_FACTOR_SUBP
        ELSE
            CASE
                WHEN {tree_alias}.DIA < 5.0 THEN {stratum_alias}.ADJ_FACTOR_MICR
                WHEN {tree_alias}.DIA < COALESCE({plot_alias}.MACRO_BREAKPOINT_DIA, 9999)
                    THEN {stratum_alias}.ADJ_FACTOR_SUBP
                ELSE {stratum_alias}.ADJ_FACTOR_MACR
            END
    END
    """


def calculate_expanded_trees(
    tpa_unadj: float,
    dia: Optional[float],
    adj_factor_micr: float,
    adj_factor_subp: float,
    adj_factor_macr: float,
    expns: float,
    macro_breakpoint: Optional[float] = None,
) -> float:
    """
    Calculate expanded tree count for a single tree record.

    This function demonstrates the complete expansion calculation
    from a single tree observation to population estimate.

    Parameters
    ----------
    tpa_unadj : float
        Unadjusted trees per acre (from TREE.TPA_UNADJ)
    dia : Optional[float]
        Tree diameter at breast height in inches
    adj_factor_micr : float
        Microplot adjustment factor (from POP_STRATUM.ADJ_FACTOR_MICR)
    adj_factor_subp : float
        Subplot adjustment factor (from POP_STRATUM.ADJ_FACTOR_SUBP)
    adj_factor_macr : float
        Macroplot adjustment factor (from POP_STRATUM.ADJ_FACTOR_MACR)
    expns : float
        Expansion factor to total acres (from POP_STRATUM.EXPNS)
    macro_breakpoint : Optional[float]
        Macroplot breakpoint diameter (from PLOT.MACRO_BREAKPOINT_DIA)

    Returns
    -------
    float
        Expanded tree count (number of trees this sample represents)

    Example
    -------
    >>> # A 6-inch tree on a plot
    >>> expanded = calculate_expanded_trees(
    ...     tpa_unadj=6.018046,  # Base trees per acre
    ...     dia=6.0,              # 6 inch diameter
    ...     adj_factor_micr=1.0,  # Not used (tree > 5")
    ...     adj_factor_subp=1.0,  # Used for this tree
    ...     adj_factor_macr=0.25, # Not used (tree < breakpoint)
    ...     expns=1234.5,         # Expansion to total acres
    ...     macro_breakpoint=20.0 # Large trees are 20"+ in this region
    ... )
    >>> print(f"This tree represents {expanded:,.0f} trees in the population")
    """
    # Determine adjustment factor based on diameter
    if dia is None:
        adj_factor = adj_factor_subp
    elif dia < 5.0:
        adj_factor = adj_factor_micr
    elif macro_breakpoint is None or dia < macro_breakpoint:
        adj_factor = adj_factor_subp
    else:
        adj_factor = adj_factor_macr

    # Calculate expanded tree count
    return tpa_unadj * adj_factor * expns


def get_area_adjustment_factor_expr(
    prop_basis_col: str = "PROP_BASIS",
    adj_factor_subp_col: str = "ADJ_FACTOR_SUBP",
    adj_factor_macr_col: str = "ADJ_FACTOR_MACR",
) -> pl.Expr:
    """
    Get Polars expression for area adjustment factor selection.

    Area expansion uses PROP_BASIS from COND table to determine adjustment factor.
    This is different from tree expansion which uses diameter classes.

    Parameters
    ----------
    prop_basis_col : str
        Column name for condition proportion basis
    adj_factor_subp_col : str
        Column name for subplot adjustment factor
    adj_factor_macr_col : str
        Column name for macroplot adjustment factor

    Returns
    -------
    pl.Expr
        Polars expression that selects the appropriate adjustment factor

    Notes
    -----
    The logic is:
    1. PROP_BASIS = 'MACR' → macroplot factor
    2. Otherwise → subplot factor (default)

    This matches the FIA area estimation methodology where conditions are
    classified as either macroplot-based or subplot-based.
    """
    return (
        pl.when(pl.col(prop_basis_col) == "MACR")
        .then(pl.col(adj_factor_macr_col))  # Macroplot conditions
        .otherwise(pl.col(adj_factor_subp_col))  # Subplot conditions (default)
    )


def apply_area_adjustment_factors(
    data: Union[pl.DataFrame, pl.LazyFrame],
    prop_basis_col: str = "PROP_BASIS",
    output_col: str = "ADJ_FACTOR_AREA",
) -> Union[pl.DataFrame, pl.LazyFrame]:
    """
    Apply area adjustment factors to a dataframe.

    This function adds the appropriate adjustment factor column based on
    condition proportion basis. This is REQUIRED for accurate area expansion.

    Parameters
    ----------
    data : Union[pl.DataFrame, pl.LazyFrame]
        Data containing condition data and adjustment factor columns
    prop_basis_col : str
        Column name for condition proportion basis
    output_col : str
        Name for the output adjustment factor column

    Returns
    -------
    Union[pl.DataFrame, pl.LazyFrame]
        Data with area adjustment factor column added

    Raises
    ------
    ValueError
        If required columns are missing

    Example
    -------
    >>> # Apply adjustment factors to condition data
    >>> cond_data = apply_area_adjustment_factors(
    ...     cond_data,
    ...     prop_basis_col="PROP_BASIS"
    ... )
    >>> # Calculate expanded area
    >>> cond_data = cond_data.with_columns([
    ...     (pl.col("CONDPROP_UNADJ") * pl.col("ADJ_FACTOR_AREA") * pl.col("EXPNS"))
    ...     .alias("area_expanded")
    ... ])
    """
    # Validate required columns
    required_cols = [prop_basis_col, "ADJ_FACTOR_SUBP", "ADJ_FACTOR_MACR"]

    # Check for missing columns using lazy frame schema
    if isinstance(data, pl.LazyFrame):
        available_cols = data.collect_schema().names()
    else:
        available_cols = data.columns
    missing_cols = [col for col in required_cols if col not in available_cols]
    if missing_cols:
        raise ValueError(
            f"Missing required columns for area adjustment: {missing_cols}\\n"
            f"These columns must be present from joining COND, POP_STRATUM tables."
        )

    # Apply adjustment factor logic
    adj_expr = get_area_adjustment_factor_expr(prop_basis_col=prop_basis_col).alias(
        output_col
    )

    return data.with_columns(adj_expr)


def get_area_adjustment_sql(
    cond_alias: str = "COND", stratum_alias: str = "POP_STRATUM"
) -> str:
    """
    Get SQL CASE expression for area adjustment factor selection.

    This returns the EXACT SQL logic needed for proper area expansion
    in direct SQL queries. This must be used for ALL area-based SQL queries.

    Parameters
    ----------
    cond_alias : str
        Table alias for COND table
    stratum_alias : str
        Table alias for POP_STRATUM table

    Returns
    -------
    str
        SQL CASE expression for area adjustment factor

    Example
    -------
    >>> sql = f'''
    ... SELECT SUM(
    ...     COND.CONDPROP_UNADJ * {get_area_adjustment_sql()} * POP_STRATUM.EXPNS
    ... ) as total_area
    ... FROM POP_STRATUM
    ... JOIN POP_PLOT_STRATUM_ASSGN ON ...
    ... JOIN PLOT ON ...
    ... JOIN COND ON ...
    ... WHERE COND.COND_STATUS_CD = 1
    ... '''

    Notes
    -----
    The SQL query must include these tables/columns:
    - COND.PROP_BASIS: Condition proportion basis
    - POP_STRATUM.ADJ_FACTOR_SUBP: Subplot adjustment
    - POP_STRATUM.ADJ_FACTOR_MACR: Macroplot adjustment
    """
    return f"""
    CASE {cond_alias}.PROP_BASIS
        WHEN 'MACR' THEN {stratum_alias}.ADJ_FACTOR_MACR
        ELSE {stratum_alias}.ADJ_FACTOR_SUBP
    END
    """


def calculate_expanded_area(
    condprop_unadj: float,
    prop_basis: str,
    adj_factor_subp: float,
    adj_factor_macr: float,
    expns: float,
) -> float:
    """
    Calculate expanded area for a single condition record.

    This function demonstrates the complete area expansion calculation
    from a single condition observation to population estimate.

    Parameters
    ----------
    condprop_unadj : float
        Unadjusted condition proportion (from COND.CONDPROP_UNADJ)
    prop_basis : str
        Proportion basis ('MACR' or other, from COND.PROP_BASIS)
    adj_factor_subp : float
        Subplot adjustment factor (from POP_STRATUM.ADJ_FACTOR_SUBP)
    adj_factor_macr : float
        Macroplot adjustment factor (from POP_STRATUM.ADJ_FACTOR_MACR)
    expns : float
        Expansion factor to total acres (from POP_STRATUM.EXPNS)

    Returns
    -------
    float
        Expanded area (acres this condition represents)

    Example
    -------
    >>> # A macroplot-based forestland condition
    >>> expanded = calculate_expanded_area(
    ...     condprop_unadj=1.0,      # Full plot is this condition
    ...     prop_basis="MACR",       # Macroplot-based
    ...     adj_factor_subp=1.0,     # Not used
    ...     adj_factor_macr=0.25,    # Used for this condition
    ...     expns=1234.5             # Expansion to total acres
    ... )
    >>> print(f"This condition represents {expanded:,.0f} acres")
    """
    # Determine adjustment factor based on proportion basis
    if prop_basis == "MACR":
        adj_factor = adj_factor_macr
    else:
        adj_factor = adj_factor_subp

    # Calculate expanded area
    return condprop_unadj * adj_factor * expns


def validate_expansion_inputs(df: pl.DataFrame) -> dict:
    """
    Validate and report on tree expansion input data.

    This function checks if all required columns are present and
    provides statistics on the adjustment factors and tree sizes.

    Parameters
    ----------
    df : pl.DataFrame
        DataFrame to validate

    Returns
    -------
    dict
        Validation report with statistics

    Example
    -------
    >>> report = validate_expansion_inputs(tree_data)
    >>> if report['is_valid']:
    >>>     print("Data is ready for tree expansion")
    >>> else:
    >>>     print(f"Missing columns: {report['missing_columns']}")
    """
    required_cols = [
        "TPA_UNADJ",
        "DIA",
        "ADJ_FACTOR_MICR",
        "ADJ_FACTOR_SUBP",
        "ADJ_FACTOR_MACR",
        "EXPNS",
    ]

    missing = [col for col in required_cols if col not in df.columns]
    has_macro = "MACRO_BREAKPOINT_DIA" in df.columns

    report = {
        "is_valid": len(missing) == 0,
        "missing_columns": missing,
        "has_macro_breakpoint": has_macro,
        "n_rows": len(df),
    }

    if report["is_valid"] and "DIA" in df.columns:
        # Add diameter statistics
        report["diameter_stats"] = {
            "min": df["DIA"].min(),
            "max": df["DIA"].max(),
            "mean": df["DIA"].mean(),
            "n_microplot": len(df.filter(pl.col("DIA") < 5.0)),
            "n_subplot": len(
                df.filter((pl.col("DIA") >= 5.0) & (pl.col("DIA") < 20.0))
            ),
            "n_null": df["DIA"].null_count(),
        }

        if has_macro:
            report["macro_breakpoint_stats"] = {
                "min": df["MACRO_BREAKPOINT_DIA"].min(),
                "max": df["MACRO_BREAKPOINT_DIA"].max(),
                "n_null": df["MACRO_BREAKPOINT_DIA"].null_count(),
                "n_no_macro": len(df.filter(pl.col("MACRO_BREAKPOINT_DIA") >= 9999)),
            }

    return report


# Standard query templates for reference
TREE_COUNT_QUERY_TEMPLATE = """
-- Standard FIA tree count query with proper expansion
SELECT
    SUM(
        TREE.TPA_UNADJ *
        {tree_adjustment_factor} *
        POP_STRATUM.EXPNS
    ) as total_trees,
    COUNT(*) as sample_trees,
    COUNT(DISTINCT PLOT.CN) as sample_plots
FROM POP_STRATUM
JOIN POP_PLOT_STRATUM_ASSGN ON (POP_PLOT_STRATUM_ASSGN.STRATUM_CN = POP_STRATUM.CN)
JOIN PLOT ON (POP_PLOT_STRATUM_ASSGN.PLT_CN = PLOT.CN)
JOIN COND ON (COND.PLT_CN = PLOT.CN)
JOIN TREE ON (TREE.PLT_CN = COND.PLT_CN AND TREE.CONDID = COND.CONDID)
WHERE
    TREE.STATUSCD = 1  -- Live trees
    AND COND.COND_STATUS_CD = 1  -- Forestland
    AND POP_STRATUM.EVALID = ?  -- Specific evaluation
"""

AREA_QUERY_TEMPLATE = """
-- Standard FIA area query with proper expansion
SELECT
    SUM(
        COND.CONDPROP_UNADJ *
        {area_adjustment_factor} *
        POP_STRATUM.EXPNS
    ) as total_area,
    COUNT(*) as sample_conditions,
    COUNT(DISTINCT PLOT.CN) as sample_plots
FROM POP_STRATUM
JOIN POP_PLOT_STRATUM_ASSGN ON (POP_PLOT_STRATUM_ASSGN.STRATUM_CN = POP_STRATUM.CN)
JOIN PLOT ON (POP_PLOT_STRATUM_ASSGN.PLT_CN = PLOT.CN)
JOIN COND ON (COND.PLT_CN = PLOT.CN)
WHERE
    COND.COND_STATUS_CD = 1  -- Forestland
    AND POP_STRATUM.EVALID = ?  -- Specific evaluation
"""


if __name__ == "__main__":
    # Example usage and documentation
    print("FIA Tree Expansion Module")
    print("=" * 60)
    print("\nThis module provides the CRITICAL logic for tree expansion")
    print("based on the FIA nested plot design.\n")

    print("Key Functions:")
    print("-" * 40)
    print("1. get_adjustment_factor_expr() - Polars expression for adjustment factor")
    print("2. apply_tree_adjustment_factors() - Apply factors to dataframe")
    print("3. get_tree_adjustment_sql() - SQL CASE expression for queries")
    print("4. calculate_expanded_trees() - Example single tree calculation")
    print("5. validate_expansion_inputs() - Check data readiness")

    print("\nExample SQL with proper expansion:")
    print("-" * 40)
    print("TREE EXPANSION:")
    tree_sql = TREE_COUNT_QUERY_TEMPLATE.replace(
        "{tree_adjustment_factor}", get_tree_adjustment_sql()
    )
    print(tree_sql)

    print("\nAREA EXPANSION:")
    area_sql = AREA_QUERY_TEMPLATE.replace(
        "{area_adjustment_factor}", get_area_adjustment_sql()
    )
    print(area_sql)

    print("\nCRITICAL Requirements:")
    print("-" * 40)
    print("✓ Always join PLOT table for MACRO_BREAKPOINT_DIA")
    print("✓ Always join POP_STRATUM for adjustment factors")
    print("✓ Use this module's functions for ALL tree expansions")
    print("✓ Handle NULL MACRO_BREAKPOINT_DIA as 9999")

    print("\nNested Plot Design:")
    print("-" * 40)
    print('• Microplot (6.8 ft): Trees 1.0" - 4.9" DBH')
    print('• Subplot (24.0 ft): Trees ≥ 5.0" DBH')
    print("• Macroplot (58.9 ft): Large trees above regional breakpoint")
