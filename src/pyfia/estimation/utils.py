"""
Utility functions for FIA estimation.

Simple utilities for common operations.
"""

import polars as pl


def _enhance_grouping_columns(df: pl.DataFrame) -> pl.DataFrame:
    """
    Add descriptive names for common FIA grouping columns.

    Automatically detects grouping columns like FORTYPCD and OWNGRPCD
    and adds human-readable name columns alongside them.

    Parameters
    ----------
    df : pl.DataFrame
        Results dataframe that may contain grouping columns

    Returns
    -------
    pl.DataFrame
        Dataframe with descriptive name columns added
    """
    from ..filtering.utils.grouping_functions import (
        add_forest_type_group,
        add_ownership_group_name,
    )

    # Enhance FORTYPCD if present and not already enhanced
    if "FORTYPCD" in df.columns and "FOREST_TYPE_GROUP" not in df.columns:
        df = add_forest_type_group(df)

    # Enhance OWNGRPCD if present and not already enhanced
    if "OWNGRPCD" in df.columns and "OWNERSHIP_GROUP" not in df.columns:
        df = add_ownership_group_name(df)

    return df


def format_output_columns(
    df: pl.DataFrame,
    estimation_type: str,
    include_se: bool = True,
    include_cv: bool = False,
) -> pl.DataFrame:
    """
    Format output columns to standard structure.

    Parameters
    ----------
    df : pl.DataFrame
        Results dataframe
    estimation_type : str
        Type of estimation (for column naming)
    include_se : bool
        Include standard error columns
    include_cv : bool
        Include coefficient of variation

    Returns
    -------
    pl.DataFrame
        Formatted dataframe
    """
    # Standard column mappings by estimation type
    column_maps = {
        "volume": {
            "VOLUME_ACRE": "VOL_ACRE",
            "VOLUME_TOTAL": "VOL_TOTAL",
        },
        "biomass": {
            "BIOMASS_ACRE": "BIO_ACRE",
            "BIOMASS_TOTAL": "BIO_TOTAL",
            "CARBON_ACRE": "CARB_ACRE",
        },
        "tpa": {
            "TPA": "TPA",
            "BAA": "BAA",
        },
        "area": {
            "AREA_TOTAL": "AREA",
            "AREA_PERCENT": "AREA_PERC",
        },
        "mortality": {
            "MORTALITY_ACRE": "MORT_ACRE",
            "MORTALITY_TOTAL": "MORT_TOTAL",
        },
        "growth": {
            "GROWTH_ACRE": "GROWTH_ACRE",
            "GROWTH_TOTAL": "GROWTH_TOTAL",
        },
    }

    # Apply column mappings if available
    if estimation_type in column_maps:
        rename_dict = {}
        for old_name, new_name in column_maps[estimation_type].items():
            if old_name in df.columns:
                rename_dict[old_name] = new_name

        if rename_dict:
            df = df.rename(rename_dict)

    # Add CV if requested
    if include_cv:
        # Find estimate and SE columns
        est_cols = [
            col for col in df.columns if col.endswith("_ACRE") or col.endswith("_TOTAL")
        ]
        se_cols = [col for col in df.columns if col.endswith("_SE")]

        for est_col in est_cols:
            se_col = f"{est_col}_SE"
            if se_col in se_cols:
                cv_col = f"{est_col}_CV"
                df = df.with_columns(
                    [
                        (100 * pl.col(se_col) / pl.col(est_col).abs())
                        .fill_null(0)
                        .alias(cv_col)
                    ]
                )

    # Enhance grouping columns with descriptive names
    df = _enhance_grouping_columns(df)

    # Order columns consistently
    priority_cols = ["YEAR", "EVALID", "STATECD", "PLOT", "SPCD"]
    estimate_cols = [
        col for col in df.columns if col.endswith(("_ACRE", "_TOTAL", "_PCT"))
    ]
    se_cols = [col for col in df.columns if col.endswith("_SE")]
    cv_cols = [col for col in df.columns if col.endswith("_CV")]
    meta_cols = ["N_PLOTS", "N_TREES", "AREA"]

    # Build ordered column list
    ordered = []
    for col in priority_cols:
        if col in df.columns:
            ordered.append(col)

    for col in estimate_cols:
        if col not in ordered:
            ordered.append(col)

    for col in se_cols:
        if col not in ordered:
            ordered.append(col)

    for col in cv_cols:
        if col not in ordered:
            ordered.append(col)

    for col in meta_cols:
        if col in df.columns and col not in ordered:
            ordered.append(col)

    # Add any remaining columns
    for col in df.columns:
        if col not in ordered:
            ordered.append(col)

    return df.select(ordered)
