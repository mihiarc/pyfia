"""
Utility functions for FIA estimation.

Simple utilities for common operations, including shared validation and
initialization patterns used across all estimator functions.
"""

from __future__ import annotations

import warnings
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import polars as pl

from ..validation import (
    validate_boolean,
    validate_domain_expression,
    validate_grp_by,
    validate_land_type,
)

if TYPE_CHECKING:
    from ..core import FIA


@dataclass
class ValidatedInputs:
    """Container for validated estimator inputs.

    This dataclass holds the validated and normalized versions of common
    estimator function parameters. Using a dataclass instead of a tuple
    provides named access and better type safety.

    Attributes
    ----------
    land_type : str
        Validated land type ('forest', 'timber', or 'all')
    grp_by : str | list[str] | None
        Validated grouping column(s)
    area_domain : str | None
        Validated area domain expression
    plot_domain : str | None
        Validated plot domain expression
    tree_domain : str | None
        Validated tree domain expression (for tree-based estimators)
    variance : bool
        Whether to calculate variance
    totals : bool
        Whether to include population totals
    most_recent : bool
        Whether to use most recent evaluation
    """

    land_type: str
    grp_by: str | list[str] | None
    area_domain: str | None
    plot_domain: str | None
    tree_domain: str | None
    variance: bool
    totals: bool
    most_recent: bool


def validate_estimator_inputs(
    land_type: str = "forest",
    grp_by: str | list[str] | None = None,
    area_domain: str | None = None,
    plot_domain: str | None = None,
    tree_domain: str | None = None,
    variance: bool = False,
    totals: bool = True,
    most_recent: bool = False,
) -> ValidatedInputs:
    """
    Validate common estimator function inputs.

    This function consolidates the validation logic that was previously
    duplicated across all estimator functions (area, volume, biomass, tpa, etc.).

    Parameters
    ----------
    land_type : str, default 'forest'
        Land type to estimate for ('forest', 'timber', or 'all')
    grp_by : str or list of str, optional
        Column name(s) to group results by
    area_domain : str, optional
        SQL-like filter for condition-level attributes
    plot_domain : str, optional
        SQL-like filter for plot-level attributes
    tree_domain : str, optional
        SQL-like filter for tree-level attributes (tree-based estimators only)
    variance : bool, default False
        Whether to calculate variance instead of standard error
    totals : bool, default True
        Whether to include population-level totals
    most_recent : bool, default False
        Whether to automatically select most recent evaluation

    Returns
    -------
    ValidatedInputs
        Dataclass containing all validated inputs

    Raises
    ------
    ValueError
        If any parameter value is invalid
    TypeError
        If any parameter has wrong type

    Examples
    --------
    >>> inputs = validate_estimator_inputs(
    ...     land_type="forest",
    ...     grp_by="OWNGRPCD",
    ...     area_domain="STDAGE > 50",
    ...     variance=True
    ... )
    >>> inputs.land_type
    'forest'
    >>> inputs.variance
    True
    """
    return ValidatedInputs(
        land_type=validate_land_type(land_type),
        grp_by=validate_grp_by(grp_by),
        area_domain=validate_domain_expression(area_domain, "area_domain"),
        plot_domain=validate_domain_expression(plot_domain, "plot_domain"),
        tree_domain=validate_domain_expression(tree_domain, "tree_domain"),
        variance=validate_boolean(variance, "variance"),
        totals=validate_boolean(totals, "totals"),
        most_recent=validate_boolean(most_recent, "most_recent"),
    )


def ensure_fia_instance(db: str | "FIA") -> tuple["FIA", bool]:
    """
    Ensure db is a FIA instance, creating one if a path string is provided.

    This function handles the common pattern of accepting either a path string
    or an existing FIA connection, and tracks ownership for proper cleanup.

    Parameters
    ----------
    db : str or FIA
        Either a path to a FIA database file, or an existing FIA connection

    Returns
    -------
    tuple[FIA, bool]
        A tuple of (FIA instance, owns_db) where owns_db is True if this
        function created the connection (caller should close it), or False
        if an existing connection was passed in (caller should NOT close it)

    Examples
    --------
    >>> db, owns_db = ensure_fia_instance("path/to/fia.duckdb")
    >>> try:
    ...     # use db
    ...     pass
    ... finally:
    ...     if owns_db:
    ...         db.close()

    >>> existing_db = FIA("path/to/fia.duckdb")
    >>> db, owns_db = ensure_fia_instance(existing_db)
    >>> owns_db
    False  # Caller passed in connection, so we don't own it
    """
    # Import here to avoid circular imports
    from ..core import FIA

    if isinstance(db, str):
        return FIA(db), True
    else:
        return db, False


def ensure_evalid_set(
    db: "FIA",
    eval_type: str = "ALL",
    estimator_name: str = "estimation",
) -> None:
    """
    Ensure EVALID is set on the database connection, auto-selecting if needed.

    This function implements the common pattern of checking if EVALID is set,
    and if not, automatically selecting the most recent evaluation with an
    appropriate warning. This prevents overcounting from multiple evaluations.

    Parameters
    ----------
    db : FIA
        FIA database connection to check/update
    eval_type : str, default 'ALL'
        Evaluation type to select if auto-selecting. Common values:
        - 'ALL': All plots (EXPALL evaluations) - use for area estimation
        - 'VOL': Volume evaluations (EXPVOL) - use for volume/biomass/tpa
        - 'GROW': Growth evaluations - use for growth estimation
        - 'MORT': Mortality evaluations - use for mortality estimation
    estimator_name : str, default 'estimation'
        Name of the estimator function for warning messages

    Warns
    -----
    UserWarning
        If no EVALID was set and one was automatically selected
    UserWarning
        If no evaluations of the requested type were found

    Examples
    --------
    >>> db = FIA("path/to/fia.duckdb")
    >>> db.clip_by_state(37)
    >>> ensure_evalid_set(db, eval_type="VOL", estimator_name="volume")
    UserWarning: No EVALID specified. Automatically selecting most recent EXPVOL evaluations...

    >>> # If EVALID already set, this is a no-op
    >>> db.clip_most_recent(eval_type="VOL")
    >>> ensure_evalid_set(db, eval_type="VOL")  # No warning
    """
    if db.evalid is not None:
        return

    # Auto-select most recent evaluation with warning
    warnings.warn(
        f"No EVALID specified. Automatically selecting most recent EXP{eval_type} evaluations. "
        f"For explicit control, use db.clip_most_recent() or db.clip_by_evalid() before calling {estimator_name}()."
    )
    db.clip_most_recent(eval_type=eval_type)

    # If still no EVALID, warn about potential issues
    if db.evalid is None:
        warnings.warn(
            f"WARNING: No EXP{eval_type} evaluations found. Results may be incorrect due to "
            "inclusion of multiple overlapping evaluations. Consider using db.clip_by_evalid() "
            "to explicitly select appropriate EVALIDs."
        )


def validate_aggregation_result(
    agg_result: Any,
    estimator_name: str,
) -> None:
    """
    Validate that input is an AggregationResult for variance calculation.

    This function consolidates the common validation pattern used in all
    estimator calculate_variance() methods.

    Parameters
    ----------
    agg_result : Any
        The input to validate (should be an AggregationResult)
    estimator_name : str
        Name of the estimator for error messages (e.g., 'Volume', 'TPA', 'Biomass')

    Raises
    ------
    ValueError
        If agg_result is not an AggregationResult instance

    Examples
    --------
    >>> from pyfia.estimation.base import AggregationResult
    >>> validate_aggregation_result(some_result, "Volume")  # Raises if not AggregationResult
    """
    # Import here to avoid circular imports
    from .base import AggregationResult

    if not isinstance(agg_result, AggregationResult):
        raise ValueError(
            f"{estimator_name} variance calculation requires AggregationResult. "
            "DataFrame input is no longer supported."
        )


def validate_required_columns(
    data: pl.DataFrame | pl.LazyFrame,
    required_cols: list[str],
    context: str = "data",
) -> None:
    """
    Validate that required columns exist in the DataFrame or LazyFrame.

    This function consolidates the common pattern of checking for required
    columns before processing data.

    Parameters
    ----------
    data : pl.DataFrame or pl.LazyFrame
        The data to validate
    required_cols : list of str
        Column names that must be present
    context : str, default 'data'
        Description of the data for error messages (e.g., 'tree data', 'plot data')

    Raises
    ------
    ValueError
        If any required columns are missing

    Examples
    --------
    >>> validate_required_columns(tree_df, ["PLT_CN", "DIA", "TPA_UNADJ"], "tree data")
    """
    if isinstance(data, pl.LazyFrame):
        schema = data.collect_schema()
        columns = schema.names()
    else:
        columns = data.columns

    missing_cols = [col for col in required_cols if col not in columns]
    if missing_cols:
        raise ValueError(f"Required columns missing from {context}: {missing_cols}")


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
    from ..filtering.utils import (
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
