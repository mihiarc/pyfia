"""
Shared GRM (Growth-Removal-Mortality) infrastructure.

This module contains common functionality used by GrowthEstimator,
MortalityEstimator, and RemovalsEstimator for working with FIA's
TREE_GRM_COMPONENT, TREE_GRM_MIDPT, and TREE_GRM_BEGIN tables.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import polars as pl

# Re-export shared variance function
from .variance import calculate_domain_total_variance  # noqa: F401

# Valid tree types for GRM estimation
TreeType = Literal["gs", "al", "sl", "live", "sawtimber"]
# Valid land types for GRM estimation
LandType = Literal["forest", "timber"]
# Valid GRM component types
ComponentType = Literal["growth", "mortality", "removals"]


@dataclass
class GRMColumns:
    """Container for resolved GRM column names.

    Attributes
    ----------
    component : str
        Column name for component type (e.g., SUBP_COMPONENT_GS_FOREST,
        MICR_COMPONENT_AL_FOREST)
    tpa : str
        Column name for TPA value (e.g., SUBP_TPAGROW_UNADJ_GS_FOREST)
    subptyp : str
        Column name for subplot type (e.g., SUBP_SUBPTYP_GRM_GS_FOREST),
        which selects the adjustment factor
    tree_type_code : str
        Normalized tree type code (GS, AL, SL)
    land_type_code : str
        Normalized land type code (FOREST, TIMBER)
    """

    component: str
    tpa: str
    subptyp: str
    tree_type_code: str
    land_type_code: str


def normalize_tree_type(tree_type: str) -> str:
    """Normalize tree type to FIA convention.

    Parameters
    ----------
    tree_type : str
        Input tree type (gs, al, sl, live, sawtimber)

    Returns
    -------
    str
        Normalized tree type code (GS, AL, SL)
    """
    tree_type = tree_type.upper()
    if tree_type in ("LIVE", "AL"):
        return "AL"
    elif tree_type == "SAWTIMBER":
        return "SL"
    elif tree_type == "SL":
        return "SL"
    else:
        return "GS"


def normalize_land_type(land_type: str) -> str:
    """Normalize land type to FIA convention.

    Parameters
    ----------
    land_type : str
        Input land type (forest, timber, all)

    Returns
    -------
    str
        Normalized land type code (FOREST, TIMBER)
    """
    land_type = land_type.upper()
    if land_type == "TIMBER":
        return "TIMBER"
    else:
        return "FOREST"


def resolve_grm_columns(
    component_type: ComponentType,
    tree_type: str = "gs",
    land_type: str = "forest",
) -> GRMColumns:
    """Resolve GRM column names based on tree type and land type.

    The GRM tables have columns named with suffixes indicating the tree
    population and land basis, e.g., SUBP_TPAGROW_UNADJ_GS_FOREST for
    growing stock on all forestland.

    All live trees use the ``MICR_`` columns, which cover every live tree at
    least 1 inch in diameter, saplings tallied on the microplot included
    (SUBPTYP_GRM 2). The ``SUBP_`` all-live columns cover trees at least
    5 inches only. Growing-stock and sawtimber trees use the ``SUBP_``
    columns. This is how EVALIDator selects them for its estimates of all
    live trees (at least 1 inch), growing-stock trees and sawtimber trees.

    Parameters
    ----------
    component_type : {'growth', 'mortality', 'removals'}
        Type of GRM component
    tree_type : str, default 'gs'
        Tree type: 'gs' (growing stock), 'al' (all live), 'sl' (sawtimber),
        'live' (alias for 'al'), 'sawtimber' (alias for 'sl')
    land_type : str, default 'forest'
        Land type: 'forest' (all forestland) or 'timber' (timberland only)

    Returns
    -------
    GRMColumns
        Resolved column names for the specified tree/land type combination

    Examples
    --------
    >>> cols = resolve_grm_columns('growth', tree_type='gs', land_type='forest')
    >>> cols.component
    'SUBP_COMPONENT_GS_FOREST'
    >>> cols.tpa
    'SUBP_TPAGROW_UNADJ_GS_FOREST'
    >>> resolve_grm_columns('mortality', tree_type='al').tpa
    'MICR_TPAMORT_UNADJ_AL_FOREST'
    """
    tree_code = normalize_tree_type(tree_type)
    land_code = normalize_land_type(land_type)

    # Map component type to TPA column prefix
    tpa_prefix_map = {
        "growth": "TPAGROW",
        "mortality": "TPAMORT",
        "removals": "TPAREMV",
    }
    tpa_prefix = tpa_prefix_map[component_type]
    design = "MICR" if tree_code == "AL" else "SUBP"

    return GRMColumns(
        component=f"{design}_COMPONENT_{tree_code}_{land_code}",
        tpa=f"{design}_{tpa_prefix}_UNADJ_{tree_code}_{land_code}",
        subptyp=f"{design}_SUBPTYP_GRM_{tree_code}_{land_code}",
        tree_type_code=tree_code,
        land_type_code=land_code,
    )


def load_grm_component(
    db,
    grm_columns: GRMColumns,
    include_dia_end: bool = False,
) -> pl.LazyFrame:
    """Load TREE_GRM_COMPONENT table with resolved column names.

    Parameters
    ----------
    db : FIA
        Database connection
    grm_columns : GRMColumns
        Resolved column names from resolve_grm_columns()
    include_dia_end : bool, default False
        Whether to include DIA_END column (needed for growth)

    Returns
    -------
    pl.LazyFrame
        GRM component data with standardized column names:
        - TRE_CN, PLT_CN: Key columns
        - DIA_BEGIN, DIA_MIDPT: Diameter columns
        - DIA_END: (optional) Ending diameter
        - COMPONENT: Component type (SURVIVOR, MORTALITY1, CUT, etc.)
        - TPA_UNADJ: Unadjusted TPA value (renamed from specific column)
        - SUBPTYP_GRM: Subplot type for adjustment factor selection
    """
    if "TREE_GRM_COMPONENT" not in db.tables:
        try:
            db.load_table("TREE_GRM_COMPONENT")
        except (KeyError, ValueError) as e:
            raise ValueError(f"TREE_GRM_COMPONENT table not found: {e}")

    grm_component = db.tables["TREE_GRM_COMPONENT"]
    if not isinstance(grm_component, pl.LazyFrame):
        grm_component = grm_component.lazy()

    # Build column selection. Numeric columns are cast to their declared types:
    # some state DuckDBs (built from CSV/older snapshots) store FIADB numeric
    # fields as VARCHAR, which breaks downstream division/comparison (#106).
    # Strict casts fail loudly naming the offending column rather than silently
    # nulling values, preserving statistical integrity.
    cols = [
        pl.col("TRE_CN"),
        pl.col("PLT_CN"),
        pl.col("DIA_BEGIN").cast(pl.Float64),
        pl.col("DIA_MIDPT").cast(pl.Float64),
        pl.col(grm_columns.component).alias("COMPONENT"),
        pl.col(grm_columns.tpa).cast(pl.Float64).alias("TPA_UNADJ"),
        pl.col(grm_columns.subptyp).cast(pl.Int64).alias("SUBPTYP_GRM"),
    ]

    if include_dia_end:
        cols.insert(4, pl.col("DIA_END").cast(pl.Float64))

    result: pl.LazyFrame = grm_component.select(cols)
    return result


def load_grm_midpt(
    db,
    measure: str = "volume",
    include_additional_cols: list[str] | None = None,
) -> pl.LazyFrame:
    """Load TREE_GRM_MIDPT table for volume/biomass data.

    Parameters
    ----------
    db : FIA
        Database connection
    measure : str, default 'volume'
        What to measure: 'volume', 'biomass', 'sawlog', 'count', 'tpa', 'basal_area'
    include_additional_cols : list of str, optional
        Additional columns to include from the table

    Returns
    -------
    pl.LazyFrame
        GRM midpoint data with appropriate columns for the measure type
    """
    if "TREE_GRM_MIDPT" not in db.tables:
        try:
            db.load_table("TREE_GRM_MIDPT")
        except (KeyError, ValueError) as e:
            raise ValueError(f"TREE_GRM_MIDPT table not found: {e}")

    grm_midpt = db.tables["TREE_GRM_MIDPT"]
    if not isinstance(grm_midpt, pl.LazyFrame):
        grm_midpt = grm_midpt.lazy()

    # Base columns always needed
    cols = ["TRE_CN", "DIA", "SPCD", "STATUSCD"]

    # Add measure-specific columns
    if measure == "volume":
        cols.append("VOLCFNET")
    elif measure == "sawlog":
        cols.append("VOLCSNET")
    elif measure == "biomass":
        cols.extend(["DRYBIO_BOLE", "DRYBIO_BRANCH", "DRYBIO_AG"])
    # count, tpa, basal_area don't need additional volume columns

    # Add any additional requested columns
    if include_additional_cols:
        cols.extend([c for c in include_additional_cols if c not in cols])

    result: pl.LazyFrame = grm_midpt.select(cols)

    # Cast numeric columns to their declared types in case the source DuckDB
    # stored them as VARCHAR (see #106). Strict casts surface bad data loudly.
    float_cols = {
        "DIA",
        "VOLCFNET",
        "VOLCSNET",
        "DRYBIO_BOLE",
        "DRYBIO_BRANCH",
        "DRYBIO_AG",
    }
    int_cols = {"SPCD", "STATUSCD"}
    casts = [pl.col(c).cast(pl.Float64) for c in cols if c in float_cols]
    casts += [pl.col(c).cast(pl.Int64) for c in cols if c in int_cols]
    if casts:
        result = result.with_columns(casts)

    return result


def load_grm_begin(
    db,
    measure: str = "volume",
) -> pl.LazyFrame:
    """Load TREE_GRM_BEGIN table for beginning measurements.

    Used primarily by growth estimation to calculate volume change.

    Parameters
    ----------
    db : FIA
        Database connection
    measure : str, default 'volume'
        What to measure: 'volume' or 'biomass'

    Returns
    -------
    pl.LazyFrame
        GRM begin data with TRE_CN and volume/biomass columns
    """
    if "TREE_GRM_BEGIN" not in db.tables:
        try:
            db.load_table("TREE_GRM_BEGIN")
        except (KeyError, ValueError) as e:
            raise ValueError(f"TREE_GRM_BEGIN table not found: {e}")

    grm_begin = db.tables["TREE_GRM_BEGIN"]
    if not isinstance(grm_begin, pl.LazyFrame):
        grm_begin = grm_begin.lazy()

    cols = ["TRE_CN"]
    value_col = None

    if measure == "volume":
        value_col = "VOLCFNET"
    elif measure == "biomass":
        value_col = "DRYBIO_AG"
    if value_col:
        cols.append(value_col)

    result: pl.LazyFrame = grm_begin.select(cols)

    # Cast the value column to numeric in case the source stored it as VARCHAR
    # (see #106).
    if value_col:
        result = result.with_columns(pl.col(value_col).cast(pl.Float64))

    return result


def apply_grm_adjustment(data: pl.LazyFrame) -> pl.LazyFrame:
    """Apply GRM-specific adjustment factors based on SUBPTYP_GRM.

    The SUBPTYP_GRM field indicates which adjustment factor to use:
    - 0: No adjustment (trees not sampled on this subplot type)
    - 1: Subplot adjustment (ADJ_FACTOR_SUBP)
    - 2: Microplot adjustment (ADJ_FACTOR_MICR)
    - 3: Macroplot adjustment (ADJ_FACTOR_MACR)

    This differs from standard tree adjustment which uses diameter
    breakpoints to determine subplot type.

    Parameters
    ----------
    data : pl.LazyFrame
        Data with SUBPTYP_GRM and ADJ_FACTOR_SUBP/MICR/MACR columns

    Returns
    -------
    pl.LazyFrame
        Data with ADJ_FACTOR column added

    Notes
    -----
    Requires columns: SUBPTYP_GRM, ADJ_FACTOR_SUBP, ADJ_FACTOR_MICR, ADJ_FACTOR_MACR
    """
    return data.with_columns(
        [
            pl.when(pl.col("SUBPTYP_GRM") == 0)
            .then(0.0)
            .when(pl.col("SUBPTYP_GRM") == 1)
            .then(pl.col("ADJ_FACTOR_SUBP"))
            .when(pl.col("SUBPTYP_GRM") == 2)
            .then(pl.col("ADJ_FACTOR_MICR"))
            .when(pl.col("SUBPTYP_GRM") == 3)
            .then(pl.col("ADJ_FACTOR_MACR"))
            .otherwise(0.0)
            .alias("ADJ_FACTOR")
        ]
    )


def attach_tree_conditions(
    data: pl.LazyFrame, tree: pl.LazyFrame, cond: pl.LazyFrame
) -> pl.LazyFrame:
    """Attach each GRM tree's own condition attributes to GRM rows.

    GRM tables carry PLT_CN but not CONDID. A tree's condition comes from its
    time-2 TREE record (``TREE_GRM_COMPONENT.TRE_CN = TREE.CN``, then
    ``TREE.CONDID``), and its COND attributes join on ``(PLT_CN, CONDID)``.
    EVALIDator groups GRM estimates by this condition.

    ``CONDPROP_UNADJ`` and ``CONDID`` stay at plot level (the plot's summed
    condition proportion and a constant 1), because the GRM two-stage
    aggregation and variance use them as each plot's area and unit keys.

    Parameters
    ----------
    data : pl.LazyFrame
        GRM rows with TRE_CN and PLT_CN.
    tree : pl.LazyFrame
        TREE table with CN and CONDID.
    cond : pl.LazyFrame
        COND table with PLT_CN, CONDID, CONDPROP_UNADJ and any attribute
        columns needed for grouping or domains.

    Returns
    -------
    pl.LazyFrame
        ``data`` with every COND attribute of the tree's own condition, plus
        plot-level CONDPROP_UNADJ and CONDID = 1.
    """
    plot_area = (
        cond.group_by("PLT_CN")
        .agg(pl.col("CONDPROP_UNADJ").sum())
        .with_columns(pl.lit(1).alias("CONDID"))
    )
    own_condid = tree.select(
        pl.col("CN").alias("TRE_CN"), pl.col("CONDID").alias("_TREE_CONDID")
    )
    return (
        data.join(own_condid, on="TRE_CN", how="left")
        .join(
            cond.drop("CONDPROP_UNADJ"),
            left_on=["PLT_CN", "_TREE_CONDID"],
            right_on=["PLT_CN", "CONDID"],
            how="left",
        )
        .drop("_TREE_CONDID")
        .join(plot_area, on="PLT_CN", how="left")
    )


def filter_by_evalid(
    data: pl.LazyFrame,
    db,
    evalid: int | list[int] | None = None,
) -> pl.LazyFrame:
    """Filter GRM data to plots in the specified EVALID(s).

    Parameters
    ----------
    data : pl.LazyFrame
        GRM data with PLT_CN column
    db : FIA
        Database connection
    evalid : int or list of int, optional
        EVALID(s) to filter to. If None, uses db.evalid if set.

    Returns
    -------
    pl.LazyFrame
        Filtered data containing only plots in the specified evaluation(s)
    """
    # Determine which EVALID to use
    if evalid is None:
        if hasattr(db, "evalid") and db.evalid:
            evalid = db.evalid
        else:
            return data  # No filtering needed

    # Load POP_PLOT_STRATUM_ASSGN
    if "POP_PLOT_STRATUM_ASSGN" not in db.tables:
        db.load_table("POP_PLOT_STRATUM_ASSGN")

    ppsa = db.tables["POP_PLOT_STRATUM_ASSGN"]
    if not isinstance(ppsa, pl.LazyFrame):
        ppsa = ppsa.lazy()

    # Filter to get PLT_CNs for the specified EVALID(s)
    if isinstance(evalid, list):
        valid_plots = ppsa.filter(pl.col("EVALID").is_in(evalid))
    else:
        valid_plots = ppsa.filter(pl.col("EVALID") == evalid)

    valid_plots = valid_plots.select("PLT_CN").unique()

    return data.join(valid_plots, on="PLT_CN", how="inner")


def get_grm_required_tables(
    component_type: ComponentType,
) -> list[str]:
    """Get list of required tables for GRM estimation.

    Parameters
    ----------
    component_type : {'growth', 'mortality', 'removals'}
        Type of GRM component

    Returns
    -------
    list of str
        Required table names
    """
    base_tables = [
        "TREE_GRM_COMPONENT",
        "TREE_GRM_MIDPT",
        "COND",
        "PLOT",
        "POP_PLOT_STRATUM_ASSGN",
        "POP_STRATUM",
    ]

    if component_type == "growth":
        # Growth also needs BEGIN table and TREE
        # Note: BEGINEND is created dynamically (not available from DataMart)
        base_tables.extend(["TREE_GRM_BEGIN", "TREE"])

    return base_tables
