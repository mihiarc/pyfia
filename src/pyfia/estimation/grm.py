"""
Shared GRM (Growth-Removal-Mortality) infrastructure.

This module contains common functionality used by GrowthEstimator,
MortalityEstimator, and RemovalsEstimator for working with FIA's
TREE_GRM_COMPONENT, TREE_GRM_MIDPT, and TREE_GRM_BEGIN tables.
"""

from dataclasses import dataclass
from typing import List, Literal, Optional

import polars as pl

# Re-export shared variance function for backward compatibility
from .variance import calculate_ratio_variance  # noqa: F401

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
        Column name for component type (e.g., SUBP_COMPONENT_GS_FOREST)
    tpa : str
        Column name for TPA value (e.g., SUBP_TPAGROW_UNADJ_GS_FOREST)
    subptyp : str
        Column name for subplot type (e.g., SUBP_SUBPTYP_GRM_GS_FOREST)
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

    return GRMColumns(
        component=f"SUBP_COMPONENT_{tree_code}_{land_code}",
        tpa=f"SUBP_{tpa_prefix}_UNADJ_{tree_code}_{land_code}",
        subptyp=f"SUBP_SUBPTYP_GRM_{tree_code}_{land_code}",
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
        except Exception as e:
            raise ValueError(f"TREE_GRM_COMPONENT table not found: {e}")

    grm_component = db.tables["TREE_GRM_COMPONENT"]
    if not isinstance(grm_component, pl.LazyFrame):
        grm_component = grm_component.lazy()

    # Build column selection
    cols = [
        pl.col("TRE_CN"),
        pl.col("PLT_CN"),
        pl.col("DIA_BEGIN"),
        pl.col("DIA_MIDPT"),
        pl.col(grm_columns.component).alias("COMPONENT"),
        pl.col(grm_columns.tpa).alias("TPA_UNADJ"),
        pl.col(grm_columns.subptyp).alias("SUBPTYP_GRM"),
    ]

    if include_dia_end:
        cols.insert(4, pl.col("DIA_END"))

    result: pl.LazyFrame = grm_component.select(cols)
    return result


def load_grm_midpt(
    db,
    measure: str = "volume",
    include_additional_cols: Optional[List[str]] = None,
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
        except Exception as e:
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
        except Exception as e:
            raise ValueError(f"TREE_GRM_BEGIN table not found: {e}")

    grm_begin = db.tables["TREE_GRM_BEGIN"]
    if not isinstance(grm_begin, pl.LazyFrame):
        grm_begin = grm_begin.lazy()

    cols = ["TRE_CN"]

    if measure == "volume":
        cols.append("VOLCFNET")
    elif measure == "biomass":
        cols.append("DRYBIO_AG")

    result: pl.LazyFrame = grm_begin.select(cols)
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


def aggregate_cond_to_plot(cond: pl.LazyFrame) -> pl.LazyFrame:
    """Aggregate COND table to plot level for GRM estimation.

    GRM tables don't have CONDID, so we need plot-level condition
    aggregates for filtering and grouping.

    Parameters
    ----------
    cond : pl.LazyFrame
        Condition table data

    Returns
    -------
    pl.LazyFrame
        Plot-level condition aggregates with columns:
        - PLT_CN
        - COND_STATUS_CD (from first/dominant condition)
        - CONDPROP_UNADJ (sum of all condition proportions)
        - OWNGRPCD, FORTYPCD, SITECLCD, RESERVCD (from first condition)
        - CONDID (dummy value of 1)
    """
    return cond.group_by("PLT_CN").agg(
        [
            pl.col("COND_STATUS_CD").first().alias("COND_STATUS_CD"),
            pl.col("CONDPROP_UNADJ").sum().alias("CONDPROP_UNADJ"),
            pl.col("OWNGRPCD").first().alias("OWNGRPCD"),
            pl.col("FORTYPCD").first().alias("FORTYPCD"),
            pl.col("SITECLCD").first().alias("SITECLCD"),
            pl.col("RESERVCD").first().alias("RESERVCD"),
            pl.lit(1).alias("CONDID"),
        ]
    )


def filter_by_evalid(
    data: pl.LazyFrame,
    db,
    evalid: Optional[int | List[int]] = None,
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
) -> List[str]:
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
        # Growth also needs BEGIN table and BEGINEND for cross-join
        base_tables.extend(["TREE_GRM_BEGIN", "TREE", "BEGINEND"])

    return base_tables
