"""
Remeasurement intervals: FIA conditions and trees paired across two
measurements.

``condition_intervals`` pairs each condition at the earlier measurement
(time 1) with the conditions it became at the later one (time 2);
``tree_intervals`` follows each tree through the interval with its GRM
component. These are unit-level building blocks for analyses of land, stand
and tree change. They carry
no expansion factors and make no modelling choices; estimates that need
expansion come from the estimators (``area_change()`` and friends).

References
----------
Burrill, E.A., et al. The Forest Inventory and Analysis Database: Database
Description and User Guide, version 9.4. Sections 2.4 (PLOT), 2.5 (COND),
2.9 (SUBP_COND_CHNG_MTRX), 3.1 (TREE), 3.3 (TREE_GRM_COMPONENT) and 3.5
(TREE_GRM_MIDPT).
"""

from __future__ import annotations

from typing import Literal

import polars as pl

from .constants.status_codes import (
    LandStatus,
    ReserveStatus,
    SiteClass,
    TreeComponent,
)
from .core import FIA
from .core.fiadb_types import cast_to_fiadb_types
from .estimation.grm import resolve_grm_columns
from .estimation.utils import ensure_fia_instance

# Condition attributes returned at both times (when present in the database)
CONDITION_COLUMNS = [
    "COND_STATUS_CD",
    "COND_NONSAMPLE_REASN_CD",
    "CONDPROP_UNADJ",
    "SUBPPROP_UNADJ",
    "MICRPROP_UNADJ",
    "MACRPROP_UNADJ",
    "PROP_BASIS",
    "OWNCD",
    "OWNGRPCD",
    "FORTYPCD",
    "STDSZCD",
    "STDORGCD",
    "STDAGE",
    "SITECLCD",
    "RESERVCD",
    "BALIVE",
    "TRTCD1",
    "TRTCD2",
    "TRTCD3",
    "TRTYR1",
    "TRTYR2",
    "TRTYR3",
    "DSTRBCD1",
    "DSTRBCD2",
    "DSTRBCD3",
    "DSTRBYR1",
    "DSTRBYR2",
    "DSTRBYR3",
    "HARVEST_TYPE1_SRS",
    "HARVEST_TYPE2_SRS",
    "HARVEST_TYPE3_SRS",
]

# Plot attributes of the time-2 measurement
PLOT_COLUMNS = [
    "STATECD",
    "UNITCD",
    "COUNTYCD",
    "REMPER",
    "INTENSITY",
    "KINDCD",
    "DESIGNCD",
    "QA_STATUS",
]

# Plot attributes returned at both times
PLOT_TIME_COLUMNS = ["INVYR", "MEASYEAR", "MEASMON"]

T2_OUTCOMES = {
    LandStatus.FOREST: "forest",
    LandStatus.NONFOREST: "nonforest",
    LandStatus.WATER: "water",
    LandStatus.CENSUS_WATER: "water",
    LandStatus.NONSAMPLED: "nonsampled",
}


def condition_intervals(
    db: str | FIA,
    *,
    at_risk_land: Literal["forest", "timber", "all"] = "forest",
    columns: list[str] | None = None,
    min_remper: float | None = None,
    max_remper: float | None = None,
    min_invyr: int | None = None,
    change_matrix: bool = True,
) -> pl.DataFrame:
    """
    Pair each condition at time 1 with the conditions it became at time 2.

    Returns one row per (time-1 condition, time-2 condition) pair on remeasured
    plots, for every time-1 condition in the at-risk land class. The pairing
    comes from ``SUBP_COND_CHNG_MTRX``, which records, subplot by subplot, how
    the previous measurement's mapped conditions overlap the current ones, so
    a condition that split or changed class appears once per resulting
    condition with its share of the plot area. Plots without change-matrix
    rows (periodic inventories and a few annual plots) fall back to pairing
    conditions with the same CONDID.

    Parameters
    ----------
    db : str | FIA
        Database connection or path to an FIA DuckDB database. If the FIA
        instance is clipped to evaluations (``clip_by_evalid``,
        ``clip_most_recent``), only time-2 plots in those evaluations are
        returned; otherwise every remeasured plot is.
    at_risk_land : {'forest', 'timber', 'all'}, default 'forest'
        Land class that defines the at-risk set at time 1:

        - 'forest': forest land (COND_STATUS_CD 1)
        - 'timber': timberland (forest land with SITECLCD 1-6 and RESERVCD 0)
        - 'all': every time-1 condition, whatever its status

        Membership is judged on the time-1 condition only; what the land
        became is reported in ``t2_OUTCOME``.
    columns : list of str, optional
        Extra COND columns to return at both times, prefixed ``t1_`` and
        ``t2_``.
    min_remper, max_remper : float, optional
        Keep plots whose remeasurement period (PLOT.REMPER, years) is within
        these bounds. Plots without REMPER are dropped when either is set.
    min_invyr : int, optional
        Keep plots whose time-2 inventory year is at least this.
    change_matrix : bool, default True
        Pair conditions through SUBP_COND_CHNG_MTRX where the plot has rows
        in it. If False, every pair uses the same-CONDID fallback.

    Returns
    -------
    pl.DataFrame
        One row per condition pair, with columns:

        - **PLT_CN**, **PREV_PLT_CN** : time-2 and time-1 plot CNs
        - **t1_CONDID**, **t2_CONDID** : condition numbers at each time
          (``t2_CONDID`` is null when ``t2_OUTCOME`` is 'no_t2_condition')
        - **EVALID** : int - the evaluation when the FIA instance is clipped
          to exactly one, else null; see ``FIA.provenance()``
        - **STATECD**, **UNITCD**, **COUNTYCD**, **REMPER**, **INTENSITY**,
          **KINDCD**, **DESIGNCD**, **QA_STATUS** : time-2 plot attributes
        - **t1_INVYR**, **t2_INVYR**, **t1_MEASYEAR**, **t2_MEASYEAR**,
          **t1_MEASMON**, **t2_MEASMON** : measurement timing
        - **t1_<COND column>**, **t2_<COND column>** : condition attributes at
          each time: status, nonsample reason, the CONDPROP/SUBPPROP/
          MICRPROP/MACRPROP proportions, PROP_BASIS, ownership, forest type,
          stand size, origin and age, site class, reserve status, BALIVE,
          treatments and disturbances with their years, the SRS harvest
          types where the database has them, and any ``columns`` requested
        - **CHNG_AREA_SHARE** : float - share of the plot's area that was in
          the time-1 condition and is in the time-2 condition: the sum of
          SUBPTYP_PROP_CHNG over the pair's change-matrix rows for the
          condition's footprint (subplot, or macroplot when PROP_BASIS is
          'MACR'), divided by 4. Null for same-CONDID pairs.
        - **LINK_METHOD** : str - how the plot's conditions were paired:
          'change_matrix' or 'same_condid'
        - **t2_OUTCOME** : str - what the time-1 condition's land became:
          'forest', 'nonforest', 'water', 'nonsampled', or 'no_t2_condition'
          when the time-1 condition has no time-2 counterpart

    See Also
    --------
    area_change : Estimate forest or timberland area change
    panel : Condition- and tree-level remeasurement panels

    Examples
    --------
    Forest conditions in Alabama's most recent change evaluation, and what
    their land became:

    >>> from pyfia import FIA, condition_intervals
    >>> with FIA("alabama.duckdb") as db:
    ...     db.clip_most_recent(eval_type="CHNG")
    ...     pairs = condition_intervals(db)
    >>> pairs.group_by("t2_OUTCOME").agg(pl.col("CHNG_AREA_SHARE").sum())

    Harvest-relevant attributes at both times:

    >>> pairs = condition_intervals(db, columns=["STDSZCD", "ALSTKCD"])

    Notes
    -----
    Columns taken from FIADB tables have their FIADB types
    (:data:`pyfia.constants.fiadb_schema.COLUMN_TYPES`) whatever the database
    stores, so frames from different states' databases concatenate cleanly.

    The change matrix links a time-1 condition to a time-2 condition only
    where they overlap on the ground, so ``CHNG_AREA_SHARE`` is the natural
    weight for area. Expanded by the plot's EXPNS and the adjustment factor of
    the time-2 condition's footprint, and restricted to conditions sampled at
    both times, it reproduces ``area_change()``: the rows with a time-1
    forest condition and a nonforest or water outcome sum to the gross loss,
    as in EVALIDator.

    The same-CONDID fallback assumes a condition keeps its number between
    measurements, which the FIADB User Guide does not guarantee; check
    ``LINK_METHOD`` before relying on those pairs. In FIADB 1.9.4 COND carries
    no PREVCOND column, so there is no better condition-level link for plots
    outside the change matrix.
    """
    if at_risk_land not in ("forest", "timber", "all"):
        raise ValueError(
            f"at_risk_land must be 'forest', 'timber' or 'all', got {at_risk_land!r}"
        )

    fia, owns_db = ensure_fia_instance(db)
    try:
        return _condition_intervals(
            fia,
            at_risk_land=at_risk_land,
            columns=columns or [],
            min_remper=min_remper,
            max_remper=max_remper,
            min_invyr=min_invyr,
            change_matrix=change_matrix,
        )
    finally:
        if owns_db and hasattr(fia, "close"):
            fia.close()


def _condition_intervals(
    fia: FIA,
    *,
    at_risk_land: str,
    columns: list[str],
    min_remper: float | None,
    max_remper: float | None,
    min_invyr: int | None,
    change_matrix: bool,
) -> pl.DataFrame:
    reader = fia._reader
    plot_schema = set(reader.get_table_schema("PLOT"))
    cond_schema = set(reader.get_table_schema("COND"))

    # Time-2 plots: remeasured, within the requested bounds and evaluations
    plot_cols = [
        c
        for c in ["CN", "PREV_PLT_CN", *PLOT_COLUMNS, *PLOT_TIME_COLUMNS]
        if c in plot_schema
    ]
    remeasured = _read_typed(reader, "PLOT", columns=plot_cols, lazy=True).filter(
        pl.col("PREV_PLT_CN").is_not_null()
    )
    if min_remper is not None:
        remeasured = remeasured.filter(pl.col("REMPER") >= min_remper)
    if max_remper is not None:
        remeasured = remeasured.filter(pl.col("REMPER") <= max_remper)
    if min_invyr is not None:
        remeasured = remeasured.filter(pl.col("INVYR") >= min_invyr)
    if fia.evalid:
        evalids = ", ".join(str(int(e)) for e in fia.evalid)
        in_eval = (
            _read_typed(
                reader,
                "POP_PLOT_STRATUM_ASSGN",
                columns=["PLT_CN"],
                where=f"EVALID IN ({evalids})",
                lazy=True,
            )
            .unique()
            .rename({"PLT_CN": "CN"})
        )
        remeasured = remeasured.join(in_eval, on="CN", how="semi")
    plot_t2 = remeasured.collect()

    time_cols = [c for c in PLOT_TIME_COLUMNS if c in plot_schema]
    plot_t1 = (
        _read_typed(reader, "PLOT", columns=["CN", *time_cols], lazy=True)
        .join(
            plot_t2.lazy().select(pl.col("PREV_PLT_CN").alias("CN")).unique(),
            on="CN",
            how="semi",
        )
        .rename({"CN": "PREV_PLT_CN", **{c: f"t1_{c}" for c in time_cols}})
        .collect()
    )
    plot_t2 = plot_t2.rename({"CN": "PLT_CN", **{c: f"t2_{c}" for c in time_cols}})

    # Conditions at both times
    missing = [c for c in columns if c not in cond_schema]
    if missing:
        raise ValueError(f"COND has no column(s) {missing}")
    attr_cols = [
        c for c in dict.fromkeys(CONDITION_COLUMNS + columns) if c in cond_schema
    ]
    cond = _read_typed(
        reader, "COND", columns=["PLT_CN", "CONDID", *attr_cols], lazy=True
    )
    cond_t1 = (
        cond.join(
            plot_t2.lazy().select(pl.col("PREV_PLT_CN").alias("PLT_CN")).unique(),
            on="PLT_CN",
            how="semi",
        )
        .rename({"PLT_CN": "PREV_PLT_CN", "CONDID": "t1_CONDID"})
        .rename({c: f"t1_{c}" for c in attr_cols})
        .collect()
    )
    cond_t2 = (
        cond.join(plot_t2.lazy().select("PLT_CN"), on="PLT_CN", how="semi")
        .rename({"CONDID": "t2_CONDID"})
        .rename({c: f"t2_{c}" for c in attr_cols})
        .collect()
    )

    at_risk = cond_t1.filter(_in_land_class(at_risk_land, "t1_"))

    # Pairs from the change matrix, on the footprint of the time-2 condition
    if change_matrix:
        chng = (
            _read_typed(
                reader,
                "SUBP_COND_CHNG_MTRX",
                columns=[
                    "PLT_CN",
                    "CONDID",
                    "PREV_PLT_CN",
                    "PREVCOND",
                    "SUBPTYP",
                    "SUBPTYP_PROP_CHNG",
                ],
                lazy=True,
            )
            .join(plot_t2.lazy().select("PLT_CN"), on="PLT_CN", how="semi")
            .collect()
        )
    else:
        chng = pl.DataFrame(
            schema={
                "PLT_CN": plot_t2.schema["PLT_CN"],
                "CONDID": pl.Int64,
                "PREV_PLT_CN": plot_t2.schema["PREV_PLT_CN"],
                "PREVCOND": pl.Int64,
                "SUBPTYP": pl.Int64,
                "SUBPTYP_PROP_CHNG": pl.Float64,
            }
        )

    footprint_subptyp = (
        pl.when(pl.col("t2_PROP_BASIS") == "MACR").then(3).otherwise(1)
        if "t2_PROP_BASIS" in cond_t2.columns
        else pl.lit(1)
    )
    matrix_pairs = (
        chng.rename({"CONDID": "t2_CONDID", "PREVCOND": "t1_CONDID"})
        .join(
            cond_t2.select(
                [
                    "PLT_CN",
                    "t2_CONDID",
                    *(["t2_PROP_BASIS"] if "t2_PROP_BASIS" in cond_t2.columns else []),
                ]
            ),
            on=["PLT_CN", "t2_CONDID"],
            how="left",
        )
        .filter(pl.col("SUBPTYP") == footprint_subptyp)
        .group_by(["PLT_CN", "PREV_PLT_CN", "t1_CONDID", "t2_CONDID"])
        .agg((pl.col("SUBPTYP_PROP_CHNG").sum() / 4.0).alias("CHNG_AREA_SHARE"))
    )

    # Same-CONDID pairs for plots outside the change matrix
    matrix_plots = chng.select("PLT_CN").unique()
    fallback_pairs: pl.DataFrame = (
        plot_t2.select(["PLT_CN", "PREV_PLT_CN"])
        .join(matrix_plots, on="PLT_CN", how="anti")
        .join(cond_t2.select(["PLT_CN", "t2_CONDID"]), on="PLT_CN", how="inner")
        .with_columns(
            pl.col("t2_CONDID").alias("t1_CONDID"),
            pl.lit(None, dtype=pl.Float64).alias("CHNG_AREA_SHARE"),
        )
        .select(matrix_pairs.columns)
    )
    pairs = pl.concat([matrix_pairs, fallback_pairs], how="vertical_relaxed")

    # How each plot's conditions were paired (also for unpaired conditions)
    plot_t2 = plot_t2.join(
        matrix_plots.with_columns(pl.lit("change_matrix").alias("LINK_METHOD")),
        on="PLT_CN",
        how="left",
    ).with_columns(pl.col("LINK_METHOD").fill_null("same_condid"))

    # Every at-risk time-1 condition, with its time-2 counterparts if any
    at_risk = at_risk.join(
        plot_t2.select(["PLT_CN", "PREV_PLT_CN"]), on="PREV_PLT_CN", how="inner"
    )
    result = (
        at_risk.join(pairs, on=["PLT_CN", "PREV_PLT_CN", "t1_CONDID"], how="left")
        .join(cond_t2, on=["PLT_CN", "t2_CONDID"], how="left")
        .join(plot_t2, on=["PLT_CN", "PREV_PLT_CN"], how="left")
        .join(plot_t1, on="PREV_PLT_CN", how="left")
    )

    outcome = pl.col("t2_COND_STATUS_CD").replace_strict(
        T2_OUTCOMES, default=None, return_dtype=pl.Utf8
    )
    result = result.with_columns(
        pl.when(pl.col("t2_CONDID").is_null())
        .then(pl.lit("no_t2_condition"))
        .otherwise(outcome)
        .alias("t2_OUTCOME")
    )

    result = result.with_columns(
        pl.lit(fia._single_evalid(), dtype=pl.Int64).alias("EVALID")
    )
    key_cols = ["PLT_CN", "PREV_PLT_CN", "t1_CONDID", "t2_CONDID"]
    plot_out = [c for c in PLOT_COLUMNS if c in result.columns]
    time_out = [f"t{t}_{c}" for c in time_cols for t in (1, 2)]
    cond_out = [f"t{t}_{c}" for c in attr_cols for t in (1, 2)]
    ordered = [
        *key_cols,
        "EVALID",
        *plot_out,
        *time_out,
        *cond_out,
        "CHNG_AREA_SHARE",
        "LINK_METHOD",
        "t2_OUTCOME",
    ]
    return _cast_keys(result.select(ordered)).sort(key_cols, nulls_last=True)


def _in_land_class(land: str, prefix: str) -> pl.Expr:
    """Whether a condition is in ``land``, judged on ``prefix``-ed columns."""
    if land == "all":
        return pl.lit(True)
    in_class = pl.col(f"{prefix}COND_STATUS_CD") == LandStatus.FOREST
    if land == "timber":
        in_class = (
            in_class
            & pl.col(f"{prefix}SITECLCD").is_in(SiteClass.PRODUCTIVE_CLASSES)
            & (pl.col(f"{prefix}RESERVCD") == ReserveStatus.NOT_RESERVED)
        )
    return in_class.fill_null(False)


# Tree attributes returned at both times (when present in the database)
TREE_COLUMNS = [
    "SPCD",
    "STATUSCD",
    "TREECLCD",
    "TREEGRCD",
    "CULL",
    "DIA",
    "HT",
    "VOLCFNET",
    "VOLCSNET",
    "VOLBFNET",
    "DRYBIO_AG",
]

# TREE_GRM_MIDPT values the GRM estimators multiply by a tree's weight
MIDPT_COLUMNS = [
    "VOLCFNET",
    "VOLCSNET",
    "VOLBFNET",
    "DRYBIO_AG",
    "DRYBIO_BOLE",
    "DRYBIO_BRANCH",
]

# TREE_GRM_COMPONENT columns that don't depend on the tree or land basis
GRM_COLUMNS = [
    "DIA_BEGIN",
    "DIA_MIDPT",
    "DIA_END",
    "ANN_DIA_GROWTH",
    "ANN_HT_GROWTH",
]

FATES = {
    TreeComponent.SURVIVOR: "survivor",
    TreeComponent.INGROWTH: "ingrowth",
    TreeComponent.CUT: "cut",
    TreeComponent.MORTALITY: "mortality",
    TreeComponent.DIVERSION: "diversion",
    TreeComponent.REVERSION: "reversion",
}


def tree_intervals(
    db: str | FIA,
    *,
    tree_basis: Literal["al", "al5", "gs", "sl"] = "al",
    land_basis: Literal["forest", "timber"] = "forest",
    components: list[str] | None = None,
    columns: list[str] | None = None,
    t1_attributes: bool = True,
) -> pl.DataFrame:
    """
    Follow each tree through its remeasurement interval.

    Returns one row per tree in ``TREE_GRM_COMPONENT`` on the requested tree
    and land basis, with its GRM component (what happened to it), its
    per-acre weights, its diameters over the interval, and its attributes at
    both measurements. Rows carry no expansion factors; weighted by the
    plot's EXPNS and the adjustment factor of ``SUBPTYP_GRM``, they reproduce
    the GRM estimators (``removals()``, ``mortality()``).

    Parameters
    ----------
    db : str | FIA
        Database connection or path to an FIA DuckDB database. If the FIA
        instance is clipped to evaluations (``clip_by_evalid``,
        ``clip_most_recent``), only time-2 plots in those evaluations are
        returned; otherwise every tree with a GRM record is.
    tree_basis : {'al', 'al5', 'gs', 'sl'}, default 'al'
        Tree population, which selects the GRM columns:

        - 'al': all live trees at least 1 inch d.b.h./d.r.c.
        - 'al5': all live trees at least 5 inches d.b.h./d.r.c., from the
          subplot all-live columns (EVALIDator's "trees at least 5 inches")
        - 'gs': growing-stock trees at least 5 inches d.b.h.
        - 'sl': sawtimber trees
    land_basis : {'forest', 'timber'}, default 'forest'
        Land basis of the GRM columns: forest land or timberland.
    components : list of str, optional
        Keep only these GRM components, matched by prefix, e.g.
        ``["CUT", "DIVERSION"]`` for removals. Default: all components.
    columns : list of str, optional
        Extra TREE columns to return at both times, prefixed ``t1_`` and
        ``t2_``.
    t1_attributes : bool, default True
        If False, skip the time-1 TREE attributes (faster).

    Returns
    -------
    pl.DataFrame
        One row per tree, with columns:

        - **TRE_CN**, **PREV_TRE_CN** : time-2 and time-1 TREE CNs
          (``PREV_TRE_CN`` is null for trees without a time-1 record, such
          as ingrowth and reconciled missed trees)
        - **PLT_CN**, **PREV_PLT_CN** : time-2 and time-1 plot CNs
        - **t1_CONDID**, **t2_CONDID** : the tree's condition at each time
          (time 1 from its previous TREE record, falling back to
          TREE.PREVCOND)
        - **EVALID** : int - the evaluation when the FIA instance is clipped
          to exactly one, else null; see ``FIA.provenance()``
        - **STATECD**, **REMPER**, **t1_INVYR**, **t2_INVYR**,
          **t1_MEASYEAR**, **t2_MEASYEAR** : plot and timing
        - **COMPONENT** : str - GRM component on the basis, as FIADB spells it
          (SURVIVOR, INGROWTH, CUT1, CUT2, MORTALITY1, MORTALITY2,
          DIVERSION1, DIVERSION2, REVERSION1, REVERSION2)
        - **FATE** : str - the component without its number: 'survivor',
          'ingrowth', 'cut', 'mortality', 'diversion' or 'reversion'
        - **TPAGROW_UNADJ** : float - trees per acre the tree represents over
          the interval, for every component
        - **TPAREMV_UNADJ**, **TPAMORT_UNADJ** : float - annual removals and
          mortality rates, TPAGROW_UNADJ / REMPER on CUT and DIVERSION rows
          and on MORTALITY rows respectively, else zero or null
        - **SUBPTYP_GRM** : int - plot footprint whose adjustment factor
          applies (0 none, 1 subplot, 2 microplot, 3 macroplot)
        - **DIA_BEGIN**, **DIA_MIDPT**, **DIA_END**, **ANN_DIA_GROWTH**,
          **ANN_HT_GROWTH** : diameters and annual growth over the interval
        - **MIDPT_<column>** : TREE_GRM_MIDPT values at the interval
          midpoint (VOLCFNET, VOLCSNET, VOLBFNET, DRYBIO_AG, DRYBIO_BOLE,
          DRYBIO_BRANCH), the values removals and mortality are measured in
        - **t1_<TREE column>**, **t2_<TREE column>** : SPCD, STATUSCD,
          TREECLCD, TREEGRCD, CULL, DIA, HT, VOLCFNET, VOLCSNET, VOLBFNET,
          DRYBIO_AG and any ``columns``, from the TREE records at each time

    See Also
    --------
    condition_intervals : Condition pairs across two measurements
    removals : Estimate annual removals
    mortality : Estimate annual mortality
    panel : Condition- and tree-level remeasurement panels

    Examples
    --------
    Trees removed from Alabama's timberland in the most recent GRM
    evaluation, with their time-1 species and diameter:

    >>> from pyfia import FIA, tree_intervals
    >>> with FIA("alabama.duckdb") as db:
    ...     db.clip_most_recent(eval_type="GRM")
    ...     cut = tree_intervals(
    ...         db, tree_basis="gs", land_basis="timber", components=["CUT"]
    ...     )
    >>> cut.select("t1_SPCD", "t1_DIA", "TPAREMV_UNADJ")

    Notes
    -----
    Columns taken from FIADB tables have their FIADB types
    (:data:`pyfia.constants.fiadb_schema.COLUMN_TYPES`) whatever the database
    stores, so frames from different states' databases concatenate cleanly.

    Removals are the CUT and DIVERSION rows and mortality the MORTALITY
    rows. Summing ``TPAREMV_UNADJ`` (or ``TPAMORT_UNADJ``) times the
    adjustment factor for ``SUBPTYP_GRM`` times the plot's EXPNS times a
    ``MIDPT_`` value, over an evaluation's plots, gives the corresponding
    ``removals()`` (or ``mortality()``) total, as in EVALIDator.

    Within a GRM evaluation, the time-1 live trees on the land basis and
    the SURVIVOR, CUT, MORTALITY and DIVERSION rows correspond one to one,
    except for trees FIA reconciled as missed at time 1 (rows without a
    ``PREV_TRE_CN``) and a few time-1 trees without a GRM record.
    """
    if tree_basis not in ("al", "al5", "gs", "sl"):
        raise ValueError(
            f"tree_basis must be 'al', 'al5', 'gs' or 'sl', got {tree_basis!r}"
        )
    if land_basis not in ("forest", "timber"):
        raise ValueError(f"land_basis must be 'forest' or 'timber', got {land_basis!r}")

    fia, owns_db = ensure_fia_instance(db)
    try:
        return _tree_intervals(
            fia,
            tree_basis=tree_basis,
            land_basis=land_basis,
            components=components,
            columns=columns or [],
            t1_attributes=t1_attributes,
        )
    finally:
        if owns_db and hasattr(fia, "close"):
            fia.close()


def _tree_intervals(
    fia: FIA,
    *,
    tree_basis: str,
    land_basis: str,
    components: list[str] | None,
    columns: list[str],
    t1_attributes: bool,
) -> pl.DataFrame:
    reader = fia._reader
    grm_schema = set(reader.get_table_schema("TREE_GRM_COMPONENT"))
    tree_schema = set(reader.get_table_schema("TREE"))
    midpt_schema = set(reader.get_table_schema("TREE_GRM_MIDPT"))
    plot_schema = set(reader.get_table_schema("PLOT"))

    missing = [c for c in columns if c not in tree_schema]
    if missing:
        raise ValueError(f"TREE has no column(s) {missing}")

    growth = resolve_grm_columns("growth", tree_basis, land_basis)
    weights = {
        growth.tpa: "TPAGROW_UNADJ",
        resolve_grm_columns("removals", tree_basis, land_basis).tpa: "TPAREMV_UNADJ",
        resolve_grm_columns("mortality", tree_basis, land_basis).tpa: "TPAMORT_UNADJ",
    }
    grm_cols = [
        "TRE_CN",
        "PLT_CN",
        growth.component,
        growth.subptyp,
        *weights,
        *[c for c in GRM_COLUMNS if c in grm_schema],
    ]
    grm = _read_typed(reader, "TREE_GRM_COMPONENT", columns=grm_cols, lazy=True).rename(
        {growth.component: "COMPONENT", growth.subptyp: "SUBPTYP_GRM", **weights}
    )
    grm = grm.filter(
        pl.col("COMPONENT").is_not_null()
        & (pl.col("COMPONENT") != TreeComponent.NOT_USED)
    )
    if components:
        grm = grm.filter(
            pl.any_horizontal(
                pl.col("COMPONENT").str.starts_with(c) for c in components
            )
        )
    if fia.evalid:
        evalids = ", ".join(str(int(e)) for e in fia.evalid)
        in_eval = _read_typed(
            reader,
            "POP_PLOT_STRATUM_ASSGN",
            columns=["PLT_CN"],
            where=f"EVALID IN ({evalids})",
            lazy=True,
        ).unique()
        grm = grm.join(in_eval, on="PLT_CN", how="semi")
    grm_df = grm.collect()

    fate = pl.lit(None, dtype=pl.Utf8)
    for prefix, name in reversed(list(FATES.items())):
        fate = (
            pl.when(pl.col("COMPONENT").str.starts_with(prefix))
            .then(pl.lit(name))
            .otherwise(fate)
        )
    grm_df = grm_df.with_columns(fate.alias("FATE"))

    trees = grm_df.lazy().select(pl.col("TRE_CN").alias("CN"))

    # Time-2 tree records
    t2_attr = [c for c in dict.fromkeys(TREE_COLUMNS + columns) if c in tree_schema]
    t2_cols = [
        "CN",
        "PREV_TRE_CN",
        "CONDID",
        *(["PREVCOND"] if "PREVCOND" in tree_schema else []),
        *t2_attr,
    ]
    t2 = (
        _read_typed(reader, "TREE", columns=t2_cols, lazy=True)
        .join(trees, on="CN", how="semi")
        .rename(
            {"CN": "TRE_CN", "CONDID": "t2_CONDID", **{c: f"t2_{c}" for c in t2_attr}}
        )
        .collect()
    )
    result = grm_df.join(t2, on="TRE_CN", how="left")

    # Time-1 tree records
    t1_attr = t2_attr if t1_attributes else []
    prev = result.lazy().select(pl.col("PREV_TRE_CN").alias("CN")).drop_nulls().unique()
    t1 = (
        _read_typed(reader, "TREE", columns=["CN", "CONDID", *t1_attr], lazy=True)
        .join(prev, on="CN", how="semi")
        .rename(
            {
                "CN": "PREV_TRE_CN",
                "CONDID": "_t1_CONDID",
                **{c: f"t1_{c}" for c in t1_attr},
            }
        )
        .collect()
    )
    result = result.join(t1, on="PREV_TRE_CN", how="left")
    t1_condid = pl.col("_t1_CONDID")
    if "PREVCOND" in result.columns:
        t1_condid = t1_condid.fill_null(pl.col("PREVCOND"))
    result = result.with_columns(t1_condid.alias("t1_CONDID"))

    # Midpoint values
    midpt_attr = [c for c in MIDPT_COLUMNS if c in midpt_schema]
    midpt = (
        _read_typed(
            reader, "TREE_GRM_MIDPT", columns=["TRE_CN", *midpt_attr], lazy=True
        )
        .join(trees.rename({"CN": "TRE_CN"}), on="TRE_CN", how="semi")
        .rename({c: f"MIDPT_{c}" for c in midpt_attr})
        .collect()
    )
    result = result.join(midpt, on="TRE_CN", how="left")

    # Plots at both times
    time_cols = [c for c in ["INVYR", "MEASYEAR"] if c in plot_schema]
    plots = _read_typed(
        reader,
        "PLOT",
        columns=["CN", "PREV_PLT_CN", "STATECD", "REMPER", *time_cols],
        lazy=True,
    )
    plot_t2 = (
        plots.join(
            result.lazy().select(pl.col("PLT_CN").alias("CN")).unique(),
            on="CN",
            how="semi",
        )
        .rename({"CN": "PLT_CN", **{c: f"t2_{c}" for c in time_cols}})
        .collect()
    )
    result = result.join(plot_t2, on="PLT_CN", how="left")
    plot_t1 = (
        plots.select(["CN", *time_cols])
        .join(
            result.lazy()
            .select(pl.col("PREV_PLT_CN").alias("CN"))
            .drop_nulls()
            .unique(),
            on="CN",
            how="semi",
        )
        .rename({"CN": "PREV_PLT_CN", **{c: f"t1_{c}" for c in time_cols}})
        .collect()
    )
    result = result.join(plot_t1, on="PREV_PLT_CN", how="left")

    key_cols = [
        "TRE_CN",
        "PREV_TRE_CN",
        "PLT_CN",
        "PREV_PLT_CN",
        "t1_CONDID",
        "t2_CONDID",
    ]
    result = result.with_columns(
        pl.lit(fia._single_evalid(), dtype=pl.Int64).alias("EVALID")
    )
    ordered = [
        *key_cols,
        "EVALID",
        "STATECD",
        "REMPER",
        *[f"t{t}_{c}" for c in time_cols for t in (1, 2)],
        "COMPONENT",
        "FATE",
        "TPAGROW_UNADJ",
        "TPAREMV_UNADJ",
        "TPAMORT_UNADJ",
        "SUBPTYP_GRM",
        *[c for c in GRM_COLUMNS if c in result.columns],
        *[f"MIDPT_{c}" for c in midpt_attr],
        *[
            f"t{t}_{c}"
            for c in t2_attr
            for t in (1, 2)
            if f"t{t}_{c}" in result.columns
        ],
    ]
    return _cast_keys(result.select(ordered)).sort(["PLT_CN", "TRE_CN"])


def _read_typed(
    reader,
    table: str,
    columns: list[str] | None = None,
    where: str | None = None,
    lazy: bool = True,
) -> pl.LazyFrame:
    """Read ``columns`` of ``table`` with FIADB's column types."""
    frame = reader.read_table(table, columns=columns, where=where, lazy=False)
    return cast_to_fiadb_types(frame, table).lazy()


def _cast_keys(df: pl.DataFrame) -> pl.DataFrame:
    """CN keys as strings and CONDIDs as integers, whatever the backend stores."""
    cn_cols = [
        c for c in ("TRE_CN", "PREV_TRE_CN", "PLT_CN", "PREV_PLT_CN") if c in df.columns
    ]
    condid_cols = [c for c in ("t1_CONDID", "t2_CONDID") if c in df.columns]
    return df.with_columns(
        pl.col(cn_cols).cast(pl.Utf8),
        pl.col(condid_cols).cast(pl.Int64),
    )
