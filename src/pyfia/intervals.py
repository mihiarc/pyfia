"""
Remeasurement intervals: FIA conditions paired across two measurements.

Each row pairs a condition at the earlier measurement (time 1) with a
condition at the later one (time 2) on the same plot location. These are
unit-level building blocks for analyses of land and stand change. They carry
no expansion factors and make no modelling choices; estimates that need
expansion come from the estimators (``area_change()`` and friends).

References
----------
Burrill, E.A., et al. The Forest Inventory and Analysis Database: Database
Description and User Guide, version 9.4. Sections 2.4 (PLOT), 2.5 (COND) and
2.9 (SUBP_COND_CHNG_MTRX).
"""

from __future__ import annotations

from typing import Literal

import polars as pl

from .constants.status_codes import LandStatus, ReserveStatus, SiteClass
from .core import FIA
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
    remeasured = reader.read_table("PLOT", columns=plot_cols, lazy=True).filter(
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
            reader.read_table(
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
        reader.read_table("PLOT", columns=["CN", *time_cols], lazy=True)
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
    cond = reader.read_table(
        "COND", columns=["PLT_CN", "CONDID", *attr_cols], lazy=True
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
            reader.read_table(
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

    key_cols = ["PLT_CN", "PREV_PLT_CN", "t1_CONDID", "t2_CONDID"]
    plot_out = [c for c in PLOT_COLUMNS if c in result.columns]
    time_out = [f"t{t}_{c}" for c in time_cols for t in (1, 2)]
    cond_out = [f"t{t}_{c}" for c in attr_cols for t in (1, 2)]
    ordered = [
        *key_cols,
        *plot_out,
        *time_out,
        *cond_out,
        "CHNG_AREA_SHARE",
        "LINK_METHOD",
        "t2_OUTCOME",
    ]
    return result.select(ordered).sort(key_cols, nulls_last=True)


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
