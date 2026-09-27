"""
Stand attributes per acre of condition, built from tree records.

A tree's ``TPA_UNADJ`` is the number of trees per acre of *plot* it stands for.
Dividing it by the proportion of the tree's plot footprint (microplot, subplot
or macroplot) that lies in the tree's condition gives trees per acre of
*condition*. Summing tree attributes with that weight reproduces FIADB's own
condition attributes, such as ``COND.BALIVE``.

References
----------
FIA Database User Guide, sections 2.4.40 (PLOT.MACRO_BREAKPOINT_DIA),
2.5.27-2.5.31 (COND.PROP_BASIS, CONDPROP_UNADJ, MICRPROP_UNADJ,
SUBPPROP_UNADJ, MACRPROP_UNADJ), 2.5.50 (COND.BALIVE) and 3.1.92
(TREE.TPA_UNADJ).
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Literal

import polars as pl

from .constants.columns import (
    CondColumns,
    PlotColumns,
    RefColumns,
    TreeColumns,
)
from .constants.plot_design import DiameterBreakpoints
from .constants.status_codes import LandStatus, TreeClass, TreeStatus
from .constants.tables import TableNames
from .core.fiadb_types import cast_to_fiadb_types
from .estimation.constants import BASAL_AREA_FACTOR, LBS_TO_SHORT_TONS
from .estimation.utils import ensure_fia_instance

if TYPE_CHECKING:
    from .core import FIA

METRICS: tuple[str, ...] = (
    "ba",
    "tpa",
    "qmd",
    "drybio_ag",
    "volcfnet",
    "volcsnet",
    "softwood_ba_share",
)

# Output column for each metric
METRIC_COLUMNS: dict[str, str] = {
    "ba": "BAA",
    "tpa": "TPA",
    "qmd": "QMD",
    "drybio_ag": "DRYBIO_AG_ACRE",
    "volcfnet": "VOLCFNET_ACRE",
    "volcsnet": "VOLCSNET_ACRE",
    "softwood_ba_share": "SOFTWOOD_BA_SHARE",
}

# Tree attribute each additive metric sums, and its unit conversion
_ATTRIBUTE_METRICS: dict[str, tuple[str, float]] = {
    "drybio_ag": (TreeColumns.DRYBIO_AG, LBS_TO_SHORT_TONS),
    "volcfnet": (TreeColumns.VOLCFNET, 1.0),
    "volcsnet": (TreeColumns.VOLCSNET, 1.0),
}

_SOFTWOOD = "S"
_KEYS = [TreeColumns.PLT_CN, TreeColumns.CONDID]


def condition_stand_metrics(
    db: str | FIA,
    *,
    metrics: Sequence[str] = METRICS,
    tree_type: Literal["live", "gs"] = "live",
    min_dia: float | None = None,
    plot_cns: Sequence[str | int] | None = None,
    zero_fill: bool = True,
) -> pl.DataFrame:
    """
    Compute stand attributes per acre of condition from tree records.

    Returns one row per forest condition (``PLT_CN``, ``CONDID``) with basal
    area, trees, quadratic mean diameter, biomass, volume and softwood share
    per acre of that condition. These are unit-level values for the sampled
    conditions, not population estimates: nothing is expanded, and no EVALID
    is needed, so the function works on any set of plots, including earlier
    measurements of remeasured plots.

    Parameters
    ----------
    db : str | FIA
        Database connection or path to FIA database. When no ``plot_cns`` are
        given, a clipped ``FIA`` limits the plots to its evaluation
        (``clip_by_evalid``, ``clip_most_recent``), state and polygon.
    metrics : sequence of str, default all
        Metrics to compute, any of:

        - 'ba': basal area, ``BAA`` (sq ft per acre)
        - 'tpa': trees, ``TPA`` (trees per acre)
        - 'qmd': quadratic mean diameter, ``QMD`` (inches)
        - 'drybio_ag': aboveground dry biomass, ``DRYBIO_AG_ACRE`` (short
          tons per acre)
        - 'volcfnet': net cubic-foot volume, ``VOLCFNET_ACRE`` (cu ft per acre)
        - 'volcsnet': net sawlog cubic-foot volume, ``VOLCSNET_ACRE`` (cu ft
          per acre)
        - 'softwood_ba_share': ``SOFTWOOD_BA_SHARE``, the share of basal area
          in species with ``REF_SPECIES.SFTWD_HRDWD = 'S'`` (0 to 1)
    tree_type : {'live', 'gs'}, default 'live'
        Trees to include:

        - 'live': live trees (``STATUSCD = 1``)
        - 'gs': live growing-stock trees (``STATUSCD = 1`` and
          ``TREECLCD = 2``)
    min_dia : float, optional
        Smallest diameter (d.b.h. or d.r.c., inches) to include. Defaults to
        1.0, the smallest tallied tree, which is the population ``COND.BALIVE``
        describes. ``min_dia=5`` gives merchantable-size attributes, such as
        the QMD of trees 5 inches and larger.
    plot_cns : sequence of str or int, optional
        Plots (``PLOT.CN``) to compute. Overrides any clip on ``db``.
    zero_fill : bool, default True
        If True, forest conditions with no qualifying tree are returned with
        zeros for the additive metrics and ``N_TREES = 0``. If False, only
        conditions with at least one qualifying tree are returned.

    Returns
    -------
    pl.DataFrame
        One row per accessible forest condition (``COND_STATUS_CD = 1``) with
        columns:

        - **PLT_CN**, **CONDID** : str, int - Condition key
        - **EVALID** : int - the evaluation when ``db`` is clipped to exactly
          one and no ``plot_cns`` are given, else null; see
          ``FIA.provenance()``
        - **STATECD**, **INVYR** : int - State and inventory year
        - **CONDPROP_UNADJ** : float - Unadjusted proportion of the plot in
          the condition
        - **N_TREES** : int - Qualifying tree records in the condition
        - One column per requested metric, named as listed under ``metrics``

        ``QMD`` and ``SOFTWOOD_BA_SHARE`` are null for a condition with no
        qualifying tree, where they are undefined.

    See Also
    --------
    tpa : Population estimates of trees and basal area per acre
    tree_metrics : TPA-weighted tree statistics by group

    Examples
    --------
    Stand attributes for every forest condition in an evaluation:

    >>> with FIA("path/to/db.duckdb") as db:
    ...     db.clip_most_recent(eval_type="VOL")
    ...     stands = condition_stand_metrics(db)

    Merchantable-size basal area and QMD for chosen plots:

    >>> merch = condition_stand_metrics(
    ...     db, metrics=("ba", "qmd"), min_dia=5, plot_cns=["1234567890"]
    ... )

    Notes
    -----
    Each tree's weight per acre of condition is ``TPA_UNADJ`` divided by the
    proportion of its plot footprint in its condition: ``MICRPROP_UNADJ`` for
    trees under 5.0 inches, ``MACRPROP_UNADJ`` for trees at or above
    ``PLOT.MACRO_BREAKPOINT_DIA`` where macroplots were installed, and
    ``SUBPPROP_UNADJ`` otherwise (FIA Database User Guide 2.4.40, 2.5.29-2.5.31).
    With this weight, basal area of live trees 1.0 inch and larger reproduces
    ``COND.BALIVE`` (User Guide 2.5.50) to within 0.5 sq ft per acre for every
    forest condition measured since 2015 in Alabama and Oregon, a macroplot
    state. Dividing by ``CONDPROP_UNADJ`` instead matches only 86% and 61% of
    them, because a condition's shares of the microplot, subplot and
    macroplot differ.

    Periodic inventories that predate the mapped-plot design record no
    footprint proportions. Their trees are divided by ``CONDPROP_UNADJ``,
    which is exact for single-condition plots. ``COND.BALIVE`` is not
    populated for those conditions.

    Missing ``VOLCFNET``, ``VOLCSNET`` or ``DRYBIO_AG`` values, as for trees
    under 5.0 inches, count as zero.
    """
    requested = _validate_metrics(metrics)
    if tree_type not in ("live", "gs"):
        raise ValueError(f"tree_type must be 'live' or 'gs', got {tree_type!r}")
    threshold = DiameterBreakpoints.MIN_DBH if min_dia is None else float(min_dia)

    fia, owns_db = ensure_fia_instance(db)
    try:
        plots = _plot_restriction(fia, plot_cns)
        evalid = fia._single_evalid() if plot_cns is None else None
        conditions = _read_conditions(fia, plots)
        trees = _read_trees(fia, plots, requested, tree_type, threshold)
        breakpoints = _read_breakpoints(fia, plots)
        if "softwood_ba_share" in requested:
            trees = trees.join(_read_softwood(fia), on=TreeColumns.SPCD, how="left")
    finally:
        if owns_db and hasattr(fia, "close"):
            fia.close()

    per_condition = _condition_metrics(trees, conditions, breakpoints, requested)

    how: Literal["left", "inner"] = "left" if zero_fill else "inner"
    result = conditions.select(
        *_KEYS,
        pl.lit(evalid, dtype=pl.Int64).alias("EVALID"),
        CondColumns.STATECD,
        CondColumns.INVYR,
        CondColumns.CONDPROP_UNADJ,
    ).join(per_condition, on=_KEYS, how=how)

    additive = ["N_TREES"] + [
        METRIC_COLUMNS[m] for m in requested if m not in ("qmd", "softwood_ba_share")
    ]
    return result.with_columns(pl.col(additive).fill_null(0)).sort(_KEYS)


def _validate_metrics(metrics: Sequence[str]) -> list[str]:
    if isinstance(metrics, str):
        metrics = [metrics]
    unknown = sorted(set(metrics) - set(METRICS))
    if unknown:
        raise ValueError(f"Unknown metrics {unknown}; choose from {list(METRICS)}")
    if not metrics:
        raise ValueError("metrics must name at least one metric")
    return [m for m in METRICS if m in metrics]


def _plot_restriction(
    fia: FIA, plot_cns: Sequence[str | int] | None
) -> pl.DataFrame | None:
    """The plots to compute, or None for every plot the database holds."""
    if plot_cns is not None:
        cns = [str(cn) for cn in plot_cns]
    else:
        evalid_cns = fia._get_valid_plot_cns()
        spatial_cns = fia._spatial_plot_cns
        if evalid_cns is None and spatial_cns is None:
            return None
        if evalid_cns is None:
            cns = [str(cn) for cn in spatial_cns or []]
        elif spatial_cns is None:
            cns = [str(cn) for cn in evalid_cns]
        else:
            cns = sorted(
                {str(cn) for cn in evalid_cns} & {str(cn) for cn in spatial_cns}
            )
    return pl.DataFrame({TreeColumns.PLT_CN: cns}, schema={TreeColumns.PLT_CN: pl.Utf8})


def _read(
    fia: FIA,
    table: str,
    columns: list[str],
    where: str | None,
    plots: pl.DataFrame | None,
    plot_key: str = TreeColumns.PLT_CN,
) -> pl.DataFrame:
    """Read columns of a table, restricted to ``plots`` and the state clip."""
    clauses = [where] if where else []
    state_filter = getattr(fia, "state_filter", None)
    if state_filter and PlotColumns.STATECD in fia._reader.get_table_schema(table):
        states = ", ".join(str(int(s)) for s in state_filter)
        clauses.append(f"{PlotColumns.STATECD} IN ({states})")
    frame = fia._reader.read_table(
        table,
        columns=columns,
        where=" AND ".join(clauses) or None,
        lazy=False,
    )
    frame = cast_to_fiadb_types(frame, table).with_columns(
        pl.col(plot_key).cast(pl.Utf8)
    )
    if plots is not None:
        frame = frame.join(
            plots, left_on=plot_key, right_on=TreeColumns.PLT_CN, how="semi"
        )
    return frame


def _read_conditions(fia: FIA, plots: pl.DataFrame | None) -> pl.DataFrame:
    conditions = _read(
        fia,
        TableNames.COND,
        [
            CondColumns.PLT_CN,
            CondColumns.CONDID,
            CondColumns.STATECD,
            CondColumns.INVYR,
            CondColumns.CONDPROP_UNADJ,
            CondColumns.MICRPROP_UNADJ,
            CondColumns.SUBPPROP_UNADJ,
            CondColumns.MACRPROP_UNADJ,
        ],
        f"{CondColumns.COND_STATUS_CD} = {LandStatus.FOREST} "
        f"AND {CondColumns.CONDPROP_UNADJ} IS NOT NULL",
        plots,
    )
    # Some state databases store the proportions as text
    proportions = [
        CondColumns.CONDPROP_UNADJ,
        CondColumns.MICRPROP_UNADJ,
        CondColumns.SUBPPROP_UNADJ,
        CondColumns.MACRPROP_UNADJ,
    ]
    return conditions.with_columns(
        pl.col(proportions).cast(pl.Float64, strict=False),
        pl.col(CondColumns.CONDID).cast(pl.Int64),
    )


def _read_trees(
    fia: FIA,
    plots: pl.DataFrame | None,
    requested: list[str],
    tree_type: str,
    threshold: float,
) -> pl.DataFrame:
    attributes = [
        column
        for metric, (column, _) in _ATTRIBUTE_METRICS.items()
        if metric in requested
    ]
    where = (
        f"{TreeColumns.STATUSCD} = {TreeStatus.LIVE} "
        f"AND {TreeColumns.DIA} >= {threshold} "
        f"AND {TreeColumns.TPA_UNADJ} > 0"
    )
    if tree_type == "gs":
        where += f" AND {TreeColumns.TREECLCD} = {TreeClass.GROWING_STOCK}"
    trees = _read(
        fia,
        TableNames.TREE,
        [
            TreeColumns.PLT_CN,
            TreeColumns.CONDID,
            TreeColumns.SPCD,
            TreeColumns.DIA,
            TreeColumns.TPA_UNADJ,
            *attributes,
        ],
        where,
        plots,
    )
    return trees.with_columns(
        pl.col(TreeColumns.CONDID).cast(pl.Int64),
        pl.col(TreeColumns.SPCD).cast(pl.Int64),
        pl.col([TreeColumns.DIA, TreeColumns.TPA_UNADJ, *attributes]).cast(pl.Float64),
    )


def _read_breakpoints(fia: FIA, plots: pl.DataFrame | None) -> pl.DataFrame:
    """PLOT.MACRO_BREAKPOINT_DIA, keyed by PLT_CN (null where no macroplot)."""
    breakpoints = _read(
        fia,
        TableNames.PLOT,
        [PlotColumns.CN, PlotColumns.MACRO_BREAKPOINT_DIA],
        None,
        plots,
        plot_key=PlotColumns.CN,
    )
    return breakpoints.select(
        pl.col(PlotColumns.CN).alias(TreeColumns.PLT_CN),
        # Stored as text in some state databases
        pl.col(PlotColumns.MACRO_BREAKPOINT_DIA).cast(pl.Float64, strict=False),
    )


def _read_softwood(fia: FIA) -> pl.DataFrame:
    species = fia._reader.read_table(
        TableNames.REF_SPECIES,
        columns=[RefColumns.SPCD, RefColumns.SFTWD_HRDWD],
        lazy=False,
    )
    return species.select(
        pl.col(RefColumns.SPCD).cast(pl.Int64).alias(TreeColumns.SPCD),
        (pl.col(RefColumns.SFTWD_HRDWD) == _SOFTWOOD).alias("_SOFTWOOD"),
    )


def _condition_metrics(
    trees: pl.DataFrame,
    conditions: pl.DataFrame,
    breakpoints: pl.DataFrame,
    requested: list[str],
) -> pl.DataFrame:
    """Sum tree attributes per acre of condition."""
    dia = pl.col(TreeColumns.DIA)
    footprint = (
        pl.when(dia < DiameterBreakpoints.MICROPLOT_MAX_DIA)
        .then(pl.col(CondColumns.MICRPROP_UNADJ))
        .when(dia >= pl.col(PlotColumns.MACRO_BREAKPOINT_DIA))
        .then(pl.col(CondColumns.MACRPROP_UNADJ))
        .otherwise(pl.col(CondColumns.SUBPPROP_UNADJ))
    )
    # Periodic inventories have no footprint proportions
    proportion = footprint.fill_null(pl.col(CondColumns.CONDPROP_UNADJ))

    weighted = (
        trees.join(conditions, on=_KEYS, how="inner")
        .join(breakpoints, on=TreeColumns.PLT_CN, how="left")
        .with_columns(
            pl.when(proportion > 0)
            .then(pl.col(TreeColumns.TPA_UNADJ) / proportion)
            .alias("_TPA_COND"),
            (BASAL_AREA_FACTOR * dia.pow(2)).alias("_BA_TREE"),
        )
    )

    tpa = pl.col("_TPA_COND")
    ba = pl.col("_BA_TREE") * tpa
    aggregations = [pl.len().alias("N_TREES")]
    if "ba" in requested or "softwood_ba_share" in requested:
        aggregations.append(ba.sum().alias("_BAA"))
    if "tpa" in requested or "qmd" in requested:
        aggregations.append(tpa.sum().alias("_TPA"))
    if "qmd" in requested:
        aggregations.append((dia.pow(2) * tpa).sum().alias("_DIA2"))
    for metric, (column, factor) in _ATTRIBUTE_METRICS.items():
        if metric in requested:
            aggregations.append(
                (pl.col(column).fill_null(0.0) * tpa)
                .sum()
                .mul(factor)
                .alias(METRIC_COLUMNS[metric])
            )
    if "softwood_ba_share" in requested:
        aggregations.append(ba.filter(pl.col("_SOFTWOOD")).sum().alias("_BAA_SOFTWOOD"))

    per_condition = weighted.group_by(_KEYS).agg(aggregations)

    derived = []
    if "ba" in requested:
        derived.append(pl.col("_BAA").alias(METRIC_COLUMNS["ba"]))
    if "tpa" in requested:
        derived.append(pl.col("_TPA").alias(METRIC_COLUMNS["tpa"]))
    if "qmd" in requested:
        derived.append(
            pl.when(pl.col("_TPA") > 0)
            .then((pl.col("_DIA2") / pl.col("_TPA")).sqrt())
            .alias(METRIC_COLUMNS["qmd"])
        )
    if "softwood_ba_share" in requested:
        derived.append(
            pl.when(pl.col("_BAA") > 0)
            .then(pl.col("_BAA_SOFTWOOD") / pl.col("_BAA"))
            .alias(METRIC_COLUMNS["softwood_ba_share"])
        )
    for metric in _ATTRIBUTE_METRICS:
        if metric in requested:
            derived.append(pl.col(METRIC_COLUMNS[metric]))

    ordered = [METRIC_COLUMNS[m] for m in requested]
    return per_condition.select(*_KEYS, "N_TREES", *derived).select(
        *_KEYS, "N_TREES", *ordered
    )
