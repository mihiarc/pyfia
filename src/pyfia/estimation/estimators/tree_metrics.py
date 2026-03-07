"""
Derived tree metrics estimation for FIA data.

Computes TPA-weighted descriptive statistics at the condition or group level.
These are sample-level metrics (not population estimates), so they do not
require expansion factors or variance estimation.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

from ...validation import (
    validate_domain_expression,
    validate_grp_by,
    validate_land_type,
)
from ..base import AggregationResult, BaseEstimator
from ..columns import get_cond_columns as _get_cond_columns
from ..columns import get_tree_columns as _get_tree_columns
from ..utils import ensure_evalid_set, ensure_fia_instance

if TYPE_CHECKING:
    from ...core import FIA

# Metrics that require specific tree columns beyond the base set
METRIC_REQUIRED_COLUMNS: dict[str, list[str]] = {
    "qmd": [],
    "mean_dia": [],
    "mean_height": ["HT"],
    "softwood_prop": ["DRYBIO_BOLE"],
    "sawtimber_prop": [],
    "max_dia": [],
    "stocking": [],
}

VALID_METRICS = set(METRIC_REQUIRED_COLUMNS.keys())


def _build_metric_expressions(
    metrics: list[str],
    sawtimber_threshold: float,
) -> list[pl.Expr]:
    """Build Polars aggregation expressions for requested metrics."""
    exprs: list[pl.Expr] = []

    for metric in metrics:
        if metric == "qmd":
            exprs.append(
                (
                    (pl.col("DIA").cast(pl.Float64).pow(2) * pl.col("TPA_UNADJ")).sum()
                    / pl.col("TPA_UNADJ").sum()
                )
                .sqrt()
                .alias("QMD")
            )
        elif metric == "mean_dia":
            exprs.append(
                (
                    (pl.col("DIA").cast(pl.Float64) * pl.col("TPA_UNADJ")).sum()
                    / pl.col("TPA_UNADJ").sum()
                ).alias("MEAN_DIA")
            )
        elif metric == "mean_height":
            exprs.append(
                (
                    (pl.col("HT").cast(pl.Float64) * pl.col("TPA_UNADJ"))
                    .filter(pl.col("HT").is_not_null())
                    .sum()
                    / pl.col("TPA_UNADJ").filter(pl.col("HT").is_not_null()).sum()
                ).alias("MEAN_HT")
            )
        elif metric == "softwood_prop":
            exprs.append(
                (
                    pl.col("DRYBIO_BOLE").filter(pl.col("SPCD") < 300).sum()
                    / pl.col("DRYBIO_BOLE").sum()
                ).alias("SOFTWOOD_PROP")
            )
        elif metric == "sawtimber_prop":
            exprs.append(
                (
                    pl.col("TPA_UNADJ")
                    .filter(pl.col("DIA").cast(pl.Float64) >= sawtimber_threshold)
                    .sum()
                    / pl.col("TPA_UNADJ").sum()
                ).alias("SAWTIMBER_PROP")
            )
        elif metric == "max_dia":
            exprs.append(pl.col("DIA").cast(pl.Float64).max().alias("MAX_DIA"))
        elif metric == "stocking":
            exprs.append(
                (pl.col("TPA_UNADJ") * (pl.col("DIA").cast(pl.Float64) / 10.0).pow(1.6))
                .sum()
                .alias("STOCKING")
            )

    # Always include diagnostic counts
    exprs.extend(
        [
            pl.n_unique("PLT_CN").alias("N_PLOTS"),
            pl.len().alias("N_TREES"),
        ]
    )

    return exprs


class TreeMetricsEstimator(BaseEstimator):
    """Estimator for TPA-weighted tree metrics.

    Computes sample-level descriptive statistics. Overrides the base
    estimation pipeline to skip expansion factors and variance.
    """

    def __init__(self, db: str | "FIA", config: dict) -> None:
        super().__init__(db, config)

    def get_required_tables(self) -> list[str]:
        return ["TREE", "COND", "PLOT", "POP_PLOT_STRATUM_ASSGN", "POP_STRATUM"]

    def get_tree_columns(self) -> list[str]:
        # Collect extra columns needed by requested metrics
        extra_cols: list[str] = []
        for m in self.config.get("metrics", []):
            for col in METRIC_REQUIRED_COLUMNS.get(m, []):
                if col not in extra_cols:
                    extra_cols.append(col)
        return _get_tree_columns(
            estimator_cols=extra_cols,
            grp_by=self.config.get("grp_by"),
        )

    def get_cond_columns(self) -> list[str]:
        cols = _get_cond_columns(
            land_type=self.config.get("land_type", "forest"),
            grp_by=self.config.get("grp_by"),
        )
        for col in self.config.get("include_cond_attrs") or []:
            if col not in cols:
                cols.append(col)
        return cols

    def calculate_values(self, data: pl.LazyFrame) -> pl.LazyFrame:
        return data  # No pre-calculation needed

    def aggregate_results(self, data: pl.LazyFrame | None) -> AggregationResult:
        """Compute metrics via group_by aggregation."""
        if data is None:
            return AggregationResult(
                results=pl.DataFrame(),
                plot_tree_data=pl.DataFrame(),
                group_cols=[],
            )

        metrics = self.config.get("metrics", [])
        sawtimber_threshold = self.config.get("sawtimber_threshold", 9.0)
        include_cond_attrs = self.config.get("include_cond_attrs") or []

        agg_exprs = _build_metric_expressions(metrics, sawtimber_threshold)

        group_cols = self._setup_grouping()

        # Add include_cond_attrs to grouping (they're carried through as group keys)
        effective_grp = list(group_cols)
        for col in include_cond_attrs:
            if col not in effective_grp:
                effective_grp.append(col)

        if effective_grp:
            # Avoid N_PLOTS conflict when PLT_CN is a grouping column
            if "PLT_CN" in effective_grp:
                agg_exprs = [e for e in agg_exprs if e.meta.output_name() != "N_PLOTS"]
                agg_exprs.append(pl.lit(1).alias("N_PLOTS"))

            result = data.group_by(effective_grp).agg(agg_exprs).collect()
        else:
            result = data.select(agg_exprs).collect()

        # Fill nulls for proportion metrics
        fill_cols = [
            c for c in ("SOFTWOOD_PROP", "SAWTIMBER_PROP") if c in result.columns
        ]
        if fill_cols:
            result = result.with_columns([pl.col(c).fill_null(0.0) for c in fill_cols])

        return AggregationResult(
            results=result,
            plot_tree_data=pl.DataFrame(),
            group_cols=effective_grp,
        )

    def estimate(self) -> pl.DataFrame:
        """Simplified pipeline: load -> filter -> aggregate. No variance."""
        data = self.load_data()
        if data is not None:
            data = self.apply_filters(data)
            data = self.calculate_values(data)
        return self.aggregate_results(data).results


def tree_metrics(
    db: "FIA",
    metrics: list[str],
    grp_by: str | list[str] | None = None,
    land_type: str = "forest",
    tree_type: str = "live",
    tree_domain: str | None = None,
    area_domain: str | None = None,
    sawtimber_threshold: float = 9.0,
    include_cond_attrs: list[str] | None = None,
) -> pl.DataFrame:
    """Compute TPA-weighted tree metrics from FIA data.

    Calculates derived per-condition or per-group tree metrics such as
    quadratic mean diameter (QMD), mean height, and species composition.
    These are sample-level descriptive statistics, not population-level
    estimates -- they do not use expansion factors or variance estimation.

    Parameters
    ----------
    db : FIA
        FIA database connection with EVALID set.
    metrics : list of str
        Metrics to compute. Valid options:

        - ``"qmd"``: Quadratic mean diameter
        - ``"mean_dia"``: Arithmetic mean diameter (TPA-weighted)
        - ``"mean_height"``: Mean tree height (TPA-weighted)
        - ``"softwood_prop"``: Softwood proportion of biomass (SPCD < 300)
        - ``"sawtimber_prop"``: Proportion of TPA above sawtimber threshold
        - ``"max_dia"``: Maximum tree diameter
        - ``"stocking"``: Rough stocking index
    grp_by : str or list of str, optional
        Grouping columns. Supports standard FIA columns (FORTYPCD, STDAGE,
        etc.) and plot-condition level grouping (PLT_CN, CONDID).
    land_type : str, default "forest"
        Land type filter: "forest", "timber", or "all".
    tree_type : str, default "live"
        Tree status filter: "live", "dead", or "gs" (growing stock).
    tree_domain : str, optional
        SQL-like tree filter (e.g., ``"DIA >= 5.0"``).
    area_domain : str, optional
        SQL-like condition filter (e.g., ``"FORTYPCD IN (161, 162)"``).
    sawtimber_threshold : float, default 9.0
        Diameter threshold for sawtimber_prop metric.
    include_cond_attrs : list of str, optional
        COND table columns to pass through in the output (e.g.,
        ``["SLOPE", "SICOND", "ASPECT"]``). Only useful when grouping
        by PLT_CN + CONDID.

    Returns
    -------
    pl.DataFrame
        Metrics with one row per group. Columns include the requested
        metrics plus N_PLOTS and N_TREES counts.

    Examples
    --------
    QMD and mean height by forest type:

    >>> result = tree_metrics(db, metrics=["qmd", "mean_height"], grp_by="FORTYPCD")

    Condition-level metrics for timber valuation:

    >>> result = tree_metrics(
    ...     db,
    ...     metrics=["qmd", "mean_height", "softwood_prop", "sawtimber_prop"],
    ...     grp_by=["PLT_CN", "CONDID", "STDAGE", "FORTYPCD"],
    ...     land_type="timber",
    ...     tree_domain="DIA >= 1.0",
    ...     include_cond_attrs=["SLOPE", "SICOND"],
    ... )
    """
    # Validate inputs
    invalid_metrics = set(metrics) - VALID_METRICS
    if invalid_metrics:
        raise ValueError(
            f"Invalid metric(s): {invalid_metrics}. "
            f"Valid metrics: {sorted(VALID_METRICS)}"
        )
    if not metrics:
        raise ValueError("At least one metric must be specified.")

    land_type = validate_land_type(land_type)
    if grp_by is not None:
        grp_by = validate_grp_by(grp_by)
    if tree_domain is not None:
        tree_domain = validate_domain_expression(tree_domain, "tree_domain")
    if area_domain is not None:
        area_domain = validate_domain_expression(area_domain, "area_domain")

    db, _owns_db = ensure_fia_instance(db)
    ensure_evalid_set(db, eval_type="VOL", estimator_name="tree_metrics")

    config = {
        "metrics": metrics,
        "grp_by": grp_by,
        "land_type": land_type,
        "tree_type": tree_type,
        "tree_domain": tree_domain,
        "area_domain": area_domain,
        "sawtimber_threshold": sawtimber_threshold,
        "include_cond_attrs": include_cond_attrs,
    }

    estimator = TreeMetricsEstimator(db, config)
    return estimator.estimate()
