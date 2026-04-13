"""
Carbon stock-change estimation for condition-level FIADB carbon pools.

Computes the change in carbon stocks between two inventory periods for
the four condition-level pools: understory vegetation, downed dead wood,
litter, and soil organic carbon.  Stock change is calculated as
``C(t₂) − C(t₁)`` per remeasured condition, optionally annualized by
the remeasurement period (REMPER), then aggregated via the standard
post-stratified estimation pipeline using t₂'s stratification.

This module follows the ``AreaChangeEstimator`` pattern: t₂ data comes
from the EVALID-scoped pipeline; t₁ data is loaded from the full
(unfiltered) COND table and linked via ``PREV_PLT_CN + PREVCOND``.

Tree-level stock change (live tree, standing dead) requires GRM fate
decomposition and is deferred to Phase B.

Public API: :func:`stock_change`.

References
----------
- Bechtold, W.A. & Patterson, P.L. (2005). GTR-SRS-80, Chapter 4:
  Change Estimation.
- Woodall, C.W. et al. (2015). GTR-NRS-154 (FCAF methodology).
"""

from __future__ import annotations

import logging

import polars as pl

from ..core import FIA
from ..estimation.base import AggregationResult, BaseEstimator
from ..estimation.columns import get_cond_columns as _get_cond_columns
from ..estimation.utils import (
    ensure_evalid_set,
    ensure_fia_instance,
    validate_aggregation_result,
    validate_required_columns,
)

logger = logging.getLogger(__name__)

# Pool name → COND column(s) for the carbon density attribute.
# Understory has AG + BG; the other three have a single total column.
_POOL_COLUMNS: dict[str, list[str]] = {
    "understory": ["CARBON_UNDERSTORY_AG", "CARBON_UNDERSTORY_BG"],
    "downed_dead": ["CARBON_DOWN_DEAD"],
    "litter": ["CARBON_LITTER"],
    "soil_organic": ["CARBON_SOIL_ORG"],
}

_VALID_POOLS = set(_POOL_COLUMNS.keys())


class CarbonStockChangeEstimator(BaseEstimator):
    """Condition-level carbon stock-change estimator.

    Computes ``C(t₂) − C(t₁)`` per remeasured condition for one
    condition-level carbon pool, then aggregates via the standard
    post-stratified two-stage pipeline.

    Follows the ``AreaChangeEstimator`` pattern:

    * t₂ data is loaded through the standard EVALID-scoped pipeline.
    * t₁ data is loaded from the **full** COND table (unfiltered by
      EVALID) via ``db._reader.read_table()``, then joined to t₂ via
      ``PREV_PLT_CN + PREVCOND``.
    * Stratification (``POP_PLOT_STRATUM_ASSGN``, ``POP_STRATUM``) is
      always from t₂'s EVALID.
    """

    _estimator_label = "CarbonStockChange"

    # ------------------------------------------------------------------
    # Table / column requirements
    # ------------------------------------------------------------------

    def get_required_tables(self) -> list[str]:
        return ["COND", "PLOT", "POP_PLOT_STRATUM_ASSGN", "POP_STRATUM"]

    def get_tree_columns(self) -> list[str]:
        return []

    def get_cond_columns(self) -> list[str]:
        cols = _get_cond_columns(
            land_type=self.config.get("land_type", "forest"),
            grp_by=self.config.get("grp_by"),
            include_prop_basis=False,
        )
        # Add the carbon column(s) for this pool
        pool = self.config["pool"]
        for c in _POOL_COLUMNS[pool]:
            if c not in cols:
                cols.append(c)
        return cols

    # ------------------------------------------------------------------
    # load_data — the key override (area_change.py pattern)
    # ------------------------------------------------------------------

    def load_data(self) -> pl.LazyFrame | None:
        """Load t₂ and t₁ condition data and link via PREV_PLT_CN.

        Join sequence:
        1. COND (t₂) — EVALID-scoped via standard pipeline
        2. PLOT (t₂) — EVALID-scoped; filter to PREV_PLT_CN IS NOT NULL
        3. COND (t₁) — **full table** loaded via ``db._reader.read_table``
        4. Stratification data — from t₂'s EVALID
        """
        pool = self.config["pool"]
        carbon_cols = _POOL_COLUMNS[pool]

        # --- Load EVALID-scoped t2 tables ---
        for table in self.get_required_tables():
            if table not in self.db.tables:
                self.db.load_table(table)

        cond = self.db.tables["COND"]
        if not isinstance(cond, pl.LazyFrame):
            cond = cond.lazy()

        # Select the columns we need from COND (t2)
        cond_cols = self.get_cond_columns()
        available = cond.collect_schema().names()
        cond_select = [c for c in cond_cols if c in available]
        cond_t2 = cond.select(cond_select)

        # --- Load PLOT for REMPER and PREV_PLT_CN ---
        plot = self.db.tables["PLOT"]
        if not isinstance(plot, pl.LazyFrame):
            plot = plot.lazy()

        plot_cols = ["CN", "STATECD", "INVYR", "REMPER", "PREV_PLT_CN"]
        plot_available = plot.collect_schema().names()
        plot_cols = [c for c in plot_cols if c in plot_available]
        plot = plot.select(plot_cols)

        # Join t2 COND with PLOT
        data = cond_t2.join(
            plot,
            left_on="PLT_CN",
            right_on="CN",
            how="inner",
        )

        # Filter to remeasured plots only
        data = data.filter(
            pl.col("PREV_PLT_CN").is_not_null() & pl.col("REMPER").is_not_null()
            & (pl.col("REMPER") > 0)
        )

        # --- Load FULL COND table (unfiltered) for t1 ---
        # IMPORTANT: PREV_PLT_CN references plots from previous inventory
        # cycles that are outside the current EVALID scope.
        t1_cols = ["PLT_CN", "CONDID"] + carbon_cols
        cond_prev = self.db._reader.read_table(
            "COND",
            columns=t1_cols,
            lazy=True,
        )

        # Add a sentinel column to detect successful joins (vs NULL carbon)
        cond_prev = cond_prev.with_columns(pl.lit(True).alias("_t1_matched"))

        # Rename t1 carbon columns with t1_ prefix
        t1_rename: dict[str, str] = {"PLT_CN": "t1_PLT_CN", "CONDID": "t1_CONDID"}
        for c in carbon_cols:
            t1_rename[c] = f"t1_{c}"
        cond_prev = cond_prev.rename(t1_rename)

        # Rename t2 carbon columns with t2_ prefix for clarity
        t2_rename: dict[str, str] = {}
        for c in carbon_cols:
            if c in data.collect_schema().names():
                t2_rename[c] = f"t2_{c}"
        if t2_rename:
            data = data.rename(t2_rename)

        # Join t1 conditions via PREV_PLT_CN + matching CONDID.
        # CONDID is assigned per condition per plot visit; most conditions
        # retain the same CONDID across remeasurements (~97% in Georgia).
        # The more precise PREVCOND mapping lives in SUBP_COND_CHNG_MTRX
        # (subplot-level), which is overkill for condition-level carbon.
        data = data.join(
            cond_prev,
            left_on=["PREV_PLT_CN", "CONDID"],
            right_on=["t1_PLT_CN", "t1_CONDID"],
            how="left",
        )

        # Filter to conditions where t1 match was found (LEFT JOIN hit).
        # The _t1_matched sentinel distinguishes "no match" (NULL) from
        # "match but NULL carbon value" (which fill_null handles later).
        data = data.filter(pl.col("_t1_matched").is_not_null())
        data = data.drop("_t1_matched")

        # --- Join stratification data (from t2's EVALID) ---
        strat_data = self._get_stratification_data()
        data = data.join(strat_data, on="PLT_CN", how="inner")

        return data

    # ------------------------------------------------------------------
    # apply_filters — condition/area filters on t2 data
    # ------------------------------------------------------------------

    def apply_filters(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """Apply condition/area filters only (no tree-type filtering)."""
        from ..filtering import apply_area_filters, get_land_domain_indicator

        columns = data.collect_schema().names()

        if self.config.get("area_domain"):
            data = apply_area_filters(data, area_domain=self.config["area_domain"])

        land_type = self.config.get("land_type", "forest")
        if land_type and land_type != "all" and "COND_STATUS_CD" in columns:
            data = data.filter(get_land_domain_indicator(land_type))

        return data

    # ------------------------------------------------------------------
    # calculate_values — compute delta per condition
    # ------------------------------------------------------------------

    def calculate_values(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """Compute ``C(t₂) − C(t₁)`` and optionally annualize by REMPER."""
        pool = self.config["pool"]
        carbon_cols = _POOL_COLUMNS[pool]
        annualize = self.config.get("annualize", True)

        # Sum all carbon columns for this pool (understory has AG+BG)
        t2_expr = pl.lit(0.0)
        t1_expr = pl.lit(0.0)
        for c in carbon_cols:
            t2_expr = t2_expr + pl.col(f"t2_{c}").cast(pl.Float64).fill_null(0.0)
            t1_expr = t1_expr + pl.col(f"t1_{c}").cast(pl.Float64).fill_null(0.0)

        delta = t2_expr - t1_expr

        if annualize:
            delta = delta / pl.col("REMPER").cast(pl.Float64)

        return data.with_columns(delta.alias("CARBON_CHANGE_ACRE"))

    # ------------------------------------------------------------------
    # aggregate_results — condition-level with ADJ_FACTOR_SUBP
    # ------------------------------------------------------------------

    def aggregate_results(self, data: pl.LazyFrame | None) -> AggregationResult:
        if data is None:
            return AggregationResult(
                results=pl.DataFrame(),
                plot_tree_data=pl.DataFrame(),
                group_cols=[],
            )

        validate_required_columns(
            data, ["PLT_CN", "CARBON_CHANGE_ACRE"], "carbon stock-change data"
        )

        # Condition-level attribute: CARBON_CHANGE_ACRE is a density
        # (tons/acre/year). The two-stage pipeline denominator is
        # sum(CONDPROP_UNADJ * EXPNS), so the numerator must include
        # CONDPROP_UNADJ to give the condition's contribution proportional
        # to its area on the plot. ADJ_FACTOR_SUBP corrects for nonresponse.
        data = data.with_columns(
            pl.col("ADJ_FACTOR_SUBP").cast(pl.Float64).alias("ADJ_FACTOR")
        )

        data = data.with_columns(
            (
                pl.col("CARBON_CHANGE_ACRE")
                * pl.col("CONDPROP_UNADJ").cast(pl.Float64)
                * pl.col("ADJ_FACTOR")
            ).alias("CARBON_CHANGE_ADJ")
        )

        group_cols = self._setup_grouping()

        plot_tree_data, data = self._preserve_plot_tree_data(
            data,
            metric_cols=["CARBON_CHANGE_ADJ"],
            group_cols=group_cols,
        )

        results = self._apply_two_stage_aggregation(
            data_with_strat=data,
            metric_mappings={"CARBON_CHANGE_ADJ": "CONDITION_CARBON_CHANGE"},
            group_cols=group_cols,
            use_grm_adjustment=False,
        )

        if not self.config.get("totals", True):
            if "CARBON_CHANGE_TOTAL" in results.columns:
                results = results.drop("CARBON_CHANGE_TOTAL")

        return AggregationResult(
            results=results,
            plot_tree_data=plot_tree_data,
            group_cols=group_cols,
        )

    # ------------------------------------------------------------------
    # Variance and output formatting
    # ------------------------------------------------------------------

    def calculate_variance(self, agg_result: AggregationResult) -> pl.DataFrame:
        validate_aggregation_result(agg_result, self._estimator_label)
        metric_configs = [
            {
                "adjusted_col": "CARBON_CHANGE_ADJ",
                "acre_se_col": "CARBON_CHANGE_ACRE_SE",
                "total_se_col": "CARBON_CHANGE_TOTAL_SE",
            },
        ]
        return self._calculate_variance_for_metrics(agg_result, metric_configs)

    def format_output(self, results: pl.DataFrame) -> pl.DataFrame:
        year = self._extract_evaluation_year()
        results = results.with_columns(pl.lit(year).alias("YEAR"))

        pool = self.config["pool"].upper()
        results = results.with_columns(pl.lit(pool).alias("POOL"))

        col_order = [
            "YEAR",
            "POOL",
            "CARBON_CHANGE_ACRE",
            "CARBON_CHANGE_TOTAL",
            "CARBON_CHANGE_ACRE_SE",
            "CARBON_CHANGE_TOTAL_SE",
            "N_PLOTS",
            "N_TREES",
        ]

        for col in results.columns:
            if col not in col_order:
                col_order.insert(2, col)

        final_cols = [col for col in col_order if col in results.columns]
        return results.select(final_cols)


# ======================================================================
# Public API
# ======================================================================


def stock_change(
    db: str | FIA,
    pool: str | list[str] = "all",
    grp_by: str | list[str] | None = None,
    land_type: str = "forest",
    area_domain: str | None = None,
    annualize: bool = True,
    totals: bool = True,
    variance: bool = False,
    most_recent: bool = False,
) -> pl.DataFrame:
    """
    Estimate carbon stock change between two inventory periods.

    Computes the change in carbon stocks for condition-level pools
    (understory, downed dead wood, litter, soil organic carbon) between
    the current evaluation and the previous measurement on remeasured
    plots.  The t₂ evaluation is the EVALID set on the database; t₁ is
    found automatically via ``PLOT.PREV_PLT_CN``.

    Stock change is calculated as ``C(t₂) − C(t₁)`` per remeasured
    condition, optionally annualized by dividing by the remeasurement
    period (REMPER).  Positive values indicate carbon accumulation
    (sequestration); negative values indicate carbon loss.

    Parameters
    ----------
    db : str | FIA
        Database connection or path to FIA database.
    pool : str or list of str, default 'all'
        Carbon pool(s) to estimate stock change for.  Accepts:

        - ``'understory'``: Understory vegetation (AG + BG)
        - ``'downed_dead'``: Downed dead wood
        - ``'litter'``: Litter and duff
        - ``'soil_organic'``: Soil organic carbon
        - ``'all'``: All four condition-level pools (default)

        Tree-level pools (``'live_tree'``, ``'standing_dead'``) are not
        yet supported and will raise ``ValueError``.
    grp_by : str or list of str, optional
        Column name(s) to group results by.
    land_type : {'forest', 'timber', 'all'}, default 'forest'
        Land type to include in estimation.
    area_domain : str, optional
        SQL-like filter expression for condition-level filtering.
    annualize : bool, default True
        If True, divide stock change by the remeasurement period
        (REMPER) to produce annual change rates (tons/acre/year).
        If False, report total change over the remeasurement period.
    totals : bool, default True
        If True, include population-level total estimates.
    variance : bool, default False
        If True, calculate standard error estimates.
    most_recent : bool, default False
        If True, automatically filter to the most recent evaluation.

    Returns
    -------
    pl.DataFrame
        Stock-change estimates with columns:

        - **YEAR** : int — Evaluation reference year (t₂).
        - **POOL** : str — Pool identifier.
        - **CARBON_CHANGE_ACRE** : float — Change per acre (tons/acre
          or tons/acre/year if annualized).
        - **CARBON_CHANGE_TOTAL** : float — Population total change
          (if ``totals=True``).
        - **CARBON_CHANGE_ACRE_SE** / **CARBON_CHANGE_TOTAL_SE** :
          float — Standard errors (if ``variance=True``).
        - **N_PLOTS** : int — Number of remeasured plots.

    See Also
    --------
    downed_dead : Estimate downed dead wood carbon stocks.
    litter : Estimate litter carbon stocks.
    soil_organic : Estimate soil organic carbon stocks.
    understory : Estimate understory vegetation carbon stocks.
    total_ecosystem : Estimate total ecosystem carbon stocks.

    Notes
    -----
    **Methodology**

    Stock change follows Bechtold & Patterson (2005), Chapter 4.  For
    each remeasured condition, the change in carbon density is computed
    from pre-computed COND attributes.  The delta is annualized by
    REMPER (typically 5–7 years), adjusted by ``CONDPROP_UNADJ ×
    ADJ_FACTOR_SUBP``, and aggregated via the two-stage post-stratified
    pipeline using t₂'s stratification.

    Only remeasured plots (``PREV_PLT_CN IS NOT NULL AND REMPER > 0``)
    contribute.  The t₁ conditions are loaded from the full COND table
    (unfiltered by EVALID) and linked via ``PREV_PLT_CN + PREVCOND``.

    **Tree-level pools**

    Live tree and standing dead stock change require GRM fate
    decomposition and are not yet implemented.

    Examples
    --------
    Annual stock change for all condition-level pools:

    >>> results = stock_change(db)

    Downed dead wood stock change by ownership:

    >>> results = stock_change(db, pool="downed_dead", grp_by="OWNGRPCD")

    Non-annualized litter change with standard errors:

    >>> results = stock_change(db, pool="litter", annualize=False, variance=True)

    References
    ----------
    .. [1] Bechtold, W.A. & Patterson, P.L. (2005). The Enhanced Forest
       Inventory and Analysis Program. GTR-SRS-80, Chapter 4.
    .. [2] Woodall, C.W. et al. (2015). GTR-NRS-154.
    """
    from ..validation import (
        validate_boolean,
        validate_domain_expression,
        validate_grp_by,
        validate_land_type,
    )

    # ----- Validate pool -----
    tree_pools = {"live_tree", "standing_dead"}
    if isinstance(pool, str):
        pool_lower = pool.lower()
        if pool_lower in tree_pools:
            raise ValueError(
                f"Tree-level stock change for '{pool}' is not yet implemented. "
                f"Supported pools: {sorted(_VALID_POOLS)} or 'all'."
            )
        if pool_lower == "all":
            pools = sorted(_VALID_POOLS)
        elif pool_lower in _VALID_POOLS:
            pools = [pool_lower]
        else:
            raise ValueError(
                f"Invalid pool '{pool}'. "
                f"Must be one of: {sorted(_VALID_POOLS | {'all'})}"
            )
    elif isinstance(pool, list):
        pools = []
        for p in pool:
            p_lower = p.lower()
            if p_lower in tree_pools:
                raise ValueError(
                    f"Tree-level stock change for '{p}' is not yet implemented."
                )
            if p_lower not in _VALID_POOLS:
                raise ValueError(
                    f"Invalid pool '{p}'. Must be one of: {sorted(_VALID_POOLS)}"
                )
            pools.append(p_lower)
    else:
        raise TypeError(f"pool must be str or list[str], got {type(pool).__name__}")

    # ----- Validate standard inputs -----
    land_type = validate_land_type(land_type)
    grp_by = validate_grp_by(grp_by)
    area_domain = validate_domain_expression(area_domain, "area_domain")
    annualize = validate_boolean(annualize, "annualize")
    totals = validate_boolean(totals, "totals")
    variance = validate_boolean(variance, "variance")
    most_recent = validate_boolean(most_recent, "most_recent")

    # ----- Resolve db + EVALID -----
    db, owns_db = ensure_fia_instance(db)
    if most_recent and db.evalid is None:
        db.clip_most_recent(eval_type="VOL")
    else:
        ensure_evalid_set(db, eval_type="VOL", estimator_name="stock_change")

    try:
        results = []
        for p in pools:
            config = {
                "pool": p,
                "grp_by": grp_by,
                "by_species": False,
                "by_size_class": False,
                "land_type": land_type,
                "area_domain": area_domain,
                "annualize": annualize,
                "totals": totals,
                "variance": variance,
            }
            estimator = CarbonStockChangeEstimator(db, config)
            results.append(estimator.estimate())

        if len(results) == 1:
            return results[0]

        # Stack multi-pool results
        return pl.concat(results, how="diagonal_relaxed")

    finally:
        if owns_db and hasattr(db, "close"):
            db.close()
