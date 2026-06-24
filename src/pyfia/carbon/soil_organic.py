"""
Soil organic carbon estimation from FIADB condition-level attributes.

Soil organic carbon (SOC) — the organic carbon stored in mineral soil to
a depth of 1 metre — is estimated in the FIADB using the Domke et al.
(2017) model, which relates SOC density
to soil taxonomic order, clay content, and climate variables.

This estimator reads ``COND.CARBON_SOIL_ORG`` (short tons per acre),
then runs it through pyFIA's post-stratified aggregation pipeline to
produce per-acre and population estimates that match EVALIDator.

Soil organic carbon has no above-ground / below-ground split; the single
``CARBON_SOIL_ORG`` column represents the total pool.

National magnitude: ~20,400 Tg C total SOC on forestland, roughly 52 %
of total forest ecosystem carbon and the single largest pool
(GTR-NRS-154, Table 2).

Public API: :func:`soil_organic`.  See its docstring for parameters,
examples, and the pool semantics.

References
----------
- Domke, G.M.; Perry, C.H.; Walters, B.F.; et al. (2017). Toward
  inventory-based estimates of soil organic carbon in forests of the
  United States. Ecological Applications, 27(4), 1223-1235.
- Woodall, C.W. et al. (2015). GTR-NRS-154 (FCAF methodology blueprint).
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


class SoilOrganicEstimator(BaseEstimator):
    """Soil organic carbon estimator.

    Reads pre-computed ``COND.CARBON_SOIL_ORG`` from the FIADB and
    aggregates via the standard post-stratified estimation pipeline.

    This is a **condition-level** estimator — there is no tree-level data.
    The TREE table is not loaded; data comes from ``COND x PLOT`` only.
    The adjustment factor is ``ADJ_FACTOR_SUBP`` (subplot-level),
    matching FIADB/EVALIDator conventions for condition-level carbon
    attributes.
    """

    _estimator_label = "SoilOrganic"

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
        if "CARBON_SOIL_ORG" not in cols:
            cols.append("CARBON_SOIL_ORG")
        return cols

    # ------------------------------------------------------------------
    # Override apply_filters for condition-level data
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
    # Core estimation logic
    # ------------------------------------------------------------------

    def calculate_values(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """Alias CARBON_SOIL_ORG to CARBON_ACRE."""
        cond_carbon = pl.col("CARBON_SOIL_ORG").cast(pl.Float64).fill_null(0.0)
        return data.with_columns(cond_carbon.alias("CARBON_ACRE"))

    # ------------------------------------------------------------------
    # Aggregation — condition-level with ADJ_FACTOR_SUBP
    # ------------------------------------------------------------------

    def aggregate_results(self, data: pl.LazyFrame | None) -> AggregationResult:
        if data is None:
            return AggregationResult(
                results=pl.DataFrame(),
                plot_tree_data=pl.DataFrame(),
                group_cols=[],
            )

        validate_required_columns(
            data, ["PLT_CN", "CARBON_ACRE"], "soil organic carbon data"
        )

        strat_data = self._get_stratification_data()
        data_with_strat = data.join(strat_data, on="PLT_CN", how="inner")

        # Condition-level attribute: CARBON_ACRE is a density (tons/acre).
        # The two-stage pipeline denominator is sum(CONDPROP_UNADJ * EXPNS),
        # so the numerator must include CONDPROP_UNADJ to give the
        # condition's contribution proportional to its area on the plot.
        # ADJ_FACTOR_SUBP corrects for nonresponse at the subplot level.
        data_with_strat = data_with_strat.with_columns(
            pl.col("ADJ_FACTOR_SUBP").cast(pl.Float64).alias("ADJ_FACTOR")
        )

        data_with_strat = data_with_strat.with_columns(
            (
                pl.col("CARBON_ACRE")
                * pl.col("CONDPROP_UNADJ").cast(pl.Float64)
                * pl.col("ADJ_FACTOR")
            ).alias("CARBON_ADJ")
        )

        group_cols = self._setup_grouping()

        plot_tree_data, data_with_strat = self._preserve_plot_tree_data(
            data_with_strat,
            metric_cols=["CARBON_ADJ"],
            group_cols=group_cols,
        )

        results = self._apply_two_stage_aggregation(
            data_with_strat=data_with_strat,
            metric_mappings={"CARBON_ADJ": "CONDITION_CARBON"},
            group_cols=group_cols,
            use_grm_adjustment=False,
        )

        if not self.config.get("totals", True):
            if "CARBON_TOTAL" in results.columns:
                results = results.drop("CARBON_TOTAL")

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
                "adjusted_col": "CARBON_ADJ",
                "acre_se_col": "CARBON_ACRE_SE",
                "total_se_col": "CARBON_TOTAL_SE",
            },
        ]
        return self._calculate_variance_for_metrics(agg_result, metric_configs)

    def format_output(self, results: pl.DataFrame) -> pl.DataFrame:
        year = self._extract_evaluation_year()
        results = results.with_columns(pl.lit(year).alias("YEAR"))
        results = results.with_columns(pl.lit("TOTAL").alias("POOL"))

        col_order = [
            "YEAR",
            "POOL",
            "CARBON_ACRE",
            "CARBON_TOTAL",
            "CARBON_ACRE_SE",
            "CARBON_TOTAL_SE",
            "N_PLOTS",
            "N_TREES",
        ]

        for col in results.columns:
            if col not in col_order:
                col_order.insert(1, col)

        final_cols = [col for col in col_order if col in results.columns]
        return results.select(final_cols)


# ======================================================================
# Public API
# ======================================================================


def soil_organic(
    db: str | FIA,
    pool: str = "total",
    grp_by: str | list[str] | None = None,
    land_type: str = "forest",
    area_domain: str | None = None,
    plot_domain: str | None = None,
    totals: bool = True,
    variance: bool = False,
    most_recent: bool = False,
) -> pl.DataFrame:
    """
    Estimate soil organic carbon from FIA data.

    Soil organic carbon (SOC) encompasses all organic carbon stored in
    mineral soil to a depth of 1 metre on forestland.  Carbon density is
    estimated from the Domke et al. (2017) model, parameterised on soil
    taxonomic order, clay content, and climate variables.  The FIADB
    pre-computes condition-level carbon density and stores it in
    ``COND.CARBON_SOIL_ORG``.

    Nationally, SOC totals ~20,400 Tg C, representing ~52 % of total
    forest ecosystem carbon — the single largest pool (GTR-NRS-154,
    Table 2).  Per-acre values are typically 15-40 short tons/acre
    depending on soil type, climate, and region.

    Parameters
    ----------
    db : str | FIA
        Database connection or path to FIA database. Can be either a path
        string to a DuckDB/SQLite file or an existing FIA connection object.
    pool : {'total'}, default 'total'
        Carbon pool to estimate.  Soil organic carbon has no above-ground /
        below-ground split — only ``'total'`` is accepted.
    grp_by : str or list of str, optional
        Column name(s) to group results by. Can be any column from the
        FIA COND or PLOT tables. Common grouping columns include:

        - 'FORTYPCD': Forest type code
        - 'FORTYPGRPCD': Forest type group code
        - 'OWNGRPCD': Ownership group
        - 'STATECD': State FIPS code
        - 'COUNTYCD': County code

        For complete column descriptions, see USDA FIA Database User Guide.
    land_type : {'forest', 'timber', 'all'}, default 'forest'
        Land type to include in estimation:

        - 'forest': All forestland
        - 'timber': Productive timberland only (unreserved, productive)
        - 'all': All land conditions
    area_domain : str, optional
        SQL-like filter expression for area/condition-level filtering.
        Example: ``"OWNGRPCD == 40 AND FORTYPCD == 161"``.
    plot_domain : str, optional
        SQL-like filter expression for plot-level filtering.
    totals : bool, default True
        If True, include population-level total estimates in addition to
        per-acre values.
    variance : bool, default False
        If True, calculate and include variance and standard error
        estimates following Bechtold & Patterson (2005).
    most_recent : bool, default False
        If True, automatically filter to the most recent EXPVOL evaluation
        for each state in the database before estimation.

    Returns
    -------
    pl.DataFrame
        Soil organic carbon estimates with the following columns:

        - **YEAR** : int
            Evaluation reference year from EVALID.
        - **POOL** : str
            Pool identifier — always ``'TOTAL'``.
        - **CARBON_ACRE** : float
            Carbon per acre in short tons.
        - **CARBON_TOTAL** : float (if ``totals=True``)
            Total carbon in short tons expanded to population level.
        - **CARBON_ACRE_SE** : float (if ``variance=True``)
            Standard error of the per-acre estimate.
        - **CARBON_TOTAL_SE** : float (if ``variance=True`` and ``totals=True``)
            Standard error of the population total.
        - **N_PLOTS** : int
            Number of FIA plots included in the estimation.
        - **N_TREES** : int
            Number of tree records (used for distribution weighting).
        - **[grouping columns]** : various
            Any columns specified in ``grp_by``.

    See Also
    --------
    live_tree : Estimate live tree carbon using the NSVB framework.
    standing_dead : Estimate standing dead tree carbon.
    understory : Estimate understory vegetation carbon.
    downed_dead : Estimate downed dead wood carbon.
    litter : Estimate litter carbon.
    pyfia.carbon : Overview of all carbon pool estimators.

    Notes
    -----
    **Methodology**

    Soil organic carbon is not directly measured on FIA plots.  Instead,
    the FIADB pre-computes carbon density per condition using the Domke
    et al. (2017) model, which combines gridded soil survey data
    (gNATSGO/SSURGO) with FIA plot locations to assign SOC densities
    based on soil taxonomic order, clay content, and climate variables.

    This estimator reads those pre-computed values directly from the COND
    table and runs them through the standard post-stratified aggregation
    pipeline, ensuring exact agreement with EVALIDator estimates.

    **No AG/BG split**

    Soil organic carbon has no above-ground / below-ground partitioning.
    The ``CARBON_SOIL_ORG`` column represents the entire pool (mineral
    soil to 1 m depth).

    Examples
    --------
    Total soil organic carbon per acre on forestland:

    >>> results = soil_organic(db, pool="total")
    >>> print(f"Carbon: {results['CARBON_ACRE'][0]:.3f} tons/acre")

    Soil organic carbon by forest type group:

    >>> results = soil_organic(db, pool="total", grp_by="FORTYPGRPCD")

    Soil organic carbon on timberland with standard errors:

    >>> results = soil_organic(
    ...     db,
    ...     land_type="timber",
    ...     variance=True,
    ... )

    References
    ----------
    .. [1] Domke, G.M.; Perry, C.H.; Walters, B.F.; et al. (2017).
       Toward inventory-based estimates of soil organic carbon in forests
       of the United States. Ecological Applications, 27(4), 1223-1235.
    .. [2] Woodall, C.W. et al. (2015). The current and future role of
       forest carbon in the United States. Gen. Tech. Rep. NRS-154.
    """
    from ..validation import (
        validate_boolean,
        validate_domain_expression,
        validate_grp_by,
        validate_land_type,
    )

    # ----- Validate pool -----
    pool = pool.lower()
    if pool != "total":
        raise ValueError(
            f"Invalid pool '{pool}' for soil organic carbon. "
            f"Only 'total' is supported — soil organic carbon has no AG/BG split."
        )

    # ----- Validate standard inputs -----
    land_type = validate_land_type(land_type)
    grp_by = validate_grp_by(grp_by)
    area_domain = validate_domain_expression(area_domain, "area_domain")
    plot_domain = validate_domain_expression(plot_domain, "plot_domain")
    totals = validate_boolean(totals, "totals")
    variance = validate_boolean(variance, "variance")
    most_recent = validate_boolean(most_recent, "most_recent")

    # ----- Resolve db + EVALID -----
    db, owns_db = ensure_fia_instance(db)
    if most_recent and db.evalid is None:
        db.clip_most_recent(eval_type="VOL")
    else:
        ensure_evalid_set(db, eval_type="VOL", estimator_name="soil_organic")

    # ----- Build config and run estimator -----
    config = {
        "pool": pool,
        "grp_by": grp_by,
        "by_species": False,
        "by_size_class": False,
        "land_type": land_type,
        "area_domain": area_domain,
        "plot_domain": plot_domain,
        "totals": totals,
        "variance": variance,
        "most_recent": most_recent,
    }

    try:
        estimator = SoilOrganicEstimator(db, config)
        return estimator.estimate()
    finally:
        if owns_db and hasattr(db, "close"):
            db.close()
