"""
Shared base class for NSVB carbon pool estimators.

Extracts the ~200 lines of duplicated infrastructure that LiveTreeEstimator
and StandingDeadEstimator share verbatim: reference-table loading
(_load_ref_species, _load_plotgeom), the two-stage aggregation pipeline,
variance calculation, and output formatting. Pool-specific logic
(calculate_values, apply_filters, get_tree_columns) stays in the subclasses.
"""

from __future__ import annotations

import logging

import polars as pl

from ..core import FIA
from ..estimation.base import AggregationResult, BaseEstimator
from ..estimation.columns import get_cond_columns as _get_cond_columns
from ..estimation.constants import LBS_TO_SHORT_TONS
from ..estimation.tree_expansion import apply_tree_adjustment_factors
from ..estimation.utils import (
    validate_aggregation_result,
    validate_required_columns,
)
from .nsvb.coefficients import ecosubcd_to_division_expr

logger = logging.getLogger(__name__)


class CarbonEstimatorBase(BaseEstimator):
    """Shared infrastructure for NSVB carbon pool estimators.

    Subclasses must implement :meth:`get_tree_columns` and
    :meth:`calculate_values`. Everything else — table requirements,
    condition columns, reference-table loading, aggregation, variance,
    and output formatting — is identical across pools.

    Class attributes that subclasses should set:

    - ``_estimator_label``: human-readable name for validation messages
      (e.g., ``"live_tree"``, ``"standing_dead"``).
    """

    _estimator_label: str = "carbon"

    def __init__(self, db: str | FIA, config: dict) -> None:
        super().__init__(db, config)
        self._plotgeom_cache: pl.DataFrame | None = None

    # ------------------------------------------------------------------
    # Table / column requirements
    # ------------------------------------------------------------------

    def get_required_tables(self) -> list[str]:
        return ["TREE", "COND", "PLOT", "POP_PLOT_STRATUM_ASSGN", "POP_STRATUM"]

    def get_cond_columns(self) -> list[str]:
        return _get_cond_columns(
            land_type=self.config.get("land_type", "forest"),
            grp_by=self.config.get("grp_by"),
            include_prop_basis=False,
        )

    # ------------------------------------------------------------------
    # Reference-table helpers
    # ------------------------------------------------------------------

    def _load_ref_species(self) -> pl.DataFrame:
        """Load REF_SPECIES columns needed by the NSVB pipeline.

        Returns ``(SPCD Int64, JENKINS_SPGRPCD Int64, WDSG Float64,
        WOODLAND Utf8)``. Cached on the instance for the duration of one
        estimator run.

        ``WOODLAND`` ('Y'/'N') flags species measured at diameter at root
        collar that NSVB does not model (GTR-WO-104 p. 6); the live-tree
        estimator uses it to route those trees to FIADB-stored carbon
        rather than letting them recompute to 0 (issue #6).
        """
        if self._ref_species_cache is not None:
            return self._ref_species_cache
        df = self.db._reader.read_table(
            "REF_SPECIES",
            columns=[
                "SPCD",
                "JENKINS_SPGRPCD",
                "WOOD_SPGR_GREENVOL_DRYWT",
                "WOODLAND",
            ],
        )
        if hasattr(df, "collect"):
            df = df.collect()
        df = df.with_columns(
            [
                pl.col("SPCD").cast(pl.Int64),
                pl.col("JENKINS_SPGRPCD").cast(pl.Int64),
                pl.col("WOOD_SPGR_GREENVOL_DRYWT").cast(pl.Float64).alias("WDSG"),
                pl.col("WOODLAND")
                .cast(pl.Utf8)
                .str.strip_chars()
                .str.to_uppercase()
                .fill_null("N")
                .alias("WOODLAND"),
            ]
        ).select(["SPCD", "JENKINS_SPGRPCD", "WDSG", "WOODLAND"])
        self._ref_species_cache = df
        return df

    def _load_plotgeom(self) -> pl.DataFrame | None:
        """Load ``PLOTGEOM.ECOSUBCD`` for the DIVISION / ECOPROV lookups.

        Returns ``(PLT_CN, ECOSUBCD)`` or ``None`` when the table is
        missing.  Negative result cached as an empty DataFrame sentinel.
        """
        if self._plotgeom_cache is not None:
            return self._plotgeom_cache if self._plotgeom_cache.height > 0 else None

        try:
            df = self.db._reader.read_table(
                "PLOTGEOM",
                columns=["CN", "ECOSUBCD"],
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "PLOTGEOM not available (%s) — DIVISION lookup disabled, "
                "falling back to species-level + Jenkins coefficient "
                "precedence (~3%% high biomass bias on growing-stock trees).",
                exc,
            )
            self._plotgeom_cache = pl.DataFrame()
            return None

        if hasattr(df, "collect"):
            df = df.collect()
        df = df.select(
            [
                pl.col("CN").alias("PLT_CN"),
                pl.col("ECOSUBCD"),
            ]
        ).unique(subset=["PLT_CN"])
        self._plotgeom_cache = df
        return df

    # ------------------------------------------------------------------
    # Shared helpers used inside calculate_values
    # ------------------------------------------------------------------

    def _join_ref_species(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """Normalize SPCD dtype and join REF_SPECIES for WDSG + JENKINS_SPGRPCD."""
        data = data.with_columns(pl.col("SPCD").cast(pl.Int64))
        ref_species = self._load_ref_species()
        return data.join(ref_species.lazy(), on="SPCD", how="left")

    def _join_plotgeom_division(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """Join PLOTGEOM and derive DIVISION from ECOSUBCD.

        Adds both ``ECOSUBCD`` and ``DIVISION`` columns when PLOTGEOM is
        available; otherwise returns the frame unchanged.
        """
        plotgeom = self._load_plotgeom()
        if plotgeom is None:
            return data
        data = data.join(plotgeom.lazy(), on="PLT_CN", how="left")
        data = data.with_columns(
            ecosubcd_to_division_expr("ECOSUBCD").alias("DIVISION")
        )
        return data

    def _apply_bg_bridge(self, data: pl.LazyFrame, pool: str) -> pl.LazyFrame:
        """Add ``_CARBON_BG_LB`` column from the FIADB BG bridge.

        For ``pool in ('bg', 'total')``, reads ``TREE.CARBON_BG`` directly
        (in lb per tree).  For ``pool='ag'``, zeroes out the BG
        contribution.  BG bridge is in pounds per tree — the same units as
        ``_CARBON_AG_LB``.
        """
        if pool in ("bg", "total"):
            # TREE.CARBON_BG is in pounds per tree (same as _CARBON_AG_LB).
            # This BG bridge will be replaced by a native NSVB root model.
            return data.with_columns(
                pl.col("CARBON_BG")
                .cast(pl.Float64)
                .fill_null(0.0)
                .alias("_CARBON_BG_LB")
            )
        return data.with_columns(pl.lit(0.0).alias("_CARBON_BG_LB"))

    def _compute_carbon_acre(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """Sum AG + BG, convert lb → short tons, multiply by TPA_UNADJ."""
        return data.with_columns(
            (
                (pl.col("_CARBON_AG_LB") + pl.col("_CARBON_BG_LB"))
                * pl.col("TPA_UNADJ").cast(pl.Float64)
                * LBS_TO_SHORT_TONS
            ).alias("CARBON_ACRE"),
        )

    # ------------------------------------------------------------------
    # Aggregation / variance / formatting (identical across pools)
    # ------------------------------------------------------------------

    def aggregate_results(self, data: pl.LazyFrame | None) -> AggregationResult:
        if data is None:
            return AggregationResult(
                results=pl.DataFrame(),
                plot_tree_data=pl.DataFrame(),
                group_cols=[],
            )

        validate_required_columns(
            data, ["PLT_CN", "CARBON_ACRE"], f"{self._estimator_label} carbon data"
        )

        strat_data = self._get_stratification_data()
        data_with_strat = data.join(strat_data, on="PLT_CN", how="inner")

        data_with_strat = apply_tree_adjustment_factors(
            data_with_strat,
            size_col="DIA",
            macro_breakpoint_col="MACRO_BREAKPOINT_DIA",
        )

        data_with_strat = data_with_strat.with_columns(
            (pl.col("CARBON_ACRE") * pl.col("ADJ_FACTOR")).alias("CARBON_ADJ")
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

        pool = self.config.get("pool", "ag").upper()
        results = results.with_columns(pl.lit(pool).alias("POOL"))

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
