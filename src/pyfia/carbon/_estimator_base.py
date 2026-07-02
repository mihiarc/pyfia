"""
Shared base class for NSVB carbon pool estimators.

Extracts the ~200 lines of duplicated infrastructure that LiveTreeEstimator
and StandingDeadEstimator share verbatim: reference-table loading
(_load_ref_species, _load_plotgeom), the two-stage aggregation pipeline,
variance calculation, and output formatting. Pool-specific logic
(calculate_values, apply_filters, get_tree_columns) stays in the subclasses.
"""

from __future__ import annotations

import dataclasses
import logging
from typing import TYPE_CHECKING

import polars as pl

from ..core import FIA
from ..estimation.base import AggregationResult, BaseEstimator
from ..estimation.columns import get_cond_columns as _get_cond_columns
from ..estimation.constants import LBS_TO_SHORT_TONS
from ..estimation.tree_expansion import apply_tree_adjustment_factors
from ..estimation.utils import (
    ensure_evalid_set,
    ensure_fia_instance,
    validate_aggregation_result,
    validate_required_columns,
)
from ..filtering.utils import create_size_class_expr
from .nsvb.coefficients import ecosubcd_to_division_expr

if TYPE_CHECKING:
    from .nsvb.coefficients import VectorizedLookupTables

logger = logging.getLogger(__name__)

# Model forms implemented by ``nsvb_biomass_expr`` (GTR-WO-104 Models 1-5).
# Model 6 is the volume-ratio model (merchantable volume / broken-top), which
# never appears in the five biomass-component tables; any row dispatching to an
# unimplemented model would evaluate to null biomass, so we reject it up front.
_IMPLEMENTED_MODELS = frozenset({1, 2, 3, 4, 5})


def _unimplemented_lookup_models(lookup: VectorizedLookupTables) -> list[int]:
    """Return model codes present in the coefficient lookup that NSVB cannot
    evaluate (i.e. not in :data:`_IMPLEMENTED_MODELS`).

    A static, data-independent scan across every tier of every component table
    (~1200 rows total). Catches CSV re-vendor drift that would introduce a
    Model 6 (or future form) into a biomass component and otherwise produce a
    silent null. Empty when every model is implemented.
    """
    models: set[int] = set()
    for field in dataclasses.fields(lookup):
        table = getattr(lookup, field.name)
        if "model" in table.columns:
            models |= set(table["model"].drop_nulls().cast(pl.Int64).to_list())
    return sorted(m for m in models if m not in _IMPLEMENTED_MODELS)


def _uncovered_nonwoodland_spcds(
    data: pl.LazyFrame,
    lookup: VectorizedLookupTables,
) -> list[int]:
    """Return SPCDs present in ``data`` that NSVB cannot compute and that are
    not woodland (so cannot be routed to FIADB-stored carbon).

    A tree's total-AGB prediction (NSVB Supp1 S8a) resolves only if its
    ``SPCD`` matches the species-level coefficient table *or* its
    ``JENKINS_SPGRPCD`` matches a Jenkins-group fallback row. Woodland
    species (``REF_SPECIES.WOODLAND='Y'``, Jenkins group 10) match neither —
    they are out of NSVB scope (GTR-WO-104 p. 6) and handled separately via
    the FIADB ``CARBON_AG`` substitution. Any *non-woodland* SPCD that also
    matches neither is an unexpected coverage gap that would silently
    evaluate to 0 above-ground biomass; the caller raises on it (issue #6).

    The check runs on distinct species present in the data (a tiny frame),
    not per tree, so it does not materialize the full NSVB pipeline. Shared
    by the live-tree and standing-dead estimators, which both predict AGB
    via the same ``total_agb_*`` coefficient tables.

    Parameters
    ----------
    data : pl.LazyFrame
        Tree frame already joined to REF_SPECIES (must carry ``SPCD``,
        ``JENKINS_SPGRPCD`` and ``WOODLAND``).
    lookup : VectorizedLookupTables
        The coefficient bundle whose ``total_agb_*`` tables define coverage.

    Returns
    -------
    list[int]
        Sorted offending SPCDs, empty when every species is covered.
    """
    covered_spcds = lookup.total_agb_spcd["SPCD"].cast(pl.Int64).to_list()
    covered_jenkins = lookup.total_agb_jen["JENKINS_SPGRPCD"].cast(pl.Int64).to_list()

    # Keep rows that match no species-level row, no Jenkins fallback, and
    # are not woodland. ``fill_null(False)`` makes a null Jenkins group
    # (no REF_SPECIES match at all) count as uncovered rather than dropping
    # the row through null-propagation.
    uncovered = (
        data.filter(
            ~pl.col("SPCD").cast(pl.Int64).is_in(covered_spcds)
            & ~pl.col("JENKINS_SPGRPCD")
            .cast(pl.Int64)
            .is_in(covered_jenkins)
            .fill_null(False)
            & (pl.col("WOODLAND").fill_null("N") != "Y")
        )
        .select(pl.col("SPCD").cast(pl.Int64))
        .unique()
        .collect()
    )
    return sorted(uncovered["SPCD"].to_list())


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
        cols = _get_cond_columns(
            land_type=self.config.get("land_type", "forest"),
            grp_by=self.config.get("grp_by"),
            include_prop_basis=False,
        )
        # STDORGCD selects the planted vs. natural NSVB coefficient sets for
        # the stand-origin species (slash/loblolly pine). Load it
        # unconditionally (issue #123); otherwise it is pulled in only when the
        # user groups by it, and 111/131 collapse to the Jenkins fallback.
        if "STDORGCD" not in cols:
            cols.append("STDORGCD")
        return cols

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

        # Gate on table existence rather than catching a broad Exception around
        # the read (issue #126). A missing PLOTGEOM is an expected, benign
        # fallback; but a query/backend/dtype error while reading a table that
        # *does* exist is a real fault that must not be silently swallowed into
        # the ~3% high-biomass bias the warning below documents.
        if not self.db._reader._backend.table_exists("PLOTGEOM"):
            logger.warning(
                "PLOTGEOM not available — DIVISION lookup disabled, falling "
                "back to species-level + Jenkins coefficient precedence "
                "(~3% high biomass bias on growing-stock trees).",
            )
            self._plotgeom_cache = pl.DataFrame()
            return None

        df = self.db._reader.read_table(
            "PLOTGEOM",
            columns=["CN", "ECOSUBCD"],
        )
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

    def _prepare_stdorgcd(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """Normalize ``COND.STDORGCD`` for the stand-origin coefficient lookup.

        Casts to Int64 and fills nulls with 0 (natural — FIA's meaning of "no
        evidence of artificial regeneration"), so the Level 1/1b stand-origin
        joins resolve for every tree of a stand-origin species (issue #123).
        Only slash/loblolly pine (SPCD 111/131) carry STDORGCD-specific NSVB
        coefficients; for every other species the value is inert (their lookup
        rows have a null STDORGCD, so they match via the division/species
        tiers regardless). No-op if the column is absent (e.g. a caller that
        did not load COND).
        """
        if "STDORGCD" not in data.collect_schema().names():
            return data
        return data.with_columns(
            pl.col("STDORGCD")
            .cast(pl.Int64, strict=False)
            .fill_null(0)
            .alias("STDORGCD")
        )

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
    # NSVB woodland-species handling (issue #6) — shared by live + dead
    # ------------------------------------------------------------------

    def _guard_nsvb_coverage(
        self, data: pl.LazyFrame, lookup: VectorizedLookupTables
    ) -> None:
        """Fail loud, don't zero silently (issue #6).

        Woodland species are handled by :meth:`_substitute_woodland_carbon_ag`;
        any *other* species the NSVB pipeline cannot compute would otherwise
        resolve to 0 above-ground biomass without warning. Raises
        :class:`ValueError` naming the offending SPCDs. Real non-woodland
        species always carry a Jenkins-group (1-9) fallback, so this only
        fires on genuine data anomalies (e.g. an SPCD absent from REF_SPECIES).

        Also statically rejects a coefficient lookup that dispatches any row to
        an unimplemented model form (issue #124) — a re-vendor drift guard, so
        an unhandled model becomes a loud failure instead of a silent null.
        """
        bad_models = _unimplemented_lookup_models(lookup)
        if bad_models:
            raise ValueError(
                f"{self._estimator_label}: NSVB coefficient lookup contains "
                f"unimplemented model form(s) {bad_models} (only "
                f"{sorted(_IMPLEMENTED_MODELS)} are evaluated by "
                "nsvb_biomass_expr). Rows dispatching to these would produce "
                "null biomass and be silently dropped from the population sum. "
                "This indicates a coefficient CSV re-vendor introduced a new "
                "model form — extend nsvb_biomass_expr before proceeding."
            )

        uncovered = _uncovered_nonwoodland_spcds(data, lookup)
        if uncovered:
            raise ValueError(
                f"{self._estimator_label}: {len(uncovered)} non-woodland "
                f"species code(s) {uncovered} present in the data match "
                "neither an NSVB species-level coefficient row nor a "
                "Jenkins-group fallback (groups 1-9). NSVB would silently "
                "assign them 0 above-ground biomass. Woodland species "
                "(REF_SPECIES.WOODLAND='Y') are routed to FIADB-stored "
                "CARBON_AG automatically; these SPCDs are an unexpected "
                "coverage gap — verify they exist in REF_SPECIES with a valid "
                "JENKINS_SPGRPCD."
            )

    def _assert_biomass_nonnull(
        self, data: pl.LazyFrame, col: str = "_CARBON_AG_LB"
    ) -> None:
        """Fail loud if the NSVB pipeline produced a null carbon value (#124).

        A null per-tree carbon is silently skipped by the downstream ``sum``
        aggregation — the tree stays in ``N_TREES`` but contributes 0 carbon,
        an undetectable undercount. This is the guard the pipeline docstrings
        promised: it collects the distinct SPCDs whose ``col`` is null and
        raises naming them.

        With Models 1-5 implemented, the coverage/model guards
        (:meth:`_guard_nsvb_coverage`), and the dead-path carbon-fraction
        ``fill_null``, the only residual null sources are data anomalies (e.g.
        a null ``HT`` or ``WDSG`` feeding an ``a*D^b*H^c`` form). This converts
        each from a silent wrong answer into a loud failure. Runs one filtered
        collect over the (tiny-lookup-joined) tree frame per estimator call.
        """
        offending = (
            data.filter(pl.col(col).is_null())
            .select(pl.col("SPCD").cast(pl.Int64).unique())
            .collect()
        )
        spcds = sorted(offending["SPCD"].to_list())
        if spcds:
            raise ValueError(
                f"{self._estimator_label}: the NSVB pipeline produced a null "
                f"{col} for {len(spcds)} species code(s) {spcds}. These trees "
                "would be counted in N_TREES but contribute 0 carbon (a silent "
                "undercount). Likely a null HT/WDSG feeding the allometric "
                "model, or a coefficient row dispatching to an unimplemented "
                "model form — investigate before trusting the estimate."
            )

    def _substitute_woodland_carbon_ag(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """Route woodland species to FIADB-stored ``CARBON_AG`` (issue #6).

        Woodland species (``REF_SPECIES.WOODLAND='Y'``, measured at diameter
        at root collar) are outside the NSVB framework (GTR-WO-104 p. 6) and
        recompute to a null/0 AGB. Substitute FIADB's stored ``CARBON_AG`` —
        FIA's production legacy/CRM woodland biomass (Woodall et al. 2011),
        already in pounds, the unit-consistent stand-in for the absent NSVB
        value. ``fill_null(0.0)`` matches the ``carbon_pool`` estimator's
        handling of the rare unpopulated ``CARBON_AG`` record.

        Requires ``WOODLAND`` and ``CARBON_AG`` columns and an existing
        ``_CARBON_AG_LB`` (the NSVB-recomputed carbon, in pounds) on ``data``.
        """
        return data.with_columns(
            pl.when(pl.col("WOODLAND") == "Y")
            .then(pl.col("CARBON_AG").cast(pl.Float64).fill_null(0.0))
            .otherwise(pl.col("_CARBON_AG_LB"))
            .alias("_CARBON_AG_LB")
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

        # Diameter size-class grouping (issue #125). Built here rather than in
        # _setup_grouping because it needs the tree-level DIA column. Uses the
        # shared "standard" FIA size classes (1.0-4.9", 5.0-9.9", 10.0-19.9",
        # 20.0-29.9", 30.0+"), matching biomass() and the documented output.
        if self.config.get("by_size_class") and "SIZE_CLASS" not in group_cols:
            data_with_strat = data_with_strat.with_columns(
                create_size_class_expr("DIA", size_class_type="standard")
            )
            group_cols.append("SIZE_CLASS")

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
        results = self._calculate_variance_for_metrics(agg_result, metric_configs)
        # Don't emit an orphaned CARBON_TOTAL_SE when the total column itself was
        # dropped for totals=False (issue #127); the SE has nothing to annotate.
        if not self.config.get("totals", True) and "CARBON_TOTAL_SE" in results.columns:
            results = results.drop("CARBON_TOTAL_SE")
        return results

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


# ----------------------------------------------------------------------
# Shared public-function scaffolding (issue #127)
# ----------------------------------------------------------------------
# The two public entry points (live_tree, standing_dead) share identical
# pool/input validation, EVALID resolution, config assembly, and the
# pool='total' cross-era warning. They differ only by estimator class,
# tree_type, the estimator name in messages, and one phrase in the warning.
# Hoisted here so the two cannot drift.


def _warn_cross_era_bg_bridge(
    estimator: CarbonEstimatorBase,
    estimator_name: str,
    legacy_allometry: str,
) -> None:
    """Warn when a pre-NSVB EVALID is combined with the FIADB BG bridge.

    Best-effort: if the inventory year can't be determined (EVALID parse
    failures, missing POP_EVAL, type coercion), skip the warning rather than
    fail the whole estimation.
    """
    try:
        year = estimator._extract_evaluation_year()
        if int(year) < 2024:
            logger.warning(
                "%s(pool='total'): selected EVALID year (%d) pre-dates the "
                "NSVB framework transition (September 2023). The BG bridge "
                "reads FIADB TREE.CARBON_BG directly, which for pre-NSVB "
                "inventories was computed via %s allometry — combining it with "
                "NSVB-recomputed AG may produce cross-era inconsistencies. Use "
                "pool='ag' if you need NSVB-only consistency.",
                estimator_name,
                int(year),
                legacy_allometry,
            )
    except (ValueError, TypeError, AttributeError, IndexError, KeyError) as exc:
        logger.debug("Skipping %s year warning: %s", estimator_name, exc)


def run_carbon_estimator(
    estimator_cls: type[CarbonEstimatorBase],
    *,
    estimator_name: str,
    tree_type: str,
    legacy_allometry: str,
    db: str | FIA,
    pool: str,
    grp_by: str | list[str] | None,
    by_species: bool,
    by_size_class: bool,
    land_type: str,
    tree_domain: str | None,
    area_domain: str | None,
    plot_domain: str | None,
    totals: bool,
    variance: bool,
    most_recent: bool,
) -> pl.DataFrame:
    """Validate inputs, resolve EVALID, and run an NSVB carbon estimator.

    Shared body for :func:`pyfia.carbon.live_tree.live_tree` and
    :func:`pyfia.carbon.standing_dead.standing_dead`; see those functions'
    docstrings for the user-facing parameter semantics.
    """
    from ..validation import (
        validate_boolean,
        validate_domain_expression,
        validate_grp_by,
        validate_land_type,
    )

    # ----- Validate pool -----
    pool = pool.lower()
    valid_pools = {"ag", "bg", "total"}
    if pool not in valid_pools:
        raise ValueError(
            f"Invalid pool '{pool}'. Must be one of: {sorted(valid_pools)}"
        )

    # ----- Validate standard estimator inputs -----
    land_type = validate_land_type(land_type)
    grp_by = validate_grp_by(grp_by)
    tree_domain = validate_domain_expression(tree_domain, "tree_domain")
    area_domain = validate_domain_expression(area_domain, "area_domain")
    plot_domain = validate_domain_expression(plot_domain, "plot_domain")
    by_species = validate_boolean(by_species, "by_species")
    by_size_class = validate_boolean(by_size_class, "by_size_class")
    totals = validate_boolean(totals, "totals")
    variance = validate_boolean(variance, "variance")
    most_recent = validate_boolean(most_recent, "most_recent")

    # ----- Resolve db + EVALID (carbon uses EXPVOL, same as biomass) -----
    db, owns_db = ensure_fia_instance(db)
    if most_recent and db.evalid is None:
        db.clip_most_recent(eval_type="VOL")
    else:
        ensure_evalid_set(db, eval_type="VOL", estimator_name=estimator_name)

    # ----- Build config and run estimator -----
    config = {
        "pool": pool,
        "grp_by": grp_by,
        "by_species": by_species,
        "by_size_class": by_size_class,
        "land_type": land_type,
        "tree_type": tree_type,
        "tree_domain": tree_domain,
        "area_domain": area_domain,
        "plot_domain": plot_domain,
        "totals": totals,
        "variance": variance,
        "most_recent": most_recent,
    }

    try:
        estimator = estimator_cls(db, config)
        if pool == "total":
            _warn_cross_era_bg_bridge(estimator, estimator_name, legacy_allometry)
        return estimator.estimate()
    finally:
        if owns_db and hasattr(db, "close"):
            db.close()
