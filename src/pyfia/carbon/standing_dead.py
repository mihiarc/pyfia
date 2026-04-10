"""
Standing dead tree carbon estimation using the NSVB biomass framework.

Implements the Phase 2 standing dead pool of the Schmidt Sciences "Synthetic
Inventory" project — a publicly auditable Python reconstruction of the
U.S. NGHGI LULUCF forest carbon time series. Recomputes above-ground
standing dead tree biomass tree-by-tree via the vectorized NSVB pipeline
in :mod:`pyfia.carbon.nsvb.equations`, applies the FIADB
``REF_TREE_DECAY_PROP`` density and loss reductions
(``DENSITY_PROP`` × wood, ``BARK_LOSS_PROP`` × bark, ``BRANCH_LOSS_PROP`` ×
branch) keyed by hardwood/softwood × ``DECAYCD``, and converts the reduced
biomass to carbon via species-class S10b dead-tree carbon fractions.

Belowground (BG) carbon for standing dead trees is bridged directly to the
FIADB ``TREE.CARBON_BG`` column, mirroring the live-tree estimator's
Phase 1 BG bridge. A native NSVB coarse-root model for dead trees is
deferred to a later phase.

Public API: :func:`standing_dead`. See its docstring for parameters,
examples, and the pool semantics.
"""

from __future__ import annotations

import logging

import polars as pl

from ..core import FIA
from ..estimation.base import AggregationResult, BaseEstimator
from ..estimation.columns import get_cond_columns as _get_cond_columns
from ..estimation.columns import get_tree_columns as _get_tree_columns
from ..estimation.constants import LBS_TO_SHORT_TONS
from ..estimation.tree_expansion import apply_tree_adjustment_factors
from ..estimation.utils import (
    ensure_evalid_set,
    ensure_fia_instance,
    validate_aggregation_result,
    validate_required_columns,
)
from .nsvb.carbon_fractions import (
    load_carbon_fractions_dead_df,
    load_dead_decay_proportions_df,
)
from .nsvb.coefficients import ecosubcd_to_division
from .nsvb.equations import compute_nsvb_dead_biomass

logger = logging.getLogger(__name__)


class StandingDeadEstimator(BaseEstimator):
    """Standing dead tree carbon estimator using the NSVB biomass framework.

    Mirrors :class:`pyfia.carbon.live_tree.LiveTreeEstimator` in shape — the
    only differences are in :meth:`calculate_values` (runs
    :func:`pyfia.carbon.nsvb.equations.compute_nsvb_dead_biomass` and
    converts via S10b dead carbon fractions instead of the live-tree path)
    and in the additional ``STANDING_DEAD_CD = 1`` + ``DECAYCD IS NOT NULL``
    filters that the standing-dead population requires.

    Reference tables (``REF_SPECIES``, ``PLOTGEOM``) are loaded inside
    :meth:`calculate_values` for the same reason the live-tree estimator
    loads them out-of-band — pyfia's
    :class:`~pyfia.estimation.data_loading.DataLoader` only knows how to
    wire ``TREE``/``COND``/``PLOT`` join graphs.

    Config keys consumed from ``self.config``:

    - ``pool`` : ``"ag"`` | ``"bg"`` | ``"total"`` — which standing-dead
      carbon pool to estimate. ``"ag"`` runs the NSVB dead pipeline.
      ``"bg"`` and ``"total"`` read FIADB's pre-computed ``TREE.CARBON_BG``
      column for the belowground component (Phase 2 BG bridge, mirrors
      the live-tree estimator's bridge).
    - ``grp_by``, ``by_species``, ``by_size_class``, ``land_type``,
      ``tree_domain``, ``area_domain``, ``plot_domain``, ``totals``,
      ``variance``, ``most_recent`` — standard pyFIA estimator knobs.
    """

    def __init__(self, db: str | FIA, config: dict) -> None:
        super().__init__(db, config)
        # PLOTGEOM cache: ``None`` = not yet loaded, an empty DataFrame =
        # tried-and-failed sentinel (so we don't retry per call), a non-empty
        # DataFrame = the loaded ``(PLT_CN, ECOSUBCD)`` lookup. Same scheme as
        # LiveTreeEstimator.
        self._plotgeom_cache: pl.DataFrame | None = None

    def get_required_tables(self) -> list[str]:
        """Standing dead carbon requires tree, condition, and stratification tables.

        ``REF_SPECIES`` and ``PLOTGEOM`` are loaded separately inside
        :meth:`calculate_values` because pyFIA's DataLoader does not know
        how to plumb reference / spatial tables into the standard
        tree-cond-plot join graph. Same out-of-band loading pattern as
        :class:`~pyfia.carbon.live_tree.LiveTreeEstimator`.
        """
        return ["TREE", "COND", "PLOT", "POP_PLOT_STRATUM_ASSGN", "POP_STRATUM"]

    def get_tree_columns(self) -> list[str]:
        """Tree columns needed for the NSVB dead pipeline + BG bridge.

        - ``SPCD``: species code → NSVB coefficient join + S10b carbon fraction
        - ``DIA``: diameter at breast height (inches)
        - ``HT``: total tree height (feet) — used as the *intact* height
          for the NSVB component predictions. Broken-top corrections are
          deferred (see :func:`compute_nsvb_dead_biomass` notes).
        - ``DECAYCD``: standing-dead decay class (1-5) →
          ``REF_TREE_DECAY_PROP`` join key
        - ``STANDING_DEAD_CD``: required to filter out downed dead and
          dead saplings (FIADB only computes ``CARBON_AG`` for
          ``STANDING_DEAD_CD = 1`` trees with a populated ``DECAYCD``)
        - ``TPA_UNADJ``: trees-per-acre expansion factor
        - ``CARBON_BG``: FIADB pre-computed belowground carbon for the
          ``pool in ('bg', 'total')`` BG bridge
        - ``STATUSCD``: dead tree filter (applied by
          ``BaseEstimator.apply_filters`` via ``tree_type = 'dead'``)
        """
        estimator_cols = [
            "SPCD",
            "DIA",
            "HT",
            "DECAYCD",
            "STANDING_DEAD_CD",
            "CARBON_BG",
        ]
        return _get_tree_columns(
            estimator_cols=estimator_cols,
            grp_by=self.config.get("grp_by"),
        )

    def get_cond_columns(self) -> list[str]:
        """Condition columns for land-type filtering and grouping."""
        return _get_cond_columns(
            land_type=self.config.get("land_type", "forest"),
            grp_by=self.config.get("grp_by"),
            include_prop_basis=False,
        )

    def apply_filters(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """Apply standard estimator filters plus the standing-dead requirements.

        On top of the base ``STATUSCD = 2`` filter (driven by
        ``tree_type = 'dead'``), this method narrows to FIADB's standing-dead
        population:

        - ``STANDING_DEAD_CD = 1`` (excludes downed dead trees, which belong
          in the down dead wood pool, and dead saplings, which FIADB tracks
          but does not compute biomass for)
        - ``DECAYCD IS NOT NULL`` (the join key for
          ``REF_TREE_DECAY_PROP``)
        - ``DIA >= 1.0`` (the NSVB Models 1-5 are not parameterized below
          1.0" d.b.h. and the FIA tally floor is 1.0" anyway)

        Without these filters the estimator would either fail the
        coefficient join or produce nulls for trees that FIADB does not
        treat as standing dead.
        """
        data = super().apply_filters(data)

        columns = data.collect_schema().names()
        if "STANDING_DEAD_CD" in columns:
            data = data.filter(
                pl.col("STANDING_DEAD_CD").cast(pl.Utf8, strict=False) == "1"
            )
        if "DECAYCD" in columns:
            data = data.filter(
                pl.col("DECAYCD").cast(pl.Utf8, strict=False).is_not_null()
            )
        if "DIA" in columns:
            data = data.filter(pl.col("DIA") >= 1.0)
        return data

    def _load_ref_species(self) -> pl.DataFrame:
        """Load the ``REF_SPECIES`` columns needed by the NSVB pipeline.

        Identical to :meth:`pyfia.carbon.live_tree.LiveTreeEstimator._load_ref_species`.
        Reads ``SPCD``, ``JENKINS_SPGRPCD``, and ``WOOD_SPGR_GREENVOL_DRYWT``
        and caches the result on the instance for the duration of one
        estimator run.
        """
        if self._ref_species_cache is not None:
            return self._ref_species_cache
        df = self.db._reader.read_table(
            "REF_SPECIES",
            columns=["SPCD", "JENKINS_SPGRPCD", "WOOD_SPGR_GREENVOL_DRYWT"],
        )
        if hasattr(df, "collect"):
            df = df.collect()
        df = df.with_columns(
            [
                pl.col("SPCD").cast(pl.Int64),
                pl.col("JENKINS_SPGRPCD").cast(pl.Int64),
                pl.col("WOOD_SPGR_GREENVOL_DRYWT").cast(pl.Float64).alias("WDSG"),
            ]
        ).select(["SPCD", "JENKINS_SPGRPCD", "WDSG"])
        self._ref_species_cache = df
        return df

    def _load_plotgeom(self) -> pl.DataFrame | None:
        """Load ``PLOTGEOM.ECOSUBCD`` for the Phase 1.5 DIVISION lookup.

        Identical to :meth:`pyfia.carbon.live_tree.LiveTreeEstimator._load_plotgeom`.
        See its docstring for the negative-cache sentinel and graceful
        fallback semantics.
        """
        if self._plotgeom_cache is not None:
            return self._plotgeom_cache if self._plotgeom_cache.height > 0 else None

        try:
            df = self.db._reader.read_table(
                "PLOTGEOM",
                columns=["CN", "ECOSUBCD"],
            )
        except Exception as exc:  # noqa: BLE001 — backend-specific errors vary
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

    def calculate_values(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """Run the NSVB dead pipeline and produce per-acre carbon columns.

        Steps:

        1. Cast ``SPCD`` to ``Int64`` (CSV-loaded TREE.SPCD lands as
           ``Float64`` if any null is present, which breaks the coefficient
           joins).
        2. Cast ``DECAYCD`` from ``Utf8`` (FIADB's CSV column type for
           dead-tree decay class) to ``Int64``.
        3. Join ``REF_SPECIES`` to bring in ``WDSG`` and
           ``JENKINS_SPGRPCD``.
        4. Join ``PLOTGEOM`` and derive ``DIVISION`` from ``ECOSUBCD`` to
           activate the Level 2 NSVB lookup precedence (silently skipped
           when ``PLOTGEOM`` is missing).
        5. Call :func:`compute_nsvb_dead_biomass` to get ``agb`` (lb) per
           tree, with the ``REF_TREE_DECAY_PROP`` reductions applied and
           the components harmonized against the reduced predicted AGB.
        6. Join the S10b dead carbon fractions on (hw_sw, DECAYCD) and
           compute ``CARBON_AG = agb × CARBON_FRAC_DEAD``.
        7. For ``pool in ('bg', 'total')``, bridge to FIADB ``CARBON_BG``
           directly.
        8. Convert lb → short tons and multiply by ``TPA_UNADJ`` for the
           per-acre basis.
        """
        pool = self.config.get("pool", "ag").lower()

        # Step 1 — SPCD dtype normalization. Same reason as the live path.
        data = data.with_columns(pl.col("SPCD").cast(pl.Int64))

        # Step 2 — DECAYCD comes from FIADB as Utf8 (e.g., '3') because the
        # CSV columns sometimes contain '' for null. Cast to Int64 for the
        # decay-prop join. We've already filtered out null DECAYCD in
        # apply_filters; the strict=False guards against any junk that
        # slipped through.
        data = data.with_columns(pl.col("DECAYCD").cast(pl.Int64, strict=False))

        # Step 3 — Join REF_SPECIES for WDSG + JENKINS_SPGRPCD.
        ref_species = self._load_ref_species()
        data = data.join(ref_species.lazy(), on="SPCD", how="left")

        # Step 4 — Join PLOTGEOM and derive DIVISION (Phase 1.5 lookup).
        plotgeom = self._load_plotgeom()
        if plotgeom is not None:
            data = data.join(plotgeom.lazy(), on="PLT_CN", how="left")
            data = data.with_columns(
                pl.col("ECOSUBCD")
                .map_elements(ecosubcd_to_division, return_dtype=pl.Utf8)
                .alias("DIVISION")
            )

        # Step 5 — NSVB dead biomass pipeline (apply REF_TREE_DECAY_PROP
        # reductions and harmonize). For pool='bg', the AG column is zeroed
        # out and only the BG bridge contributes.
        if pool in ("ag", "total"):
            decay_props = load_dead_decay_proportions_df()
            data = compute_nsvb_dead_biomass(data, decay_props)

            # Step 6 — Carbon conversion via S10b dead fractions, joined on
            # (hw_sw, DECAYCD). The hw_sw expression mirrors the SPCD<300
            # rule used inside compute_nsvb_dead_biomass for consistency.
            cf_df = load_carbon_fractions_dead_df()
            data = data.with_columns(
                [
                    pl.when(pl.col("SPCD") >= 300)
                    .then(pl.lit("hardwood"))
                    .otherwise(pl.lit("softwood"))
                    .alias("_hw_sw_cf"),
                ]
            )
            data = data.join(
                cf_df.rename({"hw_sw": "_hw_sw_cf"}).lazy(),
                on=["_hw_sw_cf", "DECAYCD"],
                how="left",
            )
            data = data.with_columns(
                [(pl.col("agb") * pl.col("CARBON_FRAC_DEAD")).alias("_CARBON_AG_LB")]
            )
            data = data.drop(["_hw_sw_cf"])
        else:  # pool == "bg"
            data = data.with_columns([pl.lit(0.0).alias("_CARBON_AG_LB")])

        # Step 7 — BG bridge (mirrors live-tree estimator). For pool='bg' or
        # 'total', read FIADB's pre-computed CARBON_BG directly. For
        # pool='ag', zero out the BG contribution.
        if pool in ("bg", "total"):
            data = data.with_columns(
                [
                    pl.col("CARBON_BG")
                    .cast(pl.Float64)
                    .fill_null(0.0)
                    .alias("_CARBON_BG_LB")
                ]
            )
        else:  # pool == "ag"
            data = data.with_columns([pl.lit(0.0).alias("_CARBON_BG_LB")])

        # Step 8 — Sum AG + BG, convert to short tons, multiply by TPA_UNADJ.
        data = data.with_columns(
            [
                (
                    (pl.col("_CARBON_AG_LB") + pl.col("_CARBON_BG_LB"))
                    * pl.col("TPA_UNADJ").cast(pl.Float64)
                    * LBS_TO_SHORT_TONS
                ).alias("CARBON_ACRE"),
            ]
        )

        return data

    def aggregate_results(self, data: pl.LazyFrame | None) -> AggregationResult:
        """Two-stage aggregation, identical in shape to the live-tree estimator."""
        if data is None:
            return AggregationResult(
                results=pl.DataFrame(),
                plot_tree_data=pl.DataFrame(),
                group_cols=[],
            )

        validate_required_columns(
            data, ["PLT_CN", "CARBON_ACRE"], "standing_dead carbon data"
        )

        strat_data = self._get_stratification_data()
        data_with_strat = data.join(strat_data, on="PLT_CN", how="inner")

        data_with_strat = apply_tree_adjustment_factors(
            data_with_strat, size_col="DIA", macro_breakpoint_col="MACRO_BREAKPOINT_DIA"
        )

        data_with_strat = data_with_strat.with_columns(
            [(pl.col("CARBON_ACRE") * pl.col("ADJ_FACTOR")).alias("CARBON_ADJ")]
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
        """Domain-total variance, reusing the BiomassEstimator infrastructure.

        Follows Bechtold & Patterson (2005) for stratified ratio-of-means;
        same metric configs as :class:`~pyfia.carbon.live_tree.LiveTreeEstimator`.
        """
        validate_aggregation_result(agg_result, "StandingDead")

        metric_configs = [
            {
                "adjusted_col": "CARBON_ADJ",
                "acre_se_col": "CARBON_ACRE_SE",
                "total_se_col": "CARBON_TOTAL_SE",
            },
        ]

        return self._calculate_variance_for_metrics(agg_result, metric_configs)

    def format_output(self, results: pl.DataFrame) -> pl.DataFrame:
        """Attach YEAR, POOL tag, and reorder columns to the canonical layout."""
        year = self._extract_evaluation_year()
        results = results.with_columns([pl.lit(year).alias("YEAR")])

        pool = self.config.get("pool", "ag").upper()
        results = results.with_columns([pl.lit(pool).alias("POOL")])

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


def standing_dead(
    db: str | FIA,
    pool: str = "ag",
    grp_by: str | list[str] | None = None,
    by_species: bool = False,
    by_size_class: bool = False,
    land_type: str = "forest",
    tree_domain: str | None = None,
    area_domain: str | None = None,
    plot_domain: str | None = None,
    totals: bool = True,
    variance: bool = False,
    most_recent: bool = False,
) -> pl.DataFrame:
    """
    Estimate standing dead tree carbon from FIA data using the NSVB framework.

    Recomputes above-ground standing dead tree biomass from scratch using
    the National Scale Volume and Biomass (NSVB) framework of Westfall et
    al. (2023, GTR-WO-104) and applies the FIADB ``REF_TREE_DECAY_PROP``
    decay reductions (DENSITY_PROP × wood, BARK_LOSS_PROP × bark,
    BRANCH_LOSS_PROP × branch) keyed by hardwood/softwood × DECAYCD. The
    reduced biomass is then converted to carbon via species-class S10b
    dead-tree carbon fractions from GTR-WO-104, replacing the flat ~0.47
    multiplier and producing carbon estimates that align with the EPA
    NGHGI LULUCF standing dead pool.

    Belowground carbon for standing dead trees is bridged directly to the
    FIADB pre-computed ``TREE.CARBON_BG`` column for Phase 2; a native
    NSVB coarse-root model for dead trees is deferred to a later phase.

    The standing-dead population is filtered as
    ``STATUSCD = 2 AND STANDING_DEAD_CD = 1 AND DECAYCD IS NOT NULL``,
    which matches the trees FIADB itself populates ``CARBON_AG`` for.
    Trees with ``STANDING_DEAD_CD = 0`` (downed dead) belong to the down
    dead wood pool and are excluded.

    Parameters
    ----------
    db : str | FIA
        Database connection or path to FIA database. Can be either a path
        string to a DuckDB/SQLite file or an existing FIA connection object.
    pool : {'ag', 'bg', 'total'}, default 'ag'
        Standing dead carbon pool to estimate:

        - 'ag': Above-ground standing dead carbon via the NSVB pipeline +
          REF_TREE_DECAY_PROP reductions + S10b dead carbon fractions.
          Foliage is excluded (NSVB AGB does not include foliage; standing
          dead trees rarely have foliage anyway).
        - 'bg': Below-ground standing dead carbon (coarse roots) via the
          Phase 2 bridge to FIADB ``TREE.CARBON_BG``. A native NSVB root
          model is planned for a later phase.
        - 'total': ``'ag' + 'bg'`` (NSVB AG + FIADB BG bridge).
    grp_by : str or list of str, optional
        Column name(s) to group results by. Can be any column from the
        FIA tables used in the estimation (PLOT, COND, TREE). Common
        grouping columns include:

        - 'FORTYPCD': Forest type code
        - 'OWNGRPCD': Ownership group (10=National Forest, 20=Other Federal,
          30=State/Local, 40=Private)
        - 'STATECD': State FIPS code
        - 'COUNTYCD': County code
        - 'INVYR': Inventory year
        - 'STDAGE': Stand age class
        - 'SITECLCD': Site productivity class
        - 'DECAYCD': Standing dead decay class (1-5) — useful for tracing
          carbon distribution across the decay continuum

        For complete column descriptions, see USDA FIA Database User Guide.
    by_species : bool, default False
        If True, group results by species code (SPCD). Convenience parameter
        equivalent to adding 'SPCD' to ``grp_by``.
    by_size_class : bool, default False
        If True, group results by diameter size classes (1.0-4.9", 5.0-9.9",
        10.0-19.9", 20.0-29.9", 30.0+ in).
    land_type : {'forest', 'timber', 'all'}, default 'forest'
        Land type to include in estimation:

        - 'forest': All forestland
        - 'timber': Productive timberland only (unreserved, productive)
        - 'all': All land conditions
    tree_domain : str, optional
        SQL-like filter expression for tree-level filtering. Example:
        ``"DIA >= 10.0 AND SPCD == 131"``. Applied on top of the
        standing-dead filter (``STATUSCD == 2 AND STANDING_DEAD_CD == 1
        AND DECAYCD IS NOT NULL``), which is always on for this function.
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
        Standing dead carbon estimates with the following columns:

        - **YEAR** : int
            Evaluation reference year from EVALID.
        - **POOL** : str
            Pool identifier — one of ``'AG'``, ``'BG'``, ``'TOTAL'``.
        - **CARBON_ACRE** : float
            Standing dead carbon per acre in short tons.
        - **CARBON_TOTAL** : float (if ``totals=True``)
            Total standing dead carbon in short tons expanded to population level.
        - **CARBON_ACRE_SE** : float (if ``variance=True``)
            Standard error of the per-acre estimate.
        - **CARBON_TOTAL_SE** : float (if ``variance=True`` and ``totals=True``)
            Standard error of the population total.
        - **N_PLOTS** : int
            Number of FIA plots included in the estimation.
        - **N_TREES** : int
            Number of individual standing dead tree records.
        - **[grouping columns]** : various
            Any columns specified in ``grp_by`` or via ``by_species`` /
            ``by_size_class``.

    See Also
    --------
    live_tree : Estimate live tree carbon via the NSVB framework.
    biomass : Estimate tree biomass (dry weight) using FIA's pre-computed DRYBIO columns.
    pyfia.carbon.nsvb.equations.compute_nsvb_dead_biomass : The vectorized NSVB
        dead-tree pipeline this function wraps.

    Notes
    -----
    **NSVB Standing Dead Pipeline**

    For each standing dead tree the function predicts gross intact biomass
    using the same NSVB Models 1/2/4/5 the live-tree estimator uses, then
    applies decay reductions:

    1. Stem inside-bark wood volume × WDSG × 62.4 → gross stem wood weight
    2. Predicted gross bark biomass (S6a/S6b)
    3. Predicted gross branch biomass (S7a/S7b)
    4. Predicted total intact AGB (S8a/S8b)

    These four are then reduced by the FIADB ``REF_TREE_DECAY_PROP``
    factors:

    - ``W_wood_dead = W_wood_gross × DENSITY_PROP``
    - ``W_bark_dead = W_bark_gross × BARK_LOSS_PROP``
    - ``W_branch_dead = W_branch_gross × BRANCH_LOSS_PROP``

    The dead components are summed and an AGB reduction factor is computed
    as their ratio to the gross sum. The directly-predicted AGB is then
    scaled by this factor and the components are harmonized against the
    reduced AGB while preserving their relative dead-component ratios.

    Carbon = AGB × dead carbon fraction (S10b, by hardwood/softwood ×
    DECAYCD). Per FIADB User Guide v9.1 Appendix K, **TREE.CULL is not
    applied to standing dead tree biomass** — the decay reductions above
    are the only mass adjustments.

    **What this implementation does NOT do (deferred):**

    - **Broken-top corrections.** Standing dead trees frequently have
      broken tops (``TREE.ACTUALHT < TREE.HT`` for ~75% of EVALID 132401
      Georgia SDs). The full FIADB pipeline applies a crown-proportion
      adjustment to branch biomass and a volume-ratio adjustment to
      wood/bark biomass, looking up the mean intact crown ratio via
      ``REF_TREE_STND_DEAD_CR_PROP`` keyed on Bailey ECOPROV × hw/sw.
      The Phase 2 baseline uses the intact ``HT`` with no broken-top
      correction, which will systematically over-estimate biomass for
      broken-top trees. The validation gate's ratchet thresholds are
      loose to accommodate this.
    - **STDORGCD Level 1 lookup.** Same status as the live-tree path
      (~10 dead-code rows across 5 tables).

    **EVALID Handling**

    If no EVALID is set on the database and ``most_recent=True``, the
    function auto-selects the most recent EXPVOL evaluation. For explicit
    control, call ``db.clip_by_evalid(...)`` before calling
    ``standing_dead``.

    Required FIA tables and columns:

    - TREE: CN, PLT_CN, CONDID, STATUSCD, STANDING_DEAD_CD, SPCD, DIA, HT,
      DECAYCD, TPA_UNADJ, CARBON_BG
    - COND: PLT_CN, CONDID, COND_STATUS_CD, CONDPROP_UNADJ, OWNGRPCD,
      FORTYPCD, ...
    - PLOT: CN, STATECD, INVYR, MACRO_BREAKPOINT_DIA
    - REF_SPECIES: SPCD, JENKINS_SPGRPCD, WOOD_SPGR_GREENVOL_DRYWT
    - POP_PLOT_STRATUM_ASSGN: PLT_CN, STRATUM_CN
    - POP_STRATUM: CN, EXPNS, ADJ_FACTOR_*
    - PLOTGEOM (optional): CN, ECOSUBCD — used to derive Bailey ``DIVISION``
      and activate the Level 2 NSVB coefficient lookup. When the table
      is absent, the estimator falls back to species-level + Jenkins
      coefficient precedence.

    Raises
    ------
    ValueError
        If ``pool`` is not one of ``'ag'``, ``'bg'``, ``'total'``, or if
        any of the other validated parameters are invalid.
    RuntimeError
        If no data matches the specified filters and domains.

    Examples
    --------
    Above-ground standing dead carbon per acre on forestland:

    >>> results = standing_dead(db, pool="ag")
    >>> print(f"SD Carbon: {results['CARBON_ACRE'][0]:.1f} tons/acre")

    Total standing dead carbon (AG + BG bridge) by ownership group:

    >>> results = standing_dead(db, pool="total", grp_by="OWNGRPCD")

    Above-ground standing dead carbon by decay class on timberland:

    >>> results = standing_dead(
    ...     db,
    ...     pool="ag",
    ...     grp_by="DECAYCD",
    ...     land_type="timber",
    ... )

    Large standing dead carbon (≥ 20" DBH) by forest type:

    >>> results = standing_dead(
    ...     db,
    ...     pool="ag",
    ...     grp_by="FORTYPCD",
    ...     tree_domain="DIA >= 20.0",
    ...     totals=True,
    ... )
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

    # ----- Resolve db + EVALID -----
    db, owns_db = ensure_fia_instance(db)
    if most_recent and db.evalid is None:
        db.clip_most_recent(eval_type="VOL")
    else:
        ensure_evalid_set(db, eval_type="VOL", estimator_name="standing_dead")

    # ----- Build config and run estimator -----
    config = {
        "pool": pool,
        "grp_by": grp_by,
        "by_species": by_species,
        "by_size_class": by_size_class,
        "land_type": land_type,
        "tree_type": "dead",  # hardcoded — standing dead is, by definition, STATUSCD=2
        "tree_domain": tree_domain,
        "area_domain": area_domain,
        "plot_domain": plot_domain,
        "totals": totals,
        "variance": variance,
        "most_recent": most_recent,
    }

    try:
        estimator = StandingDeadEstimator(db, config)
        # Best-effort cross-era warning: pool='total' sums NSVB-recomputed AG
        # with FIADB TREE.CARBON_BG. For pre-NSVB inventories the BG bridge
        # may produce cross-era inconsistencies (legacy CRM). Mirrors the
        # live-tree estimator's warning.
        if pool == "total":
            try:
                year = estimator._extract_evaluation_year()
                if int(year) < 2024:
                    logger.warning(
                        "standing_dead(pool='total'): selected EVALID year "
                        "(%d) pre-dates the NSVB framework transition "
                        "(September 2023). The BG bridge reads FIADB "
                        "TREE.CARBON_BG directly, which for pre-NSVB "
                        "inventories was computed via legacy CRM-based "
                        "allometry — combining it with NSVB-recomputed AG "
                        "may produce cross-era inconsistencies. Use "
                        "pool='ag' if you need NSVB-only consistency.",
                        int(year),
                    )
            except Exception:
                pass
        return estimator.estimate()
    finally:
        if owns_db and hasattr(db, "close"):
            db.close()
