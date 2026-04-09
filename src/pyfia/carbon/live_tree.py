"""
Live tree carbon estimation using the NSVB biomass framework.

Implements the Phase 1 live tree pool of the Schmidt Sciences "Synthetic
Inventory" project — a publicly auditable Python reconstruction of the
U.S. NGHGI LULUCF forest carbon time series. Recomputes above-ground
live tree biomass tree-by-tree via the vectorized NSVB pipeline in
:mod:`pyfia.carbon.nsvb.equations`, converts to carbon using
species-specific S10a live-tree carbon fractions, and expands to
per-acre and population estimates via pyFIA's post-stratified estimator.

Belowground (BG) carbon is bridged directly to the FIADB
``TREE.CARBON_BG`` column for Phase 1; a native NSVB coarse-root model
will land in a later phase. The bridge is an architectural shortcut
acknowledged in the PR 2 contract at ``pyfia/carbon/__init__.py``.

Public API: :func:`live_tree`. See its docstring for parameters,
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
    _compute_default_live_carbon_fraction,
    load_carbon_fractions_live_df,
)
from .nsvb.coefficients import ecosubcd_to_division
from .nsvb.equations import compute_nsvb_biomass

logger = logging.getLogger(__name__)


class LiveTreeEstimator(BaseEstimator):
    """Live tree carbon estimator using the NSVB biomass framework.

    Follows the ``BaseEstimator`` template-method pattern: ``load_data →
    apply_filters → calculate_values → aggregate_results →
    calculate_variance → format_output``. The only estimator-specific logic
    lives in :meth:`calculate_values` (which runs the vectorized NSVB
    pipeline and does the per-tree biomass → carbon conversion) and in the
    column mapping used by :meth:`format_output`.

    Reference tables (``REF_SPECIES``, ``PLOTGEOM``) are loaded inside
    :meth:`calculate_values` rather than via :meth:`get_required_tables`
    because pyfia's :class:`~pyfia.estimation.data_loading.DataLoader` only
    knows how to wire ``TREE``/``COND``/``PLOT`` join graphs. ``PLOTGEOM``
    in particular powers the Phase 1.5 ``ECOSUBCD → DIVISION`` lookup that
    activates Level 2 of the NSVB coefficient precedence.

    Config keys consumed from ``self.config``:

    - ``pool`` : ``"ag"`` | ``"bg"`` | ``"total"`` — which live tree carbon
      pool to estimate. ``"ag"`` uses the NSVB pipeline. ``"bg"`` and
      ``"total"`` read FIADB's pre-computed ``TREE.CARBON_BG`` column for
      the belowground component (Phase 1 BG bridge).
    - ``grp_by``, ``by_species``, ``by_size_class``, ``land_type``,
      ``tree_domain``, ``area_domain``, ``plot_domain``, ``totals``,
      ``variance``, ``most_recent`` — standard pyFIA estimator knobs.
    """

    def __init__(self, db: str | FIA, config: dict) -> None:
        super().__init__(db, config)
        # PLOTGEOM cache: ``None`` = not yet loaded, an empty DataFrame =
        # tried-and-failed sentinel (so we don't retry per call), a non-empty
        # DataFrame = the loaded ``(PLT_CN, ECOSUBCD)`` lookup.
        self._plotgeom_cache: pl.DataFrame | None = None

    def get_required_tables(self) -> list[str]:
        """Live tree carbon requires tree, condition, and stratification tables.

        ``REF_SPECIES`` and ``PLOTGEOM`` are loaded separately inside
        :meth:`calculate_values` (via :meth:`_load_ref_species` and
        :meth:`_load_plotgeom`) because pyFIA's DataLoader does not know how
        to plumb reference / spatial tables into the standard tree-cond-plot
        join graph. ``REF_SPECIES`` provides ``JENKINS_SPGRPCD`` and
        ``WOOD_SPGR_GREENVOL_DRYWT`` for the NSVB pipeline; ``PLOTGEOM``
        provides ``ECOSUBCD`` for the Phase 1.5 DIVISION lookup that
        activates Level 2 of the NSVB coefficient precedence.
        """
        return ["TREE", "COND", "PLOT", "POP_PLOT_STRATUM_ASSGN", "POP_STRATUM"]

    def get_tree_columns(self) -> list[str]:
        """Tree columns needed for NSVB + BG bridge.

        - ``SPCD``: species code → NSVB coefficient join + S10a carbon fraction
        - ``DIA``: diameter at breast height (inches)
        - ``HT``: total tree height (feet). NSVB Models 1-5 are parameterized
          in total height; ``ACTUALHT`` (broken-top height) is not loaded
          because Phase 1 assumes intact tops.
        - ``CULL``: rotten/missing cull percentage (0-100, nullable)
        - ``TPA_UNADJ``: trees-per-acre expansion factor
        - ``CARBON_BG``: FIADB pre-computed belowground carbon, used only
          when ``pool in ("bg", "total")`` (the Phase 1 BG bridge)
        - ``STATUSCD``: live tree filter (applied by ``BaseEstimator.apply_filters``)
        """
        estimator_cols = ["SPCD", "DIA", "HT", "CULL", "CARBON_BG"]
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

    def _load_ref_species(self) -> pl.DataFrame:
        """Load the ``REF_SPECIES`` columns needed by the NSVB pipeline.

        Reads ``SPCD``, ``JENKINS_SPGRPCD``, and ``WOOD_SPGR_GREENVOL_DRYWT``
        and caches the result on the instance for the duration of one
        estimator run. Follows the same access pattern as
        ``pyfia.utils.reference_tables.join_species_names``.
        """
        if self._ref_species_cache is not None:
            return self._ref_species_cache
        df = self.db._reader.read_table(
            "REF_SPECIES",
            columns=["SPCD", "JENKINS_SPGRPCD", "WOOD_SPGR_GREENVOL_DRYWT"],
        )
        if hasattr(df, "collect"):
            df = df.collect()
        # Cast SPCD to Int64 for consistent joining with the trees frame
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

        Reads ``CN`` and ``ECOSUBCD`` from ``PLOTGEOM``, renames ``CN`` to
        ``PLT_CN`` for the downstream tree join, deduplicates by ``PLT_CN``,
        and caches the result on the instance.

        Returns ``None`` (and logs a one-shot warning) if the database does
        not have a ``PLOTGEOM`` table — typically older test databases that
        were downloaded before ``PLOTGEOM`` was added to
        :data:`pyfia.downloader.tables.COMMON_TABLES`. In that case
        :meth:`calculate_values` skips the ``DIVISION`` join and the NSVB
        pipeline falls back to species-level + Jenkins coefficient lookups
        (Phase 1 behavior, ~3% high biomass bias on growing-stock trees).

        The negative result is cached as an empty DataFrame sentinel so we
        don't retry the failing read on every call.
        """
        if self._plotgeom_cache is not None:
            # Cached: empty df = tried-and-failed sentinel; otherwise the
            # real lookup table.
            return self._plotgeom_cache if self._plotgeom_cache.height > 0 else None

        try:
            df = self.db._reader.read_table(
                "PLOTGEOM",
                columns=["CN", "ECOSUBCD"],
            )
        except Exception as exc:  # noqa: BLE001 — backend-specific errors vary
            logger.warning(
                "PLOTGEOM not available (%s) — Phase 1.5 DIVISION lookup "
                "disabled, falling back to Phase 1 species-level + Jenkins "
                "coefficient precedence (~3%% high biomass bias on "
                "growing-stock trees). To enable the DIVISION lookup, "
                "re-download the database via pyfia.download() to pull the "
                "PLOTGEOM table (added to COMMON_TABLES in Phase 1.5).",
                exc,
            )
            self._plotgeom_cache = pl.DataFrame()  # negative sentinel
            return None

        if hasattr(df, "collect"):
            df = df.collect()
        df = df.select(
            [
                pl.col("CN").alias("PLT_CN"),
                pl.col("ECOSUBCD"),
            ]
        ).unique(subset=["PLT_CN"])  # belt-and-braces against duplicate plot rows
        self._plotgeom_cache = df
        return df

    def calculate_values(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """Run the NSVB pipeline and produce per-acre carbon columns.

        Steps:

        1. Join ``REF_SPECIES`` for ``WDSG`` and ``JENKINS_SPGRPCD``.
        2. (Phase 1.5) Join ``PLOTGEOM`` on ``PLT_CN`` and derive ``DIVISION``
           from ``ECOSUBCD`` via :func:`ecosubcd_to_division`. This activates
           Level 2 of the NSVB lookup precedence inside
           :func:`compute_nsvb_biomass`. Skipped silently when ``PLOTGEOM``
           is missing from the database (older test databases).
        3. Filter out any trees with ``DIA < 1.0`` (the NSVB parameter
           space starts at 1.0 inch; anything below would produce
           numerically unsafe values).
        4. Call :func:`compute_nsvb_biomass` to get ``agb`` (lb) per tree.
        5. Join ``CARBON_FRAC_LIVE`` from S10a and compute
           ``CARBON_AG = agb × CARBON_FRAC_LIVE``.
        6. For ``pool in ("bg", "total")``, bridge to FIADB ``CARBON_BG``
           directly and include it in the total.
        7. Convert lb → short tons and multiply by ``TPA_UNADJ`` to get
           per-acre carbon.
        """
        pool = self.config.get("pool", "ag").lower()

        # Normalize SPCD dtype before any joins. The NSVB coefficient tables
        # (volib_spcd, volbk_spcd, …) and REF_SPECIES are all keyed on Int64
        # SPCD, but FIA CSV dumps frequently load TREE.SPCD as Float64 because
        # DuckDB's read_csv_auto infers Float64 whenever the source data
        # contains a null in what is otherwise an integer column. A Float64
        # vs Int64 join key triggers a polars SchemaError with no automatic
        # cast, so we cast the trees frame to Int64 up front once.
        data = data.with_columns(pl.col("SPCD").cast(pl.Int64))

        # Step 1: Join REF_SPECIES for WDSG + JENKINS_SPGRPCD
        ref_species = self._load_ref_species()
        data = data.join(ref_species.lazy(), on="SPCD", how="left")

        # Step 1b (Phase 1.5): Join PLOTGEOM and derive DIVISION. This adds
        # a ``DIVISION`` column to the trees frame, which compute_nsvb_biomass
        # auto-detects to activate Level 2 (SPCD + DIVISION) of the NSVB
        # coefficient precedence — closing the ~3% growing-stock biomass bias
        # that the species-level-only fallback produces. When PLOTGEOM is
        # missing, we silently skip the join and the NSVB pipeline falls
        # back to Phase 1 species-level + Jenkins lookups.
        plotgeom = self._load_plotgeom()
        if plotgeom is not None:
            data = data.join(plotgeom.lazy(), on="PLT_CN", how="left")
            data = data.with_columns(
                pl.col("ECOSUBCD")
                .map_elements(ecosubcd_to_division, return_dtype=pl.Utf8)
                .alias("DIVISION")
            )

        # Step 2: Filter out sub-inch trees (NSVB not parameterized below 1.0")
        # This is a hard floor; the standard FIA tally threshold is 1.0" d.b.h.
        # so in practice no real trees are dropped.
        data = data.filter(pl.col("DIA") >= 1.0)

        # Step 3: Vectorized NSVB biomass pipeline
        if pool in ("ag", "total"):
            data = compute_nsvb_biomass(data)
            # Step 4: Carbon conversion via species-specific S10a fractions.
            # Unknown SPCDs (no S10a entry) fall back to the S10a arithmetic mean.
            default_frac = _compute_default_live_carbon_fraction()
            cf_df = load_carbon_fractions_live_df()
            data = data.join(cf_df.lazy(), on="SPCD", how="left")
            data = data.with_columns(
                [
                    pl.col("CARBON_FRAC_LIVE")
                    .fill_null(default_frac)
                    .alias("CARBON_FRAC_LIVE"),
                ]
            )
            # CARBON_AG in lb per tree
            data = data.with_columns(
                [(pl.col("agb") * pl.col("CARBON_FRAC_LIVE")).alias("_CARBON_AG_LB")]
            )
        else:  # pool == "bg"
            # Placeholder so the column exists and downstream code is uniform.
            data = data.with_columns([pl.lit(0.0).alias("_CARBON_AG_LB")])

        # Step 5: BG bridge. For pool='bg' or 'total', use FIADB's pre-computed
        # CARBON_BG column directly (it's in lb per tree). For pool='ag', zero
        # out the BG contribution.
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

        # Step 6: Sum AG + BG (only one will be nonzero unless pool='total'),
        # convert to short tons, multiply by TPA_UNADJ for per-acre basis.
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
        """Two-stage aggregation identical in shape to ``BiomassEstimator``.

        Stage 1: Aggregate trees to plot-condition level.
        Stage 2: Apply stratification expansion factors and compute
        ratio-of-means per-acre / total estimates.
        """
        if data is None:
            return AggregationResult(
                results=pl.DataFrame(),
                plot_tree_data=pl.DataFrame(),
                group_cols=[],
            )

        validate_required_columns(
            data, ["PLT_CN", "CARBON_ACRE"], "live_tree carbon data"
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

        # CARBON_ACRE / CARBON_TOTAL are already the canonical names produced
        # by _apply_two_stage_aggregation from the CARBON_ADJ metric mapping;
        # no rename needed.

        if not self.config.get("totals", True):
            if "CARBON_TOTAL" in results.columns:
                results = results.drop("CARBON_TOTAL")

        return AggregationResult(
            results=results,
            plot_tree_data=plot_tree_data,
            group_cols=group_cols,
        )

    def calculate_variance(self, agg_result: AggregationResult) -> pl.DataFrame:
        """Domain-total variance via the shared ``_calculate_variance_for_metrics``.

        Follows Bechtold & Patterson (2005) for stratified ratio-of-means;
        reuses ``BiomassEstimator``'s infrastructure verbatim.
        """
        validate_aggregation_result(agg_result, "LiveTree")

        metric_configs = [
            {
                "adjusted_col": "CARBON_ADJ",
                "acre_se_col": "CARBON_ACRE_SE",
                "total_se_col": "CARBON_TOTAL_SE",
            },
        ]

        return self._calculate_variance_for_metrics(agg_result, metric_configs)

    def format_output(self, results: pl.DataFrame) -> pl.DataFrame:
        """Attach YEAR and reorder columns to the canonical layout."""
        year = self._extract_evaluation_year()
        results = results.with_columns([pl.lit(year).alias("YEAR")])

        # Tag the output with the pool identifier for downstream filtering.
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

        # Insert grouping columns after YEAR so they show up first in output.
        for col in results.columns:
            if col not in col_order:
                col_order.insert(1, col)

        final_cols = [col for col in col_order if col in results.columns]
        return results.select(final_cols)


def live_tree(
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
    Estimate live tree carbon from FIA data using the NSVB framework.

    Recomputes above-ground live tree biomass from scratch using the
    National Scale Volume and Biomass (NSVB) framework of Westfall et al.
    (2023, GTR-WO-104) — the same framework USDA FIA uses to populate the
    FIADB ``CARBON_AG`` column for inventories from September 2023 onward.
    Species-specific live carbon fractions from Table S10a (GTR-WO-104)
    replace the flat ~0.47 multiplier used by ``pyfia.biomass()``, producing
    carbon estimates that align with the EPA NGHGI LULUCF live tree pool.

    Belowground carbon is bridged directly to the FIADB pre-computed
    ``TREE.CARBON_BG`` column for Phase 1; a native NSVB coarse-root
    model is deferred to a later phase.

    Parameters
    ----------
    db : str | FIA
        Database connection or path to FIA database. Can be either a path
        string to a DuckDB/SQLite file or an existing FIA connection object.
    pool : {'ag', 'bg', 'total'}, default 'ag'
        Live tree carbon pool to estimate:

        - 'ag': Above-ground live tree carbon via the NSVB pipeline —
          stem wood + stem bark + branches, harmonized to the directly-
          predicted total AGB and converted to carbon via species-specific
          S10a fractions. Foliage is excluded (not part of AGB in NSVB).
        - 'bg': Below-ground live tree carbon (coarse roots) via the Phase 1
          bridge to FIADB ``TREE.CARBON_BG``. A native NSVB root model is
          planned for a later phase.
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
        ``"DIA >= 10.0 AND SPCD == 131"``. Applied on top of the live-tree
        filter (``STATUSCD == 1``), which is always on for this function.
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
        Live tree carbon estimates with the following columns:

        - **YEAR** : int
            Evaluation reference year from EVALID.
        - **POOL** : str
            Pool identifier — one of ``'AG'``, ``'BG'``, ``'TOTAL'``.
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
            Number of individual tree records.
        - **[grouping columns]** : various
            Any columns specified in ``grp_by`` or via ``by_species`` /
            ``by_size_class``.

    See Also
    --------
    biomass : Estimate tree biomass (dry weight) using FIA's pre-computed DRYBIO columns.
    pyfia.estimation.estimators.carbon.carbon : Legacy carbon estimator that reads the
        FIADB ``CARBON_AG`` / ``CARBON_BG`` columns directly. ``live_tree`` is the
        NSVB-native alternative; both should agree at the tree level for NSVB-era
        inventories (Sep 2023 and later).
    pyfia.carbon.nsvb.equations.compute_nsvb_biomass : The vectorized NSVB biomass
        pipeline this function wraps.

    Notes
    -----
    **NSVB Pipeline**

    For each live tree the function predicts:

    1. Stem inside-bark wood volume (S1a)
    2. Stem bark volume (S2a)
    3. Stem bark biomass (S6a)
    4. Branch biomass (S7a)
    5. Total AGB (S8a), predicted directly from D and H

    The first four are summed and harmonized proportionally to the
    directly-predicted total AGB (which becomes the truth), yielding
    ``w_wood + w_bark + w_branch == agb`` by construction. Cull-reduced
    wood weight uses the Harmon et al. (2011) ``DECAYCD=3`` density
    proportions (0.54 hardwood, 0.92 softwood). The hardwood/softwood
    split is the ``SPCD < 300`` rule, which is consistent with the
    NSVB Model 2 ``k`` constant selection and correctly classifies
    SPCD=10 (fir spp.) as softwood despite S10a's misclassification.

    Carbon = AGB × species-specific S10a fraction. Species missing from
    S10a fall back to the S10a arithmetic mean (~0.4741), with a
    warn-once log entry.

    **Belowground Bridge**

    Phase 1 does not implement the Heath et al. (2009) coarse-root model.
    When ``pool in ('bg', 'total')``, the function reads FIADB
    ``TREE.CARBON_BG`` directly and adds it to the estimate. A native
    NSVB BG model is planned for a later phase.

    **EVALID Handling**

    If no EVALID is set on the database and ``most_recent=True``, the
    function auto-selects the most recent EXPVOL evaluation. For explicit
    control, call ``db.clip_by_evalid(...)`` before calling
    ``live_tree``.

    Required FIA tables and columns:

    - TREE: CN, PLT_CN, CONDID, STATUSCD, SPCD, DIA, HT, CULL, TPA_UNADJ, CARBON_BG
    - COND: PLT_CN, CONDID, COND_STATUS_CD, CONDPROP_UNADJ, OWNGRPCD, FORTYPCD, ...
    - PLOT: CN, STATECD, INVYR, MACRO_BREAKPOINT_DIA
    - REF_SPECIES: SPCD, JENKINS_SPGRPCD, WOOD_SPGR_GREENVOL_DRYWT
    - POP_PLOT_STRATUM_ASSGN: PLT_CN, STRATUM_CN
    - POP_STRATUM: CN, EXPNS, ADJ_FACTOR_*
    - PLOTGEOM (optional): CN, ECOSUBCD — used to derive Bailey ``DIVISION``
      and activate the Phase 1.5 Level 2 NSVB coefficient lookup. When the
      table is absent, the estimator silently falls back to species-level +
      Jenkins coefficient precedence (Phase 1 behavior, ~3% high biomass
      bias on growing-stock trees).

    Raises
    ------
    ValueError
        If ``pool`` is not one of ``'ag'``, ``'bg'``, ``'total'``, or if
        any of the other validated parameters are invalid.
    RuntimeError
        If no data matches the specified filters and domains.

    Examples
    --------
    Above-ground live tree carbon per acre on forestland:

    >>> results = live_tree(db, pool="ag")
    >>> print(f"Carbon: {results['CARBON_ACRE'][0]:.1f} tons/acre")

    Total live tree carbon (AG + BG bridge) by ownership group:

    >>> results = live_tree(db, pool="total", grp_by="OWNGRPCD")
    >>> for row in results.iter_rows(named=True):
    ...     print(f"OWNGRPCD {row['OWNGRPCD']}: {row['CARBON_ACRE']:.2f} tons/acre")

    Above-ground carbon by species on timberland with standard errors:

    >>> results = live_tree(
    ...     db,
    ...     pool="ag",
    ...     by_species=True,
    ...     land_type="timber",
    ...     variance=True,
    ... )

    Large live tree carbon (≥ 20" DBH) by forest type:

    >>> results = live_tree(
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
    # Live tree carbon uses EXPVOL evaluations (same as biomass).
    if most_recent and db.evalid is None:
        db.clip_most_recent(eval_type="VOL")
    else:
        ensure_evalid_set(db, eval_type="VOL", estimator_name="live_tree")

    # ----- Build config and run estimator -----
    config = {
        "pool": pool,
        "grp_by": grp_by,
        "by_species": by_species,
        "by_size_class": by_size_class,
        "land_type": land_type,
        "tree_type": "live",  # hardcoded — live tree carbon pool is, by definition, live trees only
        "tree_domain": tree_domain,
        "area_domain": area_domain,
        "plot_domain": plot_domain,
        "totals": totals,
        "variance": variance,
        "most_recent": most_recent,
    }

    try:
        estimator = LiveTreeEstimator(db, config)
        # Best-effort cross-era warning: pool='total' sums NSVB-recomputed AG
        # with FIADB TREE.CARBON_BG. For pre-NSVB inventories (before the
        # Sep 2023 framework transition), CARBON_BG was computed via legacy
        # Jenkins-based allometry, so the sum is a cross-era methodological
        # mix. Warning only — doesn't block estimation, and wrapped in a
        # try/except so a year-lookup failure never breaks the caller.
        if pool == "total":
            try:
                year = estimator._extract_evaluation_year()
                if int(year) < 2024:
                    logger.warning(
                        "live_tree(pool='total'): selected EVALID year (%d) "
                        "pre-dates the NSVB framework transition "
                        "(September 2023). The BG bridge reads FIADB "
                        "TREE.CARBON_BG directly, which for pre-NSVB "
                        "inventories was computed via legacy Jenkins-based "
                        "allometry — combining it with NSVB-recomputed AG "
                        "may produce cross-era inconsistencies. Use "
                        "pool='ag' if you need NSVB-only consistency.",
                        int(year),
                    )
            except Exception:
                pass  # best-effort; year lookup failure must not break estimation
        return estimator.estimate()
    finally:
        if owns_db and hasattr(db, "close"):
            db.close()
