"""
Live tree carbon estimation using the NSVB biomass framework.

Recomputes above-ground live tree biomass tree-by-tree via the vectorized
NSVB pipeline in :mod:`pyfia.carbon.nsvb.equations`, converts to carbon
using species-specific S10a live-tree carbon fractions, and expands to
per-acre and population estimates via pyfia's post-stratified estimator.

Belowground (BG) carbon is currently bridged directly to the FIADB
``TREE.CARBON_BG`` column; a native NSVB coarse-root model is deferred.
The BG bridge is acknowledged as architectural tech debt at
``pyfia/carbon/__init__.py``.

Public API: :func:`live_tree`. See its docstring for parameters,
examples, and the pool semantics.
"""

from __future__ import annotations

import logging

import polars as pl

from ..core import FIA
from ..estimation.columns import get_tree_columns as _get_tree_columns
from ..estimation.utils import (
    ensure_evalid_set,
    ensure_fia_instance,
)
from ._estimator_base import CarbonEstimatorBase
from .nsvb.carbon_fractions import (
    _compute_default_live_carbon_fraction,
    load_carbon_fractions_live_df,
)
from .nsvb.equations import compute_nsvb_biomass

logger = logging.getLogger(__name__)


def _uncovered_nonwoodland_spcds(
    data: pl.LazyFrame,
    lookup,
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
    not per tree, so it does not materialize the full NSVB pipeline.

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


class LiveTreeEstimator(CarbonEstimatorBase):
    """Live tree carbon estimator using the NSVB biomass framework.

    Inherits shared infrastructure (reference-table loading, aggregation,
    variance, formatting) from :class:`CarbonEstimatorBase`. The only
    estimator-specific logic is :meth:`calculate_values` (NSVB pipeline +
    S10a carbon fractions) and the tree column list.
    """

    _estimator_label = "LiveTree"

    def get_tree_columns(self) -> list[str]:
        # CARBON_AG: FIADB-stored above-ground carbon, used as the woodland-
        # species substitute since NSVB does not model them (issue #6).
        # CARBON_BG: the FIADB below-ground bridge.
        estimator_cols = ["SPCD", "DIA", "HT", "CULL", "CARBON_AG", "CARBON_BG"]
        return _get_tree_columns(
            estimator_cols=estimator_cols,
            grp_by=self.config.get("grp_by"),
        )

    def calculate_values(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """Run the NSVB pipeline and produce per-acre carbon columns.

        Steps: join REF_SPECIES → join PLOTGEOM/DIVISION → filter sub-inch
        trees → NSVB biomass → S10a carbon fractions → BG bridge →
        CARBON_ACRE.
        """
        pool = self.config.get("pool", "ag").lower()

        # Join REF_SPECIES and PLOTGEOM/DIVISION
        data = self._join_ref_species(data)
        data = self._join_plotgeom_division(data)

        # Filter sub-inch trees (NSVB not parameterized below 1.0")
        data = data.filter(pl.col("DIA") >= 1.0)

        # NSVB biomass + S10a carbon conversion
        if pool in ("ag", "total"):
            from .nsvb.coefficients import get_vectorized_lookup_tables

            lookup = get_vectorized_lookup_tables()

            # Fail loud, don't zero silently (issue #6). Woodland species are
            # handled below; any *other* species NSVB cannot compute would
            # otherwise resolve to 0 AGB without warning.
            uncovered = _uncovered_nonwoodland_spcds(data, lookup)
            if uncovered:
                raise ValueError(
                    f"live_tree: {len(uncovered)} non-woodland species code(s) "
                    f"{uncovered} present in the data match neither an NSVB "
                    "species-level coefficient row nor a Jenkins-group fallback "
                    "(groups 1-9). NSVB would silently assign them 0 "
                    "above-ground biomass. Woodland species "
                    "(REF_SPECIES.WOODLAND='Y') are routed to FIADB-stored "
                    "CARBON_AG automatically; these SPCDs are an unexpected "
                    "coverage gap — verify they exist in REF_SPECIES with a "
                    "valid JENKINS_SPGRPCD."
                )

            data = compute_nsvb_biomass(data, lookup)
            default_frac = _compute_default_live_carbon_fraction()
            cf_df = load_carbon_fractions_live_df()
            data = data.join(cf_df.lazy(), on="SPCD", how="left")
            data = data.with_columns(
                pl.col("CARBON_FRAC_LIVE")
                .fill_null(default_frac)
                .alias("CARBON_FRAC_LIVE"),
            )
            data = data.with_columns(
                (pl.col("agb") * pl.col("CARBON_FRAC_LIVE")).alias("_CARBON_AG_LB")
            )

            # Woodland species (REF_SPECIES.WOODLAND='Y', measured at DRC) are
            # outside the NSVB framework (GTR-WO-104 p. 6) and recompute to a
            # null/0 AGB. Substitute FIADB's stored CARBON_AG — FIA's
            # production legacy/CRM woodland biomass, already in pounds, the
            # unit-consistent stand-in for the absent NSVB value (issue #6).
            # fill_null(0.0) matches the carbon_pool estimator's handling of
            # the rare unpopulated CARBON_AG record.
            data = data.with_columns(
                pl.when(pl.col("WOODLAND") == "Y")
                .then(pl.col("CARBON_AG").cast(pl.Float64).fill_null(0.0))
                .otherwise(pl.col("_CARBON_AG_LB"))
                .alias("_CARBON_AG_LB")
            )
        else:  # pool == "bg"
            data = data.with_columns(pl.lit(0.0).alias("_CARBON_AG_LB"))

        # BG bridge + CARBON_ACRE
        data = self._apply_bg_bridge(data, pool)
        data = self._compute_carbon_acre(data)

        return data


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
    carbon estimates that align with the FIADB ``TREE.CARBON_AG`` column.

    Belowground carbon is bridged directly to the FIADB pre-computed
    ``TREE.CARBON_BG`` column; a native NSVB coarse-root model is
    deferred.

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
        - 'bg': Below-ground live tree carbon (coarse roots) via a bridge
          to FIADB ``TREE.CARBON_BG``. A native NSVB root model is deferred.
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

    **Woodland Species**

    Woodland species (``REF_SPECIES.WOODLAND='Y'`` — pinyon, juniper,
    mountain-mahogany, Gambel oak; measured at diameter at root collar) are
    outside the NSVB framework by design (Westfall et al. 2023, p. 6); there
    are no NSVB coefficients for them. Rather than let them recompute to 0
    above-ground biomass — which collapses interior-West / Great Basin
    live-tree carbon — this function substitutes FIADB's stored
    ``TREE.CARBON_AG`` for these species (FIA's production legacy/CRM
    woodland biomass per Woodall et al. 2011). Any *non-woodland* species
    that matches neither an NSVB species-level row nor a Jenkins-group
    fallback raises a ``ValueError`` rather than silently zeroing.

    **Belowground Bridge**

    The current implementation does not include the Heath et al. (2009)
    coarse-root model. When ``pool in ('bg', 'total')``, the function reads
    FIADB ``TREE.CARBON_BG`` directly and adds it to the estimate. A native
    NSVB BG model is deferred.

    **EVALID Handling**

    If no EVALID is set on the database and ``most_recent=True``, the
    function auto-selects the most recent EXPVOL evaluation. For explicit
    control, call ``db.clip_by_evalid(...)`` before calling
    ``live_tree``.

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
        "tree_type": "live",
        "tree_domain": tree_domain,
        "area_domain": area_domain,
        "plot_domain": plot_domain,
        "totals": totals,
        "variance": variance,
        "most_recent": most_recent,
    }

    try:
        estimator = LiveTreeEstimator(db, config)
        if pool == "total":
            # The cross-era warning is best-effort: if we can't determine
            # the inventory year (EVALID parse failures, missing POP_EVAL,
            # type coercion problems), skip the warning rather than fail
            # the whole estimation.
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
            except (ValueError, TypeError, AttributeError, IndexError, KeyError) as exc:
                logger.debug("Skipping live_tree year warning: %s", exc)
        return estimator.estimate()
    finally:
        if owns_db and hasattr(db, "close"):
            db.close()
