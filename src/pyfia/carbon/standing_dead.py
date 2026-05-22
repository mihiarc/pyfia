"""
Standing dead tree carbon estimation using the NSVB biomass framework.

Recomputes above-ground standing dead tree biomass tree-by-tree via the
vectorized NSVB pipeline in :mod:`pyfia.carbon.nsvb.equations`, applies
the FIADB ``REF_TREE_DECAY_PROP`` density and loss reductions
(``DENSITY_PROP`` × wood, ``BARK_LOSS_PROP`` × bark, ``BRANCH_LOSS_PROP`` ×
branch) keyed by hardwood/softwood × ``DECAYCD``, and converts the reduced
biomass to carbon via species-class S10b dead-tree carbon fractions.

Broken-top corrections (``ACTUALHT < HT``) apply the Appendix K
crown-proportion adjustment to branch biomass and a volume-ratio adjustment
to wood/bark biomass, using the mean intact crown ratio from Table S11
(``REF_TREE_STND_DEAD_CR_PROP``) keyed by Bailey ecoregion province ×
hardwood/softwood.

Belowground (BG) carbon for standing dead trees is bridged directly to the
FIADB ``TREE.CARBON_BG`` column, mirroring the live-tree estimator's
BG bridge. A native NSVB coarse-root model for dead trees is deferred.

Public API: :func:`standing_dead`. See its docstring for parameters,
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
    load_carbon_fractions_dead_df,
    load_dead_cr_prop_df,
    load_dead_decay_proportions_df,
)
from .nsvb.equations import compute_nsvb_dead_biomass

logger = logging.getLogger(__name__)


class StandingDeadEstimator(CarbonEstimatorBase):
    """Standing dead tree carbon estimator using the NSVB biomass framework.

    Inherits shared infrastructure from :class:`CarbonEstimatorBase`. The
    standing-dead-specific logic is in :meth:`apply_filters` (the
    ``STANDING_DEAD_CD = 1 + DECAYCD IS NOT NULL`` requirements) and
    :meth:`calculate_values` (dead biomass pipeline + S10b fractions +
    broken-top corrections).
    """

    _estimator_label = "StandingDead"

    def get_tree_columns(self) -> list[str]:
        estimator_cols = [
            "SPCD",
            "DIA",
            "HT",
            "ACTUALHT",
            "DECAYCD",
            "STANDING_DEAD_CD",
            "CARBON_BG",
        ]
        return _get_tree_columns(
            estimator_cols=estimator_cols,
            grp_by=self.config.get("grp_by"),
        )

    def apply_filters(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """Apply standard filters plus standing-dead requirements.

        On top of ``STATUSCD = 2`` (from ``tree_type = 'dead'``):
        ``STANDING_DEAD_CD = 1``, ``DECAYCD IS NOT NULL``, ``DIA >= 1.0``.
        """
        data = super().apply_filters(data)

        columns = data.collect_schema().names()
        if "STANDING_DEAD_CD" in columns:
            data = data.filter(
                pl.col("STANDING_DEAD_CD").cast(pl.Utf8, strict=False) == "1"
            )
        if "DECAYCD" in columns:
            data = data.filter(
                pl.col("DECAYCD").cast(pl.Int64, strict=False).is_not_null()
            )
        if "DIA" in columns:
            data = data.filter(pl.col("DIA") >= 1.0)
        return data

    def calculate_values(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """Run the NSVB dead pipeline with broken-top corrections.

        Steps: join REF_SPECIES → join PLOTGEOM/DIVISION → cast DECAYCD →
        NSVB dead biomass (with broken-top corrections) → S10b carbon
        fractions → BG bridge → CARBON_ACRE.
        """
        pool = self.config.get("pool", "ag").lower()

        # Join REF_SPECIES and PLOTGEOM/DIVISION
        data = self._join_ref_species(data)
        data = self._join_plotgeom_division(data)

        # Cast DECAYCD from Utf8 to Int64 for the decay-prop join
        data = data.with_columns(pl.col("DECAYCD").cast(pl.Int64, strict=False))

        # NSVB dead biomass pipeline (with broken-top corrections when
        # ACTUALHT is available and the CR prop table loads)
        if pool in ("ag", "total"):
            decay_props = load_dead_decay_proportions_df()
            cr_prop_table = load_dead_cr_prop_df()
            data = compute_nsvb_dead_biomass(
                data, decay_props, cr_prop_table=cr_prop_table
            )

            # S10b dead carbon fractions joined on (hw_sw, DECAYCD)
            cf_df = load_carbon_fractions_dead_df()
            data = data.with_columns(
                pl.when(pl.col("SPCD") >= 300)
                .then(pl.lit("hardwood"))
                .otherwise(pl.lit("softwood"))
                .alias("_hw_sw_cf"),
            )
            data = data.join(
                cf_df.rename({"hw_sw": "_hw_sw_cf"}).lazy(),
                on=["_hw_sw_cf", "DECAYCD"],
                how="left",
            )
            data = data.with_columns(
                (pl.col("agb") * pl.col("CARBON_FRAC_DEAD")).alias("_CARBON_AG_LB")
            )
            data = data.drop(["_hw_sw_cf"])
        else:  # pool == "bg"
            data = data.with_columns(pl.lit(0.0).alias("_CARBON_AG_LB"))

        # BG bridge + CARBON_ACRE
        data = self._apply_bg_bridge(data, pool)
        data = self._compute_carbon_acre(data)

        return data


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
    multiplier and producing carbon estimates that align with the FIADB
    ``TREE.CARBON_AG`` column for standing dead trees (STATUSCD=2).

    Broken-top corrections apply the Appendix K crown-proportion
    adjustment to branch biomass and a volume-ratio adjustment to wood/bark
    for trees with ``ACTUALHT < HT``, using the mean intact crown ratio
    from Table S11 (``REF_TREE_STND_DEAD_CR_PROP``) keyed by Bailey
    ecoregion province × hardwood/softwood.

    Belowground carbon for standing dead trees is bridged directly to the
    FIADB pre-computed ``TREE.CARBON_BG`` column; a native NSVB coarse-root
    model for dead trees is deferred.

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
          REF_TREE_DECAY_PROP reductions + broken-top corrections + S10b
          dead carbon fractions.
        - 'bg': Below-ground standing dead carbon (coarse roots) via the
          bridge to FIADB ``TREE.CARBON_BG``.
        - 'total': ``'ag' + 'bg'`` (NSVB AG + FIADB BG bridge).
    grp_by : str or list of str, optional
        Column name(s) to group results by.
    by_species : bool, default False
        If True, group results by species code (SPCD).
    by_size_class : bool, default False
        If True, group results by diameter size classes.
    land_type : {'forest', 'timber', 'all'}, default 'forest'
        Land type to include in estimation.
    tree_domain : str, optional
        SQL-like filter expression for tree-level filtering.
    area_domain : str, optional
        SQL-like filter expression for area/condition-level filtering.
    plot_domain : str, optional
        SQL-like filter expression for plot-level filtering.
    totals : bool, default True
        If True, include population-level total estimates.
    variance : bool, default False
        If True, calculate variance and standard error estimates.
    most_recent : bool, default False
        If True, auto-filter to the most recent EXPVOL evaluation.

    Returns
    -------
    pl.DataFrame
        Standing dead carbon estimates with columns: YEAR, POOL,
        CARBON_ACRE, CARBON_TOTAL (if totals), CARBON_ACRE_SE (if
        variance), CARBON_TOTAL_SE (if variance and totals), N_PLOTS,
        N_TREES, plus any grouping columns.

    See Also
    --------
    live_tree : Estimate live tree carbon via the NSVB framework.

    Examples
    --------
    Above-ground standing dead carbon per acre on forestland:

    >>> results = standing_dead(db, pool="ag")
    >>> print(f"SD Carbon: {results['CARBON_ACRE'][0]:.1f} tons/acre")

    Standing dead carbon by decay class on timberland:

    >>> results = standing_dead(
    ...     db,
    ...     pool="ag",
    ...     grp_by="DECAYCD",
    ...     land_type="timber",
    ... )
    """
    from ..validation import (
        validate_boolean,
        validate_domain_expression,
        validate_grp_by,
        validate_land_type,
    )

    pool = pool.lower()
    valid_pools = {"ag", "bg", "total"}
    if pool not in valid_pools:
        raise ValueError(
            f"Invalid pool '{pool}'. Must be one of: {sorted(valid_pools)}"
        )

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

    db, owns_db = ensure_fia_instance(db)
    if most_recent and db.evalid is None:
        db.clip_most_recent(eval_type="VOL")
    else:
        ensure_evalid_set(db, eval_type="VOL", estimator_name="standing_dead")

    config = {
        "pool": pool,
        "grp_by": grp_by,
        "by_species": by_species,
        "by_size_class": by_size_class,
        "land_type": land_type,
        "tree_type": "dead",
        "tree_domain": tree_domain,
        "area_domain": area_domain,
        "plot_domain": plot_domain,
        "totals": totals,
        "variance": variance,
        "most_recent": most_recent,
    }

    try:
        estimator = StandingDeadEstimator(db, config)
        if pool == "total":
            # Best-effort cross-era warning; see live_tree.py for rationale.
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
            except (ValueError, TypeError, AttributeError, IndexError, KeyError) as exc:
                logger.debug("Skipping standing_dead year warning: %s", exc)
        return estimator.estimate()
    finally:
        if owns_db and hasattr(db, "close"):
            db.close()
