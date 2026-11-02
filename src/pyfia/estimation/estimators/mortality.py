"""
Mortality estimation for FIA data using GRM tables.

Implements FIA's Growth-Removal-Mortality methodology for calculating
annual tree mortality using TREE_GRM_COMPONENT and TREE_GRM_MIDPT tables.
"""

from __future__ import annotations

from typing import Literal

import polars as pl

from ...constants.columns import RefColumns, TreeColumns
from ...constants.tables import TableNames
from ...core import FIA
from ...core.exceptions import NoEVALIDError
from ..base import AggregationResult
from ..constants import BASAL_AREA_FACTOR, LBS_TO_SHORT_TONS
from ..grm_base import GRMBaseEstimator

# Columns mortality_trees() always returns, in order.
TREE_FRAME_COLUMNS = [
    "PLT_CN",
    "TRE_CN",
    "STATECD",
    "STRATUM_CN",
    "ESTN_UNIT_CN",
    "EXPNS",
    "COMPONENT",
    "SUBPTYP_GRM",
    "DIA_BEGIN",
    "DIA_MIDPT",
    "SPCD",
    "TPA_UNADJ",
    "ADJ_FACTOR",
    "TPA_ADJ",
]


def _green_weight_lbs() -> pl.Expr:
    """Green weight of a tree's bole wood and bark, in pounds.

    Dry bole wood (DRYBIO_BOLE) and bole bark (DRYBIO_BOLE_BARK) are each
    raised to green weight by their REF_SPECIES moisture content, a percent
    of oven-dry weight. Null dry biomass adds nothing. A null moisture
    content never meets positive biomass: ``_check_green_moisture`` raises
    first.
    """
    wood = pl.col(TreeColumns.DRYBIO_BOLE).cast(pl.Float64).fill_null(0.0) * (
        1.0 + pl.col(RefColumns.MC_PCT_GREEN_WOOD).fill_null(0.0) / 100.0
    )
    bark = pl.col(TreeColumns.DRYBIO_BOLE_BARK).cast(pl.Float64).fill_null(0.0) * (
        1.0 + pl.col(RefColumns.MC_PCT_GREEN_BARK).fill_null(0.0) / 100.0
    )
    return wood + bark


def _check_green_moisture(data: pl.LazyFrame) -> None:
    """Raise if a tree with dry bole biomass has no green moisture content."""
    wood = pl.col(TreeColumns.DRYBIO_BOLE).fill_null(0.0) > 0
    bark = pl.col(TreeColumns.DRYBIO_BOLE_BARK).fill_null(0.0) > 0
    missing = (
        data.filter(
            (wood & pl.col(RefColumns.MC_PCT_GREEN_WOOD).is_null())
            | (bark & pl.col(RefColumns.MC_PCT_GREEN_BARK).is_null())
        )
        .select(pl.col(TreeColumns.SPCD).unique().sort())
        .collect()
    )
    if missing.height:
        spcds = missing[TreeColumns.SPCD].to_list()
        raise ValueError(
            f"REF_SPECIES has no green moisture content (MC_PCT_GREEN_WOOD or "
            f"MC_PCT_GREEN_BARK) for SPCD {spcds}, so their green weight is "
            f"unknown. Exclude them with tree_domain, for example "
            f"tree_domain='SPCD NOT IN ({', '.join(map(str, spcds))})'."
        )


class MortalityEstimator(GRMBaseEstimator):
    """
    Mortality estimator for FIA data using GRM methodology.

    Estimates annual tree mortality in terms of volume, biomass, or trees per acre
    using the TREE_GRM_COMPONENT and TREE_GRM_MIDPT tables.
    """

    @property
    def component_type(self) -> Literal["growth", "mortality", "removals"]:
        """Return 'mortality' as the GRM component type."""
        return "mortality"

    def get_component_filter(self) -> pl.Expr | None:
        """Filter to mortality components only."""
        return pl.col("COMPONENT").str.starts_with("MORTALITY")

    def load_data(self) -> pl.LazyFrame | None:
        """Load GRM data for mortality estimation."""
        from ..columns import PLOT_GROUPING_COLUMNS

        # Use the simple GRM data loading pattern
        data = self._load_simple_grm_data()
        if data is None:
            return None

        # Add PLOT data for additional info if needed
        if "PLOT" not in self.db.tables:
            self.db.load_table("PLOT")

        plot = self.db.tables["PLOT"]
        if not isinstance(plot, pl.LazyFrame):
            plot = plot.lazy()

        # Base plot columns always needed
        plot_cols = ["CN", "STATECD", "INVYR", "MACRO_BREAKPOINT_DIA"]

        # Add any plot-level grouping columns from grp_by
        grp_by = self.config.get("grp_by")
        if grp_by:
            if isinstance(grp_by, str):
                grp_by = [grp_by]
            plot_schema = plot.collect_schema().names()
            for col in grp_by:
                if (
                    col in PLOT_GROUPING_COLUMNS
                    and col in plot_schema
                    and col not in plot_cols
                ):
                    plot_cols.append(col)
        for col in self._route_extra_columns()[TableNames.PLOT]:
            if col not in plot_cols:
                plot_cols.append(col)

        plot = plot.select(plot_cols)
        data = data.join(plot, left_on="PLT_CN", right_on="CN", how="left")

        if self.config.get("measure") == "green_weight":
            data = data.join(self._green_moisture(), on=TreeColumns.SPCD, how="left")

        return data

    def _green_moisture(self) -> pl.LazyFrame:
        """REF_SPECIES green moisture contents of wood and bark, by SPCD."""
        cols = [
            RefColumns.SPCD,
            RefColumns.MC_PCT_GREEN_WOOD,
            RefColumns.MC_PCT_GREEN_BARK,
        ]
        ref = self._load_table_columns(TableNames.REF_SPECIES, cols)
        return ref.select(
            pl.col(RefColumns.SPCD).cast(pl.Int64),
            pl.col(RefColumns.MC_PCT_GREEN_WOOD).cast(pl.Float64),
            pl.col(RefColumns.MC_PCT_GREEN_BARK).cast(pl.Float64),
        )

    def apply_filters(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """Apply mortality-specific filters."""
        # Use common GRM filter application
        data = self._apply_grm_filters(data)

        # Additional mortality-specific filters. Sawtimber ('sl'/'sawtimber') is
        # already encoded by the GRM SL population columns, so it needs no extra
        # diameter filter here — that kept 'sawtimber' identical to 'sl'.
        tree_type = self.config.get("tree_type", "gs")

        if tree_type == "gs":
            # Ensure volume is positive for growing stock
            schema = data.collect_schema().names()
            if "VOLCFNET" in schema:
                data = data.filter(pl.col("VOLCFNET") > 0)

        if self.config.get("measure") == "green_weight":
            _check_green_moisture(data)

        return data

    def calculate_values(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """
        Calculate mortality values per acre.

        TPA_UNADJ is already annualized, so no remeasurement period adjustment needed.
        """
        measure = self.config.get("measure", "volume")

        if measure == "volume":
            data = data.with_columns(
                [
                    (
                        pl.col("TPA_UNADJ").cast(pl.Float64)
                        * pl.col("VOLCFNET").cast(pl.Float64)
                    ).alias("MORT_VALUE")
                ]
            )
        elif measure == "sawlog":
            data = data.with_columns(
                [
                    (
                        pl.col("TPA_UNADJ").cast(pl.Float64)
                        * pl.col("VOLCSNET").cast(pl.Float64)
                    ).alias("MORT_VALUE")
                ]
            )
        elif measure == "total_stem":
            # FIADB leaves VOLTSSND null on a few trees; like EVALIDator's SUM,
            # they add nothing to the total.
            data = data.with_columns(
                (
                    pl.col("TPA_UNADJ").cast(pl.Float64)
                    * pl.col(TreeColumns.VOLTSSND).fill_null(0.0)
                ).alias("MORT_VALUE")
            )
        elif measure == "biomass":
            data = data.with_columns(
                [
                    (
                        pl.col("TPA_UNADJ").cast(pl.Float64)
                        * (pl.col("DRYBIO_BOLE") + pl.col("DRYBIO_BRANCH")).cast(
                            pl.Float64
                        )
                        * LBS_TO_SHORT_TONS
                    ).alias("MORT_VALUE")
                ]
            )
        elif measure == "green_weight":
            data = data.with_columns(
                (
                    pl.col("TPA_UNADJ").cast(pl.Float64)
                    * _green_weight_lbs()
                    * LBS_TO_SHORT_TONS
                ).alias("MORT_VALUE")
            )
        elif measure == "basal_area":
            data = data.with_columns(
                [
                    (
                        pl.col("TPA_UNADJ").cast(pl.Float64)
                        * (pl.col("DIA").cast(pl.Float64) ** 2 * BASAL_AREA_FACTOR)
                    ).alias("MORT_VALUE")
                ]
            )
        else:  # tpa or count
            data = data.with_columns(
                [pl.col("TPA_UNADJ").cast(pl.Float64).alias("MORT_VALUE")]
            )

        # Create annual column for consistency
        data = data.with_columns([pl.col("MORT_VALUE").alias("MORT_ANNUAL")])

        return data

    def aggregate_results(self, data: pl.LazyFrame | None) -> AggregationResult:
        """Aggregate mortality with two-stage aggregation.

        Returns
        -------
        AggregationResult
            Bundle containing results, plot_tree_data, and group_cols for
            explicit variance calculation.
        """
        if data is None:
            return AggregationResult(
                results=pl.DataFrame(),
                plot_tree_data=pl.DataFrame(),
                group_cols=[],
            )

        # Use shared GRM aggregation - returns AggregationResult
        agg_result = self._aggregate_grm_results(
            data,
            value_col="MORT_ANNUAL",
            adjusted_col="MORT_ADJ",
        )

        # Rename columns to mortality-specific names
        results = agg_result.results
        rename_map = {"MORT_ACRE": "MORT_ACRE", "MORT_TOTAL": "MORT_TOTAL"}
        for old, new in rename_map.items():
            if old in results.columns:
                results = results.rename({old: new})

        if "N_TREES" in results.columns:
            results = results.rename({"N_TREES": "N_DEAD_TREES"})

        # Calculate mortality rate if requested
        if self.config.get("as_rate", False):
            results = results.with_columns([pl.col("MORT_ACRE").alias("MORT_RATE")])

        # Return updated AggregationResult
        return AggregationResult(
            results=results,
            plot_tree_data=agg_result.plot_tree_data,
            group_cols=agg_result.group_cols,
        )

    def calculate_variance(self, agg_result: AggregationResult) -> pl.DataFrame:
        """Calculate variance for mortality estimates using ratio-of-means formula.

        Implements Bechtold & Patterson (2005) stratified variance calculation.
        MORT_RATE uses the same variance as MORT_ACRE since they represent the
        same per-acre estimate (MORT_RATE = mortality per acre, not mortality/growing_stock).

        Parameters
        ----------
        agg_result : AggregationResult
            Bundle containing results, plot_tree_data, and group_cols from
            aggregate_results().

        Returns
        -------
        pl.DataFrame
            Results with variance columns added.
        """
        results = self._calculate_grm_variance(
            agg_result.results,
            adjusted_col="MORT_ADJ",
            acre_se_col="MORT_ACRE_SE",
            total_se_col="MORT_TOTAL_SE",
            plot_tree_data=agg_result.plot_tree_data,
            group_cols=agg_result.group_cols,
        )

        # MORT_RATE is an alias for MORT_ACRE (per-acre mortality)
        # so its SE is the same as MORT_ACRE_SE
        if "MORT_RATE" in results.columns:
            results = results.with_columns(
                [pl.col("MORT_ACRE_SE").alias("MORT_RATE_SE")]
            )

        # Add CV if requested
        if self.config.get("include_cv", False):
            results = results.with_columns(
                [
                    pl.when(pl.col("MORT_ACRE") > 0)
                    .then(pl.col("MORT_ACRE_SE") / pl.col("MORT_ACRE") * 100)
                    .otherwise(None)
                    .alias("MORT_ACRE_CV"),
                    pl.when(pl.col("MORT_TOTAL") > 0)
                    .then(pl.col("MORT_TOTAL_SE") / pl.col("MORT_TOTAL") * 100)
                    .otherwise(None)
                    .alias("MORT_TOTAL_CV"),
                ]
            )

        return results

    def format_output(self, results: pl.DataFrame) -> pl.DataFrame:
        """Format mortality estimation output."""
        return self._format_grm_output(
            results,
            estimation_type="mortality",
            include_cv=self.config.get("include_cv", False),
        )

    def tree_frame(self) -> pl.DataFrame:
        """Return one row per mortality tree with its design keys and weight.

        Runs the same loading, population and domain filters as
        ``estimate()``, joins each tree's plot to its stratum, and applies the
        GRM adjustment factor for the tree's ``SUBPTYP_GRM``. ``TPA_ADJ`` is
        ``TPA_UNADJ * ADJ_FACTOR``: the tree's annual mortality per acre after
        adjustment.
        """
        from ..grm import apply_grm_adjustment

        data = self.load_data()
        if data is None:
            raise ValueError("No GRM data available for mortality_trees()")
        data = self.apply_filters(data)
        data = data.join(self._get_stratification_data(), on="PLT_CN", how="inner")
        data = apply_grm_adjustment(data).with_columns(
            (pl.col("TPA_UNADJ") * pl.col("ADJ_FACTOR")).alias("TPA_ADJ")
        )
        requested = [
            c
            for c in dict.fromkeys(self.config.get("columns") or [])
            if c not in TREE_FRAME_COLUMNS
        ]
        return (
            data.select(TREE_FRAME_COLUMNS + requested)
            .sort(["PLT_CN", "TRE_CN"])
            .collect()
        )


def mortality(
    db: str | FIA,
    grp_by: str | list[str] | None = None,
    by_species: bool = False,
    by_size_class: bool = False,
    size_class_type: str = "standard",
    land_type: str = "timber",
    tree_type: str = "gs",
    measure: str = "volume",
    tree_domain: str | None = None,
    area_domain: str | None = None,
    as_rate: bool = False,
    totals: bool = True,
    variance: bool = False,
    most_recent: bool = False,
) -> pl.DataFrame:
    """
    Estimate annual tree mortality from FIA data using GRM methodology.

    Uses TREE_GRM_COMPONENT and TREE_GRM_MIDPT tables to calculate
    annual mortality following FIA's Growth-Removal-Mortality approach.
    This is the correct FIA statistical methodology for mortality estimation.

    Parameters
    ----------
    db : str | FIA
        Database connection or path to FIA database.
    grp_by : str or list of str, optional
        Column name(s) to group results by.
    by_species : bool, default False
        If True, group results by species code (SPCD).
    by_size_class : bool, default False
        If True, group results by diameter size classes.
    size_class_type : {'standard', 'descriptive', 'market'}, default 'standard'
        Type of size class grouping to use (only applies when by_size_class=True):
        - "standard": FIA numeric ranges (1.0-4.9, 5.0-9.9, etc.)
        - "descriptive": Text labels (Saplings, Small, Medium, Large)
        - "market": Timber market categories (Pre-merchantable, Pulpwood, Chip-n-Saw, Sawtimber)
    land_type : {'forest', 'timber'}, default 'timber'
        Land type to include in estimation.
    tree_type : {'gs', 'al', 'al5', 'sl', 'live', 'sawtimber'}, default 'gs'
        Tree population to include (GRM tables):

        - 'gs': growing stock
        - 'al' or 'live': all live trees at least 1 inch d.b.h., saplings on
          the microplot included (EVALIDator's "trees at least 1 inch")
        - 'al5': all live trees at least 5 inches d.b.h./d.r.c.
          (EVALIDator's "trees at least 5 inches"), from the subplot GRM
          columns; see Notes
        - 'sl' or 'sawtimber': sawtimber-size trees
    measure : {'volume', 'sawlog', 'total_stem', 'biomass', 'green_weight', 'basal_area', 'tpa', 'count'}, default 'volume'
        What to measure, from each tree's midpoint record (TREE_GRM_MIDPT):

        - 'volume': net merchantable bole wood volume (VOLCFNET), in cubic
          feet. FIA computes it for trees at least 5 inches.
        - 'sawlog': net sawlog wood volume (VOLCSNET), in cubic feet. It is
          zero below sawtimber size, so merchantable volume above the sawlog
          is 'volume' minus 'sawlog'. EVALIDator snums 216 (forest land) and
          222 (timberland) with ``tree_type="sl"``.
        - 'total_stem': sound total-stem wood volume, ground to tip
          (VOLTSSND), in cubic feet. FIA computes it for trees at least
          1 inch, so it is the volume measure for saplings. EVALIDator snums
          11159 (forest land) and 11160 (timberland) with ``tree_type="al"``;
          see Notes.
        - 'biomass': dry bole and branch biomass (DRYBIO_BOLE +
          DRYBIO_BRANCH), in dry short tons.
        - 'green_weight': green weight of bole wood and bark, in green short
          tons; see Notes. EVALIDator has no counterpart.
        - 'basal_area': basal area, in square feet.
        - 'tpa' or 'count': number of trees.
    tree_domain : str, optional
        SQL-like filter expression for tree-level filtering.
    area_domain : str, optional
        SQL-like filter expression for area/condition-level filtering.
    as_rate : bool, default False
        If True, also return ``MORT_RATE`` and ``MORT_RATE_SE``, equal to
        ``MORT_ACRE`` and ``MORT_ACRE_SE`` (annual mortality per acre).
    totals : bool, default True
        If True, include population-level total estimates.
    variance : bool, default False
        If True, also return variance columns (``*_VARIANCE``) alongside the
        standard errors (``*_SE``, always returned). Variance = SE squared.
    most_recent : bool, default False
        If True, automatically filter to the most recent evaluation.

    Returns
    -------
    pl.DataFrame
        Mortality estimates with columns:
        - MORT_ACRE: Annual mortality per acre
        - MORT_TOTAL: Total annual mortality (if totals=True)
        - MORT_ACRE_SE: Standard error of per-acre estimate
        - MORT_TOTAL_SE: Standard error of total estimate (if totals=True)
        - MORT_ACRE_VARIANCE, MORT_TOTAL_VARIANCE: Variance of the estimates,
          equal to the matching _SE squared (if variance=True)
        - Additional grouping columns if specified

    See Also
    --------
    mortality_trees : The tree-level frame behind these estimates
    domain_total : Totals and standard errors of a per-tree value you define
    growth : Estimate annual growth using GRM tables
    removals : Estimate annual removals/harvest using GRM tables

    Examples
    --------
    Basic volume mortality on forestland:

    >>> results = mortality(db, measure="volume", land_type="forest")

    Mortality by species (tree count):

    >>> results = mortality(db, by_species=True, measure="tpa")

    Sapling mortality volume (trees 1.0-4.9 inches), in cubic feet per year:

    >>> results = mortality(
    ...     db,
    ...     tree_type="al",  # all live trees at least 1 inch, saplings included
    ...     measure="total_stem",
    ...     tree_domain="DIA_MIDPT < 5",
    ... )

    Sawlog volume of dying sawtimber trees on timberland (EVALIDator snum 222):

    >>> results = mortality(db, tree_type="sl", measure="sawlog")

    Mortality of all live trees at least 5 inches, in trees per year
    (EVALIDator snum 901):

    >>> results = mortality(db, tree_type="al5", land_type="forest", measure="tpa")

    Notes
    -----
    ``tree_type="al5"`` reads the ``SUBP_*_AL_*`` columns of
    TREE_GRM_COMPONENT, the population EVALIDator uses for all live trees at
    least 5 inches. It is not ``tree_type="al"`` limited to trees at least
    5 inches. 'al' reads the ``MICR_*_AL_*`` columns, where a tree that
    grew past 5 inches during the remeasurement period can keep the
    microplot's expansion (``SUBPTYP_GRM`` 2). 'al5' expands each tree by
    its subplot or macroplot. Like EVALIDator, 'al5' applies no midpoint
    diameter filter, so ``size_class_type="market"`` can place a few of its
    trees in "Pre-merchantable".

    This function uses FIA's GRM tables which contain pre-calculated annual
    mortality values. The TPA_UNADJ fields are already annualized.

    **Pre-merchantable Tree Support:**
    By default (``tree_type="gs"``), only growing stock trees (≥5" DBH) are
    included. To include pre-merchantable trees (1.0"-4.9" DBH), use
    ``tree_type="live"``. When using market size classes, pre-merchantable
    trees are automatically categorized as "Pre-merchantable".

    FIA computes merchantable volume ('volume', 'sawlog') only for trees at
    least 5 inches. Use ``measure="total_stem"`` for the volume of smaller
    trees. EVALIDator's total-stem mortality attributes are sound volume
    (VOLTSSND, gross volume less rotten and missing cull); FIADB's gross
    total-stem volume (VOLTSGRS) has no EVALIDator mortality attribute and
    is available through :func:`mortality_trees`. Trees whose VOLTSSND is
    null add nothing to the total, as in EVALIDator. Snums 11159 and 11160
    cover timber species only (REF_SPECIES.WOODLAND = 'N'), while
    ``measure="total_stem"`` covers every species in the population; to
    match them where woodland species grow, leave those species out with
    ``tree_domain``.

    **Green weight** converts each tree's dry bole wood and bark to green
    weight with REF_SPECIES moisture contents, which are percents of
    oven-dry weight::

        DRYBIO_BOLE * (1 + MC_PCT_GREEN_WOOD / 100)
          + DRYBIO_BOLE_BARK * (1 + MC_PCT_GREEN_BARK / 100)

    in pounds, divided by 2,000. EVALIDator publishes no green-weight
    attribute, so these estimates are validated against an independent SQL
    computation only. Trees with null dry biomass add nothing. If a tree in
    the population has dry biomass but its species has no moisture content,
    the estimate raises ``ValueError`` and names the species.
    """
    from ...validation import (
        validate_boolean,
        validate_domain_expression,
        validate_grp_by,
        validate_land_type,
        validate_mortality_measure,
        validate_tree_type_grm,
    )

    land_type = validate_land_type(land_type)
    tree_type = validate_tree_type_grm(tree_type)
    measure = validate_mortality_measure(measure)
    grp_by = validate_grp_by(grp_by)
    tree_domain = validate_domain_expression(tree_domain, "tree_domain")
    area_domain = validate_domain_expression(area_domain, "area_domain")
    by_species = validate_boolean(by_species, "by_species")
    by_size_class = validate_boolean(by_size_class, "by_size_class")
    as_rate = validate_boolean(as_rate, "as_rate")
    totals = validate_boolean(totals, "totals")
    variance = validate_boolean(variance, "variance")
    most_recent = validate_boolean(most_recent, "most_recent")

    valid_size_class_types = ("standard", "descriptive", "market")
    if size_class_type not in valid_size_class_types:
        raise ValueError(
            f"size_class_type must be one of {valid_size_class_types}, got {size_class_type!r}"
        )

    config = {
        "grp_by": grp_by,
        "by_species": by_species,
        "by_size_class": by_size_class,
        "size_class_type": size_class_type,
        "land_type": land_type,
        "tree_type": tree_type,
        "measure": measure,
        "tree_domain": tree_domain,
        "area_domain": area_domain,
        "as_rate": as_rate,
        "totals": totals,
        "variance": variance,
        "most_recent": most_recent,
        "include_cv": False,
    }

    estimator = MortalityEstimator(db, config)
    return estimator.estimate()


def mortality_trees(
    db: str | FIA,
    land_type: str = "timber",
    tree_type: str = "gs",
    tree_domain: str | None = None,
    area_domain: str | None = None,
    columns: str | list[str] | None = None,
) -> pl.DataFrame:
    """
    Return the tree-level frame behind ``mortality()``, one row per dying tree.

    Each row is a mortality record from TREE_GRM_COMPONENT in the database's
    GRM evaluation, with its plot's stratum keys and its adjusted annual
    mortality per acre (``TPA_ADJ``). Add a per-tree value of your own, such
    as a stumpage value, then pass the frame to :func:`domain_total` for
    design-based totals and standard errors.

    Parameters
    ----------
    db : str | FIA
        Database connection or path to FIA database. It must be clipped to
        a GRM evaluation (``db.clip_by_evalid()`` or
        ``db.clip_most_recent(eval_type="GRM")``).
    land_type : {'forest', 'timber'}, default 'timber'
        Land type to include in estimation.
    tree_type : {'gs', 'al', 'al5', 'sl', 'live', 'sawtimber'}, default 'gs'
        Tree population to include, as in ``mortality()``:

        - 'gs': growing stock
        - 'al' or 'live': all live trees at least 1 inch d.b.h., saplings on
          the microplot included
        - 'al5': all live trees at least 5 inches d.b.h./d.r.c., from the
          subplot GRM columns
        - 'sl' or 'sawtimber': sawtimber-size trees
    tree_domain : str, optional
        SQL-like filter expression for tree-level filtering, for example
        ``"DIA_MIDPT < 5"``.
    area_domain : str, optional
        SQL-like filter expression for area/condition-level filtering.
    columns : str or list of str, optional
        Extra columns to return. Each name is looked up in TREE_GRM_MIDPT
        (the tree at the remeasurement midpoint), then COND (the tree's own
        condition), PLOT, then TREE (the tree's time-2 record). Examples:
        ``"VOLCFNET"``, ``"VOLCSNET"``, ``"VOLTSGRS"``, ``"DRYBIO_BOLE"``,
        ``"DSTRBCD1"``, ``"UNITCD"``, ``"AGENTCD"``.

    Returns
    -------
    pl.DataFrame
        One row per mortality tree, sorted by PLT_CN and TRE_CN, with columns:

        - PLT_CN, TRE_CN: plot and tree keys
        - STATECD: state code
        - STRATUM_CN, ESTN_UNIT_CN, EXPNS: the plot's stratum, estimation
          unit and expansion factor (acres per plot)
        - COMPONENT: GRM component (MORTALITY1, MORTALITY2)
        - SUBPTYP_GRM: plot footprint that expands the tree (1 subplot,
          2 microplot, 3 macroplot)
        - DIA_BEGIN, DIA_MIDPT: diameter at time 1 and at the midpoint
        - SPCD: species code
        - TPA_UNADJ: annual mortality trees per acre, unadjusted
        - ADJ_FACTOR: the stratum's adjustment factor for SUBPTYP_GRM
        - TPA_ADJ: TPA_UNADJ * ADJ_FACTOR
        - The requested ``columns``

    See Also
    --------
    domain_total : Totals and standard errors from this frame
    mortality : Mortality estimates for FIA's own measures

    Examples
    --------
    Value dying sawtimber at a sawlog price and a pulpwood price for the
    merchantable volume above the sawlog, by state and disturbance:

    >>> trees = mortality_trees(
    ...     db,
    ...     tree_type="al5",
    ...     columns=["VOLCFNET", "VOLCSNET", "DSTRBCD1", "AGENTCD"],
    ... )
    >>> sawlog = pl.col("VOLCSNET").fill_null(0)
    >>> topwood = pl.col("VOLCFNET").fill_null(0) - sawlog
    >>> trees = trees.with_columns(
    ...     VALUE=sawlog * 0.90 + topwood * 0.20,  # dollars per cubic foot
    ...     LOSS=pl.when(pl.col("DSTRBCD1").is_between(30, 32))
    ...     .then(pl.lit("fire"))
    ...     .otherwise(pl.lit("other")),
    ... )
    >>> by_state = domain_total(db, trees, "VALUE", grp_by=["STATECD", "LOSS"])

    Gross total-stem volume of dying saplings, in cubic feet per year:

    >>> saplings = mortality_trees(
    ...     db, tree_type="al", tree_domain="DIA_MIDPT < 5", columns="VOLTSGRS"
    ... ).with_columns(pl.col("VOLTSGRS").fill_null(0))
    >>> domain_total(db, saplings, "VOLTSGRS")

    Notes
    -----
    The frame holds the same trees ``mortality()`` estimates from, with the
    same population columns, domain filters and condition join (each tree's
    time-2 condition, TREE.CONDID). For ``tree_type="gs"`` it keeps only
    trees with positive VOLCFNET, as ``mortality(measure="volume")`` does.

    Summing ``TPA_ADJ * value * EXPNS`` over the frame gives a total, but
    not its standard error. Use :func:`domain_total` for both: it counts
    every plot in the evaluation, including the plots with no dying tree.
    """
    from ...validation import (
        validate_domain_expression,
        validate_land_type,
        validate_tree_type_grm,
    )

    land_type = validate_land_type(land_type)
    tree_type = validate_tree_type_grm(tree_type)
    tree_domain = validate_domain_expression(tree_domain, "tree_domain")
    area_domain = validate_domain_expression(area_domain, "area_domain")
    if isinstance(columns, str):
        columns = [columns]
    if columns is not None and not all(isinstance(c, str) for c in columns):
        raise TypeError("columns must be a column name or a list of column names")
    if columns is not None:
        columns = [c for c in columns if c not in TREE_FRAME_COLUMNS]

    config = {
        "land_type": land_type,
        "tree_type": tree_type,
        "measure": "volume",
        "tree_domain": tree_domain,
        "area_domain": area_domain,
        "columns": columns,
    }
    return MortalityEstimator(db, config).tree_frame()


def domain_total(
    db: FIA,
    trees: pl.DataFrame,
    value: str | None = None,
    grp_by: str | list[str] | None = None,
    variance: bool = False,
) -> pl.DataFrame:
    """
    Estimate totals and standard errors of a per-tree value over a tree frame.

    The total of each group is the sum over its trees of
    ``TPA_ADJ * value * EXPNS``. Its standard error follows Bechtold and
    Patterson (2005): every plot in the evaluation enters the variance, with
    zero for plots that have no tree in the group, and each estimation unit
    contributes its post-stratified variance.

    Parameters
    ----------
    db : FIA
        The database the frame was built from, still clipped to the same
        evaluation. It supplies every plot in the evaluation and its stratum.
    trees : pl.DataFrame
        A tree frame from :func:`mortality_trees`, with any columns you added.
        It needs ``PLT_CN``, ``TPA_ADJ``, ``value`` and the ``grp_by``
        columns. To leave trees out of an estimate, filter rows or set their
        value to zero; both give the same total and standard error.
    value : str, optional
        Column holding each tree's value per tree, such as cubic feet or
        dollars. It must have no nulls. If None, each tree counts once and
        the total is in trees.
    grp_by : str or list of str, optional
        Columns to group by. Null is a group of its own.
    variance : bool, default False
        If True, also return ``TOTAL_VARIANCE`` (``TOTAL_SE`` squared).

    Returns
    -------
    pl.DataFrame
        One row per group present in ``trees``, sorted by the group columns:

        - The ``grp_by`` columns
        - TOTAL: estimated total of ``value`` per year
        - TOTAL_SE: standard error of TOTAL
        - TOTAL_VARIANCE: TOTAL_SE squared (if variance=True)
        - N_PLOTS: plots with at least one tree in the group
        - N_TREES: tree records in the group

    Raises
    ------
    NoEVALIDError
        If ``db`` is not clipped to an evaluation.
    ValueError
        If a needed column is missing, ``value`` has nulls, a TRE_CN appears
        twice (a join repeated trees), or a tree's plot is not in the
        evaluation of ``db``.

    See Also
    --------
    mortality_trees : Build the tree frame
    mortality : Mortality estimates for FIA's own measures

    Examples
    --------
    Merchantable volume of dying trees at least 5 inches, by state. This
    equals ``mortality(db, tree_type="al5", grp_by="STATECD")``:

    >>> trees = mortality_trees(db, tree_type="al5", columns="VOLCFNET")
    >>> trees = trees.with_columns(pl.col("VOLCFNET").fill_null(0))
    >>> domain_total(db, trees, "VOLCFNET", grp_by="STATECD")

    Notes
    -----
    Group totals add up to the ungrouped total, but their standard errors
    don't: groups share plots, so their errors are correlated. Estimate a
    combined group by grouping on a combined column, never by adding the
    groups' variances.

    Only totals are estimated. A per-acre value needs the domain's area as
    its denominator, which a tree frame doesn't carry.
    """
    from ...validation import validate_boolean, validate_grp_by
    from ..data_loading import DataLoader
    from ..utils import apply_variance_columns
    from ..variance import (
        calculate_grouped_domain_total_variance,
        join_on_group_keys,
    )

    variance = validate_boolean(variance, "variance")
    grp = validate_grp_by(grp_by) or []
    if isinstance(grp, str):
        grp = [grp]
    if not db.evalid:
        raise NoEVALIDError(
            operation="domain_total",
            suggestion=(
                "Clip db to the evaluation the tree frame came from, e.g. "
                "db.clip_most_recent(eval_type='GRM')."
            ),
        )

    needed = ["PLT_CN", "TPA_ADJ"] + ([value] if value else []) + grp
    missing = [c for c in needed if c not in trees.columns]
    if missing:
        raise ValueError(f"trees is missing columns {missing}")
    if value and trees[value].null_count():
        raise ValueError(
            f"{trees[value].null_count()} trees have a null {value!r}. Fill "
            f"them (for example with fill_null(0)) or filter them out first."
        )
    if "TRE_CN" in trees.columns and trees["TRE_CN"].is_duplicated().any():
        raise ValueError(
            "trees has repeated TRE_CN rows; a join may have duplicated trees"
        )

    # Group keys run under internal names so a group column can share a name
    # with a design column (ESTN_UNIT_CN, EXPNS, ...).
    keys = [f"_grp{i}" for i in range(len(grp))]
    y = pl.col("TPA_ADJ").cast(pl.Float64)
    if value:
        y = y * pl.col(value).cast(pl.Float64)
    plot_y = (
        trees.select(
            ["PLT_CN"]
            + [pl.col(c).alias(k) for c, k in zip(grp, keys, strict=True)]
            + [y.alias("_y")]
        )
        .group_by(["PLT_CN"] + keys)
        .agg(pl.sum("_y").alias("y_i"), pl.len().alias("_n_trees"))
    )

    strat = DataLoader(db, {}).get_stratification_data()
    strat_schema = strat.collect_schema().names()
    bp_cols = ["ESTN_UNIT_CN", "STRATUM_WGT", "AREA_USED", "P2POINTCNT"]
    all_plots = (
        strat.select(
            ["PLT_CN", "STRATUM_CN", "EXPNS"]
            + [c for c in bp_cols if c in strat_schema]
        )
        .unique()
        .collect()
    )

    outside = plot_y.select("PLT_CN").unique().join(all_plots, on="PLT_CN", how="anti")
    if outside.height:
        raise ValueError(
            f"{outside.height} plots in trees are not in the evaluation of db "
            f"(EVALID {db.evalid}). Build the tree frame from the same db."
        )

    if keys:
        expanded = join_on_group_keys(
            all_plots.join(plot_y.select(keys).unique(), how="cross"),
            plot_y,
            ["PLT_CN"] + keys,
        )
    else:
        expanded = all_plots.join(plot_y, on="PLT_CN", how="left")
    expanded = expanded.with_columns(
        pl.col("y_i").fill_null(0.0), pl.col("_n_trees").fill_null(0)
    )

    summary = [
        (pl.col("y_i") * pl.col("EXPNS").cast(pl.Float64)).sum().alias("TOTAL"),
        (pl.col("_n_trees") > 0).sum().cast(pl.UInt32).alias("N_PLOTS"),
        pl.col("_n_trees").sum().cast(pl.UInt32).alias("N_TREES"),
    ]
    var = calculate_grouped_domain_total_variance(expanded, keys, "y_i").select(
        keys + [pl.col("se_total").alias("TOTAL_SE")]
    )
    if keys:
        totals = expanded.group_by(keys).agg(summary)
        results = join_on_group_keys(totals, var, keys).sort(keys, nulls_last=True)
    else:
        results = expanded.select(summary).with_columns(var["TOTAL_SE"])

    results = results.select(
        [pl.col(k).alias(c) for k, c in zip(keys, grp, strict=True)]
        + ["TOTAL", "TOTAL_SE", "N_PLOTS", "N_TREES"]
    )
    return apply_variance_columns(results, variance)
