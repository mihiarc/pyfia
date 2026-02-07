"""
Mortality estimation for FIA data using GRM tables.

Implements FIA's Growth-Removal-Mortality methodology for calculating
annual tree mortality using TREE_GRM_COMPONENT and TREE_GRM_MIDPT tables.
"""

from __future__ import annotations

from typing import Literal

import polars as pl

from ...core import FIA
from ..base import AggregationResult
from ..constants import BASAL_AREA_FACTOR, LBS_TO_SHORT_TONS
from ..grm_base import GRMBaseEstimator


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

        plot = plot.select(plot_cols)
        data = data.join(plot, left_on="PLT_CN", right_on="CN", how="left")

        return data

    def apply_filters(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """Apply mortality-specific filters."""
        # Use common GRM filter application
        data = self._apply_grm_filters(data)

        # Additional mortality-specific filters
        tree_type = self.config.get("tree_type", "gs")

        if tree_type == "gs":
            # Ensure volume is positive for growing stock
            schema = data.collect_schema().names()
            if "VOLCFNET" in schema:
                data = data.filter(pl.col("VOLCFNET") > 0)
        elif tree_type == "sawtimber":
            # Sawtimber requires larger diameter thresholds
            data = data.filter(
                ((pl.col("SPCD") < 300) & (pl.col("DIA_MIDPT") >= 9.0))
                | ((pl.col("SPCD") >= 300) & (pl.col("DIA_MIDPT") >= 11.0))
            )
            schema = data.collect_schema().names()
            if "VOLCSNET" in schema:
                data = data.filter(pl.col("VOLCSNET") > 0)

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

    def aggregate_results(self, data: pl.LazyFrame) -> AggregationResult:  # type: ignore[override]
        """Aggregate mortality with two-stage aggregation.

        Returns
        -------
        AggregationResult
            Bundle containing results, plot_tree_data, and group_cols for
            explicit variance calculation.
        """
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

    def calculate_variance(self, agg_result: AggregationResult) -> pl.DataFrame:  # type: ignore[override]
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
    tree_type : {'gs', 'al', 'sawtimber', 'live'}, default 'gs'
        Tree type to include.
    measure : {'volume', 'sawlog', 'biomass', 'tpa', 'count', 'basal_area'}, default 'volume'
        What to measure in the mortality estimation.
    tree_domain : str, optional
        SQL-like filter expression for tree-level filtering.
    area_domain : str, optional
        SQL-like filter expression for area/condition-level filtering.
    as_rate : bool, default False
        If True, return mortality as a rate (mortality/live).
    totals : bool, default True
        If True, include population-level total estimates.
    variance : bool, default False
        If True, calculate and include variance and standard error estimates.
    most_recent : bool, default False
        If True, automatically filter to the most recent evaluation.

    Returns
    -------
    pl.DataFrame
        Mortality estimates with columns:
        - MORT_ACRE: Annual mortality per acre
        - MORT_TOTAL: Total annual mortality (if totals=True)
        - MORT_ACRE_SE: Standard error of per-acre estimate (if variance=True)
        - MORT_TOTAL_SE: Standard error of total estimate (if variance=True)
        - Additional grouping columns if specified

    See Also
    --------
    growth : Estimate annual growth using GRM tables
    removals : Estimate annual removals/harvest using GRM tables

    Examples
    --------
    Basic volume mortality on forestland:

    >>> results = mortality(db, measure="volume", land_type="forest")

    Mortality by species (tree count):

    >>> results = mortality(db, by_species=True, measure="tpa")

    Pre-merchantable tree mortality (trees < 5" DBH):

    >>> results = mortality(
    ...     db,
    ...     tree_type="live",  # Include all live trees, not just growing stock
    ...     by_size_class=True,
    ...     size_class_type="market",  # Returns Pre-merchantable, Pulpwood, etc.
    ...     measure="tpa",  # TPA recommended for small trees (no volume calculated)
    ... )
    >>> # Filter to pre-merchantable only:
    >>> premerch = results.filter(pl.col("SIZE_CLASS") == "Pre-merchantable")

    Notes
    -----
    This function uses FIA's GRM tables which contain pre-calculated annual
    mortality values. The TPA_UNADJ fields are already annualized.

    **Pre-merchantable Tree Support:**
    By default (``tree_type="gs"``), only growing stock trees (≥5" DBH) are
    included. To include pre-merchantable trees (1.0"-4.9" DBH), use
    ``tree_type="live"`` or ``tree_type="al"``. When using market size classes,
    pre-merchantable trees are automatically categorized as "Pre-merchantable".

    Note that FIA does not calculate volume for trees < 5" DBH, so
    ``measure="tpa"`` is recommended for pre-merchantable mortality analysis.
    For economic valuation of pre-merchantable mortality, consider the
    "discount timber price" method (Bruck et al.) which values small trees
    based on their future merchantable potential.
    """
    from ...validation import (
        validate_boolean,
        validate_domain_expression,
        validate_grp_by,
        validate_land_type,
        validate_mortality_measure,
        validate_tree_type,
    )

    land_type = validate_land_type(land_type)
    tree_type = validate_tree_type(tree_type)
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
