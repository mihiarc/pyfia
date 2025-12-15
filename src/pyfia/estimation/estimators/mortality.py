"""
Mortality estimation for FIA data using GRM tables.

Implements FIA's Growth-Removal-Mortality methodology for calculating
annual tree mortality using TREE_GRM_COMPONENT and TREE_GRM_MIDPT tables.
"""

from typing import List, Literal, Optional, Union

import polars as pl

from ...core import FIA
from ..base import GRMBaseEstimator


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

    def get_component_filter(self) -> Optional[pl.Expr]:
        """Filter to mortality components only."""
        return pl.col("COMPONENT").str.starts_with("MORTALITY")

    def load_data(self) -> Optional[pl.LazyFrame]:
        """Load GRM data for mortality estimation."""
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

        plot = plot.select(["CN", "STATECD", "INVYR", "MACRO_BREAKPOINT_DIA"])
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
                        / 2000.0
                    ).alias("MORT_VALUE")
                ]
            )
        elif measure == "basal_area":
            data = data.with_columns(
                [
                    (
                        pl.col("TPA_UNADJ").cast(pl.Float64)
                        * (pl.col("DIA").cast(pl.Float64) ** 2 * 0.005454154)
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

    def aggregate_results(self, data: pl.LazyFrame) -> pl.DataFrame:  # type: ignore[override]
        """Aggregate mortality with two-stage aggregation."""
        # Use shared GRM aggregation
        results = self._aggregate_grm_results(
            data,
            value_col="MORT_ANNUAL",
            adjusted_col="MORT_ADJ",
        )

        # Rename columns to mortality-specific names
        rename_map = {"MORT_ACRE": "MORT_ACRE", "MORT_TOTAL": "MORT_TOTAL"}
        for old, new in rename_map.items():
            if old in results.columns:
                results = results.rename({old: new})

        if "N_TREES" in results.columns:
            results = results.rename({"N_TREES": "N_DEAD_TREES"})

        # Calculate mortality rate if requested
        if self.config.get("as_rate", False):
            results = results.with_columns([pl.col("MORT_ACRE").alias("MORT_RATE")])

        return results

    def calculate_variance(self, results: pl.DataFrame) -> pl.DataFrame:
        """Calculate variance for mortality estimates using ratio-of-means formula.

        Implements Bechtold & Patterson (2005) stratified variance calculation.
        MORT_RATE uses the same variance as MORT_ACRE since they represent the
        same per-acre estimate (MORT_RATE = mortality per acre, not mortality/growing_stock).
        """
        results = self._calculate_grm_variance(
            results,
            adjusted_col="MORT_ADJ",
            acre_se_col="MORT_ACRE_SE",
            total_se_col="MORT_TOTAL_SE",
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
    db: Union[str, FIA],
    grp_by: Optional[Union[str, List[str]]] = None,
    by_species: bool = False,
    by_size_class: bool = False,
    land_type: str = "timber",
    tree_type: str = "gs",
    measure: str = "volume",
    tree_domain: Optional[str] = None,
    area_domain: Optional[str] = None,
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
    db : Union[str, FIA]
        Database connection or path to FIA database.
    grp_by : str or list of str, optional
        Column name(s) to group results by.
    by_species : bool, default False
        If True, group results by species code (SPCD).
    by_size_class : bool, default False
        If True, group results by diameter size classes.
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

    >>> results = mortality(db, by_species=True, measure="count")

    Notes
    -----
    This function uses FIA's GRM tables which contain pre-calculated annual
    mortality values. The TPA_UNADJ fields are already annualized.
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

    config = {
        "grp_by": grp_by,
        "by_species": by_species,
        "by_size_class": by_size_class,
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
