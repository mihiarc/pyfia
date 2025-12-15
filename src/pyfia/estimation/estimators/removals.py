"""
Removals estimation for FIA data.

Simple implementation for calculating average annual removals of merchantable
bole wood volume of growing-stock trees.
"""

from typing import List, Literal, Optional, Union

import polars as pl

from ...core import FIA
from ..base import GRMBaseEstimator


class RemovalsEstimator(GRMBaseEstimator):
    """
    Removals estimator for FIA data.

    Estimates average annual removals of merchantable bole wood volume of
    growing-stock trees (at least 5 inches d.b.h.) on forest land.
    """

    @property
    def component_type(self) -> Literal["growth", "mortality", "removals"]:
        """Return 'removals' as the GRM component type."""
        return "removals"

    def get_tree_columns(self) -> List[str]:
        """Required tree columns for removals estimation."""
        cols = ["CN", "PLT_CN", "CONDID", "STATUSCD", "SPCD", "DIA", "TPA_UNADJ"]

        measure = self.config.get("measure", "volume")
        if measure == "biomass":
            cols.extend(["DRYBIO_AG", "DRYBIO_BG"])

        return cols

    def load_data(self) -> Optional[pl.LazyFrame]:
        """Load GRM data for removals estimation."""
        # Use the simple GRM data loading pattern
        return self._load_simple_grm_data()

    def apply_filters(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """Apply removals-specific filters."""
        # Use common GRM filter application
        return self._apply_grm_filters(data)

    def calculate_values(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """
        Calculate removal values.

        EVALIDator methodology: TPAREMV_UNADJ * VOLCFNET
        TPAREMV_UNADJ is already annualized (trees removed per acre per year).
        """
        measure = self.config.get("measure", "volume")

        if measure == "volume":
            data = data.with_columns(
                [
                    (
                        pl.col("TPA_UNADJ").cast(pl.Float64)
                        * pl.col("VOLCFNET").cast(pl.Float64)
                    ).alias("REMV_ANNUAL")
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
                        / 2000.0  # Convert pounds to tons
                    ).alias("REMV_ANNUAL")
                ]
            )
        else:  # count
            data = data.with_columns(
                [pl.col("TPA_UNADJ").cast(pl.Float64).alias("REMV_ANNUAL")]
            )

        return data

    def aggregate_results(self, data: pl.LazyFrame) -> pl.DataFrame:  # type: ignore[override]
        """Aggregate removals with two-stage aggregation."""
        # Use shared GRM aggregation
        results = self._aggregate_grm_results(
            data,
            value_col="REMV_ANNUAL",
            adjusted_col="REMV_ADJ",
        )

        # Rename columns to removals-specific names
        rename_map = {"REMV_ACRE": "REMV_ACRE", "REMV_TOTAL": "REMV_TOTAL"}
        for old, new in rename_map.items():
            if old in results.columns:
                results = results.rename({old: new})

        if "N_TREES" in results.columns:
            results = results.rename({"N_TREES": "N_REMOVED_TREES"})

        return results

    def calculate_variance(self, results: pl.DataFrame) -> pl.DataFrame:
        """Calculate variance for removals estimates using ratio-of-means formula.

        Implements Bechtold & Patterson (2005) stratified variance calculation.
        """
        results = self._calculate_grm_variance(
            results,
            adjusted_col="REMV_ADJ",
            acre_se_col="REMV_ACRE_SE",
            total_se_col="REMV_TOTAL_SE",
        )

        # Add coefficient of variation
        results = results.with_columns(
            [
                pl.when(pl.col("REMV_ACRE") > 0)
                .then(pl.col("REMV_ACRE_SE") / pl.col("REMV_ACRE") * 100)
                .otherwise(0.0)
                .alias("REMV_ACRE_CV"),
                pl.when(pl.col("REMV_TOTAL") > 0)
                .then(pl.col("REMV_TOTAL_SE") / pl.col("REMV_TOTAL") * 100)
                .otherwise(0.0)
                .alias("REMV_TOTAL_CV"),
            ]
        )

        return results

    def format_output(self, results: pl.DataFrame) -> pl.DataFrame:
        """Format removals estimation output."""
        results = self._format_grm_output(
            results,
            estimation_type="removals",
            include_cv=True,
        )

        # Rename to removals-specific column names
        column_renames = {
            "REMV_ACRE": "REMOVALS_PER_ACRE",
            "REMV_TOTAL": "REMOVALS_TOTAL",
            "REMV_ACRE_SE": "REMOVALS_PER_ACRE_SE",
            "REMV_TOTAL_SE": "REMOVALS_TOTAL_SE",
            "REMV_ACRE_CV": "REMOVALS_PER_ACRE_CV",
            "REMV_TOTAL_CV": "REMOVALS_TOTAL_CV",
        }

        for old, new in column_renames.items():
            if old in results.columns:
                results = results.rename({old: new})

        return results


def removals(
    db: Union[str, FIA],
    grp_by: Optional[Union[str, List[str]]] = None,
    by_species: bool = False,
    by_size_class: bool = False,
    land_type: str = "forest",
    tree_type: str = "gs",
    measure: str = "volume",
    tree_domain: Optional[str] = None,
    area_domain: Optional[str] = None,
    totals: bool = True,
    variance: bool = False,
    most_recent: bool = False,
    remeasure_period: float = 5.0,
) -> pl.DataFrame:
    """
    Estimate average annual removals from FIA data.

    Calculates average annual removals of merchantable bole wood volume of
    growing-stock trees (at least 5 inches d.b.h.) on forest land.

    Parameters
    ----------
    db : Union[str, FIA]
        Database connection or path
    grp_by : Optional[Union[str, List[str]]]
        Columns to group by (e.g., "STATECD", "FORTYPCD")
    by_species : bool
        Group by species code
    by_size_class : bool
        Group by diameter size classes
    land_type : str
        Land type: "forest", "timber", or "all"
    tree_type : str
        Tree type: "gs" (growing stock), "all"
    measure : str
        What to measure: "volume", "biomass", or "count"
    tree_domain : Optional[str]
        SQL-like filter for trees
    area_domain : Optional[str]
        SQL-like filter for area
    totals : bool
        Include population totals
    variance : bool
        Return variance instead of SE
    most_recent : bool
        Use most recent evaluation
    remeasure_period : float
        Remeasurement period in years for annualization

    Returns
    -------
    pl.DataFrame
        Removals estimates with columns:
        - REMOVALS_PER_ACRE: Annual removals per acre
        - REMOVALS_TOTAL: Total annual removals
        - REMOVALS_PER_ACRE_SE: Standard error of per-acre estimate
        - REMOVALS_TOTAL_SE: Standard error of total estimate
        - Additional grouping columns if specified

    Examples
    --------
    >>> # Basic volume removals on forestland
    >>> results = removals(db, measure="volume")

    >>> # Removals by species (tree count)
    >>> results = removals(db, by_species=True, measure="count")

    >>> # Biomass removals by forest type
    >>> results = removals(
    ...     db,
    ...     grp_by="FORTYPCD",
    ...     measure="biomass"
    ... )

    >>> # Removals on timberland only
    >>> results = removals(
    ...     db,
    ...     land_type="timber",
    ...     area_domain="SITECLCD >= 225"  # Productive sites
    ... )

    Notes
    -----
    Removals include trees cut or otherwise removed from the inventory,
    including those diverted to non-forest use. The calculation uses
    TREE_GRM_COMPONENT table with CUT and DIVERSION components.

    The estimate is annualized by dividing by the remeasurement period
    (default 5 years).
    """
    from ...validation import (
        validate_boolean,
        validate_domain_expression,
        validate_grp_by,
        validate_land_type,
        validate_mortality_measure,
        validate_positive_number,
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
    totals = validate_boolean(totals, "totals")
    variance = validate_boolean(variance, "variance")
    most_recent = validate_boolean(most_recent, "most_recent")
    remeasure_period = validate_positive_number(remeasure_period, "remeasure_period")

    config = {
        "grp_by": grp_by,
        "by_species": by_species,
        "by_size_class": by_size_class,
        "land_type": land_type,
        "tree_type": tree_type,
        "measure": measure,
        "tree_domain": tree_domain,
        "area_domain": area_domain,
        "totals": totals,
        "variance": variance,
        "most_recent": most_recent,
        "remeasure_period": remeasure_period,
    }

    estimator = RemovalsEstimator(db, config)
    return estimator.estimate()
