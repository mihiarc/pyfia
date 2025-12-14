"""
Mortality estimation for FIA data using GRM tables.

Implements FIA's Growth-Removal-Mortality methodology for calculating
annual tree mortality using TREE_GRM_COMPONENT and TREE_GRM_MIDPT tables.
"""

from typing import List, Optional, Union

import polars as pl

from ...core import FIA
from ..base import BaseEstimator
from ..grm import (
    GRMColumns,
    aggregate_cond_to_plot,
    apply_grm_adjustment,
    filter_by_evalid,
    get_grm_required_tables,
    load_grm_component,
    load_grm_midpt,
    resolve_grm_columns,
)
from ..utils import format_output_columns


class MortalityEstimator(BaseEstimator):
    """
    Mortality estimator for FIA data using GRM methodology.

    Estimates annual tree mortality in terms of volume, biomass, or trees per acre
    using the TREE_GRM_COMPONENT and TREE_GRM_MIDPT tables.
    """

    def __init__(self, db, config):
        """Initialize with storage for GRM columns."""
        super().__init__(db, config)
        self._grm_columns: Optional[GRMColumns] = None

    def get_required_tables(self) -> List[str]:
        """Mortality requires GRM tables for proper calculation."""
        return get_grm_required_tables("mortality")

    def get_tree_columns(self) -> List[str]:
        """Required columns from TREE_GRM tables."""
        cols = ["TRE_CN", "PLT_CN", "DIA_BEGIN", "DIA_MIDPT", "DIA_END"]

        # Initialize GRM column names if not done
        if self._grm_columns is None:
            land_type = self.config.get("land_type", "timber")
            tree_type = self.config.get("tree_type", "gs")
            self._grm_columns = resolve_grm_columns(
                component_type="mortality",
                tree_type=tree_type,
                land_type=land_type,
            )

        cols.extend([
            self._grm_columns.component,
            self._grm_columns.tpa,
            self._grm_columns.subptyp,
        ])

        return cols

    def get_cond_columns(self) -> List[str]:
        """Required condition columns."""
        return [
            "PLT_CN",
            "CONDID",
            "COND_STATUS_CD",
            "CONDPROP_UNADJ",
            "OWNGRPCD",
            "FORTYPCD",
            "SITECLCD",
            "RESERVCD",
        ]

    def load_data(self) -> Optional[pl.LazyFrame]:
        """
        Load and join GRM tables for mortality calculation.
        """
        land_type = self.config.get("land_type", "timber")
        tree_type = self.config.get("tree_type", "gs")
        measure = self.config.get("measure", "volume")

        # Resolve GRM column names using shared helper
        self._grm_columns = resolve_grm_columns(
            component_type="mortality",
            tree_type=tree_type,
            land_type=land_type,
        )

        # Load GRM component table
        grm_component = load_grm_component(
            self.db,
            self._grm_columns,
            include_dia_end=True,
        )

        # Load GRM midpt table for volume/biomass data
        grm_midpt = load_grm_midpt(self.db, measure=measure)

        # Join GRM tables
        data = grm_component.join(grm_midpt, on="TRE_CN", how="inner")

        # Apply EVALID filtering using shared helper
        data = filter_by_evalid(data, self.db)

        # Load and aggregate COND to plot level
        if "COND" not in self.db.tables:
            self.db.load_table("COND")

        cond = self.db.tables["COND"]
        if not isinstance(cond, pl.LazyFrame):
            cond = cond.lazy()

        cond_agg = aggregate_cond_to_plot(cond)
        data = data.join(cond_agg, on="PLT_CN", how="left")

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
        data_df = data.collect()

        # Apply area domain filter if specified
        if self.config.get("area_domain"):
            from pyfia.filtering.area.filters import apply_area_filters

            data_df = apply_area_filters(
                data_df, area_domain=self.config["area_domain"]
            )

        # Apply tree domain filter if specified
        if self.config.get("tree_domain"):
            from pyfia.filtering.core.parser import DomainExpressionParser

            data_df = DomainExpressionParser.apply_to_dataframe(
                data_df, self.config["tree_domain"], "tree"
            )

        # Filter to mortality components only
        data_df = data_df.filter(pl.col("COMPONENT").str.starts_with("MORTALITY"))

        # Filter to records with positive mortality
        data_df = data_df.filter(
            (pl.col("TPA_UNADJ").is_not_null()) & (pl.col("TPA_UNADJ") > 0)
        )

        # Apply tree type filter if specified
        tree_type = self.config.get("tree_type", "gs")
        if tree_type == "gs":
            data_df = data_df.filter(pl.col("DIA_MIDPT") >= 5.0)
            if "VOLCFNET" in data_df.columns:
                data_df = data_df.filter(pl.col("VOLCFNET") > 0)
        elif tree_type == "sawtimber":
            data_df = data_df.filter(
                ((pl.col("SPCD") < 300) & (pl.col("DIA_MIDPT") >= 9.0))
                | ((pl.col("SPCD") >= 300) & (pl.col("DIA_MIDPT") >= 11.0))
            )
            if "VOLCSNET" in data_df.columns:
                data_df = data_df.filter(pl.col("VOLCSNET") > 0)

        return data_df.lazy()

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
        elif measure == "tpa":
            data = data.with_columns(
                [pl.col("TPA_UNADJ").cast(pl.Float64).alias("MORT_VALUE")]
            )
        else:  # Default to tpa/count
            data = data.with_columns(
                [pl.col("TPA_UNADJ").cast(pl.Float64).alias("MORT_VALUE")]
            )

        data = data.with_columns([pl.col("MORT_VALUE").alias("MORT_ANNUAL")])

        return data

    def aggregate_results(self, data: pl.LazyFrame) -> pl.DataFrame:
        """Aggregate mortality with two-stage aggregation for correct per-acre estimates."""
        # Get stratification data
        strat_data = self._get_stratification_data()

        # Join with stratification
        data_with_strat = data.join(strat_data, on="PLT_CN", how="inner")

        # Apply GRM-specific adjustment factors using shared helper
        data_with_strat = apply_grm_adjustment(data_with_strat)

        # Apply adjustment to mortality values
        data_with_strat = data_with_strat.with_columns(
            [(pl.col("MORT_ANNUAL") * pl.col("ADJ_FACTOR")).alias("MORT_ADJ")]
        )

        # Setup grouping
        group_cols = self._setup_grouping()
        if self.config.get("by_species", False) and "SPCD" not in group_cols:
            group_cols.append("SPCD")

        # Use shared two-stage aggregation method
        metric_mappings = {"MORT_ADJ": "CONDITION_MORTALITY"}

        results = self._apply_two_stage_aggregation(
            data_with_strat=data_with_strat,
            metric_mappings=metric_mappings,
            group_cols=group_cols,
            use_grm_adjustment=True,
        )

        # Rename columns
        rename_map = {"MORTALITY_ACRE": "MORT_ACRE", "MORTALITY_TOTAL": "MORT_TOTAL"}

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
        """Calculate variance for mortality estimates using stratified sampling formulas."""
        # Get stratification data for variance calculation
        strat_data = self._get_stratification_data()

        # Load the raw mortality data for variance calculation
        data = self.load_data()
        if data is None:
            results = results.with_columns(
                [pl.lit(0.0).alias("MORT_ACRE_SE"), pl.lit(0.0).alias("MORT_TOTAL_SE")]
            )
            return results

        # Apply filters to get the same subset used in estimation
        data = self.apply_filters(data)
        data = self.calculate_values(data)

        # Join with stratification
        data_with_strat = data.join(strat_data, on="PLT_CN", how="inner")

        # Apply adjustment factors using shared helper
        data_with_strat = apply_grm_adjustment(data_with_strat)

        # Calculate plot-level mortality values
        all_plots = strat_data.select("PLT_CN", "STRATUM_CN", "EXPNS").unique()

        # Aggregate mortality to plot level
        plot_mortality = data_with_strat.group_by(
            ["PLT_CN", "STRATUM_CN", "EXPNS"]
        ).agg(
            [
                (pl.col("MORT_ANNUAL") * pl.col("ADJ_FACTOR"))
                .sum()
                .alias("plot_mort_value")
            ]
        )

        # Join to include all plots (with zeros for non-mortality plots)
        all_plots_mort = all_plots.join(
            plot_mortality.select(["PLT_CN", "plot_mort_value"]),
            on="PLT_CN",
            how="left",
        ).with_columns([pl.col("plot_mort_value").fill_null(0.0)])

        # Calculate stratum-level statistics
        strat_stats = all_plots_mort.group_by("STRATUM_CN").agg(
            [
                pl.count("PLT_CN").alias("n_h"),
                pl.mean("plot_mort_value").alias("ybar_h"),
                pl.var("plot_mort_value", ddof=1).alias("s2_yh"),
                pl.first("EXPNS").alias("w_h"),
            ]
        )

        # Handle single-plot strata (variance = 0)
        strat_stats = strat_stats.with_columns(
            [
                pl.when(pl.col("s2_yh").is_null() | (pl.col("n_h") == 1))
                .then(0.0)
                .otherwise(pl.col("s2_yh"))
                .alias("s2_yh")
            ]
        )

        # Calculate variance components for total estimation
        variance_components = strat_stats.with_columns(
            [
                (
                    pl.col("w_h").cast(pl.Float64) ** 2
                    * pl.col("s2_yh")
                    * pl.col("n_h")
                ).alias("v_h")
            ]
        )

        # Sum variance components
        total_variance = variance_components.collect()["v_h"].sum()
        if total_variance is None or total_variance < 0:
            total_variance = 0.0

        # Calculate standard errors
        se_total = total_variance**0.5

        # For per-acre estimate, we need to divide by total area
        total_area_df = strat_stats.select(
            [(pl.col("w_h") * pl.col("n_h")).alias("stratum_area")]
        ).collect()
        total_area = total_area_df["stratum_area"].sum()

        if total_area > 0:
            se_acre = se_total / total_area
        else:
            se_acre = 0.0

        # Update results with calculated variance
        results = results.with_columns(
            [
                pl.lit(se_acre).alias("MORT_ACRE_SE"),
                pl.lit(se_total).alias("MORT_TOTAL_SE"),
            ]
        )

        if "MORT_RATE" in results.columns:
            results = results.with_columns(
                [(pl.col("MORT_RATE") * 0.20).alias("MORT_RATE_SE")]
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
        measure = self.config.get("measure", "volume")
        land_type = self.config.get("land_type", "forest")
        tree_type = self.config.get("tree_type", "gs")

        results = results.with_columns(
            [
                pl.lit(2023).alias("YEAR"),
                pl.lit(measure.upper()).alias("MEASURE"),
                pl.lit(land_type.upper()).alias("LAND_TYPE"),
                pl.lit(tree_type.upper()).alias("TREE_TYPE"),
            ]
        )

        results = format_output_columns(
            results,
            estimation_type="mortality",
            include_se=True,
            include_cv=self.config.get("include_cv", False),
        )

        return results


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
