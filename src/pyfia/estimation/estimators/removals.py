"""
Removals estimation for FIA data.

Simple implementation for calculating average annual removals of merchantable
bole wood volume of growing-stock trees.
"""

from typing import Dict, List, Optional, Union

import polars as pl

from ...core import FIA
from ..base import BaseEstimator
from ..grm import (
    GRMColumns,
    aggregate_cond_to_plot,
    apply_grm_adjustment,
    calculate_ratio_variance,
    filter_by_evalid,
    get_grm_required_tables,
    load_grm_component,
    load_grm_midpt,
    resolve_grm_columns,
)
from ..utils import format_output_columns


class RemovalsEstimator(BaseEstimator):
    """
    Removals estimator for FIA data.

    Estimates average annual removals of merchantable bole wood volume of
    growing-stock trees (at least 5 inches d.b.h.) on forest land.
    """

    def __init__(self, db, config):
        """Initialize with storage for variance calculation."""
        super().__init__(db, config)
        self.plot_tree_data = None  # Store for variance calculation
        self.group_cols = None  # Store grouping columns
        self._grm_columns: Optional[GRMColumns] = None

    def get_required_tables(self) -> List[str]:
        """Removals requires tree growth/removal/mortality tables."""
        return get_grm_required_tables("removals")

    def get_tree_columns(self) -> List[str]:
        """Required tree columns for removals estimation."""
        cols = ["CN", "PLT_CN", "CONDID", "STATUSCD", "SPCD", "DIA", "TPA_UNADJ"]

        measure = self.config.get("measure", "volume")
        if measure == "biomass":
            cols.extend(["DRYBIO_AG", "DRYBIO_BG"])

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
        Load and join required tables including GRM component tables.

        EVALIDator calculates removals as: TPAREMV_UNADJ * VOLCFNET * ADJ * EXPNS
        NOT using the pre-calculated REMVCFGS column (which doesn't include ADJ).
        """
        tree_type = self.config.get("tree_type", "gs")
        land_type = self.config.get("land_type", "forest")
        measure = self.config.get("measure", "volume")

        # Resolve GRM column names using shared helper
        self._grm_columns = resolve_grm_columns(
            component_type="removals",
            tree_type=tree_type,
            land_type=land_type,
        )

        # Load GRM component table
        grm_component = load_grm_component(
            self.db,
            self._grm_columns,
            include_dia_end=False,
        )

        # Load GRM midpt table for volume/biomass data
        grm_midpt = load_grm_midpt(self.db, measure=measure)

        # Join component with midpt
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

        return data

    def apply_filters(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """Apply removals-specific filters.

        Filter to trees with positive TPAREMV_UNADJ values.
        """
        data_df = data.collect()

        # Apply tree domain filter if specified
        if self.config.get("tree_domain"):
            from pyfia.filtering.core.parser import DomainExpressionParser

            data_df = DomainExpressionParser.apply_to_dataframe(
                data_df, self.config["tree_domain"], "tree"
            )

        # Apply area domain filter if specified
        if self.config.get("area_domain"):
            from pyfia.filtering.area.filters import apply_area_filters

            data_df = apply_area_filters(
                data_df, area_domain=self.config["area_domain"]
            )

        data = data_df.lazy()

        # Filter to trees with positive removal TPA
        data = data.filter(
            pl.col("TPA_UNADJ").is_not_null() & (pl.col("TPA_UNADJ") > 0)
        )

        # Apply tree type filter if specified
        tree_type = self.config.get("tree_type", "gs")
        if tree_type == "gs":
            # Growing stock trees (>= 5 inches DBH)
            data = data.filter(pl.col("DIA_MIDPT") >= 5.0)

        return data

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

    def aggregate_results(self, data: pl.LazyFrame) -> pl.DataFrame:
        """Aggregate removals with two-stage aggregation for correct per-acre estimates."""
        # Get stratification data
        strat_data = self._get_stratification_data()

        # Join with stratification
        data_with_strat = data.join(strat_data, on="PLT_CN", how="inner")

        # Apply GRM-specific adjustment factors using shared helper
        data_with_strat = apply_grm_adjustment(data_with_strat)

        # Apply adjustment to removal values
        data_with_strat = data_with_strat.with_columns(
            [(pl.col("REMV_ANNUAL") * pl.col("ADJ_FACTOR")).alias("REMV_ADJ")]
        )

        # Setup grouping
        group_cols = self._setup_grouping()
        self.group_cols = group_cols

        # Store plot-tree level data for variance calculation using shared helper
        self.plot_tree_data, data_with_strat = self._preserve_plot_tree_data(
            data_with_strat,
            metric_cols=["REMV_ADJ"],
            group_cols=group_cols,
        )

        # Use shared two-stage aggregation method
        metric_mappings = {"REMV_ADJ": "CONDITION_REMOVALS"}

        results = self._apply_two_stage_aggregation(
            data_with_strat=data_with_strat,
            metric_mappings=metric_mappings,
            group_cols=group_cols,
            use_grm_adjustment=True,
        )

        # Rename columns
        rename_map = {"REMOVALS_ACRE": "REMV_ACRE", "REMOVALS_TOTAL": "REMV_TOTAL"}

        for old, new in rename_map.items():
            if old in results.columns:
                results = results.rename({old: new})

        if "N_TREES" in results.columns:
            results = results.rename({"N_TREES": "N_REMOVED_TREES"})

        return results

    def calculate_variance(self, results: pl.DataFrame) -> pl.DataFrame:
        """Calculate variance for removals estimates using proper ratio estimation formula."""
        if self.plot_tree_data is None:
            import warnings

            warnings.warn(
                "Plot-tree data not available for proper variance calculation. "
                "Using placeholder 20% CV."
            )
            results = results.with_columns(
                [
                    (pl.col("REMV_ACRE") * 0.20).alias("REMV_ACRE_SE"),
                    (pl.col("REMV_TOTAL") * 0.20).alias("REMV_TOTAL_SE"),
                ]
            )
            results = results.with_columns(
                [
                    (pl.col("REMV_ACRE_SE") / pl.col("REMV_ACRE") * 100).alias(
                        "REMV_ACRE_CV"
                    ),
                    (pl.col("REMV_TOTAL_SE") / pl.col("REMV_TOTAL") * 100).alias(
                        "REMV_TOTAL_CV"
                    ),
                ]
            )
            return results

        # Aggregate to plot-condition level
        plot_group_cols = ["PLT_CN", "CONDID", "EXPNS"]
        if "STRATUM_CN" in self.plot_tree_data.columns:
            plot_group_cols.insert(2, "STRATUM_CN")

        if self.group_cols:
            for col in self.group_cols:
                if col in self.plot_tree_data.columns and col not in plot_group_cols:
                    plot_group_cols.append(col)

        plot_cond_data = self.plot_tree_data.group_by(plot_group_cols).agg(
            [pl.sum("REMV_ADJ").alias("y_remv_ic")]
        )

        # Aggregate to plot level
        plot_level_cols = ["PLT_CN", "EXPNS"]
        if "STRATUM_CN" in plot_cond_data.columns:
            plot_level_cols.insert(1, "STRATUM_CN")
        if self.group_cols:
            plot_level_cols.extend(
                [c for c in self.group_cols if c in plot_cond_data.columns]
            )

        plot_data = plot_cond_data.group_by(plot_level_cols).agg(
            [
                pl.sum("y_remv_ic").alias("y_i"),
                pl.lit(1.0).alias("x_i"),
            ]
        )

        # Calculate variance using shared helper
        if self.group_cols:
            strat_data = self._get_stratification_data()
            all_plots = (
                strat_data.select("PLT_CN", "STRATUM_CN", "EXPNS").unique().collect()
            )

            variance_results = []

            for group_vals in results.iter_rows():
                group_filter = pl.lit(True)
                group_dict = {}

                for i, col in enumerate(self.group_cols):
                    if col in plot_data.columns:
                        group_dict[col] = group_vals[results.columns.index(col)]
                        group_filter = group_filter & (
                            pl.col(col) == group_vals[results.columns.index(col)]
                        )

                group_plot_data = plot_data.filter(group_filter)

                all_plots_group = all_plots.join(
                    group_plot_data.select(["PLT_CN", "y_i", "x_i"]),
                    on="PLT_CN",
                    how="left",
                ).with_columns(
                    [pl.col("y_i").fill_null(0.0), pl.col("x_i").fill_null(0.0)]
                )

                if len(all_plots_group) > 0:
                    var_stats = calculate_ratio_variance(all_plots_group, "y_i")
                    variance_results.append(
                        {
                            **group_dict,
                            "REMV_ACRE_SE": var_stats["se_acre"],
                            "REMV_TOTAL_SE": var_stats["se_total"],
                        }
                    )
                else:
                    variance_results.append(
                        {**group_dict, "REMV_ACRE_SE": 0.0, "REMV_TOTAL_SE": 0.0}
                    )

            if variance_results:
                var_df = pl.DataFrame(variance_results)
                results = results.join(var_df, on=self.group_cols, how="left")
        else:
            var_stats = calculate_ratio_variance(plot_data, "y_i")
            results = results.with_columns(
                [
                    pl.lit(var_stats["se_acre"]).alias("REMV_ACRE_SE"),
                    pl.lit(var_stats["se_total"]).alias("REMV_TOTAL_SE"),
                ]
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
        measure = self.config.get("measure", "volume")

        # Extract actual inventory year using shared helper
        year = self._extract_evaluation_year()

        results = results.with_columns(
            [
                pl.lit(year).alias("YEAR"),
                pl.lit(measure.upper()).alias("MEASURE"),
                pl.lit("REMOVALS").alias("ESTIMATE_TYPE"),
            ]
        )

        results = format_output_columns(
            results, estimation_type="removals", include_se=True, include_cv=True
        )

        column_renames = {
            "REMV_ACRE": "REMOVALS_PER_ACRE",
            "REMV_TOTAL": "REMOVALS_TOTAL",
            "REMV_ACRE_SE": "REMOVALS_PER_ACRE_SE",
            "REMV_TOTAL_SE": "REMOVALS_TOTAL_SE",
            "REMV_ACRE_CV": "REMOVALS_PER_ACRE_CV",
            "REMV_TOTAL_CV": "REMOVALS_TOTAL_CV",
        }

        results = results.rename(column_renames)

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
