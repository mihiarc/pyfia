"""
Removals estimation for FIA data.

Simple implementation for calculating average annual removals of merchantable
bole wood volume of growing-stock trees.
"""

from typing import Dict, List, Optional, Union

import polars as pl

from ...core import FIA
from ..base import BaseEstimator
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

    def get_required_tables(self) -> List[str]:
        """Removals requires tree growth/removal/mortality tables."""
        return [
            "TREE",
            "COND",
            "PLOT",
            "POP_PLOT_STRATUM_ASSGN",
            "POP_STRATUM",
            "TREE_GRM_COMPONENT",
            "TREE_GRM_MIDPT",
        ]

    def get_tree_columns(self) -> List[str]:
        """Required tree columns for removals estimation.

        Note: Grouping columns (grp_by) are handled by the base class's
        _load_tree_cond_data method which properly determines whether each
        grouping column exists in TREE or COND tables.
        """
        cols = ["CN", "PLT_CN", "CONDID", "STATUSCD", "SPCD", "DIA", "TPA_UNADJ"]

        # Add columns based on what we're measuring
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
        """
        # Use base class to load standard tree/condition data
        data = super().load_data()

        if data is None:
            return None

        # Now augment with GRM-specific data
        # Load TREE_GRM_COMPONENT table
        if "TREE_GRM_COMPONENT" not in self.db.tables:
            try:
                self.db.load_table("TREE_GRM_COMPONENT")
            except Exception as e:
                # If GRM tables don't exist, return None or raise error
                raise ValueError(f"TREE_GRM_COMPONENT table not found in database: {e}")

        grm_component = self.db.tables["TREE_GRM_COMPONENT"]

        # Ensure LazyFrame
        if not isinstance(grm_component, pl.LazyFrame):
            grm_component = grm_component.lazy()

        # Select and rename GRM columns
        # IMPORTANT: Use pre-calculated REMVCFGS_FOREST for volume removals
        # This column is already annualized and uses proper FIA methodology
        # Do NOT use VOLCFNET * TPAREMV_UNADJ which gives incorrect results
        tree_type = self.config.get("tree_type", "gs")
        land_type = self.config.get("land_type", "forest")

        # Build column selection based on tree_type and land_type
        # GS = Growing Stock, AL = All Live, SL = Sawtimber/Sawlog
        type_suffix = "GS" if tree_type == "gs" else "AL"
        land_suffix = "FOREST" if land_type in ("forest", "all") else "TIMBER"

        grm_cols = [
            pl.col("TRE_CN"),
            pl.col("DIA_BEGIN"),
            pl.col("DIA_MIDPT"),
            pl.col(f"SUBP_COMPONENT_{type_suffix}_{land_suffix}").alias("COMPONENT"),
            pl.col(f"SUBP_SUBPTYP_GRM_{type_suffix}_{land_suffix}").alias("SUBPTYP_GRM"),
            pl.col(f"SUBP_TPAREMV_UNADJ_{type_suffix}_{land_suffix}").alias("TPAREMV_UNADJ"),
            # Pre-calculated annual removal volume (cu ft/year)
            pl.col(f"REMVCF{type_suffix}_{land_suffix}").alias("REMV_VOLUME"),
        ]

        grm_component = grm_component.select(grm_cols)

        # Join with GRM component data
        data = data.join(grm_component, left_on="CN", right_on="TRE_CN", how="left")

        # Add PLOT data for macro breakpoint
        # NOTE: We avoid calling collect_schema() on a LazyFrame with pending joins
        # because Polars may fail to resolve the schema if grouping columns exist
        # in a different table than expected. Instead, always add PLOT columns.
        if "PLOT" not in self.db.tables:
            self.db.load_table("PLOT")

        plot = self.db.tables["PLOT"]
        if not isinstance(plot, pl.LazyFrame):
            plot = plot.lazy()

        # Select PLOT columns needed for GRM estimation
        # Using suffix to handle potential column name conflicts
        plot_cols = plot.select(
            [
                pl.col("CN").alias("PLOT_CN"),
                pl.col("MACRO_BREAKPOINT_DIA"),
            ]
        )

        data = data.join(plot_cols, left_on="PLT_CN", right_on="PLOT_CN", how="left")

        return data

    def apply_filters(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """Apply removals-specific filters.

        IMPORTANT: For GRM estimates using pre-calculated columns (REMVCFGS_FOREST),
        we should NOT apply standard COND_STATUS_CD filtering. The "FOREST" suffix
        in the column name already indicates it's forest land removals.

        The condition at T2 (current inventory) might be non-forest (e.g., after
        harvest the land was converted to non-forest use), but the removal event
        occurred when the land was forest. The pre-calculated columns account for
        this correctly.
        """
        # Collect to DataFrame for filtering functions
        data_df = data.collect()

        # Apply tree domain filter if specified (custom SQL-like filter)
        if self.config.get("tree_domain"):
            from ..filters import apply_tree_filters

            data_df = apply_tree_filters(
                data_df, tree_domain=self.config["tree_domain"]
            )

        # Apply area domain filter if specified (custom SQL-like filter)
        if self.config.get("area_domain"):
            from ..filters import apply_area_filters

            data_df = apply_area_filters(
                data_df, area_domain=self.config["area_domain"]
            )

        # NOTE: We intentionally do NOT apply tree_type or land_type filters here
        # because the pre-calculated REMVCFGS_FOREST column already handles these:
        # - GS suffix = growing stock trees (DBH >= 5")
        # - FOREST suffix = forest land at beginning of remeasurement period

        # Convert back to lazy
        data = data_df.lazy()

        # Filter to trees with removal values
        # REMV_VOLUME is the pre-calculated annual removal volume
        # It's only non-null/non-zero for trees that were actually removed
        data = data.filter(
            pl.col("REMV_VOLUME").is_not_null() & (pl.col("REMV_VOLUME") != 0)
        )

        return data

    def calculate_values(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """
        Calculate removal values.

        Uses pre-calculated FIA columns:
        - REMV_VOLUME: Pre-calculated annual removal volume (REMVCFGS/REMVCFAL)
          This is already annualized - no need to divide by remeasurement period.

        For biomass and count, we still need to calculate from tree attributes.
        """
        measure = self.config.get("measure", "volume")

        if measure == "volume":
            # Use pre-calculated annual removal volume (cu ft/year per tree)
            # REMV_VOLUME is already annualized by FIA
            data = data.with_columns(
                [pl.col("REMV_VOLUME").cast(pl.Float64).alias("REMV_ANNUAL")]
            )
        elif measure == "biomass":
            # For biomass, use TPAREMV_UNADJ × biomass / REMPER
            # We need to load TREE_GRM_ESTN or calculate from attributes
            # For now, estimate from volume using standard conversion
            # Biomass (tons) ≈ Volume (cuft) × 0.03 (approximate conversion)
            # TODO: Use proper DRYBIO columns when available
            data = data.with_columns(
                [
                    (
                        pl.col("REMV_VOLUME").cast(pl.Float64) * 0.03  # Approximate
                    ).alias("REMV_ANNUAL")
                ]
            )
        else:  # count
            # For tree count, use TPAREMV_UNADJ
            # Need to annualize by dividing by remeasurement period
            remeasure_period = self.config.get("remeasure_period", 5.0)
            data = data.with_columns(
                [
                    (
                        pl.col("TPAREMV_UNADJ").cast(pl.Float64) / remeasure_period
                    ).alias("REMV_ANNUAL")
                ]
            )

        return data

    def aggregate_results(self, data: pl.LazyFrame) -> pl.DataFrame:
        """Aggregate removals with two-stage aggregation for correct per-acre estimates.

        Uses the shared _apply_two_stage_aggregation method with GRM-specific adjustment
        logic applied before calling the shared method.
        """
        # Get stratification data
        strat_data = self._get_stratification_data()

        # Join with stratification
        data_with_strat = data.join(strat_data, on="PLT_CN", how="inner")

        # Apply GRM-specific adjustment factors based on SUBPTYP_GRM
        # This is done BEFORE calling the shared aggregation method
        # SUBPTYP_GRM indicates which adjustment factor to use:
        # 0 = No adjustment, 1 = SUBP, 2 = MICR, 3 = MACR
        # This is different from the standard tree adjustment which uses DIA size classes
        data_with_strat = data_with_strat.with_columns(
            [
                pl.when(pl.col("SUBPTYP_GRM") == 0)
                .then(0.0)
                .when(pl.col("SUBPTYP_GRM") == 1)
                .then(pl.col("ADJ_FACTOR_SUBP"))
                .when(pl.col("SUBPTYP_GRM") == 2)
                .then(pl.col("ADJ_FACTOR_MICR"))
                .when(pl.col("SUBPTYP_GRM") == 3)
                .then(pl.col("ADJ_FACTOR_MACR"))
                .otherwise(0.0)
                .alias("ADJ_FACTOR")
            ]
        )

        # Apply adjustment to removal values
        data_with_strat = data_with_strat.with_columns(
            [(pl.col("REMV_ANNUAL") * pl.col("ADJ_FACTOR")).alias("REMV_ADJ")]
        )

        # Setup grouping
        group_cols = self._setup_grouping()
        self.group_cols = group_cols  # Store for variance calculation

        # CRITICAL: Store plot-tree level data for variance calculation
        data_collected = data_with_strat.collect()
        available_cols = data_collected.columns

        # Build column list for preservation
        cols_to_preserve = ["PLT_CN", "CONDID"]

        # Add stratification columns
        if "STRATUM_CN" in available_cols:
            cols_to_preserve.append("STRATUM_CN")
        if "ESTN_UNIT" in available_cols:
            cols_to_preserve.append("ESTN_UNIT")
        elif "UNITCD" in available_cols:
            data_collected = data_collected.with_columns(
                pl.col("UNITCD").alias("ESTN_UNIT")
            )
            cols_to_preserve.append("ESTN_UNIT")

        # Add essential columns for variance calculation
        cols_to_preserve.extend(["REMV_ADJ", "ADJ_FACTOR", "CONDPROP_UNADJ", "EXPNS"])

        # Add grouping columns if they exist
        if group_cols:
            for col in group_cols:
                if col in available_cols and col not in cols_to_preserve:
                    cols_to_preserve.append(col)

        # Store the plot-tree data for variance calculation
        self.plot_tree_data = data_collected.select(
            [c for c in cols_to_preserve if c in data_collected.columns]
        )

        # Convert back to lazy for two-stage aggregation
        data_with_strat = data_collected.lazy()

        # Use shared two-stage aggregation method
        metric_mappings = {"REMV_ADJ": "CONDITION_REMOVALS"}

        results = self._apply_two_stage_aggregation(
            data_with_strat=data_with_strat,
            metric_mappings=metric_mappings,
            group_cols=group_cols,
            use_grm_adjustment=True,  # Indicates this is a GRM-based estimator
        )

        # The shared method returns REMOVALS_ACRE and REMOVALS_TOTAL
        # Rename to match removals-specific naming convention
        rename_map = {"REMOVALS_ACRE": "REMV_ACRE", "REMOVALS_TOTAL": "REMV_TOTAL"}

        for old, new in rename_map.items():
            if old in results.columns:
                results = results.rename({old: new})

        # Rename N_TREES to N_REMOVED_TREES for clarity in removals context
        if "N_TREES" in results.columns:
            results = results.rename({"N_TREES": "N_REMOVED_TREES"})

        return results

    def calculate_variance(self, results: pl.DataFrame) -> pl.DataFrame:
        """Calculate variance for removals estimates using proper ratio estimation formula.

        Removals estimation uses ratio-of-means: R = Y/X where Y is removal value and X is area.
        The variance formula accounts for covariance between numerator and denominator.

        Following Bechtold & Patterson (2005) methodology for stratified sampling.
        """
        if self.plot_tree_data is None:
            # Fallback to conservative estimate
            import warnings

            warnings.warn(
                "Plot-tree data not available for proper variance calculation. "
                "Using placeholder 20% CV. To enable proper variance, ensure data "
                "preservation is working correctly."
            )
            results = results.with_columns(
                [
                    (pl.col("REMV_ACRE") * 0.20).alias("REMV_ACRE_SE"),
                    (pl.col("REMV_TOTAL") * 0.20).alias("REMV_TOTAL_SE"),
                ]
            )
            # Add coefficient of variation
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

        # Step 1: Aggregate to plot-condition level
        # Sum removals within each condition (trees are already adjusted)
        plot_group_cols = ["PLT_CN", "CONDID", "EXPNS"]
        if "STRATUM_CN" in self.plot_tree_data.columns:
            plot_group_cols.insert(2, "STRATUM_CN")

        # Add grouping columns
        if self.group_cols:
            for col in self.group_cols:
                if col in self.plot_tree_data.columns and col not in plot_group_cols:
                    plot_group_cols.append(col)

        plot_cond_agg = [
            pl.sum("REMV_ADJ").alias("y_remv_ic"),  # Removals per condition
        ]

        plot_cond_data = self.plot_tree_data.group_by(plot_group_cols).agg(
            plot_cond_agg
        )

        # Step 2: Aggregate to plot level
        plot_level_cols = ["PLT_CN", "EXPNS"]
        if "STRATUM_CN" in plot_cond_data.columns:
            plot_level_cols.insert(1, "STRATUM_CN")
        if self.group_cols:
            plot_level_cols.extend(
                [c for c in self.group_cols if c in plot_cond_data.columns]
            )

        plot_data = plot_cond_data.group_by(plot_level_cols).agg(
            [
                pl.sum("y_remv_ic").alias("y_i"),  # Total removals per plot
                pl.lit(1.0).alias("x_i"),  # Area proportion per plot (full plot = 1)
            ]
        )

        # Step 3: Calculate variance for each group or overall
        if self.group_cols:
            # Get ALL plots in the evaluation for proper variance calculation
            strat_data = self._get_stratification_data()
            all_plots = (
                strat_data.select("PLT_CN", "STRATUM_CN", "EXPNS").unique().collect()
            )

            # Calculate variance for each group separately
            variance_results = []

            for group_vals in results.iter_rows():
                # Build filter for this group
                group_filter = pl.lit(True)
                group_dict = {}

                for i, col in enumerate(self.group_cols):
                    if col in plot_data.columns:
                        group_dict[col] = group_vals[results.columns.index(col)]
                        group_filter = group_filter & (
                            pl.col(col) == group_vals[results.columns.index(col)]
                        )

                # Filter plot data for this specific group
                group_plot_data = plot_data.filter(group_filter)

                # Join with ALL plots, filling missing with zeros
                all_plots_group = all_plots.join(
                    group_plot_data.select(["PLT_CN", "y_i", "x_i"]),
                    on="PLT_CN",
                    how="left",
                ).with_columns(
                    [pl.col("y_i").fill_null(0.0), pl.col("x_i").fill_null(0.0)]
                )

                if len(all_plots_group) > 0:
                    # Calculate variance using ALL plots (including zeros)
                    var_stats = self._calculate_ratio_variance(all_plots_group, "y_i")

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

            # Join variance results back to main results
            if variance_results:
                var_df = pl.DataFrame(variance_results)
                results = results.join(var_df, on=self.group_cols, how="left")
        else:
            # No grouping, calculate overall variance
            var_stats = self._calculate_ratio_variance(plot_data, "y_i")

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

    def _calculate_ratio_variance(self, plot_data: pl.DataFrame, y_col: str) -> Dict:
        """Calculate variance for ratio-of-means estimator.

        For ratio estimation R = Y/X, the variance formula is:
        V(R) ≈ (1/X̄²) × Σ_h w_h² × [s²_yh + R² × s²_xh - 2R × s_yxh] / n_h

        Where:
        - Y is the numerator (removals)
        - X is the denominator (area)
        - R is the ratio estimate
        - s_yxh is the covariance between Y and X in stratum h
        - w_h is the stratum weight (EXPNS)
        - n_h is the number of plots in stratum h
        """
        # Determine stratification columns
        strat_cols = ["STRATUM_CN"] if "STRATUM_CN" in plot_data.columns else []

        if not strat_cols:
            # No stratification, treat as single stratum
            plot_data = plot_data.with_columns(pl.lit(1).alias("STRATUM"))
            strat_cols = ["STRATUM"]

        # Calculate stratum-level statistics
        strata_stats = plot_data.group_by(strat_cols).agg(
            [
                pl.count("PLT_CN").alias("n_h"),
                pl.mean(y_col).alias("ybar_h"),
                pl.mean("x_i").alias("xbar_h"),
                pl.var(y_col, ddof=1).alias("s2_yh"),
                pl.var("x_i", ddof=1).alias("s2_xh"),
                pl.first("EXPNS").cast(pl.Float64).alias("w_h"),
                # Calculate covariance
                (
                    (
                        (pl.col(y_col) - pl.col(y_col).mean())
                        * (pl.col("x_i") - pl.col("x_i").mean())
                    ).sum()
                    / (pl.len() - 1)
                ).alias("cov_yxh"),
            ]
        )

        # Handle null variances
        strata_stats = strata_stats.with_columns(
            [
                pl.when(pl.col("s2_yh").is_null())
                .then(0.0)
                .otherwise(pl.col("s2_yh"))
                .cast(pl.Float64)
                .alias("s2_yh"),
                pl.when(pl.col("s2_xh").is_null())
                .then(0.0)
                .otherwise(pl.col("s2_xh"))
                .cast(pl.Float64)
                .alias("s2_xh"),
                pl.when(pl.col("cov_yxh").is_null())
                .then(0.0)
                .otherwise(pl.col("cov_yxh"))
                .cast(pl.Float64)
                .alias("cov_yxh"),
                pl.col("xbar_h").cast(pl.Float64).alias("xbar_h"),
                pl.col("ybar_h").cast(pl.Float64).alias("ybar_h"),
            ]
        )

        # Calculate population totals
        total_y = (
            strata_stats["ybar_h"] * strata_stats["w_h"] * strata_stats["n_h"]
        ).sum()
        total_x = (
            strata_stats["xbar_h"] * strata_stats["w_h"] * strata_stats["n_h"]
        ).sum()

        # Calculate ratio estimate
        ratio = total_y / total_x if total_x > 0 else 0

        # Filter out single-plot strata (variance undefined with n=1)
        # These strata cannot contribute to variance estimation
        strata_with_variance = strata_stats.filter(pl.col("n_h") > 1)

        # Calculate variance components only for strata with n > 1
        variance_components = strata_with_variance.with_columns(
            [
                (
                    pl.col("w_h") ** 2
                    * (
                        pl.col("s2_yh")
                        + ratio**2 * pl.col("s2_xh")
                        - 2 * ratio * pl.col("cov_yxh")
                    )
                    * pl.col("n_h")
                ).alias("v_h")
            ]
        )

        # Sum variance components, handling NaN values
        variance_of_numerator = variance_components["v_h"].drop_nans().sum()
        if variance_of_numerator is None or variance_of_numerator < 0:
            variance_of_numerator = 0.0

        # Convert to variance of the ratio
        variance_of_ratio = variance_of_numerator / (total_x**2) if total_x > 0 else 0.0

        # Standard errors
        se_acre = variance_of_ratio**0.5
        se_total = se_acre * total_x if total_x > 0 else 0

        return {
            "variance_acre": variance_of_ratio,
            "variance_total": (se_total**2) if se_total > 0 else 0,
            "se_acre": se_acre,
            "se_total": se_total,
            "ratio": ratio,
            "total_y": total_y,
            "total_x": total_x,
        }

    def format_output(self, results: pl.DataFrame) -> pl.DataFrame:
        """Format removals estimation output."""
        # Add metadata columns
        measure = self.config.get("measure", "volume")

        # Try to extract actual year from data if available
        year = self.config.get("year", 2023)
        if "INVYR" in results.columns:
            year = results["INVYR"].max()

        results = results.with_columns(
            [
                pl.lit(year).alias("YEAR"),
                pl.lit(measure.upper()).alias("MEASURE"),
                pl.lit("REMOVALS").alias("ESTIMATE_TYPE"),
            ]
        )

        # Format columns
        results = format_output_columns(
            results, estimation_type="removals", include_se=True, include_cv=True
        )

        # Rename columns for clarity
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
    # Import validation functions
    from ...validation import (
        validate_boolean,
        validate_domain_expression,
        validate_grp_by,
        validate_land_type,
        validate_mortality_measure,  # Reuse for removals measure
        validate_positive_number,
        validate_tree_type,
    )

    # Validate inputs
    land_type = validate_land_type(land_type)
    tree_type = validate_tree_type(tree_type)
    measure = validate_mortality_measure(measure)  # Same valid values as mortality
    grp_by = validate_grp_by(grp_by)
    tree_domain = validate_domain_expression(tree_domain, "tree_domain")
    area_domain = validate_domain_expression(area_domain, "area_domain")
    by_species = validate_boolean(by_species, "by_species")
    by_size_class = validate_boolean(by_size_class, "by_size_class")
    totals = validate_boolean(totals, "totals")
    variance = validate_boolean(variance, "variance")
    most_recent = validate_boolean(most_recent, "most_recent")
    remeasure_period = validate_positive_number(remeasure_period, "remeasure_period")

    # Create config
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

    # Create and run estimator
    estimator = RemovalsEstimator(db, config)
    return estimator.estimate()
