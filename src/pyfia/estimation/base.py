"""
Base estimator for FIA statistical estimation.

This module provides the base class for all FIA estimators using a simple,
straightforward approach without unnecessary abstractions.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass

import polars as pl

from ..constants.defaults import EVALIDYearParsing
from ..core import FIA
from ..filtering import (
    apply_area_filters,
    apply_tree_filters,
    get_land_domain_indicator,
)
from .aggregation import (
    aggregate_to_condition_level as _aggregate_to_condition_level_impl,
)
from .aggregation import (
    aggregate_to_population_level as _aggregate_to_population_level_impl,
)
from .aggregation import (
    apply_two_stage_aggregation as _apply_two_stage_aggregation_impl,
)
from .aggregation import (
    compute_per_acre_values as _compute_per_acre_values_impl,
)
from .data_loading import DataLoader

logger = logging.getLogger(__name__)


@dataclass
class AggregationResult:
    """
    Bundle of data produced by aggregate_results() for use in calculate_variance().

    This dataclass explicitly passes the data needed for variance calculation,
    replacing the previous pattern of setting instance state as side effects
    (self.plot_tree_data, self.group_cols) which was fragile implicit coupling.

    Attributes
    ----------
    results : pl.DataFrame
        The aggregated estimation results (per-acre values, totals, counts).
    plot_tree_data : pl.DataFrame
        Plot-tree level data preserved for variance calculation. Contains
        the individual measurements needed to compute variance following
        Bechtold & Patterson (2005) methodology.
    group_cols : list[str]
        The grouping columns used in aggregation. Needed for grouped variance
        calculation to ensure variance is computed for each group.
    """

    results: pl.DataFrame
    plot_tree_data: pl.DataFrame
    group_cols: list[str]


class BaseEstimator(ABC):
    """
    Base class for FIA design-based estimators.

    Implements a simple Template Method pattern for the estimation workflow
    without unnecessary abstractions like FrameWrapper, complex caching, or
    deep inheritance hierarchies.
    """

    def __init__(self, db: str | FIA, config: dict):
        """
        Initialize the estimator.

        Parameters
        ----------
        db : str | FIA
            Database connection or path
        config : dict
            Configuration dictionary with estimation parameters
        """
        # Set up database connection
        if isinstance(db, str):
            self.db = FIA(db)
            self._owns_db = True
        else:
            self.db = db
            self._owns_db = False

        # Store config as simple dict
        self.config = config

        # Initialize data loader for composition-based data loading
        self.data_loader = DataLoader(self.db, self.config)

        # Simple caches for commonly used data
        self._ref_species_cache: pl.DataFrame | None = None
        self._stratification_cache: pl.LazyFrame | None = None

    def estimate(self) -> pl.DataFrame:
        """
        Main estimation workflow.

        Returns
        -------
        pl.DataFrame
            Final estimation results
        """
        # 1. Load required data
        data = self.load_data()

        # 2. Apply filters (domain filtering)
        if data is not None:
            data = self.apply_filters(data)

        # 3. Calculate estimation values
        if data is not None:
            data = self.calculate_values(data)

        # 4. Aggregate results with stratification
        # Returns AggregationResult with results, plot_tree_data, and group_cols
        agg_result = self.aggregate_results(data)

        # 5. Calculate variance using explicit AggregationResult
        results = self.calculate_variance(agg_result)

        # 6. Format output
        return self.format_output(results)

    def load_data(self) -> pl.LazyFrame | None:
        """
        Load and join required tables.

        Delegates to DataLoader for actual data loading operations.

        Returns
        -------
        pl.LazyFrame | None
            Joined data or None if no tree data needed
        """
        tables = self.get_required_tables()
        tree_columns = self.get_tree_columns()
        cond_columns = self.get_cond_columns()

        return self.data_loader.load_data(
            required_tables=tables,
            tree_columns=tree_columns,
            cond_columns=cond_columns,
        )

    def apply_filters(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """
        Apply domain filtering.

        This method applies all configured filters (tree domain, area domain,
        tree type, land type) directly on the LazyFrame without materializing
        the data, enabling efficient memory usage for large datasets.

        Parameters
        ----------
        data : pl.LazyFrame
            Input data

        Returns
        -------
        pl.LazyFrame
            Filtered data
        """
        # Get column names for conditional filtering (single schema collection)
        columns = data.collect_schema().names()

        # Apply tree domain filter (works with LazyFrames)
        if self.config.get("tree_domain"):
            data = apply_tree_filters(data, tree_domain=self.config["tree_domain"])

        # Apply area domain filter (works with LazyFrames)
        if self.config.get("area_domain"):
            data = apply_area_filters(data, area_domain=self.config["area_domain"])

        # Apply tree type filter (live, dead, etc.)
        tree_type = self.config.get("tree_type", "live")
        if tree_type and "STATUSCD" in columns:
            if tree_type == "live":
                data = data.filter(pl.col("STATUSCD") == 1)
            elif tree_type == "dead":
                data = data.filter(pl.col("STATUSCD") == 2)
            elif tree_type == "gs":
                # Growing stock = live trees (STATUSCD=1) with TREECLCD=2
                # TREECLCD: 2=Growing stock, 3=Rough cull, 4=Rotten cull
                gs_filter = pl.col("STATUSCD") == 1
                if "TREECLCD" in columns:
                    gs_filter = gs_filter & (pl.col("TREECLCD") == 2)
                data = data.filter(gs_filter)
            # "all" means no filter

        # Apply land type filter using centralized indicator function
        # This replaces magic numbers with named constants from status_codes.py
        land_type = self.config.get("land_type", "forest")
        if land_type and land_type != "all" and "COND_STATUS_CD" in columns:
            data = data.filter(get_land_domain_indicator(land_type))

        return data

    def aggregate_results(
        self, data: pl.LazyFrame | None
    ) -> AggregationResult | pl.DataFrame:
        """
        Aggregate results with stratification.

        Subclasses should override this to return AggregationResult for proper
        variance calculation. The base implementation returns a DataFrame for
        backward compatibility.

        Parameters
        ----------
        data : pl.LazyFrame | None
            Calculated values or None for area-only

        Returns
        -------
        AggregationResult | pl.DataFrame
            AggregationResult with results, plot_tree_data, and group_cols,
            or DataFrame for backward compatibility
        """
        # Get stratification data
        strat_data = self._get_stratification_data()

        if data is None:
            # Area-only estimation
            return self._aggregate_area_only(strat_data)

        # Join with stratification
        data_with_strat = data.join(strat_data, on="PLT_CN", how="inner")

        # Setup grouping columns
        group_cols = self._setup_grouping()

        # Aggregate by groups
        if group_cols:
            results = (
                data_with_strat.group_by(group_cols)
                .agg(
                    [
                        pl.sum("ESTIMATE_VALUE").alias("ESTIMATE"),
                        pl.count("PLT_CN").alias("N_PLOTS"),
                    ]
                )
                .collect()
            )
        else:
            results = data_with_strat.select(
                [
                    pl.sum("ESTIMATE_VALUE").alias("ESTIMATE"),
                    pl.count("PLT_CN").alias("N_PLOTS"),
                ]
            ).collect()

        return results

    def calculate_variance(
        self, agg_result: AggregationResult | pl.DataFrame
    ) -> pl.DataFrame:
        """
        Calculate variance for estimates.

        Parameters
        ----------
        agg_result : AggregationResult | pl.DataFrame
            Either an AggregationResult containing results, plot_tree_data,
            and group_cols for explicit data passing, or a DataFrame for
            backward compatibility with subclasses that haven't been updated.

        Returns
        -------
        pl.DataFrame
            Results with variance columns added

        Raises
        ------
        NotImplementedError
            If called on the base class without override.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement calculate_variance() "
            "with proper stratified ratio-of-means variance calculation. "
            "See Bechtold & Patterson (2005) for methodology."
        )

    def format_output(self, results: pl.DataFrame) -> pl.DataFrame:
        """
        Format output to match expected structure.

        Parameters
        ----------
        results : pl.DataFrame
            Raw results

        Returns
        -------
        pl.DataFrame
            Formatted results
        """
        # Add metadata columns
        results = results.with_columns(
            [pl.lit(self.config.get("year", 2023)).alias("YEAR")]
        )

        # Reorder columns
        col_order = ["YEAR", "ESTIMATE", "SE", "N_PLOTS"]
        existing_cols = [col for col in col_order if col in results.columns]
        other_cols = [col for col in results.columns if col not in col_order]

        results = results.select(existing_cols + other_cols)

        return results

    def _setup_grouping(self) -> list[str]:
        """Setup grouping columns based on config."""
        group_cols = []

        # Custom grouping columns
        if self.config.get("grp_by"):
            grp_by = self.config["grp_by"]
            if isinstance(grp_by, str):
                group_cols.append(grp_by)
            else:
                group_cols.extend(grp_by)

        # Species grouping
        if self.config.get("by_species"):
            group_cols.append("SPCD")

        # Size class grouping would be added here
        # but requires the actual data to create the column

        return group_cols

    def _aggregate_to_condition_level(
        self,
        data_with_strat: pl.LazyFrame,
        metric_mappings: dict[str, str],
        group_cols: list[str],
        available_cols: list[str],
    ) -> tuple[pl.LazyFrame, list[str]]:
        """
        Stage 1: Aggregate metrics to plot-condition level.

        Each condition's area proportion (CONDPROP_UNADJ) is counted exactly once.
        Trees within a condition are summed together.

        Delegates to the pure function in aggregation module.

        Parameters
        ----------
        data_with_strat : pl.LazyFrame
            Data with stratification columns joined
        metric_mappings : dict[str, str]
            Mapping of adjusted metrics to condition-level aggregates
        group_cols : list[str]
            User-specified grouping columns
        available_cols : list[str]
            Available columns in the data

        Returns
        -------
        tuple[pl.LazyFrame, list[str]]
            Condition-level aggregated data and the grouping columns used
        """
        return _aggregate_to_condition_level_impl(
            data_with_strat, metric_mappings, group_cols, available_cols
        )

    def _aggregate_to_population_level(
        self,
        condition_agg: pl.LazyFrame,
        metric_mappings: dict[str, str],
        group_cols: list[str],
        condition_group_cols: list[str],
    ) -> pl.LazyFrame:
        """
        Stage 2: Apply expansion factors and calculate population estimates.

        Condition-level values are expanded using stratification factors (EXPNS).

        Delegates to the pure function in aggregation module.

        Parameters
        ----------
        condition_agg : pl.LazyFrame
            Condition-level aggregated data
        metric_mappings : dict[str, str]
            Mapping of adjusted metrics to condition-level aggregates
        group_cols : list[str]
            User-specified grouping columns
        condition_group_cols : list[str]
            Columns used in condition-level grouping

        Returns
        -------
        pl.LazyFrame
            Population-level aggregated results
        """
        return _aggregate_to_population_level_impl(
            condition_agg, metric_mappings, group_cols, condition_group_cols
        )

    def _compute_per_acre_values(
        self,
        results_df: pl.DataFrame,
        metric_mappings: dict[str, str],
    ) -> pl.DataFrame:
        """
        Calculate per-acre values using ratio-of-means and clean up intermediate columns.

        Per-acre estimates = sum(metric x EXPNS) / sum(CONDPROP_UNADJ x EXPNS)

        Delegates to the pure function in aggregation module.

        Parameters
        ----------
        results_df : pl.DataFrame
            Population-level results with numerator, total, and area columns
        metric_mappings : dict[str, str]
            Mapping of adjusted metrics to condition-level aggregates

        Returns
        -------
        pl.DataFrame
            Results with per-acre values calculated and intermediate columns removed
        """
        return _compute_per_acre_values_impl(results_df, metric_mappings)

    def _apply_two_stage_aggregation(
        self,
        data_with_strat: pl.LazyFrame,
        metric_mappings: dict[str, str],
        group_cols: list[str],
        use_grm_adjustment: bool = False,
    ) -> pl.DataFrame:
        """
        Apply FIA's two-stage aggregation methodology for statistically valid estimates.

        This shared method implements the critical two-stage aggregation pattern that
        is required for all FIA per-acre estimates. It eliminates ~400-600 lines of
        duplicated code across 6 estimators while ensuring consistent, correct results.

        Delegates to the pure function in aggregation module.

        Parameters
        ----------
        data_with_strat : pl.LazyFrame
            Data with stratification columns joined (must include EXPNS, CONDPROP_UNADJ)
        metric_mappings : dict[str, str]
            Mapping of adjusted metrics to condition-level aggregates, e.g.:
            {"VOLUME_ADJ": "CONDITION_VOLUME"} for volume estimation
            {"TPA_ADJ": "CONDITION_TPA", "BAA_ADJ": "CONDITION_BAA"} for TPA estimation
        group_cols : list[str]
            User-specified grouping columns (e.g., SPCD, FORTYPCD)
        use_grm_adjustment : bool, default False
            If True, use SUBPTYP_GRM for adjustment factors (mortality/growth/removals)
            If False, use standard DIA-based adjustments (volume/biomass/tpa)

        Returns
        -------
        pl.DataFrame
            Aggregated results with per-acre and total estimates

        Notes
        -----
        Stage 1: Aggregate metrics to plot-condition level
        - Each condition's area proportion (CONDPROP_UNADJ) is counted exactly once
        - Trees within a condition are summed together

        Stage 2: Apply expansion factors and calculate ratio-of-means
        - Condition-level values are expanded using stratification factors (EXPNS)
        - Per-acre estimates = sum(metric x EXPNS) / sum(CONDPROP_UNADJ x EXPNS)
        """
        return _apply_two_stage_aggregation_impl(
            data_with_strat, metric_mappings, group_cols, use_grm_adjustment
        )

    def _get_stratification_data(self) -> pl.LazyFrame:
        """
        Get stratification data with simple caching.

        Delegates to DataLoader for actual data loading operations.

        Returns
        -------
        pl.LazyFrame
            Joined PPSA, POP_STRATUM, and PLOT data including MACRO_BREAKPOINT_DIA
        """
        return self.data_loader.get_stratification_data()

    def _aggregate_area_only(self, strat_data: pl.LazyFrame) -> pl.DataFrame:
        """Handle area-only aggregation without tree data."""
        # This would be implemented by area estimator
        return pl.DataFrame()

    def _preserve_plot_tree_data(
        self,
        data_with_strat: pl.LazyFrame,
        metric_cols: list[str],
        group_cols: list[str] | None = None,
    ) -> tuple[pl.DataFrame, pl.LazyFrame]:
        """
        Preserve plot-tree level data for variance calculation.

        This shared method handles the common pattern of collecting data and
        preserving necessary columns for later variance calculation.

        Parameters
        ----------
        data_with_strat : pl.LazyFrame
            Data with stratification columns joined
        metric_cols : list[str]
            Metric columns to preserve (e.g., ["VOLUME_ADJ"], ["BIOMASS_ADJ", "CARBON_ADJ"])
        group_cols : list[str], optional
            Grouping columns to preserve

        Returns
        -------
        tuple[pl.DataFrame, pl.LazyFrame]
            (plot_tree_data for variance, data_with_strat as LazyFrame for aggregation)
        """
        # Collect the data to ensure metrics are computed
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
            # If we have UNITCD, rename it to ESTN_UNIT
            data_collected = data_collected.with_columns(
                pl.col("UNITCD").alias("ESTN_UNIT")
            )
            cols_to_preserve.append("ESTN_UNIT")

        # Add essential columns for variance calculation
        cols_to_preserve.extend(metric_cols)
        essential_cols = ["ADJ_FACTOR", "CONDPROP_UNADJ", "EXPNS"]
        for col in essential_cols:
            if col in available_cols and col not in cols_to_preserve:
                cols_to_preserve.append(col)

        # Add grouping columns if they exist
        if group_cols:
            for col in group_cols:
                if col in available_cols and col not in cols_to_preserve:
                    cols_to_preserve.append(col)

        # Store the plot-tree data for variance calculation
        plot_tree_data = data_collected.select(
            [c for c in cols_to_preserve if c in data_collected.columns]
        )

        # Convert back to lazy for two-stage aggregation
        data_lazy = data_collected.lazy()

        return plot_tree_data, data_lazy

    def _extract_evaluation_year(self) -> int:
        """
        Extract evaluation year from EVALID or INVYR.

        The year extraction follows FIA conventions:
        1. Primary: Extract from EVALID (SSYYTT format where YY is year)
        2. Fallback: Use max INVYR from PLOT table
        3. Default: Current year minus 2 (typical FIA processing lag)

        Returns
        -------
        int
            The evaluation year
        """
        year = None

        # Primary source: EVALID encodes the evaluation reference year
        # EVALIDs are 6-digit codes: SSYYTT where YY is the evaluation year
        if hasattr(self.db, "evalids") and self.db.evalids:
            evalid = self.db.evalids[0]  # Use first EVALID
            evalid_str = str(evalid)

            # Validate EVALID format: must be exactly 6 digits (SSYYTT)
            if len(evalid_str) != 6:
                logger.debug(
                    f"Invalid EVALID format: '{evalid_str}' has {len(evalid_str)} "
                    f"characters, expected 6 (SSYYTT format)"
                )
            elif not evalid_str.isdigit():
                logger.debug(
                    f"Invalid EVALID format: '{evalid_str}' contains non-digit "
                    f"characters, expected 6 digits (SSYYTT format)"
                )
            else:
                try:
                    year_part = int(evalid_str[2:4])  # Extract YY portion

                    # Handle century using Y2K windowing
                    # Years >= 90 are 1990s, years < 90 are 2000s
                    if year_part >= EVALIDYearParsing.LEGACY_THRESHOLD:
                        year = EVALIDYearParsing.CENTURY_1900 + year_part
                    else:
                        year = EVALIDYearParsing.CENTURY_2000 + year_part

                    # Validate year is within reasonable range
                    if (
                        year < EVALIDYearParsing.MIN_VALID_YEAR
                        or year > EVALIDYearParsing.MAX_VALID_YEAR
                    ):
                        logger.debug(
                            f"EVALID year {year} outside valid range "
                            f"({EVALIDYearParsing.MIN_VALID_YEAR}-"
                            f"{EVALIDYearParsing.MAX_VALID_YEAR}), using fallback"
                        )
                        year = None  # Fall back to other methods
                except ValueError as e:
                    logger.debug(
                        f"Could not parse year from EVALID '{evalid_str}': {e}"
                    )

        # Fallback: If no EVALID, use most recent INVYR as approximation
        if year is None and "PLOT" in self.db.tables:
            try:
                plot_data = self.db.tables["PLOT"]
                if isinstance(plot_data, pl.LazyFrame):
                    plot_years = plot_data.select("INVYR").collect()
                else:
                    plot_years = plot_data.select("INVYR")
                if not plot_years.is_empty():
                    # Use max year as it best represents the evaluation period
                    max_year = plot_years["INVYR"].max()
                    if max_year is not None:
                        year = int(max_year)  # type: ignore[arg-type]
            except Exception as e:
                logger.debug(f"Could not infer year from PLOT.INVYR: {e}")

        # Default to current year minus processing lag (typically 2 years)
        if year is None:
            from datetime import datetime

            year = datetime.now().year - EVALIDYearParsing.DEFAULT_YEAR_OFFSET

        return year

    def _aggregate_to_plot_level_for_variance(
        self,
        plot_tree_data: pl.DataFrame,
        metric_col: str,
        group_cols: list[str],
        y_col_alias: str,
    ) -> pl.DataFrame:
        """
        Aggregate tree data to plot level for variance calculation.

        Parameters
        ----------
        plot_tree_data : pl.DataFrame
            Plot-tree level data
        metric_col : str
            Column containing the metric to aggregate
        group_cols : list[str]
            Grouping columns
        y_col_alias : str
            Alias for the y column

        Returns
        -------
        pl.DataFrame
            Plot-level aggregated data with y and x columns
        """
        # Step 1: Aggregate to plot-condition level
        base_group_cols = ["PLT_CN", "CONDID", "STRATUM_CN", "EXPNS", "CONDPROP_UNADJ"]
        plot_cond_group_cols = [
            c for c in base_group_cols if c in plot_tree_data.columns
        ]
        plot_cond_group_cols.extend(
            [c for c in group_cols if c in plot_tree_data.columns]
        )

        plot_cond_data = plot_tree_data.group_by(plot_cond_group_cols).agg(
            [pl.sum(metric_col).alias("y_ic")]
        )

        # Step 2: Aggregate to plot level
        plot_level_cols = ["PLT_CN", "STRATUM_CN", "EXPNS"]
        plot_level_cols = [c for c in plot_level_cols if c in plot_cond_data.columns]
        plot_level_cols.extend([c for c in group_cols if c in plot_cond_data.columns])

        plot_data = plot_cond_data.group_by(plot_level_cols).agg(
            [
                pl.sum("y_ic").alias(y_col_alias),
                pl.sum("CONDPROP_UNADJ").cast(pl.Float64).alias("x_i"),
            ]
        )

        return plot_data

    def _expand_plots_for_all_groups(
        self,
        plot_data: pl.DataFrame,
        results: pl.DataFrame,
        group_cols: list[str],
        y_col_alias: str,
    ) -> tuple[pl.DataFrame, list[str]]:
        """
        Expand plot data to include all plots with zeros for missing groups.

        Parameters
        ----------
        plot_data : pl.DataFrame
            Plot-level aggregated data
        results : pl.DataFrame
            Results containing unique group combinations
        group_cols : list[str]
            Grouping columns
        y_col_alias : str
            Alias for the y column

        Returns
        -------
        tuple[pl.DataFrame, list[str]]
            Expanded plot data and valid group columns
        """
        # Get all plots from stratification (include B&P columns when available)
        strat_data = self._get_stratification_data()
        strat_schema = strat_data.collect_schema().names()
        bp_cols = ["ESTN_UNIT_CN", "STRATUM_WGT", "AREA_USED", "P2POINTCNT"]
        select_cols = ["PLT_CN", "STRATUM_CN", "EXPNS"] + [
            c for c in bp_cols if c in strat_schema
        ]
        all_plots = strat_data.select(select_cols).unique().collect()

        valid_group_cols = [c for c in group_cols if c in plot_data.columns]

        if valid_group_cols:
            # Get unique group values from results
            unique_groups = results.select(valid_group_cols).unique()

            # Cross join all plots with all groups to ensure complete coverage
            all_plots_expanded = all_plots.join(unique_groups, how="cross")

            # Left join with actual plot data to get values (missing = 0)
            join_cols = ["PLT_CN"] + valid_group_cols
            all_plots_with_data = all_plots_expanded.join(
                plot_data.select(join_cols + [y_col_alias, "x_i"]),
                on=join_cols,
                how="left",
            ).with_columns(
                [
                    pl.col(y_col_alias).fill_null(0.0),
                    pl.col("x_i").fill_null(0.0),
                ]
            )
        else:
            # No grouping - just use all plots with plot_data
            all_plots_with_data = all_plots.join(
                plot_data.select(["PLT_CN", y_col_alias, "x_i"]),
                on="PLT_CN",
                how="left",
            ).with_columns(
                [
                    pl.col(y_col_alias).fill_null(0.0),
                    pl.col("x_i").fill_null(0.0),
                ]
            )

        return all_plots_with_data, valid_group_cols

    def _rename_variance_columns(
        self,
        variance_df: pl.DataFrame,
        metric_mappings: dict[str, tuple[str, str]],
    ) -> pl.DataFrame:
        """
        Rename generic variance columns to metric-specific names.

        Parameters
        ----------
        variance_df : pl.DataFrame
            Variance results with generic column names
        metric_mappings : dict[str, tuple[str, str]]
            Mapping of metric to (SE column, variance column) names

        Returns
        -------
        pl.DataFrame
            Variance results with metric-specific column names
        """
        for adj_col, (se_col, var_col) in metric_mappings.items():
            total_se_col = se_col.replace("_ACRE_", "_TOTAL_")
            total_var_col = var_col.replace("_ACRE_", "_TOTAL_")

            variance_df = variance_df.with_columns(
                [
                    pl.col("se_acre").alias(se_col),
                    pl.col("variance_acre").alias(var_col),
                    pl.col("se_total").alias(total_se_col),
                    pl.col("variance_total").alias(total_var_col),
                ]
            )

        # Drop the generic columns
        cols_to_drop = ["se_acre", "se_total", "variance_acre", "variance_total"]
        cols_to_drop = [c for c in cols_to_drop if c in variance_df.columns]
        if cols_to_drop:
            variance_df = variance_df.drop(cols_to_drop)

        return variance_df

    def _join_variance_to_results(
        self,
        results: pl.DataFrame,
        variance_df: pl.DataFrame,
        valid_group_cols: list[str],
    ) -> pl.DataFrame:
        """
        Join variance results back to main results.

        Parameters
        ----------
        results : pl.DataFrame
            Main results dataframe
        variance_df : pl.DataFrame
            Variance results to join
        valid_group_cols : list[str]
            Columns to join on

        Returns
        -------
        pl.DataFrame
            Results with variance columns added
        """
        if valid_group_cols:
            return results.join(variance_df, on=valid_group_cols, how="left")
        else:
            # No grouping - just add the single variance row's columns
            for col in variance_df.columns:
                if col not in results.columns:
                    results = results.with_columns(
                        pl.lit(variance_df[col][0]).alias(col)
                    )
            return results

    def _calculate_grouped_variance(
        self,
        plot_tree_data: pl.DataFrame,
        results: pl.DataFrame,
        group_cols: list[str],
        metric_mappings: dict[str, tuple[str, str]],
        y_col_alias: str = "y_i",
    ) -> pl.DataFrame:
        """
        Calculate variance for grouped estimates using vectorized operations.

        This method computes variance for all groups in a single pass using
        Polars group_by operations, avoiding the N+1 query pattern of iterating
        through groups individually.

        Implements the domain total variance formula from Bechtold & Patterson (2005):
        V(Y_hat) = sum_h W_h^2 * s^2_yh * n_h

        Parameters
        ----------
        plot_tree_data : pl.DataFrame
            Plot-tree level data preserved during aggregation
        results : pl.DataFrame
            Aggregated results with grouping columns
        group_cols : list[str]
            Columns used for grouping
        metric_mappings : dict[str, tuple[str, str]]
            Mapping of adjusted metric column to (SE column name, variance column name)
            e.g., {"VOLUME_ADJ": ("VOLUME_ACRE_SE", "VOLUME_ACRE_VARIANCE")}
        y_col_alias : str, default "y_i"
            Alias for the y column in plot-level aggregation

        Returns
        -------
        pl.DataFrame
            Results with variance columns added
        """
        from .variance import calculate_grouped_domain_total_variance

        # Get the first metric column for aggregation
        metric_col = list(metric_mappings.keys())[0]

        # Step 1-2: Aggregate to plot level
        plot_data = self._aggregate_to_plot_level_for_variance(
            plot_tree_data, metric_col, group_cols, y_col_alias
        )

        # Step 3-4: Expand to include all plots with zeros for missing groups
        all_plots_with_data, valid_group_cols = self._expand_plots_for_all_groups(
            plot_data, results, group_cols, y_col_alias
        )

        # Step 5: Calculate variance for all groups in one vectorized operation
        variance_df = calculate_grouped_domain_total_variance(
            all_plots_with_data,
            group_cols=valid_group_cols,
            y_col=y_col_alias,
            x_col="x_i",
            stratum_col="STRATUM_CN",
            weight_col="EXPNS",
        )

        # Step 6: Rename variance columns to match expected output
        variance_df = self._rename_variance_columns(variance_df, metric_mappings)

        # Step 7: Join variance results back to main results
        return self._join_variance_to_results(results, variance_df, valid_group_cols)

    def _calculate_variance_for_metrics(
        self,
        agg_result: AggregationResult,
        metric_configs: list[dict[str, str]],
        include_cv: bool = False,
    ) -> pl.DataFrame:
        """
        Calculate variance for one or more metrics using domain total variance formula.

        This is the unified variance calculation method that replaces duplicated code
        across estimators. It handles both grouped and ungrouped cases, single and
        multiple metrics, using vectorized operations where possible.

        Implements the stratified domain total variance formula from Bechtold & Patterson (2005):
        V(Ŷ) = Σ_h w_h² × s²_yh × n_h

        Parameters
        ----------
        agg_result : AggregationResult
            Bundle containing results, plot_tree_data, and group_cols from
            aggregate_results().
        metric_configs : list of dict
            List of metric configurations, each with keys:
            - "adjusted_col": str - Column name in plot_tree_data (e.g., "VOLUME_ADJ")
            - "acre_se_col": str - Output SE column name for per-acre (e.g., "VOLUME_ACRE_SE")
            - "total_se_col": str - Output SE column name for total (e.g., "VOLUME_TOTAL_SE")
            - "acre_var_col": str, optional - Output variance column for per-acre
            - "total_var_col": str, optional - Output variance column for total
            - "acre_col": str, optional - The acre estimate column for CV calculation
            - "total_col": str, optional - The total estimate column for CV calculation
        include_cv : bool, default False
            If True, add coefficient of variation columns (requires acre_col/total_col
            in metric_configs).

        Returns
        -------
        pl.DataFrame
            Results with variance columns added for all metrics.

        Raises
        ------
        ValueError
            If plot_tree_data is not available for variance calculation.

        Examples
        --------
        Single metric (volume):
        >>> metric_configs = [{
        ...     "adjusted_col": "VOLUME_ADJ",
        ...     "acre_se_col": "VOLUME_ACRE_SE",
        ...     "total_se_col": "VOLUME_TOTAL_SE",
        ...     "acre_var_col": "VOLUME_ACRE_VARIANCE",
        ...     "total_var_col": "VOLUME_TOTAL_VARIANCE",
        ... }]
        >>> results = self._calculate_variance_for_metrics(agg_result, metric_configs)

        Multiple metrics (TPA + BAA):
        >>> metric_configs = [
        ...     {"adjusted_col": "TPA_ADJ", "acre_se_col": "TPA_SE", "total_se_col": "TPA_TOTAL_SE"},
        ...     {"adjusted_col": "BAA_ADJ", "acre_se_col": "BAA_SE", "total_se_col": "BAA_TOTAL_SE"},
        ... ]
        >>> results = self._calculate_variance_for_metrics(agg_result, metric_configs)
        """
        results = agg_result.results
        plot_tree_data = agg_result.plot_tree_data
        group_cols = agg_result.group_cols

        if plot_tree_data is None:
            raise ValueError(
                f"Plot-tree data is required for {self.__class__.__name__} variance "
                "calculation. Cannot compute statistically valid standard errors "
                "without tree-level data. Ensure data preservation is working "
                "correctly in the estimation pipeline."
            )

        # Step 1: Aggregate to plot-condition level for all metrics
        plot_group_cols = ["PLT_CN", "CONDID", "EXPNS"]
        if "STRATUM_CN" in plot_tree_data.columns:
            plot_group_cols.insert(2, "STRATUM_CN")
        if "CONDPROP_UNADJ" in plot_tree_data.columns:
            plot_group_cols.append("CONDPROP_UNADJ")

        # Add grouping columns
        if group_cols:
            for col in group_cols:
                if col in plot_tree_data.columns and col not in plot_group_cols:
                    plot_group_cols.append(col)

        # Build aggregation expressions for all metrics
        agg_exprs = []
        for i, cfg in enumerate(metric_configs):
            agg_exprs.append(pl.sum(cfg["adjusted_col"]).alias(f"y_{i}_ic"))

        plot_cond_data = plot_tree_data.group_by(plot_group_cols).agg(agg_exprs)

        # Step 2: Aggregate to plot level
        plot_level_cols = ["PLT_CN", "EXPNS"]
        if "STRATUM_CN" in plot_cond_data.columns:
            plot_level_cols.insert(1, "STRATUM_CN")
        if group_cols:
            plot_level_cols.extend(
                [c for c in group_cols if c in plot_cond_data.columns]
            )

        # Build plot-level aggregation
        plot_agg_exprs = (
            [pl.sum("CONDPROP_UNADJ").cast(pl.Float64).alias("x_i")]
            if "CONDPROP_UNADJ" in plot_cond_data.columns
            else [pl.lit(1.0).alias("x_i")]
        )

        for i in range(len(metric_configs)):
            plot_agg_exprs.append(pl.sum(f"y_{i}_ic").alias(f"y_{i}_i"))

        plot_data = plot_cond_data.group_by(plot_level_cols).agg(plot_agg_exprs)

        # Step 3: Get ALL plots in the evaluation for proper variance calculation
        strat_data = self._get_stratification_data()
        strat_schema = strat_data.collect_schema().names()
        # Select B&P variance columns when available
        bp_cols = ["ESTN_UNIT_CN", "STRATUM_WGT", "AREA_USED", "P2POINTCNT"]
        select_cols = ["PLT_CN", "STRATUM_CN", "EXPNS"] + [
            c for c in bp_cols if c in strat_schema
        ]
        all_plots = strat_data.select(select_cols).unique().collect()

        # Step 4: Calculate variance for each group or overall
        if group_cols:
            results = self._calculate_grouped_multi_metric_variance(
                plot_data=plot_data,
                all_plots=all_plots,
                results=results,
                group_cols=group_cols,
                metric_configs=metric_configs,
            )
        else:
            results = self._calculate_overall_multi_metric_variance(
                plot_data=plot_data,
                all_plots=all_plots,
                results=results,
                metric_configs=metric_configs,
            )

        # Step 5: Add CV if requested
        if include_cv:
            results = self._add_cv_columns(results, metric_configs)

        return results

    def _calculate_grouped_multi_metric_variance(
        self,
        plot_data: pl.DataFrame,
        all_plots: pl.DataFrame,
        results: pl.DataFrame,
        group_cols: list[str],
        metric_configs: list[dict[str, str]],
    ) -> pl.DataFrame:
        """
        Calculate variance for grouped estimates with multiple metrics.

        Uses a loop over groups for now; could be further optimized with
        vectorized operations in the future.

        Uses ratio-of-means variance for per-acre SE:
        V(R) = (1/X^2) * [V(Y) + R^2*V(X) - 2*R*Cov(Y,X)]
        """
        from .variance import calculate_ratio_of_means_variance

        variance_results = []

        for group_vals in results.iter_rows():
            # Build filter for this group
            group_filter = pl.lit(True)
            group_dict = {}

            for col in group_cols:
                if col in plot_data.columns:
                    val = group_vals[results.columns.index(col)]
                    group_dict[col] = val
                    if val is None:
                        group_filter = group_filter & pl.col(col).is_null()
                    else:
                        group_filter = group_filter & (pl.col(col) == val)

            # Filter plot data for this specific group
            group_plot_data = plot_data.filter(group_filter)

            # Build select columns for join
            select_cols = ["PLT_CN", "x_i"] + [
                f"y_{i}_i" for i in range(len(metric_configs))
            ]
            select_cols = [c for c in select_cols if c in group_plot_data.columns]

            # Join with ALL plots, filling missing with zeros
            all_plots_group = all_plots.join(
                group_plot_data.select(select_cols),
                on="PLT_CN",
                how="left",
            )

            # Fill nulls with zeros
            fill_exprs = [pl.col("x_i").fill_null(0.0)]
            for i in range(len(metric_configs)):
                col_name = f"y_{i}_i"
                if col_name in all_plots_group.columns:
                    fill_exprs.append(pl.col(col_name).fill_null(0.0))

            all_plots_group = all_plots_group.with_columns(fill_exprs)

            # Calculate variance for each metric
            result_row = dict(group_dict)

            if len(all_plots_group) > 0:
                for i, cfg in enumerate(metric_configs):
                    y_col = f"y_{i}_i"
                    if y_col in all_plots_group.columns:
                        ratio_stats = calculate_ratio_of_means_variance(
                            all_plots_group, y_col, "x_i"
                        )
                        se_acre = ratio_stats["se_ratio"]

                        result_row[cfg["acre_se_col"]] = se_acre
                        result_row[cfg["total_se_col"]] = ratio_stats["se_total"]

                        # Add variance columns if specified
                        if "acre_var_col" in cfg:
                            result_row[cfg["acre_var_col"]] = ratio_stats[
                                "variance_ratio"
                            ]
                        if "total_var_col" in cfg:
                            result_row[cfg["total_var_col"]] = ratio_stats[
                                "variance_total"
                            ]
            else:
                # No data for this group
                for cfg in metric_configs:
                    result_row[cfg["acre_se_col"]] = 0.0
                    result_row[cfg["total_se_col"]] = 0.0
                    if "acre_var_col" in cfg:
                        result_row[cfg["acre_var_col"]] = 0.0
                    if "total_var_col" in cfg:
                        result_row[cfg["total_var_col"]] = 0.0

            variance_results.append(result_row)

        # Join variance results back to main results
        if variance_results:
            var_df = pl.DataFrame(variance_results)
            # Use only valid group columns that exist in both dataframes
            join_cols = [
                c for c in group_cols if c in var_df.columns and c in results.columns
            ]
            if join_cols:
                results = results.join(var_df, on=join_cols, how="left")

        return results

    def _calculate_overall_multi_metric_variance(
        self,
        plot_data: pl.DataFrame,
        all_plots: pl.DataFrame,
        results: pl.DataFrame,
        metric_configs: list[dict[str, str]],
    ) -> pl.DataFrame:
        """
        Calculate overall variance (ungrouped) for multiple metrics.

        Uses ratio-of-means variance for per-acre SE:
        V(R) = (1/X^2) * [V(Y) + R^2*V(X) - 2*R*Cov(Y,X)]
        """
        from .variance import calculate_ratio_of_means_variance

        # Build select columns for join
        select_cols = ["PLT_CN", "x_i"] + [
            f"y_{i}_i" for i in range(len(metric_configs))
        ]
        select_cols = [c for c in select_cols if c in plot_data.columns]

        # Join with ALL plots, filling missing with zeros
        all_plots_with_values = all_plots.join(
            plot_data.select(select_cols),
            on="PLT_CN",
            how="left",
        )

        # Fill nulls with zeros
        fill_exprs = [pl.col("x_i").fill_null(0.0)]
        for i in range(len(metric_configs)):
            col_name = f"y_{i}_i"
            if col_name in all_plots_with_values.columns:
                fill_exprs.append(pl.col(col_name).fill_null(0.0))

        all_plots_with_values = all_plots_with_values.with_columns(fill_exprs)

        # Calculate variance for each metric and add to results
        for i, cfg in enumerate(metric_configs):
            y_col = f"y_{i}_i"
            if y_col in all_plots_with_values.columns:
                ratio_stats = calculate_ratio_of_means_variance(
                    all_plots_with_values, y_col, "x_i"
                )
                se_acre = ratio_stats["se_ratio"]

                results = results.with_columns(
                    [
                        pl.lit(se_acre).alias(cfg["acre_se_col"]),
                        pl.lit(ratio_stats["se_total"]).alias(cfg["total_se_col"]),
                    ]
                )

                # Add variance columns if specified
                if "acre_var_col" in cfg:
                    results = results.with_columns(
                        [
                            pl.lit(ratio_stats["variance_ratio"]).alias(
                                cfg["acre_var_col"]
                            ),
                        ]
                    )
                if "total_var_col" in cfg:
                    results = results.with_columns(
                        [
                            pl.lit(ratio_stats["variance_total"]).alias(
                                cfg["total_var_col"]
                            ),
                        ]
                    )

        return results

    def _add_cv_columns(
        self,
        results: pl.DataFrame,
        metric_configs: list[dict[str, str]],
    ) -> pl.DataFrame:
        """Add coefficient of variation columns for metrics that specify acre_col/total_col."""
        for cfg in metric_configs:
            acre_col = cfg.get("acre_col")
            total_col = cfg.get("total_col")
            acre_se_col = cfg["acre_se_col"]
            total_se_col = cfg["total_se_col"]

            if acre_col and acre_col in results.columns:
                cv_col = acre_se_col.replace("_SE", "_CV")
                results = results.with_columns(
                    [
                        pl.when(pl.col(acre_col) > 0)
                        .then(pl.col(acre_se_col) / pl.col(acre_col) * 100)
                        .otherwise(None)
                        .alias(cv_col),
                    ]
                )

            if total_col and total_col in results.columns:
                cv_col = total_se_col.replace("_SE", "_CV")
                results = results.with_columns(
                    [
                        pl.when(pl.col(total_col) > 0)
                        .then(pl.col(total_se_col) / pl.col(total_col) * 100)
                        .otherwise(None)
                        .alias(cv_col),
                    ]
                )

        return results

    # === Abstract Methods ===

    @abstractmethod
    def get_required_tables(self) -> list[str]:
        """Return list of required database tables."""
        pass

    @abstractmethod
    def calculate_values(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """Calculate estimation values."""
        pass

    def get_tree_columns(self) -> list[str] | None:
        """Return list of required tree columns."""
        return None

    def get_cond_columns(self) -> list[str] | None:
        """Return list of required condition columns."""
        return None

    def __del__(self) -> None:
        """Clean up database connection if owned."""
        if hasattr(self, "_owns_db") and self._owns_db:
            if hasattr(self.db, "close"):
                self.db.close()


# Backward compatibility: re-export GRMBaseEstimator from its new location
from .grm_base import GRMBaseEstimator

__all__ = ["AggregationResult", "BaseEstimator", "GRMBaseEstimator"]
