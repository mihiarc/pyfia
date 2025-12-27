"""
Base estimator for FIA statistical estimation.

This module provides the base class for all FIA estimators using a simple,
straightforward approach without unnecessary abstractions.
"""

import logging
from abc import ABC, abstractmethod
from functools import lru_cache
from typing import Dict, List, Literal, Optional, Union

import polars as pl

from ..constants.defaults import EVALIDYearParsing
from ..core import FIA
from ..filtering import apply_area_filters, apply_plot_filters, apply_tree_filters

logger = logging.getLogger(__name__)


class BaseEstimator(ABC):
    """
    Base class for FIA design-based estimators.

    Implements a simple Template Method pattern for the estimation workflow
    without unnecessary abstractions like FrameWrapper, complex caching, or
    deep inheritance hierarchies.
    """

    def __init__(self, db: Union[str, FIA], config: dict):
        """
        Initialize the estimator.

        Parameters
        ----------
        db : Union[str, FIA]
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

        # Simple caches for commonly used data
        self._ref_species_cache: Optional[pl.DataFrame] = None
        self._stratification_cache: Optional[pl.LazyFrame] = None

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
        results = self.aggregate_results(data)

        # 5. Calculate variance
        results = self.calculate_variance(results)

        # 6. Format output
        return self.format_output(results)

    def load_data(self) -> Optional[pl.LazyFrame]:
        """
        Load and join required tables.

        Returns
        -------
        Optional[pl.LazyFrame]
            Joined data or None if no tree data needed
        """
        tables = self.get_required_tables()

        # Handle area-only estimations (no tree data)
        if "TREE" not in tables:
            return self._load_area_data()

        # Load tree and condition data
        return self._load_tree_cond_data()

    def _load_tree_cond_data(self) -> pl.LazyFrame:
        """Load and join tree and condition data.

        Memory optimization:
        1. Column projection: Load only required columns at SQL level
        2. Database-side filtering: Push tree_type and land_type filters to SQL

        This reduces memory footprint by 60-80% for large TREE tables.
        """
        # Get required columns FIRST (before loading)
        tree_cols = self.get_tree_columns()
        cond_cols = self.get_cond_columns()

        # Get table schemas to determine where grp_by columns live
        # This is a metadata-only query, not loading actual data
        tree_schema = list(self.db._reader.get_table_schema("TREE").keys())
        cond_schema = list(self.db._reader.get_table_schema("COND").keys())

        # Add grouping columns from config if specified
        grp_by = self.config.get("grp_by")
        if grp_by:
            if isinstance(grp_by, str):
                grp_by = [grp_by]

            for col in grp_by:
                # Add to appropriate table's column list if not already present
                in_tree = col in tree_schema and col not in tree_cols
                in_cond = col in cond_schema and col not in cond_cols
                if tree_cols is not None and in_tree:
                    tree_cols.append(col)
                elif cond_cols is not None and in_cond:
                    cond_cols.append(col)

        # Build SQL WHERE clauses for database-side filtering
        # This significantly reduces data loaded into memory
        tree_where = self._build_tree_sql_filter()
        cond_where = self._build_cond_sql_filter()

        # Load TREE table with column projection and SQL filtering
        # This is the key optimization - filter at SQL level before loading
        # Check if we need to reload (new columns required that aren't in cached table)
        if "TREE" in self.db.tables:
            cached_cols = set(self.db.tables["TREE"].columns if hasattr(self.db.tables["TREE"], 'columns') else self.db.tables["TREE"].collect_schema().names())
            required_cols = set(tree_cols) if tree_cols else set()
            if not required_cols.issubset(cached_cols):
                # Reload with all required columns
                del self.db.tables["TREE"]
        if "TREE" not in self.db.tables:
            self.db.load_table("TREE", columns=tree_cols, where=tree_where)
        tree_df = self.db.tables["TREE"]

        # Load COND table with column projection and SQL filtering
        # Check if we need to reload (new columns required that aren't in cached table)
        if "COND" in self.db.tables:
            cached_cols = set(self.db.tables["COND"].columns if hasattr(self.db.tables["COND"], 'columns') else self.db.tables["COND"].collect_schema().names())
            required_cols = set(cond_cols) if cond_cols else set()
            if not required_cols.issubset(cached_cols):
                # Reload with all required columns
                del self.db.tables["COND"]
        if "COND" not in self.db.tables:
            self.db.load_table("COND", columns=cond_cols, where=cond_where)
        cond_df = self.db.tables["COND"]

        # Ensure LazyFrames
        if not isinstance(tree_df, pl.LazyFrame):
            tree_df = tree_df.lazy()
        if not isinstance(cond_df, pl.LazyFrame):
            cond_df = cond_df.lazy()

        # Apply EVALID filtering if set (via POP_PLOT_STRATUM_ASSGN)
        # Also handle plot_domain filtering by getting valid PLT_CNs from PLOT table
        valid_plots = None

        if self.db.evalid:
            # Load POP_PLOT_STRATUM_ASSGN to get plots for the EVALID
            if "POP_PLOT_STRATUM_ASSGN" not in self.db.tables:
                self.db.load_table("POP_PLOT_STRATUM_ASSGN")

            ppsa = self.db.tables["POP_PLOT_STRATUM_ASSGN"]
            if not isinstance(ppsa, pl.LazyFrame):
                ppsa = ppsa.lazy()

            # Filter to get PLT_CNs for the specified EVALID(s)
            valid_plots = (
                ppsa.filter(pl.col("EVALID").is_in(self.db.evalid))
                .select("PLT_CN")
                .unique()
            )

        # Apply plot domain filter if specified
        if self.config.get("plot_domain"):
            # Load PLOT table to filter by plot-level attributes
            if "PLOT" not in self.db.tables:
                self.db.load_table("PLOT")
            plot_df = self.db.tables["PLOT"]
            if not isinstance(plot_df, pl.LazyFrame):
                plot_df = plot_df.lazy()

            # Apply plot domain filter
            plot_df = apply_plot_filters(plot_df, plot_domain=self.config["plot_domain"])

            # Get PLT_CNs from filtered plots
            plot_filtered_plots = plot_df.select(pl.col("CN").alias("PLT_CN")).unique()

            # Combine with EVALID filter if both exist
            if valid_plots is not None:
                valid_plots = valid_plots.join(plot_filtered_plots, on="PLT_CN", how="inner")
            else:
                valid_plots = plot_filtered_plots

        # Apply the combined filter to tree and cond if any filters were set
        if valid_plots is not None:
            # Filter tree and cond to only include these plots
            tree_df = tree_df.join(
                valid_plots,
                on="PLT_CN",
                how="inner",  # This filters to only plots in the EVALID and/or plot_domain
            )
            cond_df = cond_df.join(valid_plots, on="PLT_CN", how="inner")

        # Select only needed columns (if table was already loaded without projection)
        if tree_cols:
            tree_schema_names = tree_df.collect_schema().names()
            available_tree_cols = [c for c in tree_cols if c in tree_schema_names]
            tree_df = tree_df.select(available_tree_cols)
        if cond_cols:
            cond_schema_names = cond_df.collect_schema().names()
            available_cond_cols = [c for c in cond_cols if c in cond_schema_names]
            cond_df = cond_df.select(available_cond_cols)

        # Join tree and condition
        data = tree_df.join(cond_df, on=["PLT_CN", "CONDID"], how="inner")

        return data

    def _load_area_data(self) -> pl.LazyFrame:
        """Load condition and plot data for area estimation."""
        # Load COND table
        if "COND" not in self.db.tables:
            self.db.load_table("COND")
        cond_df = self.db.tables["COND"]

        # Load PLOT table
        if "PLOT" not in self.db.tables:
            self.db.load_table("PLOT")
        plot_df = self.db.tables["PLOT"]

        # Ensure LazyFrames
        if not isinstance(cond_df, pl.LazyFrame):
            cond_df = cond_df.lazy()
        if not isinstance(plot_df, pl.LazyFrame):
            plot_df = plot_df.lazy()

        # Apply EVALID filtering through POP_PLOT_STRATUM_ASSGN
        if self.db.evalid:
            # Load POP_PLOT_STRATUM_ASSGN to get plots for the EVALID
            if "POP_PLOT_STRATUM_ASSGN" not in self.db.tables:
                self.db.load_table("POP_PLOT_STRATUM_ASSGN")

            ppsa = self.db.tables["POP_PLOT_STRATUM_ASSGN"]
            if not isinstance(ppsa, pl.LazyFrame):
                ppsa = ppsa.lazy()

            # Filter to get PLT_CNs for the specified EVALID(s)
            valid_plots = (
                ppsa.filter(pl.col("EVALID").is_in(self.db.evalid))
                .select("PLT_CN")
                .unique()
            )

            # Filter cond and plot to only include these plots
            cond_df = cond_df.join(valid_plots, on="PLT_CN", how="inner")
            # For plot table, join on CN not PLT_CN
            valid_plot_cns = valid_plots.rename({"PLT_CN": "CN"})
            plot_df = plot_df.join(valid_plot_cns, on="CN", how="inner")

        # Apply plot domain filter BEFORE joining with COND
        # This allows filtering by PLOT-level attributes like COUNTYCD
        if self.config.get("plot_domain"):
            plot_df = apply_plot_filters(plot_df, plot_domain=self.config["plot_domain"])

        # Join condition and plot (all PLOT columns for grouping flexibility)
        data = cond_df.join(plot_df, left_on="PLT_CN", right_on="CN", how="inner")

        return data

    def _build_tree_sql_filter(self) -> Optional[str]:
        """Build SQL WHERE clause for TREE table based on config.

        This pushes common filters to the database level to reduce memory usage.

        Returns
        -------
        Optional[str]
            SQL WHERE clause (without WHERE keyword) or None if no filters
        """
        filters = []

        # Tree type filter (most common optimization)
        tree_type = self.config.get("tree_type", "live")
        if tree_type == "live":
            filters.append("STATUSCD = 1")
        elif tree_type == "dead":
            filters.append("STATUSCD = 2")
        elif tree_type == "gs":
            # Growing stock: live trees with valid tree class
            # Note: TREECLCD filter applied in Polars since it's conditional
            filters.append("STATUSCD = 1")
        # "all" means no STATUSCD filter

        # Basic validity filters (these are always applied in apply_tree_filters)
        filters.append("DIA IS NOT NULL")
        filters.append("TPA_UNADJ > 0")

        if filters:
            return " AND ".join(filters)
        return None

    def _build_cond_sql_filter(self) -> Optional[str]:
        """Build SQL WHERE clause for COND table based on config.

        This pushes land type filters to the database level to reduce memory usage.

        Returns
        -------
        Optional[str]
            SQL WHERE clause (without WHERE keyword) or None if no filters
        """
        filters = []

        # Land type filter
        land_type = self.config.get("land_type", "forest")
        if land_type == "forest":
            filters.append("COND_STATUS_CD = 1")
        elif land_type == "timber":
            # Timberland: forest, productive, not reserved
            filters.append("COND_STATUS_CD = 1")
            filters.append("SITECLCD IN (1, 2, 3, 4, 5, 6)")
            filters.append("RESERVCD = 0")
        # "all" means no COND_STATUS_CD filter

        if filters:
            return " AND ".join(filters)
        return None

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

        # Apply land type filter
        land_type = self.config.get("land_type", "forest")
        if land_type and "COND_STATUS_CD" in columns:
            if land_type == "forest":
                data = data.filter(pl.col("COND_STATUS_CD") == 1)
            elif land_type == "timber":
                data = data.filter(
                    (pl.col("COND_STATUS_CD") == 1)
                    & (pl.col("SITECLCD").is_in([1, 2, 3, 4, 5, 6]))
                    & (pl.col("RESERVCD") == 0)
                )

        return data

    def aggregate_results(self, data: Optional[pl.LazyFrame]) -> pl.DataFrame:
        """
        Aggregate results with stratification.

        Parameters
        ----------
        data : Optional[pl.LazyFrame]
            Calculated values or None for area-only

        Returns
        -------
        pl.DataFrame
            Aggregated results
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

    def calculate_variance(self, results: pl.DataFrame) -> pl.DataFrame:
        """
        Calculate variance for estimates.

        Subclasses must override this method to implement proper variance
        calculation following Bechtold & Patterson (2005) methodology.

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

    def _setup_grouping(self) -> List[str]:
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

    def _apply_two_stage_aggregation(
        self,
        data_with_strat: pl.LazyFrame,
        metric_mappings: Dict[str, str],
        group_cols: List[str],
        use_grm_adjustment: bool = False,
    ) -> pl.DataFrame:
        """
        Apply FIA's two-stage aggregation methodology for statistically valid estimates.

        This shared method implements the critical two-stage aggregation pattern that
        is required for all FIA per-acre estimates. It eliminates ~400-600 lines of
        duplicated code across 6 estimators while ensuring consistent, correct results.

        Parameters
        ----------
        data_with_strat : pl.LazyFrame
            Data with stratification columns joined (must include EXPNS, CONDPROP_UNADJ)
        metric_mappings : Dict[str, str]
            Mapping of adjusted metrics to condition-level aggregates, e.g.:
            {"VOLUME_ADJ": "CONDITION_VOLUME"} for volume estimation
            {"TPA_ADJ": "CONDITION_TPA", "BAA_ADJ": "CONDITION_BAA"} for TPA estimation
        group_cols : List[str]
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
        - Per-acre estimates = sum(metric × EXPNS) / sum(CONDPROP_UNADJ × EXPNS)
        """
        # ========================================================================
        # STAGE 1: Aggregate to plot-condition level
        # ========================================================================

        # Cache schema once at the beginning to avoid repeated collection
        available_cols = data_with_strat.collect_schema().names()

        # Define condition-level grouping columns (always needed)
        condition_group_cols = [
            "PLT_CN",
            "CONDID",
            "STRATUM_CN",
            "EXPNS",
            "CONDPROP_UNADJ",
        ]

        # Add user-specified grouping columns if they exist at condition level
        if group_cols:
            for col in group_cols:
                if col in available_cols and col not in condition_group_cols:
                    condition_group_cols.append(col)

        # Build aggregation expressions for Stage 1
        agg_exprs = []
        for adj_col, cond_col in metric_mappings.items():
            agg_exprs.append(pl.col(adj_col).sum().alias(cond_col))
        # Add tree count for diagnostics
        agg_exprs.append(pl.len().alias("TREES_PER_CONDITION"))

        # Aggregate at condition level
        condition_agg = data_with_strat.group_by(condition_group_cols).agg(agg_exprs)

        # ========================================================================
        # STAGE 2: Apply expansion factors and calculate population estimates
        # ========================================================================

        # Build final aggregation expressions
        final_agg_exprs = []

        # For each metric, create numerator, total, and per-acre calculations
        for adj_col, cond_col in metric_mappings.items():
            # Extract base metric name (e.g., "VOLUME" from "CONDITION_VOLUME")
            metric_name = cond_col.replace("CONDITION_", "")

            # Numerator: sum(metric × EXPNS)
            final_agg_exprs.append(
                (pl.col(cond_col) * pl.col("EXPNS")).sum().alias(f"{metric_name}_NUM")
            )

            # Total: sum(metric × EXPNS) - same as numerator but kept for clarity
            final_agg_exprs.append(
                (pl.col(cond_col) * pl.col("EXPNS")).sum().alias(f"{metric_name}_TOTAL")
            )

        # Denominator: sum(CONDPROP_UNADJ × EXPNS) - shared across all metrics
        final_agg_exprs.append(
            (pl.col("CONDPROP_UNADJ") * pl.col("EXPNS")).sum().alias("AREA_TOTAL")
        )

        # Diagnostic counts
        final_agg_exprs.extend(
            [
                pl.n_unique("PLT_CN").alias("N_PLOTS"),
                pl.col("TREES_PER_CONDITION").sum().alias("N_TREES"),
                pl.len().alias("N_CONDITIONS"),
            ]
        )

        # Apply final aggregation based on grouping
        if group_cols:
            # Filter to valid grouping columns at condition level (using cached schema)
            # Note: After aggregation, only columns in condition_group_cols are available
            final_group_cols = [
                col for col in group_cols if col in condition_group_cols
            ]

            if final_group_cols:
                results = condition_agg.group_by(final_group_cols).agg(final_agg_exprs)
            else:
                # No valid grouping columns, aggregate all
                results = condition_agg.select(final_agg_exprs)
        else:
            # No grouping specified, aggregate all
            results = condition_agg.select(final_agg_exprs)

        # Collect results
        results_df: pl.DataFrame = results.collect()

        # Calculate per-acre values using ratio-of-means
        per_acre_exprs = []
        for adj_col, cond_col in metric_mappings.items():
            metric_name = cond_col.replace("CONDITION_", "")

            # Per-acre = numerator / denominator with division-by-zero protection
            per_acre_exprs.append(
                pl.when(pl.col("AREA_TOTAL") > 0)
                .then(pl.col(f"{metric_name}_NUM") / pl.col("AREA_TOTAL"))
                .otherwise(0.0)
                .alias(f"{metric_name}_ACRE")
            )

        results_df = results_df.with_columns(per_acre_exprs)

        # Clean up intermediate columns (keep totals and per-acre values)
        cols_to_drop = ["N_CONDITIONS", "AREA_TOTAL"]
        for adj_col, cond_col in metric_mappings.items():
            metric_name = cond_col.replace("CONDITION_", "")
            cols_to_drop.append(f"{metric_name}_NUM")

        # Only drop columns that exist
        cols_to_drop = [col for col in cols_to_drop if col in results_df.columns]
        if cols_to_drop:
            results_df = results_df.drop(cols_to_drop)

        return results_df

    @lru_cache(maxsize=1)
    def _get_stratification_data(self) -> pl.LazyFrame:
        """
        Get stratification data with simple caching.

        Returns
        -------
        pl.LazyFrame
            Joined PPSA, POP_STRATUM, and PLOT data including MACRO_BREAKPOINT_DIA
        """
        # Load PPSA
        if "POP_PLOT_STRATUM_ASSGN" not in self.db.tables:
            self.db.load_table("POP_PLOT_STRATUM_ASSGN")
        ppsa = self.db.tables["POP_PLOT_STRATUM_ASSGN"]

        # Load POP_STRATUM
        if "POP_STRATUM" not in self.db.tables:
            self.db.load_table("POP_STRATUM")
        pop_stratum = self.db.tables["POP_STRATUM"]

        # Load PLOT table for MACRO_BREAKPOINT_DIA
        if "PLOT" not in self.db.tables:
            self.db.load_table("PLOT")
        plot = self.db.tables["PLOT"]

        # Ensure LazyFrames
        if not isinstance(ppsa, pl.LazyFrame):
            ppsa = ppsa.lazy()
        if not isinstance(pop_stratum, pl.LazyFrame):
            pop_stratum = pop_stratum.lazy()
        if not isinstance(plot, pl.LazyFrame):
            plot = plot.lazy()

        # Apply EVALID filter
        if self.db.evalid:
            ppsa = ppsa.filter(pl.col("EVALID").is_in(self.db.evalid))
            pop_stratum = pop_stratum.filter(pl.col("EVALID").is_in(self.db.evalid))

        # CRITICAL: Remove duplicates from both tables
        # Texas has duplicate rows in both POP_PLOT_STRATUM_ASSGN and POP_STRATUM
        # Each plot-stratum pair and each stratum appears exactly twice
        ppsa_unique = ppsa.unique(subset=["PLT_CN", "STRATUM_CN"])
        pop_stratum_unique = pop_stratum.unique(subset=["CN"])

        # Select only necessary columns from PPSA to avoid duplicate columns
        # when joining with other tables that also have STATECD, INVYR, etc.
        ppsa_selected = ppsa_unique.select(["PLT_CN", "STRATUM_CN"])

        # Select necessary columns from POP_STRATUM
        pop_stratum_selected = pop_stratum_unique.select(
            [
                pl.col("CN").alias("STRATUM_CN"),
                "EXPNS",
                "ADJ_FACTOR_MICR",
                "ADJ_FACTOR_SUBP",
                "ADJ_FACTOR_MACR",
            ]
        )

        # Select MACRO_BREAKPOINT_DIA from PLOT table
        # This is CRITICAL for correct adjustment factor selection in states with macroplots
        plot_cols = [pl.col("CN").alias("PLT_CN"), "MACRO_BREAKPOINT_DIA"]

        # Include polygon attributes if they exist (from intersect_polygons)
        # This allows grp_by to use polygon attribute columns
        if (
            hasattr(self.db, "_polygon_attributes")
            and self.db._polygon_attributes is not None
            and isinstance(self.db._polygon_attributes, pl.DataFrame)
        ):
            # Get column names from polygon attributes (excluding CN which is the join key)
            polygon_attr_cols = [
                col
                for col in self.db._polygon_attributes.columns
                if col != "CN"
            ]
            # Add these columns to the selection if they exist in the plot schema
            plot_schema = plot.collect_schema().names()
            for col in polygon_attr_cols:
                if col in plot_schema:
                    plot_cols.append(col)

        plot_selected = plot.select(plot_cols)

        # Join PPSA with POP_STRATUM
        strat_data = ppsa_selected.join(
            pop_stratum_selected, on="STRATUM_CN", how="inner"
        )

        # Join with PLOT to get MACRO_BREAKPOINT_DIA
        strat_data = strat_data.join(plot_selected, on="PLT_CN", how="left")

        return strat_data

    def _aggregate_area_only(self, strat_data: pl.LazyFrame) -> pl.DataFrame:
        """Handle area-only aggregation without tree data."""
        # This would be implemented by area estimator
        return pl.DataFrame()

    def _preserve_plot_tree_data(
        self,
        data_with_strat: pl.LazyFrame,
        metric_cols: List[str],
        group_cols: Optional[List[str]] = None,
    ) -> tuple[pl.DataFrame, pl.LazyFrame]:
        """
        Preserve plot-tree level data for variance calculation.

        This shared method handles the common pattern of collecting data and
        preserving necessary columns for later variance calculation.

        Parameters
        ----------
        data_with_strat : pl.LazyFrame
            Data with stratification columns joined
        metric_cols : List[str]
            Metric columns to preserve (e.g., ["VOLUME_ADJ"], ["BIOMASS_ADJ", "CARBON_ADJ"])
        group_cols : List[str], optional
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
            try:
                evalid = self.db.evalids[0]  # Use first EVALID
                year_part = int(str(evalid)[2:4])  # Extract YY portion

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
                    year = None  # Fall back to other methods
            except (IndexError, ValueError, TypeError) as e:
                logger.debug(f"Could not parse year from EVALID: {e}")

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

    def _calculate_grouped_variance(
        self,
        plot_tree_data: pl.DataFrame,
        results: pl.DataFrame,
        group_cols: List[str],
        metric_mappings: Dict[str, tuple[str, str]],
        y_col_alias: str = "y_i",
        use_domain_total_variance: bool = False,
    ) -> pl.DataFrame:
        """
        Calculate variance for grouped estimates using vectorized operations.

        This method computes variance for all groups in a single pass using
        Polars group_by operations, avoiding the N+1 query pattern of iterating
        through groups individually.

        Parameters
        ----------
        plot_tree_data : pl.DataFrame
            Plot-tree level data preserved during aggregation
        results : pl.DataFrame
            Aggregated results with grouping columns
        group_cols : List[str]
            Columns used for grouping
        metric_mappings : Dict[str, tuple[str, str]]
            Mapping of adjusted metric column to (SE column name, variance column name)
            e.g., {"VOLUME_ADJ": ("VOLUME_ACRE_SE", "VOLUME_ACRE_VARIANCE")}
        y_col_alias : str, default "y_i"
            Alias for the y column in plot-level aggregation
        use_domain_total_variance : bool, default False
            If True, use domain total variance formula V(Ŷ) = Σ_h w_h² × s²_yh × n_h.
            If False, use ratio-of-means variance formula which includes covariance terms.
            EVALIDator uses domain total variance for tree-based estimates.

        Returns
        -------
        pl.DataFrame
            Results with variance columns added
        """
        from .variance import (
            calculate_grouped_domain_total_variance,
            calculate_grouped_ratio_variance,
        )

        # Get the first metric column for aggregation
        metric_col = list(metric_mappings.keys())[0]

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

        # Step 3: Get ALL plots for proper variance calculation
        strat_data = self._get_stratification_data()
        all_plots = (
            strat_data.select("PLT_CN", "STRATUM_CN", "EXPNS").unique().collect()
        )

        # Step 4: Expand plot_data to include all plots with zeros for missing groups
        # Get unique group combinations from results
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
            ).with_columns([
                pl.col(y_col_alias).fill_null(0.0),
                pl.col("x_i").fill_null(0.0),
            ])
        else:
            # No grouping - just use all plots with plot_data
            all_plots_with_data = all_plots.join(
                plot_data.select(["PLT_CN", y_col_alias, "x_i"]),
                on="PLT_CN",
                how="left",
            ).with_columns([
                pl.col(y_col_alias).fill_null(0.0),
                pl.col("x_i").fill_null(0.0),
            ])

        # Step 5: Calculate variance for all groups in one vectorized operation
        if use_domain_total_variance:
            variance_df = calculate_grouped_domain_total_variance(
                all_plots_with_data,
                group_cols=valid_group_cols,
                y_col=y_col_alias,
                x_col="x_i",
                stratum_col="STRATUM_CN",
                weight_col="EXPNS",
            )
        else:
            variance_df = calculate_grouped_ratio_variance(
                all_plots_with_data,
                group_cols=valid_group_cols,
                y_col=y_col_alias,
                x_col="x_i",
                stratum_col="STRATUM_CN",
                weight_col="EXPNS",
            )

        # Step 6: Rename variance columns to match expected output
        # Map generic variance columns to metric-specific names
        for adj_col, (se_col, var_col) in metric_mappings.items():
            total_se_col = se_col.replace("_ACRE_", "_TOTAL_")
            total_var_col = var_col.replace("_ACRE_", "_TOTAL_")

            variance_df = variance_df.with_columns([
                pl.col("se_acre").alias(se_col),
                pl.col("variance_acre").alias(var_col),
                pl.col("se_total").alias(total_se_col),
                pl.col("variance_total").alias(total_var_col),
            ])

        # Drop the generic columns
        cols_to_drop = ["se_acre", "se_total", "variance_acre", "variance_total"]
        cols_to_drop = [c for c in cols_to_drop if c in variance_df.columns]
        if cols_to_drop:
            variance_df = variance_df.drop(cols_to_drop)

        # Step 7: Join variance results back to main results
        if valid_group_cols:
            results = results.join(variance_df, on=valid_group_cols, how="left")
        else:
            # No grouping - just add the single variance row's columns
            for col in variance_df.columns:
                if col not in results.columns:
                    results = results.with_columns(
                        pl.lit(variance_df[col][0]).alias(col)
                    )

        return results

    # === Abstract Methods ===

    @abstractmethod
    def get_required_tables(self) -> List[str]:
        """Return list of required database tables."""
        pass

    @abstractmethod
    def calculate_values(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """Calculate estimation values."""
        pass

    def get_tree_columns(self) -> Optional[List[str]]:
        """Return list of required tree columns."""
        return None

    def get_cond_columns(self) -> Optional[List[str]]:
        """Return list of required condition columns."""
        return None

    def __del__(self):
        """Clean up database connection if owned."""
        if hasattr(self, "_owns_db") and self._owns_db:
            if hasattr(self.db, "close"):
                self.db.close()


class GRMBaseEstimator(BaseEstimator):
    """
    Base class for Growth-Removal-Mortality (GRM) estimators.

    Provides shared functionality for growth, mortality, and removals estimation
    using FIA's GRM tables (TREE_GRM_COMPONENT, TREE_GRM_MIDPT, TREE_GRM_BEGIN).

    Subclasses must implement:
    - component_type: Property returning 'growth', 'mortality', or 'removals'
    - calculate_values: Component-specific value calculation
    - get_component_filter: Optional filter for specific components
    """

    def __init__(self, db, config):
        """Initialize with GRM-specific attributes."""
        super().__init__(db, config)
        self.plot_tree_data: Optional[pl.DataFrame] = None
        self.group_cols: Optional[List[str]] = None
        self._grm_columns = None

    @property
    def component_type(self) -> Literal["growth", "mortality", "removals"]:
        """Return the GRM component type: 'growth', 'mortality', or 'removals'."""
        raise NotImplementedError("Subclasses must implement component_type property")

    @property
    def metric_prefix(self) -> str:
        """Return the metric column prefix (e.g., 'GROWTH', 'MORT', 'REMV')."""
        prefixes = {"growth": "GROWTH", "mortality": "MORT", "removals": "REMV"}
        return prefixes.get(self.component_type, "VALUE")

    @property
    def value_column(self) -> str:
        """Return the value column name for this component."""
        return f"{self.metric_prefix}_VALUE"

    @property
    def adjusted_column(self) -> str:
        """Return the adjusted value column name."""
        return f"{self.metric_prefix}_ADJ"

    def get_required_tables(self) -> List[str]:
        """GRM estimators require GRM tables."""
        from .grm import get_grm_required_tables

        return get_grm_required_tables(self.component_type)

    def get_cond_columns(self) -> List[str]:
        """Standard condition columns for GRM estimation."""
        base_cols = [
            "PLT_CN",
            "CONDID",
            "COND_STATUS_CD",
            "CONDPROP_UNADJ",
            "OWNGRPCD",
            "FORTYPCD",
            "SITECLCD",
            "RESERVCD",
        ]

        # Add grouping columns if they come from COND table
        if self.config.get("grp_by"):
            grp_cols = self.config["grp_by"]
            if isinstance(grp_cols, str):
                grp_cols = [grp_cols]
            for col in grp_cols:
                if col not in base_cols:
                    base_cols.append(col)

        return base_cols

    def _resolve_grm_columns(self):
        """Resolve GRM column names based on config."""
        from .grm import resolve_grm_columns

        if self._grm_columns is None:
            tree_type = self.config.get("tree_type", "gs")
            land_type = self.config.get("land_type", "forest")
            self._grm_columns = resolve_grm_columns(
                component_type=self.component_type,
                tree_type=tree_type,
                land_type=land_type,
            )
        return self._grm_columns

    def _load_simple_grm_data(self) -> Optional[pl.LazyFrame]:
        """
        Load GRM data using the simple pattern (for mortality/removals).

        This pattern:
        1. Loads GRM component table
        2. Joins with GRM midpt table
        3. Applies EVALID filtering
        4. Joins with aggregated COND data
        """
        from .grm import (
            aggregate_cond_to_plot,
            filter_by_evalid,
            load_grm_component,
            load_grm_midpt,
        )

        measure = self.config.get("measure", "volume")

        # Resolve GRM column names
        grm_cols = self._resolve_grm_columns()

        # Load GRM component table
        grm_component = load_grm_component(
            self.db,
            grm_cols,
            include_dia_end=(self.component_type != "removals"),
        )

        # Load GRM midpt table
        grm_midpt = load_grm_midpt(self.db, measure=measure)

        # Join component with midpt
        data = grm_component.join(grm_midpt, on="TRE_CN", how="inner")

        # Apply EVALID filtering
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

    def _apply_grm_filters(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """
        Apply common GRM filters.

        This method applies all GRM-specific filters directly on the LazyFrame
        without materializing the data, enabling efficient memory usage.

        Applies:
        1. Area domain filter
        2. Tree domain filter
        3. Positive TPA_UNADJ filter
        4. Component-specific filters (via get_component_filter)
        5. Tree type filter (growing stock >= 5 inches)
        """
        # Get column names once for conditional filtering
        columns = data.collect_schema().names()

        # Apply area domain filter (works with LazyFrames)
        if self.config.get("area_domain"):
            from ..filtering.area.filters import apply_area_filters

            data = apply_area_filters(data, area_domain=self.config["area_domain"])

        # Apply tree domain filter (works with LazyFrames)
        if self.config.get("tree_domain"):
            from ..filtering.core.parser import DomainExpressionParser

            data = DomainExpressionParser.apply_to_dataframe(
                data, self.config["tree_domain"], "tree"
            )

        # Filter to records with positive TPA
        data = data.filter(
            pl.col("TPA_UNADJ").is_not_null() & (pl.col("TPA_UNADJ") > 0)
        )

        # Apply component-specific filter
        component_filter = self.get_component_filter()
        if component_filter is not None:
            data = data.filter(component_filter)

        # Apply tree type filter (growing stock >= 5 inches)
        tree_type = self.config.get("tree_type", "gs")
        if tree_type == "gs" and "DIA_MIDPT" in columns:
            data = data.filter(pl.col("DIA_MIDPT") >= 5.0)

        return data

    def get_component_filter(self) -> Optional[pl.Expr]:
        """
        Return component-specific filter expression.

        Override in subclasses to filter to specific GRM components.
        Returns None for no additional filtering.
        """
        return None

    def _aggregate_grm_results(
        self,
        data: pl.LazyFrame,
        value_col: str,
        adjusted_col: str,
    ) -> pl.DataFrame:
        """
        Aggregate GRM results with two-stage aggregation.

        Common aggregation pattern for all GRM estimators:
        1. Get stratification data
        2. Apply GRM adjustment factors
        3. Setup grouping
        4. Preserve plot-tree data for variance
        5. Apply two-stage aggregation
        """
        from .grm import apply_grm_adjustment

        # Get stratification data
        strat_data = self._get_stratification_data()

        # Join with stratification
        data_with_strat = data.join(strat_data, on="PLT_CN", how="inner")

        # Apply GRM-specific adjustment factors
        data_with_strat = apply_grm_adjustment(data_with_strat)

        # Apply adjustment to values
        data_with_strat = data_with_strat.with_columns(
            [(pl.col(value_col) * pl.col("ADJ_FACTOR")).alias(adjusted_col)]
        )

        # Setup grouping
        group_cols = self._setup_grouping()
        if self.config.get("by_species", False) and "SPCD" not in group_cols:
            group_cols.append("SPCD")
        self.group_cols = group_cols

        # Store plot-tree level data for variance calculation
        self.plot_tree_data, data_with_strat = self._preserve_plot_tree_data(
            data_with_strat,
            metric_cols=[adjusted_col],
            group_cols=group_cols,
        )

        # Build metric mappings for two-stage aggregation
        condition_col = f"CONDITION_{self.metric_prefix}"
        metric_mappings = {adjusted_col: condition_col}

        results = self._apply_two_stage_aggregation(
            data_with_strat=data_with_strat,
            metric_mappings=metric_mappings,
            group_cols=group_cols,
            use_grm_adjustment=True,
        )

        return results

    def _calculate_grm_variance(
        self,
        results: pl.DataFrame,
        adjusted_col: str,
        acre_se_col: str,
        total_se_col: str,
    ) -> pl.DataFrame:
        """
        Calculate variance for GRM estimates using domain total variance formula.

        Uses the stratified domain total variance formula from Bechtold & Patterson (2005):
        V(Ŷ) = Σ_h w_h² × s²_yh × n_h

        This matches EVALIDator's variance calculation for GRM estimates.

        Parameters
        ----------
        results : pl.DataFrame
            Results dataframe to add variance columns to.
        adjusted_col : str
            Name of the adjusted metric column in plot_tree_data.
        acre_se_col : str
            Name for the per-acre standard error column.
        total_se_col : str
            Name for the total standard error column.

        Returns
        -------
        pl.DataFrame
            Results with variance columns added.

        Raises
        ------
        ValueError
            If plot_tree_data is not available for variance calculation.
        """
        from .variance import calculate_domain_total_variance

        if self.plot_tree_data is None:
            raise ValueError(
                f"Plot-tree data is required for {self.__class__.__name__} variance "
                "calculation. Cannot compute statistically valid standard errors "
                "without tree-level data. Ensure data preservation is working "
                "correctly in the estimation pipeline."
            )

        # Aggregate to plot-condition level
        plot_group_cols = ["PLT_CN", "CONDID", "EXPNS"]
        if "STRATUM_CN" in self.plot_tree_data.columns:
            plot_group_cols.insert(2, "STRATUM_CN")
        if "CONDPROP_UNADJ" in self.plot_tree_data.columns:
            plot_group_cols.append("CONDPROP_UNADJ")

        if self.group_cols:
            for col in self.group_cols:
                if col in self.plot_tree_data.columns and col not in plot_group_cols:
                    plot_group_cols.append(col)

        plot_cond_data = self.plot_tree_data.group_by(plot_group_cols).agg(
            [pl.sum(adjusted_col).alias("y_ic")]
        )

        # Aggregate to plot level
        plot_level_cols = ["PLT_CN", "EXPNS"]
        if "STRATUM_CN" in plot_cond_data.columns:
            plot_level_cols.insert(1, "STRATUM_CN")
        if self.group_cols:
            plot_level_cols.extend(
                [c for c in self.group_cols if c in plot_cond_data.columns]
            )

        # Include CONDPROP_UNADJ for area calculation
        condprop_col = (
            "CONDPROP_UNADJ" if "CONDPROP_UNADJ" in plot_cond_data.columns else None
        )
        agg_cols = [pl.sum("y_ic").alias("y_i")]
        if condprop_col:
            agg_cols.append(pl.sum(condprop_col).cast(pl.Float64).alias("x_i"))
        else:
            agg_cols.append(pl.lit(1.0).alias("x_i"))

        plot_data = plot_cond_data.group_by(plot_level_cols).agg(agg_cols)

        # Get ALL plots in the evaluation for proper variance calculation
        strat_data = self._get_stratification_data()
        all_plots = (
            strat_data.select("PLT_CN", "STRATUM_CN", "EXPNS").unique().collect()
        )

        # Calculate variance
        if self.group_cols:
            variance_results = []

            for group_vals in results.iter_rows():
                group_filter = pl.lit(True)
                group_dict = {}

                for i, col in enumerate(self.group_cols):
                    if col in plot_data.columns:
                        val = group_vals[results.columns.index(col)]
                        group_dict[col] = val
                        if val is None:
                            group_filter = group_filter & pl.col(col).is_null()
                        else:
                            group_filter = group_filter & (pl.col(col) == val)

                group_plot_data = plot_data.filter(group_filter)

                all_plots_group = all_plots.join(
                    group_plot_data.select(["PLT_CN", "y_i", "x_i"]),
                    on="PLT_CN",
                    how="left",
                ).with_columns(
                    [
                        pl.col("y_i").fill_null(0.0),
                        pl.col("x_i").fill_null(0.0),
                    ]
                )

                if len(all_plots_group) > 0:
                    # Calculate variance using domain total formula (matches EVALIDator)
                    var_stats = calculate_domain_total_variance(all_plots_group, "y_i")

                    # Calculate per-acre SE by dividing total SE by total area
                    total_area = (
                        all_plots_group["EXPNS"] * all_plots_group["x_i"]
                    ).sum()
                    se_acre = (
                        var_stats["se_total"] / total_area if total_area > 0 else 0.0
                    )

                    variance_results.append(
                        {
                            **group_dict,
                            acre_se_col: se_acre,
                            total_se_col: var_stats["se_total"],
                        }
                    )
                else:
                    variance_results.append(
                        {
                            **group_dict,
                            acre_se_col: 0.0,
                            total_se_col: 0.0,
                        }
                    )

            if variance_results:
                var_df = pl.DataFrame(variance_results)
                results = results.join(var_df, on=self.group_cols, how="left")
        else:
            # No grouping, calculate overall variance with ALL plots
            all_plots_with_values = all_plots.join(
                plot_data.select(["PLT_CN", "y_i", "x_i"]),
                on="PLT_CN",
                how="left",
            ).with_columns(
                [
                    pl.col("y_i").fill_null(0.0),
                    pl.col("x_i").fill_null(0.0),
                ]
            )

            var_stats = calculate_domain_total_variance(all_plots_with_values, "y_i")

            # Calculate per-acre SE by dividing total SE by total area
            total_area = (
                all_plots_with_values["EXPNS"] * all_plots_with_values["x_i"]
            ).sum()
            se_acre = var_stats["se_total"] / total_area if total_area > 0 else 0.0

            results = results.with_columns(
                [
                    pl.lit(se_acre).alias(acre_se_col),
                    pl.lit(var_stats["se_total"]).alias(total_se_col),
                ]
            )

        return results

    def _format_grm_output(
        self,
        results: pl.DataFrame,
        estimation_type: str,
        include_cv: bool = False,
    ) -> pl.DataFrame:
        """
        Format GRM estimation output with standard columns.
        """
        from .utils import format_output_columns

        measure = self.config.get("measure", "volume")
        land_type = self.config.get("land_type", "forest")
        tree_type = self.config.get("tree_type", "gs")

        # Extract year using shared helper
        year = self._extract_evaluation_year()

        results = results.with_columns(
            [
                pl.lit(year).alias("YEAR"),
                pl.lit(measure.upper()).alias("MEASURE"),
                pl.lit(land_type.upper()).alias("LAND_TYPE"),
                pl.lit(tree_type.upper()).alias("TREE_TYPE"),
            ]
        )

        results = format_output_columns(
            results,
            estimation_type=estimation_type,
            include_se=True,
            include_cv=include_cv,
        )

        return results
