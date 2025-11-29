"""
Base estimator for FIA statistical estimation.

This module provides the base class for all FIA estimators using a simple,
straightforward approach without unnecessary abstractions.
"""

from abc import ABC, abstractmethod
from functools import lru_cache
from typing import Dict, List, Optional, Tuple, Union

import polars as pl

from ..core import FIA
from ..filtering import apply_area_filters, apply_tree_filters, setup_grouping_columns


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
        """Load and join tree and condition data."""
        # Load TREE table
        if "TREE" not in self.db.tables:
            self.db.load_table("TREE")
        tree_df = self.db.tables["TREE"]
        
        # Load COND table
        if "COND" not in self.db.tables:
            self.db.load_table("COND")
        cond_df = self.db.tables["COND"]
        
        # Ensure LazyFrames
        if not isinstance(tree_df, pl.LazyFrame):
            tree_df = tree_df.lazy()
        if not isinstance(cond_df, pl.LazyFrame):
            cond_df = cond_df.lazy()
        
        # Apply EVALID filtering if set
        # EVALID filtering happens through POP_PLOT_STRATUM_ASSGN, not directly on TREE/COND
        if self.db.evalid:
            # Load POP_PLOT_STRATUM_ASSGN to get plots for the EVALID
            if "POP_PLOT_STRATUM_ASSGN" not in self.db.tables:
                self.db.load_table("POP_PLOT_STRATUM_ASSGN")
            
            ppsa = self.db.tables["POP_PLOT_STRATUM_ASSGN"]
            if not isinstance(ppsa, pl.LazyFrame):
                ppsa = ppsa.lazy()
            
            # Filter to get PLT_CNs for the specified EVALID(s)
            valid_plots = ppsa.filter(
                pl.col("EVALID").is_in(self.db.evalid)
            ).select("PLT_CN").unique()
            
            # Filter tree and cond to only include these plots
            tree_df = tree_df.join(
                valid_plots,
                on="PLT_CN",
                how="inner"  # This filters to only plots in the EVALID
            )
            cond_df = cond_df.join(
                valid_plots,
                on="PLT_CN",
                how="inner"
            )
        
        # Select only needed columns
        tree_cols = self.get_tree_columns()
        cond_cols = self.get_cond_columns()

        # Add grouping columns from config if specified
        grp_by = self.config.get("grp_by")
        if grp_by:
            if isinstance(grp_by, str):
                grp_by = [grp_by]

            # Get available columns from each table to check where grp_by cols exist
            tree_schema = tree_df.collect_schema().names()
            cond_schema = cond_df.collect_schema().names()

            for col in grp_by:
                # Add to appropriate table's column list if not already present
                if col in tree_schema and col not in tree_cols:
                    tree_cols.append(col)
                elif col in cond_schema and col not in cond_cols:
                    cond_cols.append(col)

        if tree_cols:
            tree_df = tree_df.select(tree_cols)
        if cond_cols:
            cond_df = cond_df.select(cond_cols)

        # Join tree and condition
        data = tree_df.join(
            cond_df,
            on=["PLT_CN", "CONDID"],
            how="inner"
        )

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
            valid_plots = ppsa.filter(
                pl.col("EVALID").is_in(self.db.evalid)
            ).select("PLT_CN").unique()
            
            # Filter cond and plot to only include these plots
            cond_df = cond_df.join(
                valid_plots,
                on="PLT_CN",
                how="inner"
            )
            # For plot table, join on CN not PLT_CN
            valid_plot_cns = valid_plots.rename({"PLT_CN": "CN"})
            plot_df = plot_df.join(
                valid_plot_cns,
                on="CN",
                how="inner"
            )
        
        # Join condition and plot
        data = cond_df.join(
            plot_df.select(["CN", "STATECD", "COUNTYCD", "PLOT"]),
            left_on="PLT_CN",
            right_on="CN",
            how="inner"
        )
        
        return data
    
    def apply_filters(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """
        Apply domain filtering.
        
        Parameters
        ----------
        data : pl.LazyFrame
            Input data
            
        Returns
        -------
        pl.LazyFrame
            Filtered data
        """
        # Collect to DataFrame for filtering functions
        data_df = data.collect()
        
        # Apply tree domain filter
        if self.config.get("tree_domain"):
            data_df = apply_tree_filters(
                data_df,
                tree_domain=self.config["tree_domain"]
            )
        
        # Apply area domain filter
        if self.config.get("area_domain"):
            data_df = apply_area_filters(
                data_df,
                area_domain=self.config["area_domain"]
            )
        
        # Apply tree type filter (live, dead, etc.)
        tree_type = self.config.get("tree_type", "live")
        if tree_type and "STATUSCD" in data_df.columns:
            if tree_type == "live":
                data_df = data_df.filter(pl.col("STATUSCD") == 1)
            elif tree_type == "dead":
                data_df = data_df.filter(pl.col("STATUSCD") == 2)
            elif tree_type == "gs":
                data_df = data_df.filter(pl.col("STATUSCD").is_in([1, 2]))
            # "all" means no filter
        
        # Apply land type filter
        land_type = self.config.get("land_type", "forest")
        if land_type and "COND_STATUS_CD" in data_df.columns:
            if land_type == "forest":
                data_df = data_df.filter(pl.col("COND_STATUS_CD") == 1)
            elif land_type == "timber":
                data_df = data_df.filter(
                    (pl.col("COND_STATUS_CD") == 1) &
                    (pl.col("SITECLCD").is_in([1, 2, 3, 4, 5, 6])) &
                    (pl.col("RESERVCD") == 0)
                )
        
        # Convert back to lazy for further processing
        return data_df.lazy()
    
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
        data_with_strat = data.join(
            strat_data,
            on="PLT_CN",
            how="inner"
        )
        
        # Setup grouping columns
        group_cols = self._setup_grouping()
        
        # Aggregate by groups
        if group_cols:
            results = data_with_strat.group_by(group_cols).agg([
                pl.sum("ESTIMATE_VALUE").alias("ESTIMATE"),
                pl.count("PLT_CN").alias("N_PLOTS")
            ]).collect()
        else:
            results = data_with_strat.select([
                pl.sum("ESTIMATE_VALUE").alias("ESTIMATE"),
                pl.count("PLT_CN").alias("N_PLOTS")
            ]).collect()
        
        return results
    
    def calculate_variance(self, results: pl.DataFrame) -> pl.DataFrame:
        """
        Calculate variance for estimates.
        
        Simple variance calculation without complex abstractions.
        """
        # This would implement the actual FIA variance formulas
        # For now, add placeholder SE column
        results = results.with_columns([
            (pl.col("ESTIMATE") * 0.1).alias("SE")  # Placeholder
        ])
        
        if not self.config.get("variance", False):
            # Convert variance to standard error
            results = results.with_columns([
                pl.col("SE").sqrt().alias("SE")
            ])
        
        return results
    
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
        results = results.with_columns([
            pl.lit(self.config.get("year", 2023)).alias("YEAR")
        ])
        
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
        use_grm_adjustment: bool = False
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
        condition_group_cols = ["PLT_CN", "CONDID", "STRATUM_CN", "EXPNS", "CONDPROP_UNADJ"]

        # Add user-specified grouping columns if they exist at condition level
        if group_cols:
            for col in group_cols:
                if col in available_cols and col not in condition_group_cols:
                    condition_group_cols.append(col)

        # Build aggregation expressions for Stage 1
        agg_exprs = []
        for adj_col, cond_col in metric_mappings.items():
            agg_exprs.append(
                pl.col(adj_col).sum().alias(cond_col)
            )
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
        final_agg_exprs.extend([
            pl.n_unique("PLT_CN").alias("N_PLOTS"),
            pl.col("TREES_PER_CONDITION").sum().alias("N_TREES"),
            pl.len().alias("N_CONDITIONS")
        ])

        # Apply final aggregation based on grouping
        if group_cols:
            # Filter to valid grouping columns at condition level (using cached schema)
            # Note: After aggregation, only columns in condition_group_cols are available
            final_group_cols = [
                col for col in group_cols
                if col in condition_group_cols
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
        results = results.collect()

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

        results = results.with_columns(per_acre_exprs)

        # Clean up intermediate columns (keep totals and per-acre values)
        cols_to_drop = ["N_CONDITIONS", "AREA_TOTAL"]
        for adj_col, cond_col in metric_mappings.items():
            metric_name = cond_col.replace("CONDITION_", "")
            cols_to_drop.append(f"{metric_name}_NUM")

        # Only drop columns that exist
        cols_to_drop = [col for col in cols_to_drop if col in results.columns]
        if cols_to_drop:
            results = results.drop(cols_to_drop)

        return results

    @lru_cache(maxsize=1)
    def _get_stratification_data(self) -> pl.LazyFrame:
        """
        Get stratification data with simple caching.

        Returns
        -------
        pl.LazyFrame
            Joined PPSA and POP_STRATUM data
        """
        # Load PPSA
        if "POP_PLOT_STRATUM_ASSGN" not in self.db.tables:
            self.db.load_table("POP_PLOT_STRATUM_ASSGN")
        ppsa = self.db.tables["POP_PLOT_STRATUM_ASSGN"]
        
        # Load POP_STRATUM
        if "POP_STRATUM" not in self.db.tables:
            self.db.load_table("POP_STRATUM")
        pop_stratum = self.db.tables["POP_STRATUM"]
        
        # Ensure LazyFrames
        if not isinstance(ppsa, pl.LazyFrame):
            ppsa = ppsa.lazy()
        if not isinstance(pop_stratum, pl.LazyFrame):
            pop_stratum = pop_stratum.lazy()
        
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
        ppsa_selected = ppsa_unique.select([
            "PLT_CN",
            "STRATUM_CN"
        ])

        # Select necessary columns from POP_STRATUM
        pop_stratum_selected = pop_stratum_unique.select([
            pl.col("CN").alias("STRATUM_CN"),
            "EXPNS",
            "ADJ_FACTOR_MICR",
            "ADJ_FACTOR_SUBP",
            "ADJ_FACTOR_MACR"
        ])

        strat_data = ppsa_selected.join(
            pop_stratum_selected,
            on="STRATUM_CN",
            how="inner"
        )

        return strat_data
    
    def _aggregate_area_only(self, strat_data: pl.LazyFrame) -> pl.DataFrame:
        """Handle area-only aggregation without tree data."""
        # This would be implemented by area estimator
        return pl.DataFrame()
    
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
        if hasattr(self, '_owns_db') and self._owns_db:
            if hasattr(self.db, 'close'):
                self.db.close()