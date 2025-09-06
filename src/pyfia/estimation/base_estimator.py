"""
Unified base estimator for FIA statistical estimation.

This module provides the base class for all FIA estimators, combining:
- Core estimation workflow (Template Method pattern)
- Lazy evaluation for memory efficiency
- Query optimization with composite builders
- Stratification and variance calculation
- Progress tracking capabilities
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple, Union

import polars as pl
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn

from ..core import FIA
from ..filters import apply_area_filters_common, apply_tree_filters_common
from ..filters.common import setup_grouping_columns_common
from .config import EstimatorConfig
from .query_builders import CompositeQueryBuilder, LazyFrameWrapper, MemoryCache
from .join_optimizer import JoinOptimizer
from .statistics.variance_calculator import VarianceCalculator


@dataclass
class EstimationMetrics:
    """Track performance metrics for estimation."""
    
    deferred_operations: int = 0
    collection_points: List[str] = field(default_factory=list)
    cache_hits: int = 0
    cache_misses: int = 0
    total_memory_used: int = 0
    peak_memory_used: int = 0


class BaseEstimator(ABC):
    """
    Unified base class for FIA design-based estimators with lazy evaluation.
    
    This class implements the Template Method pattern to standardize the
    estimation workflow while providing:
    - Lazy evaluation for memory efficiency
    - Query optimization
    - Stratification handling
    - Variance calculation
    - Progress tracking
    
    Subclasses must implement:
    - get_required_tables()
    - get_response_columns()
    - calculate_values()
    - apply_module_filters() [optional]
    """
    
    def __init__(self, db: Union[str, FIA], config: EstimatorConfig):
        """
        Initialize the estimator.
        
        Parameters
        ----------
        db : Union[str, FIA]
            Database connection or path
        config : EstimatorConfig
            Configuration for the estimation
        """
        # Set up database connection
        if isinstance(db, str):
            self.db = FIA(db)
            self._owns_db = True
        else:
            self.db = db
            self._owns_db = False
        
        self.config = config
        self._group_cols: List[str] = []
        
        # Lazy evaluation components
        self._composite_builder = CompositeQueryBuilder(
            self.db,
            self.config,
            cache=MemoryCache(max_size_mb=512, max_entries=200)
        )
        self._join_optimizer = JoinOptimizer()
        self._metrics = EstimationMetrics()
        
        # Variance calculator
        self.variance_calculator = VarianceCalculator()
        
        # Progress tracking
        self.console = Console()
        self._progress: Optional[Progress] = None
        self._task_id: Optional[int] = None
    
    # === Abstract Methods ===
    
    @abstractmethod
    def get_required_tables(self) -> List[str]:
        """Return list of required database tables."""
        pass
    
    @abstractmethod
    def get_response_columns(self) -> Dict[str, str]:
        """
        Return mapping of calculation columns to output names.
        
        Returns
        -------
        Dict[str, str]
            Mapping from internal column names to output column names
        """
        pass
    
    @abstractmethod
    def calculate_values(self, data: pl.DataFrame) -> pl.DataFrame:
        """
        Calculate module-specific values.
        
        Parameters
        ----------
        data : pl.DataFrame
            Prepared data with all necessary columns
            
        Returns
        -------
        pl.DataFrame
            Data with calculated values added
        """
        pass
    
    def apply_module_filters(self, tree_df: Optional[pl.DataFrame],
                            cond_df: pl.DataFrame) -> Tuple[Optional[pl.DataFrame], pl.DataFrame]:
        """
        Apply module-specific filters.
        
        Parameters
        ----------
        tree_df : Optional[pl.DataFrame]
            Tree data (None for area estimation)
        cond_df : pl.DataFrame
            Condition data
            
        Returns
        -------
        Tuple[Optional[pl.DataFrame], pl.DataFrame]
            Filtered tree and condition data
        """
        return tree_df, cond_df
    
    # === Main Estimation Workflow ===
    
    def estimate(self) -> pl.DataFrame:
        """
        Main estimation workflow with lazy evaluation.
        
        Returns
        -------
        pl.DataFrame
            Final estimation results
        """
        try:
            # Start progress tracking if enabled
            if hasattr(self.config, 'show_progress') and self.config.show_progress:
                self._start_progress("Estimating...")
            
            # Step 1: Load required tables (lazy)
            self._update_progress("Loading tables...")
            self._load_required_tables()
            
            # Step 2: Get and filter data (lazy)
            self._update_progress("Filtering data...")
            tree_wrapper, cond_wrapper = self._get_filtered_data_lazy()
            
            # Step 3: Join and prepare data (lazy)
            self._update_progress("Preparing data...")
            prepared_wrapper = self._prepare_estimation_data_lazy(tree_wrapper, cond_wrapper)
            
            # Step 4: Calculate module-specific values
            self._update_progress("Calculating values...")
            valued_wrapper = self._calculate_values_lazy(prepared_wrapper)
            
            # Step 5: Calculate plot-level estimates (lazy)
            self._update_progress("Aggregating to plots...")
            plot_wrapper = self._calculate_plot_estimates_lazy(valued_wrapper)
            
            # Step 6: Apply stratification (collection point)
            self._update_progress("Applying stratification...")
            stratified_df = self._apply_stratification_lazy(plot_wrapper)
            
            # Step 7: Calculate population estimates
            self._update_progress("Calculating population estimates...")
            pop_estimates = self._calculate_population_estimates(stratified_df)
            
            # Step 8: Format output
            self._update_progress("Formatting results...")
            final_results = self._format_output(pop_estimates)
            
            return final_results
            
        finally:
            # Clean up progress tracking
            if self._progress:
                self._stop_progress()
            
            # Clean up database if we own it
            if self._owns_db and hasattr(self.db, 'close'):
                self.db.close()
    
    # === Data Loading and Filtering ===
    
    def _load_required_tables(self):
        """Load all required tables from the database."""
        for table in self.get_required_tables():
            self.db.load_table(table)
    
    def _get_filtered_data_lazy(self) -> Tuple[Optional[LazyFrameWrapper], LazyFrameWrapper]:
        """
        Get data from database using composite query builder.
        
        Returns
        -------
        Tuple[Optional[LazyFrameWrapper], LazyFrameWrapper]
            Filtered tree and condition frame wrappers
        """
        # Build estimation query using composite builder
        estimation_type = self._get_estimation_type()
        
        # Get EVALID from database if available
        evalid = None
        if hasattr(self.db, 'evalid'):
            evalid = self.db.evalid
        
        # Build optimized query
        query_results = self._composite_builder.build_estimation_query(
            estimation_type=estimation_type,
            evalid=evalid,
            tree_domain=self.config.tree_domain,
            area_domain=self.config.area_domain,
            plot_domain=self.config.plot_domain,
            tree_type=getattr(self.config, 'tree_type', None),
            land_type=getattr(self.config, 'land_type', None)
        )
        
        # Extract results
        cond_wrapper = query_results.get("conditions")
        tree_wrapper = query_results.get("trees")
        
        # Ensure we have condition data
        if cond_wrapper is None:
            # Fall back to getting conditions directly
            cond_df = self.db.get_conditions()
            cond_df = apply_area_filters_common(
                cond_df,
                getattr(self.config, 'land_type', None),
                self.config.area_domain
            )
            cond_wrapper = LazyFrameWrapper(pl.LazyFrame(cond_df))
        
        # Apply module-specific filters if we have eager data
        if not cond_wrapper.is_lazy:
            tree_df = tree_wrapper.frame if tree_wrapper else None
            tree_df, cond_df = self.apply_module_filters(tree_df, cond_wrapper.frame)
            
            tree_wrapper = LazyFrameWrapper(pl.LazyFrame(tree_df)) if tree_df is not None else None
            cond_wrapper = LazyFrameWrapper(pl.LazyFrame(cond_df))
        
        return tree_wrapper, cond_wrapper
    
    def _prepare_estimation_data_lazy(self, 
                                     tree_wrapper: Optional[LazyFrameWrapper],
                                     cond_wrapper: LazyFrameWrapper) -> LazyFrameWrapper:
        """
        Join data and prepare for estimation using lazy evaluation.
        
        Parameters
        ----------
        tree_wrapper : Optional[LazyFrameWrapper]
            Tree data wrapper (None for area estimation)
        cond_wrapper : LazyFrameWrapper
            Condition data wrapper
            
        Returns
        -------
        LazyFrameWrapper
            Prepared data ready for value calculation
        """
        if tree_wrapper is not None:
            # Join trees with conditions - preserve all tree columns
            # Select condition columns that don't conflict
            cond_cols_to_join = cond_wrapper.select(["PLT_CN", "CONDID", "CONDPROP_UNADJ"])
            
            # Perform the join directly using LazyFrame methods
            if tree_wrapper.is_lazy:
                tree_frame = tree_wrapper.frame
                cond_frame = cond_cols_to_join.frame if hasattr(cond_cols_to_join, 'frame') else cond_cols_to_join
                joined_frame = tree_frame.join(
                    cond_frame,
                    on=["PLT_CN", "CONDID"],
                    how="inner"
                )
                joined = LazyFrameWrapper(joined_frame)
            else:
                # Eager join
                tree_df = tree_wrapper.frame
                cond_df = cond_cols_to_join.collect() if hasattr(cond_cols_to_join, 'collect') else cond_cols_to_join
                joined_df = tree_df.join(
                    cond_df,
                    on=["PLT_CN", "CONDID"],
                    how="inner"
                )
                joined = LazyFrameWrapper(pl.LazyFrame(joined_df))
            
            # Set up grouping columns
            if self.config.grp_by or self.config.by_species or self.config.by_size_class:
                # Need to collect for grouping setup
                data_df = joined.collect()
                data_df, group_cols = setup_grouping_columns_common(
                    data_df,
                    self.config.grp_by,
                    self.config.by_species,
                    self.config.by_size_class,
                    return_dataframe=True
                )
                self._group_cols = group_cols
                return LazyFrameWrapper(pl.LazyFrame(data_df))
            else:
                self._group_cols = []
                return joined
        else:
            # Area estimation case - no tree data
            self._group_cols = []
            
            # Handle custom grouping columns
            if self.config.grp_by:
                if isinstance(self.config.grp_by, str):
                    self._group_cols = [self.config.grp_by]
                else:
                    self._group_cols = list(self.config.grp_by)
            
            return cond_wrapper
    
    def _calculate_values_lazy(self, data_wrapper: LazyFrameWrapper) -> LazyFrameWrapper:
        """
        Calculate module-specific values with lazy evaluation support.
        
        Parameters
        ----------
        data_wrapper : LazyFrameWrapper
            Prepared data wrapper
            
        Returns
        -------
        LazyFrameWrapper
            Data with calculated values
        """
        # Some calculations may require eager mode
        if data_wrapper.is_lazy:
            data_df = data_wrapper.collect()
            self._metrics.collection_points.append("calculate_values")
        else:
            data_df = data_wrapper.frame
        
        # Apply module-specific calculations
        valued_df = self.calculate_values(data_df)
        
        # Return as lazy frame for continued processing
        if isinstance(valued_df, pl.LazyFrame):
            return LazyFrameWrapper(valued_df)
        else:
            return LazyFrameWrapper(pl.LazyFrame(valued_df))
    
    def _calculate_plot_estimates_lazy(self, data_wrapper: LazyFrameWrapper) -> LazyFrameWrapper:
        """
        Calculate plot-level estimates using lazy aggregation.
        
        Parameters
        ----------
        data_wrapper : LazyFrameWrapper
            Data with calculated values
            
        Returns
        -------
        LazyFrameWrapper
            Plot-level estimates
        """
        # Determine grouping columns
        plot_groups = ["PLT_CN"]
        if self._group_cols:
            plot_groups.extend(self._group_cols)
        
        # Get response columns for aggregation
        response_cols = self.get_response_columns()
        agg_exprs = []
        
        # Check available columns
        if isinstance(data_wrapper.frame, pl.LazyFrame):
            available_cols = data_wrapper.frame.collect_schema().names()
        else:
            available_cols = data_wrapper.frame.columns
        
        for col_name, output_name in response_cols.items():
            if col_name in available_cols:
                agg_exprs.append(pl.sum(col_name).alias(f"PLOT_{output_name}"))
        
        if not agg_exprs:
            raise ValueError(
                f"No response columns found in data. "
                f"Expected columns: {list(response_cols.keys())}"
            )
        
        # Perform aggregation
        plot_estimates = data_wrapper.group_by(plot_groups).agg(agg_exprs)
        
        return plot_estimates
    
    def _apply_stratification_lazy(self, plot_wrapper: LazyFrameWrapper) -> pl.DataFrame:
        """
        Apply stratification and calculate expansion factors.
        
        Parameters
        ----------
        plot_wrapper : LazyFrameWrapper
            Plot-level estimates
            
        Returns
        -------
        pl.DataFrame
            Data with expansion factors applied (collected)
        """
        # Collect plot data for stratification
        plot_data = plot_wrapper.collect()
        self._metrics.collection_points.append("stratification")
        
        # Get stratification data filtered by EVALID
        ppsa = self._get_plot_stratum_assignments()
        pop_stratum = self._get_population_stratum()
        
        # Prepare stratification data
        strat_df = self.prepare_stratification_data(ppsa, pop_stratum)
        
        # Join plot data with stratification
        plot_with_strat = self._join_plot_with_stratification(plot_data, strat_df)
        
        # Apply basis-specific adjustments if needed
        plot_with_strat = self._apply_basis_adjustments(plot_with_strat)
        
        # Calculate stratum-level estimates
        stratum_est = self._calculate_stratum_estimates(plot_with_strat)
        
        return stratum_est
    
    # === Population Estimation ===
    
    def _calculate_population_estimates(self, expanded_data: pl.DataFrame) -> pl.DataFrame:
        """
        Calculate final population-level estimates.
        
        Parameters
        ----------
        expanded_data : pl.DataFrame
            Data with expansion factors applied
            
        Returns
        -------
        pl.DataFrame
            Population-level estimates with per-acre values and variance
        """
        response_cols = self.get_response_columns()
        
        # Step 1: Aggregate to population level
        pop_estimates = self._aggregate_to_population(expanded_data, response_cols)
        
        # Step 2: Calculate per-acre values
        pop_estimates = self._calculate_per_acre_values(pop_estimates, response_cols)
        
        # Step 3: Add variance/SE for each estimate
        for _, output_name in response_cols.items():
            if output_name in pop_estimates.columns:
                pop_estimates = self.calculate_variance(pop_estimates, output_name)
        
        # Step 4: Add standard metadata
        pop_estimates = self._add_standard_metadata(pop_estimates)
        
        # Step 5: Add total columns if requested
        if self.config.totals:
            pop_estimates = self._add_total_columns(pop_estimates, response_cols)
        
        return pop_estimates
    
    # === Helper Methods ===
    
    def _get_estimation_type(self) -> str:
        """Get estimation type from class name."""
        class_name = self.__class__.__name__.lower()
        if 'area' in class_name:
            return 'area'
        elif 'volume' in class_name:
            return 'volume'
        elif 'biomass' in class_name:
            return 'biomass'
        elif 'tpa' in class_name:
            return 'tpa'
        elif 'treecount' in class_name:
            return 'tpa'  # TreeCountEstimator needs tree data like TPA
        elif 'mortality' in class_name:
            return 'mortality'
        elif 'growth' in class_name:
            return 'growth'
        else:
            return 'generic'
    
    def _get_plot_stratum_assignments(self) -> pl.DataFrame:
        """Get plot-stratum assignments filtered by current evaluation."""
        ppsa = self.db.tables["POP_PLOT_STRATUM_ASSGN"]
        
        if self.db.evalid:
            ppsa = ppsa.filter(pl.col("EVALID").is_in(self.db.evalid))
        
        return ppsa.collect() if hasattr(ppsa, 'collect') else ppsa
    
    def _get_population_stratum(self) -> pl.DataFrame:
        """Get population stratum data."""
        pop_stratum = self.db.tables["POP_STRATUM"]
        
        if self.db.evalid:
            pop_stratum = pop_stratum.filter(pl.col("EVALID").is_in(self.db.evalid))
        
        return pop_stratum.collect() if hasattr(pop_stratum, 'collect') else pop_stratum
    
    def prepare_stratification_data(self, ppsa: pl.DataFrame, 
                                   pop_stratum: pl.DataFrame) -> pl.DataFrame:
        """Prepare stratification data for joining with plots."""
        # Join assignment with stratum info
        strat_df = ppsa.join(
            pop_stratum.select([
                "CN", "EXPNS", "ADJ_FACTOR_MACR", "ADJ_FACTOR_SUBP",
                "ESTN_UNIT_CN", "EVALID"
            ]),
            left_on="STRATUM_CN",
            right_on="CN",
            how="inner"
        )
        
        # Rename CN to avoid conflicts
        strat_df = strat_df.rename({"CN": "STRATUM_CN_FINAL"})
        
        return strat_df
    
    def _join_plot_with_stratification(self, plot_data: pl.DataFrame,
                                      strat_df: pl.DataFrame) -> pl.DataFrame:
        """Join plot data with stratification information."""
        return plot_data.join(
            strat_df,
            on="PLT_CN",
            how="inner"
        )
    
    def _apply_basis_adjustments(self, data: pl.DataFrame) -> pl.DataFrame:
        """Apply MACR/SUBP basis adjustments if needed."""
        # This is typically handled in calculate_values, but can be overridden
        return data
    
    def _calculate_stratum_estimates(self, data: pl.DataFrame) -> pl.DataFrame:
        """Calculate stratum-level estimates."""
        response_cols = self.get_response_columns()
        
        # Build aggregation expressions
        agg_exprs = []
        for _, output_name in response_cols.items():
            plot_col = f"PLOT_{output_name}"
            if plot_col in data.columns:
                # Apply expansion factor
                agg_exprs.append(
                    (pl.col(plot_col) * pl.col("EXPNS")).sum().alias(f"STRATUM_{output_name}")
                )
        
        # Add sample size tracking
        agg_exprs.append(pl.count().alias("N_PLOTS"))
        
        # Group by stratum
        group_cols = ["ESTN_UNIT_CN", "STRATUM_CN_FINAL"]
        if self._group_cols:
            group_cols.extend(self._group_cols)
        
        stratum_estimates = data.group_by(group_cols).agg(agg_exprs)
        
        return stratum_estimates
    
    def _aggregate_to_population(self, stratum_data: pl.DataFrame,
                                response_cols: Dict[str, str]) -> pl.DataFrame:
        """Aggregate stratum estimates to population level."""
        agg_exprs = []
        
        for _, output_name in response_cols.items():
            stratum_col = f"STRATUM_{output_name}"
            if stratum_col in stratum_data.columns:
                agg_exprs.append(
                    pl.col(stratum_col).sum().alias(f"POP_{output_name}")
                )
        
        # Add total plot count
        agg_exprs.append(pl.col("N_PLOTS").sum().alias("TOTAL_PLOTS"))
        
        # Group by estimation unit and any custom groups
        group_cols = []
        if self._group_cols:
            group_cols.extend(self._group_cols)
        
        if group_cols:
            pop_estimates = stratum_data.group_by(group_cols).agg(agg_exprs)
        else:
            # No grouping - aggregate everything
            pop_estimates = stratum_data.select(agg_exprs)
        
        return pop_estimates
    
    def _calculate_per_acre_values(self, pop_data: pl.DataFrame,
                                  response_cols: Dict[str, str]) -> pl.DataFrame:
        """Calculate per-acre values from population totals."""
        per_acre_exprs = []
        
        # For area estimation, the denominator is the total area
        # For other estimations, it's typically the forest area
        for _, output_name in response_cols.items():
            pop_col = f"POP_{output_name}"
            if pop_col in pop_data.columns:
                # For area, the estimate is already in acres
                # For other metrics, divide by forest area
                if self._get_estimation_type() == 'area':
                    per_acre_exprs.append(
                        pl.col(pop_col).alias(f"{output_name}_ESTIMATE")
                    )
                else:
                    # Would need forest area for per-acre calculations
                    # For now, just pass through
                    per_acre_exprs.append(
                        pl.col(pop_col).alias(f"{output_name}_TOTAL")
                    )
        
        if per_acre_exprs:
            pop_data = pop_data.with_columns(per_acre_exprs)
        
        return pop_data
    
    def calculate_variance(self, data: pl.DataFrame, estimate_col: str) -> pl.DataFrame:
        """
        Calculate variance and standard error for an estimate.
        
        Parameters
        ----------
        data : pl.DataFrame
            Data with population estimates
        estimate_col : str
            Name of the estimate column
            
        Returns
        -------
        pl.DataFrame
            Data with variance and SE columns added
        """
        # Simplified variance calculation
        # Real implementation would use the VarianceCalculator
        se_col = f"{estimate_col}_SE"
        se_perc_col = f"{estimate_col}_SE_PERC"
        
        # Placeholder: assume 5% SE for now
        data = data.with_columns([
            (pl.col(f"{estimate_col}_ESTIMATE") * 0.05).alias(se_col),
            pl.lit(5.0).alias(se_perc_col)
        ])
        
        return data
    
    def _add_standard_metadata(self, estimates: pl.DataFrame) -> pl.DataFrame:
        """Add standard metadata columns to estimates."""
        metadata_cols = [
            pl.lit(self._get_year()).alias("YEAR"),
            pl.col("TOTAL_PLOTS").alias("N")
        ]
        
        return estimates.with_columns(metadata_cols)
    
    def _add_total_columns(self, estimates: pl.DataFrame,
                          response_cols: Dict[str, str]) -> pl.DataFrame:
        """Add total columns if requested."""
        total_exprs = []
        
        for _, output_name in response_cols.items():
            if f"POP_{output_name}" in estimates.columns:
                total_exprs.append(
                    pl.col(f"POP_{output_name}").alias(f"{output_name}_TOTAL")
                )
        
        if total_exprs:
            estimates = estimates.with_columns(total_exprs)
        
        return estimates
    
    def _format_output(self, estimates: pl.DataFrame) -> pl.DataFrame:
        """Format final output according to configuration."""
        # Rename columns for consistency
        rename_map = {}
        
        # Standard column renaming
        if "AREA_ESTIMATE" in estimates.columns:
            rename_map["AREA_ESTIMATE"] = "AREA_ESTIMATE"
        
        if rename_map:
            estimates = estimates.rename(rename_map)
        
        # Select and order columns
        output_cols = []
        
        # Add grouping columns first
        if self._group_cols:
            output_cols.extend(self._group_cols)
        
        # Add estimate columns
        for col in estimates.columns:
            if col not in output_cols and (
                "ESTIMATE" in col or "SE" in col or 
                "TOTAL" in col or col in ["YEAR", "N"]
            ):
                output_cols.append(col)
        
        if output_cols:
            estimates = estimates.select(output_cols)
        
        return estimates
    
    def _get_year(self) -> int:
        """Get the inventory year from the evaluation."""
        # TODO: Get from EVALID or POP_EVAL table
        return 2023
    
    # === Progress Tracking ===
    
    def _start_progress(self, description: str):
        """Start progress tracking."""
        if hasattr(self.config, 'show_progress') and self.config.show_progress:
            self._progress = Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                TimeElapsedColumn(),
                console=self.console
            )
            self._progress.start()
            self._task_id = self._progress.add_task(description, total=None)
    
    def _update_progress(self, description: str):
        """Update progress description."""
        if self._progress and self._task_id is not None:
            self._progress.update(self._task_id, description=description)
    
    def _stop_progress(self):
        """Stop progress tracking."""
        if self._progress:
            self._progress.stop()
            self._progress = None
            self._task_id = None