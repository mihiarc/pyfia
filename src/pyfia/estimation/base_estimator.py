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
from typing import Dict, List, Optional, Tuple, Union

import polars as pl
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn

from ..core import FIA
from ..filters import apply_area_filters
from ..filters.common import setup_grouping_columns
from .aggregation import (
    EstimationType,
    UnifiedAggregationConfig,
    UnifiedEstimationWorkflow,
)
from .caching import cached_operation
from .config import EstimatorConfig
from .join import JoinManager, get_join_manager
from .query_builders import CompositeQueryBuilder, FrameWrapper, MemoryCache


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
        # Use the new JoinManager instead of JoinOptimizer
        self._join_manager = JoinManager(
            config=self.config,
            cache=MemoryCache(max_size_mb=256),
            enable_optimization=True,
            enable_caching=True
        )
        self._metrics = EstimationMetrics()
        
        # Shared caches for commonly used tables
        self._ref_species_cache: Optional[pl.DataFrame] = None
        self._pop_stratum_cache: Optional[pl.DataFrame] = None
        self._ppsa_cache: Optional[pl.DataFrame] = None

        # Progress tracking
        self.console = Console()
        self._progress: Optional[Progress] = None
        self._task_id: Optional[int] = None

    # === Join Helper Methods (using new JoinManager) ===
    
    def _optimized_join(
        self,
        left: Union[pl.DataFrame, pl.LazyFrame],
        right: Union[pl.DataFrame, pl.LazyFrame],
        **kwargs
    ) -> pl.LazyFrame:
        """
        Perform an optimized join using the JoinManager.
        
        This replaces direct .join() calls with optimized versions.
        """
        result = self._join_manager.join(left, right, **kwargs)
        # Convert FrameWrapper back to LazyFrame for compatibility
        return result.frame if hasattr(result, 'frame') else result
    
    # === Shared Table Access Methods ===
    
    @cached_operation("ref_species", ttl_seconds=3600)
    def _get_ref_species(self) -> pl.DataFrame:
        """
        Get reference species table with caching.
        
        This is a shared method used by multiple estimators (TPA, Volume, Biomass, Mortality)
        to access the REF_SPECIES table with consistent caching behavior.
        
        Returns
        -------
        pl.DataFrame
            Reference species table, or empty DataFrame if not available
        """
        if self._ref_species_cache is None:
            if "REF_SPECIES" not in self.db.tables:
                try:
                    self.db.load_table("REF_SPECIES")
                except Exception:
                    # REF_SPECIES may not be available
                    return pl.DataFrame()
            
            ref_species = self.db.tables.get("REF_SPECIES")
            if ref_species is None:
                return pl.DataFrame()
            
            # Collect if lazy
            if isinstance(ref_species, pl.LazyFrame):
                self._ref_species_cache = ref_species.collect()
            else:
                self._ref_species_cache = ref_species
        
        return self._ref_species_cache
    
    @cached_operation("stratification_data", ttl_seconds=1800)
    def _get_stratification_data(self, required_adj_factors: Optional[List[str]] = None) -> pl.LazyFrame:
        """
        Get stratification data with caching.
        
        This is a shared method used by all estimators to load and join
        stratification tables (POP_PLOT_STRATUM_ASSGN and POP_STRATUM).
        
        Parameters
        ----------
        required_adj_factors : Optional[List[str]]
            List of required adjustment factors: ["MICR", "SUBP", "MACR"]
            If None, includes all available factors
        
        Returns
        -------
        pl.LazyFrame
            Lazy frame with joined PPSA and POP_STRATUM data
        """
        # Load and cache PPSA data
        if self._ppsa_cache is None:
            # Load table from database
            if "POP_PLOT_STRATUM_ASSGN" not in self.db.tables:
                self.db.load_table("POP_PLOT_STRATUM_ASSGN")
            ppsa_lazy = self.db.tables["POP_PLOT_STRATUM_ASSGN"]
            
            # Ensure it's a LazyFrame
            if not isinstance(ppsa_lazy, pl.LazyFrame):
                ppsa_lazy = ppsa_lazy.lazy()
            
            # Apply EVALID filter if needed
            if self.db.evalid:
                ppsa_lazy = ppsa_lazy.filter(pl.col("EVALID").is_in(self.db.evalid))
            self._ppsa_cache = ppsa_lazy

        # Load and cache POP_STRATUM data
        if self._pop_stratum_cache is None:
            # Load table from database
            if "POP_STRATUM" not in self.db.tables:
                self.db.load_table("POP_STRATUM")
            pop_stratum_lazy = self.db.tables["POP_STRATUM"]
            
            # Ensure it's a LazyFrame
            if not isinstance(pop_stratum_lazy, pl.LazyFrame):
                pop_stratum_lazy = pop_stratum_lazy.lazy()
            
            # Apply EVALID filter if needed
            if self.db.evalid:
                pop_stratum_lazy = pop_stratum_lazy.filter(
                    pl.col("EVALID").is_in(self.db.evalid)
                )
            self._pop_stratum_cache = pop_stratum_lazy

        # Dynamic column selection based on availability and requirements
        pop_stratum_cols = self._pop_stratum_cache.collect_schema().names()
        select_cols = ["CN", "EXPNS"]
        
        # Add adjustment factors based on requirements and availability
        if required_adj_factors is None:
            # Default to all standard adjustment factors
            required_adj_factors = ["MICR", "SUBP", "MACR"]
            
        for factor in required_adj_factors:
            col_name = f"ADJ_FACTOR_{factor}"
            if col_name in pop_stratum_cols:
                select_cols.append(col_name)
        
        # Select columns and rename CN to STRATUM_CN for joining
        pop_stratum_selected = self._pop_stratum_cache.select(select_cols).rename({"CN": "STRATUM_CN"})
        
        # Use optimized join with consistent approach
        strat_lazy = self._optimized_join(
            self._ppsa_cache,
            pop_stratum_selected,
            on="STRATUM_CN",
            how="inner"
        )
        
        return strat_lazy

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
            tree_wrapper, cond_wrapper = self._get_filtered_data()

            # Step 3: Join and prepare data (lazy)
            self._update_progress("Preparing data...")
            prepared_wrapper = self._prepare_estimation_data(tree_wrapper, cond_wrapper)

            # Step 4: Calculate module-specific values
            self._update_progress("Calculating values...")
            valued_wrapper = self._calculate_values(prepared_wrapper)

            # Step 5: Calculate plot-level estimates (lazy)
            self._update_progress("Aggregating to plots...")
            plot_wrapper = self._calculate_plot_estimates(valued_wrapper)

            # Step 6: Apply stratification (collection point)
            self._update_progress("Applying stratification...")
            stratified_df = self._apply_stratification(plot_wrapper)

            # Step 7: Calculate population estimates
            self._update_progress("Calculating population estimates...")
            pop_estimates = self._calculate_population_estimates(stratified_df)

            # Step 8: Format output
            self._update_progress("Formatting results...")
            final_results = self.format_output(pop_estimates)

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

    def _get_filtered_data(self) -> Tuple[Optional[FrameWrapper], FrameWrapper]:
        """
        Get data from database using composite query builder.
        
        Returns
        -------
        Tuple[Optional[FrameWrapper], FrameWrapper]
            Filtered tree and condition frame wrappers
        """
        # Build estimation query using composite builder
        estimation_type = self._get_estimation_type()

        # Get EVALID from database if available
        evalid = None
        if hasattr(self.db, 'evalid'):
            evalid = self.db.evalid

        # Get tree columns if the subclass defines them
        tree_columns = None
        if hasattr(self, 'get_tree_columns'):
            tree_columns = self.get_tree_columns()
        
        # Build optimized query
        query_results = self._composite_builder.build_estimation_query(
            estimation_type=estimation_type,
            evalid=evalid,
            tree_domain=self.config.tree_domain,
            area_domain=self.config.area_domain,
            plot_domain=self.config.plot_domain,
            tree_type=getattr(self.config, 'tree_type', None),
            land_type=getattr(self.config, 'land_type', None),
            tree_columns=tree_columns  # Pass tree columns if available
        )

        # Extract results
        cond_wrapper = query_results.get("conditions")
        tree_wrapper = query_results.get("trees")

        # Ensure we have condition data
        if cond_wrapper is None:
            # Fall back to getting conditions directly
            cond_df = self.db.get_conditions()
            cond_df = apply_area_filters(
                cond_df,
                getattr(self.config, 'land_type', None),
                self.config.area_domain
            )
            cond_wrapper = FrameWrapper(pl.LazyFrame(cond_df))

        # Apply module-specific filters - ALWAYS apply for proper filtering
        # Convert to eager if needed
        if cond_wrapper.is_lazy:
            cond_df = cond_wrapper.collect()
        else:
            cond_df = cond_wrapper.frame

        if tree_wrapper is not None and tree_wrapper.is_lazy:
            tree_df = tree_wrapper.collect()
        else:
            tree_df = tree_wrapper.frame if tree_wrapper else None

        # Apply filters
        tree_df, cond_df = self.apply_module_filters(tree_df, cond_df)

        # Convert back to lazy
        tree_wrapper = FrameWrapper(pl.LazyFrame(tree_df)) if tree_df is not None else None
        cond_wrapper = FrameWrapper(pl.LazyFrame(cond_df))

        return tree_wrapper, cond_wrapper

    def _prepare_estimation_data(self,
                                     tree_wrapper: Optional[FrameWrapper],
                                     cond_wrapper: FrameWrapper) -> FrameWrapper:
        """
        Join data and prepare for estimation using lazy evaluation.
        
        Parameters
        ----------
        tree_wrapper : Optional[FrameWrapper]
            Tree data wrapper (None for area estimation)
        cond_wrapper : FrameWrapper
            Condition data wrapper
            
        Returns
        -------
        FrameWrapper
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
                joined = FrameWrapper(joined_frame)
            else:
                # Eager join
                tree_df = tree_wrapper.frame
                cond_df = cond_cols_to_join.collect() if hasattr(cond_cols_to_join, 'collect') else cond_cols_to_join
                joined_df = tree_df.join(
                    cond_df,
                    on=["PLT_CN", "CONDID"],
                    how="inner"
                )
                joined = FrameWrapper(pl.LazyFrame(joined_df))

            # Set up grouping columns
            if self.config.grp_by or self.config.by_species or self.config.by_size_class:
                # Need to collect for grouping setup
                data_df = joined.collect()
                data_df, group_cols = setup_grouping_columns(
                    data_df,
                    self.config.grp_by,
                    self.config.by_species,
                    self.config.by_size_class,
                    return_dataframe=True
                )
                self._group_cols = group_cols
                return FrameWrapper(pl.LazyFrame(data_df))
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

    def _calculate_values(self, data_wrapper: FrameWrapper) -> FrameWrapper:
        """
        Calculate module-specific values with lazy evaluation support.
        
        Parameters
        ----------
        data_wrapper : FrameWrapper
            Prepared data wrapper
            
        Returns
        -------
        FrameWrapper
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
            return FrameWrapper(valued_df)
        else:
            return FrameWrapper(pl.LazyFrame(valued_df))

    def _calculate_plot_estimates(self, data_wrapper: FrameWrapper) -> FrameWrapper:
        """
        Calculate plot-level estimates using lazy aggregation.
        
        Parameters
        ----------
        data_wrapper : FrameWrapper
            Data with calculated values
            
        Returns
        -------
        FrameWrapper
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

    def _apply_stratification(self, plot_wrapper: FrameWrapper) -> pl.DataFrame:
        """
        Apply stratification and calculate expansion factors.
        
        Parameters
        ----------
        plot_wrapper : FrameWrapper
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
        Calculate population estimates using the unified aggregation system.
        
        Parameters
        ----------
        expanded_data : pl.DataFrame
            Data with expansion factors applied
            
        Returns
        -------
        pl.DataFrame
            Population-level estimates with per-acre values and variance
        """
        # Get group columns if they exist
        group_cols = getattr(self, '_group_cols', None)
        if hasattr(self.config, 'grp_by') and self.config.grp_by:
            if isinstance(self.config.grp_by, str):
                group_cols = [self.config.grp_by]
            else:
                group_cols = list(self.config.grp_by)

        # Configure unified aggregation workflow
        config = UnifiedAggregationConfig(
            estimation_type=self._get_estimation_type_enum(),
            group_cols=group_cols,
            include_totals=getattr(self.config, 'totals', False),
            include_variance=not (hasattr(self.config, 'se') and self.config.se),
            response_columns=self.get_response_columns(),
            use_rfia_variance=("POP_ESTN_UNIT" in self.db.tables)
        )

        # Create and execute unified workflow
        workflow = UnifiedEstimationWorkflow(config)

        # Validate input data
        validation = workflow.validate_input_data(expanded_data)
        if not validation["is_valid"]:
            raise ValueError(f"Input validation failed: {validation['warnings']}")

        # Execute unified workflow
        return workflow.calculate_population_estimates(expanded_data)

    # === Helper Methods ===

    def _get_estimation_type(self) -> str:
        """Get estimation type from class name (legacy method)."""
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

    def _get_estimation_type_enum(self) -> EstimationType:
        """Get estimation type as enum for unified aggregation system."""
        class_name = self.__class__.__name__.lower()
        if 'area' in class_name:
            return EstimationType.AREA
        elif 'volume' in class_name:
            return EstimationType.VOLUME
        elif 'biomass' in class_name:
            return EstimationType.BIOMASS
        elif 'tpa' in class_name or 'treecount' in class_name:
            return EstimationType.TPA
        elif 'mortality' in class_name:
            return EstimationType.MORTALITY
        elif 'growth' in class_name:
            return EstimationType.GROWTH
        else:
            # Default to TPA for generic estimators
            return EstimationType.TPA

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
        # Use optimized join through JoinManager
        strat_lazy = self._optimized_join(
            ppsa,
            pop_stratum.select([
                "CN", "EXPNS", "ADJ_FACTOR_MACR", "ADJ_FACTOR_SUBP",
                "ESTN_UNIT_CN", "EVALID"
            ]),
            left_on="STRATUM_CN",
            right_on="CN",
            how="inner",
            left_name="POP_PLOT_STRATUM_ASSGN",
            right_name="POP_STRATUM"
        )
        strat_df = strat_lazy.collect() if hasattr(strat_lazy, 'collect') else strat_lazy

        # Rename CN to avoid conflicts
        strat_df = strat_df.rename({"CN": "STRATUM_CN_FINAL"})

        return strat_df

    def _join_plot_with_stratification(self, plot_data: pl.DataFrame,
                                      strat_df: pl.DataFrame) -> pl.DataFrame:
        """Join plot data with stratification information."""
        result = self._optimized_join(
            plot_data,
            strat_df,
            on="PLT_CN",
            how="inner",
            left_name="PLOT",
            right_name="STRATIFICATION"
        )
        return result.collect() if hasattr(result, 'collect') else result

    def _apply_basis_adjustments(self, data: pl.DataFrame) -> pl.DataFrame:
        """Apply MACR/SUBP basis adjustments if needed."""
        # This is typically handled in calculate_values, but can be overridden
        return data

    def _calculate_stratum_estimates(self, data: pl.DataFrame) -> pl.DataFrame:
        """Calculate stratum-level estimates."""
        response_cols = self.get_response_columns()

        # Build aggregation expressions
        agg_exprs = []
        rename_exprs = []
        
        for internal_col, output_name in response_cols.items():
            plot_col = f"PLOT_{output_name}"
            if plot_col in data.columns:
                # Apply expansion factor and aggregate
                stratum_col = f"STRATUM_{output_name}"
                agg_exprs.append(
                    (pl.col(plot_col) * pl.col("EXPNS")).sum().alias(stratum_col)
                )
                # Create rename expression to match expected column names
                rename_exprs.append(pl.col(stratum_col).alias(internal_col))

        # Add EXPNS as sum of expansion factors for the stratum
        agg_exprs.append(pl.col("EXPNS").sum().alias("EXPNS"))
        
        # Add sample size tracking
        agg_exprs.append(pl.count().alias("N_PLOTS"))

        # Group by stratum
        group_cols = ["ESTN_UNIT_CN", "STRATUM_CN_FINAL"]
        if self._group_cols:
            group_cols.extend(self._group_cols)

        stratum_estimates = data.group_by(group_cols).agg(agg_exprs)
        
        # Rename STRATUM_CN_FINAL to STRATUM_CN for workflow compatibility
        stratum_estimates = stratum_estimates.rename({"STRATUM_CN_FINAL": "STRATUM_CN"})
        
        # Add renamed columns to match response column names
        if rename_exprs:
            stratum_estimates = stratum_estimates.with_columns(rename_exprs)

        return stratum_estimates

    # All population aggregation logic is now handled by UnifiedEstimationWorkflow

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
