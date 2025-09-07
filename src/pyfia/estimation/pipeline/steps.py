"""
FIA-specific pipeline steps for common estimation patterns.

This module provides concrete implementations of pipeline steps that handle
the typical workflow patterns in FIA estimation: data loading, filtering,
joining, value calculation, aggregation, and output formatting.
"""

from typing import Dict, List, Optional, Type, Union, Any, Set
import warnings

import polars as pl

from ...core import FIA
from ...filters.common import (
    apply_tree_filters_common,
    apply_area_filters_common,
    setup_grouping_columns_common
)
from ..config import EstimatorConfig
from ..lazy_evaluation import LazyFrameWrapper
from ..query_builders import QueryBuilderFactory, CompositeQueryBuilder
from ..join import JoinManager, JoinOptimizer
from ..caching import MemoryCache

from .core import (
    PipelineStep,
    ExecutionContext,
    DataContract,
    TableDataContract,
    FilteredDataContract,
    JoinedDataContract,
    ValuedDataContract,
    PlotEstimatesContract,
    StratifiedEstimatesContract,
    PopulationEstimatesContract,
    FormattedOutputContract,
    StepValidationError,
    PipelineException
)


# === Data Loading Steps ===

class LoadTablesStep(PipelineStep[TableDataContract, TableDataContract]):
    """
    Load required tables from the FIA database.
    
    This step loads the specified tables using optimized query builders
    and prepares them for filtering and processing.
    """
    
    def __init__(
        self,
        tables: List[str],
        apply_evalid_filter: bool = True,
        **kwargs
    ):
        """
        Initialize table loading step.
        
        Parameters
        ----------
        tables : List[str]
            Names of tables to load
        apply_evalid_filter : bool
            Whether to apply EVALID filtering
        """
        super().__init__(**kwargs)
        self.tables = tables
        self.apply_evalid_filter = apply_evalid_filter
    
    def get_input_contract(self) -> Type[TableDataContract]:
        return TableDataContract
    
    def get_output_contract(self) -> Type[TableDataContract]:
        return TableDataContract
    
    def execute_step(
        self, 
        input_data: TableDataContract, 
        context: ExecutionContext
    ) -> TableDataContract:
        """Load the required tables."""
        loaded_tables = {}
        
        # Initialize query factory and cache
        cache = MemoryCache(max_size_mb=256, max_entries=100)
        query_factory = QueryBuilderFactory()
        
        # Get EVALID filter if applicable
        evalid = None
        if self.apply_evalid_filter and hasattr(context.db, 'current_evalids'):
            evalid = context.db.current_evalids
        
        for table_name in self.tables:
            try:
                # Use query builders for optimized loading where available
                builder_map = {
                    "TREE": "tree",
                    "PLOT": "plot", 
                    "COND": "condition",
                    "POP_STRATUM": "stratification",
                    "POP_PLOT_STRATUM_ASSGN": "stratification"
                }
                
                if table_name in builder_map:
                    builder_type = builder_map[table_name]
                    builder = query_factory.create_builder(
                        builder_type, context.db, context.config, cache
                    )
                    
                    # Build query plan with EVALID filter
                    kwargs = {}
                    if evalid:
                        kwargs["evalid"] = evalid
                    
                    plan = builder.build_query_plan(**kwargs)
                    table_wrapper = builder.execute(plan)
                else:
                    # Fall back to direct table loading
                    if table_name not in context.db.tables:
                        context.db.load_table(table_name)
                    
                    table_ref = context.db.tables[table_name]
                    
                    # Apply EVALID filter if available and applicable
                    if evalid and table_name in ["PLOT", "COND", "TREE"]:
                        if "EVALID" in table_ref.columns:
                            if isinstance(evalid, list):
                                table_ref = table_ref.filter(pl.col("EVALID").is_in(evalid))
                            else:
                                table_ref = table_ref.filter(pl.col("EVALID") == evalid)
                    
                    table_wrapper = LazyFrameWrapper(table_ref)
                
                loaded_tables[table_name] = table_wrapper
                
            except Exception as e:
                raise PipelineException(
                    f"Failed to load table {table_name}: {e}",
                    step_id=self.step_id,
                    cause=e
                )
        
        return TableDataContract(
            tables=loaded_tables,
            evalid=evalid,
            step_id=self.step_id
        )


class LoadRequiredTablesStep(LoadTablesStep):
    """
    Load tables required for a specific estimation type.
    
    Automatically determines which tables are needed based on the
    estimation configuration and loads them optimally.
    """
    
    def __init__(self, estimation_type: str, **kwargs):
        """
        Initialize with estimation type.
        
        Parameters
        ----------
        estimation_type : str
            Type of estimation (volume, biomass, tpa, area, growth, mortality)
        """
        # Define required tables for each estimation type
        table_requirements = {
            "volume": ["PLOT", "COND", "TREE"],
            "biomass": ["PLOT", "COND", "TREE"], 
            "tpa": ["PLOT", "COND", "TREE"],
            "area": ["PLOT", "COND"],
            "growth": ["PLOT", "COND", "TREE"],
            "mortality": ["PLOT", "COND", "TREE"]
        }
        
        # All estimations need stratification tables for proper variance calculation
        base_tables = table_requirements.get(estimation_type, ["PLOT", "COND"])
        stratification_tables = ["POP_STRATUM", "POP_PLOT_STRATUM_ASSGN"]
        
        all_tables = base_tables + stratification_tables
        
        super().__init__(tables=all_tables, **kwargs)
        self.estimation_type = estimation_type


# === Data Filtering Steps ===

class FilterDataStep(PipelineStep[TableDataContract, FilteredDataContract]):
    """
    Apply domain filters to loaded data.
    
    Filters tree, condition, and plot data based on the configured
    domain expressions and estimation parameters.
    """
    
    def __init__(
        self,
        tree_domain: Optional[str] = None,
        area_domain: Optional[str] = None,
        plot_domain: Optional[str] = None,
        **kwargs
    ):
        """
        Initialize filtering step.
        
        Parameters
        ----------
        tree_domain : Optional[str]
            Tree-level domain filter
        area_domain : Optional[str]  
            Area/condition-level domain filter
        plot_domain : Optional[str]
            Plot-level domain filter
        """
        super().__init__(**kwargs)
        self.tree_domain = tree_domain
        self.area_domain = area_domain
        self.plot_domain = plot_domain
    
    def get_input_contract(self) -> Type[TableDataContract]:
        return TableDataContract
    
    def get_output_contract(self) -> Type[FilteredDataContract]:
        return FilteredDataContract
    
    def execute_step(
        self,
        input_data: TableDataContract,
        context: ExecutionContext
    ) -> FilteredDataContract:
        """Apply domain filters to the loaded data."""
        
        # Get domain filters from step config or context config
        tree_domain = self.tree_domain or context.config.tree_domain
        area_domain = self.area_domain or context.config.area_domain
        plot_domain = self.plot_domain or getattr(context.config, "plot_domain", None)
        
        # Filter tree data if available
        tree_data = None
        if "TREE" in input_data.tables:
            tree_df = input_data.tables["TREE"].collect()
            
            # Apply common tree filters
            tree_df = apply_tree_filters_common(
                tree_df,
                context.config.tree_type,
                tree_domain,
                require_volume=False  # Will be handled by specific estimation steps
            )
            
            tree_data = LazyFrameWrapper(tree_df.lazy())
        
        # Filter condition data (always required)
        if "COND" not in input_data.tables:
            raise StepValidationError(
                "Condition data (COND) is required for all estimations",
                step_id=self.step_id
            )
        
        cond_df = input_data.tables["COND"].collect()
        
        # Apply common area filters
        cond_df = apply_area_filters_common(
            cond_df,
            context.config.land_type,
            area_domain
        )
        
        condition_data = LazyFrameWrapper(cond_df.lazy())
        
        # Filter plot data if available and plot_domain specified
        plot_data = None
        if "PLOT" in input_data.tables and plot_domain:
            plot_df = input_data.tables["PLOT"].collect()
            
            # Apply plot domain filter (simplified - could be enhanced)
            if plot_domain:
                try:
                    # Convert simple expressions to Polars expressions
                    # This is a simplified implementation - could be more sophisticated
                    filter_expr = pl.Expr.from_string(plot_domain)
                    plot_df = plot_df.filter(filter_expr)
                except Exception as e:
                    warnings.warn(
                        f"Failed to apply plot_domain filter '{plot_domain}': {e}. "
                        "Filter will be ignored.",
                        UserWarning
                    )
            
            plot_data = LazyFrameWrapper(plot_df.lazy())
        
        return FilteredDataContract(
            tree_data=tree_data,
            condition_data=condition_data,
            plot_data=plot_data,
            tree_domain=tree_domain,
            area_domain=area_domain, 
            plot_domain=plot_domain,
            step_id=self.step_id
        )


class ApplyDomainFiltersStep(FilterDataStep):
    """
    Apply domain filters using configuration from execution context.
    
    This step automatically uses the domain filters specified in the
    EstimatorConfig rather than requiring explicit parameters.
    """
    
    def __init__(self, **kwargs):
        """Initialize with no explicit domain filters."""
        super().__init__(**kwargs)
    
    def execute_step(
        self,
        input_data: TableDataContract,
        context: ExecutionContext
    ) -> FilteredDataContract:
        """Apply domain filters from configuration."""
        # Override domain filters with config values
        self.tree_domain = context.config.tree_domain
        self.area_domain = context.config.area_domain
        self.plot_domain = getattr(context.config, "plot_domain", None)
        
        return super().execute_step(input_data, context)


class ApplyModuleFiltersStep(PipelineStep[FilteredDataContract, FilteredDataContract]):
    """
    Apply module-specific filters to data.
    
    This step applies filters that are specific to particular estimation
    modules (e.g., requiring diameter measurements for volume estimation).
    """
    
    def __init__(self, estimation_type: str, **kwargs):
        """
        Initialize module filter step.
        
        Parameters
        ----------
        estimation_type : str
            Type of estimation requiring specific filters
        """
        super().__init__(**kwargs)
        self.estimation_type = estimation_type
    
    def get_input_contract(self) -> Type[FilteredDataContract]:
        return FilteredDataContract
    
    def get_output_contract(self) -> Type[FilteredDataContract]:
        return FilteredDataContract
    
    def execute_step(
        self,
        input_data: FilteredDataContract,
        context: ExecutionContext
    ) -> FilteredDataContract:
        """Apply module-specific filters."""
        
        # Apply estimation-specific filters
        tree_data = input_data.tree_data
        cond_data = input_data.condition_data
        
        if tree_data and self.estimation_type in ["volume", "biomass"]:
            # Volume and biomass require diameter measurements
            tree_df = tree_data.collect()
            
            # Filter out trees without diameter measurements
            tree_df = tree_df.filter(
                (pl.col("DIA").is_not_null()) & 
                (pl.col("DIA") > 0.0)
            )
            
            tree_data = LazyFrameWrapper(tree_df.lazy())
        
        elif tree_data and self.estimation_type == "growth":
            # Growth requires previous diameter measurements
            tree_df = tree_data.collect()
            
            tree_df = tree_df.filter(
                (pl.col("DIA").is_not_null()) & 
                (pl.col("PREVDIA").is_not_null()) &
                (pl.col("DIA") > 0.0) &
                (pl.col("PREVDIA") > 0.0)
            )
            
            tree_data = LazyFrameWrapper(tree_df.lazy())
        
        return FilteredDataContract(
            tree_data=tree_data,
            condition_data=cond_data,
            plot_data=input_data.plot_data,
            tree_domain=input_data.tree_domain,
            area_domain=input_data.area_domain,
            plot_domain=input_data.plot_domain,
            step_id=self.step_id
        )


# === Data Joining Steps ===

class JoinDataStep(PipelineStep[FilteredDataContract, JoinedDataContract]):
    """
    Join filtered data for estimation.
    
    Performs optimized joins between tree, condition, and plot data
    to create the dataset needed for value calculations.
    """
    
    def __init__(
        self,
        join_strategy: str = "optimized",
        include_plot_data: bool = False,
        **kwargs
    ):
        """
        Initialize join step.
        
        Parameters
        ----------
        join_strategy : str
            Join strategy to use (optimized, standard)
        include_plot_data : bool
            Whether to include plot-level data in joins
        """
        super().__init__(**kwargs)
        self.join_strategy = join_strategy
        self.include_plot_data = include_plot_data
    
    def get_input_contract(self) -> Type[FilteredDataContract]:
        return FilteredDataContract
    
    def get_output_contract(self) -> Type[JoinedDataContract]:
        return JoinedDataContract
    
    def execute_step(
        self,
        input_data: FilteredDataContract,
        context: ExecutionContext
    ) -> JoinedDataContract:
        """Join the filtered data."""
        
        if input_data.tree_data is not None:
            # Tree-based estimation: join trees with conditions
            
            # Select needed columns from conditions
            cond_cols = ["PLT_CN", "CONDID", "CONDPROP_UNADJ", "COND_STATUS_CD"]
            cond_df = input_data.condition_data.collect().select(cond_cols)
            
            # Join trees with conditions
            tree_df = input_data.tree_data.collect()
            
            # Use optimized join if available
            if self.join_strategy == "optimized":
                # Initialize join manager
                cache = MemoryCache(max_size_mb=256, max_entries=100) 
                join_manager = JoinManager(config=context.config, cache=cache)
                
                # Convert to LazyFrameWrapper for optimizer
                tree_wrapper = LazyFrameWrapper(tree_df.lazy())
                cond_wrapper = LazyFrameWrapper(cond_df.lazy())
                
                # Execute optimized join
                joined_wrapper = join_manager.join(
                    tree_wrapper,
                    cond_wrapper,
                    left_on=["PLT_CN", "CONDID"],
                    right_on=["PLT_CN", "CONDID"],
                    how="inner",
                    left_name="TREE",
                    right_name="COND"
                )
                joined_df = joined_wrapper.collect() if hasattr(joined_wrapper, 'collect') else joined_wrapper
            else:
                # Standard join
                joined_df = tree_df.join(
                    cond_df,
                    on=["PLT_CN", "CONDID"],
                    how="inner"
                )
            
            # Include plot data if requested
            if self.include_plot_data and input_data.plot_data:
                plot_df = input_data.plot_data.collect()
                plot_cols = ["PLT_CN"] + [c for c in plot_df.columns if c != "PLT_CN"]
                plot_subset = plot_df.select(plot_cols)
                
                joined_df = joined_df.join(plot_subset, on="PLT_CN", how="left")
            
        else:
            # Area estimation: use conditions only
            joined_df = input_data.condition_data.collect()
            
            # Include plot data if requested  
            if self.include_plot_data and input_data.plot_data:
                plot_df = input_data.plot_data.collect()
                plot_cols = ["PLT_CN"] + [c for c in plot_df.columns if c != "PLT_CN"]
                plot_subset = plot_df.select(plot_cols)
                
                joined_df = joined_df.join(plot_subset, on="PLT_CN", how="left")
        
        # Set up grouping columns
        group_cols = []
        if context.config.grp_by:
            if isinstance(context.config.grp_by, str):
                group_cols = [context.config.grp_by]
            else:
                group_cols = list(context.config.grp_by)
        
        # Add common grouping columns
        if context.config.by_species and "SPCD" not in group_cols:
            group_cols.append("SPCD")
        
        # Set up grouping columns using common utilities
        if group_cols or context.config.by_species or context.config.by_size_class:
            joined_df, group_cols = setup_grouping_columns_common(
                joined_df,
                context.config.grp_by,
                context.config.by_species,
                context.config.by_size_class,
                return_dataframe=True
            )
        
        return JoinedDataContract(
            data=LazyFrameWrapper(joined_df.lazy()),
            group_columns=group_cols,
            step_id=self.step_id
        )


class OptimizedJoinStep(JoinDataStep):
    """
    Optimized join step using Phase 3 join optimization.
    
    This step uses the advanced join optimization capabilities
    from Phase 3 to achieve optimal join performance.
    """
    
    def __init__(self, **kwargs):
        """Initialize with optimized join strategy."""
        super().__init__(join_strategy="optimized", **kwargs)


class PrepareEstimationDataStep(JoinDataStep):
    """
    Prepare data for estimation with all required joins and grouping.
    
    This is a comprehensive step that handles the complete data preparation
    process including joins, grouping setup, and validation.
    """
    
    def __init__(self, estimation_type: str, **kwargs):
        """
        Initialize data preparation step.
        
        Parameters
        ----------
        estimation_type : str
            Type of estimation to prepare for
        """
        super().__init__(**kwargs)
        self.estimation_type = estimation_type
    
    def execute_step(
        self,
        input_data: FilteredDataContract,
        context: ExecutionContext
    ) -> JoinedDataContract:
        """Prepare data with all required processing."""
        
        # Determine if plot data is needed
        plot_needed = self.estimation_type in ["growth", "mortality"] 
        self.include_plot_data = plot_needed
        
        # Execute the join
        result = super().execute_step(input_data, context)
        
        # Add estimation-specific validation
        joined_df = result.data.collect()
        
        if self.estimation_type in ["volume", "biomass"] and "DIA" not in joined_df.columns:
            raise StepValidationError(
                f"{self.estimation_type} estimation requires diameter (DIA) measurements",
                step_id=self.step_id
            )
        
        if self.estimation_type == "growth" and "PREVDIA" not in joined_df.columns:
            raise StepValidationError(
                "Growth estimation requires previous diameter (PREVDIA) measurements",
                step_id=self.step_id
            )
        
        # Update the result with validated data
        result.data = LazyFrameWrapper(joined_df.lazy())
        
        return result