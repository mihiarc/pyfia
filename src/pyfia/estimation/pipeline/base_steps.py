"""
Base step implementations and common patterns for the pyFIA pipeline framework.

This module provides foundational step implementations that serve as building
blocks for more specific estimation steps. These base classes encapsulate
common patterns, error handling, validation helpers, and lazy evaluation
integration that other steps can inherit from.

Base Step Types:
- DataLoadingStep: Base for steps that load data from database
- FilteringStep: Base for steps that apply domain filters
- JoiningStep: Base for steps that combine multiple datasets
- CalculationStep: Base for steps that perform calculations
- AggregationStep: Base for steps that aggregate data
- FormattingStep: Base for steps that format output

Each base step provides:
- Standard error handling patterns
- Validation helpers for common FIA data requirements
- Integration with lazy evaluation framework
- Performance monitoring and caching support
- Proper type safety with generic contracts
"""

import time
from abc import abstractmethod
from typing import Any, Dict, List, Optional, Set, Type, Union, Tuple, TypeVar, Generic
import warnings

import polars as pl
from pydantic import BaseModel

# Type variables for generic pipeline steps
TInput = TypeVar("TInput", bound=BaseModel)
TOutput = TypeVar("TOutput", bound=BaseModel)

from ...core import FIA
from ...filters.common import (
    apply_tree_filters_common,
    apply_area_filters_common,
    setup_grouping_columns_common
)
from ..config import EstimatorConfig
from ..lazy_evaluation import LazyFrameWrapper
from ..caching import MemoryCache, CacheKey
from ..query_builders import QueryBuilderFactory

from .core import (
    PipelineStep,
    ExecutionContext,
    DataContract,
    StepValidationError,
    PipelineException,
    StepResult,
    StepStatus
)
from .contracts import (
    RawTablesContract,
    FilteredDataContract,
    JoinedDataContract,
    ValuedDataContract,
    PlotEstimatesContract,
    StratifiedEstimatesContract,
    PopulationEstimatesContract,
    FormattedOutputContract
)


class BaseEstimationStep(PipelineStep[TInput, TOutput]):
    """
    Abstract base class for all FIA estimation pipeline steps.
    
    Provides common functionality including error handling, validation,
    lazy evaluation integration, and performance monitoring that all
    estimation steps can inherit from.
    """
    
    def __init__(
        self,
        enable_caching: bool = True,
        cache_ttl_seconds: Optional[float] = None,
        validate_data: bool = True,
        performance_tracking: bool = True,
        **kwargs
    ):
        """
        Initialize base estimation step.
        
        Parameters
        ----------
        enable_caching : bool
            Whether to enable result caching
        cache_ttl_seconds : Optional[float]
            Cache time-to-live in seconds
        validate_data : bool
            Whether to perform data validation
        performance_tracking : bool
            Whether to track performance metrics
        **kwargs
            Additional arguments passed to parent
        """
        super().__init__(**kwargs)
        self.enable_caching = enable_caching
        self.cache_ttl_seconds = cache_ttl_seconds
        self.validate_data = validate_data
        self.performance_tracking = performance_tracking
        
        # Initialize caching if enabled
        if self.enable_caching:
            self.cache = MemoryCache(max_size_mb=128, max_entries=50)
        else:
            self.cache = None
    
    def validate_fia_identifiers(self, frame: Union[pl.DataFrame, pl.LazyFrame, LazyFrameWrapper]) -> None:
        """
        Validate that frame contains required FIA identifier columns.
        
        Parameters
        ----------
        frame : Union[pl.DataFrame, pl.LazyFrame, LazyFrameWrapper]
            Frame to validate
            
        Raises
        ------
        StepValidationError
            If required FIA identifiers are missing
        """
        # Extract underlying frame
        if isinstance(frame, LazyFrameWrapper):
            actual_frame = frame.frame
        else:
            actual_frame = frame
        
        # Get column names
        if isinstance(actual_frame, pl.LazyFrame):
            cols = set(actual_frame.collect_schema().names())
        else:
            cols = set(actual_frame.columns)
        
        # Check for core FIA identifiers
        required_identifiers = {"PLT_CN"}
        missing_identifiers = required_identifiers - cols
        
        if missing_identifiers:
            raise StepValidationError(
                f"Missing required FIA identifiers: {missing_identifiers}",
                step_id=self.step_id
            )
    
    def validate_record_count(
        self, 
        frame: Union[pl.DataFrame, pl.LazyFrame, LazyFrameWrapper],
        min_records: int = 1
    ) -> int:
        """
        Validate record count and return the count.
        
        Parameters
        ----------
        frame : Union[pl.DataFrame, pl.LazyFrame, LazyFrameWrapper]
            Frame to check
        min_records : int
            Minimum required record count
            
        Returns
        -------
        int
            Number of records in frame
            
        Raises
        ------
        StepValidationError
            If record count is below minimum
        """
        # Extract underlying frame
        if isinstance(frame, LazyFrameWrapper):
            actual_frame = frame.frame
        else:
            actual_frame = frame
        
        # Get record count efficiently
        if isinstance(actual_frame, pl.LazyFrame):
            record_count = actual_frame.select(pl.count()).collect().item()
        else:
            record_count = len(actual_frame)
        
        if record_count < min_records:
            raise StepValidationError(
                f"Insufficient records: {record_count} (minimum: {min_records})",
                step_id=self.step_id
            )
        
        return record_count
    
    def create_cache_key(self, input_data: DataContract, **kwargs) -> Optional[CacheKey]:
        """
        Create a cache key for this step's execution.
        
        Parameters
        ----------
        input_data : DataContract
            Input data contract
        **kwargs
            Additional parameters affecting the result
            
        Returns
        -------
        Optional[CacheKey]
            Cache key or None if caching disabled
        """
        if not self.enable_caching or not self.cache:
            return None
        
        # Create cache key from step configuration and input
        key_components = {
            "step_id": self.step_id,
            "step_class": self.__class__.__name__,
            "input_hash": hash(str(input_data)),
            "kwargs": kwargs
        }
        
        return CacheKey.from_dict(key_components)
    
    def handle_step_error(self, error: Exception, context: ExecutionContext) -> None:
        """
        Handle errors that occur during step execution.
        
        Parameters
        ----------
        error : Exception
            The error that occurred
        context : ExecutionContext
            Execution context for logging
        """
        error_info = {
            "step_id": self.step_id,
            "step_class": self.__class__.__name__,
            "error_type": type(error).__name__,
            "error_message": str(error),
            "timestamp": time.time()
        }
        
        # Log error details if debug mode enabled
        if context.debug:
            warnings.warn(
                f"Step {self.step_id} failed: {error}",
                category=UserWarning
            )
    
    def track_performance(self, context: ExecutionContext, **metrics) -> None:
        """
        Track performance metrics for this step.
        
        Parameters
        ----------
        context : ExecutionContext
            Execution context
        **metrics
            Performance metrics to track
        """
        if not self.performance_tracking:
            return
        
        # Add metrics to context
        step_metrics = {
            "step_id": self.step_id,
            "timestamp": time.time(),
            **metrics
        }
        
        context.set_context_data(f"metrics_{self.step_id}", step_metrics)


class DataLoadingStep(BaseEstimationStep[RawTablesContract, RawTablesContract]):
    """
    Base class for steps that load data from the FIA database.
    
    Provides common functionality for loading tables, applying EVALID filters,
    and managing database connections efficiently.
    """
    
    def __init__(
        self,
        tables: List[str],
        apply_evalid_filter: bool = True,
        optimize_query: bool = True,
        **kwargs
    ):
        """
        Initialize data loading step.
        
        Parameters
        ----------
        tables : List[str]
            Names of tables to load
        apply_evalid_filter : bool
            Whether to apply EVALID filtering during load
        optimize_query : bool
            Whether to use query optimization
        **kwargs
            Additional arguments
        """
        super().__init__(**kwargs)
        self.tables = tables
        self.apply_evalid_filter = apply_evalid_filter
        self.optimize_query = optimize_query
        
        # Initialize query builder factory
        self.query_factory = QueryBuilderFactory()
    
    def get_input_contract(self) -> Type[RawTablesContract]:
        return RawTablesContract
    
    def get_output_contract(self) -> Type[RawTablesContract]:
        return RawTablesContract
    
    def load_table_with_optimization(
        self, 
        table_name: str, 
        db: FIA,
        evalid: Optional[Union[int, List[int]]] = None
    ) -> LazyFrameWrapper:
        """
        Load a table with query optimization.
        
        Parameters
        ----------
        table_name : str
            Name of table to load
        db : FIA
            Database connection
        evalid : Optional[Union[int, List[int]]]
            EVALID filter to apply
            
        Returns
        -------
        LazyFrameWrapper
            Loaded table data
        """
        try:
            # Use optimized query builder if available
            builder_type = {
                "TREE": "tree",
                "COND": "condition", 
                "PLOT": "plot",
                "POP_PLOT_STRATUM_ASSGN": "stratum"
            }.get(table_name)
            
            if builder_type and self.optimize_query:
                builder = self.query_factory.create_builder(builder_type)
                if evalid:
                    builder.add_evalid_filter(evalid)
                query_result = builder.build_and_execute(db)
                return LazyFrameWrapper(query_result)
            else:
                # Fall back to standard loading
                return self._load_table_standard(table_name, db, evalid)
                
        except Exception as e:
            raise PipelineException(
                f"Failed to load table {table_name}: {e}",
                step_id=self.step_id,
                cause=e
            )
    
    def _load_table_standard(
        self, 
        table_name: str, 
        db: FIA,
        evalid: Optional[Union[int, List[int]]] = None
    ) -> LazyFrameWrapper:
        """Standard table loading without optimization."""
        # Use FIA database's data reader
        reader = db.data_reader
        
        # Build WHERE clause for EVALID if needed
        where_clause = None
        if evalid and self.apply_evalid_filter:
            if isinstance(evalid, list):
                evalid_str = ",".join(map(str, evalid))
                where_clause = f"EVALID IN ({evalid_str})"
            else:
                where_clause = f"EVALID = {evalid}"
        
        # Load table
        table_data = reader.load_table(table_name, where_clause=where_clause)
        return LazyFrameWrapper(table_data)
    
    @abstractmethod
    def execute_step(
        self, 
        input_data: RawTablesContract, 
        context: ExecutionContext
    ) -> RawTablesContract:
        """Execute the data loading logic."""
        pass


class FilteringStep(BaseEstimationStep[RawTablesContract, FilteredDataContract]):
    """
    Base class for steps that apply domain filters to data.
    
    Provides common functionality for applying tree domain, area domain,
    and plot domain filters using the established filter utilities.
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
            Tree domain filter expression
        area_domain : Optional[str]
            Area domain filter expression  
        plot_domain : Optional[str]
            Plot domain filter expression
        **kwargs
            Additional arguments
        """
        super().__init__(**kwargs)
        self.tree_domain = tree_domain
        self.area_domain = area_domain
        self.plot_domain = plot_domain
    
    def get_input_contract(self) -> Type[RawTablesContract]:
        return RawTablesContract
    
    def get_output_contract(self) -> Type[FilteredDataContract]:
        return FilteredDataContract
    
    def apply_tree_filters(
        self, 
        tree_data: LazyFrameWrapper,
        config: EstimatorConfig
    ) -> LazyFrameWrapper:
        """
        Apply tree domain filters.
        
        Parameters
        ----------
        tree_data : LazyFrameWrapper
            Tree data to filter
        config : EstimatorConfig
            Configuration with filter settings
            
        Returns
        -------
        LazyFrameWrapper
            Filtered tree data
        """
        if not self.tree_domain:
            return tree_data
        
        try:
            # Use common filter utilities
            filtered_frame = apply_tree_filters_common(
                tree_data.frame,
                tree_domain=self.tree_domain,
                config=config
            )
            return LazyFrameWrapper(filtered_frame)
        except Exception as e:
            raise PipelineException(
                f"Failed to apply tree filters: {e}",
                step_id=self.step_id,
                cause=e
            )
    
    def apply_area_filters(
        self, 
        condition_data: LazyFrameWrapper,
        config: EstimatorConfig
    ) -> LazyFrameWrapper:
        """
        Apply area domain filters.
        
        Parameters
        ----------
        condition_data : LazyFrameWrapper
            Condition data to filter
        config : EstimatorConfig
            Configuration with filter settings
            
        Returns
        -------
        LazyFrameWrapper
            Filtered condition data
        """
        if not self.area_domain:
            return condition_data
        
        try:
            # Use common filter utilities
            filtered_frame = apply_area_filters_common(
                condition_data.frame,
                area_domain=self.area_domain,
                config=config
            )
            return LazyFrameWrapper(filtered_frame)
        except Exception as e:
            raise PipelineException(
                f"Failed to apply area filters: {e}",
                step_id=self.step_id,
                cause=e
            )
    
    def apply_plot_filters(
        self, 
        plot_data: LazyFrameWrapper
    ) -> LazyFrameWrapper:
        """
        Apply plot domain filters.
        
        Parameters
        ----------
        plot_data : LazyFrameWrapper
            Plot data to filter
            
        Returns
        -------
        LazyFrameWrapper
            Filtered plot data
        """
        if not self.plot_domain:
            return plot_data
        
        try:
            # Parse and apply plot domain filter
            # This is a simplified implementation - could be more sophisticated
            filtered_frame = plot_data.frame.filter(pl.expr(self.plot_domain))
            return LazyFrameWrapper(filtered_frame)
        except Exception as e:
            raise PipelineException(
                f"Failed to apply plot filters: {e}",
                step_id=self.step_id,
                cause=e
            )


class JoiningStep(BaseEstimationStep[FilteredDataContract, JoinedDataContract]):
    """
    Base class for steps that join multiple datasets together.
    
    Provides common functionality for joining tables using various strategies
    and optimizing join performance.
    """
    
    def __init__(
        self,
        join_strategy: str = "standard",
        optimize_joins: bool = True,
        **kwargs
    ):
        """
        Initialize joining step.
        
        Parameters
        ----------
        join_strategy : str
            Join strategy to use (standard, optimized, etc.)
        optimize_joins : bool
            Whether to optimize join performance
        **kwargs
            Additional arguments
        """
        super().__init__(**kwargs)
        self.join_strategy = join_strategy
        self.optimize_joins = optimize_joins
    
    def get_input_contract(self) -> Type[FilteredDataContract]:
        return FilteredDataContract
    
    def get_output_contract(self) -> Type[JoinedDataContract]:
        return JoinedDataContract
    
    def join_tree_condition(
        self, 
        tree_data: LazyFrameWrapper, 
        condition_data: LazyFrameWrapper
    ) -> LazyFrameWrapper:
        """
        Join tree and condition data.
        
        Parameters
        ----------
        tree_data : LazyFrameWrapper
            Tree data
        condition_data : LazyFrameWrapper
            Condition data
            
        Returns
        -------
        LazyFrameWrapper
            Joined data
        """
        try:
            joined_frame = tree_data.frame.join(
                condition_data.frame,
                on=["PLT_CN", "CONDID"],
                how="inner"
            )
            return LazyFrameWrapper(joined_frame)
        except Exception as e:
            raise PipelineException(
                f"Failed to join tree and condition data: {e}",
                step_id=self.step_id,
                cause=e
            )
    
    def join_with_plot(
        self, 
        data: LazyFrameWrapper, 
        plot_data: LazyFrameWrapper
    ) -> LazyFrameWrapper:
        """
        Join data with plot information.
        
        Parameters
        ----------
        data : LazyFrameWrapper
            Primary data
        plot_data : LazyFrameWrapper
            Plot data
            
        Returns
        -------
        LazyFrameWrapper
            Joined data
        """
        try:
            joined_frame = data.frame.join(
                plot_data.frame,
                on="PLT_CN",
                how="inner"
            )
            return LazyFrameWrapper(joined_frame)
        except Exception as e:
            raise PipelineException(
                f"Failed to join with plot data: {e}",
                step_id=self.step_id,
                cause=e
            )


class CalculationStep(BaseEstimationStep[JoinedDataContract, ValuedDataContract]):
    """
    Base class for steps that perform value calculations.
    
    Provides common functionality for calculating tree and condition level
    values like volume, biomass, TPA, etc.
    """
    
    def __init__(
        self,
        calculation_type: str,
        value_columns: List[str],
        **kwargs
    ):
        """
        Initialize calculation step.
        
        Parameters
        ----------
        calculation_type : str
            Type of calculation being performed
        value_columns : List[str]
            Names of columns that will contain calculated values
        **kwargs
            Additional arguments
        """
        super().__init__(**kwargs)
        self.calculation_type = calculation_type
        self.value_columns = value_columns
    
    def get_input_contract(self) -> Type[JoinedDataContract]:
        return JoinedDataContract
    
    def get_output_contract(self) -> Type[ValuedDataContract]:
        return ValuedDataContract
    
    def validate_calculation_inputs(self, data: LazyFrameWrapper) -> None:
        """
        Validate that required columns for calculation are present.
        
        Parameters
        ----------
        data : LazyFrameWrapper
            Data to validate
            
        Raises
        ------
        StepValidationError
            If required columns are missing
        """
        required_columns = self.get_required_calculation_columns()
        
        # Get available columns
        if isinstance(data.frame, pl.LazyFrame):
            available_columns = set(data.frame.collect_schema().names())
        else:
            available_columns = set(data.frame.columns)
        
        missing_columns = required_columns - available_columns
        if missing_columns:
            raise StepValidationError(
                f"Missing required columns for {self.calculation_type} calculation: {missing_columns}",
                step_id=self.step_id
            )
    
    @abstractmethod
    def get_required_calculation_columns(self) -> Set[str]:
        """Get the columns required for this type of calculation."""
        pass
    
    @abstractmethod
    def perform_calculations(self, data: LazyFrameWrapper) -> LazyFrameWrapper:
        """Perform the actual calculations."""
        pass


class AggregationStep(BaseEstimationStep):
    """
    Base class for steps that aggregate data.
    
    Provides common functionality for aggregating tree-level or condition-level
    data to plot level or higher levels.
    """
    
    def __init__(
        self,
        aggregation_method: str = "sum",
        group_columns: Optional[List[str]] = None,
        **kwargs
    ):
        """
        Initialize aggregation step.
        
        Parameters
        ----------
        aggregation_method : str
            Method for aggregation (sum, mean, etc.)
        group_columns : Optional[List[str]]
            Columns to group by during aggregation
        **kwargs
            Additional arguments
        """
        super().__init__(**kwargs)
        self.aggregation_method = aggregation_method
        self.group_columns = group_columns or []
    
    def aggregate_to_plot_level(
        self, 
        data: LazyFrameWrapper,
        value_columns: List[str]
    ) -> LazyFrameWrapper:
        """
        Aggregate data to plot level.
        
        Parameters
        ----------
        data : LazyFrameWrapper
            Data to aggregate
        value_columns : List[str]
            Columns containing values to aggregate
            
        Returns
        -------
        LazyFrameWrapper
            Aggregated data
        """
        try:
            # Standard grouping columns for plot-level aggregation
            group_cols = ["PLT_CN"] + self.group_columns
            
            # Build aggregation expressions
            if self.aggregation_method == "sum":
                agg_exprs = [pl.col(col).sum().alias(col) for col in value_columns]
            elif self.aggregation_method == "mean":
                agg_exprs = [pl.col(col).mean().alias(col) for col in value_columns]
            else:
                raise ValueError(f"Unsupported aggregation method: {self.aggregation_method}")
            
            # Perform aggregation
            aggregated_frame = data.frame.group_by(group_cols).agg(agg_exprs)
            return LazyFrameWrapper(aggregated_frame)
            
        except Exception as e:
            raise PipelineException(
                f"Failed to aggregate data to plot level: {e}",
                step_id=self.step_id,
                cause=e
            )


class FormattingStep(BaseEstimationStep):
    """
    Base class for steps that format output data.
    
    Provides common functionality for formatting data for user consumption,
    including column renaming, unit conversion, and metadata addition.
    """
    
    def __init__(
        self,
        column_mappings: Optional[Dict[str, str]] = None,
        include_metadata: bool = True,
        **kwargs
    ):
        """
        Initialize formatting step.
        
        Parameters
        ----------
        column_mappings : Optional[Dict[str, str]]
            Mapping of internal column names to output names
        include_metadata : bool
            Whether to include analysis metadata in output
        **kwargs
            Additional arguments
        """
        super().__init__(**kwargs)
        self.column_mappings = column_mappings or {}
        self.include_metadata = include_metadata
    
    def apply_column_mappings(self, data: LazyFrameWrapper) -> LazyFrameWrapper:
        """
        Apply column name mappings to data.
        
        Parameters
        ----------
        data : LazyFrameWrapper
            Data to rename columns in
            
        Returns
        -------
        LazyFrameWrapper
            Data with renamed columns
        """
        if not self.column_mappings:
            return data
        
        try:
            renamed_frame = data.frame.rename(self.column_mappings)
            return LazyFrameWrapper(renamed_frame)
        except Exception as e:
            raise PipelineException(
                f"Failed to apply column mappings: {e}",
                step_id=self.step_id,
                cause=e
            )
    
    def collect_final_dataframe(self, data: LazyFrameWrapper) -> pl.DataFrame:
        """
        Collect lazy frame to concrete DataFrame.
        
        Parameters
        ----------
        data : LazyFrameWrapper
            Lazy data to collect
            
        Returns
        -------
        pl.DataFrame
            Concrete DataFrame
        """
        try:
            if isinstance(data.frame, pl.LazyFrame):
                return data.frame.collect()
            else:
                return data.frame
        except Exception as e:
            raise PipelineException(
                f"Failed to collect final DataFrame: {e}",
                step_id=self.step_id,
                cause=e
            )


# Export all base step classes
__all__ = [
    "BaseEstimationStep",
    "DataLoadingStep",
    "FilteringStep", 
    "JoiningStep",
    "CalculationStep",
    "AggregationStep",
    "FormattingStep",
]