"""
Lazy evaluation support for pyFIA estimators.

This module provides core lazy evaluation functionality including computation
nodes, lazy operation decorators, and collection strategies. It enables
deferred computation for improved performance with large datasets.
"""

from dataclasses import dataclass, field
from enum import Enum, auto
from functools import wraps
from typing import Any, Callable, Dict, List, Optional, Set, Union
import hashlib
import json
from datetime import datetime

import polars as pl
from pydantic import BaseModel, Field

from ..core import FIA


class ComputationStatus(Enum):
    """Status of a lazy computation node."""
    PENDING = auto()
    COMPUTING = auto()
    COMPLETED = auto()
    FAILED = auto()
    CACHED = auto()


@dataclass
class LazyComputationNode:
    """
    Represents a node in the lazy computation graph.
    
    Each node tracks its operation, dependencies, and computation state.
    This enables deferred execution and optimization of the computation graph.
    """
    
    node_id: str
    operation: str
    params: Dict[str, Any]
    dependencies: List[str] = field(default_factory=list)
    status: ComputationStatus = ComputationStatus.PENDING
    result: Optional[Union[pl.DataFrame, pl.LazyFrame]] = None
    error: Optional[Exception] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.now)
    completed_at: Optional[datetime] = None
    
    def __post_init__(self):
        """Initialize node ID if not provided."""
        if not self.node_id:
            # Generate ID from operation and params
            # Handle non-serializable objects (like Polars expressions)
            serializable_params = {}
            for key, value in self.params.items():
                try:
                    # Try to serialize the value
                    json.dumps(value)
                    serializable_params[key] = value
                except (TypeError, ValueError):
                    # If it fails, convert to string representation
                    serializable_params[key] = str(value)
            
            content = f"{self.operation}:{json.dumps(serializable_params, sort_keys=True)}"
            self.node_id = hashlib.md5(content.encode()).hexdigest()[:12]
    
    @property
    def execution_time(self) -> Optional[float]:
        """Get execution time in seconds if completed."""
        if self.completed_at and self.created_at:
            return (self.completed_at - self.created_at).total_seconds()
        return None
    
    def mark_completed(self, result: Union[pl.DataFrame, pl.LazyFrame]):
        """Mark node as completed with result."""
        self.status = ComputationStatus.COMPLETED
        self.result = result
        self.completed_at = datetime.now()
    
    def mark_failed(self, error: Exception):
        """Mark node as failed with error."""
        self.status = ComputationStatus.FAILED
        self.error = error
        self.completed_at = datetime.now()
    
    def mark_cached(self, result: Union[pl.DataFrame, pl.LazyFrame]):
        """Mark node as retrieved from cache."""
        self.status = ComputationStatus.CACHED
        self.result = result
        self.completed_at = datetime.now()


class CollectionStrategy(Enum):
    """Strategy for collecting lazy frames."""
    SEQUENTIAL = auto()      # Collect one by one
    PARALLEL = auto()        # Collect in parallel
    STREAMING = auto()       # Use streaming engine
    ADAPTIVE = auto()        # Choose based on query plan


class LazyEstimatorMixin:
    """
    Mixin class that adds lazy evaluation capabilities to estimators.
    
    This mixin provides:
    - Lazy operation decorator for automatic deferred execution
    - Computation graph management
    - Smart collection strategies
    - Integration with caching system
    """
    
    def __init__(self, *args, **kwargs):
        """Initialize lazy evaluation attributes."""
        super().__init__(*args, **kwargs)
        
        # Computation graph
        self._computation_graph: Dict[str, LazyComputationNode] = {}
        self._execution_order: List[str] = []
        
        # Configuration
        self._lazy_enabled: bool = True
        self._collection_strategy: CollectionStrategy = CollectionStrategy.ADAPTIVE
        self._max_parallel_collections: int = 4
        
        # Statistics
        self._lazy_stats: Dict[str, Any] = {
            "operations_deferred": 0,
            "operations_collected": 0,
            "cache_hits": 0,
            "total_execution_time": 0.0
        }
    
    @property
    def is_lazy_enabled(self) -> bool:
        """Check if lazy evaluation is enabled."""
        return self._lazy_enabled
    
    def enable_lazy_evaluation(self):
        """Enable lazy evaluation mode."""
        self._lazy_enabled = True
    
    def disable_lazy_evaluation(self):
        """Disable lazy evaluation mode (eager execution)."""
        self._lazy_enabled = False
    
    def set_collection_strategy(self, strategy: CollectionStrategy):
        """Set the collection strategy for lazy frames."""
        self._collection_strategy = strategy
    
    def get_computation_graph(self) -> Dict[str, LazyComputationNode]:
        """Get the current computation graph."""
        return self._computation_graph.copy()
    
    def get_lazy_statistics(self) -> Dict[str, Any]:
        """Get statistics about lazy operations."""
        return self._lazy_stats.copy()
    
    def clear_computation_graph(self):
        """Clear the computation graph and statistics."""
        self._computation_graph.clear()
        self._execution_order.clear()
        self._lazy_stats = {
            "operations_deferred": 0,
            "operations_collected": 0,
            "cache_hits": 0,
            "total_execution_time": 0.0
        }
    
    def add_computation_node(self, node: LazyComputationNode) -> str:
        """
        Add a computation node to the graph.
        
        Parameters
        ----------
        node : LazyComputationNode
            The node to add
            
        Returns
        -------
        str
            The node ID
        """
        self._computation_graph[node.node_id] = node
        self._execution_order.append(node.node_id)
        self._lazy_stats["operations_deferred"] += 1
        return node.node_id
    
    def _determine_collection_strategy(self, lazy_frames: List[pl.LazyFrame]) -> CollectionStrategy:
        """
        Determine optimal collection strategy based on query characteristics.
        
        Parameters
        ----------
        lazy_frames : List[pl.LazyFrame]
            List of lazy frames to collect
            
        Returns
        -------
        CollectionStrategy
            The recommended collection strategy
        """
        if self._collection_strategy != CollectionStrategy.ADAPTIVE:
            return self._collection_strategy
        
        # Analyze query complexity
        total_operations = sum(
            len(str(lf.explain(optimized=True)).split('\n'))
            for lf in lazy_frames
        )
        
        # Simple heuristics for strategy selection
        if len(lazy_frames) == 1:
            return CollectionStrategy.SEQUENTIAL
        elif total_operations > 1000:
            return CollectionStrategy.STREAMING
        elif len(lazy_frames) <= self._max_parallel_collections:
            return CollectionStrategy.PARALLEL
        else:
            return CollectionStrategy.SEQUENTIAL
    
    def collect_lazy_frames(self, 
                          lazy_frames: Union[pl.LazyFrame, List[pl.LazyFrame]],
                          strategy: Optional[CollectionStrategy] = None) -> Union[pl.DataFrame, List[pl.DataFrame]]:
        """
        Collect lazy frames using the specified or adaptive strategy.
        
        Parameters
        ----------
        lazy_frames : Union[pl.LazyFrame, List[pl.LazyFrame]]
            Lazy frame(s) to collect
        strategy : Optional[CollectionStrategy]
            Override collection strategy
            
        Returns
        -------
        Union[pl.DataFrame, List[pl.DataFrame]]
            Collected dataframe(s)
        """
        # Handle single frame
        if isinstance(lazy_frames, pl.LazyFrame):
            return self._collect_single_frame(lazy_frames, strategy)
        
        # Determine strategy
        if strategy is None:
            strategy = self._determine_collection_strategy(lazy_frames)
        
        # Collect based on strategy
        if strategy == CollectionStrategy.SEQUENTIAL:
            return [self._collect_single_frame(lf) for lf in lazy_frames]
        
        elif strategy == CollectionStrategy.PARALLEL:
            # Use polars' collect_all for parallel collection
            return pl.collect_all(lazy_frames)
        
        elif strategy == CollectionStrategy.STREAMING:
            # Use streaming engine for large queries
            return [
                self._collect_single_frame(lf, streaming=True)
                for lf in lazy_frames
            ]
        
        else:  # ADAPTIVE already handled above
            return [self._collect_single_frame(lf) for lf in lazy_frames]
    
    def _collect_single_frame(self, 
                            lazy_frame: pl.LazyFrame,
                            strategy: Optional[CollectionStrategy] = None,
                            streaming: bool = False) -> pl.DataFrame:
        """
        Collect a single lazy frame with optional streaming.
        
        Parameters
        ----------
        lazy_frame : pl.LazyFrame
            Frame to collect
        strategy : Optional[CollectionStrategy]
            Collection strategy override
        streaming : bool
            Whether to use streaming engine
            
        Returns
        -------
        pl.DataFrame
            Collected dataframe
        """
        self._lazy_stats["operations_collected"] += 1
        
        if streaming:
            # Use streaming engine for memory efficiency
            return lazy_frame.collect(streaming=True)
        else:
            return lazy_frame.collect()
    
    @staticmethod
    def operation(operation_name: str, 
                      cache_key_params: Optional[List[str]] = None,
                      supports_lazy: bool = True):
        """
        Decorator for lazy operations that automatically handles deferred execution.
        
        Parameters
        ----------
        operation_name : str
            Name of the operation for tracking
        cache_key_params : Optional[List[str]]
            Parameter names to include in cache key
        supports_lazy : bool
            Whether this operation supports lazy evaluation
            
        Returns
        -------
        Callable
            Decorated function
        """
        def decorator(func: Callable) -> Callable:
            @wraps(func)
            def wrapper(self, *args, **kwargs):
                # Check if lazy evaluation is enabled and supported
                if not (hasattr(self, '_lazy_enabled') and 
                       self._lazy_enabled and 
                       supports_lazy):
                    # Eager execution
                    return func(self, *args, **kwargs)
                
                # Build operation parameters
                import inspect
                sig = inspect.signature(func)
                bound_args = sig.bind(self, *args, **kwargs)
                bound_args.apply_defaults()
                
                # Extract parameters for cache key
                params = {}
                if cache_key_params:
                    for param in cache_key_params:
                        if param in bound_args.arguments:
                            params[param] = bound_args.arguments[param]
                
                # Create computation node
                node = LazyComputationNode(
                    node_id="",  # Will be auto-generated
                    operation=operation_name,
                    params=params,
                    metadata={
                        "function": func.__name__,
                        "module": func.__module__
                    }
                )
                
                # Check cache if available
                if hasattr(self, '_check_cache'):
                    cached_result = self._check_cache(node)
                    if cached_result is not None:
                        node.mark_cached(cached_result)
                        self._lazy_stats["cache_hits"] += 1
                        return cached_result
                
                # Add to computation graph
                node_id = self.add_computation_node(node)
                
                # Execute the operation
                try:
                    node.status = ComputationStatus.COMPUTING
                    result = func(self, *args, **kwargs)
                    
                    # Handle different result types
                    if isinstance(result, pl.LazyFrame):
                        # Keep as lazy frame for deferred execution
                        node.mark_completed(result)
                        return result
                    elif isinstance(result, pl.DataFrame):
                        # Convert to lazy if beneficial
                        if len(result) > 10000:  # Threshold for lazy conversion
                            lazy_result = result.lazy()
                            node.mark_completed(lazy_result)
                            return lazy_result
                        else:
                            node.mark_completed(result)
                            return result
                    else:
                        # Non-dataframe result
                        node.mark_completed(result)
                        return result
                        
                except Exception as e:
                    node.mark_failed(e)
                    raise
                finally:
                    # Update statistics
                    if node.execution_time:
                        self._lazy_stats["total_execution_time"] += node.execution_time
            
            return wrapper
        return decorator


class FrameWrapper:
    """
    Wrapper for frame-agnostic operations that work with both DataFrame and LazyFrame.
    
    This class provides a unified interface for operations that can be applied
    to either eager or lazy frames, automatically handling conversions and
    maintaining the appropriate frame type.
    """
    
    def __init__(self, frame: Union[pl.DataFrame, pl.LazyFrame]):
        """
        Initialize wrapper with a frame.
        
        Parameters
        ----------
        frame : Union[pl.DataFrame, pl.LazyFrame]
            The frame to wrap
        """
        self._frame = frame
        self._is_lazy = isinstance(frame, pl.LazyFrame)
    
    @property
    def frame(self) -> Union[pl.DataFrame, pl.LazyFrame]:
        """Get the underlying frame."""
        return self._frame
    
    @property
    def is_lazy(self) -> bool:
        """Check if the frame is lazy."""
        return self._is_lazy
    
    def to_lazy(self) -> pl.LazyFrame:
        """Convert to lazy frame if not already."""
        if self._is_lazy:
            return self._frame
        else:
            return self._frame.lazy()
    
    def collect(self, **kwargs) -> pl.DataFrame:
        """Collect to eager dataframe if lazy."""
        if self._is_lazy:
            return self._frame.collect(**kwargs)
        else:
            return self._frame
    
    def apply_operation(self, 
                       operation: Callable,
                       *args,
                       maintain_type: bool = True,
                       **kwargs) -> 'FrameWrapper':
        """
        Apply an operation to the frame while maintaining type consistency.
        
        Parameters
        ----------
        operation : Callable
            Operation to apply to the frame
        *args
            Positional arguments for operation
        maintain_type : bool
            Whether to maintain the original frame type
        **kwargs
            Keyword arguments for operation
            
        Returns
        -------
        FrameWrapper
            New wrapper with the result
        """
        result = operation(self._frame, *args, **kwargs)
        
        # Maintain original type if requested
        if maintain_type and not self._is_lazy and isinstance(result, pl.LazyFrame):
            result = result.collect()
        elif maintain_type and self._is_lazy and isinstance(result, pl.DataFrame):
            result = result.lazy()
        
        return FrameWrapper(result)
    
    def __getattr__(self, name: str) -> Any:
        """Delegate attribute access to the underlying frame."""
        return getattr(self._frame, name)


# Export the decorator for external use
operation = LazyEstimatorMixin.operation


class LazyConfigMixin(BaseModel):
    """
    Configuration mixin for lazy evaluation settings.
    
    Add this to estimator configurations to provide lazy evaluation options.
    """
    
    # Lazy evaluation settings
    lazy_enabled: bool = Field(
        default=True,
        description="Enable lazy evaluation for improved performance"
    )
    
    collection_strategy: CollectionStrategy = Field(
        default=CollectionStrategy.ADAPTIVE,
        description="Strategy for collecting lazy frames"
    )
    
    max_parallel_collections: int = Field(
        default=4,
        ge=1,
        le=16,
        description="Maximum number of parallel collections"
    )
    
    cache_operations: bool = Field(
        default=True,
        description="Cache results of lazy operations"
    )
    
    lazy_threshold_rows: int = Field(
        default=10000,
        ge=1000,
        description="Row count threshold for automatic lazy conversion"
    )
    
    enable_query_optimization: bool = Field(
        default=True,
        description="Enable Polars query optimization for lazy frames"
    )