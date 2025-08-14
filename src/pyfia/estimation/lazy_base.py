"""
Lazy base estimator for pyFIA with frame-agnostic operations.

This module provides LazyBaseEstimator that extends both BaseEstimator and
EnhancedBaseEstimator with lazy evaluation capabilities. All data loading
and filtering methods are made lazy-aware to support deferred execution.
"""

from typing import Any, Dict, List, Optional, Tuple, Union
import warnings

import polars as pl

from ..core import FIA
from .base import BaseEstimator, EnhancedBaseEstimator, EstimatorConfig
from .lazy_evaluation import (
    LazyEstimatorMixin,
    LazyFrameWrapper,
    lazy_operation,
    CollectionStrategy,
    LazyComputationNode,
    ComputationStatus
)


class LazyBaseEstimator(LazyEstimatorMixin, EnhancedBaseEstimator):
    """
    Base estimator with lazy evaluation support.
    
    This class extends EnhancedBaseEstimator with lazy evaluation capabilities,
    providing frame-agnostic operations that work seamlessly with both
    DataFrame and LazyFrame. It maintains backward compatibility while
    offering significant performance improvements for large datasets.
    
    Key features:
    - Automatic lazy/eager mode switching based on data size
    - Frame-agnostic operations using LazyFrameWrapper
    - Intelligent collection strategies
    - Integration with caching system
    - Progress tracking for long operations
    """
    
    def __init__(self, db: Union[str, FIA], config: EstimatorConfig):
        """
        Initialize lazy base estimator.
        
        Parameters
        ----------
        db : Union[str, FIA]
            FIA database object or path
        config : EstimatorConfig
            Estimator configuration
        """
        # Initialize parent classes
        super().__init__(db, config)
        
        # Lazy evaluation settings
        self._auto_lazy_threshold = getattr(config, 'lazy_threshold_rows', 10000)
        self._prefer_lazy = getattr(config, 'lazy_enabled', True)
        
        # Frame wrappers for current operation
        self._lazy_frames: Dict[str, LazyFrameWrapper] = {}
        
        # Collection points tracking
        self._collection_points: List[str] = []
        self._deferred_operations: int = 0
    
    # === Lazy Data Loading Methods ===
    
    @lazy_operation("load_table", cache_key_params=["table_name"])
    def load_table_lazy(self, table_name: str) -> Union[pl.DataFrame, pl.LazyFrame]:
        """
        Load a table with automatic lazy evaluation based on size.
        
        Parameters
        ----------
        table_name : str
            Name of the table to load
            
        Returns
        -------
        Union[pl.DataFrame, pl.LazyFrame]
            The loaded table, lazy if large
        """
        # Get table reference
        if table_name not in self.db.tables:
            self.db.load_table(table_name)
        
        table_ref = self.db.tables[table_name]
        
        # For lazy frames, return as-is
        if isinstance(table_ref, pl.LazyFrame):
            return table_ref
        
        # For DataFrames, check if we should convert to lazy
        if self._prefer_lazy and len(table_ref) > self._auto_lazy_threshold:
            return table_ref.lazy()
        
        return table_ref
    
    @lazy_operation("get_trees", cache_key_params=["filters"])
    def get_trees_lazy(self, filters: Optional[Dict[str, Any]] = None) -> LazyFrameWrapper:
        """
        Get tree data with lazy evaluation support.
        
        Parameters
        ----------
        filters : Optional[Dict[str, Any]]
            Additional filters to apply
            
        Returns
        -------
        LazyFrameWrapper
            Wrapped tree data
        """
        # Load tree table
        tree_data = self.load_table_lazy("TREE")
        
        # Apply EVALID filter if present
        if self.db.evalid:
            if isinstance(tree_data, pl.LazyFrame):
                tree_data = tree_data.filter(pl.col("EVALID").is_in(self.db.evalid))
            else:
                tree_data = tree_data.filter(pl.col("EVALID").is_in(self.db.evalid))
        
        # Apply additional filters
        if filters:
            for col, value in filters.items():
                if isinstance(tree_data, pl.LazyFrame):
                    tree_data = tree_data.filter(pl.col(col) == value)
                else:
                    tree_data = tree_data.filter(pl.col(col) == value)
        
        return LazyFrameWrapper(tree_data)
    
    @lazy_operation("get_conditions", cache_key_params=["filters"])
    def get_conditions_lazy(self, filters: Optional[Dict[str, Any]] = None) -> LazyFrameWrapper:
        """
        Get condition data with lazy evaluation support.
        
        Parameters
        ----------
        filters : Optional[Dict[str, Any]]
            Additional filters to apply
            
        Returns
        -------
        LazyFrameWrapper
            Wrapped condition data
        """
        # Load condition table
        cond_data = self.load_table_lazy("COND")
        
        # Apply EVALID filter if present
        if self.db.evalid:
            if isinstance(cond_data, pl.LazyFrame):
                cond_data = cond_data.filter(pl.col("EVALID").is_in(self.db.evalid))
            else:
                cond_data = cond_data.filter(pl.col("EVALID").is_in(self.db.evalid))
        
        # Apply additional filters
        if filters:
            for col, value in filters.items():
                if isinstance(cond_data, pl.LazyFrame):
                    cond_data = cond_data.filter(pl.col(col) == value)
                else:
                    cond_data = cond_data.filter(pl.col(col) == value)
        
        return LazyFrameWrapper(cond_data)
    
    # === Frame-Agnostic Filtering Methods ===
    
    def apply_filters_lazy(self, 
                          frame: LazyFrameWrapper,
                          filter_expr: Optional[str] = None,
                          filter_dict: Optional[Dict[str, Any]] = None) -> LazyFrameWrapper:
        """
        Apply filters to a wrapped frame in a frame-agnostic way.
        
        Parameters
        ----------
        frame : LazyFrameWrapper
            The wrapped frame to filter
        filter_expr : Optional[str]
            SQL-like filter expression
        filter_dict : Optional[Dict[str, Any]]
            Dictionary of column-value filters
            
        Returns
        -------
        LazyFrameWrapper
            Filtered frame wrapper
        """
        result = frame.frame
        
        # Apply expression filter
        if filter_expr:
            expr = pl.Expr.from_string(filter_expr)
            result = result.filter(expr)
        
        # Apply dictionary filters
        if filter_dict:
            for col, value in filter_dict.items():
                if isinstance(value, list):
                    result = result.filter(pl.col(col).is_in(value))
                else:
                    result = result.filter(pl.col(col) == value)
        
        return LazyFrameWrapper(result)
    
    def join_frames_lazy(self,
                        left: LazyFrameWrapper,
                        right: LazyFrameWrapper,
                        on: Union[str, List[str]],
                        how: str = "inner",
                        suffix: str = "_right") -> LazyFrameWrapper:
        """
        Join two wrapped frames in a frame-agnostic way.
        
        Parameters
        ----------
        left : LazyFrameWrapper
            Left frame to join
        right : LazyFrameWrapper
            Right frame to join
        on : Union[str, List[str]]
            Column(s) to join on
        how : str
            Join type
        suffix : str
            Suffix for overlapping columns
            
        Returns
        -------
        LazyFrameWrapper
            Joined frame wrapper
        """
        # Ensure both frames have the same type for join
        if left.is_lazy and not right.is_lazy:
            right_frame = right.to_lazy()
        elif not left.is_lazy and right.is_lazy:
            # Collect the smaller frame
            if len(left.frame) < len(right.frame):
                left_frame = left.to_lazy()
                right_frame = right.frame
            else:
                left_frame = left.frame
                right_frame = right.collect()
        else:
            left_frame = left.frame
            right_frame = right.frame
        
        # Perform join
        result = left_frame.join(right_frame, on=on, how=how, suffix=suffix)
        
        return LazyFrameWrapper(result)
    
    # === Lazy Aggregation Methods ===
    
    @lazy_operation("aggregate", cache_key_params=["group_cols", "agg_exprs"])
    def aggregate_lazy(self,
                      frame: LazyFrameWrapper,
                      group_cols: Optional[List[str]] = None,
                      agg_exprs: Optional[List[pl.Expr]] = None) -> LazyFrameWrapper:
        """
        Perform aggregation on a wrapped frame.
        
        Parameters
        ----------
        frame : LazyFrameWrapper
            Frame to aggregate
        group_cols : Optional[List[str]]
            Columns to group by
        agg_exprs : Optional[List[pl.Expr]]
            Aggregation expressions
            
        Returns
        -------
        LazyFrameWrapper
            Aggregated frame wrapper
        """
        if not agg_exprs:
            return frame
        
        if group_cols:
            result = frame.frame.group_by(group_cols).agg(agg_exprs)
        else:
            result = frame.frame.select(agg_exprs)
        
        return LazyFrameWrapper(result)
    
    # === Override Base Methods with Lazy Support ===
    
    def _get_filtered_data(self) -> Tuple[Optional[LazyFrameWrapper], LazyFrameWrapper]:
        """
        Get data from database and apply common filters with lazy support.
        
        Returns
        -------
        Tuple[Optional[LazyFrameWrapper], LazyFrameWrapper]
            Filtered tree and condition frame wrappers
        """
        # Always get condition data
        cond_wrapper = self.get_conditions_lazy()
        
        # Apply common area filters
        from ..filters.common import apply_area_filters_common
        
        # Convert to eager for filter application if needed
        cond_df = cond_wrapper.collect()
        cond_df = apply_area_filters_common(
            cond_df,
            self.config.land_type,
            self.config.area_domain
        )
        
        # Convert back to lazy if beneficial
        if self._prefer_lazy and len(cond_df) > self._auto_lazy_threshold:
            cond_wrapper = LazyFrameWrapper(cond_df.lazy())
        else:
            cond_wrapper = LazyFrameWrapper(cond_df)
        
        # Get tree data if needed
        tree_wrapper = None
        if "TREE" in self.get_required_tables():
            tree_wrapper = self.get_trees_lazy()
            
            # Apply common tree filters
            from ..filters.common import apply_tree_filters_common
            
            # Convert to eager for filter application
            tree_df = tree_wrapper.collect()
            tree_df = apply_tree_filters_common(
                tree_df,
                self.config.tree_type,
                self.config.tree_domain,
                require_volume=False
            )
            
            # Convert back to lazy if beneficial
            if self._prefer_lazy and len(tree_df) > self._auto_lazy_threshold:
                tree_wrapper = LazyFrameWrapper(tree_df.lazy())
            else:
                tree_wrapper = LazyFrameWrapper(tree_df)
        
        # Apply module-specific filters
        if tree_wrapper:
            tree_df = tree_wrapper.collect() if tree_wrapper.is_lazy else tree_wrapper.frame
            cond_df = cond_wrapper.collect() if cond_wrapper.is_lazy else cond_wrapper.frame
            
            tree_df, cond_df = self.apply_module_filters(tree_df, cond_df)
            
            # Wrap results
            tree_wrapper = LazyFrameWrapper(tree_df.lazy() if self._prefer_lazy and len(tree_df) > self._auto_lazy_threshold else tree_df)
            cond_wrapper = LazyFrameWrapper(cond_df.lazy() if self._prefer_lazy and len(cond_df) > self._auto_lazy_threshold else cond_df)
        
        return tree_wrapper, cond_wrapper
    
    def _prepare_estimation_data(self, 
                                tree_wrapper: Optional[LazyFrameWrapper],
                                cond_wrapper: LazyFrameWrapper) -> LazyFrameWrapper:
        """
        Join data and prepare for estimation with lazy support.
        
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
            # Select needed columns from conditions
            cond_cols = ["PLT_CN", "CONDID", "CONDPROP_UNADJ"]
            cond_subset = cond_wrapper.apply_operation(
                lambda df: df.select(cond_cols)
            )
            
            # Join trees with conditions
            data_wrapper = self.join_frames_lazy(
                tree_wrapper,
                cond_subset,
                on=["PLT_CN", "CONDID"],
                how="inner"
            )
            
            # Set up grouping columns
            from ..filters.common import setup_grouping_columns_common
            
            # Need to collect for grouping setup
            data_df = data_wrapper.collect()
            data_df, group_cols = setup_grouping_columns_common(
                data_df,
                self.config.grp_by,
                self.config.by_species,
                self.config.by_size_class,
                return_dataframe=True
            )
            self._group_cols = group_cols
            
            # Convert back to lazy if beneficial
            if self._prefer_lazy and len(data_df) > self._auto_lazy_threshold:
                data_wrapper = LazyFrameWrapper(data_df.lazy())
            else:
                data_wrapper = LazyFrameWrapper(data_df)
        else:
            # Area estimation case
            data_wrapper = cond_wrapper
            self._group_cols = []
            
            # Handle custom grouping columns
            if self.config.grp_by:
                if isinstance(self.config.grp_by, str):
                    self._group_cols = [self.config.grp_by]
                else:
                    self._group_cols = list(self.config.grp_by)
        
        return data_wrapper
    
    def _calculate_plot_estimates_lazy(self, data_wrapper: LazyFrameWrapper) -> LazyFrameWrapper:
        """
        Calculate plot-level estimates with lazy support.
        
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
        plot_estimates = self.aggregate_lazy(data_wrapper, plot_groups, agg_exprs)
        
        return plot_estimates
    
    def estimate(self) -> pl.DataFrame:
        """
        Main estimation workflow with lazy evaluation support.
        
        This method overrides the base estimate() to use lazy evaluation
        throughout the workflow, collecting only at the final step.
        
        Returns
        -------
        pl.DataFrame
            Final estimation results
        """
        # Track start of estimation
        self._deferred_operations = 0
        self._collection_points.clear()
        
        # Step 1: Load required tables (lazy)
        self._load_required_tables()
        
        # Step 2: Get and filter data (lazy)
        tree_wrapper, cond_wrapper = self._get_filtered_data()
        self._deferred_operations += 2
        
        # Step 3: Join and prepare data (lazy)
        prepared_wrapper = self._prepare_estimation_data(tree_wrapper, cond_wrapper)
        self._deferred_operations += 1
        
        # Step 4: Calculate module-specific values
        # This may require collection depending on the calculation
        if prepared_wrapper.is_lazy:
            # Some calculations may need eager mode
            prepared_df = prepared_wrapper.collect()
            self._collection_points.append("calculate_values")
        else:
            prepared_df = prepared_wrapper.frame
        
        valued_data = self.calculate_values(prepared_df)
        
        # Convert back to lazy for remaining operations
        if self._prefer_lazy and len(valued_data) > self._auto_lazy_threshold:
            valued_wrapper = LazyFrameWrapper(valued_data.lazy())
        else:
            valued_wrapper = LazyFrameWrapper(valued_data)
        
        # Step 5: Calculate plot-level estimates (lazy)
        plot_wrapper = self._calculate_plot_estimates_lazy(valued_wrapper)
        self._deferred_operations += 1
        
        # Step 6: Apply stratification and expansion
        # This typically requires collection for joins
        plot_df = plot_wrapper.collect()
        self._collection_points.append("stratification")
        
        expanded_estimates = self._apply_stratification(plot_df)
        
        # Step 7: Calculate population estimates
        pop_estimates = self._calculate_population_estimates(expanded_estimates)
        
        # Step 8: Format and return results
        result = self.format_output(pop_estimates)
        
        # Log lazy evaluation statistics
        if self._lazy_enabled:
            stats = self.get_lazy_statistics()
            if stats["operations_deferred"] > 0:
                warnings.warn(
                    f"Lazy evaluation completed: "
                    f"{stats['operations_deferred']} operations deferred, "
                    f"{len(self._collection_points)} collection points, "
                    f"{stats['cache_hits']} cache hits",
                    category=UserWarning
                )
        
        return result
    
    # === Utility Methods for Lazy Operations ===
    
    def optimize_computation_graph(self):
        """
        Optimize the computation graph for better performance.
        
        This method analyzes the current computation graph and optimizes
        the execution order to minimize data movement and maximize reuse.
        """
        if not self._computation_graph:
            return
        
        # Build dependency graph
        dependencies = {}
        for node_id, node in self._computation_graph.items():
            dependencies[node_id] = set(node.dependencies)
        
        # Topological sort for optimal execution order
        visited = set()
        temp_visited = set()
        optimized_order = []
        
        def visit(node_id: str):
            if node_id in temp_visited:
                raise ValueError(f"Circular dependency detected at node {node_id}")
            if node_id in visited:
                return
            
            temp_visited.add(node_id)
            for dep in dependencies.get(node_id, set()):
                visit(dep)
            temp_visited.remove(node_id)
            visited.add(node_id)
            optimized_order.append(node_id)
        
        # Visit all nodes
        for node_id in self._computation_graph:
            if node_id not in visited:
                visit(node_id)
        
        self._execution_order = optimized_order
    
    def collect_all_pending(self) -> Dict[str, Any]:
        """
        Collect all pending lazy operations in the computation graph.
        
        Returns
        -------
        Dict[str, Any]
            Results of all collected operations
        """
        results = {}
        
        # Optimize execution order first
        self.optimize_computation_graph()
        
        # Collect lazy frames
        lazy_frames = []
        node_mapping = {}
        
        for node_id in self._execution_order:
            node = self._computation_graph.get(node_id)
            if node and node.status == ComputationStatus.PENDING:
                if isinstance(node.result, pl.LazyFrame):
                    lazy_frames.append(node.result)
                    node_mapping[len(lazy_frames) - 1] = node_id
        
        if lazy_frames:
            # Collect all at once
            collected = self.collect_lazy_frames(lazy_frames)
            
            # Map results back to nodes
            for idx, result in enumerate(collected):
                node_id = node_mapping[idx]
                results[node_id] = result
                self._computation_graph[node_id].mark_completed(result)
        
        return results
    
    def get_execution_plan(self) -> str:
        """
        Get a string representation of the current execution plan.
        
        Returns
        -------
        str
            Execution plan description
        """
        plan_lines = ["Lazy Execution Plan:"]
        plan_lines.append(f"Total nodes: {len(self._computation_graph)}")
        plan_lines.append(f"Deferred operations: {self._deferred_operations}")
        plan_lines.append(f"Collection points: {len(self._collection_points)}")
        
        if self._collection_points:
            plan_lines.append("\nCollection Points:")
            for i, point in enumerate(self._collection_points, 1):
                plan_lines.append(f"  {i}. {point}")
        
        if self._computation_graph:
            plan_lines.append("\nComputation Graph:")
            for node_id, node in self._computation_graph.items():
                status_str = node.status.name
                plan_lines.append(
                    f"  {node.operation} ({node_id[:8]}...) - {status_str}"
                )
        
        return "\n".join(plan_lines)