"""
Data joining steps for the pyFIA estimation pipeline.

This module provides pipeline steps for joining FIA tables together with
various optimization strategies. These steps handle the complex relationships
between FIA tables and ensure proper data linkage for estimation.

Steps:
- JoinPlotConditionStep: Join plot and condition data
- JoinTreePlotStep: Join tree and plot data
- JoinTreeConditionStep: Join tree and condition data
- JoinStratificationStep: Join with stratification tables
- OptimizedMultiJoinStep: Multi-table joins with cost optimization
"""

from typing import Dict, List, Optional, Type, Tuple
import warnings
import time

import polars as pl

from ...lazy_evaluation import LazyFrameWrapper
from ...join_optimizer import JoinOptimizer, JoinNode
from ..core import ExecutionContext, PipelineException
from ..contracts import FilteredDataContract, JoinedDataContract
from ..base_steps import JoiningStep


class JoinPlotConditionStep(JoiningStep):
    """
    Join plot and condition data for area estimation.
    
    This step joins PLOT and COND tables on PLT_CN, which is the fundamental
    join for area-based estimates. It handles the one-to-many relationship
    where plots can have multiple conditions.
    
    Examples
    --------
    >>> # Standard plot-condition join
    >>> step = JoinPlotConditionStep()
    >>> 
    >>> # Join with specific columns only
    >>> step = JoinPlotConditionStep(
    ...     plot_columns=["PLT_CN", "LAT", "LON", "INVYR"],
    ...     condition_columns=["PLT_CN", "CONDID", "COND_STATUS_CD", "CONDPROP_UNADJ"]
    ... )
    """
    
    def __init__(
        self,
        plot_columns: Optional[List[str]] = None,
        condition_columns: Optional[List[str]] = None,
        join_type: str = "inner",
        validate_join: bool = True,
        **kwargs
    ):
        """
        Initialize plot-condition joining step.
        
        Parameters
        ----------
        plot_columns : Optional[List[str]]
            Specific plot columns to keep (None = all)
        condition_columns : Optional[List[str]]
            Specific condition columns to keep (None = all)
        join_type : str
            Type of join ("inner", "left", "outer")
        validate_join : bool
            Whether to validate join cardinality
        **kwargs
            Additional arguments passed to base class
        """
        super().__init__(join_strategy="plot_condition", **kwargs)
        self.plot_columns = plot_columns
        self.condition_columns = condition_columns
        self.join_type = join_type
        self.validate_join = validate_join
    
    def execute_step(
        self, 
        input_data: FilteredDataContract, 
        context: ExecutionContext
    ) -> JoinedDataContract:
        """
        Execute plot-condition join.
        
        Parameters
        ----------
        input_data : FilteredDataContract
            Input contract with filtered data
        context : ExecutionContext
            Execution context
            
        Returns
        -------
        JoinedDataContract
            Contract with joined plot-condition data
        """
        try:
            # Validate input data
            if not input_data.plot_data:
                raise PipelineException(
                    "Plot data required for plot-condition join",
                    step_id=self.step_id
                )
            
            if not input_data.condition_data:
                raise PipelineException(
                    "Condition data required for plot-condition join",
                    step_id=self.step_id
                )
            
            start_time = time.time()
            
            # Select columns if specified
            plot_frame = input_data.plot_data.frame
            if self.plot_columns:
                plot_frame = plot_frame.select(self.plot_columns)
            
            condition_frame = input_data.condition_data.frame
            if self.condition_columns:
                condition_frame = condition_frame.select(self.condition_columns)
            
            # Perform join
            joined_frame = plot_frame.join(
                condition_frame,
                on="PLT_CN",
                how=self.join_type
            )
            
            # Validate join if requested
            if self.validate_join:
                self._validate_join_cardinality(plot_frame, condition_frame, joined_frame)
            
            join_time = time.time() - start_time
            
            # Create output contract
            output = JoinedDataContract(
                data=LazyFrameWrapper(joined_frame),
                join_strategy="plot_condition",
                tables_joined=["PLOT", "COND"],
                step_id=self.step_id
            )
            
            # Add performance metadata
            output.join_performance = {
                "join_time": join_time,
                "join_type": self.join_type,
                "plot_columns": len(self.plot_columns) if self.plot_columns else "all",
                "condition_columns": len(self.condition_columns) if self.condition_columns else "all"
            }
            
            # Track performance
            self.track_performance(
                context,
                join_time=join_time,
                join_type=self.join_type
            )
            
            return output
            
        except Exception as e:
            self.handle_step_error(e, context)
            raise PipelineException(
                f"Failed to join plot and condition data: {e}",
                step_id=self.step_id,
                cause=e
            )
    
    def _validate_join_cardinality(
        self, 
        plot_frame: pl.LazyFrame,
        condition_frame: pl.LazyFrame,
        joined_frame: pl.LazyFrame
    ) -> None:
        """Validate that join cardinality is as expected."""
        # Get counts
        plot_count = plot_frame.select(pl.count()).collect().item()
        condition_count = condition_frame.select(pl.count()).collect().item()
        joined_count = joined_frame.select(pl.count()).collect().item()
        
        # For inner join, result should be <= condition count
        if self.join_type == "inner" and joined_count > condition_count:
            warnings.warn(
                f"Unexpected join cardinality: {joined_count} rows from "
                f"{plot_count} plots and {condition_count} conditions",
                category=UserWarning
            )


class JoinTreePlotStep(JoiningStep):
    """
    Join tree and plot data - the most common expensive join in FIA.
    
    This step performs the tree-plot join which is often the most expensive
    operation in FIA estimation pipelines. It includes optimizations for
    this specific join pattern.
    
    Examples
    --------
    >>> # Standard tree-plot join
    >>> step = JoinTreePlotStep()
    >>> 
    >>> # Optimized join with column selection
    >>> step = JoinTreePlotStep(
    ...     optimize_for_aggregation=True,
    ...     include_spatial=False
    ... )
    """
    
    def __init__(
        self,
        optimize_for_aggregation: bool = True,
        include_spatial: bool = False,
        include_design_columns: bool = True,
        use_join_optimizer: bool = True,
        **kwargs
    ):
        """
        Initialize tree-plot joining step.
        
        Parameters
        ----------
        optimize_for_aggregation : bool
            Whether to optimize for subsequent aggregation operations
        include_spatial : bool
            Whether to include LAT/LON columns from plot
        include_design_columns : bool
            Whether to include survey design columns
        use_join_optimizer : bool
            Whether to use the join optimizer for planning
        **kwargs
            Additional arguments passed to base class
        """
        super().__init__(join_strategy="tree_plot", **kwargs)
        self.optimize_for_aggregation = optimize_for_aggregation
        self.include_spatial = include_spatial
        self.include_design_columns = include_design_columns
        self.use_join_optimizer = use_join_optimizer
    
    def execute_step(
        self, 
        input_data: FilteredDataContract, 
        context: ExecutionContext
    ) -> JoinedDataContract:
        """
        Execute tree-plot join with optimization.
        
        Parameters
        ----------
        input_data : FilteredDataContract
            Input contract with filtered data
        context : ExecutionContext
            Execution context
            
        Returns
        -------
        JoinedDataContract
            Contract with joined tree-plot data
        """
        try:
            # Validate input data
            if not input_data.tree_data:
                raise PipelineException(
                    "Tree data required for tree-plot join",
                    step_id=self.step_id
                )
            
            if not input_data.plot_data:
                raise PipelineException(
                    "Plot data required for tree-plot join",
                    step_id=self.step_id
                )
            
            start_time = time.time()
            
            # Determine plot columns to include
            plot_columns = self._determine_plot_columns()
            
            # Get frames
            tree_frame = input_data.tree_data.frame
            plot_frame = input_data.plot_data.frame
            
            # Select plot columns
            if plot_columns:
                plot_frame = plot_frame.select(plot_columns)
            
            # Use join optimizer if enabled
            if self.use_join_optimizer:
                joined_frame = self._optimized_join(tree_frame, plot_frame, context)
            else:
                # Standard join
                joined_frame = tree_frame.join(
                    plot_frame,
                    on="PLT_CN",
                    how="inner"
                )
            
            join_time = time.time() - start_time
            
            # Add condition data if available
            if input_data.condition_data:
                # Also join with condition data for complete dataset
                joined_frame = joined_frame.join(
                    input_data.condition_data.frame,
                    on=["PLT_CN", "CONDID"],
                    how="inner"
                )
            
            # Create output contract
            output = JoinedDataContract(
                data=LazyFrameWrapper(joined_frame),
                join_strategy="tree_plot_optimized" if self.use_join_optimizer else "tree_plot",
                tables_joined=["TREE", "PLOT"] + (["COND"] if input_data.condition_data else []),
                step_id=self.step_id
            )
            
            # Add performance metadata
            output.join_performance = {
                "join_time": join_time,
                "optimized": self.use_join_optimizer,
                "plot_columns_selected": len(plot_columns) if plot_columns else "all"
            }
            
            # Track performance
            self.track_performance(
                context,
                join_time=join_time,
                optimization_used=self.use_join_optimizer
            )
            
            return output
            
        except Exception as e:
            self.handle_step_error(e, context)
            raise PipelineException(
                f"Failed to join tree and plot data: {e}",
                step_id=self.step_id,
                cause=e
            )
    
    def _determine_plot_columns(self) -> List[str]:
        """Determine which plot columns to include based on settings."""
        columns = ["PLT_CN", "INVYR", "STATECD", "UNITCD", "COUNTYCD", "PLOT"]
        
        if self.include_spatial:
            columns.extend(["LAT", "LON", "ELEV"])
        
        if self.include_design_columns:
            columns.extend(["PLOT_STATUS_CD", "KINDCD", "DESIGNCD"])
        
        if self.optimize_for_aggregation:
            # Include columns commonly needed for aggregation
            columns.extend(["MACRO_BREAKPOINT_DIA", "MICROPLOT_LOC"])
        
        return columns
    
    def _optimized_join(
        self, 
        tree_frame: pl.LazyFrame,
        plot_frame: pl.LazyFrame,
        context: ExecutionContext
    ) -> pl.LazyFrame:
        """Perform optimized join using join optimizer."""
        # Create join optimizer
        optimizer = JoinOptimizer()
        
        # Create join nodes
        tree_node = JoinNode("TREE", estimated_rows=1000000)  # Estimate
        plot_node = JoinNode("PLOT", estimated_rows=10000)    # Estimate
        
        # Add to graph
        optimizer.add_table(tree_node)
        optimizer.add_table(plot_node)
        optimizer.add_join(tree_node, plot_node, ["PLT_CN"])
        
        # Get optimal plan
        join_order = optimizer.get_optimal_join_order()
        
        if context.debug:
            warnings.warn(
                f"Using optimized join order: {' -> '.join(join_order)}",
                category=UserWarning
            )
        
        # Execute join
        return tree_frame.join(plot_frame, on="PLT_CN", how="inner")


class JoinTreeConditionStep(JoiningStep):
    """
    Join tree and condition data directly.
    
    This step joins tree and condition data on PLT_CN and CONDID,
    which is necessary for tree-level estimates that require condition
    attributes.
    
    Examples
    --------
    >>> # Standard tree-condition join
    >>> step = JoinTreeConditionStep()
    >>> 
    >>> # Join with condition proportion adjustment
    >>> step = JoinTreeConditionStep(
    ...     apply_condition_proportion=True
    ... )
    """
    
    def __init__(
        self,
        apply_condition_proportion: bool = False,
        condition_columns: Optional[List[str]] = None,
        **kwargs
    ):
        """
        Initialize tree-condition joining step.
        
        Parameters
        ----------
        apply_condition_proportion : bool
            Whether to apply CONDPROP_UNADJ in the join
        condition_columns : Optional[List[str]]
            Specific condition columns to include
        **kwargs
            Additional arguments passed to base class
        """
        super().__init__(join_strategy="tree_condition", **kwargs)
        self.apply_condition_proportion = apply_condition_proportion
        self.condition_columns = condition_columns
    
    def execute_step(
        self, 
        input_data: FilteredDataContract, 
        context: ExecutionContext
    ) -> JoinedDataContract:
        """
        Execute tree-condition join.
        
        Parameters
        ----------
        input_data : FilteredDataContract
            Input contract with filtered data
        context : ExecutionContext
            Execution context
            
        Returns
        -------
        JoinedDataContract
            Contract with joined tree-condition data
        """
        try:
            # Validate input data
            if not input_data.tree_data:
                raise PipelineException(
                    "Tree data required for tree-condition join",
                    step_id=self.step_id
                )
            
            if not input_data.condition_data:
                raise PipelineException(
                    "Condition data required for tree-condition join",
                    step_id=self.step_id
                )
            
            start_time = time.time()
            
            # Get frames
            tree_frame = input_data.tree_data.frame
            condition_frame = input_data.condition_data.frame
            
            # Select condition columns if specified
            if self.condition_columns:
                condition_frame = condition_frame.select(
                    ["PLT_CN", "CONDID"] + 
                    [col for col in self.condition_columns if col not in ["PLT_CN", "CONDID"]]
                )
            
            # Perform join
            joined_frame = tree_frame.join(
                condition_frame,
                on=["PLT_CN", "CONDID"],
                how="inner"
            )
            
            # Apply condition proportion if requested
            if self.apply_condition_proportion:
                if "CONDPROP_UNADJ" in joined_frame.collect_schema().names():
                    # Adjust TPA by condition proportion
                    joined_frame = joined_frame.with_columns(
                        (pl.col("TPA_UNADJ") * pl.col("CONDPROP_UNADJ")).alias("TPA_UNADJ_COND")
                    )
            
            join_time = time.time() - start_time
            
            # Create output contract
            output = JoinedDataContract(
                data=LazyFrameWrapper(joined_frame),
                join_strategy="tree_condition",
                tables_joined=["TREE", "COND"],
                step_id=self.step_id
            )
            
            # Add performance metadata
            output.join_performance = {
                "join_time": join_time,
                "condition_proportion_applied": self.apply_condition_proportion
            }
            
            return output
            
        except Exception as e:
            self.handle_step_error(e, context)
            raise PipelineException(
                f"Failed to join tree and condition data: {e}",
                step_id=self.step_id,
                cause=e
            )


class JoinStratificationStep(JoiningStep):
    """
    Join data with FIA stratification tables.
    
    This step joins estimation data with stratification tables
    (POP_PLOT_STRATUM_ASSGN, POP_STRATUM, POP_ESTN_UNIT) for proper
    variance calculation and expansion factor application.
    
    Examples
    --------
    >>> # Join with stratification for variance calculation
    >>> step = JoinStratificationStep(
    ...     include_expansion_factors=True,
    ...     include_adjustment_factors=True
    ... )
    """
    
    def __init__(
        self,
        include_expansion_factors: bool = True,
        include_adjustment_factors: bool = False,
        stratification_tables: Optional[Dict[str, LazyFrameWrapper]] = None,
        **kwargs
    ):
        """
        Initialize stratification joining step.
        
        Parameters
        ----------
        include_expansion_factors : bool
            Whether to include expansion factors from POP_STRATUM
        include_adjustment_factors : bool
            Whether to include micro/macro/subplot adjustment factors
        stratification_tables : Optional[Dict[str, LazyFrameWrapper]]
            Pre-loaded stratification tables (if not in input data)
        **kwargs
            Additional arguments passed to base class
        """
        super().__init__(join_strategy="stratification", **kwargs)
        self.include_expansion_factors = include_expansion_factors
        self.include_adjustment_factors = include_adjustment_factors
        self.stratification_tables = stratification_tables
    
    def execute_step(
        self, 
        input_data: Union[FilteredDataContract, JoinedDataContract], 
        context: ExecutionContext
    ) -> JoinedDataContract:
        """
        Execute stratification join.
        
        Parameters
        ----------
        input_data : Union[FilteredDataContract, JoinedDataContract]
            Input contract with data to stratify
        context : ExecutionContext
            Execution context
            
        Returns
        -------
        JoinedDataContract
            Contract with stratification data joined
        """
        try:
            # Get the main data frame
            if isinstance(input_data, JoinedDataContract):
                main_frame = input_data.data.frame
            else:
                # Need to determine which data to use
                if input_data.plot_data:
                    main_frame = input_data.plot_data.frame
                elif input_data.condition_data:
                    main_frame = input_data.condition_data.frame
                else:
                    raise PipelineException(
                        "No suitable data for stratification join",
                        step_id=self.step_id
                    )
            
            start_time = time.time()
            
            # Get stratification tables
            ppsa_frame = self._get_stratification_table("POP_PLOT_STRATUM_ASSGN", context)
            ps_frame = self._get_stratification_table("POP_STRATUM", context)
            peu_frame = self._get_stratification_table("POP_ESTN_UNIT", context)
            
            # Join with POP_PLOT_STRATUM_ASSGN first
            stratified_frame = main_frame.join(
                ppsa_frame.select(["PLT_CN", "EVALID", "ESTN_UNIT", "STRATUMCD"]),
                on="PLT_CN",
                how="inner"
            )
            
            # Join with POP_STRATUM for expansion factors
            if self.include_expansion_factors:
                ps_columns = ["EVALID", "ESTN_UNIT", "STRATUMCD", "EXPNS", "P1POINTCNT", "ACRES"]
                
                if self.include_adjustment_factors:
                    ps_columns.extend(["ADJ_FACTOR_MICR", "ADJ_FACTOR_SUBP", "ADJ_FACTOR_MACR"])
                
                stratified_frame = stratified_frame.join(
                    ps_frame.select(ps_columns),
                    on=["EVALID", "ESTN_UNIT", "STRATUMCD"],
                    how="inner"
                )
            
            # Join with POP_ESTN_UNIT for estimation unit info
            stratified_frame = stratified_frame.join(
                peu_frame.select(["EVALID", "ESTN_UNIT", "AREA_USED", "P1PNTCNT_EU"]),
                on=["EVALID", "ESTN_UNIT"],
                how="inner"
            )
            
            join_time = time.time() - start_time
            
            # Create output contract
            output = JoinedDataContract(
                data=LazyFrameWrapper(stratified_frame),
                join_strategy="stratification",
                tables_joined=["POP_PLOT_STRATUM_ASSGN", "POP_STRATUM", "POP_ESTN_UNIT"],
                step_id=self.step_id
            )
            
            # Add performance metadata
            output.join_performance = {
                "join_time": join_time,
                "expansion_factors_included": self.include_expansion_factors,
                "adjustment_factors_included": self.include_adjustment_factors
            }
            
            # Add stratification metadata
            output.add_processing_metadata("stratification_applied", True)
            output.add_processing_metadata("expansion_factors", self.include_expansion_factors)
            
            return output
            
        except Exception as e:
            self.handle_step_error(e, context)
            raise PipelineException(
                f"Failed to join stratification data: {e}",
                step_id=self.step_id,
                cause=e
            )
    
    def _get_stratification_table(self, table_name: str, context: ExecutionContext) -> pl.LazyFrame:
        """Get stratification table from input or pre-loaded tables."""
        # Check if provided in constructor
        if self.stratification_tables and table_name in self.stratification_tables:
            return self.stratification_tables[table_name].frame
        
        # Try to load from database
        try:
            data = context.db.data_reader.load_table(table_name)
            return data if isinstance(data, pl.LazyFrame) else data.lazy()
        except Exception as e:
            raise PipelineException(
                f"Failed to load stratification table {table_name}: {e}",
                step_id=self.step_id
            )


class OptimizedMultiJoinStep(JoiningStep):
    """
    Perform multiple table joins with cost-based optimization.
    
    This step uses the JoinOptimizer to determine the optimal join order
    for multiple tables, significantly improving performance for complex
    join operations.
    
    Examples
    --------
    >>> # Multi-table join with optimization
    >>> step = OptimizedMultiJoinStep(
    ...     tables_to_join=["TREE", "COND", "PLOT"],
    ...     join_keys={
    ...         ("TREE", "COND"): ["PLT_CN", "CONDID"],
    ...         ("TREE", "PLOT"): ["PLT_CN"],
    ...         ("COND", "PLOT"): ["PLT_CN"]
    ...     },
    ...     estimate_cardinalities=True
    ... )
    """
    
    def __init__(
        self,
        tables_to_join: List[str],
        join_keys: Dict[Tuple[str, str], List[str]],
        estimate_cardinalities: bool = True,
        use_statistics: bool = True,
        **kwargs
    ):
        """
        Initialize optimized multi-join step.
        
        Parameters
        ----------
        tables_to_join : List[str]
            Names of tables to join
        join_keys : Dict[Tuple[str, str], List[str]]
            Join keys between table pairs
        estimate_cardinalities : bool
            Whether to estimate table cardinalities for optimization
        use_statistics : bool
            Whether to use table statistics for optimization
        **kwargs
            Additional arguments passed to base class
        """
        super().__init__(join_strategy="optimized_multi", **kwargs)
        self.tables_to_join = tables_to_join
        self.join_keys = join_keys
        self.estimate_cardinalities = estimate_cardinalities
        self.use_statistics = use_statistics
    
    def execute_step(
        self, 
        input_data: FilteredDataContract, 
        context: ExecutionContext
    ) -> JoinedDataContract:
        """
        Execute optimized multi-table join.
        
        Parameters
        ----------
        input_data : FilteredDataContract
            Input contract with filtered data
        context : ExecutionContext
            Execution context
            
        Returns
        -------
        JoinedDataContract
            Contract with all tables joined
        """
        try:
            start_time = time.time()
            
            # Collect available tables
            available_tables = {}
            if "TREE" in self.tables_to_join and input_data.tree_data:
                available_tables["TREE"] = input_data.tree_data.frame
            if "COND" in self.tables_to_join and input_data.condition_data:
                available_tables["COND"] = input_data.condition_data.frame
            if "PLOT" in self.tables_to_join and input_data.plot_data:
                available_tables["PLOT"] = input_data.plot_data.frame
            
            # Validate that all required tables are available
            missing_tables = set(self.tables_to_join) - set(available_tables.keys())
            if missing_tables:
                raise PipelineException(
                    f"Missing required tables for multi-join: {missing_tables}",
                    step_id=self.step_id
                )
            
            # Create join optimizer
            optimizer = JoinOptimizer()
            
            # Add tables to optimizer with cardinality estimates
            table_nodes = {}
            for table_name, frame in available_tables.items():
                if self.estimate_cardinalities:
                    # Estimate cardinality
                    if isinstance(frame, pl.LazyFrame):
                        cardinality = frame.select(pl.count()).collect().item()
                    else:
                        cardinality = len(frame)
                else:
                    # Use default estimates
                    cardinality = {"TREE": 1000000, "COND": 100000, "PLOT": 10000}.get(table_name, 50000)
                
                node = JoinNode(table_name, estimated_rows=cardinality)
                table_nodes[table_name] = node
                optimizer.add_table(node)
            
            # Add joins to optimizer
            for (table1, table2), keys in self.join_keys.items():
                if table1 in table_nodes and table2 in table_nodes:
                    optimizer.add_join(table_nodes[table1], table_nodes[table2], keys)
            
            # Get optimal join order
            join_order = optimizer.get_optimal_join_order()
            
            if context.debug:
                warnings.warn(
                    f"Optimal join order: {' -> '.join(join_order)}",
                    category=UserWarning
                )
            
            # Execute joins in optimal order
            result_frame = self._execute_join_plan(available_tables, join_order, context)
            
            join_time = time.time() - start_time
            
            # Create output contract
            output = JoinedDataContract(
                data=LazyFrameWrapper(result_frame),
                join_strategy="optimized_multi",
                tables_joined=self.tables_to_join,
                step_id=self.step_id
            )
            
            # Add performance metadata
            output.join_performance = {
                "join_time": join_time,
                "join_order": join_order,
                "optimization_used": True,
                "cardinalities_estimated": self.estimate_cardinalities
            }
            
            # Track performance
            self.track_performance(
                context,
                join_time=join_time,
                tables_joined=len(self.tables_to_join),
                optimization_used=True
            )
            
            return output
            
        except Exception as e:
            self.handle_step_error(e, context)
            raise PipelineException(
                f"Failed to execute optimized multi-join: {e}",
                step_id=self.step_id,
                cause=e
            )
    
    def _execute_join_plan(
        self, 
        tables: Dict[str, pl.LazyFrame],
        join_order: List[str],
        context: ExecutionContext
    ) -> pl.LazyFrame:
        """Execute joins according to the optimal plan."""
        # Start with the first table
        result = tables[join_order[0]]
        tables_in_result = {join_order[0]}
        
        # Join remaining tables in order
        for table_name in join_order[1:]:
            # Find the join keys for this table
            join_keys = None
            for (t1, t2), keys in self.join_keys.items():
                if (t1 == table_name and t2 in tables_in_result) or \
                   (t2 == table_name and t1 in tables_in_result):
                    join_keys = keys
                    break
            
            if join_keys:
                result = result.join(
                    tables[table_name],
                    on=join_keys,
                    how="inner"
                )
                tables_in_result.add(table_name)
            else:
                warnings.warn(
                    f"No join keys found for table {table_name} with existing tables",
                    category=UserWarning
                )
        
        return result


# Export all joining step classes
__all__ = [
    "JoinPlotConditionStep",
    "JoinTreePlotStep",
    "JoinTreeConditionStep",
    "JoinStratificationStep",
    "OptimizedMultiJoinStep",
]