"""
Data aggregation steps for the pyFIA estimation pipeline.

This module provides pipeline steps for aggregating tree and condition level
data to plot level and higher levels, supporting various grouping strategies.

Steps:
- AggregateToPlotLevelStep: Aggregate tree data to plot level
- GroupBySpeciesStep: Group data by species code
- GroupByDiameterClassStep: Group by diameter classes
- ApplyGroupingStep: Generic grouping by user-specified columns
- CombineGroupsStep: Combine multiple grouping levels
"""

from typing import Dict, List, Optional, Type
import warnings

import polars as pl

from ...lazy_evaluation import LazyFrameWrapper
from ..core import ExecutionContext, PipelineException
from ..contracts import ValuedDataContract, PlotEstimatesContract
from ..base_steps import AggregationStep


class AggregateToPlotLevelStep(AggregationStep):
    """
    Aggregate tree-level data to plot level.
    
    This is the fundamental aggregation step in FIA estimation, converting
    tree-level values to plot-level estimates.
    
    Examples
    --------
    >>> # Aggregate volume to plot level
    >>> step = AggregateToPlotLevelStep(
    ...     value_columns=["VOLCFNET_AC", "VOLBFNET_AC"],
    ...     aggregation_method="sum"
    ... )
    >>> 
    >>> # Aggregate with grouping by species
    >>> step = AggregateToPlotLevelStep(
    ...     value_columns=["TPA", "BA_AC"],
    ...     group_columns=["SPCD"],
    ...     include_plot_metadata=True
    ... )
    """
    
    def __init__(
        self,
        value_columns: List[str],
        aggregation_method: str = "sum",
        group_columns: Optional[List[str]] = None,
        include_plot_metadata: bool = True,
        **kwargs
    ):
        """
        Initialize plot-level aggregation step.
        
        Parameters
        ----------
        value_columns : List[str]
            Columns containing values to aggregate
        aggregation_method : str
            Method for aggregation ("sum", "mean", "count")
        group_columns : Optional[List[str]]
            Additional columns to group by beyond PLT_CN
        include_plot_metadata : bool
            Whether to include plot metadata columns
        **kwargs
            Additional arguments passed to base class
        """
        super().__init__(
            aggregation_method=aggregation_method,
            group_columns=group_columns,
            **kwargs
        )
        self.value_columns = value_columns
        self.include_plot_metadata = include_plot_metadata
    
    def get_input_contract(self) -> Type[ValuedDataContract]:
        return ValuedDataContract
    
    def get_output_contract(self) -> Type[PlotEstimatesContract]:
        return PlotEstimatesContract
    
    def execute_step(
        self, 
        input_data: ValuedDataContract, 
        context: ExecutionContext
    ) -> PlotEstimatesContract:
        """
        Execute plot-level aggregation.
        
        Parameters
        ----------
        input_data : ValuedDataContract
            Input contract with valued tree data
        context : ExecutionContext
            Execution context
            
        Returns
        -------
        PlotEstimatesContract
            Contract with plot-level estimates
        """
        try:
            # Aggregate to plot level
            aggregated_data = self.aggregate_to_plot_level(
                input_data.data,
                self.value_columns
            )
            
            # Add plot metadata if requested
            if self.include_plot_metadata:
                aggregated_data = self._add_plot_metadata(aggregated_data, context)
            
            # Count plots processed
            if isinstance(aggregated_data.frame, pl.LazyFrame):
                plots_processed = aggregated_data.frame.select(
                    pl.col("PLT_CN").n_unique()
                ).collect().item()
            else:
                plots_processed = aggregated_data.frame["PLT_CN"].n_unique()
            
            # Create output contract
            output = PlotEstimatesContract(
                data=aggregated_data,
                estimate_columns=self.value_columns,
                estimate_type=input_data.value_type,
                group_columns=self.group_columns or [],
                aggregation_method=self.aggregation_method,
                plots_processed=plots_processed,
                step_id=self.step_id
            )
            
            # Track performance
            self.track_performance(
                context,
                plots_processed=plots_processed,
                values_aggregated=len(self.value_columns)
            )
            
            return output
            
        except Exception as e:
            self.handle_step_error(e, context)
            raise PipelineException(
                f"Failed to aggregate to plot level: {e}",
                step_id=self.step_id,
                cause=e
            )
    
    def _add_plot_metadata(self, data: LazyFrameWrapper, context: ExecutionContext) -> LazyFrameWrapper:
        """Add plot metadata columns if available."""
        # This would join with PLOT table for metadata
        # Simplified for demonstration
        return data


class GroupBySpeciesStep(AggregationStep):
    """
    Group estimation data by species.
    
    This step groups data by species code (SPCD), optionally mapping to
    species groups and including species names.
    
    Examples
    --------
    >>> # Group by individual species
    >>> step = GroupBySpeciesStep(
    ...     include_species_names=True,
    ...     include_totals=True
    ... )
    >>> 
    >>> # Group by species groups
    >>> step = GroupBySpeciesStep(
    ...     use_species_groups=True,
    ...     species_group_mapping="FIA_STANDARD"
    ... )
    """
    
    def __init__(
        self,
        use_species_groups: bool = False,
        species_group_mapping: Optional[str] = None,
        include_species_names: bool = False,
        include_totals: bool = True,
        min_trees_threshold: Optional[int] = None,
        **kwargs
    ):
        """
        Initialize species grouping step.
        
        Parameters
        ----------
        use_species_groups : bool
            Whether to group species into broader groups
        species_group_mapping : Optional[str]
            Species group mapping to use
        include_species_names : bool
            Whether to add species common/scientific names
        include_totals : bool
            Whether to include a total across all species
        min_trees_threshold : Optional[int]
            Minimum number of trees to include species
        **kwargs
            Additional arguments passed to base class
        """
        group_columns = ["SPCD"] if not use_species_groups else ["SPGRPCD"]
        super().__init__(group_columns=group_columns, **kwargs)
        
        self.use_species_groups = use_species_groups
        self.species_group_mapping = species_group_mapping
        self.include_species_names = include_species_names
        self.include_totals = include_totals
        self.min_trees_threshold = min_trees_threshold
    
    def execute_step(
        self, 
        input_data: Union[ValuedDataContract, PlotEstimatesContract], 
        context: ExecutionContext
    ) -> Union[PlotEstimatesContract, ValuedDataContract]:
        """
        Execute species grouping.
        
        Parameters
        ----------
        input_data : Union[ValuedDataContract, PlotEstimatesContract]
            Input contract with data to group
        context : ExecutionContext
            Execution context
            
        Returns
        -------
        Union[PlotEstimatesContract, ValuedDataContract]
            Contract with species-grouped data
        """
        try:
            frame = input_data.data.frame
            
            # Add species group column if needed
            if self.use_species_groups:
                frame = self._add_species_groups(frame, context)
                group_col = "SPGRPCD"
            else:
                group_col = "SPCD"
            
            # Determine value columns
            if isinstance(input_data, PlotEstimatesContract):
                value_columns = input_data.estimate_columns
            else:
                value_columns = input_data.value_columns
            
            # Group by species
            grouped_frame = frame.group_by([group_col]).agg(
                [pl.col(col).sum().alias(col) for col in value_columns] +
                [pl.count().alias("N_RECORDS")]
            )
            
            # Filter by minimum trees threshold if specified
            if self.min_trees_threshold:
                grouped_frame = grouped_frame.filter(
                    pl.col("N_RECORDS") >= self.min_trees_threshold
                )
            
            # Add species names if requested
            if self.include_species_names:
                grouped_frame = self._add_species_names(grouped_frame, group_col, context)
            
            # Add totals row if requested
            if self.include_totals:
                totals = frame.select(
                    [pl.col(col).sum().alias(col) for col in value_columns]
                ).with_columns(
                    pl.lit(-1).alias(group_col),  # Special code for total
                    pl.lit("TOTAL").alias("SPECIES_NAME") if self.include_species_names else pl.lit(None)
                )
                
                grouped_frame = pl.concat([grouped_frame, totals])
            
            # Create output contract
            output_data = LazyFrameWrapper(grouped_frame)
            
            if isinstance(input_data, PlotEstimatesContract):
                output = PlotEstimatesContract(
                    data=output_data,
                    estimate_columns=value_columns,
                    estimate_type=input_data.estimate_type,
                    group_columns=[group_col],
                    step_id=self.step_id
                )
            else:
                output = ValuedDataContract(
                    data=output_data,
                    value_columns=value_columns,
                    value_type=input_data.value_type,
                    group_columns=[group_col],
                    step_id=self.step_id
                )
            
            # Add metadata
            output.add_processing_metadata("species_grouping", self.use_species_groups)
            output.add_processing_metadata("include_totals", self.include_totals)
            
            return output
            
        except Exception as e:
            self.handle_step_error(e, context)
            raise PipelineException(
                f"Failed to group by species: {e}",
                step_id=self.step_id,
                cause=e
            )
    
    def _add_species_groups(self, frame: pl.LazyFrame, context: ExecutionContext) -> pl.LazyFrame:
        """Add species group codes based on mapping."""
        # This would use REF_SPECIES_GROUP table
        # Simplified mapping for demonstration
        return frame.with_columns(
            pl.when(pl.col("SPCD") < 100).then(1)  # Softwoods
            .when(pl.col("SPCD") < 300).then(2)     # Hardwoods
            .otherwise(3)                            # Other
            .alias("SPGRPCD")
        )
    
    def _add_species_names(self, frame: pl.LazyFrame, group_col: str, context: ExecutionContext) -> pl.LazyFrame:
        """Add species common and scientific names."""
        # This would join with REF_SPECIES table
        # Simplified for demonstration
        return frame.with_columns(
            pl.col(group_col).cast(pl.Utf8).alias("SPECIES_NAME")
        )


class GroupByDiameterClassStep(AggregationStep):
    """
    Group data by diameter classes.
    
    This step groups tree data into diameter classes for size distribution
    analysis.
    
    Examples
    --------
    >>> # Standard 2-inch diameter classes
    >>> step = GroupByDiameterClassStep(
    ...     class_width=2.0,
    ...     min_diameter=5.0,
    ...     max_diameter=30.0
    ... )
    >>> 
    >>> # Custom diameter classes
    >>> step = GroupByDiameterClassStep(
    ...     custom_classes=[(1, 5), (5, 10), (10, 15), (15, 20), (20, None)]
    ... )
    """
    
    def __init__(
        self,
        class_width: float = 2.0,
        min_diameter: float = 0.0,
        max_diameter: Optional[float] = None,
        custom_classes: Optional[List[tuple]] = None,
        include_class_labels: bool = True,
        **kwargs
    ):
        """
        Initialize diameter class grouping step.
        
        Parameters
        ----------
        class_width : float
            Width of diameter classes in inches
        min_diameter : float
            Minimum diameter to include
        max_diameter : Optional[float]
            Maximum diameter (None = no limit)
        custom_classes : Optional[List[tuple]]
            Custom diameter class boundaries
        include_class_labels : bool
            Whether to include human-readable class labels
        **kwargs
            Additional arguments passed to base class
        """
        super().__init__(group_columns=["DIA_CLASS"], **kwargs)
        self.class_width = class_width
        self.min_diameter = min_diameter
        self.max_diameter = max_diameter
        self.custom_classes = custom_classes
        self.include_class_labels = include_class_labels
    
    def execute_step(
        self, 
        input_data: ValuedDataContract, 
        context: ExecutionContext
    ) -> ValuedDataContract:
        """
        Execute diameter class grouping.
        
        Parameters
        ----------
        input_data : ValuedDataContract
            Input contract with tree data
        context : ExecutionContext
            Execution context
            
        Returns
        -------
        ValuedDataContract
            Contract with diameter-class grouped data
        """
        try:
            frame = input_data.data.frame
            
            # Add diameter class column
            if self.custom_classes:
                frame = self._add_custom_diameter_classes(frame)
            else:
                frame = self._add_standard_diameter_classes(frame)
            
            # Add class labels if requested
            if self.include_class_labels:
                frame = self._add_class_labels(frame)
            
            # Group by diameter class
            grouped_frame = frame.group_by(["DIA_CLASS"]).agg(
                [pl.col(col).sum().alias(col) for col in input_data.value_columns] +
                [pl.count().alias("N_TREES")]
            ).sort("DIA_CLASS")
            
            # Create output contract
            output = ValuedDataContract(
                data=LazyFrameWrapper(grouped_frame),
                value_columns=input_data.value_columns,
                value_type=input_data.value_type,
                group_columns=["DIA_CLASS"],
                step_id=self.step_id
            )
            
            # Add metadata
            output.add_processing_metadata("class_width", self.class_width)
            output.add_processing_metadata("custom_classes", self.custom_classes)
            
            return output
            
        except Exception as e:
            self.handle_step_error(e, context)
            raise PipelineException(
                f"Failed to group by diameter class: {e}",
                step_id=self.step_id,
                cause=e
            )
    
    def _add_standard_diameter_classes(self, frame: pl.LazyFrame) -> pl.LazyFrame:
        """Add standard width diameter classes."""
        return frame.with_columns(
            ((pl.col("DIA") / self.class_width).floor() * self.class_width).alias("DIA_CLASS")
        ).filter(
            (pl.col("DIA") >= self.min_diameter) &
            (pl.col("DIA") < self.max_diameter if self.max_diameter else True)
        )
    
    def _add_custom_diameter_classes(self, frame: pl.LazyFrame) -> pl.LazyFrame:
        """Add custom diameter classes."""
        # Build conditional expression for custom classes
        expr = pl.lit(None)
        
        for i, (min_dia, max_dia) in enumerate(self.custom_classes):
            if min_dia is None:
                condition = pl.col("DIA") < max_dia
            elif max_dia is None:
                condition = pl.col("DIA") >= min_dia
            else:
                condition = (pl.col("DIA") >= min_dia) & (pl.col("DIA") < max_dia)
            
            expr = pl.when(condition).then(i).otherwise(expr)
        
        return frame.with_columns(expr.alias("DIA_CLASS"))
    
    def _add_class_labels(self, frame: pl.LazyFrame) -> pl.LazyFrame:
        """Add human-readable diameter class labels."""
        if self.custom_classes:
            # Create labels for custom classes
            labels = []
            for min_dia, max_dia in self.custom_classes:
                if min_dia is None:
                    label = f"< {max_dia}\""
                elif max_dia is None:
                    label = f">= {min_dia}\""
                else:
                    label = f"{min_dia}-{max_dia}\""
                labels.append(label)
            
            # Map class indices to labels
            label_expr = pl.lit(None)
            for i, label in enumerate(labels):
                label_expr = pl.when(pl.col("DIA_CLASS") == i).then(label).otherwise(label_expr)
            
            return frame.with_columns(label_expr.alias("DIA_CLASS_LABEL"))
        else:
            # Standard class labels
            return frame.with_columns(
                pl.concat_str([
                    pl.col("DIA_CLASS").cast(pl.Utf8),
                    pl.lit("-"),
                    (pl.col("DIA_CLASS") + self.class_width).cast(pl.Utf8),
                    pl.lit("\"")
                ]).alias("DIA_CLASS_LABEL")
            )


class ApplyGroupingStep(AggregationStep):
    """
    Generic grouping by user-specified columns.
    
    This step provides flexible grouping by any combination of columns,
    supporting complex aggregation scenarios.
    
    Examples
    --------
    >>> # Group by multiple attributes
    >>> step = ApplyGroupingStep(
    ...     group_columns=["OWNGRPCD", "FORTYPCD", "STDSZCD"],
    ...     aggregation_functions={
    ...         "TPA": "sum",
    ...         "BA_AC": "sum", 
    ...         "HT": "mean"
    ...     }
    ... )
    """
    
    def __init__(
        self,
        group_columns: List[str],
        aggregation_functions: Optional[Dict[str, str]] = None,
        include_counts: bool = True,
        sort_by_groups: bool = True,
        **kwargs
    ):
        """
        Initialize generic grouping step.
        
        Parameters
        ----------
        group_columns : List[str]
            Columns to group by
        aggregation_functions : Optional[Dict[str, str]]
            Specific aggregation functions per column
        include_counts : bool
            Whether to include record counts
        sort_by_groups : bool
            Whether to sort output by group columns
        **kwargs
            Additional arguments passed to base class
        """
        super().__init__(group_columns=group_columns, **kwargs)
        self.aggregation_functions = aggregation_functions or {}
        self.include_counts = include_counts
        self.sort_by_groups = sort_by_groups
    
    def execute_step(
        self, 
        input_data: Union[ValuedDataContract, PlotEstimatesContract], 
        context: ExecutionContext
    ) -> Union[ValuedDataContract, PlotEstimatesContract]:
        """
        Execute generic grouping.
        
        Parameters
        ----------
        input_data : Union[ValuedDataContract, PlotEstimatesContract]
            Input contract with data to group
        context : ExecutionContext
            Execution context
            
        Returns
        -------
        Union[ValuedDataContract, PlotEstimatesContract]
            Contract with grouped data
        """
        try:
            frame = input_data.data.frame
            
            # Determine value columns
            if isinstance(input_data, PlotEstimatesContract):
                value_columns = input_data.estimate_columns
            else:
                value_columns = input_data.value_columns
            
            # Build aggregation expressions
            agg_exprs = []
            for col in value_columns:
                agg_func = self.aggregation_functions.get(col, self.aggregation_method)
                
                if agg_func == "sum":
                    agg_exprs.append(pl.col(col).sum().alias(col))
                elif agg_func == "mean":
                    agg_exprs.append(pl.col(col).mean().alias(col))
                elif agg_func == "count":
                    agg_exprs.append(pl.col(col).count().alias(col))
                elif agg_func == "min":
                    agg_exprs.append(pl.col(col).min().alias(col))
                elif agg_func == "max":
                    agg_exprs.append(pl.col(col).max().alias(col))
                else:
                    agg_exprs.append(pl.col(col).sum().alias(col))  # Default to sum
            
            # Add count if requested
            if self.include_counts:
                agg_exprs.append(pl.count().alias("N_RECORDS"))
            
            # Perform grouping
            grouped_frame = frame.group_by(self.group_columns).agg(agg_exprs)
            
            # Sort if requested
            if self.sort_by_groups:
                grouped_frame = grouped_frame.sort(self.group_columns)
            
            # Create output contract
            output_data = LazyFrameWrapper(grouped_frame)
            
            if isinstance(input_data, PlotEstimatesContract):
                output = PlotEstimatesContract(
                    data=output_data,
                    estimate_columns=value_columns,
                    estimate_type=input_data.estimate_type,
                    group_columns=self.group_columns,
                    step_id=self.step_id
                )
            else:
                output = ValuedDataContract(
                    data=output_data,
                    value_columns=value_columns,
                    value_type=input_data.value_type,
                    group_columns=self.group_columns,
                    step_id=self.step_id
                )
            
            # Add metadata
            output.add_processing_metadata("aggregation_functions", self.aggregation_functions)
            output.add_processing_metadata("groups_used", self.group_columns)
            
            return output
            
        except Exception as e:
            self.handle_step_error(e, context)
            raise PipelineException(
                f"Failed to apply grouping: {e}",
                step_id=self.step_id,
                cause=e
            )


# Export all aggregation step classes
__all__ = [
    "AggregateToPlotLevelStep",
    "GroupBySpeciesStep",
    "GroupByDiameterClassStep",
    "ApplyGroupingStep",
]