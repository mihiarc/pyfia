"""
Output formatting and finalization steps for FIA estimation pipeline.

This module implements pipeline steps for formatting estimation results
for user consumption, including calculating population estimates,
formatting output structure, adding totals, calculating percentages,
and formatting variance/standard error columns.

Steps include:
- CalculatePopulationEstimatesStep: Finalize population-level estimates
- FormatOutputStep: Format to expected output structure
- AddTotalsStep: Add total estimates if requested
- CalculatePercentagesStep: Calculate percentage estimates (for area)
- FormatVarianceOutputStep: Format variance/SE columns properly

All steps ensure output compatibility with rFIA and proper formatting
for user consumption.
"""

from typing import Dict, List, Optional, Set, Type, Union, Any
import warnings

import polars as pl
from pydantic import Field

from ...config import EstimatorConfig
from ...lazy_evaluation import LazyFrameWrapper
from ...statistics.expressions import safe_divide, safe_percentage

from ..base_steps import BaseEstimationStep, FormattingStep
from ..core import (
    ExecutionContext,
    PipelineException,
    StepValidationError
)
from ..contracts import (
    PopulationEstimatesContract,
    FormattedOutputContract,
    StratifiedEstimatesContract
)


class CalculatePopulationEstimatesStep(FormattingStep):
    """
    Finalize population-level estimates for output.
    
    This step takes the raw population estimates and prepares them for
    user consumption by calculating final estimate values, confidence
    intervals, and ensuring all required statistics are present.
    """
    
    def __init__(
        self,
        confidence_level: float = 0.95,
        include_confidence_intervals: bool = True,
        include_percentiles: bool = False,
        round_decimals: Optional[int] = None,
        **kwargs
    ):
        """
        Initialize population estimates calculation step.
        
        Parameters
        ----------
        confidence_level : float
            Confidence level for intervals (0.95 for 95% CI)
        include_confidence_intervals : bool
            Whether to include confidence intervals
        include_percentiles : bool
            Whether to include percentile estimates
        round_decimals : Optional[int]
            Number of decimals for rounding (None = no rounding)
        **kwargs
            Additional arguments passed to parent
        """
        super().__init__(**kwargs)
        self.confidence_level = confidence_level
        self.include_confidence_intervals = include_confidence_intervals
        self.include_percentiles = include_percentiles
        self.round_decimals = round_decimals
    
    def get_input_contract(self) -> Type[PopulationEstimatesContract]:
        return PopulationEstimatesContract
    
    def get_output_contract(self) -> Type[PopulationEstimatesContract]:
        return PopulationEstimatesContract
    
    def execute_step(
        self,
        input_data: PopulationEstimatesContract,
        context: ExecutionContext
    ) -> PopulationEstimatesContract:
        """
        Calculate final population estimates.
        
        Parameters
        ----------
        input_data : PopulationEstimatesContract
            Raw population estimates
        context : ExecutionContext
            Execution context
            
        Returns
        -------
        PopulationEstimatesContract
            Finalized population estimates
        """
        try:
            # Calculate confidence intervals if requested
            if self.include_confidence_intervals:
                data_with_ci = self._add_confidence_intervals(
                    input_data.data,
                    input_data.estimate_columns,
                    input_data.variance_columns
                )
            else:
                data_with_ci = input_data.data
            
            # Add percentiles if requested
            if self.include_percentiles:
                data_with_percentiles = self._add_percentiles(
                    data_with_ci,
                    input_data.estimate_columns
                )
            else:
                data_with_percentiles = data_with_ci
            
            # Round values if requested
            if self.round_decimals is not None:
                final_data = self._round_estimates(
                    data_with_percentiles,
                    input_data.estimate_columns,
                    self.round_decimals
                )
            else:
                final_data = data_with_percentiles
            
            # Create output contract
            output = PopulationEstimatesContract(
                data=final_data,
                estimate_columns=input_data.estimate_columns,
                variance_columns=input_data.variance_columns,
                total_columns=input_data.total_columns,
                estimation_method=input_data.estimation_method,
                confidence_level=self.confidence_level,
                degrees_of_freedom=input_data.degrees_of_freedom,
                step_id=self.step_id
            )
            
            # Add metadata
            output.add_processing_metadata("confidence_intervals_included", self.include_confidence_intervals)
            output.add_processing_metadata("percentiles_included", self.include_percentiles)
            output.add_processing_metadata("rounding_applied", self.round_decimals)
            
            return output
            
        except Exception as e:
            self.handle_step_error(e, context)
            raise PipelineException(
                f"Failed to calculate population estimates: {e}",
                step_id=self.step_id,
                cause=e
            )
    
    def _add_confidence_intervals(
        self,
        data: LazyFrameWrapper,
        estimate_columns: List[str],
        variance_columns: List[str]
    ) -> LazyFrameWrapper:
        """Add confidence intervals to estimates."""
        # Calculate critical value for confidence level
        # For large samples, use normal approximation
        # For 95% CI, z = 1.96; for 90% CI, z = 1.645
        if self.confidence_level == 0.95:
            z_value = 1.96
        elif self.confidence_level == 0.90:
            z_value = 1.645
        elif self.confidence_level == 0.68:  # 1 SE
            z_value = 1.0
        else:
            # Use normal approximation for arbitrary confidence levels
            from scipy import stats
            z_value = stats.norm.ppf((1 + self.confidence_level) / 2)
        
        ci_exprs = []
        for est_col in estimate_columns:
            # Find corresponding variance column
            var_col = f"{est_col}_VAR"
            if var_col in variance_columns:
                # Calculate SE from variance
                se_expr = pl.col(var_col).sqrt()
                
                # Calculate confidence intervals
                ci_exprs.extend([
                    (pl.col(est_col) - z_value * se_expr).alias(f"{est_col}_CI_LOWER"),
                    (pl.col(est_col) + z_value * se_expr).alias(f"{est_col}_CI_UPPER")
                ])
        
        return LazyFrameWrapper(data.frame.with_columns(ci_exprs))
    
    def _add_percentiles(
        self,
        data: LazyFrameWrapper,
        estimate_columns: List[str]
    ) -> LazyFrameWrapper:
        """Add percentile estimates (e.g., median, quartiles)."""
        # This would typically be done with bootstrap or other resampling
        # For now, we'll add placeholder columns
        percentile_exprs = []
        for col in estimate_columns:
            percentile_exprs.extend([
                pl.col(col).alias(f"{col}_P50"),  # Median estimate
                (pl.col(col) * 0.75).alias(f"{col}_P25"),  # Placeholder for 25th percentile
                (pl.col(col) * 1.25).alias(f"{col}_P75")   # Placeholder for 75th percentile
            ])
        
        if percentile_exprs:
            return LazyFrameWrapper(data.frame.with_columns(percentile_exprs))
        return data
    
    def _round_estimates(
        self,
        data: LazyFrameWrapper,
        estimate_columns: List[str],
        decimals: int
    ) -> LazyFrameWrapper:
        """Round estimate values to specified decimals."""
        round_exprs = []
        
        # Get all numeric columns
        if isinstance(data.frame, pl.LazyFrame):
            schema = data.frame.collect_schema()
        else:
            schema = data.frame.schema
        
        for col_name, dtype in schema.items():
            if dtype in [pl.Float32, pl.Float64]:
                round_exprs.append(
                    pl.col(col_name).round(decimals).alias(col_name)
                )
        
        if round_exprs:
            return LazyFrameWrapper(data.frame.with_columns(round_exprs))
        return data


class FormatOutputStep(FormattingStep):
    """
    Format estimation results to expected output structure.
    
    This step transforms the internal data structure to the final output
    format expected by users, including column renaming, reordering,
    and adding metadata.
    """
    
    def __init__(
        self,
        output_format: str = "standard",
        column_order: Optional[List[str]] = None,
        include_metadata: bool = True,
        drop_internal_columns: bool = True,
        **kwargs
    ):
        """
        Initialize output formatting step.
        
        Parameters
        ----------
        output_format : str
            Output format style (standard, compact, detailed)
        column_order : Optional[List[str]]
            Specific column ordering (None = automatic)
        include_metadata : bool
            Whether to include analysis metadata
        drop_internal_columns : bool
            Whether to drop internal processing columns
        **kwargs
            Additional arguments passed to parent
        """
        # Set up column mappings based on output format
        if output_format == "standard":
            column_mappings = {
                # Map internal names to user-friendly names
                "PLT_CN": "PLOT_ID",
                "NPLOTS_SAMPLED": "N",
                "NPLOTS_TOTAL": "N_TOTAL"
            }
        else:
            column_mappings = {}
        
        super().__init__(
            column_mappings=column_mappings,
            include_metadata=include_metadata,
            **kwargs
        )
        
        self.output_format = output_format
        self.column_order = column_order
        self.drop_internal_columns = drop_internal_columns
    
    def get_input_contract(self) -> Type[PopulationEstimatesContract]:
        return PopulationEstimatesContract
    
    def get_output_contract(self) -> Type[FormattedOutputContract]:
        return FormattedOutputContract
    
    def execute_step(
        self,
        input_data: PopulationEstimatesContract,
        context: ExecutionContext
    ) -> FormattedOutputContract:
        """
        Format output for user consumption.
        
        Parameters
        ----------
        input_data : PopulationEstimatesContract
            Population estimates to format
        context : ExecutionContext
            Execution context
            
        Returns
        -------
        FormattedOutputContract
            Formatted output ready for return
        """
        try:
            # Apply column mappings
            formatted_data = self.apply_column_mappings(input_data.data)
            
            # Drop internal columns if requested
            if self.drop_internal_columns:
                formatted_data = self._drop_internal_columns(formatted_data)
            
            # Reorder columns if specified
            if self.column_order:
                formatted_data = self._reorder_columns(formatted_data, self.column_order)
            else:
                formatted_data = self._apply_default_ordering(formatted_data)
            
            # Collect to concrete DataFrame
            final_df = self.collect_final_dataframe(formatted_data)
            
            # Create output contract
            output = FormattedOutputContract(
                data=final_df,
                format_version="1.0",
                step_id=self.step_id
            )
            
            # Add metadata if requested
            if self.include_metadata:
                output.output_metadata = self._build_output_metadata(input_data, context)
            
            # Add column descriptions
            self._add_column_descriptions(output, input_data)
            
            # Validate output
            validation_warnings = output.validate_final_output()
            if validation_warnings:
                for warning in validation_warnings:
                    warnings.warn(f"Output validation: {warning}", UserWarning)
            
            return output
            
        except Exception as e:
            self.handle_step_error(e, context)
            raise PipelineException(
                f"Failed to format output: {e}",
                step_id=self.step_id,
                cause=e
            )
    
    def _drop_internal_columns(self, data: LazyFrameWrapper) -> LazyFrameWrapper:
        """Drop internal processing columns from output."""
        # List of internal column patterns to drop
        internal_patterns = [
            "_h$",  # Stratum-level statistics
            "^w_",  # Stratum weights
            "^n_",  # Sample sizes (except N)
            "^s_",  # Standard deviations
            "^fpc",  # Finite population correction
            "EXPNS",  # Expansion factors
            "ADJ_FACTOR",  # Adjustment factors
            "PLT_CN",  # Internal plot ID (unless mapped)
            "CONDID",  # Condition ID
            "ESTN_UNIT",  # Estimation unit
            "STRATUM"  # Stratum ID
        ]
        
        # Get current columns
        if isinstance(data.frame, pl.LazyFrame):
            current_cols = data.frame.collect_schema().names()
        else:
            current_cols = data.frame.columns
        
        # Identify columns to keep
        cols_to_keep = []
        for col in current_cols:
            # Check if column matches any internal pattern
            is_internal = False
            for pattern in internal_patterns:
                if pattern.startswith("^"):
                    if col.startswith(pattern[1:]):
                        is_internal = True
                        break
                elif pattern.endswith("$"):
                    if col.endswith(pattern[:-1]):
                        is_internal = True
                        break
                elif pattern in col:
                    is_internal = True
                    break
            
            if not is_internal:
                cols_to_keep.append(col)
        
        return LazyFrameWrapper(data.frame.select(cols_to_keep))
    
    def _reorder_columns(
        self,
        data: LazyFrameWrapper,
        column_order: List[str]
    ) -> LazyFrameWrapper:
        """Reorder columns according to specified order."""
        # Get available columns
        if isinstance(data.frame, pl.LazyFrame):
            available_cols = set(data.frame.collect_schema().names())
        else:
            available_cols = set(data.frame.columns)
        
        # Build ordered column list
        ordered_cols = []
        for col in column_order:
            if col in available_cols:
                ordered_cols.append(col)
        
        # Add any remaining columns not in the order list
        for col in available_cols:
            if col not in ordered_cols:
                ordered_cols.append(col)
        
        return LazyFrameWrapper(data.frame.select(ordered_cols))
    
    def _apply_default_ordering(self, data: LazyFrameWrapper) -> LazyFrameWrapper:
        """Apply default column ordering for output."""
        # Get current columns
        if isinstance(data.frame, pl.LazyFrame):
            current_cols = data.frame.collect_schema().names()
        else:
            current_cols = data.frame.columns
        
        # Define ordering priority
        priority_order = [
            # Grouping columns first
            lambda x: x in ["YEAR", "STATECD", "SPCD", "OWNGRPCD"],
            # Main estimates
            lambda x: not any(suffix in x for suffix in ["_VAR", "_SE", "_CV", "_CI", "_PCT"]),
            # Totals
            lambda x: "_TOTAL" in x,
            # Percentages
            lambda x: "_PCT" in x or "_PERC" in x,
            # Standard errors
            lambda x: "_SE" in x,
            # Variance
            lambda x: "_VAR" in x,
            # Confidence intervals
            lambda x: "_CI" in x,
            # Coefficients of variation
            lambda x: "_CV" in x,
            # Sample sizes
            lambda x: x.startswith("N"),
            # Everything else
            lambda x: True
        ]
        
        # Sort columns by priority
        ordered_cols = []
        remaining_cols = list(current_cols)
        
        for priority_func in priority_order:
            matching_cols = [col for col in remaining_cols if priority_func(col)]
            ordered_cols.extend(sorted(matching_cols))
            remaining_cols = [col for col in remaining_cols if col not in matching_cols]
        
        return LazyFrameWrapper(data.frame.select(ordered_cols))
    
    def _build_output_metadata(
        self,
        input_data: PopulationEstimatesContract,
        context: ExecutionContext
    ) -> Dict[str, Any]:
        """Build metadata dictionary for output."""
        metadata = {
            "estimation_method": input_data.estimation_method,
            "confidence_level": input_data.confidence_level,
            "output_format": self.output_format,
            "processing_time": context.get_context_data("total_time", 0),
            "pyfia_version": "0.1.0"  # Would get from package version
        }
        
        # Add any additional metadata from context
        if context.debug:
            metadata["debug_info"] = {
                "step_count": len(context.get_context_data("executed_steps", [])),
                "cache_hits": context.get_context_data("cache_hits", 0)
            }
        
        return metadata
    
    def _add_column_descriptions(
        self,
        output: FormattedOutputContract,
        input_data: PopulationEstimatesContract
    ) -> None:
        """Add descriptions for output columns."""
        # Standard column descriptions
        descriptions = {
            "N": "Number of plots sampled",
            "N_TOTAL": "Total number of plots in population",
            "YEAR": "Inventory year",
            "STATECD": "State FIPS code",
            "SPCD": "Species code",
            "OWNGRPCD": "Ownership group code"
        }
        
        # Add descriptions for estimate columns
        for col in input_data.estimate_columns:
            base_name = self.column_mappings.get(col, col)
            descriptions[base_name] = f"Estimate of {col.lower()}"
            descriptions[f"{base_name}_TOTAL"] = f"Total {col.lower()} estimate"
            descriptions[f"{base_name}_SE"] = f"Standard error of {col.lower()} estimate"
            descriptions[f"{base_name}_VAR"] = f"Variance of {col.lower()} estimate"
            descriptions[f"{base_name}_CI_LOWER"] = f"Lower confidence bound for {col.lower()}"
            descriptions[f"{base_name}_CI_UPPER"] = f"Upper confidence bound for {col.lower()}"
            descriptions[f"{base_name}_PCT"] = f"Percentage of total for {col.lower()}"
        
        # Apply descriptions
        for col, desc in descriptions.items():
            if col in output.data.columns:
                output.add_column_description(col, desc)


class AddTotalsStep(BaseEstimationStep[PopulationEstimatesContract, PopulationEstimatesContract]):
    """
    Add total estimates to population results if requested.
    
    This step calculates and adds total row(s) to grouped estimates,
    providing overall totals across all groups.
    """
    
    def __init__(
        self,
        total_label: str = "Total",
        group_columns: Optional[List[str]] = None,
        calculate_percentages: bool = True,
        **kwargs
    ):
        """
        Initialize totals addition step.
        
        Parameters
        ----------
        total_label : str
            Label for total rows
        group_columns : Optional[List[str]]
            Columns that define groups
        calculate_percentages : bool
            Whether to calculate percentages of total
        **kwargs
            Additional arguments passed to parent
        """
        super().__init__(**kwargs)
        self.total_label = total_label
        self.group_columns = group_columns or []
        self.calculate_percentages = calculate_percentages
    
    def get_input_contract(self) -> Type[PopulationEstimatesContract]:
        return PopulationEstimatesContract
    
    def get_output_contract(self) -> Type[PopulationEstimatesContract]:
        return PopulationEstimatesContract
    
    def execute_step(
        self,
        input_data: PopulationEstimatesContract,
        context: ExecutionContext
    ) -> PopulationEstimatesContract:
        """
        Add totals to population estimates.
        
        Parameters
        ----------
        input_data : PopulationEstimatesContract
            Population estimates
        context : ExecutionContext
            Execution context
            
        Returns
        -------
        PopulationEstimatesContract
            Estimates with totals added
        """
        try:
            # Calculate totals
            totals_data = self._calculate_totals(
                input_data.data,
                input_data.estimate_columns,
                input_data.variance_columns
            )
            
            # Add total label
            totals_with_label = self._add_total_label(totals_data)
            
            # Combine with original data
            combined_data = self._combine_with_totals(
                input_data.data,
                totals_with_label
            )
            
            # Calculate percentages if requested
            if self.calculate_percentages:
                final_data = self._calculate_percentage_of_total(
                    combined_data,
                    input_data.estimate_columns
                )
            else:
                final_data = combined_data
            
            # Create output contract
            output = PopulationEstimatesContract(
                data=final_data,
                estimate_columns=input_data.estimate_columns,
                variance_columns=input_data.variance_columns,
                total_columns=input_data.total_columns + [self.total_label],
                estimation_method=input_data.estimation_method,
                confidence_level=input_data.confidence_level,
                degrees_of_freedom=input_data.degrees_of_freedom,
                step_id=self.step_id
            )
            
            # Add metadata
            output.add_processing_metadata("totals_added", True)
            output.add_processing_metadata("total_label", self.total_label)
            output.add_processing_metadata("percentages_calculated", self.calculate_percentages)
            
            return output
            
        except Exception as e:
            self.handle_step_error(e, context)
            raise PipelineException(
                f"Failed to add totals: {e}",
                step_id=self.step_id,
                cause=e
            )
    
    def _calculate_totals(
        self,
        data: LazyFrameWrapper,
        estimate_columns: List[str],
        variance_columns: List[str]
    ) -> LazyFrameWrapper:
        """Calculate total row from grouped data."""
        # Build aggregation expressions for totals
        agg_exprs = []
        
        for col in estimate_columns:
            agg_exprs.append(pl.col(col).sum().alias(col))
        
        for col in variance_columns:
            # Variance of sum = sum of variances (assuming independence)
            agg_exprs.append(pl.col(col).sum().alias(col))
        
        # Add sample size totals
        if "N" in data.frame.collect_schema().names():
            agg_exprs.append(pl.col("N").sum().alias("N"))
        
        # Calculate totals
        totals_frame = data.frame.select(agg_exprs)
        
        return LazyFrameWrapper(totals_frame)
    
    def _add_total_label(self, totals_data: LazyFrameWrapper) -> LazyFrameWrapper:
        """Add label column to totals row."""
        # Add group column values as "Total" for the totals row
        label_exprs = []
        
        for col in self.group_columns:
            label_exprs.append(pl.lit(self.total_label).alias(col))
        
        if label_exprs:
            labeled_frame = totals_data.frame.with_columns(label_exprs)
        else:
            # Add a generic total indicator column
            labeled_frame = totals_data.frame.with_columns([
                pl.lit(self.total_label).alias("GROUP")
            ])
        
        return LazyFrameWrapper(labeled_frame)
    
    def _combine_with_totals(
        self,
        original_data: LazyFrameWrapper,
        totals_data: LazyFrameWrapper
    ) -> LazyFrameWrapper:
        """Combine original data with totals row."""
        # Use concat to combine the dataframes
        combined_frame = pl.concat([
            original_data.frame,
            totals_data.frame
        ], how="diagonal")
        
        return LazyFrameWrapper(combined_frame)
    
    def _calculate_percentage_of_total(
        self,
        data: LazyFrameWrapper,
        estimate_columns: List[str]
    ) -> LazyFrameWrapper:
        """Calculate each group's percentage of total."""
        pct_exprs = []
        
        for col in estimate_columns:
            # Get total value for this column
            total_expr = pl.col(col).filter(
                pl.col(self.group_columns[0] if self.group_columns else "GROUP") == self.total_label
            ).first()
            
            # Calculate percentage
            pct_exprs.append(
                safe_percentage(pl.col(col), total_expr).alias(f"{col}_PCT")
            )
        
        return LazyFrameWrapper(data.frame.with_columns(pct_exprs))


class CalculatePercentagesStep(BaseEstimationStep[PopulationEstimatesContract, PopulationEstimatesContract]):
    """
    Calculate percentage estimates for area and other proportion-based metrics.
    
    This step calculates percentages with proper handling of edge cases
    and maintains statistical validity for proportion estimates.
    """
    
    def __init__(
        self,
        percentage_base: str = "total",
        percentage_columns: Optional[List[str]] = None,
        min_denominator: float = 0.001,
        **kwargs
    ):
        """
        Initialize percentage calculation step.
        
        Parameters
        ----------
        percentage_base : str
            Base for percentage calculation (total, group, etc.)
        percentage_columns : Optional[List[str]]
            Specific columns to calculate percentages for
        min_denominator : float
            Minimum denominator to avoid division issues
        **kwargs
            Additional arguments passed to parent
        """
        super().__init__(**kwargs)
        self.percentage_base = percentage_base
        self.percentage_columns = percentage_columns
        self.min_denominator = min_denominator
    
    def get_input_contract(self) -> Type[PopulationEstimatesContract]:
        return PopulationEstimatesContract
    
    def get_output_contract(self) -> Type[PopulationEstimatesContract]:
        return PopulationEstimatesContract
    
    def execute_step(
        self,
        input_data: PopulationEstimatesContract,
        context: ExecutionContext
    ) -> PopulationEstimatesContract:
        """
        Calculate percentage estimates.
        
        Parameters
        ----------
        input_data : PopulationEstimatesContract
            Population estimates
        context : ExecutionContext
            Execution context
            
        Returns
        -------
        PopulationEstimatesContract
            Estimates with percentages added
        """
        try:
            # Determine columns for percentage calculation
            if self.percentage_columns:
                calc_columns = self.percentage_columns
            else:
                calc_columns = input_data.estimate_columns
            
            # Calculate percentages based on specified base
            if self.percentage_base == "total":
                data_with_pct = self._calculate_total_percentages(
                    input_data.data,
                    calc_columns
                )
            elif self.percentage_base == "group":
                data_with_pct = self._calculate_group_percentages(
                    input_data.data,
                    calc_columns
                )
            else:
                data_with_pct = input_data.data
            
            # Create output contract
            output = PopulationEstimatesContract(
                data=data_with_pct,
                estimate_columns=input_data.estimate_columns,
                variance_columns=input_data.variance_columns,
                total_columns=input_data.total_columns,
                estimation_method=input_data.estimation_method,
                confidence_level=input_data.confidence_level,
                degrees_of_freedom=input_data.degrees_of_freedom,
                step_id=self.step_id
            )
            
            # Add metadata
            output.add_processing_metadata("percentage_base", self.percentage_base)
            output.add_processing_metadata("percentage_columns", calc_columns)
            
            return output
            
        except Exception as e:
            self.handle_step_error(e, context)
            raise PipelineException(
                f"Failed to calculate percentages: {e}",
                step_id=self.step_id,
                cause=e
            )
    
    def _calculate_total_percentages(
        self,
        data: LazyFrameWrapper,
        columns: List[str]
    ) -> LazyFrameWrapper:
        """Calculate percentages relative to grand total."""
        pct_exprs = []
        
        for col in columns:
            # Calculate total for denominator
            total_expr = pl.col(col).sum()
            
            # Calculate percentage with safe division
            pct_exprs.append(
                safe_percentage(
                    pl.col(col),
                    total_expr,
                    min_denominator=self.min_denominator
                ).alias(f"{col}_PCT")
            )
        
        return LazyFrameWrapper(data.frame.with_columns(pct_exprs))
    
    def _calculate_group_percentages(
        self,
        data: LazyFrameWrapper,
        columns: List[str]
    ) -> LazyFrameWrapper:
        """Calculate percentages within groups."""
        # This would typically use window functions
        # Simplified implementation for demonstration
        pct_exprs = []
        
        for col in columns:
            # Calculate group total
            group_total = pl.col(col).sum().over(["STATECD", "YEAR"])
            
            # Calculate percentage within group
            pct_exprs.append(
                safe_percentage(
                    pl.col(col),
                    group_total,
                    min_denominator=self.min_denominator
                ).alias(f"{col}_PCT")
            )
        
        return LazyFrameWrapper(data.frame.with_columns(pct_exprs))


class FormatVarianceOutputStep(FormattingStep):
    """
    Format variance and standard error columns for proper display.
    
    This step ensures variance-related columns are properly formatted,
    including handling of missing values, precision, and display format.
    """
    
    def __init__(
        self,
        se_format: str = "standard",
        show_cv_as_percentage: bool = True,
        null_display_value: str = "--",
        precision: int = 4,
        **kwargs
    ):
        """
        Initialize variance output formatting step.
        
        Parameters
        ----------
        se_format : str
            Format for SE display (standard, parentheses, plusminus)
        show_cv_as_percentage : bool
            Whether to show CV as percentage
        null_display_value : str
            Display value for null/missing values
        precision : int
            Number of significant digits
        **kwargs
            Additional arguments passed to parent
        """
        super().__init__(**kwargs)
        self.se_format = se_format
        self.show_cv_as_percentage = show_cv_as_percentage
        self.null_display_value = null_display_value
        self.precision = precision
    
    def get_input_contract(self) -> Type[FormattedOutputContract]:
        return FormattedOutputContract
    
    def get_output_contract(self) -> Type[FormattedOutputContract]:
        return FormattedOutputContract
    
    def execute_step(
        self,
        input_data: FormattedOutputContract,
        context: ExecutionContext
    ) -> FormattedOutputContract:
        """
        Format variance-related columns.
        
        Parameters
        ----------
        input_data : FormattedOutputContract
            Output data to format
        context : ExecutionContext
            Execution context
            
        Returns
        -------
        FormattedOutputContract
            Output with formatted variance columns
        """
        try:
            # Format standard error columns
            formatted_df = self._format_se_columns(input_data.data)
            
            # Format CV columns if present
            formatted_df = self._format_cv_columns(formatted_df)
            
            # Format confidence intervals
            formatted_df = self._format_ci_columns(formatted_df)
            
            # Handle null values
            formatted_df = self._handle_null_values(formatted_df)
            
            # Apply precision formatting
            formatted_df = self._apply_precision(formatted_df)
            
            # Create output contract
            output = FormattedOutputContract(
                data=formatted_df,
                output_metadata=input_data.output_metadata,
                format_version=input_data.format_version,
                column_descriptions=input_data.column_descriptions,
                step_id=self.step_id
            )
            
            # Add formatting metadata
            output.output_metadata["variance_formatting"] = {
                "se_format": self.se_format,
                "cv_as_percentage": self.show_cv_as_percentage,
                "precision": self.precision
            }
            
            return output
            
        except Exception as e:
            self.handle_step_error(e, context)
            raise PipelineException(
                f"Failed to format variance output: {e}",
                step_id=self.step_id,
                cause=e
            )
    
    def _format_se_columns(self, df: pl.DataFrame) -> pl.DataFrame:
        """Format standard error columns based on format style."""
        se_columns = [col for col in df.columns if "_SE" in col]
        
        if not se_columns:
            return df
        
        if self.se_format == "parentheses":
            # Format SE in parentheses
            for se_col in se_columns:
                base_col = se_col.replace("_SE", "")
                if base_col in df.columns:
                    # Combine estimate and SE
                    df = df.with_columns([
                        (pl.col(base_col).cast(str) + " (" + 
                         pl.col(se_col).round(self.precision).cast(str) + ")")
                        .alias(f"{base_col}_WITH_SE")
                    ])
        elif self.se_format == "plusminus":
            # Format as estimate ± SE
            for se_col in se_columns:
                base_col = se_col.replace("_SE", "")
                if base_col in df.columns:
                    df = df.with_columns([
                        (pl.col(base_col).cast(str) + " ± " + 
                         pl.col(se_col).round(self.precision).cast(str))
                        .alias(f"{base_col}_WITH_SE")
                    ])
        
        return df
    
    def _format_cv_columns(self, df: pl.DataFrame) -> pl.DataFrame:
        """Format coefficient of variation columns."""
        cv_columns = [col for col in df.columns if "_CV" in col]
        
        if not cv_columns:
            return df
        
        if self.show_cv_as_percentage:
            # Convert CV to percentage
            for cv_col in cv_columns:
                df = df.with_columns([
                    (pl.col(cv_col) * 100).round(2).alias(cv_col)
                ])
        
        return df
    
    def _format_ci_columns(self, df: pl.DataFrame) -> pl.DataFrame:
        """Format confidence interval columns."""
        ci_lower_cols = [col for col in df.columns if "_CI_LOWER" in col]
        ci_upper_cols = [col for col in df.columns if "_CI_UPPER" in col]
        
        if not ci_lower_cols:
            return df
        
        # Combine CI bounds into single column
        for lower_col in ci_lower_cols:
            upper_col = lower_col.replace("_LOWER", "_UPPER")
            if upper_col in ci_upper_cols:
                base_col = lower_col.replace("_CI_LOWER", "")
                df = df.with_columns([
                    ("[" + pl.col(lower_col).round(self.precision).cast(str) + 
                     ", " + pl.col(upper_col).round(self.precision).cast(str) + "]")
                    .alias(f"{base_col}_CI")
                ])
        
        return df
    
    def _handle_null_values(self, df: pl.DataFrame) -> pl.DataFrame:
        """Replace null values with display value."""
        # Replace nulls in numeric columns
        numeric_cols = [col for col, dtype in zip(df.columns, df.dtypes) 
                       if dtype in [pl.Float32, pl.Float64, pl.Int32, pl.Int64]]
        
        for col in numeric_cols:
            df = df.with_columns([
                pl.col(col).fill_null(self.null_display_value)
            ])
        
        return df
    
    def _apply_precision(self, df: pl.DataFrame) -> pl.DataFrame:
        """Apply precision formatting to numeric columns."""
        # Round all float columns to specified precision
        float_cols = [col for col, dtype in zip(df.columns, df.dtypes)
                     if dtype in [pl.Float32, pl.Float64]]
        
        for col in float_cols:
            df = df.with_columns([
                pl.col(col).round(self.precision)
            ])
        
        return df


# Export all output step classes
__all__ = [
    "CalculatePopulationEstimatesStep",
    "FormatOutputStep",
    "AddTotalsStep",
    "CalculatePercentagesStep",
    "FormatVarianceOutputStep",
]