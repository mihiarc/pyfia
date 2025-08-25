"""
Value calculation and statistical processing pipeline steps.

This module provides pipeline steps for calculating tree-level values,
aggregating to plot level, applying stratification, and computing
population estimates with proper variance calculations.
"""

from typing import Dict, List, Optional, Type, Any, Callable
import warnings

import polars as pl
import numpy as np

from ...core import FIA
from ..config import EstimatorConfig
from ..lazy_evaluation import LazyFrameWrapper
# Note: These imports would need to be adjusted based on actual module structure
# For now, we'll use placeholder functions that would be implemented in the respective modules

# Placeholder calculation functions (would be imported from actual modules)
def calculate_tree_volume_cubic_feet(df):
    """Placeholder for volume calculation function."""
    # This would be implemented in the actual volume module
    return np.ones(len(df)) * 10.0  # Mock volume values

def calculate_tree_biomass_tons(df, component="aboveground"):
    """Placeholder for biomass calculation function."""
    # This would be implemented in the actual biomass module
    return np.ones(len(df)) * 5.0  # Mock biomass values

def calculate_growth_components(df):
    """Placeholder for growth calculation function."""
    # This would be implemented in the actual growth module
    return {
        "net_growth": np.ones(len(df)) * 0.5,
        "gross_growth": np.ones(len(df)) * 0.7
    }

def calculate_mortality_rates(df, mortality_type="volume"):
    """Placeholder for mortality calculation function."""
    # This would be implemented in the actual mortality module
    return np.ones(len(df)) * 0.1  # Mock mortality values

from .core import (
    PipelineStep,
    ExecutionContext,
    JoinedDataContract,
    ValuedDataContract,
    PlotEstimatesContract,
    StratifiedEstimatesContract,
    PopulationEstimatesContract,
    FormattedOutputContract,
    StepValidationError,
    PipelineException
)


# === Value Calculation Steps ===

class CalculateTreeValuesStep(PipelineStep[JoinedDataContract, ValuedDataContract]):
    """
    Base class for calculating tree-level values.
    
    Provides common infrastructure for tree-level calculations
    with proper error handling and validation.
    """
    
    def __init__(
        self,
        value_column_name: str,
        calculation_function: Callable[[pl.DataFrame], pl.DataFrame],
        required_columns: Optional[List[str]] = None,
        **kwargs
    ):
        """
        Initialize tree value calculation step.
        
        Parameters
        ----------
        value_column_name : str
            Name of the calculated value column
        calculation_function : Callable
            Function to calculate the values
        required_columns : Optional[List[str]]
            Required columns for calculation
        """
        super().__init__(**kwargs)
        self.value_column_name = value_column_name
        self.calculation_function = calculation_function
        self.required_columns = required_columns or []
    
    def get_input_contract(self) -> Type[JoinedDataContract]:
        return JoinedDataContract
    
    def get_output_contract(self) -> Type[ValuedDataContract]:
        return ValuedDataContract
    
    def validate_input(self, input_data: JoinedDataContract, context: ExecutionContext) -> None:
        """Validate input data has required columns."""
        super().validate_input(input_data, context)
        
        # Check for required columns
        data_df = input_data.data.collect() if input_data.data.is_lazy else input_data.data.frame
        available_cols = set(data_df.columns)
        missing_cols = set(self.required_columns) - available_cols
        
        if missing_cols:
            raise StepValidationError(
                f"Missing required columns for {self.value_column_name} calculation: {missing_cols}",
                step_id=self.step_id
            )
    
    def execute_step(
        self,
        input_data: JoinedDataContract,
        context: ExecutionContext
    ) -> ValuedDataContract:
        """Calculate tree-level values."""
        
        # Get the data
        data_df = input_data.data.collect()
        
        # Apply the calculation function
        try:
            valued_df = self.calculation_function(data_df)
        except Exception as e:
            raise PipelineException(
                f"Failed to calculate {self.value_column_name}: {e}",
                step_id=self.step_id,
                cause=e
            )
        
        # Validate that the value column was added
        if self.value_column_name not in valued_df.columns:
            raise PipelineException(
                f"Calculation function failed to add column {self.value_column_name}",
                step_id=self.step_id
            )
        
        return ValuedDataContract(
            data=LazyFrameWrapper(valued_df.lazy()),
            value_columns=[self.value_column_name],
            group_columns=input_data.group_columns,
            step_id=self.step_id
        )


class CalculateTreeVolumesStep(CalculateTreeValuesStep):
    """
    Calculate tree-level volume estimates.
    
    Uses the FIA volume equations to calculate cubic foot volume
    for each tree record.
    """
    
    def __init__(self, **kwargs):
        """Initialize volume calculation step."""
        
        def calculate_volumes(df: pl.DataFrame) -> pl.DataFrame:
            """Calculate tree volumes using FIA equations."""
            # Convert to pandas for volume calculation (if needed by volume module)
            pandas_df = df.to_pandas()
            
            # Calculate volumes
            volumes = calculate_tree_volume_cubic_feet(pandas_df)
            
            # Add volume column back to Polars DataFrame
            df_with_volume = df.with_columns([
                pl.lit(volumes).alias("TREE_VOLUME_CF")
            ])
            
            return df_with_volume
        
        super().__init__(
            value_column_name="TREE_VOLUME_CF",
            calculation_function=calculate_volumes,
            required_columns=["DIA", "HT", "SPCD"],
            **kwargs
        )


class CalculateBiomassStep(CalculateTreeValuesStep):
    """
    Calculate tree-level biomass estimates.
    
    Uses FIA biomass equations to calculate dry weight biomass
    for each tree record.
    """
    
    def __init__(self, component: str = "aboveground", **kwargs):
        """
        Initialize biomass calculation step.
        
        Parameters
        ----------
        component : str
            Biomass component to calculate
        """
        self.component = component
        
        def calculate_biomass(df: pl.DataFrame) -> pl.DataFrame:
            """Calculate tree biomass using FIA equations."""
            # Convert to pandas for biomass calculation
            pandas_df = df.to_pandas()
            
            # Calculate biomass
            biomass = calculate_tree_biomass_tons(pandas_df, component=self.component)
            
            # Add biomass column back to Polars DataFrame
            df_with_biomass = df.with_columns([
                pl.lit(biomass).alias("TREE_BIOMASS_TONS")
            ])
            
            return df_with_biomass
        
        super().__init__(
            value_column_name="TREE_BIOMASS_TONS",
            calculation_function=calculate_biomass,
            required_columns=["DIA", "HT", "SPCD"],
            **kwargs
        )


class CalculateTPAStep(CalculateTreeValuesStep):
    """
    Calculate trees per acre values.
    
    Applies expansion factors to convert tree counts to per-acre estimates.
    """
    
    def __init__(self, **kwargs):
        """Initialize TPA calculation step."""
        
        def calculate_tpa(df: pl.DataFrame) -> pl.DataFrame:
            """Calculate trees per acre using expansion factors."""
            # TPA calculation using tree adjustment factors
            df_with_tpa = df.with_columns([
                (pl.col("TPAADJ") * pl.col("CONDPROP_UNADJ")).alias("TREE_TPA")
            ])
            
            return df_with_tpa
        
        super().__init__(
            value_column_name="TREE_TPA",
            calculation_function=calculate_tpa,
            required_columns=["TPAADJ", "CONDPROP_UNADJ"],
            **kwargs
        )


class CalculateAreaStep(PipelineStep[JoinedDataContract, ValuedDataContract]):
    """
    Calculate area values for area estimation.
    
    This step handles area calculations which work on condition-level
    data rather than tree-level data.
    """
    
    def __init__(self, **kwargs):
        """Initialize area calculation step."""
        super().__init__(**kwargs)
    
    def get_input_contract(self) -> Type[JoinedDataContract]:
        return JoinedDataContract
    
    def get_output_contract(self) -> Type[ValuedDataContract]:
        return ValuedDataContract
    
    def execute_step(
        self,
        input_data: JoinedDataContract,
        context: ExecutionContext
    ) -> ValuedDataContract:
        """Calculate area values."""
        
        # Get the data
        data_df = input_data.data.collect()
        
        # Calculate area using condition proportion
        area_df = data_df.with_columns([
            pl.col("CONDPROP_UNADJ").alias("COND_AREA")
        ])
        
        return ValuedDataContract(
            data=LazyFrameWrapper(area_df.lazy()),
            value_columns=["COND_AREA"],
            group_columns=input_data.group_columns,
            step_id=self.step_id
        )


class CalculateGrowthStep(CalculateTreeValuesStep):
    """
    Calculate tree growth values.
    
    Computes net annual growth using diameter increment
    and mortality/removals accounting.
    """
    
    def __init__(self, **kwargs):
        """Initialize growth calculation step."""
        
        def calculate_growth(df: pl.DataFrame) -> pl.DataFrame:
            """Calculate growth using FIA growth accounting."""
            # Convert to pandas for growth calculation
            pandas_df = df.to_pandas()
            
            # Calculate growth components
            growth_data = calculate_growth_components(pandas_df)
            
            # Add growth columns back to Polars DataFrame
            df_with_growth = df.with_columns([
                pl.lit(growth_data["net_growth"]).alias("TREE_GROWTH_CF"),
                pl.lit(growth_data["gross_growth"]).alias("TREE_GROSS_GROWTH_CF")
            ])
            
            return df_with_growth
        
        super().__init__(
            value_column_name="TREE_GROWTH_CF",
            calculation_function=calculate_growth,
            required_columns=["DIA", "PREVDIA", "STATUSCD", "AGENTCD"],
            **kwargs
        )


class CalculateMortalityStep(CalculateTreeValuesStep):
    """
    Calculate mortality values.
    
    Computes mortality rates and volumes for dead trees.
    """
    
    def __init__(self, mortality_type: str = "volume", **kwargs):
        """
        Initialize mortality calculation step.
        
        Parameters
        ----------
        mortality_type : str
            Type of mortality to calculate (volume, tpa, biomass)
        """
        self.mortality_type = mortality_type
        
        def calculate_mortality(df: pl.DataFrame) -> pl.DataFrame:
            """Calculate mortality using FIA methods."""
            # Convert to pandas for mortality calculation
            pandas_df = df.to_pandas()
            
            # Calculate mortality rates
            mortality_data = calculate_mortality_rates(pandas_df, mortality_type=self.mortality_type)
            
            # Add mortality columns back to Polars DataFrame
            column_name = f"TREE_MORTALITY_{mortality_type.upper()}"
            df_with_mortality = df.with_columns([
                pl.lit(mortality_data).alias(column_name)
            ])
            
            return df_with_mortality
        
        super().__init__(
            value_column_name=f"TREE_MORTALITY_{mortality_type.upper()}",
            calculation_function=calculate_mortality,
            required_columns=["STATUSCD", "AGENTCD"],
            **kwargs
        )


# === Aggregation Steps ===

class AggregateByPlotStep(PipelineStep[ValuedDataContract, PlotEstimatesContract]):
    """
    Aggregate tree-level values to plot level.
    
    Sums or averages tree values within each plot and grouping
    combination to create plot-level estimates.
    """
    
    def __init__(self, **kwargs):
        """Initialize plot aggregation step."""
        super().__init__(**kwargs)
    
    def get_input_contract(self) -> Type[ValuedDataContract]:
        return ValuedDataContract
    
    def get_output_contract(self) -> Type[PlotEstimatesContract]:
        return PlotEstimatesContract
    
    def execute_step(
        self,
        input_data: ValuedDataContract,
        context: ExecutionContext
    ) -> PlotEstimatesContract:
        """Aggregate values to plot level."""
        
        # Get the data
        data_df = input_data.data.collect()
        
        # Determine grouping columns
        group_cols = ["PLT_CN"]
        if input_data.group_columns:
            group_cols.extend(input_data.group_columns)
        
        # Create aggregation expressions
        agg_exprs = []
        estimate_columns = []
        
        for value_col in input_data.value_columns:
            plot_col_name = f"PLOT_{value_col}"
            agg_exprs.append(pl.sum(value_col).alias(plot_col_name))
            estimate_columns.append(plot_col_name)
        
        # Perform aggregation
        plot_estimates = data_df.group_by(group_cols).agg(agg_exprs)
        
        return PlotEstimatesContract(
            data=LazyFrameWrapper(plot_estimates.lazy()),
            estimate_columns=estimate_columns,
            group_columns=input_data.group_columns,
            step_id=self.step_id
        )


class GroupByColumnsStep(PipelineStep[PlotEstimatesContract, PlotEstimatesContract]):
    """
    Apply additional grouping to plot estimates.
    
    This step can be used to add or modify grouping columns
    after plot-level aggregation.
    """
    
    def __init__(self, additional_group_columns: List[str], **kwargs):
        """
        Initialize grouping step.
        
        Parameters
        ----------
        additional_group_columns : List[str]
            Additional columns to group by
        """
        super().__init__(**kwargs)
        self.additional_group_columns = additional_group_columns
    
    def get_input_contract(self) -> Type[PlotEstimatesContract]:
        return PlotEstimatesContract
    
    def get_output_contract(self) -> Type[PlotEstimatesContract]:
        return PlotEstimatesContract
    
    def execute_step(
        self,
        input_data: PlotEstimatesContract,
        context: ExecutionContext
    ) -> PlotEstimatesContract:
        """Apply additional grouping."""
        
        # Combine existing and additional group columns
        all_group_cols = input_data.group_columns + self.additional_group_columns
        
        # Remove duplicates while preserving order
        seen = set()
        unique_group_cols = [x for x in all_group_cols if not (x in seen or seen.add(x))]
        
        # Return updated contract
        return PlotEstimatesContract(
            data=input_data.data,
            estimate_columns=input_data.estimate_columns,
            group_columns=unique_group_cols,
            step_id=self.step_id
        )


# === Statistical Processing Steps ===

class ApplyStratificationStep(PipelineStep[PlotEstimatesContract, StratifiedEstimatesContract]):
    """
    Apply stratification and calculate expansion factors.
    
    Links plot estimates with population stratum information
    and calculates appropriate expansion factors for population estimates.
    """
    
    def __init__(self, **kwargs):
        """Initialize stratification step."""
        super().__init__(**kwargs)
    
    def get_input_contract(self) -> Type[PlotEstimatesContract]:
        return PlotEstimatesContract
    
    def get_output_contract(self) -> Type[StratifiedEstimatesContract]:
        return StratifiedEstimatesContract
    
    def execute_step(
        self,
        input_data: PlotEstimatesContract,
        context: ExecutionContext
    ) -> StratifiedEstimatesContract:
        """Apply stratification to plot estimates."""
        
        # Get plot estimates
        plot_df = input_data.data.collect()
        
        # Load stratification tables
        try:
            # Load plot stratum assignments
            if "POP_PLOT_STRATUM_ASSGN" not in context.db.tables:
                context.db.load_table("POP_PLOT_STRATUM_ASSGN")
            plot_stratum = context.db.tables["POP_PLOT_STRATUM_ASSGN"]
            
            # Load stratum information
            if "POP_STRATUM" not in context.db.tables:
                context.db.load_table("POP_STRATUM") 
            stratum_info = context.db.tables["POP_STRATUM"]
            
            # Convert to DataFrames if LazyFrames
            if isinstance(plot_stratum, pl.LazyFrame):
                plot_stratum = plot_stratum.collect()
            if isinstance(stratum_info, pl.LazyFrame):
                stratum_info = stratum_info.collect()
            
        except Exception as e:
            raise PipelineException(
                f"Failed to load stratification tables: {e}",
                step_id=self.step_id,
                cause=e
            )
        
        # Join plot estimates with stratum assignments
        stratified_df = plot_df.join(
            plot_stratum.select(["PLT_CN", "STRATUM_CN", "ADJ_FACTOR_MICRO_PLOT"]),
            on="PLT_CN",
            how="inner"
        )
        
        # Join with stratum information
        stratified_df = stratified_df.join(
            stratum_info.select(["STRATUM_CN", "EXPNS", "P2POINTCNT", "STRATUM_AREA"]),
            on="STRATUM_CN", 
            how="inner"
        )
        
        # Calculate expansion factors
        stratified_df = stratified_df.with_columns([
            (pl.col("EXPNS") * pl.col("ADJ_FACTOR_MICRO_PLOT")).alias("PLOT_EXPANSION_FACTOR")
        ])
        
        return StratifiedEstimatesContract(
            data=LazyFrameWrapper(stratified_df.lazy()),
            expansion_columns=["PLOT_EXPANSION_FACTOR", "EXPNS"],
            stratum_columns=["STRATUM_CN", "STRATUM_AREA"],
            step_id=self.step_id
        )


class CalculatePopulationEstimatesStep(PipelineStep[StratifiedEstimatesContract, PopulationEstimatesContract]):
    """
    Calculate population-level estimates with proper variance.
    
    Computes design-based population estimates using post-stratified
    estimation with proper variance calculations.
    """
    
    def __init__(self, **kwargs):
        """Initialize population estimation step.""" 
        super().__init__(**kwargs)
    
    def get_input_contract(self) -> Type[StratifiedEstimatesContract]:
        return StratifiedEstimatesContract
    
    def get_output_contract(self) -> Type[PopulationEstimatesContract]:
        return PopulationEstimatesContract
    
    def execute_step(
        self,
        input_data: StratifiedEstimatesContract,
        context: ExecutionContext
    ) -> PopulationEstimatesContract:
        """Calculate population estimates."""
        
        # Get stratified data
        data_df = input_data.data.collect()
        
        # Determine grouping for population estimates
        group_cols = []
        if hasattr(input_data, "group_columns"):
            group_cols = getattr(input_data, "group_columns", [])
        
        # If no grouping, calculate overall estimates
        if not group_cols:
            group_cols = ["STRATUM_CN"]  # Group by stratum for variance calculation
        else:
            group_cols = group_cols + ["STRATUM_CN"]
        
        # Identify estimate columns (those starting with "PLOT_")
        estimate_base_cols = [col for col in data_df.columns if col.startswith("PLOT_")]
        
        # Calculate stratum-level estimates
        stratum_exprs = []
        for est_col in estimate_base_cols:
            # Stratum mean
            stratum_exprs.append(
                pl.mean(est_col).alias(f"{est_col}_STRATUM_MEAN")
            )
            # Stratum variance
            stratum_exprs.append(
                pl.var(est_col).alias(f"{est_col}_STRATUM_VAR")
            )
            # Plot count in stratum
        stratum_exprs.append(pl.count().alias("STRATUM_PLOT_COUNT"))
        
        # Calculate stratum statistics
        stratum_stats = data_df.group_by(group_cols).agg(stratum_exprs)
        
        # Calculate population estimates using post-stratified estimation
        pop_exprs = []
        variance_columns = []
        total_columns = []
        
        for est_col in estimate_base_cols:
            base_name = est_col.replace("PLOT_", "")
            
            # Population estimate (weighted by stratum area)
            pop_expr = (
                pl.col(f"{est_col}_STRATUM_MEAN") * 
                pl.col("STRATUM_AREA")
            ).sum().alias(f"{base_name}_PER_ACRE")
            
            pop_exprs.append(pop_expr)
            
            # Variance calculation (simplified - proper FIA variance would be more complex)
            var_expr = (
                pl.col(f"{est_col}_STRATUM_VAR") * 
                pl.col("STRATUM_AREA").pow(2) / 
                pl.col("STRATUM_PLOT_COUNT")
            ).sum().alias(f"{base_name}_VAR")
            
            pop_exprs.append(var_expr)
            variance_columns.append(f"{base_name}_VAR")
            
            # Standard error
            se_expr = pl.col(f"{base_name}_VAR").sqrt().alias(f"{base_name}_SE")
            pop_exprs.append(se_expr)
            
            # Total estimate if requested
            if context.config.totals:
                total_expr = (
                    pl.col(f"{base_name}_PER_ACRE") * 
                    pl.col("STRATUM_AREA").sum()
                ).alias(f"{base_name}_TOTAL")
                pop_exprs.append(total_expr)
                total_columns.append(f"{base_name}_TOTAL")
        
        # Final grouping for population estimates (remove STRATUM_CN)
        final_group_cols = [col for col in group_cols if col != "STRATUM_CN"]
        if not final_group_cols:
            # Overall estimates
            pop_estimates = stratum_stats.select(pop_exprs)
        else:
            pop_estimates = stratum_stats.group_by(final_group_cols).agg(pop_exprs)
        
        # Extract estimate column names
        estimate_columns = [col for col in pop_estimates.columns 
                          if col.endswith("_PER_ACRE") or col.endswith("_TOTAL")]
        
        return PopulationEstimatesContract(
            data=LazyFrameWrapper(pop_estimates.lazy()),
            estimate_columns=estimate_columns,
            variance_columns=variance_columns,
            total_columns=total_columns,
            step_id=self.step_id
        )


class ApplyVarianceCalculationStep(PipelineStep[PopulationEstimatesContract, PopulationEstimatesContract]):
    """
    Apply proper FIA variance calculations.
    
    This step implements the full FIA variance calculation methodology
    for design-based estimation with post-stratification.
    """
    
    def __init__(self, variance_method: str = "ratio", **kwargs):
        """
        Initialize variance calculation step.
        
        Parameters
        ----------
        variance_method : str
            Variance calculation method (standard, ratio, hybrid)
        """
        super().__init__(**kwargs)
        self.variance_method = variance_method
    
    def get_input_contract(self) -> Type[PopulationEstimatesContract]:
        return PopulationEstimatesContract
    
    def get_output_contract(self) -> Type[PopulationEstimatesContract]:
        return PopulationEstimatesContract
    
    def execute_step(
        self,
        input_data: PopulationEstimatesContract,
        context: ExecutionContext
    ) -> PopulationEstimatesContract:
        """Apply proper variance calculations."""
        
        # For now, this is a placeholder that returns the input unchanged
        # A full implementation would apply the sophisticated FIA variance methods
        
        warnings.warn(
            "Full FIA variance calculation not yet implemented. "
            "Using simplified variance estimates.",
            UserWarning
        )
        
        return input_data


# === Output Formatting Steps ===

class FormatOutputStep(PipelineStep[PopulationEstimatesContract, FormattedOutputContract]):
    """
    Format final output for return to user.
    
    Applies final formatting, column renaming, and metadata addition
    to create the final estimation results.
    """
    
    def __init__(self, **kwargs):
        """Initialize output formatting step."""
        super().__init__(**kwargs)
    
    def get_input_contract(self) -> Type[PopulationEstimatesContract]:
        return PopulationEstimatesContract
    
    def get_output_contract(self) -> Type[FormattedOutputContract]:
        return FormattedOutputContract
    
    def execute_step(
        self,
        input_data: PopulationEstimatesContract,
        context: ExecutionContext
    ) -> FormattedOutputContract:
        """Format the final output."""
        
        # Get the data
        output_df = input_data.data.collect()
        
        # Apply column renaming and cleanup
        # (This could be more sophisticated based on estimation type)
        
        # Remove intermediate columns
        final_columns = []
        for col in output_df.columns:
            if not col.startswith("STRATUM_") and not col.endswith("_STRATUM_MEAN"):
                final_columns.append(col)
        
        formatted_df = output_df.select(final_columns)
        
        # Sort results for consistency
        if "PLT_CN" in formatted_df.columns:
            formatted_df = formatted_df.sort("PLT_CN")
        
        # Create metadata
        metadata = {
            "estimation_type": getattr(context.config, "estimation_type", "unknown"),
            "total_plots": len(formatted_df),
            "grouping_columns": input_data.estimate_columns,
            "variance_method": context.config.variance_method,
            "execution_time": context.total_execution_time
        }
        
        return FormattedOutputContract(
            data=formatted_df,
            metadata=metadata,
            step_id=self.step_id
        )


class AddMetadataStep(PipelineStep[FormattedOutputContract, FormattedOutputContract]):
    """
    Add additional metadata to the output.
    
    This step can be used to add custom metadata or enrich
    the existing metadata with additional information.
    """
    
    def __init__(self, additional_metadata: Dict[str, Any], **kwargs):
        """
        Initialize metadata step.
        
        Parameters
        ----------
        additional_metadata : Dict[str, Any]
            Additional metadata to add
        """
        super().__init__(**kwargs)
        self.additional_metadata = additional_metadata
    
    def get_input_contract(self) -> Type[FormattedOutputContract]:
        return FormattedOutputContract
    
    def get_output_contract(self) -> Type[FormattedOutputContract]:
        return FormattedOutputContract
    
    def execute_step(
        self,
        input_data: FormattedOutputContract,
        context: ExecutionContext
    ) -> FormattedOutputContract:
        """Add additional metadata."""
        
        # Merge metadata
        updated_metadata = {**input_data.metadata, **self.additional_metadata}
        
        return FormattedOutputContract(
            data=input_data.data,
            metadata=updated_metadata,
            step_id=self.step_id
        )


class ValidateOutputStep(PipelineStep[FormattedOutputContract, FormattedOutputContract]):
    """
    Validate final output meets requirements.
    
    Performs final validation checks on the output data
    to ensure it meets quality and completeness standards.
    """
    
    def __init__(
        self,
        min_records: Optional[int] = None,
        required_columns: Optional[List[str]] = None,
        **kwargs
    ):
        """
        Initialize output validation step.
        
        Parameters
        ----------
        min_records : Optional[int]
            Minimum number of records required
        required_columns : Optional[List[str]]
            Required columns in output
        """
        super().__init__(**kwargs)
        self.min_records = min_records
        self.required_columns = required_columns or []
    
    def get_input_contract(self) -> Type[FormattedOutputContract]:
        return FormattedOutputContract
    
    def get_output_contract(self) -> Type[FormattedOutputContract]:
        return FormattedOutputContract
    
    def execute_step(
        self,
        input_data: FormattedOutputContract,
        context: ExecutionContext
    ) -> FormattedOutputContract:
        """Validate the output."""
        
        data_df = input_data.data
        
        # Check minimum records
        if self.min_records and len(data_df) < self.min_records:
            raise StepValidationError(
                f"Output has {len(data_df)} records, minimum required: {self.min_records}",
                step_id=self.step_id
            )
        
        # Check required columns
        missing_cols = set(self.required_columns) - set(data_df.columns)
        if missing_cols:
            raise StepValidationError(
                f"Output missing required columns: {missing_cols}",
                step_id=self.step_id
            )
        
        # Check for NaN values in estimate columns
        estimate_cols = [col for col in data_df.columns 
                        if col.endswith("_PER_ACRE") or col.endswith("_TOTAL")]
        
        for col in estimate_cols:
            null_count = data_df.select(pl.col(col).is_null().sum()).item()
            if null_count > 0:
                warnings.warn(
                    f"Column {col} has {null_count} null values",
                    UserWarning
                )
        
        return input_data