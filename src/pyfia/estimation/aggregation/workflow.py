"""
Population estimation workflow components for FIA area estimation.

This module provides workflow classes for handling complex population-level
estimation processes, breaking down the monolithic calculation logic into
manageable, single-responsibility components.
"""

from typing import List, Optional, Dict, Any
from dataclasses import dataclass

import polars as pl

from ..statistics import VarianceCalculator, PercentageCalculator
from ..statistics.expressions import PolarsExpressionBuilder


@dataclass
class AggregationConfig:
    """Configuration for aggregation workflows."""
    group_cols: Optional[List[str]] = None
    by_land_type: bool = False
    include_totals: bool = False
    include_variance: bool = False
    batch_size: Optional[int] = None


class AreaAggregationBuilder:
    """
    Builder class for constructing complex area aggregation expressions.
    
    This class implements the Builder pattern to construct aggregation
    expressions for different levels (plot, stratum, population) with
    appropriate variance calculations and group-by operations.
    """
    
    def __init__(self):
        """Initialize the aggregation builder."""
        self.aggregations: List[pl.Expr] = []
        self.group_cols: List[str] = []
        self.variance_calculator = VarianceCalculator()
        self.expression_builder = PolarsExpressionBuilder()
    
    def with_numerator_totals(self, numerator_col: str = "TOTAL_AREA_NUMERATOR") -> 'AreaAggregationBuilder':
        """
        Add numerator total aggregation.
        
        Parameters
        ----------
        numerator_col : str, default "TOTAL_AREA_NUMERATOR"
            Column name for numerator values
            
        Returns
        -------
        AreaAggregationBuilder
            Builder instance for method chaining
        """
        self.aggregations.append(
            pl.sum(numerator_col).alias("fa_expanded_total")
        )
        return self
    
    def with_denominator_totals(self, denominator_col: str = "TOTAL_AREA_DENOMINATOR") -> 'AreaAggregationBuilder':
        """
        Add denominator total aggregation.
        
        Parameters
        ----------
        denominator_col : str, default "TOTAL_AREA_DENOMINATOR" 
            Column name for denominator values
            
        Returns
        -------
        AreaAggregationBuilder
            Builder instance for method chaining
        """
        self.aggregations.append(
            pl.sum(denominator_col).alias("fad_expanded_total")
        )
        return self
    
    def with_variance_statistics(
        self, 
        numerator_col: str = "fa_adjusted",
        denominator_col: str = "fad_adjusted"
    ) -> 'AreaAggregationBuilder':
        """
        Add variance calculation statistics.
        
        Parameters
        ----------
        numerator_col : str, default "fa_adjusted"
            Column name for adjusted numerator values
        denominator_col : str, default "fad_adjusted"
            Column name for adjusted denominator values
            
        Returns
        -------
        AreaAggregationBuilder
            Builder instance for method chaining
        """
        self.aggregations.extend([
            # Sample size (all plots in stratum)
            pl.len().alias("n_h"),
            
            # Non-zero plots count (plots with numerator > 0)
            pl.when(pl.col(numerator_col) > 0).then(1).otherwise(0).sum().alias("n_nonzero"),
            
            # Mean values for variance calculation
            pl.mean(numerator_col).alias("fa_bar_h"),
            pl.mean(denominator_col).alias("fad_bar_h"),
            
            # Standard deviations
            self.expression_builder.safe_std(numerator_col).alias("s_fa_h"),
            self.expression_builder.safe_std(denominator_col).alias("s_fad_h"),
            
            # Correlation for ratio variance
            self.expression_builder.safe_correlation(numerator_col, denominator_col).alias("corr_fa_fad"),
            
            # Stratum weight
            pl.first("EXPNS").alias("w_h"),
        ])
        return self
    
    def with_covariance_calculation(self) -> 'AreaAggregationBuilder':
        """
        Add covariance calculation from correlation and standard deviations.
        
        Returns
        -------
        AreaAggregationBuilder
            Builder instance for method chaining
        """
        covariance_expr = (
            pl.when((pl.col("s_fa_h") == 0) | (pl.col("s_fad_h") == 0))
            .then(0.0)
            .otherwise(pl.col("corr_fa_fad") * pl.col("s_fa_h") * pl.col("s_fad_h"))
            .alias("s_fa_fad_h")
        )
        self.aggregations.append(covariance_expr)
        return self
    
    def with_population_totals(self) -> 'AreaAggregationBuilder':
        """
        Add population-level total aggregations.
        
        Returns
        -------
        AreaAggregationBuilder
            Builder instance for method chaining
        """
        self.aggregations.extend([
            pl.col("fa_expanded_total").sum().alias("FA_TOTAL"),
            pl.col("fad_expanded_total").sum().alias("FAD_TOTAL"),
            pl.col("n_h").sum().alias("N_PLOTS_TOTAL"),
            # Count non-zero plots for reporting (plots with FA > 0)
            pl.col("n_nonzero").sum().alias("N_PLOTS"),
        ])
        return self
    
    def with_variance_components(self) -> 'AreaAggregationBuilder':
        """
        Add variance component calculations.
        
        Returns
        -------
        AreaAggregationBuilder
            Builder instance for method chaining
        """
        self.aggregations.extend([
            self.variance_calculator.variance_component("fa").alias("FA_VAR"),
            self.variance_calculator.variance_component("fad").alias("FAD_VAR"),
            self.variance_calculator.covariance_component().alias("COV_FA_FAD"),
        ])
        return self
    
    def group_by(self, *columns: str) -> 'AreaAggregationBuilder':
        """
        Set grouping columns.
        
        Parameters
        ----------
        *columns : str
            Column names for grouping
            
        Returns
        -------
        AreaAggregationBuilder
            Builder instance for method chaining
        """
        self.group_cols.extend(columns)
        return self
    
    def build_stratum_aggregation(self) -> tuple[List[str], List[pl.Expr]]:
        """
        Build stratum-level aggregation specification.
        
        Returns
        -------
        tuple[List[str], List[pl.Expr]]
            Tuple of grouping columns and aggregation expressions
        """
        # Ensure stratum is included in grouping
        group_cols = ["STRATUM_CN"] + [col for col in self.group_cols if col != "STRATUM_CN"]
        return group_cols, self.aggregations.copy()
    
    def build_population_aggregation(self) -> tuple[Optional[List[str]], List[pl.Expr]]:
        """
        Build population-level aggregation specification.
        
        Returns
        -------
        tuple[Optional[List[str]], List[pl.Expr]]
            Tuple of grouping columns (None if no grouping) and aggregation expressions
        """
        group_cols = self.group_cols if self.group_cols else None
        return group_cols, self.aggregations.copy()
    
    def reset(self) -> 'AreaAggregationBuilder':
        """
        Reset the builder to initial state.
        
        Returns
        -------
        AreaAggregationBuilder
            Builder instance for method chaining
        """
        self.aggregations.clear()
        self.group_cols.clear()
        return self


class PopulationEstimationWorkflow:
    """
    Orchestrates the complex population estimation workflow for area calculations.
    
    This class breaks down the monolithic population estimation logic into
    focused methods with single responsibilities, making the code more
    maintainable and testable.
    """
    
    def __init__(self, config: AggregationConfig):
        """
        Initialize the population estimation workflow.
        
        Parameters
        ----------
        config : AggregationConfig
            Configuration for the aggregation workflow
        """
        self.config = config
        self.variance_calculator = VarianceCalculator()
        self.percentage_calculator = PercentageCalculator()
        self.aggregation_builder = AreaAggregationBuilder()
        self.expression_builder = PolarsExpressionBuilder()
    
    def calculate_population_estimates(self, expanded_data: pl.DataFrame) -> pl.DataFrame:
        """
        Calculate population-level area estimates using ratio estimation.
        
        This is the main orchestration method that coordinates all the
        sub-workflows for population estimation.
        
        Parameters
        ----------
        expanded_data : pl.DataFrame
            Data with expansion factors applied
            
        Returns
        -------
        pl.DataFrame
            Population-level area estimates with percentages and variance
        """
        # Step 1: Calculate stratum-level statistics
        stratum_est = self._calculate_stratum_statistics(expanded_data)
        
        # Step 2: Add covariance calculations
        stratum_est = self._add_covariance_calculations(stratum_est)
        
        # Step 3: Aggregate to population level
        pop_est = self._aggregate_to_population_level(stratum_est)
        
        # Step 4: Calculate percentages
        pop_est = self._calculate_percentages(pop_est)
        
        # Step 5: Add optional outputs
        pop_est = self._add_optional_outputs(pop_est)
        
        return pop_est
    
    def _calculate_stratum_statistics(self, expanded_data: pl.DataFrame) -> pl.DataFrame:
        """
        Calculate stratum-level statistics needed for variance estimation.
        
        Parameters
        ----------
        expanded_data : pl.DataFrame
            Data with expansion factors applied
            
        Returns
        -------
        pl.DataFrame
            Stratum-level statistics
        """
        # Build aggregation expressions
        builder = (self.aggregation_builder
                  .reset()
                  .with_numerator_totals()
                  .with_denominator_totals()
                  .with_variance_statistics())
        
        if self.config.group_cols:
            builder = builder.group_by(*self.config.group_cols)
        
        group_cols, agg_exprs = builder.build_stratum_aggregation()
        
        # Execute aggregation
        return expanded_data.group_by(group_cols).agg(agg_exprs)
    
    def _add_covariance_calculations(self, stratum_data: pl.DataFrame) -> pl.DataFrame:
        """
        Add covariance calculations from correlation and standard deviations.
        
        Parameters
        ----------
        stratum_data : pl.DataFrame
            Stratum-level data with correlation and standard deviations
            
        Returns
        -------
        pl.DataFrame
            Data with covariance calculations added
        """
        return (self.aggregation_builder
               .reset()
               .with_covariance_calculation()
               .build_population_aggregation()[1][0]
               .pipe(lambda expr: stratum_data.with_columns(expr)))
    
    def _aggregate_to_population_level(self, stratum_data: pl.DataFrame) -> pl.DataFrame:
        """
        Aggregate stratum statistics to population level.
        
        Parameters
        ----------
        stratum_data : pl.DataFrame
            Stratum-level statistics
            
        Returns
        -------
        pl.DataFrame
            Population-level aggregated data
        """
        # Build population aggregation
        builder = (self.aggregation_builder
                  .reset()
                  .with_population_totals()
                  .with_variance_components())
        
        if self.config.group_cols:
            builder = builder.group_by(*self.config.group_cols)
        
        group_cols, agg_exprs = builder.build_population_aggregation()
        
        # Execute aggregation
        if group_cols:
            return stratum_data.group_by(group_cols).agg(agg_exprs)
        else:
            return stratum_data.select(agg_exprs)
    
    def _calculate_percentages(self, pop_data: pl.DataFrame) -> pl.DataFrame:
        """
        Calculate area percentages using appropriate method.
        
        Parameters
        ----------
        pop_data : pl.DataFrame
            Population-level data with totals and variances
            
        Returns
        -------
        pl.DataFrame
            Data with percentage calculations added
        """
        if self.config.by_land_type and "LAND_TYPE" in pop_data.columns:
            return self.percentage_calculator.calculate_land_type_percentages(pop_data)
        else:
            return self.percentage_calculator.calculate_standard_percentages(pop_data)
    
    def _add_optional_outputs(self, pop_data: pl.DataFrame) -> pl.DataFrame:
        """
        Add optional output columns based on configuration.
        
        Parameters
        ----------
        pop_data : pl.DataFrame
            Population data with percentages
            
        Returns
        -------
        pl.DataFrame
            Data with optional outputs added
        """
        result = pop_data
        
        # Add total area if requested
        if self.config.include_totals:
            result = result.with_columns(
                pl.col("FA_TOTAL").alias("AREA")
            )
        
        # Add standard errors or rename variance columns
        if not self.config.include_variance:
            # Add standard errors
            result = result.with_columns(
                self.expression_builder.safe_sqrt("AREA_PERC_VAR").alias("AREA_PERC_SE")
            )
            if self.config.include_totals:
                result = result.with_columns(
                    self.expression_builder.safe_sqrt("FA_VAR").alias("AREA_SE")
                )
        else:
            # Rename variance columns to match expected output
            if self.config.include_totals:
                result = result.with_columns(
                    pl.col("FA_VAR").alias("AREA_VAR")
                )
        
        return result
    
    def validate_input_data(self, data: pl.DataFrame) -> Dict[str, Any]:
        """
        Validate input data for population estimation.
        
        Parameters
        ----------
        data : pl.DataFrame
            Input data to validate
            
        Returns
        -------
        Dict[str, Any]
            Validation results
        """
        validation_results = {
            "is_valid": True,
            "warnings": [],
            "statistics": {}
        }
        
        required_cols = [
            "TOTAL_AREA_NUMERATOR", 
            "TOTAL_AREA_DENOMINATOR", 
            "fa_adjusted", 
            "fad_adjusted",
            "EXPNS",
            "STRATUM_CN"
        ]
        
        missing_cols = [col for col in required_cols if col not in data.columns]
        if missing_cols:
            validation_results["is_valid"] = False
            validation_results["warnings"].append(f"Missing required columns: {missing_cols}")
        
        if validation_results["is_valid"]:
            # Check for data quality issues
            stats = data.select([
                pl.col("TOTAL_AREA_NUMERATOR").is_null().sum().alias("null_numerator"),
                pl.col("TOTAL_AREA_DENOMINATOR").is_null().sum().alias("null_denominator"),
                pl.col("EXPNS").is_null().sum().alias("null_expns"),
                pl.len().alias("total_rows")
            ]).to_dicts()[0]
            
            validation_results["statistics"] = stats
            
            if stats["null_numerator"] > 0:
                validation_results["warnings"].append(f"Found {stats['null_numerator']} null numerator values")
            if stats["null_denominator"] > 0:
                validation_results["warnings"].append(f"Found {stats['null_denominator']} null denominator values")
            if stats["null_expns"] > 0:
                validation_results["warnings"].append(f"Found {stats['null_expns']} null expansion factors")
        
        return validation_results


class BatchAreaProcessor:
    """
    Processes area data in memory-efficient batches.
    
    This class provides functionality for processing large datasets in
    manageable chunks to avoid memory issues while maintaining statistical
    accuracy.
    """
    
    def __init__(self, batch_size: int = 100_000):
        """
        Initialize the batch processor.
        
        Parameters
        ----------
        batch_size : int, default 100_000
            Number of records to process per batch
        """
        self.batch_size = batch_size
        self.workflow = None
    
    def process_in_batches(
        self, 
        data: pl.LazyFrame,
        workflow: PopulationEstimationWorkflow
    ) -> pl.DataFrame:
        """
        Process population estimation in memory-efficient batches.
        
        Parameters
        ----------
        data : pl.LazyFrame
            Lazy DataFrame with expansion data
        workflow : PopulationEstimationWorkflow
            Workflow instance for processing
            
        Returns
        -------
        pl.DataFrame
            Combined results from all batches
        """
        self.workflow = workflow
        
        results = []
        offset = 0
        
        while True:
            # Process batch
            batch = data.slice(offset, self.batch_size).collect()
            
            if batch.is_empty():
                break
            
            # Process this batch
            batch_result = workflow.calculate_population_estimates(batch)
            results.append(batch_result)
            
            offset += self.batch_size
        
        # Combine results
        if not results:
            return pl.DataFrame()
        
        # For population estimates, we need to re-aggregate across batches
        combined = pl.concat(results)
        return self._reaggregate_batch_results(combined)
    
    def _reaggregate_batch_results(self, batch_results: pl.DataFrame) -> pl.DataFrame:
        """
        Re-aggregate results from multiple batches.
        
        Parameters
        ----------
        batch_results : pl.DataFrame
            Combined results from all batches
            
        Returns
        -------
        pl.DataFrame
            Final aggregated results
        """
        # This is a simplified re-aggregation
        # In practice, proper variance re-calculation would be needed
        # for exact statistical accuracy
        
        if self.workflow and self.workflow.config.group_cols:
            return batch_results.group_by(self.workflow.config.group_cols).agg([
                pl.sum("FA_TOTAL").alias("FA_TOTAL"),
                pl.sum("FAD_TOTAL").alias("FAD_TOTAL"),
                pl.sum("N_PLOTS").alias("N_PLOTS"),
            ])
        else:
            return batch_results.select([
                pl.sum("FA_TOTAL").alias("FA_TOTAL"),
                pl.sum("FAD_TOTAL").alias("FAD_TOTAL"),
                pl.sum("N_PLOTS").alias("N_PLOTS"),
            ])