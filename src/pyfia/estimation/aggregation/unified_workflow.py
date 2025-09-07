"""
Unified population estimation workflow for all FIA estimation types.

This module provides a single workflow class that can handle population estimation
for any FIA estimation type (area, volume, biomass, TPA, mortality, growth) using
a configurable, strategy-based approach.
"""

from typing import Any, Dict

import polars as pl

from ..statistics import PercentageCalculator, VarianceCalculator
from ..statistics.expressions import PolarsExpressionBuilder
from .interfaces import EstimationType, IEstimationWorkflow, UnifiedAggregationConfig
from .unified_builder import UnifiedAggregationBuilder


class UnifiedEstimationWorkflow(IEstimationWorkflow):
    """
    Unified workflow for population estimation across all FIA estimation types.
    
    This class consolidates the population estimation logic that was previously
    scattered across different estimator classes, providing a single, well-tested
    implementation that handles all estimation types through configuration.
    """

    def __init__(self, config: UnifiedAggregationConfig):
        """
        Initialize the unified estimation workflow.
        
        Parameters
        ----------
        config : UnifiedAggregationConfig
            Configuration specifying the estimation type and parameters
        """
        self.config = config
        self.variance_calculator = VarianceCalculator()
        self.percentage_calculator = PercentageCalculator()
        self.aggregation_builder = UnifiedAggregationBuilder(config)
        self.expression_builder = PolarsExpressionBuilder()

    def calculate_population_estimates(self, expanded_data: pl.DataFrame) -> pl.DataFrame:
        """
        Calculate population-level estimates using the configured strategy.
        
        This is the main orchestration method that coordinates all the
        sub-workflows for population estimation across all estimation types.
        
        Parameters
        ----------
        expanded_data : pl.DataFrame
            Data with expansion factors applied
            
        Returns
        -------
        pl.DataFrame
            Population-level estimates with percentages and variance
        """
        # Step 1: Calculate stratum-level statistics
        stratum_est = self._calculate_stratum_statistics(expanded_data)

        # Step 2: Add covariance calculations
        stratum_est = self._add_covariance_calculations(stratum_est)

        # Step 3: Aggregate to population level
        pop_est = self._aggregate_to_population_level(stratum_est)

        # Step 4: Calculate response-specific values (per-acre, percentages, etc.)
        pop_est = self._calculate_response_values(pop_est)

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
        # Determine appropriate columns based on estimation type
        numerator_col, denominator_col = self._get_adjusted_columns()

        # Build aggregation expressions
        builder = (self.aggregation_builder
                  .reset()
                  .with_totals(
                      self._get_numerator_column(),
                      self._get_denominator_column()
                  )
                  .with_variance_statistics(
                      numerator_col=numerator_col,
                      denominator_col=denominator_col
                  ))

        # Add response variable aggregations if configured
        if self.config.response_columns:
            builder = builder.with_response_variables(self.config.response_columns)

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
        covariance_expr = (self.aggregation_builder
                          .reset()
                          .with_covariance_calculation()
                          .build_population_aggregation()[1][0])

        return stratum_data.with_columns(covariance_expr)

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

    def _calculate_response_values(self, pop_data: pl.DataFrame) -> pl.DataFrame:
        """
        Calculate response-specific values (per-acre, percentages, etc.).
        
        Parameters
        ----------
        pop_data : pl.DataFrame
            Population-level data with totals and variances
            
        Returns
        -------
        pl.DataFrame
            Data with response values calculated
        """
        if self.config.estimation_type == EstimationType.AREA:
            return self._calculate_area_percentages(pop_data)
        else:
            return self._calculate_per_acre_values(pop_data)

    def _calculate_area_percentages(self, pop_data: pl.DataFrame) -> pl.DataFrame:
        """Calculate area percentages using appropriate method."""
        if self.config.by_land_type and "LAND_TYPE" in pop_data.columns:
            return self.percentage_calculator.calculate_land_type_percentages(pop_data)
        else:
            return self.percentage_calculator.calculate_standard_percentages(pop_data)

    def _calculate_per_acre_values(self, pop_data: pl.DataFrame) -> pl.DataFrame:
        """
        Calculate per-acre values for non-area estimations.
        
        Parameters
        ----------
        pop_data : pl.DataFrame
            Population data with totals
            
        Returns
        -------
        pl.DataFrame
            Data with per-acre values
        """
        numerator_total = self.aggregation_builder._get_numerator_total_name()

        # Calculate per-acre value
        pop_data = pop_data.with_columns(
            pl.when(pl.col("FAD_TOTAL") > 0)
            .then(pl.col(numerator_total) / pl.col("FAD_TOTAL"))
            .otherwise(0.0)
            .alias(self._get_per_acre_column_name())
        )

        # Calculate per-acre variance using ratio variance
        if self.config.include_variance:
            variance_expr = self.variance_calculator.calculate_ratio_variance(
                numerator_total, "FAD_TOTAL", f"{numerator_total}_VAR", "FAD_TOTAL_VAR", f"COV_{numerator_total}_FAD"
            )
            pop_data = pop_data.with_columns(variance_expr.alias(f"{self._get_per_acre_column_name()}_VAR"))

        return pop_data

    def _add_optional_outputs(self, pop_data: pl.DataFrame) -> pl.DataFrame:
        """
        Add optional output columns based on configuration.
        
        Parameters
        ----------
        pop_data : pl.DataFrame
            Data with calculated estimates
            
        Returns
        -------
        pl.DataFrame
            Data with optional outputs added
        """
        result = pop_data

        # Add total columns if requested
        if self.config.include_totals:
            numerator_total = self.aggregation_builder._get_numerator_total_name()
            if numerator_total in result.columns:
                result = result.with_columns(
                    pl.col(numerator_total).alias(self._get_total_column_name())
                )

        # Add standard errors instead of variances if requested
        if not self.config.include_variance:
            variance_cols = [col for col in result.columns if col.endswith("_VAR")]
            for var_col in variance_cols:
                se_col = var_col.replace("_VAR", "_SE")
                result = result.with_columns(
                    self.expression_builder.safe_sqrt(var_col).alias(se_col)
                )
            # Drop variance columns
            result = result.drop(variance_cols)

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

        # Base required columns for all estimation types
        required_cols = [
            self._get_numerator_column(),
            self._get_denominator_column(),
            "EXPNS",
            "STRATUM_CN"
        ]

        # Add adjusted columns for variance calculation
        numerator_adj, denominator_adj = self._get_adjusted_columns()
        required_cols.extend([numerator_adj, denominator_adj])

        missing_cols = [col for col in required_cols if col not in data.columns]
        if missing_cols:
            validation_results["is_valid"] = False
            validation_results["warnings"].append(f"Missing required columns: {missing_cols}")

        if validation_results["is_valid"]:
            # Check for data quality issues
            stats_exprs = [
                pl.col(self._get_numerator_column()).is_null().sum().alias("null_numerator"),
                pl.col(self._get_denominator_column()).is_null().sum().alias("null_denominator"),
                pl.col("EXPNS").is_null().sum().alias("null_expns"),
                pl.len().alias("total_rows")
            ]

            stats = data.select(stats_exprs).to_dicts()[0]
            validation_results["statistics"] = stats

            # Add warnings for data quality issues
            if stats["null_numerator"] > 0:
                validation_results["warnings"].append(f"Found {stats['null_numerator']} null numerator values")
            if stats["null_denominator"] > 0:
                validation_results["warnings"].append(f"Found {stats['null_denominator']} null denominator values")
            if stats["null_expns"] > 0:
                validation_results["warnings"].append(f"Found {stats['null_expns']} null expansion factors")

        return validation_results

    def _get_numerator_column(self) -> str:
        """Get the numerator column name for the estimation type."""
        defaults = {
            EstimationType.AREA: "TOTAL_AREA_NUMERATOR",
            EstimationType.VOLUME: "TOTAL_VOLUME",
            EstimationType.BIOMASS: "TOTAL_BIOMASS",
            EstimationType.TPA: "TOTAL_TPA",
            EstimationType.MORTALITY: "TOTAL_MORTALITY",
            EstimationType.GROWTH: "TOTAL_GROWTH"
        }
        return defaults.get(self.config.estimation_type, "TOTAL_AREA_NUMERATOR")

    def _get_denominator_column(self) -> str:
        """Get the denominator column name for the estimation type."""
        return "TOTAL_AREA_DENOMINATOR"  # Always forest area for per-acre calculations

    def _get_adjusted_columns(self) -> tuple[str, str]:
        """Get the adjusted column names for variance calculations."""
        base_name = self.aggregation_builder._get_base_name()
        return f"{base_name}_adjusted", "fad_adjusted"

    def _get_per_acre_column_name(self) -> str:
        """Get the per-acre column name for the estimation type."""
        mapping = {
            EstimationType.VOLUME: "VOLUME_PERC_ACRE",
            EstimationType.BIOMASS: "BIOMASS_PERC_ACRE",
            EstimationType.TPA: "TPA_PERC_ACRE",
            EstimationType.MORTALITY: "MORTALITY_PERC_ACRE",
            EstimationType.GROWTH: "GROWTH_PERC_ACRE"
        }
        return mapping.get(self.config.estimation_type, "VALUE_PERC_ACRE")

    def _get_total_column_name(self) -> str:
        """Get the total column name for the estimation type."""
        if self.config.estimation_type == EstimationType.AREA:
            return "AREA"
        else:
            return self.config.estimation_type.value.upper()
