"""
Unified aggregation builder that handles all estimation types.

This module provides a single, flexible builder that can construct aggregation
expressions for any FIA estimation type (area, volume, biomass, etc.) while
maintaining type-specific customizations.
"""

from typing import Dict, List, Optional, Tuple

import polars as pl

from ..statistics import VarianceCalculator
from ..statistics.expressions import PolarsExpressionBuilder
from .interfaces import (
    EstimationType,
    IAggregationBuilder,
    UnifiedAggregationConfig,
)


class UnifiedAggregationBuilder(IAggregationBuilder):
    """
    Unified builder for constructing aggregation expressions across all estimation types.
    
    This class replaces the estimation-specific builders with a single, configurable
    builder that can handle any FIA estimation type while maintaining the flexibility
    needed for type-specific calculations.
    """

    def __init__(self, config: UnifiedAggregationConfig):
        """
        Initialize the unified aggregation builder.
        
        Parameters
        ----------
        config : UnifiedAggregationConfig
            Configuration specifying the estimation type and aggregation strategy
        """
        self.config = config
        self.aggregations: List[pl.Expr] = []
        self.group_cols: List[str] = []

        # Initialize components
        self.variance_calculator = VarianceCalculator()
        self.expression_builder = PolarsExpressionBuilder()

        # Estimation-specific column mappings
        self._response_columns = config.response_columns or self._get_default_response_columns()

    def _get_default_response_columns(self) -> Dict[str, str]:
        """Get default response columns for the estimation type."""
        defaults = {
            EstimationType.AREA: {
                "numerator": "TOTAL_AREA_NUMERATOR",
                "denominator": "TOTAL_AREA_DENOMINATOR"
            },
            EstimationType.VOLUME: {
                "volume": "TOTAL_VOLUME",
                "denominator": "TOTAL_AREA_DENOMINATOR"
            },
            EstimationType.BIOMASS: {
                "biomass": "TOTAL_BIOMASS",
                "denominator": "TOTAL_AREA_DENOMINATOR"
            },
            EstimationType.TPA: {
                "tpa": "TOTAL_TPA",
                "denominator": "TOTAL_AREA_DENOMINATOR"
            },
            EstimationType.MORTALITY: {
                "mortality": "TOTAL_MORTALITY",
                "denominator": "TOTAL_AREA_DENOMINATOR"
            },
            EstimationType.GROWTH: {
                "growth": "TOTAL_GROWTH",
                "denominator": "TOTAL_AREA_DENOMINATOR"
            }
        }
        return defaults.get(self.config.estimation_type, {})

    def with_totals(self, numerator_col: str, denominator_col: str) -> 'UnifiedAggregationBuilder':
        """
        Add total aggregations for numerator and denominator.
        
        Parameters
        ----------
        numerator_col : str
            Column name for numerator values
        denominator_col : str
            Column name for denominator values
            
        Returns
        -------
        UnifiedAggregationBuilder
            Builder instance for method chaining
        """
        self.aggregations.extend([
            pl.sum(numerator_col).alias(self._get_numerator_total_name()),
            pl.sum(denominator_col).alias(self._get_denominator_total_name())
        ])
        return self

    def with_variance_statistics(self, **columns) -> 'UnifiedAggregationBuilder':
        """
        Add variance calculation statistics.
        
        Parameters
        ----------
        **columns : str
            Column mappings (numerator_col, denominator_col, etc.)
            
        Returns
        -------
        UnifiedAggregationBuilder
            Builder instance for method chaining
        """
        numerator_col = columns.get("numerator_col", "fa_adjusted")
        denominator_col = columns.get("denominator_col", "fad_adjusted")

        self.aggregations.extend([
            # Sample size (all plots in stratum)
            pl.len().alias("n_h"),

            # Non-zero plots count
            pl.when(pl.col(numerator_col) > 0).then(1).otherwise(0).sum().alias("n_nonzero"),

            # Mean values for variance calculation
            pl.mean(numerator_col).alias(f"{self._get_base_name()}_bar_h"),
            pl.mean(denominator_col).alias("fad_bar_h"),

            # Standard deviations
            self.expression_builder.safe_std(numerator_col).alias(f"s_{self._get_base_name()}_h"),
            self.expression_builder.safe_std(denominator_col).alias("s_fad_h"),

            # Correlation for ratio variance
            self.expression_builder.safe_correlation(numerator_col, denominator_col).alias(f"corr_{self._get_base_name()}_fad"),

            # Stratum weight
            pl.first("EXPNS").alias("w_h"),
        ])
        return self

    def with_response_variables(self, response_cols: Dict[str, str]) -> 'UnifiedAggregationBuilder':
        """
        Add response variable aggregations for specific estimation types.
        
        Parameters
        ----------
        response_cols : Dict[str, str]
            Mapping of response variable names to column names
            
        Returns
        -------
        UnifiedAggregationBuilder
            Builder instance for method chaining
        """
        for response_name, col_name in response_cols.items():
            if col_name:  # Only add if column name is provided
                # Add stratum-level total
                self.aggregations.append(
                    pl.sum(col_name).alias(f"STRATUM_{response_name.upper()}")
                )

        return self

    def with_covariance_calculation(self) -> 'UnifiedAggregationBuilder':
        """
        Add covariance calculation from correlation and standard deviations.
        
        Returns
        -------
        UnifiedAggregationBuilder
            Builder instance for method chaining
        """
        base_name = self._get_base_name()
        covariance_expr = (
            pl.when((pl.col(f"s_{base_name}_h") == 0) | (pl.col("s_fad_h") == 0))
            .then(0.0)
            .otherwise(pl.col(f"corr_{base_name}_fad") * pl.col(f"s_{base_name}_h") * pl.col("s_fad_h"))
            .alias(f"s_{base_name}_fad_h")
        )
        self.aggregations.append(covariance_expr)
        return self

    def with_population_totals(self) -> 'UnifiedAggregationBuilder':
        """
        Add population-level total aggregations.
        
        Returns
        -------
        UnifiedAggregationBuilder
            Builder instance for method chaining
        """
        numerator_total = self._get_numerator_total_name()
        denominator_total = self._get_denominator_total_name()

        self.aggregations.extend([
            pl.col(numerator_total).sum().alias(numerator_total),
            pl.col(denominator_total).sum().alias(denominator_total),
            pl.col("n_h").sum().alias("N_PLOTS_TOTAL"),
            pl.col("n_nonzero").sum().alias("N_PLOTS"),
        ])
        return self

    def with_variance_components(self) -> 'UnifiedAggregationBuilder':
        """
        Add variance component calculations.
        
        Returns
        -------
        UnifiedAggregationBuilder
            Builder instance for method chaining
        """
        base_name = self._get_base_name()
        numerator_total = self._get_numerator_total_name()

        self.aggregations.extend([
            self.variance_calculator.variance_component(base_name).alias(f"{numerator_total}_VAR"),
            self.variance_calculator.variance_component("fad").alias("FAD_TOTAL_VAR"),
            self.variance_calculator.covariance_component().alias(f"COV_{numerator_total}_FAD"),
        ])
        return self

    def group_by(self, *columns: str) -> 'UnifiedAggregationBuilder':
        """
        Set grouping columns.
        
        Parameters
        ----------
        *columns : str
            Column names for grouping
            
        Returns
        -------
        UnifiedAggregationBuilder
            Builder instance for method chaining
        """
        self.group_cols.extend(columns)
        return self

    def build_stratum_aggregation(self) -> Tuple[List[str], List[pl.Expr]]:
        """
        Build stratum-level aggregation specification.
        
        Returns
        -------
        Tuple[List[str], List[pl.Expr]]
            Tuple of grouping columns and aggregation expressions
        """
        # Ensure stratum is included in grouping
        group_cols = ["STRATUM_CN"] + [col for col in self.group_cols if col != "STRATUM_CN"]
        return group_cols, self.aggregations.copy()

    def build_population_aggregation(self) -> Tuple[Optional[List[str]], List[pl.Expr]]:
        """
        Build population-level aggregation specification.
        
        Returns
        -------
        Tuple[Optional[List[str]], List[pl.Expr]]
            Tuple of grouping columns (None if no grouping) and aggregation expressions
        """
        group_cols = self.group_cols if self.group_cols else None
        return group_cols, self.aggregations.copy()

    def reset(self) -> 'UnifiedAggregationBuilder':
        """
        Reset the builder to initial state.
        
        Returns
        -------
        UnifiedAggregationBuilder
            Builder instance for method chaining
        """
        self.aggregations.clear()
        self.group_cols.clear()
        return self

    def _get_base_name(self) -> str:
        """Get the base name for the estimation type (e.g., 'fa', 'vol', 'bio')."""
        mapping = {
            EstimationType.AREA: "fa",
            EstimationType.VOLUME: "vol",
            EstimationType.BIOMASS: "bio",
            EstimationType.TPA: "tpa",
            EstimationType.MORTALITY: "mort",
            EstimationType.GROWTH: "grow"
        }
        return mapping.get(self.config.estimation_type, "fa")

    def _get_numerator_total_name(self) -> str:
        """Get the standard numerator total column name."""
        if self.config.estimation_type == EstimationType.AREA:
            return "FA_TOTAL"
        else:
            base_name = self._get_base_name().upper()
            return f"{base_name}_TOTAL"

    def _get_denominator_total_name(self) -> str:
        """Get the standard denominator total column name."""
        return "FAD_TOTAL"  # Always forest area denominator for per-acre calculations
