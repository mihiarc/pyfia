"""
Polars expression builders for safe mathematical operations.

This module provides utility classes and functions for building Polars expressions
that handle edge cases and provide protection for common mathematical operations
used in statistical calculations.
"""

import polars as pl
from typing import List, Dict, Any, Optional


class PolarsExpressionBuilder:
    """
    Builder class for creating safe Polars expressions.
    
    This class provides methods for building Polars expressions that include
    proper handling of edge cases like small samples, division by zero,
    negative values in square roots, and correlation calculations.
    """
    
    @staticmethod
    def safe_std(col_name: str, ddof: int = 1) -> pl.Expr:
        """
        Calculate standard deviation with protection for small samples.
        
        Parameters
        ----------
        col_name : str
            Column name for standard deviation calculation
        ddof : int, default 1
            Delta degrees of freedom
            
        Returns
        -------
        pl.Expr
            Polars expression for safe standard deviation
        """
        return (
            pl.when(pl.count(col_name) > ddof)
            .then(pl.std(col_name, ddof=ddof))
            .otherwise(0.0)
        )
    
    @staticmethod
    def safe_variance(col_name: str, ddof: int = 1) -> pl.Expr:
        """
        Calculate variance with protection for small samples.
        
        Parameters
        ----------
        col_name : str
            Column name for variance calculation
        ddof : int, default 1
            Delta degrees of freedom
            
        Returns
        -------
        pl.Expr
            Polars expression for safe variance
        """
        return (
            pl.when(pl.count(col_name) > ddof)
            .then(pl.var(col_name, ddof=ddof))
            .otherwise(0.0)
        )
    
    @staticmethod
    def safe_correlation(col1: str, col2: str) -> pl.Expr:
        """
        Calculate correlation with protection for edge cases.
        
        Handles cases where:
        - Sample size is too small (n <= 1)
        - One or both columns have zero variance
        - Correlation is null/undefined
        
        Parameters
        ----------
        col1 : str
            First column name
        col2 : str
            Second column name
            
        Returns
        -------
        pl.Expr
            Polars expression for safe correlation calculation
        """
        return (
            pl.when(pl.count(col1) > 1)
            .then(
                pl.when((pl.std(col1) == 0) & (pl.std(col2) == 0))
                .then(1.0)  # Perfect correlation when both are constant
                .when((pl.std(col1) == 0) | (pl.std(col2) == 0))
                .then(0.0)  # No correlation when one is constant
                .otherwise(pl.corr(col1, col2).fill_null(0.0))
            )
            .otherwise(0.0)
        )
    
    @staticmethod
    def safe_covariance(col1: str, col2: str, ddof: int = 1) -> pl.Expr:
        """
        Calculate covariance with protection for small samples.
        
        Parameters
        ----------
        col1 : str
            First column name
        col2 : str
            Second column name
        ddof : int, default 1
            Delta degrees of freedom
            
        Returns
        -------
        pl.Expr
            Polars expression for safe covariance calculation
        """
        return (
            pl.when(pl.count(col1) > ddof)
            .then(
                ((pl.col(col1) - pl.mean(col1)) * (pl.col(col2) - pl.mean(col2))).sum() 
                / (pl.count(col1) - ddof)
            )
            .otherwise(0.0)
        )
    
    @staticmethod
    def safe_sqrt(col_name: str) -> pl.Expr:
        """
        Calculate square root with protection for negative values.
        
        Parameters
        ----------
        col_name : str
            Column name for square root calculation
            
        Returns
        -------
        pl.Expr
            Polars expression for safe square root
        """
        return (
            pl.when(pl.col(col_name) >= 0)
            .then(pl.col(col_name).sqrt())
            .otherwise(0.0)
        )
    
    @staticmethod
    def safe_division(numerator: str, denominator: str, default: float = 0.0) -> pl.Expr:
        """
        Perform safe division with protection for zero denominators.
        
        Parameters
        ----------
        numerator : str
            Numerator column name
        denominator : str
            Denominator column name
        default : float, default 0.0
            Value to return when denominator is zero
            
        Returns
        -------
        pl.Expr
            Polars expression for safe division
        """
        return (
            pl.when(pl.col(denominator) != 0)
            .then(pl.col(numerator) / pl.col(denominator))
            .otherwise(default)
        )
    
    @staticmethod
    def safe_log(col_name: str, base: Optional[float] = None) -> pl.Expr:
        """
        Calculate logarithm with protection for non-positive values.
        
        Parameters
        ----------
        col_name : str
            Column name for logarithm calculation
        base : Optional[float], default None
            Logarithm base (None for natural log)
            
        Returns
        -------
        pl.Expr
            Polars expression for safe logarithm
        """
        if base is None:
            log_expr = pl.col(col_name).log()
        else:
            log_expr = pl.col(col_name).log() / pl.lit(base).log()
            
        return (
            pl.when(pl.col(col_name) > 0)
            .then(log_expr)
            .otherwise(float('-inf'))
        )
    
    @staticmethod
    def coefficient_of_variation(col_name: str) -> pl.Expr:
        """
        Calculate coefficient of variation (CV) with safety checks.
        
        Parameters
        ----------
        col_name : str
            Column name for CV calculation
            
        Returns
        -------
        pl.Expr
            Polars expression for coefficient of variation
        """
        return (
            pl.when((pl.count(col_name) > 1) & (pl.mean(col_name) != 0))
            .then(PolarsExpressionBuilder.safe_std(col_name) / pl.mean(col_name) * 100)
            .otherwise(0.0)
        )
    
    @staticmethod
    def standardized_error(estimate_col: str, variance_col: str) -> pl.Expr:
        """
        Calculate standardized error from estimate and variance.
        
        Parameters
        ----------
        estimate_col : str
            Column name for estimate values
        variance_col : str
            Column name for variance values
            
        Returns
        -------
        pl.Expr
            Polars expression for standardized error
        """
        return (
            pl.when((pl.col(estimate_col) != 0) & (pl.col(variance_col) >= 0))
            .then(PolarsExpressionBuilder.safe_sqrt(variance_col) / pl.col(estimate_col).abs() * 100)
            .otherwise(0.0)
        )


class AggregationExpressionBuilder:
    """
    Builder class for common aggregation expressions used in FIA estimation.
    
    Provides pre-built expressions for common aggregation patterns used
    in forest inventory analysis.
    """
    
    @staticmethod
    def weighted_mean(value_col: str, weight_col: str) -> pl.Expr:
        """
        Calculate weighted mean.
        
        Parameters
        ----------
        value_col : str
            Column name for values
        weight_col : str
            Column name for weights
            
        Returns
        -------
        pl.Expr
            Polars expression for weighted mean
        """
        return (
            pl.when(pl.sum(weight_col) != 0)
            .then((pl.col(value_col) * pl.col(weight_col)).sum() / pl.sum(weight_col))
            .otherwise(0.0)
        )
    
    @staticmethod
    def weighted_variance(value_col: str, weight_col: str) -> pl.Expr:
        """
        Calculate weighted variance.
        
        Parameters
        ----------
        value_col : str
            Column name for values
        weight_col : str
            Column name for weights
            
        Returns
        -------
        pl.Expr
            Polars expression for weighted variance
        """
        weighted_mean_expr = AggregationExpressionBuilder.weighted_mean(value_col, weight_col)
        
        return (
            pl.when(pl.sum(weight_col) != 0)
            .then(
                (pl.col(weight_col) * (pl.col(value_col) - weighted_mean_expr) ** 2).sum()
                / pl.sum(weight_col)
            )
            .otherwise(0.0)
        )
    
    @staticmethod
    def stratum_aggregation(
        value_col: str,
        weight_col: str = "w_h",
        count_col: str = "n_h"
    ) -> List[pl.Expr]:
        """
        Build standard stratum-level aggregation expressions.
        
        Parameters
        ----------
        value_col : str
            Column name for values to aggregate
        weight_col : str, default "w_h"
            Column name for stratum weights
        count_col : str, default "n_h"
            Column name for stratum counts
            
        Returns
        -------
        List[pl.Expr]
            List of aggregation expressions for stratum statistics
        """
        return [
            pl.sum(value_col).alias(f"{value_col}_total"),
            pl.mean(value_col).alias(f"{value_col}_mean"),
            PolarsExpressionBuilder.safe_std(value_col).alias(f"{value_col}_std"),
            PolarsExpressionBuilder.safe_variance(value_col).alias(f"{value_col}_var"),
            pl.count(value_col).alias(f"{value_col}_count"),
            pl.first(weight_col).alias(weight_col),
            pl.first(count_col).alias(count_col)
        ]
    
    @staticmethod
    def population_aggregation(
        numerator_col: str,
        denominator_col: str,
        weight_col: str = "w_h"
    ) -> List[pl.Expr]:
        """
        Build population-level aggregation expressions for ratio estimation.
        
        Parameters
        ----------
        numerator_col : str
            Column name for numerator values
        denominator_col : str
            Column name for denominator values
        weight_col : str, default "w_h"
            Column name for stratum weights
            
        Returns
        -------
        List[pl.Expr]
            List of aggregation expressions for population statistics
        """
        return [
            AggregationExpressionBuilder.weighted_mean(numerator_col, weight_col)
            .alias(f"{numerator_col}_total"),
            AggregationExpressionBuilder.weighted_mean(denominator_col, weight_col)
            .alias(f"{denominator_col}_total"),
            AggregationExpressionBuilder.weighted_variance(numerator_col, weight_col)
            .alias(f"{numerator_col}_var"),
            AggregationExpressionBuilder.weighted_variance(denominator_col, weight_col)
            .alias(f"{denominator_col}_var"),
            PolarsExpressionBuilder.safe_correlation(numerator_col, denominator_col)
            .alias(f"corr_{numerator_col}_{denominator_col}"),
        ]


class SafeMathExpressions:
    """
    Static utility class for common safe mathematical expressions.
    
    Provides commonly used safe mathematical expressions as static methods
    for easy reuse across different estimation modules.
    """
    
    @staticmethod
    def get_all_safe_expressions() -> Dict[str, pl.Expr]:
        """
        Get a dictionary of all common safe mathematical expressions.
        
        Returns
        -------
        Dict[str, pl.Expr]
            Dictionary mapping expression names to Polars expressions
        """
        return {
            'safe_std': PolarsExpressionBuilder.safe_std,
            'safe_variance': PolarsExpressionBuilder.safe_variance,
            'safe_correlation': PolarsExpressionBuilder.safe_correlation,
            'safe_sqrt': PolarsExpressionBuilder.safe_sqrt,
            'safe_division': PolarsExpressionBuilder.safe_division,
            'safe_log': PolarsExpressionBuilder.safe_log,
            'coefficient_of_variation': PolarsExpressionBuilder.coefficient_of_variation,
            'standardized_error': PolarsExpressionBuilder.standardized_error,
        }