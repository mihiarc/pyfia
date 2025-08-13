"""
Variance and percentage calculation components for FIA estimation.

This module provides reusable components for statistical calculations including
variance estimation, percentage calculations with proper precision, and
mathematical operations with safety protections.
"""

from contextlib import contextmanager
from decimal import Decimal, ROUND_HALF_UP, localcontext
from typing import Optional

import polars as pl

from ..utils import ratio_var


@contextmanager
def statistical_precision(precision: int = 10):
    """Context manager for high-precision statistical calculations."""
    with localcontext() as ctx:
        ctx.prec = precision
        ctx.rounding = ROUND_HALF_UP
        yield ctx


class VarianceCalculator:
    """
    Handles variance calculations for stratified sampling and ratio estimation.
    
    This class provides methods for calculating variance components in stratified
    sampling designs, including proper handling of edge cases and small sample
    corrections.
    """
    
    def variance_component(self, var_name: str) -> pl.Expr:
        """
        Calculate variance component for stratified sampling.
        
        Parameters
        ----------
        var_name : str
            Name of the variable for variance calculation (e.g., 'fa', 'fad')
            
        Returns
        -------
        pl.Expr
            Polars expression for variance component calculation
        """
        return (
            pl.when(pl.col("n_h") > 1)
            .then(
                (pl.col("w_h").cast(pl.Float64).pow(2)) * 
                (pl.col(f"s_{var_name}_h").cast(pl.Float64).pow(2)) / 
                pl.col("n_h")
            )
            .otherwise(0.0)
            .sum()
        )
    
    def covariance_component(self) -> pl.Expr:
        """
        Calculate covariance component for ratio variance.
        
        Returns
        -------
        pl.Expr
            Polars expression for covariance component calculation
        """
        return (
            pl.when(pl.col("n_h") > 1)
            .then(
                (pl.col("w_h").cast(pl.Float64).pow(2)) * 
                pl.col("s_fa_fad_h") / 
                pl.col("n_h")
            )
            .otherwise(0.0)
            .sum()
        )
    
    def calculate_ratio_variance(
        self,
        numerator_col: str,
        denominator_col: str,
        numerator_var_col: str,
        denominator_var_col: str,
        covariance_col: str,
    ) -> pl.Expr:
        """
        Calculate ratio variance using the delta method.
        
        Parameters
        ----------
        numerator_col : str
            Column name for numerator values
        denominator_col : str
            Column name for denominator values
        numerator_var_col : str
            Column name for numerator variance
        denominator_var_col : str
            Column name for denominator variance
        covariance_col : str
            Column name for covariance between numerator and denominator
            
        Returns
        -------
        pl.Expr
            Polars expression for ratio variance calculation
        """
        return ratio_var(
            pl.col(numerator_col),
            pl.col(denominator_col),
            pl.col(numerator_var_col),
            pl.col(denominator_var_col),
            pl.col(covariance_col),
        )


class PercentageCalculator:
    """
    Handles percentage calculations with proper precision and variance estimation.
    
    This class provides methods for calculating area percentages using either
    standard ratio-of-means or common denominator approaches, with proper
    handling of precision and variance calculations.
    """
    
    def __init__(self, decimal_precision: int = 10, percentage_decimal_places: int = 2):
        """
        Initialize the percentage calculator.
        
        Parameters
        ----------
        decimal_precision : int, default 10
            Number of decimal places for intermediate calculations
        percentage_decimal_places : int, default 2
            Number of decimal places for final percentage results
        """
        self.decimal_precision = decimal_precision
        self.percentage_decimal_places = percentage_decimal_places
        self.variance_calculator = VarianceCalculator()
    
    def safe_percentage_calc(self, numerator: Optional[float], denominator: Optional[float]) -> float:
        """
        Safely calculate percentage with proper precision handling.
        
        Parameters
        ----------
        numerator : Optional[float]
            Numerator value
        denominator : Optional[float]
            Denominator value
            
        Returns
        -------
        float
            Calculated percentage with proper precision
        """
        try:
            # Handle zero or null denominators
            if denominator is None or denominator == 0:
                return 0.0
                
            # Handle zero or null numerators
            if numerator is None:
                numerator = 0
            
            # Use basic float arithmetic to avoid Decimal precision issues with real data
            result = (float(numerator) / float(denominator)) * 100.0
            
            # Round to specified decimal places
            return round(result, self.percentage_decimal_places)
            
        except (ValueError, TypeError, ZeroDivisionError):
            return 0.0
    
    def calculate_standard_percentages(self, data: pl.DataFrame) -> pl.DataFrame:
        """
        Calculate standard area percentages using ratio-of-means estimation.
        
        Parameters
        ----------
        data : pl.DataFrame
            DataFrame with FA_TOTAL, FAD_TOTAL, FA_VAR, FAD_VAR, and COV_FA_FAD columns
            
        Returns
        -------
        pl.DataFrame
            DataFrame with added AREA_PERC and AREA_PERC_VAR columns
        """
        # Calculate percentage with proper precision
        result = data.with_columns(
            pl.struct(["FA_TOTAL", "FAD_TOTAL"]).map_elements(
                lambda row: self.safe_percentage_calc(row["FA_TOTAL"], row["FAD_TOTAL"]),
                return_dtype=pl.Float64
            ).alias("AREA_PERC")
        )
        
        # Calculate ratio variance
        result = result.with_columns(
            self.variance_calculator.calculate_ratio_variance(
                "FA_TOTAL", "FAD_TOTAL", "FA_VAR", "FAD_VAR", "COV_FA_FAD"
            ).alias("PERC_VAR_RATIO")
        )
        
        # Convert to percentage variance
        result = result.with_columns(
            pl.when(pl.col("PERC_VAR_RATIO") < 0)
            .then(0.0)
            .otherwise(pl.col("PERC_VAR_RATIO") * 10000)  # (100)^2
            .alias("AREA_PERC_VAR")
        )
        
        return result
    
    def calculate_land_type_percentages(self, data: pl.DataFrame) -> pl.DataFrame:
        """
        Calculate percentages for land type analysis with appropriate denominators.
        
        For land types (forest, non-forest), percentages are calculated relative to 
        total land area (excluding water). For water, percentage is calculated 
        relative to total area (including water).
        
        Parameters
        ----------
        data : pl.DataFrame
            DataFrame with FA_TOTAL, FAD_TOTAL, FA_VAR, FAD_VAR and LAND_TYPE columns
            
        Returns
        -------
        pl.DataFrame
            DataFrame with added AREA_PERC and AREA_PERC_VAR columns
        """
        # Get total area including water
        total_area = data.select(pl.sum("FAD_TOTAL")).item()
        
        # Get total land area (excluding water) 
        land_area_total = (
            data.filter(~pl.col("LAND_TYPE").str.contains("Water"))
            .select(pl.sum("FAD_TOTAL").alias("TOTAL_LAND_AREA"))
        )[0, 0] if len(data.filter(~pl.col("LAND_TYPE").str.contains("Water"))) > 0 else 0.0
        
        # Calculate percentage with appropriate denominator for each land type
        def safe_land_type_calc(row):
            fa_total = row["FA_TOTAL"]
            land_type = row["LAND_TYPE"]
            if "Water" in str(land_type):
                return self.safe_percentage_calc(fa_total, total_area)
            else:
                return self.safe_percentage_calc(fa_total, land_area_total)
        
        result = data.with_columns(
            pl.struct(["FA_TOTAL", "LAND_TYPE"]).map_elements(
                safe_land_type_calc,
                return_dtype=pl.Float64
            ).alias("AREA_PERC")
        )
        
        # Calculate variance with appropriate denominators
        total_area_var = data.select(pl.sum("FAD_VAR")).item()
        land_area_var = (
            data.filter(~pl.col("LAND_TYPE").str.contains("Water"))
            .select(pl.sum("FAD_VAR").alias("TOTAL_LAND_VAR"))
        )[0, 0] if len(data.filter(~pl.col("LAND_TYPE").str.contains("Water"))) > 0 else 0.0
        
        # Ratio variance with appropriate denominator for each land type
        result = result.with_columns(
            pl.when(pl.col("LAND_TYPE").str.contains("Water"))
            .then(
                pl.when(total_area == 0)
                .then(0.0)
                .otherwise(
                    (1 / (total_area * total_area)) * (
                        pl.col("FA_VAR") +
                        ((pl.col("FA_TOTAL").cast(pl.Float64) / total_area).pow(2)) * total_area_var -
                        2 * (pl.col("FA_TOTAL") / total_area) * pl.col("FA_VAR")
                    )
                )
            )
            .otherwise(
                pl.when(land_area_total == 0)
                .then(0.0)
                .otherwise(
                    (1 / (land_area_total * land_area_total)) * (
                        pl.col("FA_VAR") +
                        ((pl.col("FA_TOTAL").cast(pl.Float64) / land_area_total).pow(2)) * land_area_var -
                        2 * (pl.col("FA_TOTAL") / land_area_total) * pl.col("FA_VAR")
                    )
                )
            )
            .alias("PERC_VAR_RATIO")
        )
        
        # Convert to percentage variance
        result = result.with_columns(
            pl.when(pl.col("PERC_VAR_RATIO") < 0)
            .then(0.0)
            .otherwise(pl.col("PERC_VAR_RATIO") * 10000)  # (100)^2
            .alias("AREA_PERC_VAR")
        )
        
        return result


class PrecisionCalculator:
    """
    Utility class for high-precision mathematical calculations.
    
    Provides methods for performing calculations with controlled precision
    using the Decimal module for statistical accuracy.
    """
    
    @staticmethod
    def calculate_with_precision(
        operation: callable,
        *args,
        precision: int = 10,
        decimal_places: int = 2
    ) -> float:
        """
        Perform calculation with specified precision.
        
        Parameters
        ----------
        operation : callable
            Mathematical operation to perform
        *args
            Arguments to pass to the operation
        precision : int, default 10
            Number of decimal places for intermediate calculations
        decimal_places : int, default 2
            Number of decimal places for final result
            
        Returns
        -------
        float
            Result of the operation with proper precision
        """
        with statistical_precision(precision):
            try:
                result = operation(*args)
                if isinstance(result, Decimal):
                    result = result.quantize(
                        Decimal(f'0.{"0" * decimal_places}'),
                        rounding=ROUND_HALF_UP
                    )
                    return float(result)
                return result
            except (ValueError, TypeError, ZeroDivisionError):
                return 0.0