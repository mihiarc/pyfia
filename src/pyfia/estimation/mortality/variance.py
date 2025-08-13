"""
Variance calculation for mortality estimates.

This module implements stratified variance calculation for mortality
estimates following FIA statistical procedures.
"""

from typing import List, Optional
import polars as pl

from ..utils import ratio_var


class MortalityVarianceCalculator:
    """
    Handles variance calculations for mortality estimates.
    
    Implements stratified sampling variance calculation following
    Bechtold & Patterson (2005) for mortality estimation.
    """
    
    def __init__(self):
        """Initialize the variance calculator."""
        pass
    
    def calculate_stratum_variance(
        self, 
        data: pl.DataFrame, 
        response_var: str,
        group_cols: Optional[List[str]] = None
    ) -> pl.DataFrame:
        """
        Calculate variance at the stratum level.
        
        Parameters
        ----------
        data : pl.DataFrame
            Plot-level data with stratum assignments
        response_var : str
            Name of the response variable (e.g., "PLOT_MORTALITY_TPA")
        group_cols : Optional[List[str]]
            Additional grouping columns beyond stratum
            
        Returns
        -------
        pl.DataFrame
            Stratum-level statistics including variance components
        """
        # Define stratum grouping
        strat_groups = ["STRATUM_CN"]
        if group_cols:
            strat_groups.extend(group_cols)
        
        # Helper function for safe standard deviation
        def _safe_std(col_name: str) -> pl.Expr:
            return (
                pl.when(pl.count(col_name) > 1)
                .then(pl.std(col_name, ddof=1))
                .otherwise(0.0)
            )
        
        # Calculate stratum-level statistics
        stratum_stats = data.group_by(strat_groups).agg([
            # Sample size
            pl.len().alias("n_h"),
            
            # Mean and standard deviation
            pl.mean(response_var).alias(f"{response_var}_bar_h"),
            _safe_std(response_var).alias(f"s_{response_var}_h"),
            
            # Stratum weight (expansion factor)
            pl.first("EXPNS").alias("A_h"),
            
            # Adjustment factor
            pl.first("ADJ_FACTOR_SUBP").alias("adj_h"),
        ])
        
        # Calculate variance contribution from each stratum
        stratum_stats = stratum_stats.with_columns([
            # Variance of the mean
            pl.when(pl.col("n_h") > 1)
            .then(
                (pl.col(f"s_{response_var}_h") ** 2) / pl.col("n_h")
            )
            .otherwise(0.0)
            .alias(f"var_{response_var}_h"),
            
            # Weight for variance calculation (A_h^2)
            (pl.col("A_h") ** 2).alias("A_h_sq"),
        ])
        
        return stratum_stats
    
    def calculate_population_variance(
        self,
        stratum_data: pl.DataFrame,
        response_var: str,
        group_cols: Optional[List[str]] = None
    ) -> pl.DataFrame:
        """
        Calculate population-level variance from stratum statistics.
        
        Parameters
        ----------
        stratum_data : pl.DataFrame
            Stratum-level statistics from calculate_stratum_variance
        response_var : str
            Name of the response variable
        group_cols : Optional[List[str]]
            Grouping columns for aggregation
            
        Returns
        -------
        pl.DataFrame
            Population-level estimates with variance
        """
        # Aggregation expressions
        agg_exprs = [
            # Total estimate (sum of stratum contributions)
            (
                pl.col(f"{response_var}_bar_h") * 
                pl.col("adj_h") * 
                pl.col("A_h")
            ).sum().alias(f"{response_var}_TOTAL"),
            
            # Variance (sum of weighted stratum variances)
            (
                pl.col("A_h_sq") * 
                pl.col(f"var_{response_var}_h") * 
                (pl.col("adj_h") ** 2)
            ).sum().alias(f"{response_var}_VAR"),
            
            # Total area (sum of expansion factors)
            pl.col("A_h").sum().alias("TOTAL_AREA"),
            
            # Total plots
            pl.col("n_h").sum().alias("N_PLOTS"),
        ]
        
        # Aggregate by groups if specified
        if group_cols:
            pop_estimates = stratum_data.group_by(group_cols).agg(agg_exprs)
        else:
            pop_estimates = stratum_data.select(agg_exprs)
        
        # Calculate per-acre values and standard errors
        pop_estimates = pop_estimates.with_columns([
            # Per-acre estimate
            (pl.col(f"{response_var}_TOTAL") / pl.col("TOTAL_AREA"))
            .alias(response_var.replace("PLOT_", "")),
            
            # Standard error (per-acre)
            (
                pl.col(f"{response_var}_VAR").sqrt() / pl.col("TOTAL_AREA")
            ).alias(f"{response_var.replace('PLOT_', '')}_SE"),
            
            # Coefficient of variation
            pl.when(pl.col(f"{response_var}_TOTAL") > 0)
            .then(
                (pl.col(f"{response_var}_VAR").sqrt() / pl.col(f"{response_var}_TOTAL")) * 100
            )
            .otherwise(0.0)
            .alias(f"{response_var.replace('PLOT_', '')}_CV"),
        ])
        
        return pop_estimates
    
    def calculate_ratio_variance(
        self,
        data: pl.DataFrame,
        numerator_col: str,
        denominator_col: str,
        group_cols: Optional[List[str]] = None
    ) -> pl.DataFrame:
        """
        Calculate variance for ratio estimates (e.g., mortality per unit area).
        
        Parameters
        ----------
        data : pl.DataFrame
            Data with numerator and denominator values
        numerator_col : str
            Column name for ratio numerator
        denominator_col : str
            Column name for ratio denominator  
        group_cols : Optional[List[str]]
            Grouping columns
            
        Returns
        -------
        pl.DataFrame
            Data with ratio variance added
        """
        # First calculate stratum-level statistics for both variables
        strat_groups = ["STRATUM_CN"]
        if group_cols:
            strat_groups.extend(group_cols)
        
        # Helper functions
        def _safe_std(col_name: str) -> pl.Expr:
            return (
                pl.when(pl.count(col_name) > 1)
                .then(pl.std(col_name, ddof=1))
                .otherwise(0.0)
            )
        
        def _safe_correlation(col1: str, col2: str) -> pl.Expr:
            return (
                pl.when(pl.count(col1) > 1)
                .then(
                    pl.when((pl.std(col1) == 0) & (pl.std(col2) == 0))
                    .then(1.0)
                    .otherwise(pl.corr(col1, col2).fill_null(0))
                )
                .otherwise(0.0)
            )
        
        # Calculate stratum statistics
        stratum_stats = data.group_by(strat_groups).agg([
            # Sample size
            pl.len().alias("n_h"),
            
            # Means
            pl.mean(numerator_col).alias("y_bar_h"),
            pl.mean(denominator_col).alias("x_bar_h"),
            
            # Standard deviations
            _safe_std(numerator_col).alias("s_y_h"),
            _safe_std(denominator_col).alias("s_x_h"),
            
            # Correlation
            _safe_correlation(numerator_col, denominator_col).alias("r_yx_h"),
            
            # Stratum weight
            pl.first("EXPNS").alias("A_h"),
            pl.first("ADJ_FACTOR_SUBP").alias("adj_h"),
        ])
        
        # Calculate covariance from correlation
        stratum_stats = stratum_stats.with_columns([
            pl.when((pl.col("s_y_h") == 0) | (pl.col("s_x_h") == 0))
            .then(0.0)
            .otherwise(pl.col("r_yx_h") * pl.col("s_y_h") * pl.col("s_x_h"))
            .alias("s_yx_h")
        ])
        
        # Aggregate to population level
        agg_exprs = [
            # Totals
            (pl.col("y_bar_h") * pl.col("adj_h") * pl.col("A_h")).sum().alias("Y_TOTAL"),
            (pl.col("x_bar_h") * pl.col("adj_h") * pl.col("A_h")).sum().alias("X_TOTAL"),
            
            # Variance components
            pl.when(pl.col("n_h") > 1)
            .then(
                (pl.col("A_h") ** 2) * (pl.col("adj_h") ** 2) *
                (pl.col("s_y_h") ** 2) / pl.col("n_h")
            )
            .otherwise(0.0)
            .sum().alias("VAR_Y"),
            
            pl.when(pl.col("n_h") > 1)
            .then(
                (pl.col("A_h") ** 2) * (pl.col("adj_h") ** 2) *
                (pl.col("s_x_h") ** 2) / pl.col("n_h")
            )
            .otherwise(0.0)
            .sum().alias("VAR_X"),
            
            pl.when(pl.col("n_h") > 1)
            .then(
                (pl.col("A_h") ** 2) * (pl.col("adj_h") ** 2) *
                pl.col("s_yx_h") / pl.col("n_h")
            )
            .otherwise(0.0)
            .sum().alias("COV_YX"),
            
            # Sample size
            pl.col("n_h").sum().alias("N_PLOTS"),
        ]
        
        if group_cols:
            pop_stats = stratum_stats.group_by(group_cols).agg(agg_exprs)
        else:
            pop_stats = stratum_stats.select(agg_exprs)
        
        # Calculate ratio and its variance
        pop_stats = pop_stats.with_columns([
            # Ratio estimate
            pl.when(pl.col("X_TOTAL") > 0)
            .then(pl.col("Y_TOTAL") / pl.col("X_TOTAL"))
            .otherwise(0.0)
            .alias("RATIO"),
            
            # Ratio variance using delta method
            ratio_var(
                pl.col("Y_TOTAL"),
                pl.col("X_TOTAL"),
                pl.col("VAR_Y"),
                pl.col("VAR_X"),
                pl.col("COV_YX")
            ).alias("RATIO_VAR"),
        ])
        
        # Add standard error and CV
        pop_stats = pop_stats.with_columns([
            pl.col("RATIO_VAR").sqrt().alias("RATIO_SE"),
            
            pl.when(pl.col("RATIO") > 0)
            .then((pl.col("RATIO_VAR").sqrt() / pl.col("RATIO")) * 100)
            .otherwise(0.0)
            .alias("RATIO_CV"),
        ])
        
        return pop_stats