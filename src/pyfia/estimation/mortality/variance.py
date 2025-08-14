"""
Variance calculation for mortality estimation.

This module implements stratified variance calculation for mortality
estimates following FIA statistical procedures from Bechtold & Patterson (2005).
"""

import polars as pl
from typing import List, Optional, Union
from pyfia.core import FIA


class MortalityVarianceCalculator:
    """
    Calculates variance for mortality estimates using stratified sampling.
    
    This class implements the variance calculation methods from
    Bechtold & Patterson (2005) for mortality estimation.
    
    The stratified variance formula:
    Var(Ŷ) = Σ_h [N_h²/n * (1-f_h) * s²_h / n_h]
    
    Where:
    - N_h = total area in stratum h (from EXPNS)
    - n = total sample size (plots)
    - f_h = sampling fraction in stratum h
    - s²_h = sample variance in stratum h
    - n_h = sample size in stratum h
    """
    
    def __init__(self, db: Optional[Union[str, FIA]] = None):
        """
        Initialize the variance calculator.
        
        Parameters
        ----------
        db : Union[str, FIA], optional
            FIA database for loading design factors if needed
        """
        if db is not None:
            if isinstance(db, str):
                self.db = FIA(db)
            else:
                self.db = db
        else:
            self.db = None
    
    def calculate_stratum_variance(
        self,
        data: pl.DataFrame,
        response_col: str,
        group_cols: Optional[List[str]] = None
    ) -> pl.DataFrame:
        """
        Calculate stratum-level variance components.
        
        This implements the stratum variance calculation:
        s²_h = Σ(y_i - ȳ_h)² / (n_h - 1)
        
        Which is equivalent to:
        s²_h = [Σy_i² - n_h * ȳ_h²] / (n_h - 1)
        
        Parameters
        ----------
        data : pl.DataFrame
            Plot-level data with stratum assignments
        response_col : str
            Column containing the response variable (e.g., mortality values)
        group_cols : List[str], optional
            Additional grouping columns (e.g., species, size class)
            
        Returns
        -------
        pl.DataFrame
            Stratum-level variance components
        """
        # Ensure numeric types for calculations
        data = data.with_columns([
            pl.col(response_col).cast(pl.Float64),
            pl.col("STRATUM_CN").cast(pl.Int64),
            pl.col("ESTN_UNIT_CN").cast(pl.Int64)
        ])
        
        # Build grouping columns
        group_by = ["STRATUM_CN", "ESTN_UNIT_CN"]
        if group_cols:
            # Only include grouping columns that exist in the data
            available_groups = [col for col in group_cols if col in data.columns]
            group_by.extend(available_groups)
        
        # Calculate stratum-level statistics
        stratum_stats = data.group_by(group_by).agg([
            # Sum of y values
            pl.col(response_col).sum().alias("y_sum"),
            # Sum of y² values
            (pl.col(response_col).pow(2)).sum().alias("y_sum_sq"),
            # Number of plots in stratum
            pl.len().alias("n_h"),
            # For joining with population data
            pl.col("EXPNS").first().alias("EXPNS"),
            pl.col("ADJ_FACTOR_SUBP").first().alias("ADJ_FACTOR_SUBP")
        ])
        
        # Calculate stratum mean and variance
        stratum_stats = stratum_stats.with_columns([
            # Mean: ȳ_h = Σy_i / n_h
            (pl.col("y_sum") / pl.col("n_h")).alias("y_mean"),
            # Degrees of freedom
            (pl.col("n_h") - 1).alias("df")
        ])
        
        # Calculate stratum variance: s²_h = [Σy_i² - n_h * ȳ_h²] / (n_h - 1)
        stratum_stats = stratum_stats.with_columns([
            pl.when(pl.col("df") > 0)
            .then(
                (pl.col("y_sum_sq") - (pl.col("n_h") * pl.col("y_mean").pow(2))) / pl.col("df")
            )
            .otherwise(0.0)  # Variance is 0 when n_h <= 1
            .alias("stratum_var")
        ])
        
        return stratum_stats
    
    def calculate_population_variance(
        self,
        stratum_data: pl.DataFrame,
        pop_data: Optional[pl.DataFrame] = None,
        group_cols: Optional[List[str]] = None
    ) -> pl.DataFrame:
        """
        Calculate population-level variance using stratified sampling formula.
        
        Implements the formula from the SQL reference:
        Var = (AREA²/n) * [Σ(w_h * n_h * s²_h) + (1/n) * Σ((1-w_h) * n_h * s²_h)]
        
        Where:
        - AREA = total area in estimation unit
        - n = total plots in estimation unit
        - w_h = stratum weight (P1POINTCNT / P1PNTCNT_EU)
        - n_h = plots in stratum
        - s²_h = stratum variance
        
        Parameters
        ----------
        stratum_data : pl.DataFrame
            Stratum-level data with variance components
        pop_data : pl.DataFrame, optional
            Population data with design factors. If not provided, will load from db
        group_cols : List[str], optional
            Additional grouping columns
            
        Returns
        -------
        pl.DataFrame
            Population-level variance estimates
        """
        # Load population design factors if not provided
        if pop_data is None and self.db is not None:
            pop_data = self._load_population_factors()
        elif pop_data is None:
            raise ValueError("Either pop_data or db must be provided")
        
        # Join stratum data with population factors
        join_cols = ["ESTN_UNIT_CN", "STRATUM_CN"]
        data_with_factors = stratum_data.join(
            pop_data,
            on=join_cols,
            how="inner"
        )
        
        # Build grouping columns for aggregation
        group_by = ["ESTN_UNIT_CN"]
        if group_cols:
            available_groups = [col for col in group_cols if col in stratum_data.columns]
            group_by.extend(available_groups)
        
        # Calculate population variance components
        pop_variance = data_with_factors.group_by(group_by).agg([
            # Total estimate (sum of stratum estimates)
            (pl.col("y_sum") * pl.col("EXPNS")).sum().alias("estimate"),
            
            # Total plots in estimation unit
            pl.col("n").first().alias("n_total"),
            
            # Total area
            pl.col("AREA_USED").first().alias("total_area"),
            
            # First variance component: Σ(w_h * n_h * s²_h)
            (pl.col("w_h") * pl.col("n_h") * pl.col("stratum_var")).sum().alias("var_component_1"),
            
            # Second variance component: Σ((1-w_h) * n_h * s²_h)
            ((1 - pl.col("w_h")) * pl.col("n_h") * pl.col("stratum_var")).sum().alias("var_component_2"),
            
            # Count of non-zero plots
            pl.col("n_h").sum().alias("total_plots")
        ])
        
        # Calculate final variance using the stratified formula
        pop_variance = pop_variance.with_columns([
            # Var = (AREA²/n) * [var_component_1 + (1/n) * var_component_2]
            pl.when(pl.col("n_total") > 0)
            .then(
                (pl.col("total_area").pow(2) / pl.col("n_total")) * 
                (pl.col("var_component_1") + (1.0 / pl.col("n_total")) * pl.col("var_component_2"))
            )
            .otherwise(0.0)
            .alias("var_of_estimate")
        ])
        
        # Calculate standard error and coefficient of variation
        pop_variance = pop_variance.with_columns([
            # Standard error = sqrt(variance)
            pl.col("var_of_estimate").sqrt().alias("se_of_estimate"),
            
            # CV% = (SE / estimate) * 100
            pl.when((pl.col("estimate") != 0) & (pl.col("estimate").is_not_null()))
            .then((pl.col("var_of_estimate").sqrt() / pl.col("estimate").abs()) * 100)
            .otherwise(0.0)
            .alias("se_of_estimate_pct")
        ])
        
        return pop_variance
    
    def _load_population_factors(self) -> pl.DataFrame:
        """
        Load population design factors from FIA database.
        
        Returns
        -------
        pl.DataFrame
            Population factors including weights and sample sizes
        """
        if self.db is None:
            raise ValueError("Database connection required to load population factors")
        
        # Load estimation unit data
        pop_eu = self.db.tables["POP_ESTN_UNIT"].collect().select([
            pl.col("CN").alias("ESTN_UNIT_CN"),
            "AREA_USED",
            "P1PNTCNT_EU",
            "P2PNTCNT_EU"
        ])
        
        # Load stratum data
        pop_stratum = self.db.tables["POP_STRATUM"].collect().select([
            "CN",
            "ESTN_UNIT_CN",
            "P1POINTCNT",
            "P2POINTCNT",
            "EXPNS",
            "ADJ_FACTOR_SUBP"
        ])
        
        # Join and calculate weights
        pop_factors = pop_stratum.join(
            pop_eu,
            on="ESTN_UNIT_CN",
            how="inner"
        ).with_columns([
            # Stratum weight
            (pl.col("P1POINTCNT") / pl.col("P1PNTCNT_EU")).alias("w_h"),
            # Total plots in estimation unit
            pl.col("P2PNTCNT_EU").alias("n")
        ]).rename({
            "CN": "STRATUM_CN"
        })
        
        return pop_factors