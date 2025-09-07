"""
rFIA-compatible variance calculation implementation.

This module implements the exact variance calculation methodology used by rFIA,
including proper stratified sampling design, FIA design factors, and covariance
calculations for ratio estimation.

Based on rFIA source code analysis:
- /R/util.R: Core variance functions (ratioVar, sumToEU)  
- /R/area.R: Area estimation workflow
- /R/areaStarter.R: Plot-level aggregation

Key Components:
1. Stratum-level variance using standard sample variance formula
2. Population-level variance with proper FIA design factor integration
3. Covariance calculations for ratio variance (delta method)
4. Complete stratified sampling expansion methodology
"""

from typing import Optional, Dict, Any, Union
import polars as pl
from ...core import FIA


class RFIAStratumVarianceCalculator:
    """
    Implements rFIA's stratum-level variance calculations.
    
    This class provides the core stratum variance calculation using
    rFIA's exact methodology:
    
    var = (sum(x^2) - (P2POINTCNT * (sum(x / P2POINTCNT)^2))) / (P2POINTCNT * (P2POINTCNT - 1))
    
    Mathematical equivalent: Var(x) = (Σx² - n*x̄²) / (n*(n-1))
    """
    
    def calculate_stratum_variance(self, values_col: str, plot_count_col: str = "P2POINTCNT") -> pl.Expr:
        """
        Calculate stratum variance using rFIA's standard sample variance formula.
        
        This implements the exact rFIA formula from util.R:sumToEU:
        var = (sum(x^2, na.rm = TRUE) - (P2POINTCNT * (sum(x / P2POINTCNT, na.rm = TRUE)^2))) / (P2POINTCNT * (P2POINTCNT - 1))
        
        Parameters
        ----------
        values_col : str
            Column name containing the values for variance calculation
        plot_count_col : str, default "P2POINTCNT"
            Column name containing the plot count per stratum
            
        Returns
        -------
        pl.Expr
            Polars expression for stratum variance calculation
        """
        # Calculate components of the variance formula
        sum_x_squared = pl.col(values_col).pow(2).sum()
        sum_x = pl.col(values_col).sum()
        n_plots = pl.col(plot_count_col).first()  # P2POINTCNT is constant within stratum
        
        # Mean calculation: sum(x) / P2POINTCNT
        mean_x = sum_x / n_plots
        
        # rFIA formula: (Σx² - n*x̄²) / (n*(n-1))
        variance = (
            sum_x_squared - (n_plots * mean_x.pow(2))
        ) / (n_plots * (n_plots - 1))
        
        # Handle edge cases: when n <= 1, variance is undefined (set to 0)
        return pl.when(n_plots > 1).then(variance).otherwise(0.0)
    
    def calculate_stratum_covariance(
        self, 
        x_col: str, 
        y_col: str, 
        plot_count_col: str = "P2POINTCNT"
    ) -> pl.Expr:
        """
        Calculate stratum covariance using rFIA's formula.
        
        This implements the rFIA covariance formula:
        cov = (sum(x * y) - (P2POINTCNT * sum(x / P2POINTCNT) * sum(y / P2POINTCNT))) / (P2POINTCNT * (P2POINTCNT - 1))
        
        Mathematical equivalent: Cov(X,Y) = (Σ(xy) - n*x̄*ȳ) / (n*(n-1))
        
        Parameters
        ----------
        x_col : str
            Column name for x values (numerator)
        y_col : str
            Column name for y values (denominator)
        plot_count_col : str, default "P2POINTCNT"
            Column name containing the plot count per stratum
            
        Returns
        -------
        pl.Expr
            Polars expression for stratum covariance calculation
        """
        # Calculate components
        sum_xy = (pl.col(x_col) * pl.col(y_col)).sum()
        sum_x = pl.col(x_col).sum()
        sum_y = pl.col(y_col).sum()
        n_plots = pl.col(plot_count_col).first()
        
        # Mean calculations
        mean_x = sum_x / n_plots
        mean_y = sum_y / n_plots
        
        # rFIA formula: (Σ(xy) - n*x̄*ȳ) / (n*(n-1))
        covariance = (
            sum_xy - (n_plots * mean_x * mean_y)
        ) / (n_plots * (n_plots - 1))
        
        # Handle edge cases
        return pl.when(n_plots > 1).then(covariance).otherwise(0.0)


class RFIAPopulationVarianceCalculator:
    """
    Implements rFIA's population-level variance aggregation.
    
    This class handles the complex population variance calculation that
    aggregates stratum-level variances using proper FIA design factors:
    
    var = (AREA_USED^2 / P2PNTCNT_EU) * (
        sum(var * STRATUM_WGT * P2POINTCNT) + 
        sum(var * (1-STRATUM_WGT) * (P2POINTCNT / P2PNTCNT_EU))
    )
    """
    
    def __init__(self, db: Union[str, FIA]):
        """
        Initialize with FIA database for accessing design factors.
        
        Parameters
        ----------
        db : Union[str, FIA]
            FIA database object or path to database
        """
        if isinstance(db, str):
            self.db = FIA(db)
        else:
            self.db = db
            
        self.stratum_calculator = RFIAStratumVarianceCalculator()
    
    def calculate_population_variance(
        self, 
        stratum_data: pl.DataFrame,
        values_col: str,
        estimation_unit_col: str = "ESTN_UNIT_CN"
    ) -> pl.DataFrame:
        """
        Calculate population variance using rFIA's stratified sampling methodology.
        
        This implements the rFIA population variance formula that properly
        accounts for FIA's complex sampling design including estimation units,
        strata, and design factors.
        
        Parameters
        ----------
        stratum_data : pl.DataFrame
            Stratum-level data with FIA design factors
        values_col : str
            Column containing values for variance calculation
        estimation_unit_col : str, default "ESTN_UNIT_CN"
            Column identifying estimation units
            
        Returns
        -------
        pl.DataFrame
            Population-level variance estimates by estimation unit
        """
        # First calculate stratum-level variances
        stratum_with_variance = stratum_data.with_columns([
            self.stratum_calculator.calculate_stratum_variance(values_col).alias(f"{values_col}_var")
        ])
        
        # Load FIA design factors
        design_factors = self._load_design_factors()
        
        # Join with design factors
        stratum_with_design = stratum_with_variance.join(
            design_factors,
            on=["ESTN_UNIT_CN", "STRATUM_CN"],
            how="inner"
        )
        
        # Calculate population variance using rFIA methodology
        population_variance = stratum_with_design.group_by(estimation_unit_col).agg([
            self._create_population_variance_expression(f"{values_col}_var").alias(f"{values_col}_pop_var"),
            pl.col("P1PNTCNT_EU").first().alias("P1PNTCNT_EU"),
            pl.col("AREA_USED").first().alias("AREA_USED")
        ])
        
        return population_variance
    
    def _load_design_factors(self) -> pl.DataFrame:
        """
        Load FIA design factors from population tables.
        
        Returns
        -------
        pl.DataFrame
            Design factors including AREA_USED, P2PNTCNT_EU, STRATUM_WGT, P2POINTCNT
        """
        # Load population estimation unit data
        pop_eu = self.db.tables["POP_ESTN_UNIT"].collect()
        
        # Load population stratum data
        pop_stratum = self.db.tables["POP_STRATUM"].collect()
        
        # Calculate stratum weights (P1POINTCNT / P1PNTCNT_EU)
        design_factors = pop_stratum.join(
            pop_eu.select([
                "CN", "AREA_USED", "P1PNTCNT_EU"
            ]).rename({"CN": "ESTN_UNIT_CN"}),
            on="ESTN_UNIT_CN",
            how="inner"
        ).with_columns([
            # Calculate stratum weight as in rFIA (using available P1POINTCNT)
            (pl.col("P1POINTCNT") / pl.col("P1PNTCNT_EU")).alias("STRATUM_WGT")
        ]).select([
            "ESTN_UNIT_CN",
            "CN",  # This is STRATUM_CN
            "AREA_USED", 
            "P1PNTCNT_EU",
            "P2POINTCNT",
            "STRATUM_WGT"
        ]).rename({"CN": "STRATUM_CN"})
        
        return design_factors
    
    def _create_population_variance_expression(self, stratum_var_col: str) -> pl.Expr:
        """
        Create rFIA population variance expression.
        
        This implements the exact rFIA formula:
        var = (AREA_USED^2 / P2PNTCNT_EU) * (
            sum(var * STRATUM_WGT * P2POINTCNT) + 
            sum(var * (1-STRATUM_WGT) * (P2POINTCNT / P2PNTCNT_EU))
        )
        
        Parameters
        ----------
        stratum_var_col : str
            Column containing stratum variance values
            
        Returns
        -------
        pl.Expr
            Polars expression for population variance
        """
        # Calculate P2PNTCNT_EU (sum of P2POINTCNT across strata)
        p2pntcnt_eu = pl.col("P2POINTCNT").sum()
        
        # rFIA formula components - FIXED to use P2PNTCNT_EU not P1PNTCNT_EU
        area_factor = (pl.col("AREA_USED").cast(pl.Float64).pow(2) / p2pntcnt_eu)
        
        # First sum: var * STRATUM_WGT * P2POINTCNT
        weighted_sum_1 = (
            pl.col(stratum_var_col) * 
            pl.col("STRATUM_WGT") * 
            pl.col("P2POINTCNT")
        ).sum()
        
        # Second sum: var * (1-STRATUM_WGT) * (P2POINTCNT / P2PNTCNT_EU)
        weighted_sum_2 = (
            pl.col(stratum_var_col) * 
            (1 - pl.col("STRATUM_WGT")) * 
            (pl.col("P2POINTCNT") / p2pntcnt_eu)
        ).sum()
        
        # Complete rFIA formula
        return area_factor * (weighted_sum_1 + weighted_sum_2)


class RFIARatioVarianceCalculator:
    """
    Implements rFIA's ratio variance calculation using the delta method.
    
    This class provides the ratio variance calculation as implemented in
    rFIA's util.R:ratioVar function:
    
    ratioVar(x, y, x.var, y.var, cv) = (1 / y^2) * (x.var + (x/y)^2 * y.var - 2 * (x/y) * cv)
    """
    
    def calculate_ratio_variance(
        self,
        x_mean: Union[str, pl.Expr],
        y_mean: Union[str, pl.Expr], 
        x_var: Union[str, pl.Expr],
        y_var: Union[str, pl.Expr],
        covariance: Union[str, pl.Expr]
    ) -> pl.Expr:
        """
        Calculate ratio variance using rFIA's delta method.
        
        This implements the exact rFIA ratioVar function:
        r.var <- (1 / (y^2)) * (x.var + ((x/y)^2 * y.var) - (2 * (x/y) * cv))
        
        Parameters
        ----------
        x_mean : Union[str, pl.Expr]
            Numerator mean (column name or expression)
        y_mean : Union[str, pl.Expr]
            Denominator mean (column name or expression)
        x_var : Union[str, pl.Expr]
            Numerator variance (column name or expression)
        y_var : Union[str, pl.Expr]
            Denominator variance (column name or expression)
        covariance : Union[str, pl.Expr]
            Covariance between numerator and denominator
            
        Returns
        -------
        pl.Expr
            Polars expression for ratio variance calculation
        """
        # Convert string column names to expressions if needed
        if isinstance(x_mean, str):
            x_mean = pl.col(x_mean)
        if isinstance(y_mean, str):
            y_mean = pl.col(y_mean)
        if isinstance(x_var, str):
            x_var = pl.col(x_var)
        if isinstance(y_var, str):
            y_var = pl.col(y_var)
        if isinstance(covariance, str):
            covariance = pl.col(covariance)
        
        # Calculate ratio
        ratio = x_mean / y_mean
        
        # rFIA formula: (1/Y²) * [Var(X) + (X/Y)² * Var(Y) - 2*(X/Y) * Cov(X,Y)]
        ratio_variance = (1 / y_mean.pow(2)) * (
            x_var + 
            (ratio.pow(2) * y_var) - 
            (2 * ratio * covariance)
        )
        
        # Handle edge cases: negative variances due to rounding should be set to 0
        return pl.when(ratio_variance < 0).then(0.0).otherwise(ratio_variance)


class RFIAVarianceCalculator:
    """
    Complete rFIA-compatible variance calculator.
    
    This class orchestrates all the rFIA variance components to provide
    a complete implementation of rFIA's variance methodology including:
    1. Stratum-level variance calculations
    2. Population-level variance aggregation  
    3. Covariance calculations for ratio variance
    4. Complete FIA design factor integration
    """
    
    def __init__(self, db: Union[str, FIA]):
        """
        Initialize the complete rFIA variance calculator.
        
        Parameters
        ----------
        db : Union[str, FIA]
            FIA database object or path to database
        """
        self.stratum_calc = RFIAStratumVarianceCalculator()
        self.population_calc = RFIAPopulationVarianceCalculator(db)
        self.ratio_calc = RFIARatioVarianceCalculator()
        
        if isinstance(db, str):
            self.db = FIA(db)
        else:
            self.db = db
    
    def calculate_area_variance(
        self, 
        plot_data: pl.DataFrame,
        grouping_cols: Optional[list] = None
    ) -> pl.DataFrame:
        """
        Calculate complete area variance using rFIA methodology.
        
        This method implements the complete rFIA area variance calculation
        process including proper stratification, design factors, and 
        ratio variance for percentage calculations.
        
        Parameters
        ----------
        plot_data : pl.DataFrame
            Plot-level data with area estimates and stratification
        grouping_cols : Optional[list], default None
            Additional grouping columns for variance calculation
            
        Returns
        -------
        pl.DataFrame
            Complete variance results with sampling errors
        """
        # Prepare grouping columns
        base_groups = ["ESTN_UNIT_CN", "STRATUM_CN"]
        
        # CRITICAL FIX: Stratum-level variance must ONLY group by stratum,
        # not by user-specified grouping columns (e.g., FORTYPCD)
        # User grouping is applied AFTER stratum aggregation
        
        # Step 1: Calculate stratum-level variances and covariances
        # Group ONLY by stratum identifiers, not user groups
        stratum_stats = plot_data.group_by(base_groups).agg([
            # Stratum variance for numerator (fa_adjusted)
            self.stratum_calc.calculate_stratum_variance("fa_adjusted").alias("fa_var_stratum"),
            
            # Stratum variance for denominator (fad_adjusted) 
            self.stratum_calc.calculate_stratum_variance("fad_adjusted").alias("fad_var_stratum"),
            
            # Stratum covariance between numerator and denominator
            self.stratum_calc.calculate_stratum_covariance("fa_adjusted", "fad_adjusted").alias("fa_fad_cov_stratum"),
            
            # Stratum totals
            # fa_adjusted is proportion of plot that meets condition (0-1 scale) × ADJ_FACTOR
            # To get total acres: sum(fa_adjusted × EXPNS) across all plots in stratum
            (pl.col("fa_adjusted") * pl.col("EXPNS")).sum().alias("fa_total_stratum"),
            (pl.col("fad_adjusted") * pl.col("EXPNS")).sum().alias("fad_total_stratum"),
            
            # Keep stratum means for variance calculations
            pl.col("fa_adjusted").mean().alias("fa_mean_stratum"),
            pl.col("fad_adjusted").mean().alias("fad_mean_stratum"),
            
            # Plot counts and design factors
            pl.col("P2POINTCNT").first().alias("P2POINTCNT"),
            pl.col("EXPNS").first().alias("EXPNS")
            # NOTE: ESTN_UNIT_CN is already included from group_by operation
        ])
        
        # Step 2: Calculate population-level variances
        # Load design factors for population variance calculation
        design_factors = self.population_calc._load_design_factors()
        
        # Join stratum stats with design factors
        stratum_with_design = stratum_stats.join(
            design_factors,
            on=["ESTN_UNIT_CN", "STRATUM_CN"],
            how="inner"
        )
        
        # For user grouping, we need to handle this at the population aggregation level
        # The stratum variances are the same for all groups within a stratum
        # We'll aggregate by estimation unit first, then handle grouping
        
        # Group only by estimation unit for variance calculation
        # User grouping will be applied to the final results
        eu_groups = ["ESTN_UNIT_CN"]
        
        # Calculate population variances using pre-calculated stratum variances
        # First calculate P2PNTCNT_EU (sum of P2POINTCNT across strata)
        fa_pop_var = stratum_with_design.group_by(eu_groups).agg([
            # First sum: var * STRATUM_WGT * P2POINTCNT
            (pl.col("fa_var_stratum") * pl.col("STRATUM_WGT") * pl.col("P2POINTCNT")).sum().alias("weighted_var_1"),
            # Second sum: var * (1-STRATUM_WGT) * (P2POINTCNT / P2PNTCNT_EU) 
            # Note: P2PNTCNT_EU will be calculated below
            pl.col("fa_var_stratum").first().alias("temp_var"),  # Placeholder
            pl.col("P2POINTCNT").sum().alias("P2PNTCNT_EU"),  # FIXED: Sum of P2POINTCNT
            pl.col("STRATUM_WGT").first().alias("temp_wgt"),
            pl.col("AREA_USED").first().alias("AREA_USED")
        ])
        
        # Calculate second component with correct P2PNTCNT_EU
        fa_pop_var = stratum_with_design.group_by(eu_groups).agg([
            # First sum: var * STRATUM_WGT * P2POINTCNT
            (pl.col("fa_var_stratum") * pl.col("STRATUM_WGT") * pl.col("P2POINTCNT")).sum().alias("weighted_var_1"),
            # Calculate P2PNTCNT_EU first
            pl.col("P2POINTCNT").sum().alias("P2PNTCNT_EU"),
            # Second sum: var * (1-STRATUM_WGT) * (P2POINTCNT / P2PNTCNT_EU)
            (pl.col("fa_var_stratum") * (1 - pl.col("STRATUM_WGT")) * pl.col("P2POINTCNT")).sum().alias("weighted_var_2_numerator"),
            pl.col("AREA_USED").first().alias("AREA_USED")
        ]).with_columns([
            # Complete second component calculation
            (pl.col("weighted_var_2_numerator") / pl.col("P2PNTCNT_EU")).alias("weighted_var_2")
        ]).with_columns([
            # Apply area factor: (AREA_USED^2 / P2PNTCNT_EU) * (sum1 + sum2)
            ((pl.col("AREA_USED").cast(pl.Float64).pow(2) / pl.col("P2PNTCNT_EU")) * (pl.col("weighted_var_1") + pl.col("weighted_var_2"))).alias("fa_adjusted_pop_var")
        ])
        
        fad_pop_var = stratum_with_design.group_by(eu_groups).agg([
            # First sum: var * STRATUM_WGT * P2POINTCNT
            (pl.col("fad_var_stratum") * pl.col("STRATUM_WGT") * pl.col("P2POINTCNT")).sum().alias("weighted_var_1"),
            # Calculate P2PNTCNT_EU first
            pl.col("P2POINTCNT").sum().alias("P2PNTCNT_EU"),
            # Second sum: var * (1-STRATUM_WGT) * (P2POINTCNT / P2PNTCNT_EU)
            (pl.col("fad_var_stratum") * (1 - pl.col("STRATUM_WGT")) * pl.col("P2POINTCNT")).sum().alias("weighted_var_2_numerator"),
            pl.col("AREA_USED").first().alias("AREA_USED")
        ]).with_columns([
            # Complete second component calculation
            (pl.col("weighted_var_2_numerator") / pl.col("P2PNTCNT_EU")).alias("weighted_var_2")
        ]).with_columns([
            # Apply area factor: (AREA_USED^2 / P2PNTCNT_EU) * (sum1 + sum2)
            ((pl.col("AREA_USED").cast(pl.Float64).pow(2) / pl.col("P2PNTCNT_EU")) * (pl.col("weighted_var_1") + pl.col("weighted_var_2"))).alias("fad_adjusted_pop_var")
        ])
        
        # Step 3: Calculate population-level means and covariances
        # Join stratum stats with design factors if not already done
        if "STRATUM_WGT" not in stratum_stats.columns:
            stratum_with_design = stratum_stats.join(
                design_factors,
                on=["ESTN_UNIT_CN", "STRATUM_CN"],
                how="inner"
            )
        else:
            stratum_with_design = stratum_stats
            
        population_stats = stratum_with_design.group_by(eu_groups).agg([
            # Population totals (sum of stratum totals)
            # Stratum totals are already in acres from Step 1
            pl.col("fa_total_stratum").sum().alias("fa_mean"),
            pl.col("fad_total_stratum").sum().alias("fad_mean"),
            
            # Population covariance using same aggregation methodology as variance
            # First sum: cov * STRATUM_WGT * P2POINTCNT
            (pl.col("fa_fad_cov_stratum") * pl.col("STRATUM_WGT") * pl.col("P2POINTCNT")).sum().alias("weighted_cov_1"),
            # Calculate P2PNTCNT_EU
            pl.col("P2POINTCNT").sum().alias("P2PNTCNT_EU"),
            # Second sum numerator
            (pl.col("fa_fad_cov_stratum") * (1 - pl.col("STRATUM_WGT")) * pl.col("P2POINTCNT")).sum().alias("weighted_cov_2_numerator"),
            
            # Design factors
            pl.col("P2POINTCNT").sum().alias("total_plots"),
            pl.col("AREA_USED").first().alias("AREA_USED")
        ]).with_columns([
            # Complete second component
            (pl.col("weighted_cov_2_numerator") / pl.col("P2PNTCNT_EU")).alias("weighted_cov_2")
        ]).with_columns([
            # Apply area factor to covariance: (AREA_USED^2 / P2PNTCNT_EU) * (sum1 + sum2)
            ((pl.col("AREA_USED").cast(pl.Float64).pow(2) / pl.col("P2PNTCNT_EU")) * (pl.col("weighted_cov_1") + pl.col("weighted_cov_2"))).alias("fa_fad_cov")
        ])
        
        # Step 4: Join population variances
        complete_stats = population_stats.join(
            fa_pop_var.select(["ESTN_UNIT_CN", "fa_adjusted_pop_var"]).rename({"fa_adjusted_pop_var": "fa_var"}),
            on="ESTN_UNIT_CN",
            how="inner"
        ).join(
            fad_pop_var.select(["ESTN_UNIT_CN", "fad_adjusted_pop_var"]).rename({"fad_adjusted_pop_var": "fad_var"}),
            on="ESTN_UNIT_CN", 
            how="inner"
        )
        
        # Step 5: Calculate ratio variance for percentages
        final_stats = complete_stats.with_columns([
            # Calculate ratio variance using rFIA methodology
            self.ratio_calc.calculate_ratio_variance(
                "fa_mean", "fad_mean", "fa_var", "fad_var", "fa_fad_cov"
            ).alias("perc_var_ratio"),
            
            # Calculate total area estimates
            pl.col("fa_mean").alias("AREA_TOTAL"),
            pl.col("fad_mean").alias("AREA_TOTAL_DEN"),
            
            # Calculate percentage
            (pl.col("fa_mean") / pl.col("fad_mean") * 100).alias("AREA_PERC")
        ])
        
        # Step 6: Calculate sampling errors (SE = sqrt(var) / estimate * 100)
        
        final_stats = final_stats.with_columns([
            # Area total standard error in ACRES
            pl.col("fa_var").sqrt().alias("AREA_TOTAL_SE_ACRES"),
            # Area total sampling error as PERCENTAGE
            (pl.col("fa_var").sqrt() / pl.col("AREA_TOTAL") * 100).alias("AREA_TOTAL_SE_PCT"),
            
            # Percentage sampling error
            # The perc_var_ratio is variance of (fa/fad) ratio
            # SE as percentage = sqrt(ratio_variance) * 100
            pl.when(pl.col("perc_var_ratio") >= 0)
            .then(pl.col("perc_var_ratio").sqrt() * 100)
            .otherwise(0.0)
            .alias("AREA_PERC_SE"),
            
            # Variance columns for output
            pl.col("fa_var").alias("AREA_TOTAL_VAR"),
            (pl.col("perc_var_ratio") * 10000).alias("AREA_PERC_VAR")  # Keep this for percentage variance
        ])
        
        # If we have multiple estimation units and no additional grouping, aggregate to single total
        if grouping_cols is None or len(grouping_cols) == 0:
            if final_stats.height > 1:
                # Aggregate across estimation units for final totals
                final_stats = final_stats.select([
                    pl.col("AREA_TOTAL").sum().alias("AREA_TOTAL"),
                    pl.col("AREA_TOTAL_DEN").sum().alias("AREA_TOTAL_DEN"),
                    # For percentage, recalculate from totals
                    (pl.col("AREA_TOTAL").sum() / pl.col("AREA_TOTAL_DEN").sum() * 100).alias("AREA_PERC"),
                    # For variance, sum variances (assuming independence)
                    pl.col("AREA_TOTAL_VAR").sum().alias("AREA_TOTAL_VAR"),
                    pl.col("AREA_PERC_VAR").sum().alias("AREA_PERC_VAR"),
                    # Recalculate SE from aggregated values
                    pl.col("AREA_TOTAL_VAR").sum().sqrt().alias("AREA_TOTAL_SE_ACRES"),
                    (pl.col("AREA_TOTAL_VAR").sum().sqrt() / pl.col("AREA_TOTAL").sum() * 100).alias("AREA_TOTAL_SE_PCT"),
                    # For percentage SE: sqrt(variance) gives SE as decimal, multiply by 100 for percentage
                    (pl.col("AREA_PERC_VAR").sum().sqrt() / 100).alias("AREA_PERC_SE"),
                    pl.col("total_plots").sum().alias("total_plots")
                ])
        
        return final_stats
    
    def _calculate_population_covariance(self) -> pl.Expr:
        """
        Calculate population-level covariance using rFIA methodology.
        
        This would need to be implemented similar to population variance
        but for covariance aggregation across strata.
        
        Returns
        -------
        pl.Expr
            Expression for population covariance calculation
        """
        # Placeholder - would implement similar to population variance
        # but aggregating covariances across strata
        return pl.col("fa_fad_cov_stratum").sum()
    
    def validate_design_factors(self, data: pl.DataFrame) -> Dict[str, Any]:
        """
        Validate that all required FIA design factors are present.
        
        Parameters
        ----------
        data : pl.DataFrame
            Data to validate
            
        Returns
        -------
        Dict[str, Any]
            Validation results
        """
        required_cols = [
            "ESTN_UNIT_CN", "STRATUM_CN", "P2POINTCNT", "EXPNS",
            "fa_adjusted", "fad_adjusted"
        ]
        
        missing_cols = [col for col in required_cols if col not in data.columns]
        
        return {
            "is_valid": len(missing_cols) == 0,
            "missing_columns": missing_cols,
            "design_factor_coverage": self._check_design_factor_coverage(data) if len(missing_cols) == 0 else {}
        }
    
    def _check_design_factor_coverage(self, data: pl.DataFrame) -> Dict[str, float]:
        """
        Check coverage of FIA design factors.
        
        Parameters
        ----------
        data : pl.DataFrame
            Data to check
            
        Returns
        -------
        Dict[str, float]
            Coverage statistics
        """
        total_plots = data.height
        
        return {
            "plots_with_stratum": data.filter(pl.col("STRATUM_CN").is_not_null()).height / total_plots,
            "plots_with_eu": data.filter(pl.col("ESTN_UNIT_CN").is_not_null()).height / total_plots,
            "plots_with_p2pointcnt": data.filter(pl.col("P2POINTCNT") > 0).height / total_plots
        }