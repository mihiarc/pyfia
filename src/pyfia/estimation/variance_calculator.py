"""
Unified variance calculator for FIA estimation procedures.

This module consolidates variance calculation implementations from across the
estimation module into a single, comprehensive component following FIA statistical
procedures from Bechtold & Patterson (2005).

Key Features:
- Two-stage variance calculation for stratified sampling
- Ratio-of-means variance using the delta method
- Domain estimation variance with proper subset handling
- Covariance calculations for ratio estimators
- Proper handling of missing strata and edge cases
- Efficient implementation using Polars LazyFrame

Statistical Background:
The FIA uses a two-phase stratified sampling design where:
1. Phase 1: Stratification using remote sensing or other auxiliary data
2. Phase 2: Field plot measurements within strata

The variance estimation follows standard stratified sampling theory with
post-stratification adjustments for the two-phase design.

References:
- Bechtold, W.A. and Patterson, P.L., 2005. The enhanced forest inventory and
  analysis program - national sampling design and estimation procedures.
  Gen. Tech. Rep. SRS-80. Asheville, NC: USDA Forest Service, Southern Research Station.
"""

from typing import Dict, List, Optional, Union, Tuple
import polars as pl
from ..core import FIA


class FIAVarianceCalculator:
    """
    Comprehensive variance calculator for FIA estimation procedures.
    
    This class provides a unified interface for all variance calculations needed
    in FIA estimation, consolidating previously duplicated implementations into
    a single, well-tested component.
    
    The calculator supports:
    - Stratified sampling variance with FIA design factors
    - Ratio variance using the delta method
    - Domain estimation variance
    - Proper handling of edge cases and missing data
    
    Examples
    --------
    >>> # Initialize calculator
    >>> var_calc = FIAVarianceCalculator(db)
    >>> 
    >>> # Calculate stratum variance
    >>> stratum_var = var_calc.calculate_stratum_variance(
    ...     data, response_col="VOLCFNET_ACRE", group_cols=["SPCD"]
    ... )
    >>> 
    >>> # Calculate population variance
    >>> pop_var = var_calc.calculate_population_variance(stratum_var)
    >>> 
    >>> # Calculate ratio variance for per-acre estimates
    >>> ratio_var = var_calc.calculate_ratio_variance(
    ...     numerator_mean=100, denominator_mean=50,
    ...     numerator_var=10, denominator_var=5, covariance=2
    ... )
    """
    
    def __init__(self, db: Optional[Union[str, FIA]] = None):
        """
        Initialize the variance calculator.
        
        Parameters
        ----------
        db : Union[str, FIA], optional
            FIA database for loading design factors. Can be provided later
            if design factors are passed directly to methods.
        """
        if db is not None:
            self.db = db if isinstance(db, FIA) else FIA(db)
        else:
            self.db = None
    
    def calculate_stratum_variance(
        self,
        data: pl.DataFrame,
        response_col: str,
        weight_col: str = "EXPNS",
        plot_count_col: str = "P2POINTCNT",
        group_cols: Optional[List[str]] = None
    ) -> pl.DataFrame:
        """
        Calculate stratum-level variance components.
        
        Implements the stratified sampling variance formula:
        s²_h = Σ(y_i - ȳ_h)² / (n_h - 1)
        
        This is computed efficiently as:
        s²_h = [Σy_i² - n_h * ȳ_h²] / (n_h - 1)
        
        Parameters
        ----------
        data : pl.DataFrame
            Plot-level data with response variable and stratification
        response_col : str
            Column containing the response variable (e.g., volume per acre)
        weight_col : str, default "EXPNS"
            Column containing expansion factors
        plot_count_col : str, default "P2POINTCNT"
            Column containing plot counts per stratum
        group_cols : List[str], optional
            Additional grouping columns (e.g., species, size class)
            
        Returns
        -------
        pl.DataFrame
            Stratum-level statistics including variance, mean, and sample size
            
        Notes
        -----
        The variance calculation uses the standard unbiased estimator with
        Bessel's correction (dividing by n-1 instead of n).
        """
        # Ensure numeric types
        numeric_conversions = [
            pl.col(response_col).cast(pl.Float64),
            pl.col(weight_col).cast(pl.Float64)
        ]
        
        # Add stratification columns if present
        if "STRATUM_CN" in data.columns:
            numeric_conversions.append(pl.col("STRATUM_CN").cast(pl.Int64))
        if "ESTN_UNIT_CN" in data.columns:
            numeric_conversions.append(pl.col("ESTN_UNIT_CN").cast(pl.Int64))
        
        data = data.with_columns(numeric_conversions)
        
        # Build grouping columns
        group_by = []
        if "STRATUM_CN" in data.columns:
            group_by.append("STRATUM_CN")
        if "ESTN_UNIT_CN" in data.columns:
            group_by.append("ESTN_UNIT_CN")
        
        if group_cols:
            # Add additional grouping columns that exist in the data
            available_groups = [col for col in group_cols if col in data.columns]
            group_by.extend(available_groups)
        
        if not group_by:
            # If no grouping columns, treat entire dataset as one stratum
            group_by = [pl.lit(1).alias("_dummy_group")]
            data = data.with_columns(group_by)
            group_by = ["_dummy_group"]
        
        # Calculate stratum-level statistics
        stratum_stats = data.group_by(group_by).agg([
            # Number of plots in stratum
            pl.len().alias("n_h"),
            
            # Sum of response values
            pl.col(response_col).sum().alias("y_sum"),
            
            # Sum of squared response values
            (pl.col(response_col).pow(2)).sum().alias("y_sum_sq"),
            
            # Mean response
            pl.col(response_col).mean().alias("y_mean"),
            
            # Keep first value of design factors for later use
            pl.col(weight_col).first().alias(weight_col)
        ])
        
        # Add plot count if available
        if plot_count_col in data.columns:
            stratum_stats = stratum_stats.join(
                data.group_by(group_by).agg(
                    pl.col(plot_count_col).first().alias(plot_count_col)
                ),
                on=group_by,
                how="left"
            )
        
        # Calculate variance using the computational formula
        stratum_stats = stratum_stats.with_columns([
            # Degrees of freedom
            (pl.col("n_h") - 1).alias("df"),
            
            # Variance: s²_h = [Σy_i² - n_h * ȳ_h²] / (n_h - 1)
            pl.when(pl.col("n_h") > 1)
            .then(
                (pl.col("y_sum_sq") - (pl.col("n_h") * pl.col("y_mean").pow(2))) / 
                (pl.col("n_h") - 1)
            )
            .otherwise(0.0)
            .alias("stratum_var"),
            
            # Standard error of the mean
            pl.when(pl.col("n_h") > 1)
            .then(
                ((pl.col("y_sum_sq") - (pl.col("n_h") * pl.col("y_mean").pow(2))) / 
                 (pl.col("n_h") * (pl.col("n_h") - 1))).sqrt()
            )
            .otherwise(0.0)
            .alias("stratum_se")
        ])
        
        # Remove dummy group column if it was added
        if "_dummy_group" in stratum_stats.columns:
            stratum_stats = stratum_stats.drop("_dummy_group")
        
        return stratum_stats
    
    def calculate_stratum_covariance(
        self,
        data: pl.DataFrame,
        x_col: str,
        y_col: str,
        group_cols: Optional[List[str]] = None
    ) -> pl.DataFrame:
        """
        Calculate stratum-level covariance between two variables.
        
        Implements the unbiased covariance estimator:
        cov(X,Y) = Σ[(x_i - x̄)(y_i - ȳ)] / (n - 1)
        
        Computed efficiently as:
        cov(X,Y) = [Σ(x_i * y_i) - n * x̄ * ȳ] / (n - 1)
        
        Parameters
        ----------
        data : pl.DataFrame
            Plot-level data with both variables
        x_col : str
            First variable column name (e.g., forest area)
        y_col : str
            Second variable column name (e.g., total area)
        group_cols : List[str], optional
            Grouping columns for stratification
            
        Returns
        -------
        pl.DataFrame
            Stratum-level covariance statistics
        """
        # Ensure numeric types
        data = data.with_columns([
            pl.col(x_col).cast(pl.Float64),
            pl.col(y_col).cast(pl.Float64)
        ])
        
        # Build grouping columns
        group_by = []
        if "STRATUM_CN" in data.columns:
            group_by.append("STRATUM_CN")
        if "ESTN_UNIT_CN" in data.columns:
            group_by.append("ESTN_UNIT_CN")
        
        if group_cols:
            available_groups = [col for col in group_cols if col in data.columns]
            group_by.extend(available_groups)
        
        if not group_by:
            group_by = [pl.lit(1).alias("_dummy_group")]
            data = data.with_columns(group_by)
            group_by = ["_dummy_group"]
        
        # Calculate covariance components
        cov_stats = data.group_by(group_by).agg([
            # Number of observations
            pl.len().alias("n"),
            
            # Sum of x * y
            (pl.col(x_col) * pl.col(y_col)).sum().alias("sum_xy"),
            
            # Means
            pl.col(x_col).mean().alias("x_mean"),
            pl.col(y_col).mean().alias("y_mean"),
            
            # Also calculate variances for completeness
            pl.col(x_col).var().alias("x_var"),
            pl.col(y_col).var().alias("y_var")
        ])
        
        # Calculate covariance
        cov_stats = cov_stats.with_columns([
            # Covariance: [Σ(xy) - n*x̄*ȳ] / (n-1)
            pl.when(pl.col("n") > 1)
            .then(
                (pl.col("sum_xy") - (pl.col("n") * pl.col("x_mean") * pl.col("y_mean"))) /
                (pl.col("n") - 1)
            )
            .otherwise(0.0)
            .alias("covariance"),
            
            # Correlation coefficient for diagnostics
            pl.when((pl.col("x_var") > 0) & (pl.col("y_var") > 0) & (pl.col("n") > 1))
            .then(
                ((pl.col("sum_xy") - (pl.col("n") * pl.col("x_mean") * pl.col("y_mean"))) /
                 ((pl.col("n") - 1) * pl.col("x_var").sqrt() * pl.col("y_var").sqrt()))
            )
            .otherwise(0.0)
            .alias("correlation")
        ])
        
        # Remove dummy group if added
        if "_dummy_group" in cov_stats.columns:
            cov_stats = cov_stats.drop("_dummy_group")
        
        return cov_stats
    
    def calculate_population_variance(
        self,
        stratum_data: pl.DataFrame,
        variance_col: str = "stratum_var",
        n_col: str = "n_h",
        group_cols: Optional[List[str]] = None,
        design_factors: Optional[pl.DataFrame] = None
    ) -> pl.DataFrame:
        """
        Calculate population-level variance from stratum variances.
        
        Implements the FIA two-phase stratified sampling variance formula:
        Var(Ŷ) = (A²/n) * [Σ(w_h * n_h * s²_h) + (1/n) * Σ((1-w_h) * n_h * s²_h)]
        
        Where:
        - A = total area in estimation unit
        - n = total plots in estimation unit (P2PNTCNT_EU)
        - w_h = stratum weight (P1POINTCNT / P1PNTCNT_EU)
        - n_h = plots in stratum h
        - s²_h = stratum variance
        
        Parameters
        ----------
        stratum_data : pl.DataFrame
            Stratum-level data with variance components
        variance_col : str, default "stratum_var"
            Column containing stratum variance values
        n_col : str, default "n_h"
            Column containing stratum sample sizes
        group_cols : List[str], optional
            Additional grouping columns beyond estimation unit
        design_factors : pl.DataFrame, optional
            FIA design factors. If not provided, will load from database
            
        Returns
        -------
        pl.DataFrame
            Population-level variance estimates
        """
        # Load design factors if not provided
        if design_factors is None:
            if self.db is None:
                raise ValueError("Either design_factors or database connection required")
            design_factors = self._load_design_factors()
        
        # Join stratum data with design factors
        join_cols = []
        if "ESTN_UNIT_CN" in stratum_data.columns:
            join_cols.append("ESTN_UNIT_CN")
        if "STRATUM_CN" in stratum_data.columns:
            join_cols.append("STRATUM_CN")
        
        if join_cols:
            data_with_factors = stratum_data.join(
                design_factors,
                on=join_cols,
                how="inner"
            )
        else:
            # If no stratification, use unstratified formula
            return self._calculate_unstratified_variance(stratum_data, variance_col, n_col)
        
        # Build grouping columns
        group_by = ["ESTN_UNIT_CN"] if "ESTN_UNIT_CN" in data_with_factors.columns else []
        if group_cols:
            available_groups = [col for col in group_cols if col in data_with_factors.columns]
            group_by.extend(available_groups)
        
        if not group_by:
            group_by = [pl.lit(1).alias("_total")]
            data_with_factors = data_with_factors.with_columns(group_by)
            group_by = ["_total"]
        
        # Calculate population variance components
        pop_variance = data_with_factors.group_by(group_by).agg([
            # Total area
            pl.col("AREA_USED").first().alias("total_area"),
            
            # Total plots in estimation unit (P2PNTCNT_EU)
            pl.col("P2PNTCNT_EU").first().alias("n_total"),
            
            # First variance component: Σ(w_h * n_h * s²_h)
            (pl.col("STRATUM_WGT") * pl.col(n_col) * pl.col(variance_col)).sum()
            .alias("var_component_1"),
            
            # Second variance component: Σ((1-w_h) * n_h * s²_h)
            ((1 - pl.col("STRATUM_WGT")) * pl.col(n_col) * pl.col(variance_col)).sum()
            .alias("var_component_2"),
            
            # Total estimate (sum of stratum estimates)
            (pl.col("y_mean") * pl.col("EXPNS") * pl.col(n_col)).sum()
            .alias("population_total")
        ])
        
        # Calculate final variance
        pop_variance = pop_variance.with_columns([
            # Variance: (A²/n) * [comp1 + (1/n) * comp2]
            pl.when(pl.col("n_total") > 0)
            .then(
                (pl.col("total_area").pow(2) / pl.col("n_total")) * 
                (pl.col("var_component_1") + 
                 (1.0 / pl.col("n_total")) * pl.col("var_component_2"))
            )
            .otherwise(0.0)
            .alias("population_var"),
            
            # Population mean
            (pl.col("population_total") / pl.col("total_area"))
            .alias("population_mean")
        ])
        
        # Calculate standard error and CV
        pop_variance = pop_variance.with_columns([
            pl.col("population_var").sqrt().alias("population_se"),
            
            pl.when((pl.col("population_mean") != 0) & (pl.col("population_mean").is_not_null()))
            .then((pl.col("population_var").sqrt() / pl.col("population_mean").abs()) * 100)
            .otherwise(0.0)
            .alias("cv_percent")
        ])
        
        # Remove dummy column if added
        if "_total" in pop_variance.columns:
            pop_variance = pop_variance.drop("_total")
        
        return pop_variance
    
    def calculate_ratio_variance(
        self,
        numerator_mean: Union[float, pl.Expr],
        denominator_mean: Union[float, pl.Expr],
        numerator_var: Union[float, pl.Expr],
        denominator_var: Union[float, pl.Expr],
        covariance: Union[float, pl.Expr]
    ) -> Union[float, pl.Expr]:
        """
        Calculate variance of a ratio using the delta method.
        
        For ratio R = X/Y, the variance is approximately:
        Var(R) ≈ (1/Y²) * [Var(X) + R² * Var(Y) - 2 * R * Cov(X,Y)]
        
        This is the standard delta method approximation for ratio variance,
        suitable for large samples where the coefficient of variation of Y
        is small (typically < 10%).
        
        Parameters
        ----------
        numerator_mean : float or pl.Expr
            Mean of numerator (X̄)
        denominator_mean : float or pl.Expr
            Mean of denominator (Ȳ)
        numerator_var : float or pl.Expr
            Variance of numerator (Var(X))
        denominator_var : float or pl.Expr
            Variance of denominator (Var(Y))
        covariance : float or pl.Expr
            Covariance between numerator and denominator (Cov(X,Y))
            
        Returns
        -------
        float or pl.Expr
            Approximate variance of the ratio
            
        Notes
        -----
        The delta method provides a first-order Taylor approximation.
        For more accurate results with small samples or high CV, consider
        using bootstrap or jackknife methods.
        """
        # Handle scalar inputs
        if all(isinstance(x, (int, float)) for x in 
               [numerator_mean, denominator_mean, numerator_var, denominator_var, covariance]):
            if denominator_mean == 0:
                return 0.0
            ratio = numerator_mean / denominator_mean
            var_ratio = (1 / denominator_mean**2) * (
                numerator_var + 
                ratio**2 * denominator_var - 
                2 * ratio * covariance
            )
            # Ensure non-negative (can be negative due to rounding in covariance)
            return max(0.0, var_ratio)
        
        # Handle Polars expressions
        ratio = numerator_mean / denominator_mean
        var_ratio = (
            pl.when(denominator_mean == 0)
            .then(0.0)
            .otherwise(
                (1 / denominator_mean.pow(2)) * (
                    numerator_var + 
                    ratio.pow(2) * denominator_var - 
                    2 * ratio * covariance
                )
            )
        )
        
        # Ensure non-negative result
        return pl.when(var_ratio < 0).then(0.0).otherwise(var_ratio)
    
    def calculate_domain_variance(
        self,
        data: pl.DataFrame,
        domain_col: str,
        response_col: str,
        variance_col: str,
        group_cols: Optional[List[str]] = None
    ) -> pl.DataFrame:
        """
        Calculate variance for domain (subset) estimation.
        
        Domain estimation involves calculating estimates for a subset of the
        population (e.g., a specific species or size class). The variance must
        account for the domain being a random subset of plots.
        
        Parameters
        ----------
        data : pl.DataFrame
            Data with domain indicators
        domain_col : str
            Column indicating domain membership (1 = in domain, 0 = not)
        response_col : str
            Response variable column
        variance_col : str
            Column containing variance estimates
        group_cols : List[str], optional
            Additional grouping columns
            
        Returns
        -------
        pl.DataFrame
            Domain-specific variance estimates
        """
        # Domain estimation treats the domain indicator as the response
        # Calculate variance of (domain_indicator * response)
        data = data.with_columns([
            (pl.col(domain_col) * pl.col(response_col)).alias("domain_response")
        ])
        
        # Calculate stratum variance for domain
        domain_var = self.calculate_stratum_variance(
            data,
            response_col="domain_response",
            group_cols=group_cols
        )
        
        # Adjust for domain proportion if needed
        domain_var = domain_var.with_columns([
            # Domain sample size
            (pl.col("n_h") * pl.col(domain_col).mean()).alias("n_domain"),
            
            # Effective variance includes uncertainty in domain membership
            pl.col("stratum_var").alias("domain_var")
        ])
        
        return domain_var
    
    def calculate_two_stage_variance(
        self,
        first_stage_var: Union[float, pl.Expr],
        second_stage_var: Union[float, pl.Expr],
        n1: Union[int, pl.Expr],
        n2: Union[int, pl.Expr]
    ) -> Union[float, pl.Expr]:
        """
        Calculate variance for two-stage sampling design.
        
        In FIA's two-phase design:
        - Phase 1: Stratification (n1 points)
        - Phase 2: Field plots (n2 plots, subset of n1)
        
        Total variance = Var₁ + (n1/n2) * Var₂
        
        Parameters
        ----------
        first_stage_var : float or pl.Expr
            Variance from first stage (stratification uncertainty)
        second_stage_var : float or pl.Expr
            Variance from second stage (within-stratum variance)
        n1 : int or pl.Expr
            First stage sample size
        n2 : int or pl.Expr
            Second stage sample size
            
        Returns
        -------
        float or pl.Expr
            Combined two-stage variance
        """
        if isinstance(first_stage_var, (int, float)):
            if n2 == 0:
                return float('inf')
            return first_stage_var + (n1 / n2) * second_stage_var
        
        # Polars expression
        return (
            pl.when(n2 == 0)
            .then(float('inf'))
            .otherwise(first_stage_var + (n1 / n2) * second_stage_var)
        )
    
    def _load_design_factors(self) -> pl.DataFrame:
        """
        Load FIA design factors from population tables.
        
        Returns
        -------
        pl.DataFrame
            Design factors including weights and sample sizes
        """
        if self.db is None:
            raise ValueError("Database connection required to load design factors")
        
        # Load estimation unit data
        pop_eu = self.db.tables["POP_ESTN_UNIT"].collect()
        pop_eu = pop_eu.select([
            pl.col("CN").alias("ESTN_UNIT_CN"),
            "AREA_USED",
            "P1PNTCNT_EU",
            "P2PNTCNT_EU"
        ])
        
        # Load stratum data
        pop_stratum = self.db.tables["POP_STRATUM"].collect()
        pop_stratum = pop_stratum.select([
            pl.col("CN").alias("STRATUM_CN"),
            "ESTN_UNIT_CN",
            "P1POINTCNT",
            "P2POINTCNT",
            "EXPNS",
            "ADJ_FACTOR_SUBP",
            "ADJ_FACTOR_MICR",
            "ADJ_FACTOR_MACR"
        ])
        
        # Join and calculate weights
        design_factors = pop_stratum.join(
            pop_eu,
            on="ESTN_UNIT_CN",
            how="inner"
        ).with_columns([
            # Stratum weight (proportion of phase 1 points)
            (pl.col("P1POINTCNT") / pl.col("P1PNTCNT_EU")).alias("STRATUM_WGT"),
            
            # Sampling fraction
            (pl.col("P2POINTCNT") / pl.col("P1POINTCNT")).alias("sampling_fraction")
        ])
        
        return design_factors
    
    def _calculate_unstratified_variance(
        self,
        data: pl.DataFrame,
        variance_col: str,
        n_col: str
    ) -> pl.DataFrame:
        """
        Calculate variance for unstratified (simple random) sampling.
        
        Used when no stratification information is available.
        
        Parameters
        ----------
        data : pl.DataFrame
            Data with variance components
        variance_col : str
            Column containing variance values
        n_col : str
            Column containing sample sizes
            
        Returns
        -------
        pl.DataFrame
            Unstratified variance estimate
        """
        result = data.select([
            pl.col(variance_col).mean().alias("population_var"),
            pl.col(n_col).sum().alias("n_total"),
            pl.col("y_mean").mean().alias("population_mean") if "y_mean" in data.columns else pl.lit(0)
        ])
        
        # Add standard error and CV
        result = result.with_columns([
            pl.col("population_var").sqrt().alias("population_se"),
            
            pl.when((pl.col("population_mean") != 0) & (pl.col("population_mean").is_not_null()))
            .then((pl.col("population_var").sqrt() / pl.col("population_mean").abs()) * 100)
            .otherwise(0.0)
            .alias("cv_percent")
        ])
        
        return result
    
    def validate_variance_inputs(
        self,
        data: pl.DataFrame,
        required_cols: List[str]
    ) -> Tuple[bool, List[str]]:
        """
        Validate that required columns exist for variance calculation.
        
        Parameters
        ----------
        data : pl.DataFrame
            Data to validate
        required_cols : List[str]
            Required column names
            
        Returns
        -------
        Tuple[bool, List[str]]
            (is_valid, missing_columns)
        """
        missing_cols = [col for col in required_cols if col not in data.columns]
        return len(missing_cols) == 0, missing_cols
    
    def calculate_confidence_interval(
        self,
        estimate: Union[float, pl.Expr],
        variance: Union[float, pl.Expr],
        confidence_level: float = 0.95,
        df: Optional[Union[int, pl.Expr]] = None
    ) -> Union[Tuple[float, float], Tuple[pl.Expr, pl.Expr]]:
        """
        Calculate confidence interval for an estimate.
        
        Parameters
        ----------
        estimate : float or pl.Expr
            Point estimate
        variance : float or pl.Expr
            Variance of the estimate
        confidence_level : float, default 0.95
            Confidence level (e.g., 0.95 for 95% CI)
        df : int or pl.Expr, optional
            Degrees of freedom. If None, uses normal approximation
            
        Returns
        -------
        Tuple[float, float] or Tuple[pl.Expr, pl.Expr]
            (lower_bound, upper_bound)
        """
        # For large samples, use normal approximation
        # TODO: Implement t-distribution for small samples when df is provided
        
        if confidence_level == 0.95:
            z_score = 1.96
        elif confidence_level == 0.90:
            z_score = 1.645
        elif confidence_level == 0.99:
            z_score = 2.576
        else:
            # Approximate z-score for other confidence levels
            from scipy import stats
            z_score = stats.norm.ppf((1 + confidence_level) / 2)
        
        if isinstance(estimate, (int, float)):
            se = variance ** 0.5
            margin = z_score * se
            return (estimate - margin, estimate + margin)
        
        # Polars expressions
        se = variance.sqrt()
        margin = z_score * se
        return (estimate - margin, estimate + margin)


# Utility functions for common variance calculations

def calculate_cv(
    estimate: Union[float, pl.Expr],
    variance: Union[float, pl.Expr]
) -> Union[float, pl.Expr]:
    """
    Calculate coefficient of variation as a percentage.
    
    CV = (SE / estimate) * 100
    
    Parameters
    ----------
    estimate : float or pl.Expr
        Point estimate
    variance : float or pl.Expr
        Variance of the estimate
        
    Returns
    -------
    float or pl.Expr
        Coefficient of variation as percentage
    """
    if isinstance(estimate, (int, float)):
        if estimate == 0:
            return 0.0
        return (variance**0.5) / abs(estimate) * 100
    
    # Polars expression
    return (
        pl.when(estimate == 0)
        .then(0.0)
        .otherwise((variance.sqrt() / estimate.abs()) * 100)
    )


def calculate_relative_se(
    variance: Union[float, pl.Expr],
    estimate: Union[float, pl.Expr]
) -> Union[float, pl.Expr]:
    """
    Calculate relative standard error (same as CV but often used interchangeably).
    
    Parameters
    ----------
    variance : float or pl.Expr
        Variance of the estimate
    estimate : float or pl.Expr
        Point estimate
        
    Returns
    -------
    float or pl.Expr
        Relative standard error as percentage
    """
    return calculate_cv(estimate, variance)