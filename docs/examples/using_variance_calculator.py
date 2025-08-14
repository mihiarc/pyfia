#!/usr/bin/env python3
"""
Example: Using the FIAVarianceCalculator in estimators.

This example demonstrates how estimators should use the new shared variance
calculator instead of implementing their own variance calculations.
"""

import polars as pl
from pyfia.estimation import FIAVarianceCalculator
from pyfia.core import FIA


def example_estimator_with_variance(db: FIA, response_col: str = "VOLCFNET_ACRE"):
    """
    Example of how an estimator should use the shared variance calculator.
    
    This demonstrates the recommended pattern for variance calculation
    in FIA estimators.
    """
    # Step 1: Initialize the variance calculator
    var_calc = FIAVarianceCalculator(db)
    
    # Step 2: Load and prepare your data
    # (This is simplified - real estimators do more complex joins)
    plot_data = db.get_plots()
    tree_data = db.get_trees()
    
    # Join and calculate plot-level values
    plot_values = plot_data.join(
        tree_data.group_by("PLT_CN").agg([
            pl.col(response_col).sum().alias("plot_total")
        ]),
        on="PLT_CN",
        how="left"
    ).with_columns([
        pl.col("plot_total").fill_null(0)
    ])
    
    # Step 3: Calculate stratum-level variance
    stratum_var = var_calc.calculate_stratum_variance(
        plot_values,
        response_col="plot_total",
        group_cols=["STATECD"]  # Could be species, size class, etc.
    )
    
    print("Stratum-level variance results:")
    print(stratum_var.select(["STRATUM_CN", "y_mean", "stratum_var", "n_h"]))
    
    # Step 4: Calculate population-level variance
    # The calculator handles loading design factors from the database
    pop_var = var_calc.calculate_population_variance(
        stratum_var,
        group_cols=["STATECD"]
    )
    
    print("\nPopulation-level variance results:")
    print(pop_var.select(["ESTN_UNIT_CN", "population_mean", "population_var", "cv_percent"]))
    
    # Step 5: For ratio estimates (e.g., per-acre values)
    # Calculate covariance between numerator and denominator
    if "area_forest" in plot_values.columns and "area_total" in plot_values.columns:
        cov_data = var_calc.calculate_stratum_covariance(
            plot_values,
            x_col="area_forest",
            y_col="area_total"
        )
        
        # Then use ratio variance for the final estimate
        ratio_var = var_calc.calculate_ratio_variance(
            numerator_mean=cov_data["x_mean"][0],
            denominator_mean=cov_data["y_mean"][0],
            numerator_var=cov_data["x_var"][0],
            denominator_var=cov_data["y_var"][0],
            covariance=cov_data["covariance"][0]
        )
        
        print(f"\nRatio variance: {ratio_var:.6f}")
    
    return pop_var


# Pattern for refactoring existing estimators:
def refactor_existing_estimator():
    """
    Example showing how to refactor an existing estimator to use the
    shared variance calculator.
    """
    # BEFORE: Each estimator had its own variance calculation
    # def calculate_variance_old(data):
    #     # Custom implementation
    #     var = data.group_by("STRATUM").agg([
    #         # ... complex custom logic ...
    #     ])
    #     return var
    
    # AFTER: Use the shared variance calculator
    def calculate_variance_new(data, db):
        var_calc = FIAVarianceCalculator(db)
        
        # For stratum variance
        stratum_var = var_calc.calculate_stratum_variance(
            data, 
            response_col="value_per_acre"
        )
        
        # For population variance
        pop_var = var_calc.calculate_population_variance(stratum_var)
        
        # For ratio variance (if needed)
        if needs_ratio_variance:
            ratio_var = var_calc.calculate_ratio_variance(
                numerator_mean=pop_var["numerator_mean"],
                denominator_mean=pop_var["denominator_mean"],
                numerator_var=pop_var["numerator_var"],
                denominator_var=pop_var["denominator_var"],
                covariance=pop_var["covariance"]
            )
            
        return pop_var
    
    print("Estimator refactored to use shared variance calculator!")


# Benefits of the shared variance calculator:
print("""
Benefits of using FIAVarianceCalculator:

1. **Consistency**: All estimators use the same, correct variance formulas
2. **Maintainability**: Fix bugs or add features in one place
3. **Testing**: Comprehensive tests for the variance calculator benefit all estimators
4. **Performance**: Optimized Polars expressions for efficient computation
5. **Flexibility**: Supports both scalar and expression-based calculations
6. **FIA Compliance**: Follows Bechtold & Patterson (2005) procedures exactly
7. **Edge Cases**: Proper handling of missing strata, zero denominators, etc.

The calculator consolidates 6 different implementations into one correct,
well-tested component!
""")