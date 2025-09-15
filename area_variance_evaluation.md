# Area Function Variance Calculation Evaluation

## Executive Summary

The `area()` function in pyFIA **does not properly calculate variance**. Instead of implementing the FIA-standard variance calculations, it uses a **hardcoded placeholder value** of 5% coefficient of variation (CV). This is a critical statistical flaw that invalidates any variance, standard error, or confidence interval outputs from the function.

## Key Findings

### 1. Placeholder Variance Implementation

In `/Users/mihiarc/repos/pyfia/src/pyfia/estimation/estimators/area.py:317-327`, the `calculate_variance` method:

```python
def calculate_variance(self, results: pl.DataFrame) -> pl.DataFrame:
    """Calculate variance for area estimates."""
    # Use simple variance calculator
    calc = VarianceCalculator(method="ratio_of_means")

    # For now, add placeholder SE
    results = results.with_columns([
        (pl.col("AREA_TOTAL") * 0.05).alias("AREA_SE")  # 5% CV placeholder
    ])

    return results
```

**Issue**:
- Creates a `VarianceCalculator` instance but **never uses it**
- Simply multiplies the area estimate by 0.05 (5%) as a placeholder
- This is not a statistical calculation - it's a hardcoded assumption

### 2. VarianceCalculator Not Actually Used

The `VarianceCalculator` class in `statistics.py` has a proper implementation of the ratio-of-means variance estimator following Bechtold & Patterson (2005), including:
- Stratum-level statistics calculation
- Covariance between response and area
- Finite population correction
- Proper variance formula: `Var(R) = Σ(w_h^2 * (1 - 1/n_h) / n_h * (s_y^2 + R^2*s_a^2 - 2*R*s_ya))`

However, **this implementation is never called** by the area estimator.

### 3. Pattern Across All Estimators

This placeholder pattern is repeated across all estimators:
- **Volume**: Uses 12% CV placeholder (`volume.py:154-164`)
- **Biomass**: Uses 10% CV placeholder (`biomass.py:173-194`)
- **TPA**: Uses 10-50% CV based on sample size (`tpa.py:166-176`)
- **Mortality**: Uses 15% CV placeholder (`mortality.py:314-325`)
- **Growth**: Uses 12% CV placeholder (`growth.py:425-433`)
- **Removals**: Uses 20% CV placeholder (`removals.py:287-295`)
- **Base Estimator**: Uses 10% CV placeholder (`base.py:323-333`)

### 4. Impact on Results

All variance-related outputs are **statistically invalid**:
- `AREA_SE` (standard error) - meaningless placeholder
- `AREA_VAR` (variance) - if calculated, would be placeholder squared
- Confidence intervals - if calculated, based on invalid SE
- Any statistical tests based on these values - invalid

### 5. Documentation Misleading

The function documentation claims:
- "Calculates area estimates using FIA's design-based estimation methods with proper expansion factors and stratification" (line 360-362)
- References Bechtold & Patterson (2005) methodology (line 461-462)
- Describes the proper variance formula in the Notes section

This documentation is **misleading** as the actual implementation does not follow these methods for variance calculation.

## Required Fixes

### Immediate Action
1. **Fix `calculate_variance` method** in `AreaEstimator` to actually use the `VarianceCalculator`:
```python
def calculate_variance(self, results: pl.DataFrame) -> pl.DataFrame:
    """Calculate variance for area estimates."""
    calc = VarianceCalculator(method="ratio_of_means")

    # Need to pass the raw data with stratification, not aggregated results
    # This requires refactoring to keep the detailed data available
    variance_results = calc.calculate(
        self.detailed_data,  # Need to store this
        response_col="AREA_VALUE",
        group_cols=self.config.get("grp_by")
    )

    # Merge variance results with main results
    return results.join(variance_results, on=group_cols, how="left")
```

### Structural Changes Needed
1. **Preserve detailed data**: The estimator needs to keep the plot-level data with stratification to calculate variance
2. **Two-stage aggregation**: First aggregate for estimates, then calculate variance on detailed data
3. **Proper data flow**: Pass stratification information through the entire pipeline

### Testing Requirements
1. Add tests that validate variance calculations against known FIA published standard errors
2. Test that variance increases appropriately with smaller sample sizes
3. Validate finite population correction is applied
4. Check that stratification reduces variance compared to simple random sampling

## Conclusion

The area() function and all other estimation functions in pyFIA currently provide **scientifically invalid variance estimates**. The hardcoded placeholder values (5% CV for area, varying for others) have no statistical basis and should not be used for any analytical purposes. This is a **critical bug** that undermines the statistical validity of the entire library.

This directly confirms **Issue #17** on GitHub: "CRITICAL: Variance calculations use placeholder values instead of proper statistical formulas"

## Recommendation

Until proper variance calculation is implemented:
1. Add a clear warning to all function outputs that variance/SE values are placeholders
2. Consider removing variance output entirely rather than providing misleading values
3. Prioritize implementing proper variance calculation as described in the FIA technical documentation
4. Add comprehensive tests comparing calculated variances to published FIA standard errors