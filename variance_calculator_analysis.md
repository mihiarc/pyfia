# Variance Calculator Analysis - Georgia FIA Data

## Executive Summary

Testing the existing `VarianceCalculator` class with real Georgia FIA data shows **significant improvements** over the placeholder implementation, but there are still issues that need to be addressed for full accuracy.

## Test Results

### 1. Current Implementation (Placeholder)
- **Forest area**: 23,673,198 acres
- **Standard error**: 1,183,660 acres
- **SE%**: 5.00% (hardcoded)
- **Method**: Simple multiplication by 0.05

### 2. Using VarianceCalculator (Ratio-of-Means)
- **Forest area**: 23,673,198 acres
- **Standard error**: 284,886 acres
- **SE%**: 1.20%
- **Method**: Proper ratio-of-means variance estimator

### 3. Published FIA Estimates (EVALIDator)
- **Forest area**: 24,764,072 acres
- **Standard error**: 233,465 acres
- **SE%**: 0.94%
- **N plots**: 5,844 (forested plots)

## Analysis

### Improvements
1. **More realistic SE**: The VarianceCalculator produces a 1.20% SE compared to the unrealistic 5% placeholder
2. **Proper statistical method**: Uses the ratio-of-means estimator as specified in Bechtold & Patterson (2005)
3. **Accounts for stratification**: The calculation properly uses stratum-level statistics

### Remaining Issues

#### 1. Area Estimate Discrepancy
- **pyFIA estimate**: 23,673,198 acres
- **Published estimate**: 24,764,072 acres
- **Difference**: -1,090,874 acres (-4.41%)

This suggests potential issues with:
- EVALID selection (using 132300 vs published 131900)
- Plot filtering or inclusion criteria
- Expansion factor calculations

#### 2. Standard Error Still Off
- **pyFIA SE**: 284,886 acres (1.20%)
- **Published SE**: 233,465 acres (0.94%)
- **Difference**: +51,421 acres (+22.03%)

The SE is 22% higher than published, indicating:
- Possible issues with variance formula implementation
- Missing finite population correction
- Incorrect handling of stratification weights

#### 3. Grouping Issues
When testing with ownership grouping (OWNGRPCD), all groups show the same SE (10,808 acres), which is clearly incorrect. This indicates the variance calculation is not properly handling grouped estimates.

## Technical Issues Found

### 1. Data Flow Problem
The current implementation calculates variance on aggregated results rather than plot-level data. The proper approach requires:
```python
# Current (incorrect):
results = aggregate_data()  # Loses plot-level detail
variance = calculate_variance(results)  # Can't properly calculate

# Correct:
plot_data = prepare_plot_level_data()
estimates = aggregate_data(plot_data)
variance = calculate_variance(plot_data, stratification)
```

### 2. Stratum Identification
The code uses `STRATUM_CN` as the stratum identifier, but the proper FIA approach may require using `ESTN_UNIT` and `STRATUM` combination.

### 3. Missing Components
The current implementation may be missing:
- Finite population correction factor
- Proper handling of partial plots (CONDPROP_UNADJ)
- Covariance terms for ratio estimation

## Recommendations

### Immediate Fixes

1. **Fix data pipeline**: Preserve plot-level data through the estimation process
```python
class AreaEstimator:
    def estimate(self):
        plot_data = self.prepare_plot_data()
        self.plot_data = plot_data  # Save for variance
        estimates = self.aggregate(plot_data)
        variance = self.calculate_variance_proper(plot_data, estimates)
        return combine(estimates, variance)
```

2. **Verify EVALID selection**: Check why we're getting different base area estimates

3. **Fix grouped variance**: Each group needs separate variance calculation

### Long-term Improvements

1. **Validate against multiple states**: Test with other states to ensure consistency
2. **Compare intermediate calculations**: Debug step-by-step against FIA's internal calculations
3. **Add comprehensive tests**: Create tests that validate against published SEs for various scenarios

## Conclusion

The existing `VarianceCalculator` class provides a **substantial improvement** over the placeholder implementation, reducing SE from an unrealistic 5% to a more reasonable 1.20%. However, there are still accuracy issues that need to be resolved:

1. The base area estimate differs by 4.4% from published values
2. The standard error is 22% higher than published
3. Grouped estimates don't calculate variance correctly

The core statistical methodology appears sound, but the implementation needs refinement in data handling and proper application of the variance formulas to match published FIA estimates exactly.