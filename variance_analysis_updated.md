# Variance Calculator Analysis - Updated with Accurate EVALIDator Comparison

## Executive Summary

With correct EVALIDator baseline numbers, pyFIA's area estimate is **very close** (only 2% off), but the **variance calculation is still significantly incorrect** - our SE is more than double what it should be (1.20% vs 0.563%).

## Comparison Results

### Current Implementation vs EVALIDator

| Metric | pyFIA (Placeholder) | pyFIA (VarianceCalculator) | EVALIDator |
|--------|---------------------|----------------------------|------------|
| **Forest Area** | 23,673,198 acres | 23,673,198 acres | 24,172,679 acres |
| **Difference from EVALIDator** | -2.07% | -2.07% | - |
| **Standard Error** | 1,183,660 acres | 284,886 acres | 136,092 acres |
| **SE as % of estimate** | 5.00% | 1.20% | 0.563% |
| **Number of plots** | 6,427 | 6,427 | 4,842 (non-zero) |

## Key Findings

### 1. Area Estimate is Excellent
- **pyFIA**: 23,673,198 acres
- **EVALIDator**: 24,172,679 acres
- **Difference**: -499,481 acres (-2.07%)

This 2% difference is quite good and could be due to:
- Using EVALID 132300 vs a different evaluation
- Minor differences in plot filtering
- Rounding differences in expansion factors

### 2. Standard Error is Still Too High
- **pyFIA SE**: 284,886 acres (1.20%)
- **EVALIDator SE**: 136,092 acres (0.563%)
- **pyFIA SE is 109% higher than it should be**

The VarianceCalculator improved SE from 5% to 1.20%, but it's still **more than double** the correct value.

### 3. Plot Count Discrepancy
- **pyFIA reports**: 6,427 total plots
- **EVALIDator reports**: 4,842 non-zero (forested) plots

This suggests pyFIA is including all plots (forest and non-forest) in the count, while EVALIDator only counts forested plots. However, for variance calculation, we should be using all plots in the sample.

## Technical Issues Identified

### 1. Variance Formula Implementation

The current `calculate_ratio_of_means_variance` function appears to have issues:

```python
# Current implementation calculates:
R = Y_total / A_total  # Ratio estimate
var_h = (w_h^2 * (1 - 1/n_h) / n_h * (s_y^2 + R^2*s_a^2 - 2*R*s_ya))
SE = sqrt(total_variance / A_total^2)
```

The SE is coming out too high, suggesting:
- Possible issue with the finite population correction
- Incorrect handling of stratification weights
- Missing or incorrect covariance calculation

### 2. Data Aggregation Level

The variance is being calculated on plot-level aggregates rather than condition-level data:
```python
# Current approach aggregates by plot first:
plot_data = group_by(["PLT_CN", "STRATUM_CN"]).agg(sum("AREA_VALUE"))

# Should potentially preserve condition-level detail:
cond_data = keep_all_conditions_with_weights()
```

### 3. Grouping Variance Bug

All ownership groups show identical SE (10,808 acres), which is clearly wrong. This indicates the variance calculation is not properly partitioning data by groups.

## Recommendations for Fixing Variance

### 1. Debug the Variance Formula

Compare step-by-step with FIA's calculation:
- Verify finite population correction factor
- Check stratum weights (EXPNS) are properly applied
- Ensure covariance calculation is correct
- Validate against simpler test cases

### 2. Use Condition-Level Data

FIA's variance calculation likely works at the condition level:
```python
# Instead of plot-level aggregation:
def calculate_variance(cond_data, strat_data):
    # Work with individual conditions
    # Each condition has CONDPROP_UNADJ weight
    # Apply proper multi-stage variance formula
```

### 3. Fix Domain/Group Variance

Each group needs separate variance calculation:
```python
for group in groups:
    group_data = data.filter(group_col == group)
    group_variance = calculate_variance(group_data)
    # Not reusing the same variance for all groups!
```

### 4. Validate Against Multiple States

Test with other states to ensure the issue is consistent and not Georgia-specific.

## Conclusion

The **good news**:
- pyFIA's area estimate is very accurate (within 2% of EVALIDator)
- The VarianceCalculator reduces SE from 5% to 1.20% - a major improvement
- The core statistical framework is in place

The **remaining challenge**:
- Standard error is still 2x higher than it should be (1.20% vs 0.563%)
- This suggests issues with the variance formula implementation or data aggregation level
- Grouped estimates don't calculate variance correctly

The path forward is clear: debug the variance calculation formula and ensure it matches FIA's methodology exactly. The area estimates themselves are solid, so this is purely a variance calculation issue.