# Statistical and Data Science Review: TPA Function Refactoring (PR #8)

## Executive Summary
This review examines the statistical correctness and implementation quality of the TPA (Trees Per Acre) and BAA (Basal Area per Acre) estimation functions in PR #8. The analysis covers mathematical formulas, variance calculations, expansion factor application, and numerical precision.

## 1. BAA Formula Review

### Mathematical Correctness ✅
The BAA formula implementation is **statistically correct**:

```python
# Line 77: BAA calculation
(math.pi * (pl.col("DIA").cast(pl.Float64) / 24.0) ** 2 *
 pl.col("TPA_UNADJ").cast(pl.Float64)).alias("BAA")
```

**Derivation Validation:**
- Basal area of a single tree = π × r²
- DIA is in inches, need radius in feet
- Conversion: DIA inches → feet: DIA/12
- Radius: (DIA/12)/2 = DIA/24
- Formula: π × (DIA/24)² × TPA_UNADJ ✅

**Strengths:**
- Clear documentation of the derivation (lines 61-68)
- Explicit casting to Float64 for precision
- Mathematically accurate conversion factors

**Minor Suggestion:**
Consider adding a constant for clarity:
```python
INCHES_TO_FEET_RADIUS = 24.0  # Convert diameter in inches to radius in feet
```

## 2. Variance Calculation Analysis

### Critical Issue: Oversimplified CV Assumption ⚠️

The current implementation uses a fixed 10% coefficient of variation (CV) for all estimates:

```python
# Lines 199-200
tpa_cv = 0.10  # 10% coefficient of variation for TPA
baa_cv = 0.10  # 10% coefficient of variation for BAA
```

**Problems with this approach:**
1. **Not statistically valid** - CV varies significantly based on:
   - Sample size (number of plots)
   - Spatial distribution of trees
   - Stratification effectiveness
   - Tree size distribution

2. **Misleading uncertainty estimates** - Users may make incorrect decisions based on inaccurate precision estimates

3. **Inconsistent with FIA methodology** - FIA uses proper stratified variance estimation

### Recommended Solution
Implement proper variance calculation following Bechtold & Patterson (2005):

```python
def calculate_variance(self, results: pl.DataFrame) -> pl.DataFrame:
    """
    Calculate variance using FIA's stratified estimation approach.

    Variance for ratio-of-means estimator:
    Var(R) = (1/X̄²) * Σ[nh*(nh-1)⁻¹ * sh²]

    Where:
    - R = ratio estimator (e.g., TPA per acre)
    - X̄ = mean of auxiliary variable (area)
    - nh = sample size in stratum h
    - sh² = sample variance in stratum h
    """
    # TODO: Implement proper stratified variance
    # For now, add clear warning about placeholder
    import warnings
    warnings.warn(
        "Using simplified 10% CV for variance. "
        "Production use requires proper stratified variance calculation.",
        UserWarning
    )
```

## 3. Ratio-of-Means Estimation

### Implementation Correctness ✅
The ratio-of-means implementation is **statistically sound**:

```python
# Lines 136-144: Correct numerator and denominator calculation
(pl.col("TPA_ADJ").cast(pl.Float64) * pl.col("EXPNS").cast(pl.Float64)).sum().alias("TPA_NUM"),
...
(pl.col("CONDPROP_UNADJ").cast(pl.Float64) * pl.col("EXPNS").cast(pl.Float64)).sum().alias("AREA_TOTAL"),

# Lines 163-166: Correct ratio calculation
pl.when(pl.col("AREA_TOTAL") > 0)
.then(pl.col("TPA_NUM") / pl.col("AREA_TOTAL"))
.otherwise(0.0)
```

**Strengths:**
- Proper handling of division by zero
- Correct expansion factor application
- Separate calculation of totals and per-acre values

**Minor Issue:**
The variable naming could be clearer - `TPA_NUM` and `TPA_TOTAL` appear to be the same (lines 136 vs 140).

## 4. Expansion Factor Application

### Tree Size Adjustment Factors ✅
The implementation correctly applies FIA's nested plot design:

```python
# Line 110-114: Proper adjustment factor application
data_with_strat = apply_tree_adjustment_factors(
    data_with_strat,
    size_col="DIA",
    macro_breakpoint_col="MACRO_BREAKPOINT_DIA"
)
```

The `tree_expansion.py` module correctly implements:
- Microplot (DIA < 5.0"): ADJ_FACTOR_MICR
- Subplot (5.0" ≤ DIA < breakpoint): ADJ_FACTOR_SUBP
- Macroplot (DIA ≥ breakpoint): ADJ_FACTOR_MACR

**Excellent Features:**
- Handles NULL diameters appropriately (defaults to subplot)
- Handles missing MACRO_BREAKPOINT_DIA (treats as 9999)
- Clear documentation of plot size radii

## 5. Stratification Handling

### Correct Implementation ✅
The stratification is properly handled:

```python
# Lines 99-106: Proper stratification join
strat_data = self._get_stratification_data()
data_with_strat = data.join(
    strat_data,
    on="PLT_CN",
    how="inner"
)
```

**Critical Texas Deduplication ✅**
The base estimator correctly handles Texas's duplicate data issue:

```python
# base.py lines 424-427
ppsa_unique = ppsa.unique(subset=["PLT_CN", "STRATUM_CN"])
pop_stratum_unique = pop_stratum.unique(subset=["CN"])
```

This prevents the ~2x overcounting that would occur with Texas data.

## 6. Data Type Handling and Numerical Precision

### Excellent Precision Management ✅
All calculations explicitly cast to Float64:

```python
# Consistent Float64 casting throughout
pl.col("TPA_UNADJ").cast(pl.Float64)
pl.col("DIA").cast(pl.Float64)
pl.col("EXPNS").cast(pl.Float64)
```

**Strengths:**
- Prevents integer overflow
- Maintains precision in division operations
- Consistent type handling throughout

### Minor Precision Concern
The 2-inch size class calculation could lose precision:

```python
# Line 85: Integer casting might cause unexpected binning
((pl.col("DIA") / 2.0).floor() * 2).cast(pl.Int32).alias("SIZE_CLASS")
```

Consider documenting edge cases (e.g., DIA=1.99 → class 0, DIA=2.00 → class 2).

## 7. EVALID Handling

### Excellent Automatic Selection ✅
The automatic EVALID selection prevents a critical overcounting issue:

```python
# Lines 627-634
if db.evalid is None:
    warnings.warn(
        "No EVALID specified. Automatically selecting most recent EXPVOL evaluations..."
    )
    eval_type_to_use = eval_type if eval_type else "VOL"
    db.clip_most_recent(eval_type=eval_type_to_use)
```

This is a **major improvement** that prevents users from accidentally including multiple overlapping evaluations.

## 8. Additional Findings

### Strengths
1. **Comprehensive documentation** - 265 lines of documentation for ~335 lines of code
2. **Clear examples** - 7 practical examples in docstring
3. **Proper resource management** - Database connection cleanup
4. **Backward compatibility** - Maintains existing API

### Areas for Improvement

1. **Variance Calculation** (Critical)
   - Replace 10% CV assumption with proper stratified variance
   - Or at minimum, make CV configurable based on sample size

2. **Performance Optimization**
   - Consider caching stratification data across multiple calls
   - The `_get_stratification_data` uses `@lru_cache` but only within instance

3. **Error Handling**
   - Add validation for minimum sample size
   - Warn when N_PLOTS < 10 (unreliable estimates)

4. **Testing Coverage**
   - Add specific tests for Texas deduplication
   - Add tests for variance calculation accuracy
   - Add tests for edge cases (single plot, single tree)

## 9. Recommendations

### Immediate Actions (Before Merge)
1. **Document the variance limitation more prominently** - Add to main README
2. **Add configurable CV** based on sample size:
   ```python
   def get_cv_estimate(n_plots: int) -> float:
       """Estimate CV based on sample size."""
       if n_plots < 10:
           return 0.30  # 30% for very small samples
       elif n_plots < 50:
           return 0.20  # 20% for small samples
       elif n_plots < 100:
           return 0.15  # 15% for moderate samples
       else:
           return 0.10  # 10% for large samples
   ```

3. **Add validation for degenerate cases**:
   ```python
   if results["N_PLOTS"][0] < 2:
       warnings.warn("Estimates based on < 2 plots are unreliable")
   ```

### Future Enhancements
1. **Implement full stratified variance** following Bechtold & Patterson (2005)
2. **Add bootstrap variance option** for complex groupings
3. **Cache stratification data** at module level for repeated calls
4. **Add diagnostic outputs** (CV per stratum, effective sample size)

## 10. Statistical Validation Tests

To validate the implementation, consider these statistical tests:

```python
def test_tpa_statistical_properties():
    """Test statistical properties of TPA estimates."""

    # Test 1: Additivity - Sum of species should equal total
    total = tpa(db, land_type="forest")
    by_species = tpa(db, by_species=True, land_type="forest")
    species_sum = by_species["TPA"].sum()
    assert abs(total["TPA"][0] - species_sum) < 0.01

    # Test 2: Monotonicity - Filtered should be less than total
    all_trees = tpa(db)
    large_trees = tpa(db, tree_domain="DIA >= 10")
    assert large_trees["TPA"][0] <= all_trees["TPA"][0]

    # Test 3: Variance increases with smaller samples
    full = tpa(db)
    subset = tpa(db, tree_domain="SPCD == 131")  # Single species
    cv_full = full["TPA_SE"][0] / full["TPA"][0]
    cv_subset = subset["TPA_SE"][0] / subset["TPA"][0]
    assert cv_subset >= cv_full  # Generally true
```

## Conclusion

The TPA function refactoring represents a **significant improvement** in code quality, documentation, and usability. The core statistical calculations for TPA and BAA are **mathematically correct** and properly implemented. The expansion factor application and stratification handling are **sound**.

The primary concern is the **oversimplified variance calculation** using a fixed 10% CV. While acknowledged in documentation, this should be addressed before the code is used in production or for publication-quality estimates.

**Recommendation: APPROVE with the condition that:**
1. The variance limitation is prominently documented
2. A plan is established to implement proper variance calculation
3. Consider adding sample-size-based CV adjustment as an interim solution

The refactoring successfully simplifies the codebase while maintaining statistical rigor in the core calculations. With the variance calculation addressed, this will be a robust and user-friendly implementation.