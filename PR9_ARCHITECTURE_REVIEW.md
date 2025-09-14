# Critical Software Architecture Review: PR #9
## Two-Stage Aggregation Fix for Per-Acre Estimators

**Review Date:** September 14, 2025
**PR Branch:** `dev/two-stage-aggregation-all-estimators`
**Reviewer:** Senior Software Architect

## Executive Summary

PR #9 addresses a **critical bug** that caused ~20x underestimation in per-acre forest inventory metrics. The fix implements two-stage aggregation across four estimators (volume, biomass, mortality, growth), extending the proven solution from PR #8 (TPA estimator). While the fix is mathematically correct and architecturally sound, there are significant opportunities for code consolidation and several areas requiring attention before production deployment.

## 1. Architectural Consistency Assessment

### ✅ Strengths

1. **Uniform Implementation Pattern**: All four modified estimators follow an identical two-stage aggregation pattern:
   - Stage 1: Aggregate at plot-condition level
   - Stage 2: Apply expansion factors and calculate ratio-of-means

2. **Clear Documentation**: Each implementation includes detailed comments explaining the critical fix and its rationale

3. **Consistent Column Sets**: All use the same core grouping columns:
   ```python
   ["PLT_CN", "CONDID", "STRATUM_CN", "EXPNS", "CONDPROP_UNADJ"]
   ```

### ⚠️ Concerns

1. **Inconsistent Error Messaging**: While the volume estimator mentions "~22x underestimation," others reference "~20x" - these should be standardized or made metric-specific

2. **Different Adjustment Factor Logic**:
   - **Volume/Biomass/Growth**: Use `apply_tree_adjustment_factors()` with diameter-based breakpoints
   - **Mortality**: Uses GRM-specific `SUBPTYP_GRM` field for adjustment selection
   - This difference is justified by FIA methodology but should be better documented

## 2. Code Duplication Analysis

### 🔴 Critical Issue: Massive Code Duplication

The `aggregate_results()` method is nearly identical across all four estimators, with 80-90% duplicated code. Each implementation is ~100 lines, creating ~400 lines of nearly identical code.

### Duplication Pattern

```python
# This exact pattern appears in ALL four files with minor variations:
condition_group_cols = ["PLT_CN", "CONDID", "STRATUM_CN", "EXPNS", "CONDPROP_UNADJ"]
if group_cols:
    for col in group_cols:
        if col in data_with_strat.collect_schema().names() and col not in condition_group_cols:
            condition_group_cols.append(col)

condition_agg = data_with_strat.group_by(condition_group_cols).agg([
    # Only this line varies between estimators
    pl.col("METRIC_ADJ").sum().alias("CONDITION_METRIC"),
    pl.len().alias("TREES_PER_CONDITION")
])
```

### Recommended Solution

Create a shared method in `BaseEstimator`:

```python
def _apply_two_stage_aggregation(
    self,
    data_with_strat: pl.LazyFrame,
    metric_columns: Dict[str, str],  # {"VOLUME_ADJ": "CONDITION_VOLUME", ...}
    group_cols: List[str]
) -> pl.DataFrame:
    """
    Apply two-stage aggregation following FIA methodology.

    Stage 1: Aggregate metrics to plot-condition level
    Stage 2: Apply expansion factors and calculate ratio-of-means
    """
    # Common implementation here
    ...
```

This would reduce code from ~400 lines to ~100 lines plus small per-estimator customizations.

## 3. Performance Analysis

### ✅ Performance Improvements

1. **Correct Mathematical Foundation**: The two-stage approach ensures each condition's area is counted exactly once, eliminating the multiplicative error

2. **Efficient Aggregation**: Using Polars' group_by operations is performant for the two-stage process

### ⚠️ Performance Considerations

1. **Memory Usage**: The `collect()` call in Stage 1 materializes the entire condition-level dataset:
   ```python
   condition_agg = condition_agg.collect()  # Forces materialization
   ```
   Consider keeping as LazyFrame until final collection if possible

2. **Schema Checking**: Multiple `collect_schema().names()` calls could be optimized:
   ```python
   if col in data_with_strat.collect_schema().names()  # Called in loop
   ```
   Cache schema once before the loop

3. **Redundant Grouping Logic**: The complex grouping column logic is repeated in every method

## 4. Error Handling and Edge Cases

### 🔴 Critical Gaps

1. **No Validation of Required Columns**: Methods assume all required columns exist without checking

2. **Silent Failures**: When grouping columns don't exist at condition level, they're silently excluded

3. **Zero Division**: While there's protection for `AREA_DENOM > 0`, no logging or warnings for zero-area conditions

4. **Missing Data Handling**: No explicit handling for NULL values in critical columns

### Recommended Additions

```python
# Validate required columns exist
required_cols = ["PLT_CN", "CONDID", "STRATUM_CN", "EXPNS", "CONDPROP_UNADJ"]
missing = set(required_cols) - set(data.columns)
if missing:
    raise ValueError(f"Missing required columns: {missing}")

# Log warnings for edge cases
if (results["AREA_TOTAL"] == 0).any():
    logger.warning("Zero area found in some groups - check domain filters")
```

## 5. Backward Compatibility

### ✅ API Compatibility Maintained

- Public function signatures unchanged
- Return column names remain consistent
- Default behaviors preserved

### ⚠️ Numerical Breaking Changes

- **Results will change by ~20x** - this is the intended fix
- Users relying on previous (incorrect) values will see dramatic changes
- **Recommendation**: Add migration guide or warning in release notes

## 6. Additional Critical Findings

### 🔴 Untested Estimators

The `removals` estimator shows similar single-stage aggregation pattern but was NOT fixed:
```python
# removals.py still has single-stage aggregation
(pl.col("REMV_ADJ") * pl.col("EXPNS")).sum().alias("REMV_NUM"),
(pl.col("CONDPROP_UNADJ") * pl.col("EXPNS")).sum().alias("AREA_TOTAL"),
```
**This likely has the same bug and needs fixing**

### ⚠️ Testing Coverage

- No test files were updated with the fix
- Existing tests likely fail due to ~20x value changes
- Need comprehensive validation against published FIA estimates

### ⚠️ Documentation Updates Needed

- No updates to docstrings mentioning the fix
- No changelog entry
- No migration guide for users

## 7. Recommendations

### Immediate Actions Required

1. **Extract Common Code** (Priority: HIGH)
   - Implement `_apply_two_stage_aggregation()` in BaseEstimator
   - Reduce duplication by 75%

2. **Fix Removals Estimator** (Priority: CRITICAL)
   - Apply same two-stage fix to `removals.py`
   - Verify against FIA published values

3. **Update Tests** (Priority: CRITICAL)
   - Update expected values in all test files
   - Add specific tests for two-stage aggregation
   - Validate against published FIA estimates

### Pre-Merge Checklist

- [ ] Extract duplicated code to base class
- [ ] Fix removals estimator
- [ ] Update all test files with new expected values
- [ ] Add validation tests against published estimates
- [ ] Update docstrings to mention the fix
- [ ] Add comprehensive changelog entry
- [ ] Create migration guide for users
- [ ] Add error handling for edge cases
- [ ] Profile memory usage with large datasets

### Long-term Improvements

1. **Create Aggregation Strategy Classes**
   ```python
   class TwoStageAggregator:
       """Handles two-stage aggregation for per-acre metrics"""

   class SingleStageAggregator:
       """Handles single-stage aggregation for area metrics"""
   ```

2. **Implement Comprehensive Validation**
   - Column existence checks
   - Data type validation
   - Range checks for expansion factors

3. **Add Performance Monitoring**
   - Memory usage tracking
   - Query execution time logging
   - Result size monitoring

## 8. Risk Assessment

### High Risk Items

1. **Production Impact**: 20x change in values will break downstream systems expecting old values
2. **Removals Estimator**: Unfixed estimator creates inconsistency
3. **Test Coverage**: Inadequate testing could miss edge cases

### Medium Risk Items

1. **Code Maintainability**: Current duplication makes future fixes error-prone
2. **Performance**: Memory usage could be problematic for national-scale analyses

### Low Risk Items

1. **API Compatibility**: No breaking changes to function signatures
2. **Documentation**: Can be updated post-merge if necessary

## Conclusion

PR #9 implements a **mathematically correct and critical fix** that addresses a fundamental estimation error. However, the implementation suffers from significant code duplication that should be addressed before merging. The fix is incomplete (missing removals estimator) and lacks proper test coverage.

### Recommendation: **CONDITIONAL APPROVAL**

**Merge only after:**
1. Fixing the removals estimator
2. Updating test expected values
3. Adding validation tests against published FIA estimates

**Consider for immediate follow-up PR:**
1. Extract common aggregation code
2. Add comprehensive error handling
3. Optimize performance for large-scale analyses

The core fix is sound and critical for accurate FIA estimation. With the above modifications, this will significantly improve the library's statistical accuracy and reliability.

---

*Review conducted with focus on architectural soundness, code quality, performance, and production readiness. The mathematical correctness of the two-stage aggregation approach aligns with established FIA methodology (Bechtold & Patterson, 2005).*