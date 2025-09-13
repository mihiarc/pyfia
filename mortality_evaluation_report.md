# Mortality() Function Evaluation Report

## Executive Summary

The `mortality()` function in pyFIA is designed to estimate tree mortality from FIA data. This evaluation identified critical issues in the current implementation that result in NaN values and zero results, even when valid mortality data exists in the database.

## Test Coverage Analysis

### Tests Created
- **17 test cases** covering various aspects of the mortality function
- **9 tests passing** (53% pass rate)
- **8 tests failing** (47% fail rate)

### Test Categories

#### 1. Basic Functionality Tests (3 tests, 0% passing)
- ❌ Default parameters (volume, recent mortality)
- ❌ Biomass measurement
- ❌ Tree count measurement

**Issue**: All basic tests fail with NaN values in MORT_ACRE due to division by zero when AREA_TOTAL is 0.

#### 2. Grouping Tests (3 tests, 33% passing)
- ❌ Group by species
- ❌ Custom grouping by ownership
- ✅ Multiple grouping columns

**Issue**: Grouping logic partially works but fails when aggregation results in empty datasets.

#### 3. Domain Filtering Tests (2 tests, 50% passing)
- ❌ Tree-level domain filtering (DIA >= 20.0)
- ✅ Area-level domain filtering (OWNGRPCD == 10)

**Issue**: Tree domain filtering causes complete data loss in some cases.

#### 4. Statistical Options Tests (3 tests, 33% passing)
- ❌ Variance calculation
- ❌ Mortality rate calculation
- ✅ Without totals option

**Issue**: Standard error calculations fail when base values are NaN.

#### 5. Edge Cases (3 tests, 100% passing)
- ✅ Empty database handling
- ✅ No dead trees scenario
- ✅ Invalid measure type

**Finding**: Edge case handling is robust and prevents crashes.

#### 6. Integration Tests (2 tests, 100% passing)
- ✅ Comprehensive scenario with multiple options
- ✅ Comparison across measure types

**Finding**: Integration tests pass but mask underlying calculation issues.

## Critical Issues Identified

### 1. Incorrect Table Usage - CRITICAL FINDING
**The current implementation uses the wrong approach entirely.** Based on the official FIA SQL query, mortality should use:
- `TREE_GRM_COMPONENT` table with TPAMORT_UNADJ field
- `TREE_GRM_MIDPT` table for volume values at midpoint
- `TREE_GRM_BEGIN` table for initial measurements
- COMPONENT field filtering for 'MORTALITY%' records

The current implementation incorrectly attempts to use:
- Base TREE table with STATUSCD==2 (dead trees)
- MORTYR field for mortality year filtering

This fundamental misunderstanding explains why the function returns zero values.

### 2. Data Loss During Aggregation
The mortality estimator loses data during the aggregation step. Dead trees with valid mortality years are present in the raw data but result in zero values after aggregation.

**Evidence**:
```
Raw data: 3 dead trees, total volume = 18,036
After aggregation: 0 dead trees, total volume = 0
```

### 2. Area Calculation Failure
The AREA_TOTAL calculation returns 0, causing division by zero when calculating per-acre values (MORT_ACRE).

**Root Cause**: The join between mortality data and condition data is not preserving the area information correctly.

### 3. Missing Lazy Frame Collection
The aggregation step uses LazyFrame operations but may not be collecting results at the right point, causing data to be lost.

### 4. Incorrect Filter Application Timing
The mortality filter (STATUSCD==2 and MORTYR>0) is applied too early, before joining with stratification data, potentially breaking the join relationships.

## Performance Issues

### Warnings Observed
1. **PerformanceWarning**: LazyFrame column checking is expensive (occurs 16+ times)
2. **DeprecationWarning**: `pl.count()` is deprecated, should use `pl.len()`
3. **Missing Column Warning**: MACRO_BREAKPOINT_DIA not found, affecting large tree estimates

## Recommendations

### Complete Redesign Required

1. **Use Correct FIA Tables**
   - Switch from TREE table to TREE_GRM_COMPONENT table
   - Use TPAMORT_UNADJ field instead of TPA_UNADJ
   - Filter by COMPONENT LIKE 'MORTALITY%' instead of STATUSCD==2
   - Join with TREE_GRM_MIDPT for volume values
   - Use proper subplot adjustment factors (SUBPTYP_GRM field)

2. **Fix Aggregation Logic**
   - Ensure joins preserve all necessary data
   - Collect LazyFrames at appropriate points
   - Validate data presence after each transformation

2. **Fix Area Calculation**
   - Ensure CONDPROP_UNADJ values are properly joined
   - Verify expansion factors (EXPNS) are applied correctly
   - Add validation to prevent division by zero

3. **Update Deprecated Functions**
   - Replace `pl.count()` with `pl.len()`
   - Use `LazyFrame.collect_schema().names()` for column checking

4. **Add Data Validation**
   - Check for non-zero area before division
   - Validate mortality data exists after filtering
   - Add logging for debugging data flow

### Code Quality Improvements

1. **Simplify Implementation**
   - Remove unnecessary abstraction layers
   - Use direct calculations where possible
   - Follow pyFIA's simplicity principles

2. **Improve Error Handling**
   - Return meaningful error messages instead of NaN
   - Add validation for required columns
   - Provide clear feedback when no mortality data exists

3. **Add Documentation**
   - Document expected data structure
   - Explain mortality year filtering logic
   - Provide examples of correct usage

## Comparison with Other Estimators

The mortality estimator is more complex than other estimators (area, volume, tpa) because it:
- Requires filtering to dead trees only
- Needs mortality year validation
- Calculates annualized rates
- May need to compare with live tree data for rates

## Test Data Insights

The test database created for evaluation includes:
- 5 plots with proper stratification
- 11 dead trees with various mortality years (2015-2022)
- 3 live trees for comparison
- Proper FIA table structure with all required relationships

Despite valid test data, the mortality function returns empty or zero results, confirming the implementation issues.

## Critical Discovery: Fundamental Design Flaw

After analyzing the official FIA SQL query for mortality calculations, it's clear that **the current implementation is fundamentally incorrect**. The pyFIA mortality function attempts to identify dead trees using STATUSCD==2 from the TREE table, but FIA actually uses specialized Growth-Removal-Mortality (GRM) tables that track changes between inventories.

### Correct FIA Methodology:
1. Uses TREE_GRM_COMPONENT table with pre-calculated mortality components
2. Filters records where COMPONENT LIKE 'MORTALITY%'
3. Uses TPAMORT_UNADJ (mortality trees per acre unadjusted) field
4. Joins with TREE_GRM_MIDPT for volume calculations at midpoint
5. Applies subplot-specific adjustment factors based on SUBPTYP_GRM

### Current pyFIA Approach (Incorrect):
1. Uses base TREE table filtered by STATUSCD==2
2. Attempts to use MORTYR field for mortality year
3. Uses standard TPA_UNADJ field
4. Missing critical GRM table relationships

## Conclusion

The mortality() function requires a **complete redesign** to align with FIA's actual methodology. The current implementation is based on an incorrect understanding of how FIA tracks and calculates mortality. This is not a simple bug fix but requires implementing support for the GRM (Growth-Removal-Mortality) table system.

**Priority**: CRITICAL - The function is fundamentally broken and produces incorrect results.

**Estimated Effort**: 8-16 hours for complete redesign and implementation of GRM table support.

## Next Steps

1. Debug the aggregation step to identify where data is lost
2. Fix the area calculation to ensure non-zero values
3. Update deprecated functions
4. Add comprehensive logging for troubleshooting
5. Re-run all tests to verify fixes
6. Add integration tests with real FIA data

## Appendix: Debug Output

```
Dead trees in database: 3
Total TPA: 15.036
Total Volume: 18,036
Area from raw query: 2,000

After mortality() function:
MORT_ACRE: NaN
MORT_TOTAL: 0.0
AREA_TOTAL: 0.0
N_DEAD_TREES: 0
```

This clearly shows data exists but is lost during processing.