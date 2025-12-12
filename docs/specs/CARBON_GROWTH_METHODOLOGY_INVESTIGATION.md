# Developer Spec: Carbon and Growth Methodology Investigation

## Overview

This document specifies the investigation and resolution of two known methodology differences between pyFIA and EVALIDator:

1. **Carbon Estimation** - **RESOLVED** (was 1.62% difference, now 0.00%)
2. **Growth Estimation** - **RESOLVED** (was 0.56% difference, now 0.00%)

## Current State

### Validation Results (December 2025) - ALL EXACT MATCHES

| Estimator | pyFIA | EVALIDator | Difference | Status |
|-----------|-------|------------|------------|--------|
| Forest Area | 24,172,679 | 24,172,679 | 0.00% | EXACT |
| Timberland Area | 23,596,942 | 23,596,942 | 0.00% | EXACT |
| Volume (GS) | 43,933,377,540 | 43,933,377,540 | 0.00% | EXACT |
| Biomass (AG) | 1,345,620,513 | 1,345,620,513 | 0.00% | EXACT |
| TPA | 14,111,238,542 | 14,111,238,542 | 0.00% | EXACT |
| **Carbon (Live)** | **767,736,994** | **767,736,994** | **0.00%** | **EXACT** |
| **Growth** | **2,041,792,321** | **2,041,792,321** | **0.00%** | **EXACT** |
| Mortality | 463,584,044 | 463,584,044 | 0.00% | EXACT |
| Removals | 1,408,728,566 | 1,408,728,566 | 0.00% | EXACT |

---

## Issue 1: Carbon Estimation Methodology - RESOLVED

### Problem Statement (Historical)

pyFIA's carbon estimate was 12.5 million short tons lower than EVALIDator (1.62% difference) due to using a biomass-derived carbon calculation instead of FIA's pre-calculated carbon columns.

### Root Cause

**Old Approach** (biomass-derived):
```python
# Carbon was 47% of dry biomass - flat IPCC factor
DRYBIO * TPA_UNADJ / 2000.0 * 0.47
```

**EVALIDator Approach** (snum=55000):
- Uses pre-calculated `CARBON_AG + CARBON_BG` columns directly
- Includes both aboveground AND belowground carbon
- Uses FIA's species-specific carbon conversion factors

### Solution Implemented (December 2025)

Created `CarbonPoolEstimator` class in `src/pyfia/estimation/estimators/carbon_pools.py`:

```python
class CarbonPoolEstimator(BaseEstimator):
    """
    Carbon pool estimator using FIA's pre-calculated carbon columns.

    Supports pools: 'ag', 'bg', 'total'
    Uses CARBON_AG and CARBON_BG columns with species-specific factors.
    Matches EVALIDator snum=55000 exactly.
    """

    def calculate_values(self, data: pl.LazyFrame) -> pl.LazyFrame:
        pool = self.config.get("pool", "total")

        # CARBON columns are in pounds - convert to short tons
        # (2000 lbs per ton to match EVALIDator)
        LBS_TO_SHORT_TONS = 1.0 / 2000.0

        if pool == "ag":
            carbon_expr = pl.col("CARBON_AG")
        elif pool == "bg":
            carbon_expr = pl.col("CARBON_BG")
        else:  # total
            carbon_expr = pl.col("CARBON_AG") + pl.col("CARBON_BG")

        return data.with_columns([
            (
                carbon_expr.fill_null(0)
                * pl.col("TPA_UNADJ").cast(pl.Float64)
                * LBS_TO_SHORT_TONS
            ).alias("CARBON_ACRE")
        ])
```

### Files Changed

| File | Change |
|------|--------|
| `src/pyfia/estimation/estimators/carbon_pools.py` | NEW - CarbonPoolEstimator class |
| `src/pyfia/estimation/estimators/carbon.py` | Updated to route to carbon_pool() |
| `src/pyfia/estimation/estimators/__init__.py` | Export CarbonPoolEstimator |
| `src/pyfia/estimation/__init__.py` | Export carbon_pool |
| `tests/test_carbon_pools_evalidator.py` | NEW - Validation tests |

### Validation Results

```
pyFIA CARBON_TOTAL: 767,736,994 short tons
EVALIDator snum=55000: 767,736,994 short tons
Difference: 0 short tons (0.000000%)

*** EXACT MATCH ***
```

### Acceptance Criteria - COMPLETED

- [x] New `carbon_pools.py` estimator created
- [x] `carbon(db, pool="total")` matches EVALIDator snum=55000 exactly
- [x] Unit tests pass (9/9 tests passing)
- [x] Validation test created and passing
- [x] No backward compatibility debt (legacy code removed)

---

## Issue 2: Growth Estimation Methodology - RESOLVED

### Problem Statement (Historical)

pyFIA's growth estimate was ~11.5 million cu ft/year (0.56%) lower than EVALIDator, with a plot count discrepancy of 48 plots.

### Root Cause Analysis

**Initial Theory (WRONG)**: Database version differences / data synchronization.

**Actual Root Cause**: Incorrect `COND_STATUS_CD` filtering in `growth.py` line 349-351.

**The Bug**:
```python
# OLD (INCORRECT) - filtered to COND_STATUS_CD = 1
data = data.filter(pl.col("COND_STATUS_CD") == 1)
```

This filter incorrectly **excluded DIVERSION trees** - trees that were on forest land at T1 but diverted to non-forest by T2. These trees have:
- Non-forest `COND_STATUS_CD` at T2 (because the land was diverted)
- Valid GRM data with non-null `SUBP_TPAGROW_UNADJ_GS_FOREST`
- Volume contribution to growth calculation (as a loss)

**Key Insight**: The GRM column names (`SUBP_TPAGROW_UNADJ_GS_FOREST`) already incorporate land basis filtering through the `_FOREST` suffix. The tree-level `COND_STATUS_CD` filter was redundant and incorrect.

### Solution Implemented (December 2025)

**Removed the `COND_STATUS_CD` filter** from `growth.py` `apply_filters()` method:

```python
# NEW (CORRECT) - No COND_STATUS_CD filter
# Land filtering is handled by GRM column selection (_FOREST suffix)
# DIVERSION trees on non-forest conditions are correctly included
```

### Files Changed

| File | Change |
|------|--------|
| `src/pyfia/estimation/estimators/growth.py` | Removed COND_STATUS_CD filter from apply_filters() |

### Validation Results

```
Before Fix:
  pyFIA:      2,030,335,500 cu ft/year
  EVALIDator: 2,041,792,321 cu ft/year
  Difference: 0.56%

After Fix:
  pyFIA:      2,041,792,321 cu ft/year
  EVALIDator: 2,041,792,321 cu ft/year
  Difference: 0.00%

*** EXACT MATCH ***
```

### Acceptance Criteria - COMPLETED

- [x] Root cause identified (COND_STATUS_CD filter excluded DIVERSION trees)
- [x] Fix implemented (removed incorrect filter)
- [x] Validation test passing (exact match with EVALIDator)
- [x] No regressions in other estimators

---

## Summary

| Issue | Status | Resolution |
|-------|--------|------------|
| Carbon (1.62% diff) | **RESOLVED** | Created CarbonPoolEstimator using FIA CARBON columns |
| Growth (0.56% diff) | **RESOLVED** | Removed incorrect COND_STATUS_CD filter |

### Key Accomplishments

1. **Carbon estimation now matches EVALIDator exactly** (0.00% difference)
2. **Growth estimation now matches EVALIDator exactly** (0.00% difference)
3. **All 9 estimators validated against EVALIDator with exact matches (0.00%)**
4. Clean implementations with no backward compatibility debt

---

## References

- EVALIDator API: https://apps.fs.usda.gov/fiadb-api/evalidator
- FIA Database User Guide: https://www.fia.fs.usda.gov/library/database-documentation/
- Bechtold & Patterson (2005): FIA sampling methodology
- Test files: `tests/test_carbon_pools_evalidator.py`, `tests/test_evalidator_comprehensive.py`
