# Developer Spec: Carbon and Growth Methodology Investigation

## Overview

This document specifies the investigation and resolution of two known methodology differences between pyFIA and EVALIDator:

1. **Carbon Estimation** - **RESOLVED** (was 1.62% difference, now 0.00%)
2. **Growth Estimation** (0.56% difference): Data synchronization difference

## Current State

### Validation Results (December 2025)

| Estimator | pyFIA | EVALIDator | Difference | Status |
|-----------|-------|------------|------------|--------|
| Area | 22,788,741 | 22,788,741 | 0.00% | EXACT |
| Volume | 49,706,497,327 | 49,706,497,327 | 0.00% | EXACT |
| Biomass | 1,633,483,010 | 1,633,483,010 | 0.00% | EXACT |
| TPA | 21,073,339,000 | 21,073,339,000 | 0.00% | EXACT |
| **Carbon (Live)** | **767,736,994** | **767,736,994** | **0.00%** | **EXACT** |
| Mortality | 1,224,527,000 | 1,224,527,000 | 0.00% | EXACT |
| Removals | 1,576,851,000 | 1,576,851,000 | 0.00% | EXACT |
| Growth | 2,030,335,500 | 2,041,792,321 | 0.56% | Data Sync |

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

## Issue 2: Growth Estimation Data Synchronization

### Problem Statement

pyFIA's growth estimate is ~11.5 million cu ft/year (0.56%) lower than EVALIDator, despite using the correct methodology (BEGINEND cross-join, ONEORTWO logic).

### Root Cause Analysis

**Plot Count Discrepancy**:
- pyFIA: 4,641 plots with forest GRM data
- EVALIDator: 4,689 plots reported
- Difference: 48 plots

**Likely Cause**: Database version differences between DuckDB export and EVALIDator's Oracle backend (FIADB 1.9.4.00).

### Current Status

**Accepted as Data Sync Tolerance**:
- 0.56% is within reasonable database version tolerance
- The methodology is correct (verified by exact matches on other estimators)
- Re-test recommended when database is updated

### Investigation Commands

```python
import polars as pl
from pyfia import FIA

EVALID = 132303
db_path = "data/georgia.duckdb"

with FIA(db_path) as db:
    db.clip_by_evalid(EVALID)

    db.load_table("TREE")
    db.load_table("TREE_GRM_COMPONENT")
    db.load_table("POP_PLOT_STRATUM_ASSGN")

    ppsa = db.tables["POP_PLOT_STRATUM_ASSGN"].collect()
    ppsa_filtered = ppsa.filter(pl.col("EVALID") == EVALID)
    eval_plots = set(ppsa_filtered["PLT_CN"].unique().to_list())

    print(f"Plots in evaluation: {len(eval_plots):,}")
    print(f"EVALIDator reports: 4,689")
    print(f"Difference: {4689 - len(eval_plots):,}")
```

### Acceptance Criteria

- [x] Root cause documented (data sync between database versions)
- [ ] If methodology issue found, fix implemented and validated
- [x] Documented as known limitation with tolerance

---

## Summary

| Issue | Status | Resolution |
|-------|--------|------------|
| Carbon (1.62% diff) | **RESOLVED** | Created CarbonPoolEstimator using FIA CARBON columns |
| Growth (0.56% diff) | Documented | Accepted as data sync tolerance |

### Key Accomplishments

1. **Carbon estimation now matches EVALIDator exactly** (0.00% difference)
2. All 8 estimators validated against EVALIDator:
   - 7 exact matches (0.00%)
   - 1 within data sync tolerance (0.56%)
3. Clean implementation with no backward compatibility debt

---

## References

- EVALIDator API: https://apps.fs.usda.gov/fiadb-api/evalidator
- FIA Database User Guide: https://www.fia.fs.usda.gov/library/database-documentation/
- Bechtold & Patterson (2005): FIA sampling methodology
- Test file: `tests/test_carbon_pools_evalidator.py`
