# plot_domain Parameter Implementation

## Summary

Added `plot_domain` parameter to all pyFIA estimators (`area`, `volume`, `biomass`, `tpa`) to enable filtering by PLOT-level attributes like COUNTYCD, UNITCD, LAT, LON, and ELEV.

## Problem Solved

Previously, users could only filter by COND-level attributes using `area_domain`. PLOT-level attributes like county codes were not accessible for filtering, making it impossible to estimate values for specific counties without custom SQL.

**Example of what didn't work before:**
```python
# This would fail - COUNTYCD is in PLOT, not COND
area(db, area_domain="COUNTYCD == 183")
```

**Now works with plot_domain:**
```python
# This works - plot_domain filters PLOT table
area(db, plot_domain="COUNTYCD == 183")
```

## Implementation Details

### Files Modified

1. **Configuration** (`src/pyfia/estimation/config.py`)
   - Added `plot_domain: Optional[str] = None` to `EstimatorConfig`
   - Updated `to_dict()` to include plot_domain

2. **Filtering Module** (New files)
   - `src/pyfia/filtering/plot/filters.py` - Plot filtering logic
   - `src/pyfia/filtering/plot/__init__.py` - Module exports
   - Updated `src/pyfia/filtering/__init__.py` to export `apply_plot_filters`

3. **Base Estimator** (`src/pyfia/estimation/base.py`)
   - Imported `apply_plot_filters`
   - Updated `_load_area_data()` to apply plot filters before joining PLOT with COND
   - Updated `_load_tree_cond_data()` to filter plots before joining with TREE/COND

4. **Estimator Functions** (All updated to accept and validate plot_domain)
   - `src/pyfia/estimation/estimators/area.py`
   - `src/pyfia/estimation/estimators/volume.py`
   - `src/pyfia/estimation/estimators/biomass.py`
   - `src/pyfia/estimation/estimators/tpa.py`

### How It Works

#### For Area Estimation (PLOT + COND only):
1. Load PLOT and COND tables
2. Apply EVALID filtering (if set)
3. **Apply plot_domain filter to PLOT table** (new step)
4. Join PLOT with COND
5. Continue with normal estimation

#### For Tree-Based Estimation (TREE + COND + PLOT):
1. Load TREE and COND tables
2. Apply EVALID filtering (if set)
3. **If plot_domain is specified:**
   - Load PLOT table
   - Apply plot_domain filter
   - Extract filtered PLT_CNs
   - Filter TREE and COND to only include these plots
4. Continue with normal estimation

### Filter Execution Order

The filters are applied in this order:
1. EVALID filter (via POP_PLOT_STRATUM_ASSGN)
2. **plot_domain filter** (PLOT table attributes)
3. area_domain filter (COND table attributes)
4. tree_domain filter (TREE table attributes)

## Available PLOT Columns for Filtering

Common PLOT-level columns that can be used with plot_domain:

**Location:**
- `COUNTYCD`: County FIPS code
- `UNITCD`: Survey unit code
- `STATECD`: State FIPS code

**Geographic:**
- `LAT`: Latitude (decimal degrees)
- `LON`: Longitude (decimal degrees)
- `ELEV`: Elevation (feet)

**Plot Attributes:**
- `PLOT`: Plot number
- `INVYR`: Inventory year
- `MEASYEAR`: Measurement year
- `MEASMON`: Measurement month
- `MEASDAY`: Measurement day

**Design:**
- `DESIGNCD`: Plot design code
- `KINDCD`: Kind of plot code

## Usage Examples

### Basic County Filter
```python
from pyfia import FIA, area

with FIA("path/to/fia.duckdb") as db:
    db.clip_by_state(37)
    results = area(db, plot_domain="COUNTYCD == 183")
```

### Multiple Counties
```python
results = area(
    db,
    plot_domain="COUNTYCD IN (183, 185, 187)",
    grp_by="COUNTYCD"
)
```

### Geographic Bounding Box
```python
results = volume(
    db,
    plot_domain="LAT >= 35.0 AND LAT <= 36.0 AND LON >= -80.0 AND LON <= -79.0"
)
```

### Elevation Filter
```python
results = biomass(
    db,
    plot_domain="ELEV > 2000",
    land_type="forest"
)
```

### Combined PLOT and COND Filters
```python
results = area(
    db,
    plot_domain="COUNTYCD == 183",  # Plot-level: county
    area_domain="OWNGRPCD == 40",   # Cond-level: ownership
    grp_by="FORTYPCD"
)
```

## Testing

All existing tests pass:
- Area estimation integration tests: 11/11 passed
- No regressions introduced
- Filter validation works correctly
- Type checking passes (mypy)
- Linting passes (ruff)

## Documentation

1. **Function signatures** - All estimators now include plot_domain parameter
2. **Docstrings** - Updated with plot_domain documentation and examples
3. **Example script** - Created `examples/plot_domain_example.py`

## Design Principles Followed

1. **Simplicity First** - Used existing DomainExpressionParser, no new complex patterns
2. **Consistency** - Follows same pattern as area_domain
3. **Statistical Rigor** - Filters applied before estimation, variance calculations unaffected
4. **No Breaking Changes** - plot_domain is optional, defaults to None

## Future Considerations

1. **Performance** - Plot filtering happens in-memory (LazyFrame), could optimize with SQL WHERE clause
2. **Validation** - Could add validation to check if requested columns exist in PLOT table
3. **Documentation** - Could add section to main docs about PLOT vs COND filtering

## Migration Guide

No migration needed - this is a new optional parameter. Existing code continues to work without changes.

**Before:**
```python
# Users had to use spatial filtering or custom SQL for county-level estimates
```

**After:**
```python
# Simple and straightforward
area(db, plot_domain="COUNTYCD == 183")
```
