# Mortality Estimator Fix Summary

## Changes Made

### 1. Added `treeClass` Parameter to `mortality()` Function
- **Location**: `pyfia/mortality.py`
- **New Parameter**: `treeClass: str = "all"`
- **Options**: 
  - `"all"` (default): Uses _AL_FOREST/_AL_TIMBER columns for all live trees
  - `"growing_stock"`: Uses _GS_FOREST/_GS_TIMBER columns for merchantable volume mortality

### 2. Updated Column Selection Logic
- Modified column suffix selection based on `treeClass` parameter
- Growing stock mode now correctly uses _GS_FOREST or _GS_TIMBER columns
- This matches the SQL query pattern for merchantable volume mortality

### 3. Added COMPONENT Filtering
- Added filter: `pl.col("COMPONENT").str.starts_with("MORTALITY")`
- This matches EVALIDator methodology: `COMPONENT LIKE 'MORTALITY%'`
- Ensures only mortality components are included (MORTALITY1, MORTALITY2)

### 4. Updated Documentation
- Updated CLAUDE.md to reflect the new growing stock support
- Added note about proper EVALIDator methodology
- Documented the new `treeClass` parameter in function docstring

## Key Insights from Colorado SQL Query

The validated SQL query showed:
1. Use of `SUBP_COMPONENT_GS_FOREST` for growing stock mortality
2. Filtering with `COMPONENT LIKE 'MORTALITY%'`
3. Multiplication of mortality rate by VOLCFNET for volume mortality
4. This gives merchantable bole wood volume mortality

## Next Steps

1. Test the updated function with both treeClass options
2. Verify results match EVALIDator for growing stock mortality
3. Consider deprecating mortality_direct.py if this implementation is sufficient
4. Update unit tests to include treeClass parameter testing

## Usage Example

```python
from pyfia import FIA
from pyfia.mortality import mortality

# Initialize and clip to GRM evaluation
fia = FIA("path/to/db")
fia.clip_most_recent(eval_type="GRM")

# All live trees mortality (default)
mort_all = mortality(fia)

# Growing stock mortality (merchantable volume)
mort_gs = mortality(fia, treeClass="growing_stock")
```