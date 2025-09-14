# Biomass API Function Documentation Evaluation

## Executive Summary
The `biomass()` function in `/src/pyfia/estimation/estimators/biomass.py` requires significant documentation improvements to meet pyFIA's public API standards as established by the `mortality()` function reference implementation.

## Current State Assessment

### Documentation Sections Present ✅
1. **Summary Line** - Present but minimal: "Estimate tree biomass from FIA data."
2. **Parameters** - Present but lacks detail
3. **Returns** - Present but minimal
4. **Examples** - Present (3 examples)

### Documentation Sections Missing ❌
1. **Extended Summary** - No methodology explanation
2. **See Also** - No references to related functions
3. **Notes** - No technical implementation details
4. **Warnings** - No limitations or caveats mentioned
5. **Raises** - No exception documentation

## Detailed Deficiencies

### 1. Parameter Documentation Issues
Current parameters lack comprehensive descriptions. For example:

**Current `grp_by` documentation:**
```python
grp_by : Optional[Union[str, List[str]]]
    Columns to group by
```

**Required standard (from mortality()):**
```python
grp_by : str or list of str, optional
    Column name(s) to group results by. Can be any column from the
    FIA tables used in the estimation (PLOT, COND, TREE_GRM_COMPONENT,
    TREE_GRM_MIDPT). Common grouping columns include:

    - 'FORTYPCD': Forest type code
    - 'OWNGRPCD': Ownership group (10=National Forest, 20=Other Federal,
      30=State/Local, 40=Private)
    - 'STATECD': State FIPS code
    [... more examples ...]
```

### 2. Missing Critical Information

#### Extended Summary
Should explain:
- FIA biomass methodology
- What biomass components are available
- Carbon calculation approach (47% of biomass)
- Two-stage aggregation approach (mentioned in code comments)

#### Notes Section
Should include:
- Available biomass components (AG, BG, TOTAL, STEM, BRANCH, etc.)
- DRYBIO column explanations
- Conversion factors (pounds to tons: /2000)
- Carbon percentage (0.47)
- Required tables and columns
- References to Bechtold & Patterson (2005)

#### See Also Section
Should reference:
- Related functions: `volume()`, `tpa()`, `mortality()`, `growth()`
- Constants: `pyfia.constants.TreeStatus`, `pyfia.constants.OwnershipGroup`
- Utilities: `pyfia.utils.reference_tables`

### 3. Parameter Value Enumerations

Several parameters accept specific values but don't document them properly:

**`component` parameter:**
- Current: `component : str` with description "Biomass component: "AG", "BG", "TOTAL", "STEM", etc."
- Should enumerate all valid values with descriptions

**`land_type` parameter:**
- Current: `land_type : str` with description "Land type: "forest", "timber", or "all""
- Should use format: `land_type : {'forest', 'timber', 'all'}, default 'forest'`

**`tree_type` parameter:**
- Current: `tree_type : str` with description "Tree type: "live", "dead", "gs", or "all""
- Should use format: `tree_type : {'live', 'dead', 'gs', 'all'}, default 'live'`

### 4. Returns Documentation

**Current:**
```python
Returns
-------
pl.DataFrame
    Biomass and carbon estimates
```

**Should be:**
```python
Returns
-------
pl.DataFrame
    Biomass and carbon estimates with the following columns:

    - **BIO_ACRE** : float
        Biomass per acre in tons (dry weight)
    - **BIO_TOTAL** : float (if totals=True)
        Total biomass expanded to population level
    - **CARB_ACRE** : float
        Carbon per acre in tons (47% of biomass)
    - **CARB_TOTAL** : float (if totals=True)
        Total carbon expanded to population level
    [... additional columns ...]
```

### 5. Examples Need Enhancement

Current examples are too brief and don't show output or explain results:

**Current example:**
```python
>>> # Aboveground biomass on forestland
>>> results = biomass(db, component="AG")
```

**Should include:**
```python
>>> # Aboveground biomass on forestland
>>> results = biomass(db, component="AG")
>>> if not results.is_empty():
...     print(f"Aboveground biomass: {results['BIO_ACRE'][0]:.1f} tons/acre")
...     print(f"Carbon storage: {results['CARB_ACRE'][0]:.1f} tons/acre")
```

## Code Quality Notes

### Positive Aspects
1. Implementation follows simplified architecture principles
2. Two-stage aggregation properly implemented (lines 135-227)
3. Clear component selection logic
4. Proper conversion factors applied

### Areas for Improvement
1. Placeholder variance calculation (lines 229-239) - only returns 10% of estimate
2. Hard-coded year (2023) in format_output (line 245)
3. Missing validation for component parameter values

## Recommendations

### Priority 1: Documentation Completeness
1. Add all missing documentation sections (Extended Summary, See Also, Notes, Warnings, Raises)
2. Expand parameter descriptions to match mortality() standard
3. Document all return columns with types and descriptions
4. Add more comprehensive examples with output

### Priority 2: Code Improvements
1. Implement proper variance calculation or document limitation in Warnings
2. Make year dynamic based on inventory data
3. Add validation for component parameter
4. Consider adding biomass_type parameter for more flexibility

### Priority 3: Additional Enhancements
1. Add support for biomass by species groups
2. Include diameter distribution options
3. Document relationship between biomass components
4. Add cross-references to FIA documentation

## Compliance Score
**Current: 3/10** - Basic documentation present but far below standard
**Target: 10/10** - Match mortality() function documentation quality

## Next Steps
1. Update biomass() docstring to match mortality() standard
2. Add comprehensive parameter descriptions with valid values
3. Include technical notes about biomass calculation methodology
4. Add warnings about current variance calculation limitations
5. Enhance examples with practical use cases and output