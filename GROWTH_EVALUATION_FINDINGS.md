# Growth Function Evaluation - Findings and Recommendations

## Summary

The current `growth()` function in pyFIA is fundamentally incompatible with FIA GRM (Growth-Removal-Mortality) methodology and cannot produce results that match EVALIDator outputs.

## Key Problems Identified

### 1. Wrong Table Structure
- **Current**: pyFIA uses `TREE_GRM` table (which doesn't exist in FIA databases)
- **Required**: EVALIDator uses `TREE_GRM_COMPONENT`, `TREE_GRM_BEGIN`, `TREE_GRM_MIDPT` tables
- **Impact**: Complete failure - function cannot run with actual FIA data

### 2. Wrong Adjustment Factor Logic
- **Current**: Uses diameter-based adjustment factors (`DIA < 5.0 → MICR`, etc.)
- **Required**: Uses `SUBPTYP_GRM` field from GRM component table
- **Impact**: Even if tables existed, would produce incorrect expansion factors

### 3. Missing Component-Based Logic
- **Current**: Simple multiplication of growth columns by TPA
- **Required**: Complex component logic (SURVIVOR, INGROWTH, CUT1, MORTALITY1, etc.)
- **Impact**: Completely different calculation methodology

### 4. Missing Volume Change Calculations
- **Current**: Uses pre-calculated growth columns (`GROWCFAL`, etc.)
- **Required**: Calculates volume changes based on:
  - Component type (SURVIVOR vs CUT vs MORTALITY, etc.)
  - BEGINEND.ONEORTWO value (beginning vs ending volume approach)
  - Tree volumes at different time points (begin, midpoint, end)

## EVALIDator vs pyFIA Results

### EVALIDator Results (EVALID 132303, Georgia)
Growth by stocking class on forest land:
- Overstocked: 239,458,337 cubic feet
- Fully stocked: 1,168,883,001 cubic feet
- Medium stocked: 539,624,898 cubic feet
- Poorly stocked: 76,582,344 cubic feet
- Nonstocked: 5,786,921 cubic feet
- Unavailable: 11,456,821 cubic feet

**Total: ~2.04 billion cubic feet**

### pyFIA Results
- Current `growth()` function: **FAILED** (table doesn't exist)
- Component-based prototype: **0 rows returned** (implementation issues)

## Technical Analysis

### EVALIDator Query Structure
The EVALIDator uses a sophisticated multi-table join:

```sql
FROM BEGINEND BE,
     POP_STRATUM
     JOIN POP_PLOT_STRATUM_ASSGN ON (stratum linking)
     JOIN PLOT ON (plot linking)
     JOIN COND ON (condition linking)
     JOIN TREE ON (tree linking with condition matching)
     LEFT JOIN TREE_GRM_COMPONENT ON (GRM component data)
     LEFT JOIN TREE_GRM_BEGIN ON (beginning measurements)
     LEFT JOIN TREE_GRM_MIDPT ON (midpoint measurements)
```

### Key Calculation Logic
1. **Component-based expansion**: `TPAGROW_UNADJ * adjustment_factor * volume_change`
2. **Adjustment factors**: Based on `SUBPTYP_GRM` (0=none, 1=SUBP, 2=MICR, 3=MACR)
3. **Volume changes**: Complex logic based on component type and BEGINEND.ONEORTWO

### Data Availability (Georgia database)
- TREE_GRM_COMPONENT records: 168,873
- After component filtering: 105,044
- After domain filtering: 169,572
- **Data exists but complex joins create processing challenges**

## Recommendations

### Immediate Actions
1. **Mark current `growth()` function as deprecated** - it cannot work with real FIA data
2. **Remove from public API** until proper implementation exists
3. **Update documentation** to indicate growth estimation is not currently supported

### Long-term Implementation
1. **Complete rewrite required** - cannot fix incrementally
2. **Follow EVALIDator methodology exactly** - no shortcuts or simplifications
3. **Implement proper GRM component logic**:
   - Component type handling (SURVIVOR, INGROWTH, CUT, MORTALITY, etc.)
   - BEGINEND table integration
   - Multi-timepoint volume calculations
   - GRM-specific adjustment factors

### Alternative Approaches
1. **Direct SQL implementation** - bypass Polars/Python for complex GRM queries
2. **EVALIDator integration** - use EVALIDator as backend for growth calculations
3. **Simplified growth metrics** - implement basic tree-level growth without full GRM methodology

## Code Changes Made

### Created Files
- `src/pyfia/estimation/estimators/growth_component.py` - Prototype implementation
- `tests/test_growth_evaluation.py` - Evaluation test script

### Findings
- Prototype attempted to replicate EVALIDator logic but encountered multiple technical issues
- Data joins are complex and require careful handling of duplicates
- Column naming and case sensitivity issues throughout
- Memory and disk space challenges with large datasets

## Next Steps

1. **Decision needed**: Invest in complete GRM rewrite vs. remove growth functionality
2. **If rewrite chosen**: Dedicate significant development time (weeks, not days)
3. **If removal chosen**: Update documentation and API to reflect limitations
4. **Consider**: Partner with USDA/FIA for official growth calculation guidance

## Impact Assessment

- **Current users**: May be expecting growth functionality that doesn't work
- **API compatibility**: Breaking change required regardless of chosen approach
- **Documentation**: Must be updated to reflect current limitations
- **Testing**: Comprehensive test suite needed for any new implementation

---

**Evaluation completed**: January 14, 2025
**Database tested**: georgia.duckdb (EVALID 132303)
**EVALIDator query**: Average annual net growth of merchantable bole wood volume