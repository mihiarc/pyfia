# Variance Implementation Evaluation Summary

## Branch: `evaluate-volume-variance`

## Overview
This evaluation compared the variance calculation implementations between the `area()` and `volume()` functions in pyFIA, revealing significant differences in statistical rigor.

## Key Findings

### ✅ area() Function - Proper Implementation
Location: `src/pyfia/estimation/estimators/area.py` (lines 382-541)

**Strengths:**
- Implements proper stratified variance following Bechtold & Patterson (2005)
- Formula: V(Ŷ_D) = Σ_h [w_h² × s²_yDh × n_h] for domain total estimation
- Preserves plot-condition level data for variance calculation
- Handles grouped estimates with separate variance per group
- CV responds appropriately to sample size and domain restrictions
- Texas forestland example: CV = 0.85% (appropriate for state-level)

**Technical Details:**
- Stores `self.plot_condition_data` during aggregation
- Uses domain indicator approach (0/1 values)
- Correctly handles decimal types from database
- Implements `_calculate_variance_for_group()` method

### ⚠️ volume() Function - Placeholder Implementation
Location: `src/pyfia/estimation/estimators/volume.py` (lines 154-181)

**Current Issues:**
- Uses fixed 12% coefficient of variation for all estimates
- No data preservation for variance calculation
- CV doesn't respond to sample size or domain restrictions
- Same variance applied uniformly to all groups
- Texas volume example: CV = 12.00% (always fixed)

**Code:**
```python
# Current simplified placeholder
results = results.with_columns([
    (pl.col("VOLUME_ACRE") * 0.12).alias("VOLUME_ACRE_SE"),
    (pl.col("VOLUME_TOTAL") * 0.12).alias("VOLUME_TOTAL_SE"),
])
```

## Bugs Fixed During Evaluation

1. **Decimal Type Error in area():**
   - Issue: `pow` operation not supported for decimal[12,4] type
   - Fix: Cast `w_h` to Float64 before power operation
   - Line 514 in area.py

2. **Missing Columns in volume():**
   - Issue: Hard-coded COND columns not always available
   - Fix: Made column selection dynamic based on configuration
   - Lines 55-80 in volume.py

## Implementation Proposal

Created comprehensive proposal: `docs/variance_implementation_proposal.md`

### Key Recommendations:
1. **Data Preservation:** Add `self.plot_tree_data` to VolumeEstimator
2. **Variance Method:** Implement ratio-of-means variance formula
3. **Formula:** V(R̂) ≈ (1/X̄²) × [V(Ŷ) + R̂² × V(X̄) - 2R̂ × Cov(Ŷ,X̄)]
4. **Stratification:** Account for stratified sampling design
5. **Grouping:** Calculate separate variance for each group

### Implementation Timeline:
- **Phase 1:** Data preservation structure (immediate)
- **Phase 2:** Core variance calculation (1 week)
- **Phase 3:** Testing & validation (1 week)
- **Phase 4:** Extend to other estimators (ongoing)

## Test Results

### Test Files Created:
1. `test_variance_comparison.py` - Code analysis and comparison
2. `test_variance_detailed.py` - Behavioral testing with real data

### Key Test Findings:
- area() CV for Texas forestland: 0.85% (appropriate, varies by domain)
- volume() CV for Texas volume: 12.00% (fixed placeholder)
- area() CV increases with domain restrictions (correct behavior)
- volume() CV stays constant with restrictions (incorrect behavior)

## Impact on Other Estimators

Similar issues likely exist in:
- `biomass.py` - Uses same placeholder approach
- `tpa.py` - Uses same placeholder approach
- `growth.py` - Needs review
- `mortality.py` - Has better implementation, can serve as reference

## Next Steps

1. **Immediate:** Review and approve variance implementation proposal
2. **Week 1:** Implement proper variance for volume()
3. **Week 2:** Validate against FIA EVALIDator
4. **Week 3:** Extend to biomass() and tpa()
5. **Ongoing:** Update documentation and add warnings about current limitations

## Statistical Significance

Proper variance implementation is critical for:
- Valid confidence intervals
- Accurate precision assessment
- Alignment with published FIA estimates
- Statistical defensibility of results
- User trust in library outputs

## Files Modified

```
src/pyfia/estimation/estimators/area.py     - Fixed decimal type error
src/pyfia/estimation/estimators/volume.py   - Fixed column selection
docs/variance_implementation_proposal.md    - Created implementation plan
test_variance_comparison.py                 - Created comparison test
test_variance_detailed.py                   - Created behavior test
```

## Commit Reference
Branch: `evaluate-volume-variance`
Commit: f4925f6

---

*Evaluation completed on 2025-09-15*