# Module Migration Analysis for BaseEstimator Architecture

## Executive Summary

This document analyzes how each existing estimation module will be migrated to the new BaseEstimator architecture, providing specific implementation details and identifying potential challenges.

## 1. Volume Module Migration

### Current Implementation (317 lines)
- Complex volume type handling with multiple column mappings
- Direct expansion calculations
- Custom output formatting for different volume types

### New Implementation (~95 lines)
```python
class VolumeEstimator(BaseEstimator):
    # Only implement:
    - __init__(): Handle vol_type parameter
    - get_required_tables(): Return ["PLOT", "TREE", "COND", "POP_*"]
    - get_response_columns(): Map volume columns by type
    - calculate_values(): Multiply volume by TPA_UNADJ
    - apply_module_filters(): Require non-null volume columns
    - get_output_columns(): Define output structure
```

### Migration Notes
- ✅ Straightforward migration
- ✅ Clean mapping of volume types to columns
- ✅ Reuses all base workflow logic
- **Special handling**: Different volume types (NET/GROSS/SOUND/SAWLOG)

## 2. Biomass Module Migration

### Current Implementation (269 lines)
- Multiple biomass components (AG, BG, STEM, etc.)
- Carbon calculations (biomass * 0.47)
- Unit conversions (lbs to tons)

### New Implementation (~85 lines)
```python
class BiomassEstimator(BaseEstimator):
    # Only implement:
    - __init__(): Handle component parameter
    - get_required_tables(): Same as volume
    - get_response_columns(): {"BIO_ACRE": "BIO_ACRE", "CARB_ACRE": "CARB_ACRE"}
    - calculate_values(): DRYBIO * TPA_UNADJ / 2000
    - _get_biomass_column(): Map component to column
    - format_output(): Add carbon calculations
```

### Migration Notes
- ✅ Similar pattern to volume
- ✅ Component selection like volume types
- **Special handling**: Carbon ratio calculations in format_output()

## 3. TPA Module Migration

### Current Implementation (450+ lines)
- Complex tree basis assignment
- Basal area calculations
- Species and size class handling

### New Implementation (~120 lines)
```python
class TPAEstimator(BaseEstimator):
    # Only implement:
    - get_required_tables(): Standard tables
    - get_response_columns(): {"TPA": "TPA", "BAA": "BAA"}
    - calculate_values(): Add TREE_BASIS and BASAL_AREA columns
    - apply_module_filters(): Apply diameter thresholds
    - _assign_tree_basis(): Custom logic for tree basis
    - format_output(): Species name handling
```

### Migration Notes
- ✅ Major code reduction from complex helper functions
- ⚠️ Tree basis assignment needs careful migration
- **Special handling**: Species reference table joins

## 4. Area Module Migration

### Current Implementation (380+ lines)
- No tree data in some cases
- Land type indicators
- Domain indicators for different land classes

### New Implementation (~110 lines)
```python
class AreaEstimator(BaseEstimator):
    # Override more methods due to unique pattern:
    - get_required_tables(): ["PLOT", "COND", "POP_*"] (no TREE usually)
    - calculate_values(): Calculate domain indicators
    - _prepare_estimation_data(): Special handling for no trees
    - _calculate_plot_estimates(): Aggregate CONDPROP_UNADJ
    - _add_land_type_categories(): Land classification logic
```

### Migration Notes
- ⚠️ Most different from standard pattern
- ⚠️ Requires more method overrides
- **Special handling**: Optional tree data, land type indicators

## 5. Mortality Module Migration

### Current Implementation (350+ lines)
- DuckDB query optimization
- GRM table handling
- Complex WHERE clause construction

### New Implementation (~140 lines)
```python
class MortalityEstimator(BaseEstimator):
    def __init__(self, db, **kwargs):
        super().__init__(db, **kwargs)
        self.query_optimizer = DuckDBQueryOptimizer(db)
    
    # Override data loading completely:
    - _get_filtered_data(): Use DuckDB optimized queries
    - get_required_tables(): Include GRM tables
    - calculate_values(): Use pre-calculated mortality columns
    - _get_mortality_column(): Select based on tree_class/land_type
```

### Migration Notes
- ⚠️ Requires composition with DuckDBQueryOptimizer
- ⚠️ Different data loading pattern
- **Special handling**: Direct DuckDB queries for performance

## 6. Growth Module Migration

### Current Implementation (400+ lines)
- Multiple growth components (recruitment, diameter, volume, biomass)
- Complex GRM table joins
- Component-specific calculations

### New Implementation (~150 lines)
```python
class GrowthEstimator(BaseEstimator):
    # Multiple calculation strategies:
    - calculate_values(): Dispatch to component calculators
    - _calculate_recruitment(): Handle new trees
    - _calculate_diameter_growth(): Survivor growth
    - _calculate_volume_growth(): Volume increment
    - _calculate_biomass_growth(): Biomass increment
    - _combine_growth_results(): Merge components
```

### Migration Notes
- ⚠️ Most complex due to multiple components
- ⚠️ May benefit from strategy pattern for components
- **Special handling**: GRM table relationships

## Implementation Priority & Risk Assessment

### Migration Order (Low to High Risk)

1. **Volume** (Low Risk) ✅
   - Clean implementation complete
   - Clear pattern for others to follow
   - Test coverage exists

2. **Biomass** (Low Risk)
   - Nearly identical to volume pattern
   - Simple component selection
   - Week 1 completion

3. **TPA** (Medium Risk)
   - Tree basis assignment complexity
   - Species reference handling
   - Week 1-2 completion

4. **Area** (Medium Risk)
   - Different workflow pattern
   - Optional tree data
   - Week 2 completion

5. **Mortality** (High Risk)
   - DuckDB optimization critical
   - Performance requirements
   - Week 2-3 completion

6. **Growth** (High Risk)
   - Most complex logic
   - Multiple components
   - Week 3 completion

## Common Patterns Identified

### Standard Pattern (Volume, Biomass, TPA)
```python
class StandardEstimator(BaseEstimator):
    def calculate_values(self, data):
        # Simple calculation: COLUMN * TPA_UNADJ
        return data.with_columns([...])
```

### Complex Pattern (Area)
```python
class ComplexEstimator(BaseEstimator):
    def _prepare_estimation_data(self, tree_df, cond_df):
        # Override for different data preparation
        if self.uses_tree_data():
            return super()._prepare_estimation_data(tree_df, cond_df)
        else:
            # Custom logic for condition-only estimation
            return self._prepare_condition_data(cond_df)
```

### Optimized Pattern (Mortality)
```python
class OptimizedEstimator(BaseEstimator):
    def _get_filtered_data(self):
        # Complete override for performance
        return self.query_optimizer.get_optimized_data(self.config)
```

## Testing Strategy for Migration

### 1. Unit Tests per Module
```python
def test_volume_estimator_matches_original():
    """Ensure VolumeEstimator produces identical results."""
    original_result = volume_original(db, **params)
    new_result = VolumeEstimator(db, **params).estimate()
    assert_frames_equal(original_result, new_result)
```

### 2. Performance Tests
```python
def test_estimator_performance():
    """Ensure no performance regression."""
    time_original = timeit(lambda: volume_original(db))
    time_new = timeit(lambda: VolumeEstimator(db).estimate())
    assert time_new <= time_original * 1.1  # Allow 10% overhead
```

### 3. Statistical Validation
```python
def test_statistical_accuracy():
    """Validate against rFIA benchmarks."""
    pyfia_result = VolumeEstimator(db).estimate()
    rfia_result = load_rfia_benchmark()
    assert_statistical_equivalence(pyfia_result, rfia_result)
```

## Configuration Mapping

### Current Parameter Names → EstimatorConfig

| Original Parameter | EstimatorConfig Field | Notes |
|-------------------|----------------------|-------|
| grp_by | grp_by | Direct mapping |
| bySpecies | by_species | Name change for Python |
| bySizeClass | by_size_class | Name change for Python |
| landType | land_type | Name change for Python |
| treeType | tree_type | Name change for Python |
| treeDomain | tree_domain | Name change for Python |
| areaDomain | area_domain | Name change for Python |
| method | method | Direct mapping |
| lambda | lambda_ | Python keyword conflict |
| totals | totals | Direct mapping |
| variance | variance | Direct mapping |
| byPlot | by_plot | Name change for Python |
| nCores | (ignored) | Not implemented |
| mr | most_recent | Name change for clarity |

### Module-Specific Parameters

| Module | Parameter | Storage Location |
|--------|-----------|-----------------|
| Volume | vol_type | VolumeEstimator attribute |
| Biomass | component | BiomassEstimator attribute |
| Biomass | model_snag | EstimatorConfig.extra_params |
| Mortality | tree_class | MortalityEstimator attribute |
| Area | by_land_type | EstimatorConfig.extra_params |

## Success Metrics

### Code Quality Metrics
- ✅ 60-70% reduction in module code
- ✅ <100 lines per estimator subclass
- ✅ Zero code duplication between modules
- ✅ 100% test coverage maintained

### Performance Metrics
- ✅ No regression in execution time
- ✅ Memory usage equal or better
- ✅ DuckDB optimization preserved (Mortality)

### Maintainability Metrics
- ✅ Single workflow implementation
- ✅ Clear extension points
- ✅ Consistent error handling
- ✅ Simplified debugging

## Risk Mitigation

### Risk 1: Performance Regression
**Mitigation**: 
- Keep direct DuckDB queries where needed
- Profile before/after each migration
- Use lazy evaluation throughout

### Risk 2: Statistical Accuracy
**Mitigation**:
- Comprehensive regression tests
- Compare with rFIA outputs
- Validate with known benchmarks

### Risk 3: API Breaking Changes
**Mitigation**:
- Maintain wrapper functions
- Deprecation warnings
- Clear migration guide

### Risk 4: Complex Special Cases
**Mitigation**:
- Allow complete method overrides
- Composition for special handlers
- Document extension patterns

## Conclusion

The BaseEstimator architecture is well-suited for all six estimation modules. While Area, Mortality, and Growth require more customization, the benefits of standardization far outweigh the implementation complexity. The phased migration approach, starting with simpler modules, will validate the architecture and build confidence before tackling complex cases.