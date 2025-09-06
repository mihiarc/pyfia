# pyFIA Estimator Refactoring Guide

## Executive Summary

This document describes the strategy for refactoring pyFIA's existing estimators to use the new pipeline framework while maintaining full backward compatibility and identical statistical results.

## Refactoring Objectives

1. **Maintain 100% Backward Compatibility**: All existing function signatures and class interfaces must remain identical
2. **Preserve Statistical Results**: Refactored estimators must produce identical numerical results
3. **Add Pipeline Capabilities**: Expose new pipeline-aware methods for advanced users
4. **Enable Gradual Migration**: Allow refactoring one estimator at a time without breaking others
5. **Improve Performance**: Leverage pipeline optimizations where possible

## Implementation Strategy

### Phase 1: Pipeline Wrapper (Demonstrated)

Create a wrapper that maintains the existing implementation while adding pipeline interface:

```python
class AreaEstimator(OriginalAreaEstimator):
    def __init__(self, db, config):
        super().__init__(db, config)
        self._pipeline = self._build_pipeline()
    
    def estimate(self):
        # Can delegate to original or use pipeline
        return self._pipeline.execute(self.db, self.config)
    
    # New pipeline-aware methods
    def get_pipeline(self):
        return self._pipeline
    
    def get_execution_metrics(self):
        return self._execution_metrics
```

**Status**: ✅ Implemented in `area_refactored_simple.py`

### Phase 2: Internal Refactoring

Replace monolithic internal methods with pipeline steps:

```python
# Before: Monolithic method
def calculate_values(self, data):
    # 200+ lines of complex logic
    ...

# After: Pipeline steps
class CalculateAreaStep(PipelineStep):
    def execute(self, input_data, context):
        # Focused, testable logic
        ...
```

### Phase 3: Full Pipeline Integration

Integrate with the complete pipeline framework:

```python
def area(...):
    pipeline = create_area_pipeline(
        land_type=land_type,
        grp_by=grp_by,
        ...
    )
    return pipeline.execute(db, config)
```

## Estimator-Specific Refactoring Plans

### 1. Area Estimator (`area.py`)
- **Complexity**: Low
- **Status**: Phase 1 Complete (simplified version)
- **Key Steps**:
  1. Load COND and PLOT tables
  2. Apply land type filtering
  3. Calculate condition proportions
  4. Aggregate to plot level
  5. Apply stratification
  6. Calculate population estimates
  7. Format output

### 2. Volume Estimator (`volume.py`)
- **Complexity**: Medium
- **Status**: Ready for Phase 1
- **Key Steps**:
  1. Load TREE, COND, PLOT tables
  2. Apply tree and area domains
  3. Calculate tree volumes (VOLCFNET, etc.)
  4. Aggregate by plot/species/size class
  5. Apply stratification
  6. Calculate population estimates
  7. Format output

### 3. Biomass Estimator (`biomass.py`)
- **Complexity**: Medium
- **Status**: Ready for Phase 1
- **Key Steps**:
  1. Load TREE, COND, PLOT tables
  2. Apply domains
  3. Calculate biomass components
  4. Aggregate by groups
  5. Apply stratification
  6. Calculate population estimates
  7. Format output

### 4. TPA Estimator (`tpa.py`)
- **Complexity**: Low-Medium
- **Status**: Ready for Phase 1
- **Key Steps**:
  1. Load TREE, COND, PLOT tables
  2. Apply domains
  3. Calculate trees per acre
  4. Aggregate by groups
  5. Apply stratification
  6. Calculate population estimates
  7. Format output

### 5. Mortality Estimator (`mortality.py`)
- **Complexity**: High
- **Status**: Requires careful planning
- **Key Steps**:
  1. Load mortality-specific tables
  2. Apply complex temporal filters
  3. Calculate mortality by cause
  4. Handle remeasurement logic
  5. Aggregate by groups
  6. Apply stratification
  7. Calculate population estimates
  8. Format output

### 6. Growth Estimator (`growth.py`)
- **Complexity**: High
- **Status**: Requires careful planning
- **Key Steps**:
  1. Load growth tables (TREE_GRM_*)
  2. Apply temporal filters
  3. Calculate growth components
  4. Handle ingrowth/mortality
  5. Aggregate by groups
  6. Apply stratification
  7. Calculate population estimates
  8. Format output

## Testing Strategy

### 1. Unit Tests for Pipeline Steps
```python
def test_calculate_area_step():
    step = CalculateAreaStep()
    input_data = create_test_data()
    result = step.execute(input_data, context)
    assert_expected_output(result)
```

### 2. Integration Tests
```python
def test_area_pipeline_integration():
    pipeline = create_area_pipeline(...)
    result = pipeline.execute(db, config)
    assert result.equals(expected_result)
```

### 3. Backward Compatibility Tests
```python
def test_backward_compatibility():
    # Original
    result_orig = area_original(db, **params)
    
    # Refactored
    result_new = area_refactored(db, **params)
    
    assert results_identical(result_orig, result_new)
```

### 4. Performance Benchmarks
```python
def benchmark_area_estimation():
    with Timer() as original_time:
        result_orig = area_original(large_db)
    
    with Timer() as pipeline_time:
        result_new = area_refactored(large_db)
    
    assert pipeline_time < original_time * 1.1  # Max 10% slower
```

## New Pipeline-Aware Methods

All refactored estimators will gain these methods:

### 1. `get_pipeline()` 
Returns the underlying pipeline for inspection and customization.

### 2. `estimate_with_pipeline(custom_pipeline)`
Execute with a user-provided pipeline.

### 3. `get_execution_metrics()`
Returns detailed performance metrics:
- Total execution time
- Time per step
- Memory usage
- Cache hit rates

### 4. `get_pipeline_steps()`
Returns list of pipeline step names.

### 5. `describe_pipeline()`
Returns human-readable pipeline description.

### 6. `rebuild_pipeline(**overrides)`
Create a new pipeline with modified parameters.

## Implementation Files

### Core Refactored Estimators
- `/src/pyfia/estimation/area_refactored.py` - Full pipeline integration (when ready)
- `/src/pyfia/estimation/area_refactored_simple.py` - Simplified demonstration ✅
- `/src/pyfia/estimation/volume_refactored.py` - To be implemented
- `/src/pyfia/estimation/biomass_refactored.py` - To be implemented
- `/src/pyfia/estimation/tpa_refactored.py` - To be implemented
- `/src/pyfia/estimation/mortality_refactored.py` - To be implemented
- `/src/pyfia/estimation/growth_refactored.py` - To be implemented

### Test Files
- `/test_area_refactoring.py` - Validation tests ✅
- `/test_area_refactoring_demo.py` - Demonstration script ✅
- `/tests/test_refactored_estimators.py` - Comprehensive test suite (to be created)

## Benefits of Refactoring

### For Users
1. **Backward Compatibility**: Existing code continues to work
2. **Pipeline Transparency**: Can inspect and understand the workflow
3. **Customization**: Can modify pipeline steps for specific needs
4. **Performance Metrics**: Detailed timing information for optimization
5. **Gradual Adoption**: Can use new features when ready

### For Developers
1. **Modularity**: Each step is isolated and testable
2. **Reusability**: Steps can be shared across estimators
3. **Maintainability**: Easier to fix bugs and add features
4. **Type Safety**: Pipeline framework ensures type correctness
5. **Performance**: Opportunities for parallelization and caching

## Migration Timeline

### Month 1
- [x] Design refactoring approach
- [x] Create simplified demonstration (`area_refactored_simple.py`)
- [ ] Complete Phase 1 for area estimator

### Month 2
- [ ] Phase 1 for volume, biomass, and TPA estimators
- [ ] Comprehensive testing framework
- [ ] Performance benchmarks

### Month 3
- [ ] Phase 1 for mortality and growth estimators
- [ ] Begin Phase 2 internal refactoring
- [ ] Documentation and examples

### Month 4+
- [ ] Complete Phase 2 for all estimators
- [ ] Phase 3 full pipeline integration
- [ ] Deprecation notices for old implementations
- [ ] Final validation and release

## Validation Criteria

Before considering an estimator successfully refactored:

1. ✅ All existing tests pass without modification
2. ✅ Results match original implementation to within numerical precision
3. ✅ Performance is equal or better than original
4. ✅ New pipeline methods are tested and documented
5. ✅ Code review completed
6. ✅ Integration tests with real FIA data pass
7. ✅ Documentation updated

## Example Usage After Refactoring

```python
# Traditional usage (unchanged)
result = area(db, by_land_type=True, totals=True)

# Pipeline-aware usage (new capabilities)
estimator = AreaEstimator(db, config)

# Inspect pipeline
print(estimator.describe_pipeline())

# Get metrics
result = estimator.estimate()
metrics = estimator.get_execution_metrics()
print(f"Execution time: {metrics['total_time']}s")

# Custom pipeline
custom_pipeline = estimator.get_pipeline()
custom_pipeline.add_step(MyCustomStep())
result = estimator.estimate_with_pipeline(custom_pipeline)
```

## Conclusion

This refactoring strategy provides a clear path to modernize pyFIA's estimation system while maintaining full backward compatibility. The phased approach allows gradual migration with continuous validation, ensuring no disruption to existing users while enabling powerful new capabilities for advanced use cases.