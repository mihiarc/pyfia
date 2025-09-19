# pyFIA Test Strategy and Implementation Plan

## Current State Summary

**Overall Test Coverage: 23%**
- **Critical Issue**: 40% of tests (25 files) fail due to outdated imports
- **Major Gaps**: EVALID system (0%), GRM operations (0%), database backends (14%)
- **Infrastructure**: Good fixtures and real data integration available

## Priority Implementation Plan

### Phase 1: Critical Repairs (Week 1)

#### 1.1 Fix Import Issues (HIGH PRIORITY)
**Files affected**: 25 test files

**Action Required**: Update all test imports to match refactored module structure:

```python
# OLD (failing)
from pyfia.estimation.area import area
from pyfia.filters import apply_tree_filters

# NEW (correct)
from pyfia import area
from pyfia.filtering.tree.filters import apply_tree_filters
```

**Script to fix imports**:
```bash
# Run this command to fix common import issues
find tests/ -name "*.py" -exec sed -i '' 's/from pyfia.estimation.area/from pyfia/g' {} \;
find tests/ -name "*.py" -exec sed -i '' 's/from pyfia.estimation.volume/from pyfia/g' {} \;
find tests/ -name "*.py" -exec sed -i '' 's/from pyfia.filters/from pyfia.filtering/g' {} \;
```

#### 1.2 Restore Core Test Coverage
- Fix `test_area.py`, `test_volume.py`, `test_biomass.py`
- Target: Achieve 40% coverage on estimation functions
- Install missing dependencies: `uv pip install hypothesis`

### Phase 2: Critical Missing Tests (Week 2)

#### 2.1 EVALID System Tests (CRITICAL)
**Priority**: Highest - Core to statistical validity

**Required Tests**:
```python
def test_evalid_filtering_basic()
def test_most_recent_evalid_selection()
def test_texas_duplicate_handling()
def test_evalid_consistency_across_tables()
```

**Target Coverage**: 80% of EVALID-related code paths

#### 2.2 Database Backend Tests
**Priority**: High - Core infrastructure

**Required Tests**:
```python
def test_backend_auto_detection()
def test_backend_switching()
def test_connection_error_handling()
def test_concurrent_connections()
```

### Phase 3: GRM Testing Infrastructure (Week 3)

#### 3.1 GRM Table Structure Tests
**Priority**: High - Required for mortality/growth/removals

**Test Coverage Needed**:
- Component identification (SURVIVOR, MORTALITY, CUT, INGROWTH)
- SUBPTYP_GRM adjustment factor application
- Diameter consistency across remeasurement periods
- TPA calculation validation

#### 3.2 Statistical Method Validation
**Priority**: Medium-High

**Property-Based Tests**:
- Volume ≤ Gross Volume ≤ Live Volume invariants
- Area estimates sum correctly across groups
- Variance calculations are non-negative
- Estimation repeatability

### Phase 4: Integration & Performance Testing (Week 4)

#### 4.1 Multi-State Database Testing
- Cross-state estimation consistency
- State-level vs combined estimation validation
- Performance regression tests

#### 4.2 Error Recovery Testing
- Corrupted database handling
- Missing table recovery
- Memory pressure simulation

## Test Organization Strategy

### 1. Test File Structure
```
tests/
├── unit/                      # Pure unit tests
│   ├── test_estimation_units.py
│   ├── test_filtering_units.py
│   └── test_backend_units.py
├── integration/               # Integration tests
│   ├── test_database_integration.py
│   ├── test_evalid_integration.py
│   └── test_multi_state_integration.py
├── property/                  # Property-based tests
│   ├── test_statistical_properties.py
│   └── test_estimation_invariants.py
├── performance/               # Performance tests
│   ├── test_benchmarks.py
│   └── test_regression.py
└── fixtures/                  # Shared fixtures
    ├── conftest.py
    ├── fixtures.py
    └── database_fixtures.py
```

### 2. Test Data Strategy

#### Real Data Integration
```python
@pytest.fixture(scope="session")
def real_fia_database():
    """Use real FIA data when PYFIA_DATABASE_PATH is set."""
    db_path = os.getenv("PYFIA_DATABASE_PATH")
    if db_path and Path(db_path).exists():
        return FIA(db_path)
    return None
```

#### Synthetic Data for Edge Cases
- Mock GRM tables with all component types
- Synthetic Texas data with duplicates
- Edge case databases (empty, corrupted, incomplete)

### 3. Coverage Targets by Module

| Module | Current | Target | Priority |
|--------|---------|---------|----------|
| Core FIA Class | 14% | 85% | High |
| Estimation Functions | 12-19% | 80% | High |
| EVALID System | 0% | 90% | Critical |
| GRM Operations | 0% | 75% | High |
| Database Backends | 14% | 80% | Medium |
| Filtering/Parsing | 11-80% | 85% | Medium |
| Statistical Functions | 19% | 90% | High |

**Overall Target**: 80% coverage within 4 weeks

## Quality Assurance Standards

### 1. Test Quality Metrics
- **Test Independence**: Each test should be runnable in isolation
- **Clear Naming**: `test_function_scenario_expected_outcome`
- **Comprehensive Assertions**: Test both success and error conditions
- **Performance Bounds**: Include timing assertions for critical paths

### 2. Test Documentation
Every test class should include:
```python
class TestEvalidSystem:
    """
    Test EVALID filtering and selection functionality.

    EVALID (evaluation ID) system is critical for statistical validity
    in FIA estimation. These tests ensure proper EVALID handling.
    """
```

### 3. Error Testing Standards
```python
def test_invalid_evalid_error_handling(self, sample_fia_instance):
    """Test descriptive errors for invalid EVALIDs."""
    with pytest.raises(ValueError) as exc_info:
        sample_fia_instance.clip_by_evalid([999999])  # Invalid EVALID

    # Error should be descriptive
    error_msg = str(exc_info.value)
    assert "EVALID" in error_msg
    assert "999999" in error_msg
```

## Performance Testing Strategy

### 1. Benchmark Tests
```python
@pytest.mark.performance
def test_area_estimation_performance_benchmark():
    """Area estimation should complete within performance bounds."""
    start_time = time.time()
    result = area(large_database, land_type="forest")
    duration = time.time() - start_time

    assert duration < 10.0, f"Area estimation took {duration:.2f}s (>10s limit)"
    assert len(result) > 0, "Should produce results"
```

### 2. Memory Usage Tests
```python
@pytest.mark.memory
def test_large_database_memory_usage():
    """Test memory efficiency with large databases."""
    import psutil
    process = psutil.Process()

    initial_memory = process.memory_info().rss / 1024 / 1024  # MB

    # Load large dataset
    result = volume(very_large_database, by_species=True)

    peak_memory = process.memory_info().rss / 1024 / 1024  # MB
    memory_increase = peak_memory - initial_memory

    # Should not use more than 500MB additional memory
    assert memory_increase < 500, f"Memory usage: {memory_increase:.1f}MB"
```

## Continuous Integration Strategy

### 1. Test Environments
- **Unit Tests**: Run on all commits (fast, <2 minutes)
- **Integration Tests**: Run on PR (with real data, <10 minutes)
- **Property Tests**: Run nightly (comprehensive, <30 minutes)
- **Performance Tests**: Run weekly (benchmark suite, <1 hour)

### 2. Test Data Management
```yaml
# .github/workflows/test.yml
env:
  PYFIA_TEST_DATABASE: "s3://pyfia-test-data/georgia.duckdb"
  PYFIA_LARGE_DATABASE: "s3://pyfia-test-data/nfi_south.duckdb"
```

### 3. Coverage Gates
- **Minimum Coverage**: 70% overall
- **Critical Modules**: 85% minimum (FIA class, estimation functions)
- **New Code**: 90% coverage required

## Implementation Timeline

### Week 1: Foundation Repair
- [ ] Fix all import issues (Day 1-2)
- [ ] Restore core estimation tests (Day 3-4)
- [ ] Achieve 40% overall coverage (Day 5)

### Week 2: Critical Gaps
- [ ] Implement EVALID system tests (Day 1-3)
- [ ] Add database backend tests (Day 4-5)
- [ ] Achieve 60% overall coverage

### Week 3: GRM & Advanced Testing
- [ ] Complete GRM table tests (Day 1-3)
- [ ] Add property-based tests (Day 4-5)
- [ ] Achieve 70% overall coverage

### Week 4: Integration & Performance
- [ ] Multi-state integration tests (Day 1-2)
- [ ] Performance regression suite (Day 3-4)
- [ ] Final coverage push to 80% (Day 5)

## Success Metrics

### Quantitative Goals
- **Coverage**: 80% overall, 90% on critical paths
- **Test Count**: 300+ comprehensive tests
- **Performance**: <10s for basic estimations, <30s for complex queries
- **Reliability**: 0 flaky tests, 95%+ test pass rate

### Qualitative Goals
- **Maintainability**: Clear test organization and documentation
- **Confidence**: Comprehensive edge case and error condition coverage
- **Regression Prevention**: Property-based tests catch statistical errors
- **Developer Experience**: Fast feedback loop, clear error messages

## Risk Mitigation

### High-Risk Areas
1. **Texas Data Complexity**: Comprehensive duplicate handling tests
2. **EVALID System**: Multi-evaluation scenario testing
3. **GRM Methodology**: Component interaction validation
4. **Performance Regression**: Continuous benchmark monitoring

### Contingency Plans
- **Blocked by Missing Data**: Synthetic data generation scripts
- **Complex Integration Issues**: Staged rollout with feature flags
- **Performance Degradation**: Fallback to cached/simplified algorithms
- **Test Infrastructure Problems**: Parallel test environments

This comprehensive test strategy addresses the critical gaps identified in the pyFIA codebase while providing a clear roadmap to achieve robust, maintainable test coverage.