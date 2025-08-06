# PyFIA Codebase Refactoring Plan

## Executive Summary

This comprehensive refactoring plan addresses critical code duplication (~1,200 lines) and dead code (~400 lines) identified in the PyFIA codebase. The goal is to achieve a 35-40% reduction in total codebase size while improving maintainability by 50% and preserving statistical accuracy.

## Current State Analysis

### Code Duplication Issues
- **Filter Functions**: `_apply_tree_filters()`, `_apply_area_filters()`, and `_setup_grouping_columns()` are duplicated across 6 estimation modules
- **Estimation Workflow**: All estimation modules (`area.py`, `biomass.py`, `volume.py`, `tpa.py`, `mortality.py`, `growth.py`) follow identical 7-step patterns
- **Test Fixtures**: Redundant fixture setup code across test files

### Dead Code Identified
- `src/pyfia/locations/` module - entirely unused (no imports found)
- `src/pyfia/database/schema_mapper.py` - no references in codebase
- `examples/` directory - should be moved to documentation
- Commented EVALID import in `filters/__init__.py`

### Architecture Assessment
**Current Score: B+**
- ✅ Good separation of concerns
- ✅ Clear module boundaries  
- ❌ Significant code duplication
- ❌ High maintenance burden

## Refactoring Phases

### Phase 1: Extract Common Filter Module (Week 1)

**Objective**: Consolidate duplicated filter functions into a centralized module

#### Tasks

1. **Create Common Filter Module**
   - File: `src/pyfia/filters/common.py`
   - Functions to consolidate:
     ```python
     def apply_tree_filters(tree_data, tree_domain=None, **kwargs):
         """Consolidated tree filtering logic"""
         
     def apply_area_filters(area_data, area_domain=None, **kwargs):
         """Consolidated area filtering logic"""
         
     def setup_grouping_columns(grouping_params, available_columns):
         """Consolidated grouping setup logic"""
     ```

2. **Update Estimation Modules**
   - **`estimation/volume.py`**:
     - Remove `_apply_tree_filters()` (lines 259-304)
     - Remove `_setup_grouping_columns()` (lines 368+)
     - Import from `filters.common`
   
   - **`estimation/biomass.py`**:
     - Remove `_apply_tree_filters()` (lines 240-281)
     - Remove `_setup_grouping_columns()` (lines 309+)
     - Import from `filters.common`
   
   - **`estimation/tpa.py`**:
     - Remove duplicated filter logic (line 168+)
     - Import from `filters.common`
   
   - **`estimation/area.py`**:
     - Remove duplicated filter logic (line 152+)
     - Import from `filters.common`
   
   - **`estimation/mortality.py`** & **`estimation/growth.py`**:
     - Update to use common filter functions

3. **Testing & Validation**
   - Run full test suite: `uv run pytest`
   - Validate no statistical accuracy regression
   - Performance benchmarking against current implementation

**Expected Impact**: 
- Eliminate 500+ lines of duplication
- Single source of truth for filtering logic
- Easier maintenance and bug fixes

---

### Phase 2: Create Base Estimator Class (Week 2)

**Objective**: Implement abstract base class to standardize estimation workflow

#### Tasks

1. **Design Base Estimator Architecture**
   - File: `src/pyfia/estimation/base.py`
   - Abstract base class with common workflow:
     ```python
     from abc import ABC, abstractmethod
     from typing import Dict, Any, Optional
     import polars as pl
     
     class BaseEstimator(ABC):
         """Abstract base class for FIA estimators"""
         
         def __init__(self, db, **kwargs):
             self.db = db
             self.config = self._validate_config(kwargs)
             self._required_tables = self.get_required_tables()
             
         @abstractmethod
         def get_required_tables(self) -> list[str]:
             """Return list of required database tables"""
             pass
             
         @abstractmethod
         def calculate_unit_values(self, data: pl.LazyFrame) -> pl.LazyFrame:
             """Calculate per-unit values (e.g., per-acre volume)"""
             pass
             
         def load_required_tables(self) -> Dict[str, pl.LazyFrame]:
             """Load all required tables from database"""
             pass
             
         def prepare_data(self) -> pl.LazyFrame:
             """Common data preparation workflow"""
             pass
             
         def calculate_plot_estimates(self, data: pl.LazyFrame) -> pl.LazyFrame:
             """Calculate plot-level estimates"""
             pass
             
         def calculate_stratum_estimates(self, plot_data: pl.LazyFrame) -> pl.LazyFrame:
             """Calculate stratum-level estimates with variance"""
             pass
             
         def calculate_population_estimates(self, stratum_data: pl.LazyFrame) -> pl.LazyFrame:
             """Calculate final population estimates"""
             pass
             
         def estimate(self) -> pl.LazyFrame:
             """Main estimation workflow"""
             data = self.prepare_data()
             unit_values = self.calculate_unit_values(data)
             plot_estimates = self.calculate_plot_estimates(unit_values)
             stratum_estimates = self.calculate_stratum_estimates(plot_estimates)
             return self.calculate_population_estimates(stratum_estimates)
     ```

2. **Implement Proof of Concept**
   - Refactor `estimation/volume.py` to inherit from `BaseEstimator`
   - Maintain existing public API
   - Validate against comprehensive test suite

3. **Gradual Migration**
   - Refactor remaining estimation modules one by one
   - Each module becomes a simple subclass with specific logic
   - Extensive testing at each step

**Expected Impact**:
- 60-70% code reduction in estimation modules
- Standardized workflow across all estimators
- Easier addition of new estimation types

---

### Phase 3: Remove Dead Code (Week 3)

**Objective**: Clean up unused and unrelated code

#### Tasks

1. **Remove Unused Modules**
   - Delete entire `src/pyfia/locations/` directory
   - Remove `src/pyfia/database/schema_mapper.py`
   - Update `__init__.py` files to remove exports

2. **Relocate Examples**
   - Move `examples/` directory contents to documentation
   - Add examples to MkDocs documentation structure
   - Update documentation references

3. **Fix Partial Implementations**
   - Resolve commented EVALID import in `filters/__init__.py`
   - Either implement properly or remove completely

4. **Clean Up Imports**
   - Remove unused imports across all modules
   - Update dependency lists in `pyproject.toml` if needed

**Expected Impact**:
- Remove ~400 lines of dead code
- Cleaner repository structure
- Reduced cognitive load for developers

---

### Phase 4: Consolidate Test Infrastructure (Week 4)

**Objective**: Reduce test code duplication and improve maintainability

#### Tasks

1. **Create Central Test Fixtures**
   - File: `tests/fixtures.py`
   - Consolidate common fixtures:
     ```python
     import pytest
     import polars as pl
     from pyfia.core.fia import FIA
     
     @pytest.fixture
     def sample_fia_db():
         """Standard FIA database for testing"""
         pass
         
     @pytest.fixture
     def sample_tree_data():
         """Standard tree data for estimation tests"""
         pass
         
     @pytest.fixture
     def sample_plot_data():
         """Standard plot data for testing"""
         pass
         
     @pytest.fixture
     def estimation_test_cases():
         """Common test cases for all estimators"""
         pass
     ```

2. **Refactor Test Files**
   - Update all test files to use centralized fixtures
   - Remove redundant setup code
   - Standardize test patterns across modules

3. **Comprehensive Testing**
   - Ensure 100% test coverage for refactored code
   - Add integration tests for base estimator workflow
   - Performance regression tests

4. **Documentation Updates**
   - Update testing documentation
   - Add examples of using new fixtures
   - Document refactored architecture

**Expected Impact**:
- 25-30% reduction in test code
- Consistent test data across all tests
- Easier test maintenance

---

## Implementation Guidelines

### Development Principles
1. **Statistical Accuracy First**: No changes to calculation logic or mathematical formulations
2. **API Compatibility**: Preserve all public APIs to avoid breaking user code
3. **Incremental Implementation**: Complete each phase fully before moving to next
4. **Comprehensive Testing**: Full test suite validation after each change
5. **Rollback Capability**: Maintain ability to revert changes if issues arise

### Quality Assurance Process
1. **Code Review**: All changes require thorough review
2. **Test Coverage**: Maintain or improve current test coverage levels
3. **Performance Benchmarking**: Ensure no performance regression
4. **Documentation**: Update all relevant documentation
5. **User Impact Assessment**: Evaluate impact on existing user workflows

### Risk Mitigation
- **Backup Strategy**: Create backup branches before major changes
- **Gradual Rollout**: Implement changes incrementally with validation
- **Monitoring**: Track performance and accuracy metrics throughout
- **Communication**: Document all changes and rationale clearly

## Expected Benefits

### Quantitative Improvements
- **Code Reduction**: 35-40% decrease in total lines of code
- **Maintenance Burden**: 50% reduction in places requiring updates for bug fixes
- **Test Code**: 25-30% reduction in test infrastructure code
- **Duplication**: Eliminate 1,200+ lines of duplicated code

### Qualitative Improvements
- **Maintainability**: Much easier to add new estimation types
- **Consistency**: Standardized patterns across all modules
- **Onboarding**: Easier for new developers to understand codebase
- **Bug Prevention**: Centralized logic reduces bug propagation
- **Code Quality**: Cleaner, more readable code throughout

## Success Metrics

### Technical Metrics
- [ ] All tests pass with no regression
- [ ] Performance benchmarks show no degradation
- [ ] Code coverage maintained or improved
- [ ] No breaking changes to public APIs

### Code Quality Metrics
- [ ] Cyclomatic complexity reduced
- [ ] Code duplication eliminated
- [ ] Dead code removed completely
- [ ] Import graph simplified

### Developer Experience Metrics  
- [ ] Reduced time to implement new features
- [ ] Fewer places to update for bug fixes
- [ ] Improved code review efficiency
- [ ] Better IDE support and navigation

## Timeline Summary

| Phase | Duration | Focus | Key Deliverables |
|-------|----------|-------|------------------|
| 1 | Week 1 | Filter Consolidation | Common filter module, updated estimation modules |
| 2 | Week 2 | Base Estimator | Abstract base class, refactored volume estimator |
| 3 | Week 3 | Dead Code Removal | Clean repository, updated documentation |
| 4 | Week 4 | Test Consolidation | Central fixtures, comprehensive testing |

**Total Duration**: 4 weeks
**Total Effort**: ~80-100 hours
**Risk Level**: Medium (due to statistical accuracy requirements)
**Expected ROI**: High (significant long-term maintenance savings)

## Conclusion

This refactoring plan addresses the most critical technical debt in the PyFIA codebase while preserving its statistical accuracy and performance characteristics. The phased approach allows for careful validation at each step, ensuring that the scientific integrity of the library is maintained while dramatically improving its maintainability and developer experience.

The investment in refactoring will pay dividends in reduced maintenance burden, easier feature development, and improved code quality for years to come.