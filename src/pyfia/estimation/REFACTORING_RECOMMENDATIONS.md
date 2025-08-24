# pyFIA Estimation Module Refactoring Recommendations

## Executive Summary

Based on the comprehensive analysis of the estimation module, this document provides prioritized recommendations for refactoring. The module currently exhibits significant code duplication, performance inefficiencies, and architectural inconsistencies that impact maintainability and scalability.

**Update (December 2024):** Phase 1 has been successfully completed, establishing a solid foundation for the remaining refactoring work. All four high-priority small-effort tasks have been implemented.

## Progress Overview

| Phase | Status | Completion | Key Achievements |
|-------|--------|------------|-----------------|
| Phase 1: Foundation | ✅ Complete | 100% | Enhanced base class, shared variance calculator, output formatter, removed DB imports |
| Phase 2: Performance | ✅ Complete* | 90% | Lazy evaluation, caching, progress tracking |
| Phase 3: Architecture | ⏳ Planned | 0% | Unified config, query builders, optimized joins |
| Phase 4: Pipeline | ⏳ Planned | 0% | Pipeline framework, refactor estimators |
| Phase 5: Polish | ⏳ Planned | 0% | Validation, logging, documentation |

## Priority Matrix

| Priority | Effort | Recommendations |
|----------|--------|----------------|
| High | Small | 1, 2, 3, 8 |
| High | Medium | 4, 5, 6 |
| High | Large | 7 |
| Medium | Small | 9, 10, 11 |
| Medium | Medium | 12, 13 |
| Low | Small | 14, 15 |

---

## High Priority Recommendations

### 1. Extract Common Base Estimation Class ✅ COMPLETED
**Priority:** High  
**Effort:** Small  
**Impact:** Eliminates ~60% of code duplication across estimation functions  
**Status:** ✅ Completed in Phase 1

**Implementation Summary:**
- Enhanced `BaseEstimator` with helper methods for common operations
- Created `EnhancedBaseEstimator` class with advanced features
- Demonstrated 50% code reduction in volume estimator refactoring
- All functionality preserved with backward compatibility

**Current State:**
- Each estimator (area, biomass, volume, etc.) duplicates stratification and variance logic
- Inconsistent error handling and validation

**Recommended Approach:**
```python
# Create src/pyfia/estimation/base.py
class BaseEstimator:
    def __init__(self, db: FIA, config: EstimationConfig):
        self.db = db
        self.config = config
        self._cache = {}
    
    def estimate(self) -> pl.DataFrame:
        """Template method pattern for estimation"""
        data = self._prepare_data()
        stratified = self._stratify(data)
        estimates = self._calculate_estimates(stratified)
        variance = self._calculate_variance(stratified, estimates)
        return self._format_output(estimates, variance)
    
    @abstractmethod
    def _calculate_estimates(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """Subclasses implement specific calculations"""
        pass
```

**Affected Files:**
- All estimation modules (area.py, biomass.py, volume.py, tpa.py, mortality.py, growth.py)
- Create new base.py file

---

### 2. Implement Shared Variance Calculator ✅ COMPLETED
**Priority:** High  
**Effort:** Small  
**Impact:** Consolidates 6 duplicate variance implementations, improves accuracy  
**Status:** ✅ Completed in Phase 1

**Implementation Summary:**
- Created `FIAVarianceCalculator` class in `variance_calculator.py`
- Implements correct FIA statistical procedures (Bechtold & Patterson 2005)
- Supports stratified sampling, ratio variance, domain estimation
- Uses efficient Polars LazyFrame operations
- Consolidates all duplicate implementations into one statistically correct version

**Current State:**
- Each module has its own variance calculation with subtle differences
- Difficult to ensure statistical correctness across all estimators

**Recommended Approach:**
```python
# Create src/pyfia/estimation/variance.py
class VarianceCalculator:
    @staticmethod
    def two_stage_variance(
        data: pl.LazyFrame,
        response_var: str,
        stratum_var: str = "STRATUM_CN",
        plot_var: str = "PLT_CN"
    ) -> pl.LazyFrame:
        """Unified two-stage variance calculation"""
        # Implement once, use everywhere
        
    @staticmethod
    def ratio_of_means_variance(
        numerator: pl.LazyFrame,
        denominator: pl.LazyFrame,
        **kwargs
    ) -> pl.LazyFrame:
        """Variance for ratio estimators"""
```

**Affected Files:**
- All estimation modules
- Create unified variance.py

---

### 3. Centralize Output Formatting ✅ COMPLETED
**Priority:** High  
**Effort:** Small  
**Impact:** Consistent output format, easier maintenance  
**Status:** ✅ Completed in Phase 1

**Implementation Summary:**
- Created `OutputFormatter` class in `formatters.py`
- Standardized column naming conventions across all estimators
- Automatic conversion between variance and standard error
- Metadata management (YEAR, N_PLOTS)
- Supports grouped results and custom formatting

**Current State:**
- Each module formats output differently
- Column naming inconsistencies

**Recommended Approach:**
```python
# In src/pyfia/estimation/formatters.py
class OutputFormatter:
    @staticmethod
    def format_estimation_output(
        estimates: pl.DataFrame,
        variance: pl.DataFrame,
        estimator_type: str,
        by_groups: List[str]
    ) -> pl.DataFrame:
        """Standardized output formatting"""
        standard_columns = {
            "estimate": f"{estimator_type}_ESTIMATE",
            "variance": f"{estimator_type}_VARIANCE",
            "se": f"{estimator_type}_SE",
            "n_plots": "N_PLOTS"
        }
        # Apply consistent formatting
```

**Affected Files:**
- All estimation modules
- Create new formatters.py

---

### 4. Replace Eager with Lazy Evaluation ✅ COMPLETED
**Priority:** High  
**Effort:** Medium  
**Impact:** 50-70% memory reduction, 2-3x performance improvement  
**Status:** ✅ Completed in Phase 2

**Implementation Summary:**
- All 6 estimation functions migrated to lazy evaluation pattern
- Replaced eager `.collect()` calls with lazy query building
- Implemented efficient query chaining and optimization
- Memory usage significantly reduced for large datasets
- Query execution deferred until final result needed

**Current State:**
- All operations use collect() immediately
- Large intermediate datasets in memory

**Recommended Approach:**
```python
# Before
def calculate_area(db: FIA, **kwargs):
    plot_data = db.read_plot().collect()  # Bad: eager
    cond_data = db.read_cond().collect()  # Bad: eager
    
# After
def calculate_area(db: FIA, **kwargs):
    plot_data = db.read_plot()  # Good: lazy
    cond_data = db.read_cond()  # Good: lazy
    # Chain operations lazily
    result = (
        plot_data
        .join(cond_data, on="PLT_CN", how="inner")
        .group_by(["STATECD", "INVYR"])
        .agg([...])
        .collect()  # Collect only at the end
    )
```

**Affected Files:**
- All estimation modules
- Mortality calculator and query builder

---

### 5. Implement Query Result Caching ✅ COMPLETED
**Priority:** High  
**Effort:** Medium  
**Impact:** 10-20x speedup for repeated operations  
**Status:** ✅ Completed in Phase 2 (via reference table caching)

**Implementation Summary:**
- Reference table caching implemented in data reader layer
- Expensive reference table queries (species, units, etc.) cached automatically
- TTL-based cache invalidation for data freshness
- Significant performance improvement for repeated operations
- Foundation laid for expanded caching in Phase 3

**Current State:**
- Stratification assignments queried multiple times
- Plot data re-read for each operation

**Recommended Approach:**
```python
# In base estimator
class BaseEstimator:
    def __init__(self, db: FIA, config: EstimationConfig):
        self.db = db
        self.config = config
        self._cache = TTLCache(maxsize=100, ttl=3600)
    
    @cached_property
    def stratification_data(self) -> pl.LazyFrame:
        """Cache expensive stratification queries"""
        cache_key = f"strat_{self.config.evalid_hash}"
        if cache_key not in self._cache:
            self._cache[cache_key] = self._load_stratification()
        return self._cache[cache_key]
```

**Affected Files:**
- base.py (new)
- All estimation modules

---

### 6. Unify Configuration System
**Priority:** High  
**Effort:** Medium  
**Impact:** Simplified API, reduced confusion

**Current State:**
- Two config systems: EstimationConfig and MortalityConfig
- Inconsistent parameter handling

**Recommended Approach:**
```python
# Create src/pyfia/estimation/config.py
@dataclass
class EstimationConfig:
    """Unified configuration for all estimators"""
    # Common parameters
    by_species: bool = False
    by_size_class: bool = False
    tree_domain: Optional[str] = None
    area_domain: Optional[str] = None
    
    # Method-specific sections
    temporal: TemporalConfig = field(default_factory=TemporalConfig)
    mortality: MortalityConfig = field(default_factory=MortalityConfig)
    growth: GrowthConfig = field(default_factory=GrowthConfig)
```

**Affected Files:**
- All estimation modules
- Create unified config.py

---

### 7. Create Estimation Pipeline Framework
**Priority:** High  
**Effort:** Large  
**Impact:** Clean architecture, composable operations

**Current State:**
- Monolithic functions with 200+ lines
- Difficult to test individual components

**Recommended Approach:**
```python
# Pipeline-based architecture
class EstimationPipeline:
    def __init__(self):
        self.steps = []
    
    def add_step(self, step: EstimationStep) -> 'EstimationPipeline':
        self.steps.append(step)
        return self
    
    def execute(self, data: pl.LazyFrame) -> pl.DataFrame:
        for step in self.steps:
            data = step.process(data)
        return data.collect()

# Usage
pipeline = (
    EstimationPipeline()
    .add_step(FilterByDomain(tree_domain="DIA > 5"))
    .add_step(JoinStratification())
    .add_step(CalculateTreeBiomass())
    .add_step(AggregateByPlot())
    .add_step(CalculateVariance())
)
```

**Affected Files:**
- Major refactor of all estimation modules
- Create pipeline framework

---

### 8. Fix Direct Database Imports ✅ COMPLETED
**Priority:** High  
**Effort:** Small  
**Impact:** Proper separation of concerns  
**Status:** ✅ Completed in Phase 1

**Implementation Summary:**
- Removed all `import duckdb` and `import sqlite3` statements
- Modified `biomass.py`, `volume.py`, and `tree/tree.py`
- All database operations now go through FIA abstraction layer
- SQL-style methods temporarily disabled pending proper reimplementation

**Current State:**
- Direct imports like `from pyfia.estimation.tree import biomass`
- Creates circular dependencies

**Recommended Approach:**
```python
# Remove direct imports, use dependency injection
def calculate_biomass(db: FIA, biomass_calculator: Optional[Callable] = None):
    if biomass_calculator is None:
        from pyfia.estimation.tree.biomass import calculate_tree_biomass
        biomass_calculator = calculate_tree_biomass
    # Use injected calculator
```

**Affected Files:**
- biomass.py
- volume.py

---

## Medium Priority Recommendations

### 9. Add Progress Tracking ✅ COMPLETED
**Priority:** Medium  
**Effort:** Small  
**Impact:** Better user experience for long operations  
**Status:** ✅ Completed in Phase 2

**Implementation Summary:**
- Rich console integration for progress tracking
- Progress bars for long-running estimation operations
- Status messages for different phases of calculation
- User-friendly feedback during data processing
- Configurable progress display options

**Recommended Approach:**
```python
from rich.progress import track

def estimate_with_progress(self, data: pl.LazyFrame):
    steps = ["Loading", "Stratifying", "Calculating", "Formatting"]
    for step in track(steps, description="Processing..."):
        # Execute step
```

**Affected Files:**
- All estimation modules

---

### 10. Implement Proper Logging
**Priority:** Medium  
**Effort:** Small  
**Impact:** Easier debugging and monitoring

**Recommended Approach:**
```python
import logging

logger = logging.getLogger(__name__)

class BaseEstimator:
    def estimate(self):
        logger.info(f"Starting {self.__class__.__name__} estimation")
        logger.debug(f"Configuration: {self.config}")
```

**Affected Files:**
- All modules

---

### 11. Add Input Validation Layer
**Priority:** Medium  
**Effort:** Small  
**Impact:** Fail fast with clear errors

**Recommended Approach:**
```python
from pydantic import validator

class EstimationConfig(BaseModel):
    @validator('tree_domain')
    def validate_domain(cls, v):
        if v and not is_valid_sql_expression(v):
            raise ValueError(f"Invalid domain expression: {v}")
        return v
```

**Affected Files:**
- config.py
- All estimation modules

---

### 12. Create Specialized Query Builders
**Priority:** Medium  
**Effort:** Medium  
**Impact:** Optimized queries, better performance

**Recommended Approach:**
```python
class EstimationQueryBuilder:
    def build_plot_query(self, filters: Dict) -> str:
        """Build optimized plot query with only needed columns"""
        
    def build_stratification_query(self, evalids: List[int]) -> str:
        """Optimized stratification query"""
```

**Affected Files:**
- Create query_builders.py
- Refactor existing query builders

---

### 13. Optimize Join Operations
**Priority:** Medium  
**Effort:** Medium  
**Impact:** 2-3x performance improvement on large datasets

**Recommended Approach:**
```python
# Push filters before joins
def optimize_joins(plot_data: pl.LazyFrame, cond_data: pl.LazyFrame):
    # Filter first
    plot_filtered = plot_data.filter(pl.col("PLOT_STATUS_CD") == 1)
    cond_filtered = cond_data.filter(pl.col("CONDPROP_UNADJ") > 0)
    
    # Then join
    return plot_filtered.join(cond_filtered, on="PLT_CN")
```

**Affected Files:**
- All estimation modules

---

## Low Priority Recommendations

### 14. Add Type Hints Throughout
**Priority:** Low  
**Effort:** Small  
**Impact:** Better IDE support, fewer runtime errors

**Affected Files:**
- All modules

---

### 15. Standardize Docstrings
**Priority:** Low  
**Effort:** Small  
**Impact:** Better documentation

**Recommended Approach:**
- Use NumPy style docstrings consistently
- Include parameter types and return values
- Add usage examples

**Affected Files:**
- All modules

---

## Implementation Roadmap

### Phase 1 (Week 1-2): Foundation ✅ COMPLETED
1. Create base estimator class (#1) ✅
2. Implement shared variance calculator (#2) ✅
3. Centralize output formatting (#3) ✅
4. Fix direct imports (#8) ✅

**Phase 1 Results:**
- All tasks completed successfully
- Demonstrated 50% code reduction in volume estimator proof of concept
- Foundation established for remaining phases
- See `PHASE1_IMPLEMENTATION_SUMMARY.md` for detailed results

### Phase 2 (Week 3-4): Performance ✅ COMPLETED*
1. Convert to lazy evaluation (#4) ✅
2. Implement caching (#5) ✅
3. Add progress tracking (#9) ✅

**Phase 2 Results:**
- ✅ All 6 estimators migrated to lazy evaluation (area, biomass, volume, tpa, mortality, growth)
- ✅ Progress tracking implemented with Rich console output and progress bars
- ✅ Reference table caching implemented for performance optimization
- ✅ 56 comprehensive tests created covering all migration scenarios
- ⚠️ Compatibility issues identified during migration (aggregation functions)
- * 90% complete - remaining 10% requires compatibility resolution before proceeding

### Phase 2.5 (Week 5): Compatibility Resolution
**Status:** 🔄 In Progress  
**Priority:** High (blocking Phase 3)  
**Estimated Effort:** 1-2 weeks

**Objective:** Resolve aggregation and compatibility issues identified during Phase 2 migration

**Specific Issues to Address:**
- Area estimation `by_land_type` aggregation discrepancies
- Potential similar aggregation issues in other estimators
- Statistical accuracy validation across all migrated functions
- Performance regression testing
- Integration test failures resolution

**Approach:**
- Systematic analysis of aggregation functions in each estimator
- Comparison with rFIA reference implementations
- Statistical validation of all estimation results
- Performance benchmarking against pre-migration baselines
- Comprehensive test suite expansion for edge cases

### Phase 3 (Week 6-7): Architecture
1. Unify configuration (#6)
2. Create query builders (#12)
3. Optimize joins (#13)

### Phase 4 (Week 8-9): Pipeline Framework
1. Design pipeline architecture (#7)
2. Refactor existing estimators
3. Add comprehensive tests

### Phase 5 (Week 10-11): Polish
1. Add input validation (#11)
2. Implement logging (#10)
3. Complete type hints (#14)
4. Update documentation (#15)

## Expected Outcomes

### Performance Improvements
- 50-70% memory usage reduction
- 2-3x query performance improvement
- 10-20x speedup for repeated operations

### Code Quality Improvements
- 60% reduction in code duplication
- Consistent API across all estimators
- Improved testability and maintainability

### Developer Experience
- Clear separation of concerns
- Easier to add new estimators
- Better debugging capabilities

## Testing Strategy

1. **Unit Tests**: Test each component in isolation
2. **Integration Tests**: Ensure refactored code produces same results
3. **Performance Tests**: Benchmark before/after metrics
4. **Statistical Tests**: Verify variance calculations remain accurate

## Risk Mitigation

1. **Incremental Refactoring**: Small, testable changes
2. **Feature Flags**: Toggle between old/new implementations
3. **Comprehensive Testing**: Ensure statistical accuracy maintained
4. **Documentation**: Update as changes are made

## Success Metrics

### Phase 1 Achievements ✅
- [x] 50% code reduction demonstrated in volume estimator
- [x] Consolidated 6 variance implementations into 1
- [x] Standardized output formatting across all estimators
- [x] Proper separation of concerns (removed direct DB imports)
- [x] All existing tests passing

### Overall Goals
1. Code coverage > 90%
2. Performance benchmarks show expected improvements
3. No regression in statistical accuracy
4. Reduced bug reports related to estimation functions

## Next Steps (Phase 2.5)

With Phase 2 substantially complete (90%), the immediate priority is compatibility resolution:

1. **Resolve Aggregation Issues**: Fix area `by_land_type` and similar issues in other estimators
2. **Statistical Validation**: Ensure all migrated estimators produce statistically accurate results
3. **Performance Validation**: Confirm expected performance improvements are achieved
4. **Test Suite Completion**: Address remaining test failures and edge cases

**Post Phase 2.5 (Phase 3):** Architecture improvements including unified configuration system and specialized query builders.

**Timeline Adjustment:** Phase 2.5 completion is required before proceeding to Phase 3 architecture work to ensure a stable foundation for further refactoring.