# pyFIA Pipeline Framework - Phase 4

The pyFIA Pipeline Framework represents Phase 4 of the pyFIA evolution, transforming monolithic estimator architecture into composable, testable pipeline components while maintaining all existing functionality and performance characteristics.

## Overview

The pipeline framework addresses key limitations of the current monolithic estimator approach:

- **200+ line monolithic methods** → **Composable pipeline steps**
- **Duplicated workflow logic** → **Reusable step library**
- **Hard to test components** → **Individual step testing**
- **Limited extensibility** → **Plugin architecture**
- **Rigid workflows** → **Flexible composition**

## Architecture

### Core Components

```
┌─────────────────────────────────────────────────────────────────┐
│                    Pipeline Framework                           │
├─────────────────┬─────────────────┬─────────────────┬──────────┤
│   Core Layer    │  Steps Library  │   Extensions    │ Testing  │
├─────────────────┼─────────────────┼─────────────────┼──────────┤
│ • PipelineStep  │ • Data Loading  │ • CustomStep    │ • Mocks  │
│ • Pipeline      │ • Filtering     │ • Middleware    │ • Testers│
│ • DataContract  │ • Joining       │ • Conditional   │ • Assert │
│ • Context       │ • Calculation   │ • Parallel      │ • Bench  │
│ • Validation    │ • Aggregation   │ • Plugins       │ • Debug  │
└─────────────────┴─────────────────┴─────────────────┴──────────┘
```

### Data Flow

```
Tables → Filtered → Joined → Valued → Plots → Stratified → Population → Output
   ↓        ↓        ↓       ↓       ↓         ↓           ↓         ↓
 Load    Filter    Join   Calculate Aggregate  Stratify   Estimate  Format
```

## Quick Start

### Basic Usage

```python
from pyfia.estimation.pipeline import create_volume_pipeline

# Create a volume estimation pipeline
pipeline = create_volume_pipeline(
    by_species=True,
    tree_domain="STATUSCD == 1 and DIA >= 5.0",
    land_type="forest"
)

# Execute with FIA database
with FIA("path/to/fia.db") as db:
    db.clip_by_state(37, most_recent=True)
    result = pipeline.execute(db, config)
```

### Custom Pipeline Construction

```python
from pyfia.estimation.pipeline import (
    EstimationPipeline,
    LoadTablesStep,
    FilterDataStep,
    CalculateTreeVolumesStep,
    AggregateByPlotStep,
    FormatOutputStep
)

# Build pipeline step by step
pipeline = (
    EstimationPipeline()
    .add_step(LoadTablesStep(tables=["PLOT", "COND", "TREE"]))
    .add_step(FilterDataStep(tree_domain="STATUSCD == 1"))
    .add_step(CalculateTreeVolumesStep())
    .add_step(AggregateByPlotStep())
    .add_step(FormatOutputStep())
)
```

## Pipeline Steps

### Data Loading Steps

- **`LoadTablesStep`**: Load specific tables with optimization
- **`LoadRequiredTablesStep`**: Auto-load tables for estimation type

### Filtering Steps

- **`FilterDataStep`**: Apply domain filters to data
- **`ApplyDomainFiltersStep`**: Use config-based filtering
- **`ApplyModuleFiltersStep`**: Apply estimation-specific filters

### Data Processing Steps

- **`JoinDataStep`**: Join filtered data for estimation
- **`OptimizedJoinStep`**: Use Phase 3 join optimization
- **`PrepareEstimationDataStep`**: Complete data preparation

### Value Calculation Steps

- **`CalculateTreeVolumesStep`**: FIA volume equations
- **`CalculateBiomassStep`**: FIA biomass equations
- **`CalculateTPAStep`**: Trees per acre expansion
- **`CalculateAreaStep`**: Area estimates from conditions
- **`CalculateGrowthStep`**: Growth accounting methods
- **`CalculateMortalityStep`**: Mortality calculations

### Statistical Steps

- **`AggregateByPlotStep`**: Tree to plot level aggregation
- **`ApplyStratificationStep`**: Population stratification
- **`CalculatePopulationEstimatesStep`**: Design-based estimates
- **`ApplyVarianceCalculationStep`**: Proper FIA variance

### Output Steps

- **`FormatOutputStep`**: Final output formatting
- **`ValidateOutputStep`**: Output quality validation

## Data Contracts

Type-safe data contracts ensure proper data flow between steps:

```python
# Input/Output contracts define expected data structure
class ValuedDataContract(DataContract):
    data: LazyFrameWrapper
    value_columns: List[str]
    group_columns: List[str]
    
    def get_required_columns(self) -> set[str]:
        return {"PLT_CN"}.union(set(self.value_columns))
```

### Contract Types

- **`TableDataContract`**: Raw database tables
- **`FilteredDataContract`**: Filtered tree/condition/plot data
- **`JoinedDataContract`**: Joined data ready for calculation
- **`ValuedDataContract`**: Data with calculated values
- **`PlotEstimatesContract`**: Plot-level estimates
- **`StratifiedEstimatesContract`**: Stratified with expansion factors
- **`PopulationEstimatesContract`**: Final population estimates
- **`FormattedOutputContract`**: Formatted output data

## Advanced Features

### Pipeline Builders

Pre-configured builders for common estimation types:

```python
from pyfia.estimation.pipeline import VolumeEstimationBuilder

# Use builder for customization
builder = VolumeEstimationBuilder(debug=True, enable_caching=True)
builder.skip_step("calculate_variance")
builder.add_custom_step(custom_filter_step)
pipeline = builder.build(by_species=True)
```

### Conditional Steps

Execute steps based on configuration:

```python
from pyfia.estimation.pipeline import ConditionalStep

variance_step = ConditionalStep(
    condition=lambda ctx: ctx.config.variance,
    step=ApplyVarianceCalculationStep(),
    fallback_step=None  # Skip if condition is False
)
```

### Middleware

Cross-cutting concerns handled by middleware:

```python
from pyfia.estimation.pipeline import CachingMiddleware, LoggingMiddleware

# Middleware for caching and logging
caching = CachingMiddleware(ttl_seconds=600)
logging = LoggingMiddleware(log_level="INFO")
```

### Parallel Processing

Run independent operations in parallel:

```python
from pyfia.estimation.pipeline import ParallelStep

parallel_calc = ParallelStep(
    steps=[volume_step, biomass_step],
    combine_function=merge_calculations,
    max_workers=4
)
```

## Testing Framework

Comprehensive testing utilities for steps and pipelines:

### Step Testing

```python
from pyfia.estimation.pipeline.testing import StepTester

# Test individual step
tester = StepTester(CalculateTreeVolumesStep())
result = tester.test_with_mock_data(n_plots=100)

# Performance testing
perf_results = tester.run_performance_test(n_iterations=10)
```

### Pipeline Testing

```python
from pyfia.estimation.pipeline.testing import PipelineTester

# Test complete pipeline
tester = PipelineTester(volume_pipeline)
result = tester.test_pipeline_execution(
    expected_output_columns=["VOLUME_PER_ACRE"],
    min_output_records=1
)

# Benchmark testing
benchmarks = tester.run_pipeline_benchmark(
    plot_sizes=[10, 50, 100, 500]
)
```

### Mock Data

Realistic test data generation:

```python
from pyfia.estimation.pipeline.testing import TestDataFactory

# Generate mock FIA database
mock_db = TestDataFactory.create_complete_mock_database(
    n_plots=100,
    statecd=37
)
```

## Integration with Phase 3 Components

The pipeline framework fully integrates with Phase 3 optimizations:

### Query Builders

```python
# Automatic integration with query builders
LoadTablesStep()  # Uses TreeQueryBuilder, ConditionQueryBuilder, etc.
```

### Join Optimization

```python
# Leverages Phase 3 join optimizer
OptimizedJoinStep()  # Uses JoinOptimizer for optimal performance
```

### Caching System

```python
# Integrates with Phase 3 caching
CachingMiddleware()  # Uses MemoryCache with TTL support
```

### Lazy Evaluation

```python
# Full lazy evaluation support
LazyFrameWrapper  # Integrates with Phase 2 lazy infrastructure
```

## Extension Points

### Custom Steps

Create custom estimation logic:

```python
from pyfia.estimation.pipeline import CustomStep

def custom_calculation(input_data, context):
    # Your custom logic here
    return modified_data

custom_step = CustomStep(
    step_function=custom_calculation,
    input_contract=JoinedDataContract,
    output_contract=ValuedDataContract
)
```

### Custom Estimation Types

Build new estimation types:

```python
class CarbonEstimationBuilder(PipelineBuilder):
    def get_value_calculation_step(self, config):
        return CalculateCarbonStep()

# Use like built-in builders
carbon_pipeline = CarbonEstimationBuilder().build()
```

## Performance Characteristics

The pipeline framework maintains and improves upon existing performance:

### Lazy Evaluation

- **Phase 2 Integration**: Full lazy evaluation support
- **Memory Efficiency**: Deferred computations until needed
- **Query Optimization**: Predicate and projection pushdown

### Caching

- **Step-Level Caching**: Cache intermediate results
- **TTL Support**: Configurable cache expiration
- **Memory Management**: Automatic cache eviction

### Parallel Processing

- **Independent Steps**: Run parallel where possible
- **Thread Pool**: Configurable worker threads
- **Resource Management**: Memory and CPU aware

## Migration Guide

### Phase 1: Co-existence

Use pipeline framework alongside existing estimators:

```python
# Existing monolithic estimator
old_result = volume_estimator.estimate()

# New pipeline-based approach
new_result = volume_pipeline.execute(db, config)

# Validate results match
assert_results_equivalent(old_result, new_result)
```

### Phase 2: Gradual Migration

Replace estimator methods with pipeline execution:

```python
class VolumeEstimator(LazyBaseEstimator):
    def estimate(self) -> pl.DataFrame:
        # Use pipeline internally
        if self.use_pipeline:
            return self._pipeline.execute(self.db, self.config)
        else:
            return super().estimate()  # Fallback to old method
```

### Phase 3: Full Migration

Replace monolithic estimators with pipeline builders:

```python
# Old approach
estimator = VolumeEstimator(db, config)
result = estimator.estimate()

# New approach  
pipeline = VolumeEstimationBuilder().build(**config.model_dump())
result = pipeline.execute(db, config)
```

## Production Readiness

### Error Handling

- **Comprehensive Exception Hierarchy**: Specific error types
- **Error Propagation**: Proper error context and cause chains
- **Graceful Degradation**: Fallback mechanisms where appropriate

### Validation

- **Input Validation**: Data contract validation
- **Output Validation**: Result quality checks
- **Pipeline Validation**: Step compatibility verification

### Monitoring

- **Execution Metrics**: Timing, memory, record counts
- **Debug Information**: Detailed execution traces
- **Warning System**: Non-fatal issue reporting

### Documentation

- **API Documentation**: Complete docstrings
- **Examples**: Comprehensive usage examples  
- **Migration Guide**: Step-by-step migration process

## Future Extensions

### Planned Features

- **Visual Pipeline Builder**: GUI for pipeline construction
- **Pipeline Repository**: Shared pipeline templates
- **Advanced Optimizations**: Cost-based query optimization
- **Distributed Execution**: Multi-node pipeline execution
- **Real-time Streaming**: Support for streaming data sources

### Plugin System

- **Custom Estimators**: Plugin architecture for new estimation types
- **Data Sources**: Support for non-FIA data sources
- **Output Formats**: Custom output formatters
- **Middleware**: Custom cross-cutting concerns

## Examples

See `examples.py` for comprehensive usage examples including:

- Basic estimation pipelines
- Custom pipeline construction
- Advanced features usage
- Testing and validation
- Performance benchmarking
- Integration patterns

## API Reference

### Core Classes

- **`PipelineStep`**: Abstract base for all steps
- **`EstimationPipeline`**: Main pipeline orchestrator
- **`ExecutionContext`**: Runtime execution state
- **`DataContract`**: Type-safe data contracts

### Builder Classes

- **`PipelineBuilder`**: Abstract builder base
- **`VolumeEstimationBuilder`**: Volume estimation pipelines
- **`BiomassEstimationBuilder`**: Biomass estimation pipelines
- **`TPAEstimationBuilder`**: Trees per acre pipelines
- **`AreaEstimationBuilder`**: Area estimation pipelines
- **`GrowthEstimationBuilder`**: Growth estimation pipelines
- **`MortalityEstimationBuilder`**: Mortality estimation pipelines

### Extension Classes

- **`CustomStep`**: User-defined custom steps
- **`ConditionalStep`**: Conditional execution
- **`ParallelStep`**: Parallel processing
- **`PipelineMiddleware`**: Middleware base class

### Testing Classes

- **`StepTester`**: Individual step testing
- **`PipelineTester`**: Complete pipeline testing  
- **`TestDataFactory`**: Mock data generation
- **`AssertionStep`**: Embedded test assertions

The pipeline framework represents the future of pyFIA estimation architecture, providing the composability, testability, and extensibility needed for continued evolution while maintaining backward compatibility and performance characteristics.