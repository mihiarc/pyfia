"""
Pipeline Framework for pyFIA Phase 4 - Composable Estimation Workflows.

This module provides a comprehensive pipeline framework that transforms the current
monolithic estimator architecture into composable, testable pipeline components.

Key Features:
- Composable pipeline steps with strong type safety
- Lazy evaluation integration with Phase 2 infrastructure
- Query optimization integration with Phase 3 components
- Error handling and debugging capabilities
- Extension points for new estimation types
- Comprehensive testing support

Example Usage:
```python
from pyfia.estimation.pipeline import (
    EstimationPipeline, 
    LoadTablesStep, 
    FilterDataStep,
    JoinDataStep,
    CalculateTreeVolumesStep,
    AggregateByPlotStep,
    ApplyStratificationStep,
    CalculatePopulationEstimatesStep,
    FormatOutputStep
)

# Build a volume estimation pipeline
pipeline = (
    EstimationPipeline()
    .add_step(LoadTablesStep(tables=["PLOT", "COND", "TREE"]))
    .add_step(FilterDataStep(tree_domain="STATUSCD == 1", area_domain="COND_STATUS_CD == 1"))
    .add_step(JoinDataStep(join_strategy="optimized"))
    .add_step(CalculateTreeVolumesStep())
    .add_step(AggregateByPlotStep())
    .add_step(ApplyStratificationStep())
    .add_step(CalculatePopulationEstimatesStep())
    .add_step(FormatOutputStep())
)

# Execute the pipeline
result = pipeline.execute(db, config)
```

Architecture:
- `PipelineStep`: Abstract base for all pipeline steps
- `EstimationPipeline`: Main pipeline orchestrator
- `DataContract`: Type-safe data contracts between steps
- `ExecutionContext`: Runtime state and error handling
- Step implementations for common FIA estimation patterns
- Integration with Phase 3 query builders and optimization
"""

from .core import (
    # Core abstractions
    PipelineStep,
    EstimationPipeline,
    ExecutionContext,
    PipelineException,
    StepValidationError,
    
    # Step lifecycle
    StepStatus,
    StepResult,
    ExecutionMode,
)

from .contracts import (
    # Data contracts
    DataContract,
    RawTablesContract,
    FilteredDataContract,
    JoinedDataContract,
    ValuedDataContract,
    PlotEstimatesContract,
    StratifiedEstimatesContract,
    PopulationEstimatesContract,
    FormattedOutputContract,
)

# Alias for backward compatibility
TableDataContract = RawTablesContract

from .base_steps import (
    # Base step classes
    BaseEstimationStep,
    DataLoadingStep,
    FilteringStep,
    JoiningStep,
    CalculationStep,
    AggregationStep,
    FormattingStep,
)

from .steps import (
    # Data loading steps
    LoadTablesStep,
    LoadRequiredTablesStep,
    
    # Data filtering steps
    FilterDataStep,
    ApplyDomainFiltersStep,
    ApplyModuleFiltersStep,
    
    # Data joining steps
    JoinDataStep,
    OptimizedJoinStep,
    PrepareEstimationDataStep,
)

from .steps_calculations import (
    # Value calculation steps
    CalculateTreeVolumesStep,
    CalculateBiomassStep,
    CalculateAreaStep,
    CalculateGrowthStep,
    CalculateMortalityStep,
    CalculateTPAStep,
    
    # Aggregation steps
    AggregateByPlotStep,
    GroupByColumnsStep,
    
    # Statistical processing steps
    ApplyStratificationStep,
    CalculatePopulationEstimatesStep,
    ApplyVarianceCalculationStep,
    
    # Output formatting steps
    FormatOutputStep,
    AddMetadataStep,
    ValidateOutputStep,
)

from .builders import (
    # Pipeline builders
    PipelineBuilder,
    VolumeEstimationBuilder,
    BiomassEstimationBuilder,
    TPAEstimationBuilder,
    AreaEstimationBuilder,
    GrowthEstimationBuilder,
    MortalityEstimationBuilder,
    
    # Step factories
    StepFactory,
    ConditionalStepFactory,
)

from .extensions import (
    # Extension points
    CustomStep,
    ParameterizedStep,
    ConditionalStep,
    ParallelStep,
    
    # Middleware
    CachingMiddleware,
    LoggingMiddleware,
    ProfilingMiddleware,
    ValidationMiddleware,
)

from .testing import (
    # Testing utilities
    MockStep,
    StepTester,
    PipelineTester,
    TestDataFactory,
    AssertionStep,
)

__all__ = [
    # Core abstractions
    "PipelineStep",
    "EstimationPipeline", 
    "ExecutionContext",
    "PipelineException",
    "StepValidationError",
    
    # Data contracts
    "DataContract",
    "RawTablesContract",
    "TableDataContract",
    "FilteredDataContract",
    "JoinedDataContract",
    "ValuedDataContract",
    "PlotEstimatesContract",
    "StratifiedEstimatesContract",
    "PopulationEstimatesContract",
    "FormattedOutputContract",
    
    # Base step classes
    "BaseEstimationStep",
    "DataLoadingStep",
    "FilteringStep",
    "JoiningStep",
    "CalculationStep",
    "AggregationStep",
    "FormattingStep",
    
    # Step lifecycle
    "StepStatus",
    "StepResult",
    "ExecutionMode",
    
    # Core steps
    "LoadTablesStep",
    "LoadRequiredTablesStep",
    "FilterDataStep",
    "ApplyDomainFiltersStep",
    "ApplyModuleFiltersStep",
    "JoinDataStep",
    "OptimizedJoinStep",
    "PrepareEstimationDataStep",
    
    # Value calculation steps
    "CalculateTreeVolumesStep",
    "CalculateBiomassStep", 
    "CalculateAreaStep",
    "CalculateGrowthStep",
    "CalculateMortalityStep",
    "CalculateTPAStep",
    
    # Processing steps
    "AggregateByPlotStep",
    "GroupByColumnsStep",
    "ApplyStratificationStep",
    "CalculatePopulationEstimatesStep",
    "ApplyVarianceCalculationStep",
    "FormatOutputStep",
    "AddMetadataStep",
    "ValidateOutputStep",
    
    # Pipeline builders
    "PipelineBuilder",
    "VolumeEstimationBuilder",
    "BiomassEstimationBuilder",
    "TPAEstimationBuilder",
    "AreaEstimationBuilder", 
    "GrowthEstimationBuilder",
    "MortalityEstimationBuilder",
    "StepFactory",
    "ConditionalStepFactory",
    
    # Extensions
    "CustomStep",
    "ParameterizedStep",
    "ConditionalStep",
    "ParallelStep",
    "CachingMiddleware",
    "LoggingMiddleware",
    "ProfilingMiddleware",
    "ValidationMiddleware",
    
    # Testing
    "MockStep",
    "StepTester",
    "PipelineTester", 
    "TestDataFactory",
    "AssertionStep",
]