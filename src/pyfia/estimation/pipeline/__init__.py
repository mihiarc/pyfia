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
    LoadTreeDataStep,
    LoadConditionDataStep,
    ApplyTreeDomainStep,
    JoinTreeConditionStep,
    CalculateVolumeStep,
    AggregateToPlotStep,
    ApplyStratificationStep,
    CalculateVarianceStep,
    CalculatePopulationTotalsStep,
    FormatOutputStep,
    create_volume_estimation_steps
)

# Build a volume estimation pipeline manually
pipeline = EstimationPipeline()
pipeline.add_step(LoadTreeDataStep(db, evalid=231720))
pipeline.add_step(LoadConditionDataStep(db, evalid=231720))
pipeline.add_step(ApplyTreeDomainStep(tree_domain="STATUSCD == 1 AND DIA >= 5.0"))
pipeline.add_step(JoinTreeConditionStep())
pipeline.add_step(CalculateVolumeStep(volume_type="net"))
pipeline.add_step(AggregateToPlotStep(value_columns=["VOLCFNET"]))
pipeline.add_step(ApplyStratificationStep(db, evalid=231720))
pipeline.add_step(CalculateVarianceStep())
pipeline.add_step(CalculatePopulationTotalsStep())
pipeline.add_step(FormatOutputStep())

# Or use convenience function
steps = create_volume_estimation_steps(
    db=db,
    evalid=231720,
    tree_domain="STATUSCD == 1 AND DIA >= 5.0",
    by_species=True,
    include_variance=True
)
pipeline = EstimationPipeline(steps)

# Execute the pipeline
result = pipeline.execute(ExecutionContext(db=db, config=config))
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
    LoadTreeDataStep,
    LoadConditionDataStep,
    LoadPlotDataStep,
    LoadStratumDataStep,
    LoadSeedlingDataStep,
    
    # Data filtering steps
    ApplyTreeDomainStep,
    ApplyAreaDomainStep,
    ApplyPlotDomainStep,
    ApplyLandTypeFilterStep,
    ApplyOwnershipFilterStep,
    
    # Data joining steps
    JoinTreeConditionStep,
    JoinWithPlotStep,
    JoinWithStratumStep,
    OptimizedJoinStep as OptimizedJoinDataStep,
    
    # Value calculation steps
    CalculateVolumeStep,
    CalculateBiomassStep,
    CalculateTPAStep,
    CalculateAreaStep,
    CalculateMortalityStep,
    CalculateGrowthStep,
    
    # Aggregation steps
    AggregateToPlotStep,
    AggregateBySpeciesStep,
    AggregateByDiameterClassStep,
    AggregateByOwnershipStep,
    GroupedAggregationStep,
    
    # Stratification steps
    ApplyStratificationStep,
    CalculateVarianceStep,
    CalculateStandardErrorStep,
    CalculatePopulationTotalsStep,
    ApplyExpansionFactorsStep,
    
    # Output formatting steps
    CalculatePopulationEstimatesStep,
    FormatOutputStep,
    AddTotalsStep,
    CalculatePercentagesStep,
    FormatVarianceOutputStep,
    
    # Convenience functions
    create_standard_loading_steps,
    create_standard_filtering_steps,
    create_volume_estimation_steps,
    create_area_estimation_steps,
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
    
    # Data loading steps
    "LoadTreeDataStep",
    "LoadConditionDataStep",
    "LoadPlotDataStep",
    "LoadStratumDataStep",
    "LoadSeedlingDataStep",
    
    # Filtering steps
    "ApplyTreeDomainStep",
    "ApplyAreaDomainStep",
    "ApplyPlotDomainStep",
    "ApplyLandTypeFilterStep",
    "ApplyOwnershipFilterStep",
    
    # Joining steps
    "JoinTreeConditionStep",
    "JoinWithPlotStep",
    "JoinWithStratumStep",
    "OptimizedJoinDataStep",
    
    # Value calculation steps
    "CalculateVolumeStep",
    "CalculateBiomassStep",
    "CalculateTPAStep",
    "CalculateAreaStep",
    "CalculateMortalityStep",
    "CalculateGrowthStep",
    
    # Aggregation steps
    "AggregateToPlotStep",
    "AggregateBySpeciesStep",
    "AggregateByDiameterClassStep",
    "AggregateByOwnershipStep",
    "GroupedAggregationStep",
    
    # Stratification steps
    "ApplyStratificationStep",
    "CalculateVarianceStep",
    "CalculateStandardErrorStep",
    "CalculatePopulationTotalsStep",
    "ApplyExpansionFactorsStep",
    
    # Output formatting steps
    "CalculatePopulationEstimatesStep",
    "FormatOutputStep",
    "AddTotalsStep",
    "CalculatePercentagesStep",
    "FormatVarianceOutputStep",
    
    # Convenience functions
    "create_standard_loading_steps",
    "create_standard_filtering_steps",
    "create_volume_estimation_steps",
    "create_area_estimation_steps",
    
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