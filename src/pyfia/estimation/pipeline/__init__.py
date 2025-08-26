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
    
    # Quick creation functions
    create_volume_pipeline,
    create_biomass_pipeline,
    create_tpa_pipeline,
    create_area_pipeline,
    create_growth_pipeline,
    create_mortality_pipeline,
)

from .templates import (
    # Template system
    PipelineTemplate,
    TemplateRegistry,
    TemplateCategory,
    TemplateCustomizer,
    
    # Template functions
    get_template,
    list_templates,
    register_template,
    select_template,
    get_recommended_templates,
)

from .quick_start import (
    # Quick start functions
    create_volume_pipeline as quick_volume_pipeline,
    create_biomass_pipeline as quick_biomass_pipeline,
    create_tpa_pipeline as quick_tpa_pipeline,
    create_area_pipeline as quick_area_pipeline,
    create_growth_pipeline as quick_growth_pipeline,
    create_mortality_pipeline as quick_mortality_pipeline,
    create_pipeline_from_template,
    
    # Convenience functions
    quick_volume,
    quick_biomass,
    quick_carbon_assessment,
    quick_forest_inventory,
    
    # Migration helper
    migrate_to_pipeline,
)

from .factory import (
    # Factory system
    EstimationPipelineFactory,
    PipelineConfig,
    EstimationType,
    PipelineOptimizer as FactoryPipelineOptimizer,
    
    # Factory functions
    create_pipeline,
    create_from_config,
    create_from_template as factory_create_from_template,
    auto_detect_pipeline,
    validate_config,
    optimize_config,
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

# Import orchestration components
from .orchestrator import (
    AdvancedOrchestrator,
    DependencyResolver,
    ExecutionPlan,
    StepDependency,
    DependencyType,
    ExecutionStrategy,
    RetryConfig,
    RetryStrategy,
    ParallelExecutor,
    ConditionalExecutor,
    SkipStrategy,
)

from .validation import (
    PipelineValidator,
    SchemaValidator,
    ConfigurationValidator,
    StepCompatibilityValidator,
    PreExecutionValidator,
    ValidationLevel,
    ValidationResult,
    ValidationIssue,
    ValidationReport,
    validate_input,
    validate_output,
    validate_config,
)

from .monitoring import (
    PipelineMonitor,
    MetricsCollector,
    PerformanceMonitor,
    AlertManager,
    ExecutionHistory,
    RichProgressDisplay,
    Metric,
    MetricType,
    MetricUnit,
    Alert,
    AlertLevel,
    StepMetrics,
    PerformanceSnapshot,
)

from .error_handling import (
    ErrorRecoveryEngine,
    ErrorHandler,
    DataErrorHandler,
    ResourceErrorHandler,
    ComputationErrorHandler,
    CheckpointManager,
    RollbackManager,
    ErrorContext,
    ErrorReport,
    RecoveryAction,
    RecoveryStrategy,
    ErrorSeverity,
    ErrorCategory,
    GracefulDegradation,
)

from .optimizer import (
    PipelineOptimizer,
    OptimizationLevel,
    OptimizationHint,
    OptimizationResult,
    StepFusionOptimizer,
    QueryPushdownOptimizer,
    CacheOptimizer,
    DataLocalityOptimizer,
    FusionStrategy,
    CacheStrategy,
    PipelineABTester,
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
    "create_volume_pipeline",
    "create_biomass_pipeline",
    "create_tpa_pipeline",
    "create_area_pipeline",
    "create_growth_pipeline",
    "create_mortality_pipeline",
    
    # Template system
    "PipelineTemplate",
    "TemplateRegistry",
    "TemplateCategory",
    "TemplateCustomizer",
    "get_template",
    "list_templates",
    "register_template",
    "select_template",
    "get_recommended_templates",
    
    # Quick start
    "quick_volume_pipeline",
    "quick_biomass_pipeline",
    "quick_tpa_pipeline",
    "quick_area_pipeline",
    "quick_growth_pipeline",
    "quick_mortality_pipeline",
    "create_pipeline_from_template",
    "quick_volume",
    "quick_biomass",
    "quick_carbon_assessment",
    "quick_forest_inventory",
    "migrate_to_pipeline",
    
    # Factory system
    "EstimationPipelineFactory",
    "PipelineConfig",
    "EstimationType",
    "FactoryPipelineOptimizer",
    "create_pipeline",
    "create_from_config",
    "factory_create_from_template",
    "auto_detect_pipeline",
    "validate_config",
    "optimize_config",
    
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
    
    # Orchestration
    "AdvancedOrchestrator",
    "DependencyResolver",
    "ExecutionPlan",
    "StepDependency",
    "DependencyType",
    "ExecutionStrategy",
    "RetryConfig",
    "RetryStrategy",
    "ParallelExecutor",
    "ConditionalExecutor",
    "SkipStrategy",
    
    # Validation
    "PipelineValidator",
    "SchemaValidator",
    "ConfigurationValidator",
    "StepCompatibilityValidator",
    "PreExecutionValidator",
    "ValidationLevel",
    "ValidationResult",
    "ValidationIssue",
    "ValidationReport",
    "validate_input",
    "validate_output",
    "validate_config",
    
    # Monitoring
    "PipelineMonitor",
    "MetricsCollector",
    "PerformanceMonitor",
    "AlertManager",
    "ExecutionHistory",
    "RichProgressDisplay",
    "Metric",
    "MetricType",
    "MetricUnit",
    "Alert",
    "AlertLevel",
    "StepMetrics",
    "PerformanceSnapshot",
    
    # Error Handling
    "ErrorRecoveryEngine",
    "ErrorHandler",
    "DataErrorHandler",
    "ResourceErrorHandler",
    "ComputationErrorHandler",
    "CheckpointManager",
    "RollbackManager",
    "ErrorContext",
    "ErrorReport",
    "RecoveryAction",
    "RecoveryStrategy",
    "ErrorSeverity",
    "ErrorCategory",
    "GracefulDegradation",
    
    # Optimization
    "PipelineOptimizer",
    "OptimizationLevel",
    "OptimizationHint",
    "OptimizationResult",
    "StepFusionOptimizer",
    "QueryPushdownOptimizer",
    "CacheOptimizer",
    "DataLocalityOptimizer",
    "FusionStrategy",
    "CacheStrategy",
    "PipelineABTester",
]