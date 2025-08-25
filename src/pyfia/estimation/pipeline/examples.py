"""
Examples demonstrating the pyFIA Pipeline Framework.

This module provides comprehensive examples of how to use the pipeline
framework for different estimation types, custom workflows, and
advanced features like middleware and custom steps.
"""

from typing import Dict, List, Optional
import warnings

import polars as pl

from ...core import FIA
from ..config import EstimatorConfig, VolumeConfig, BiomassConfig
from ..lazy_evaluation import LazyFrameWrapper
from .core import ExecutionContext, DataContract, JoinedDataContract, ValuedDataContract
from .builders import (
    VolumeEstimationBuilder,
    BiomassEstimationBuilder,
    TPAEstimationBuilder,
    AreaEstimationBuilder,
    create_volume_pipeline,
    create_biomass_pipeline
)
from .extensions import (
    CustomStep,
    ConditionalStep,
    CachingMiddleware,
    LoggingMiddleware,
    ProfilingMiddleware
)
from .testing import TestDataFactory, PipelineTester, StepTester
from .steps import LoadTablesStep, FilterDataStep, JoinDataStep
from .steps_calculations import (
    CalculateTreeVolumesStep,
    AggregateByPlotStep,
    FormatOutputStep
)


# === Basic Usage Examples ===

def example_basic_volume_estimation():
    """
    Example: Basic volume estimation using pipeline builders.
    
    This example shows the simplest way to create and run a volume
    estimation pipeline using the built-in builders.
    """
    print("=== Basic Volume Estimation Pipeline ===")
    
    # Create a volume estimation pipeline with default settings
    pipeline = create_volume_pipeline(
        by_species=True,
        tree_domain="STATUSCD == 1 and DIA >= 5.0",
        land_type="forest"
    )
    
    # Print pipeline structure
    print("Pipeline structure:")
    print(pipeline.visualize_pipeline())
    
    # Example execution (would use real FIA database)
    print("\nPipeline created successfully!")
    print(f"Number of steps: {len(pipeline.steps)}")
    
    # Show validation results
    validation_issues = pipeline.validate_pipeline()
    if validation_issues:
        print(f"Validation issues: {validation_issues}")
    else:
        print("Pipeline validation passed!")


def example_basic_biomass_estimation():
    """
    Example: Basic biomass estimation with custom configuration.
    """
    print("\n=== Basic Biomass Estimation Pipeline ===")
    
    # Create biomass pipeline with custom module configuration
    pipeline = create_biomass_pipeline(
        by_species=True,
        by_size_class=True,
        tree_domain="STATUSCD == 1",
        area_domain="COND_STATUS_CD == 1",
        land_type="timber",
        # Module-specific configuration
        module_config={
            "component": "aboveground",
            "include_foliage": True,
            "units": "tons"
        }
    )
    
    print("Biomass pipeline created with custom configuration")
    print(f"Steps: {[step.step_id for step in pipeline.steps]}")


def example_area_estimation():
    """
    Example: Area estimation pipeline (no tree data needed).
    """
    print("\n=== Area Estimation Pipeline ===")
    
    # Area estimation works differently - no tree aggregation
    pipeline = AreaEstimationBuilder().build(
        land_type="forest",
        area_domain="COND_STATUS_CD == 1",
        grp_by="OWNGRPCD"
    )
    
    print("Area pipeline structure:")
    print(pipeline.visualize_pipeline())


# === Advanced Pipeline Construction ===

def example_custom_pipeline_with_builder():
    """
    Example: Building custom pipeline using builders with modifications.
    """
    print("\n=== Custom Pipeline with Builder Modifications ===")
    
    # Start with volume estimation builder
    builder = VolumeEstimationBuilder(debug=True, enable_caching=True)
    
    # Add custom step before value calculation
    def custom_filter_function(input_data, context):
        """Custom filtering logic."""
        # Get the joined data
        data_df = input_data.data.collect()
        
        # Apply custom filtering (e.g., only large trees)
        filtered_df = data_df.filter(pl.col("DIA") >= 10.0)
        
        # Update the data
        input_data.data = LazyFrameWrapper(filtered_df.lazy())
        return input_data
    
    custom_step = CustomStep(
        step_function=custom_filter_function,
        input_contract=JoinedDataContract,
        output_contract=JoinedDataContract,
        step_id="custom_large_trees_filter",
        description="Filter to only large trees (DIA >= 10)"
    )
    
    # Override the prepare_data step to include our custom filtering
    builder.add_custom_step(custom_step)
    
    # Skip variance calculation for faster execution
    builder.skip_step("calculate_variance")
    
    # Build the pipeline
    pipeline = builder.build(
        by_species=True,
        tree_domain="STATUSCD == 1"
    )
    
    print("Custom pipeline with additional filtering:")
    print(pipeline.visualize_pipeline())


def example_manual_pipeline_construction():
    """
    Example: Manually constructing a pipeline step by step.
    """
    print("\n=== Manual Pipeline Construction ===")
    
    from .core import EstimationPipeline
    
    # Create empty pipeline
    pipeline = EstimationPipeline(
        pipeline_id="manual_volume_pipeline",
        description="Manually constructed volume estimation pipeline",
        debug=True
    )
    
    # Add steps one by one
    pipeline.add_step(LoadTablesStep(
        tables=["PLOT", "COND", "TREE", "POP_STRATUM", "POP_PLOT_STRATUM_ASSGN"],
        step_id="load_data",
        description="Load required FIA tables"
    ))
    
    pipeline.add_step(FilterDataStep(
        tree_domain="STATUSCD == 1 and DIA >= 5.0",
        area_domain="COND_STATUS_CD == 1",
        step_id="apply_filters",
        description="Apply domain filters"
    ))
    
    pipeline.add_step(JoinDataStep(
        join_strategy="optimized",
        step_id="join_data",
        description="Join tree and condition data"
    ))
    
    pipeline.add_step(CalculateTreeVolumesStep(
        step_id="calculate_volumes",
        description="Calculate tree-level volumes"
    ))
    
    pipeline.add_step(AggregateByPlotStep(
        step_id="aggregate_plots",
        description="Aggregate to plot level"
    ))
    
    pipeline.add_step(FormatOutputStep(
        step_id="format_output",
        description="Format final output"
    ))
    
    print("Manually constructed pipeline:")
    print(pipeline.visualize_pipeline())


# === Advanced Features Examples ===

def example_conditional_steps():
    """
    Example: Using conditional steps for optional processing.
    """
    print("\n=== Conditional Steps Example ===")
    
    from .core import EstimationPipeline
    from .steps_calculations import ApplyVarianceCalculationStep
    
    # Create pipeline
    pipeline = EstimationPipeline(
        pipeline_id="conditional_pipeline",
        description="Pipeline with conditional variance calculation"
    )
    
    # Add basic steps...
    pipeline.add_step(LoadTablesStep(
        tables=["PLOT", "COND", "TREE"],
        step_id="load_data"
    ))
    
    # Add conditional variance step
    def should_calculate_variance(context):
        """Only calculate variance if requested in config."""
        return getattr(context.config, "variance", False)
    
    variance_step = ApplyVarianceCalculationStep(
        step_id="calculate_variance",
        description="Calculate detailed variance estimates"
    )
    
    conditional_variance = ConditionalStep(
        condition=should_calculate_variance,
        step=variance_step,
        step_id="conditional_variance",
        description="Calculate variance if requested"
    )
    
    pipeline.add_step(conditional_variance)
    
    print("Pipeline with conditional variance calculation created")


def example_middleware_usage():
    """
    Example: Using middleware for cross-cutting concerns.
    """
    print("\n=== Middleware Usage Example ===")
    
    # Create pipeline with middleware
    pipeline = create_volume_pipeline(
        by_species=True,
        debug=True  # Enable debug mode
    )
    
    # Note: Middleware would be integrated at the pipeline execution level
    # This is a conceptual example of how middleware would be used
    
    print("Example middleware setup:")
    
    # Caching middleware
    caching_middleware = CachingMiddleware(ttl_seconds=600)
    print("- Caching middleware: Cache step results for 10 minutes")
    
    # Logging middleware
    logging_middleware = LoggingMiddleware(
        log_level="INFO",
        include_data_info=True
    )
    print("- Logging middleware: Log step execution with data info")
    
    # Profiling middleware
    profiling_middleware = ProfilingMiddleware(
        collect_memory_stats=True
    )
    print("- Profiling middleware: Collect performance metrics")
    
    print("Middleware would be applied during pipeline execution")


def example_parallel_processing():
    """
    Example: Using parallel processing for independent operations.
    """
    print("\n=== Parallel Processing Example ===")
    
    from .extensions import ParallelStep
    from .steps_calculations import CalculateTreeVolumesStep, CalculateBiomassStep
    
    # Create steps that can run in parallel
    volume_step = CalculateTreeVolumesStep(
        step_id="calculate_volumes",
        description="Calculate volumes"
    )
    
    biomass_step = CalculateBiomassStep(
        step_id="calculate_biomass", 
        description="Calculate biomass"
    )
    
    # Combine function to merge results
    def combine_calculations(results, context):
        """Combine volume and biomass calculations."""
        volume_result = results[0]  # Volume calculation result
        biomass_result = results[1]  # Biomass calculation result
        
        # Merge the data frames
        volume_df = volume_result.data.collect()
        biomass_df = biomass_result.data.collect()
        
        # Join on common columns
        combined_df = volume_df.join(biomass_df, on=["PLT_CN", "CONDID", "TREE"])
        
        # Return combined result
        return ValuedDataContract(
            data=LazyFrameWrapper(combined_df.lazy()),
            value_columns=volume_result.value_columns + biomass_result.value_columns,
            group_columns=volume_result.group_columns
        )
    
    # Create parallel step
    parallel_calculations = ParallelStep(
        steps=[volume_step, biomass_step],
        combine_function=combine_calculations,
        max_workers=2,
        step_id="parallel_calculations",
        description="Calculate volumes and biomass in parallel"
    )
    
    print("Parallel calculation step created")
    print("This step will calculate volumes and biomass simultaneously")


# === Testing Examples ===

def example_step_testing():
    """
    Example: Testing individual pipeline steps.
    """
    print("\n=== Step Testing Example ===")
    
    # Create a step to test
    volume_step = CalculateTreeVolumesStep(
        step_id="test_volume_step",
        description="Volume calculation for testing"
    )
    
    # Create step tester
    tester = StepTester(volume_step)
    
    # Test with mock data
    result = tester.test_with_mock_data(
        n_plots=10,
        statecd=37,
        tree_domain="STATUSCD == 1"
    )
    
    print(f"Step test result: {'PASSED' if result.success else 'FAILED'}")
    if result.success:
        print(f"Execution time: {result.execution_time:.3f}s")
        print(f"Output type: {type(result.output).__name__}")
    else:
        print(f"Error: {result.error}")
    
    # Run performance test
    perf_results = tester.run_performance_test(
        n_iterations=5,
        n_plots=50
    )
    
    print("Performance test results:")
    print(f"  Mean execution time: {perf_results['mean_execution_time']:.3f}s")
    print(f"  Std deviation: {perf_results['std_execution_time']:.3f}s")


def example_pipeline_testing():
    """
    Example: Testing complete pipelines.
    """
    print("\n=== Pipeline Testing Example ===")
    
    # Create pipeline to test
    pipeline = create_volume_pipeline(
        by_species=True,
        tree_domain="STATUSCD == 1"
    )
    
    # Create pipeline tester
    tester = PipelineTester(pipeline)
    
    # Test pipeline validation
    validation_result = tester.test_pipeline_validation()
    print(f"Pipeline validation: {'PASSED' if validation_result['valid'] else 'FAILED'}")
    if not validation_result['valid']:
        print(f"Issues: {validation_result['issues']}")
    
    # Test pipeline execution with mock data
    mock_db = TestDataFactory.create_complete_mock_database(n_plots=20)
    config = EstimatorConfig(by_species=True, tree_domain="STATUSCD == 1")
    
    execution_result = tester.test_pipeline_execution(
        db=mock_db,
        config=config,
        expected_output_columns=["SPCD", "VOLUME_PER_ACRE"],
        min_output_records=1
    )
    
    print(f"Pipeline execution: {'PASSED' if execution_result['test_passed'] else 'FAILED'}")
    print(f"Execution time: {execution_result['execution_time']:.3f}s")
    if execution_result['success']:
        print(f"Output records: {execution_result['output_records']}")
        print(f"Output columns: {execution_result['output_columns']}")


def example_benchmark_testing():
    """
    Example: Benchmark testing with different data sizes.
    """
    print("\n=== Benchmark Testing Example ===")
    
    # Create a simple pipeline for benchmarking
    pipeline = TPAEstimationBuilder().build(
        tree_domain="STATUSCD == 1"
    )
    
    tester = PipelineTester(pipeline)
    
    # Run benchmark with different plot sizes
    benchmark_results = tester.run_pipeline_benchmark(
        n_iterations=3,
        plot_sizes=[10, 50, 100]
    )
    
    print("Benchmark results:")
    for n_plots, results in benchmark_results.items():
        if "error" in results:
            print(f"  {n_plots} plots: {results['error']}")
        else:
            print(f"  {n_plots} plots: {results['mean_time']:.3f}s ± {results['std_time']:.3f}s")


# === Integration Examples ===

def example_integration_with_existing_estimators():
    """
    Example: Integrating pipeline framework with existing estimators.
    """
    print("\n=== Integration with Existing Estimators ===")
    
    # This example shows how pipeline framework can be integrated
    # with existing monolithic estimators during migration
    
    print("Migration strategy:")
    print("1. Create pipeline equivalent of existing estimator")
    print("2. Test pipeline against existing implementation")
    print("3. Gradually replace monolithic methods with pipeline")
    print("4. Maintain backward compatibility during transition")
    
    # Example of wrapper that uses pipeline internally
    class PipelineBasedVolumeEstimator:
        """Volume estimator using pipeline framework internally."""
        
        def __init__(self, db: FIA, config: EstimatorConfig):
            self.db = db
            self.config = config
            self._pipeline = None
        
        def estimate(self) -> pl.DataFrame:
            """Main estimation method using pipeline."""
            if self._pipeline is None:
                self._pipeline = create_volume_pipeline(**self.config.model_dump())
            
            return self._pipeline.execute(self.db, self.config)
    
    print("Pipeline-based estimator wrapper created")
    print("This allows gradual migration from monolithic to pipeline architecture")


def example_custom_estimation_type():
    """
    Example: Creating custom estimation type with pipeline framework.
    """
    print("\n=== Custom Estimation Type Example ===")
    
    # Example: Create a "Carbon" estimation type
    from .steps_calculations import CalculateTreeValuesStep
    from .core import EstimationPipeline
    
    def calculate_carbon_values(df: pl.DataFrame) -> pl.DataFrame:
        """Calculate tree-level carbon estimates."""
        # Simple carbon calculation (biomass * 0.5)
        return df.with_columns([
            (pl.col("DRYBIO_AG") * 0.5).alias("TREE_CARBON_TONS")
        ])
    
    # Create custom carbon calculation step
    carbon_step = CalculateTreeValuesStep(
        value_column_name="TREE_CARBON_TONS",
        calculation_function=calculate_carbon_values,
        required_columns=["DRYBIO_AG"],
        step_id="calculate_carbon",
        description="Calculate tree-level carbon estimates"
    )
    
    # Create carbon estimation pipeline
    carbon_pipeline = EstimationPipeline(
        pipeline_id="carbon_estimation",
        description="Carbon estimation pipeline"
    )
    
    # Add standard data loading and preparation steps
    carbon_pipeline.add_step(LoadTablesStep(
        tables=["PLOT", "COND", "TREE"],
        step_id="load_tables"
    ))
    
    # Add the custom carbon calculation
    carbon_pipeline.add_step(carbon_step)
    
    # Add standard aggregation and output steps
    carbon_pipeline.add_step(AggregateByPlotStep(
        step_id="aggregate_plots"
    ))
    
    carbon_pipeline.add_step(FormatOutputStep(
        step_id="format_output"
    ))
    
    print("Custom carbon estimation pipeline created")
    print(f"Steps: {[step.step_id for step in carbon_pipeline.steps]}")


# === Main Example Runner ===

def run_all_examples():
    """Run all pipeline framework examples."""
    print("===== pyFIA Pipeline Framework Examples =====\n")
    
    try:
        example_basic_volume_estimation()
        example_basic_biomass_estimation() 
        example_area_estimation()
        example_custom_pipeline_with_builder()
        example_manual_pipeline_construction()
        example_conditional_steps()
        example_middleware_usage()
        example_parallel_processing()
        example_step_testing()
        example_pipeline_testing()
        example_benchmark_testing()
        example_integration_with_existing_estimators()
        example_custom_estimation_type()
        
        print("\n===== All Examples Completed Successfully =====")
        
    except Exception as e:
        print(f"\nExample failed with error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    run_all_examples()