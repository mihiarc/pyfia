"""
Comprehensive examples demonstrating pyFIA Phase 4 pipeline framework usage.

This module provides complete working examples that show how to use the
pipeline framework for common FIA estimation tasks, demonstrating the
integration of Phase 2 lazy evaluation, Phase 3 query optimization, and
the new Phase 4 composable pipeline architecture.
"""

from typing import Optional, List, Dict, Any
import time

from ...core import FIA
from ..config import EstimatorConfig, VolumeConfig
from .core import EstimationPipeline, ExecutionContext
from .contracts import RawTablesContract, FormattedOutputContract
from .steps import LoadTablesStep, FilterDataStep, JoinDataStep, PrepareEstimationDataStep
from .steps_calculations import (
    CalculateTreeVolumesStep, AggregateByPlotStep, ApplyStratificationStep,
    CalculatePopulationEstimatesStep, FormatOutputStep
)
from .base_steps import BaseEstimationStep


def create_volume_estimation_pipeline(
    tree_domain: Optional[str] = None,
    area_domain: Optional[str] = None,
    by_species: bool = False,
    by_size_class: bool = False
) -> EstimationPipeline:
    """
    Create a complete volume estimation pipeline.
    
    This example demonstrates how to build a composable pipeline for
    volume estimation that integrates all Phase 2, 3, and 4 components.
    
    Parameters
    ----------
    tree_domain : Optional[str]
        Tree domain filter (e.g., "STATUSCD == 1 AND DIA >= 5.0")
    area_domain : Optional[str]
        Area domain filter (e.g., "COND_STATUS_CD == 1")
    by_species : bool
        Whether to group results by species
    by_size_class : bool
        Whether to group results by diameter size class
        
    Returns
    -------
    EstimationPipeline
        Configured volume estimation pipeline
    """
    pipeline = EstimationPipeline(
        pipeline_id="volume_estimation",
        description="Complete FIA volume estimation with Phase 2-4 integration",
        fail_fast=True,
        enable_caching=True,
        debug=False
    )
    
    # Step 1: Load required tables using Phase 3 query optimization
    pipeline.add_step(LoadTablesStep(
        step_id="load_tables",
        description="Load TREE, COND, PLOT tables with optimized queries",
        tables=["TREE", "COND", "PLOT", "POP_PLOT_STRATUM_ASSGN"],
        apply_evalid_filter=True
    ))
    
    # Step 2: Apply domain filters using Phase 2 lazy evaluation
    pipeline.add_step(FilterDataStep(
        step_id="apply_filters",
        description="Apply tree and area domain filters",
        tree_domain=tree_domain,
        area_domain=area_domain
    ))
    
    # Step 3: Join tables with optimization
    pipeline.add_step(JoinDataStep(
        step_id="join_data",
        description="Join filtered tables for estimation",
        join_strategy="optimized"
    ))
    
    # Step 4: Calculate tree volumes using equations
    pipeline.add_step(CalculateTreeVolumesStep(
        step_id="calculate_volumes",
        description="Calculate tree volumes using FIA equations"
    ))
    
    # Step 5: Aggregate to plot level
    pipeline.add_step(AggregateByPlotStep(
        step_id="aggregate_plots",
        description="Aggregate tree volumes to plot level"
    ))
    
    # Step 6: Apply stratification and expansion factors
    pipeline.add_step(ApplyStratificationStep(
        step_id="apply_stratification", 
        description="Apply post-stratification and expansion factors"
    ))
    
    # Step 7: Calculate population estimates with variance
    pipeline.add_step(CalculatePopulationEstimatesStep(
        step_id="population_estimates",
        description="Calculate final population estimates and variance"
    ))
    
    # Step 8: Format output for user consumption
    pipeline.add_step(FormatOutputStep(
        step_id="format_output",
        description="Format results for user consumption"
    ))
    
    return pipeline


def run_volume_estimation_example(db_path: str) -> None:
    """
    Run a complete volume estimation example.
    
    Parameters
    ----------
    db_path : str
        Path to FIA database
    """
    print("🌲 pyFIA Phase 4 Pipeline Example: Volume Estimation")
    print("=" * 60)
    
    # Initialize FIA database connection
    print("📊 Connecting to FIA database...")
    with FIA(db_path) as db:
        # Filter to a single state for demonstration
        print("🌍 Applying geographic filters...")
        db.clip_by_state(37)  # North Carolina
        db.clip_most_recent("VOL")
        
        # Create configuration
        config = VolumeConfig(
            land_type="forest",
            temporal_method="TI",
            variance_method="post_stratified",
            by_species=True
        )
        
        print("🔧 Building estimation pipeline...")
        pipeline = create_volume_estimation_pipeline(
            tree_domain="STATUSCD == 1 AND DIA >= 5.0",
            area_domain="COND_STATUS_CD == 1",
            by_species=True,
            by_size_class=False
        )
        
        print("📋 Pipeline structure:")
        print(pipeline.visualize_pipeline())
        
        print("⚡ Executing pipeline...")
        start_time = time.time()
        
        try:
            results = pipeline.execute(db, config)
            execution_time = time.time() - start_time
            
            print(f"✅ Pipeline completed successfully in {execution_time:.2f}s")
            print(f"📈 Results shape: {results.shape}")
            print(f"🧮 Columns: {list(results.columns)}")
            
            # Display first few rows
            print("\n📊 Sample results:")
            print(results.head())
            
            # Show execution summary
            summary = pipeline.get_execution_summary()
            if summary:
                print(f"\n⚡ Execution summary:")
                print(f"  - Total execution time: {summary['total_time']:.2f}s")
                print(f"  - Steps completed: {summary['steps_completed']}")
                print(f"  - Steps failed: {summary['steps_failed']}")
                print(f"  - Total warnings: {summary['total_warnings']}")
                
                if summary['step_timing']:
                    print(f"  - Step timing:")
                    for step_id, timing in summary['step_timing'].items():
                        print(f"    * {step_id}: {timing:.2f}s")
            
        except Exception as e:
            execution_time = time.time() - start_time
            print(f"❌ Pipeline failed after {execution_time:.2f}s: {e}")
            
            # Show any partial results or debug info
            summary = pipeline.get_execution_summary()
            if summary:
                print(f"\n🔍 Debug info:")
                print(f"  - Steps completed: {summary['steps_completed']}")
                print(f"  - Steps failed: {summary['steps_failed']}")
                if summary['total_errors'] > 0:
                    print(f"  - Errors: {summary['total_errors']}")


class CustomValidationStep(BaseEstimationStep[RawTablesContract, RawTablesContract]):
    """
    Example of a custom pipeline step that adds validation.
    
    This demonstrates how to create custom steps that integrate with
    the pipeline framework while adding domain-specific logic.
    """
    
    def __init__(self, min_plots: int = 100, **kwargs):
        super().__init__(**kwargs)
        self.min_plots = min_plots
    
    def get_input_contract(self):
        return RawTablesContract
    
    def get_output_contract(self):
        return RawTablesContract
    
    def execute_step(self, input_data: RawTablesContract, context: ExecutionContext):
        """Validate that we have sufficient data for reliable estimates."""
        
        # Check if PLOT table is available
        if "PLOT" not in input_data.tables:
            raise ValueError("PLOT table required for validation")
        
        plot_data = input_data.tables["PLOT"]
        
        # Count unique plots using lazy evaluation
        plot_count = plot_data.frame.select("PLT_CN").n_unique()
        
        # Collect count to check
        if hasattr(plot_count, 'collect'):
            actual_count = plot_count.collect().item()
        else:
            actual_count = plot_count
        
        if actual_count < self.min_plots:
            context.warnings.append(
                f"Warning: Only {actual_count} plots available, "
                f"minimum recommended is {self.min_plots}"
            )
        
        # Add validation metadata
        input_data.add_processing_metadata("validation_plot_count", actual_count)
        input_data.add_processing_metadata("validation_passed", actual_count >= self.min_plots)
        
        return input_data


def create_pipeline_with_custom_step() -> EstimationPipeline:
    """
    Example of creating a pipeline with custom validation step.
    
    Returns
    -------
    EstimationPipeline
        Pipeline with custom validation
    """
    pipeline = EstimationPipeline(
        pipeline_id="volume_with_validation",
        description="Volume estimation with custom validation step"
    )
    
    # Add standard loading step
    pipeline.add_step(LoadTablesStep(
        step_id="load_tables",
        tables=["TREE", "COND", "PLOT"]
    ))
    
    # Add custom validation step
    pipeline.add_step(CustomValidationStep(
        step_id="validate_data",
        description="Validate sufficient plot data",
        min_plots=50
    ))
    
    # Continue with standard steps...
    pipeline.add_step(FilterDataStep(
        step_id="apply_filters",
        tree_domain="STATUSCD == 1"
    ))
    
    return pipeline


def demonstrate_caching_and_performance():
    """
    Demonstrate caching and performance monitoring features.
    """
    print("\n🚀 Performance and Caching Demonstration")
    print("=" * 50)
    
    # Create pipeline with caching enabled
    pipeline = EstimationPipeline(
        pipeline_id="cached_pipeline",
        enable_caching=True,
        debug=True  # Enable performance tracking
    )
    
    # Add steps that benefit from caching
    pipeline.add_step(LoadTablesStep(
        step_id="cached_load",
        description="Load tables with caching",
        tables=["TREE", "COND"]
    ))
    
    print("Pipeline created with caching enabled")
    print("- Step results will be cached for reuse")
    print("- Performance metrics will be tracked")
    print("- Debug information will be available")


def demonstrate_error_handling():
    """
    Demonstrate error handling and recovery features.
    """
    print("\n🛡️ Error Handling Demonstration")  
    print("=" * 40)
    
    # Create pipeline with different error handling strategies
    pipeline = EstimationPipeline(
        pipeline_id="error_handling_demo",
        fail_fast=False,  # Continue execution even if steps fail
        debug=True
    )
    
    # Add a step that might fail
    pipeline.add_step(LoadTablesStep(
        step_id="might_fail",
        tables=["NONEXISTENT_TABLE"],  # This will fail
        skip_on_error=True  # Skip subsequent steps if this fails
    ))
    
    # Add a step that can handle failures
    pipeline.add_step(LoadTablesStep(
        step_id="recovery_step", 
        tables=["TREE", "COND"],
        description="Fallback data loading"
    ))
    
    print("Pipeline created with error handling:")
    print("- fail_fast=False: Continue execution after errors")
    print("- skip_on_error=True: Skip dependent steps after failures")
    print("- Debug mode enabled for detailed error information")


if __name__ == "__main__":
    # Example usage - these would require an actual FIA database
    print("pyFIA Phase 4 Pipeline Framework Examples")
    print("=========================================")
    
    # Demonstrate pipeline creation
    pipeline = create_volume_estimation_pipeline(
        tree_domain="STATUSCD == 1",
        by_species=True
    )
    print(f"\n📋 Created pipeline: {pipeline}")
    print(f"📊 Steps: {len(pipeline.steps)}")
    
    # Show pipeline structure
    print("\n🏗️ Pipeline structure:")
    print(pipeline.visualize_pipeline())
    
    # Demonstrate custom steps
    custom_pipeline = create_pipeline_with_custom_step()
    print(f"\n🔧 Custom pipeline: {custom_pipeline}")
    
    # Show other demonstrations
    demonstrate_caching_and_performance()
    demonstrate_error_handling()
    
    print("\n✅ All examples created successfully!")
    print("To run with real data, call run_volume_estimation_example(db_path)")