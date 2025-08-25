"""
Advanced Pipeline Orchestration Examples for pyFIA Phase 4.

This module demonstrates the sophisticated orchestration capabilities of the
pyFIA pipeline framework, including parallel execution, error recovery,
monitoring, validation, and optimization.
"""

import time
from pathlib import Path
from typing import List, Dict, Any

import polars as pl
from rich.console import Console

from pyfia import FIA
from pyfia.estimation.config import EstimatorConfig
from pyfia.estimation.pipeline import (
    # Core components
    EstimationPipeline,
    ExecutionContext,
    
    # Steps
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
    
    # Advanced orchestration
    AdvancedOrchestrator,
    DependencyType,
    ExecutionStrategy,
    RetryConfig,
    RetryStrategy,
    ConditionalExecutor,
    SkipStrategy,
    
    # Validation
    PipelineValidator,
    ValidationLevel,
    
    # Monitoring
    PipelineMonitor,
    AlertLevel,
    
    # Error handling
    ErrorRecoveryEngine,
    RecoveryStrategy,
    GracefulDegradation,
    
    # Optimization
    PipelineOptimizer,
    OptimizationLevel,
    OptimizationHint,
    FusionStrategy,
    CacheStrategy,
    PipelineABTester,
)


console = Console()


def example_advanced_orchestration():
    """
    Example demonstrating advanced orchestration with dependency resolution,
    parallel execution, and conditional steps.
    """
    console.print("[bold cyan]Advanced Pipeline Orchestration Example[/bold cyan]\n")
    
    # Initialize database and config
    db = FIA("path/to/fia.db")
    config = EstimatorConfig(
        estimator="volume",
        eval_type="VOL",
        tree_domain="STATUSCD == 1 AND DIA >= 5.0",
        by_species=True,
        include_variance=True
    )
    
    # Create advanced orchestrator
    orchestrator = AdvancedOrchestrator(
        pipeline_id="volume_estimation_advanced",
        execution_strategy=ExecutionStrategy.ADAPTIVE,
        max_parallel_steps=4,
        enable_checkpointing=True,
        retry_config=RetryConfig(
            strategy=RetryStrategy.EXPONENTIAL,
            max_retries=3,
            base_delay=1.0
        )
    )
    
    # Add steps with dependencies
    console.print("[yellow]Building pipeline with dependencies...[/yellow]")
    
    # Data loading steps (can run in parallel)
    orchestrator.add_step(LoadTreeDataStep(db, evalid=231720))
    orchestrator.add_step(LoadConditionDataStep(db, evalid=231720))
    orchestrator.add_step(LoadPlotDataStep(db, evalid=231720))
    
    # Filtering step (depends on tree data)
    orchestrator.add_step(
        ApplyTreeDomainStep(tree_domain=config.tree_domain),
        depends_on=["LoadTreeDataStep"]
    )
    
    # Joining step (depends on multiple data sources)
    orchestrator.add_step(
        JoinTreeConditionStep(),
        depends_on=["ApplyTreeDomainStep", "LoadConditionDataStep"]
    )
    
    # Calculation step
    orchestrator.add_step(
        CalculateVolumeStep(volume_type="net"),
        depends_on=["JoinTreeConditionStep"]
    )
    
    # Conditional aggregation (only if by_species is True)
    orchestrator.add_conditional_step(
        AggregateBySpeciesStep(),
        condition=lambda ctx: ctx.config.by_species,
        depends_on=["CalculateVolumeStep"]
    )
    
    # Stratification and variance (can run in parallel after aggregation)
    orchestrator.add_step(
        ApplyStratificationStep(db, evalid=231720),
        depends_on=["AggregateBySpeciesStep", "LoadPlotDataStep"]
    )
    
    orchestrator.add_conditional_step(
        CalculateVarianceStep(),
        condition=lambda ctx: ctx.config.include_variance,
        depends_on=["ApplyStratificationStep"]
    )
    
    # Final steps
    orchestrator.add_step(
        CalculatePopulationTotalsStep(),
        depends_on=["ApplyStratificationStep", "CalculateVarianceStep"]
    )
    
    orchestrator.add_step(
        FormatOutputStep(),
        depends_on=["CalculatePopulationTotalsStep"]
    )
    
    # Visualize execution plan
    console.print("\n[cyan]Execution Plan:[/cyan]")
    orchestrator.visualize_execution_plan()
    
    # Execute with monitoring
    console.print("\n[green]Executing pipeline...[/green]")
    result, context = orchestrator.execute(db, config, show_progress=True)
    
    # Display execution summary
    summary = orchestrator.get_execution_summary()
    console.print(f"\n[bold]Execution Summary:[/bold]")
    console.print(f"  Total Executions: {summary['total_executions']}")
    console.print(f"  Success Rate: {summary['success_rate']:.1%}")
    console.print(f"  Average Duration: {summary['average_duration']:.2f}s")
    console.print(f"  Checkpoints Saved: {summary['checkpoints_saved']}")
    
    return result


def example_validation_pipeline():
    """
    Example demonstrating comprehensive pipeline validation before execution.
    """
    console.print("[bold cyan]Pipeline Validation Example[/bold cyan]\n")
    
    # Initialize components
    db = FIA("path/to/fia.db")
    config = EstimatorConfig(
        estimator="volume",
        eval_type="VOL",
        tree_domain="STATUSCD == 1 AND DIA >= 5.0"
    )
    
    # Build pipeline
    pipeline = EstimationPipeline(
        pipeline_id="volume_validation_example",
        debug=True
    )
    
    # Add steps
    pipeline.add_step(LoadTreeDataStep(db, evalid=231720))
    pipeline.add_step(ApplyTreeDomainStep(tree_domain=config.tree_domain))
    pipeline.add_step(CalculateVolumeStep(volume_type="net"))
    pipeline.add_step(FormatOutputStep())
    
    # Create validator with comprehensive checks
    validator = PipelineValidator(
        validation_level=ValidationLevel.COMPREHENSIVE,
        stop_on_error=False
    )
    
    # Validate pipeline
    console.print("[yellow]Validating pipeline...[/yellow]\n")
    report = validator.validate_pipeline(
        pipeline=pipeline,
        db=db,
        config=config,
        show_report=True
    )
    
    # Check validation results
    if report.is_valid:
        console.print("\n[green]✓ Pipeline validation passed![/green]")
        console.print("\n[cyan]Executing validated pipeline...[/cyan]")
        
        # Execute only if valid
        result = pipeline.execute(db, config)
        return result
    else:
        console.print("\n[red]✗ Pipeline validation failed![/red]")
        console.print(f"Critical issues: {len(report.get_issues_by_level(ValidationResult.FAILED))}")
        return None


def example_monitored_pipeline():
    """
    Example demonstrating comprehensive pipeline monitoring and alerting.
    """
    console.print("[bold cyan]Pipeline Monitoring Example[/bold cyan]\n")
    
    # Initialize components
    db = FIA("path/to/fia.db")
    config = EstimatorConfig(
        estimator="biomass",
        eval_type="VOL"
    )
    
    # Create pipeline monitor
    monitor = PipelineMonitor(
        enable_metrics=True,
        enable_performance=True,
        enable_alerts=True,
        enable_history=True,
        enable_display=True,
        db_path=Path.home() / ".pyfia" / "monitoring.db"
    )
    
    # Build pipeline
    steps = [
        LoadTreeDataStep(db, evalid=231720),
        LoadConditionDataStep(db, evalid=231720),
        JoinTreeConditionStep(),
        CalculateBiomassStep(),
        AggregateToPlotStep(value_columns=["DRYBIO_AG", "DRYBIO_BG"]),
        ApplyStratificationStep(db, evalid=231720),
        CalculatePopulationTotalsStep(),
        FormatOutputStep()
    ]
    
    # Start monitoring
    monitor.start_pipeline(
        pipeline_id="biomass_monitored",
        execution_id=f"exec_{int(time.time())}",
        total_steps=len(steps)
    )
    
    # Execute steps with monitoring
    context = ExecutionContext(db, config)
    current_data = None
    
    for step in steps:
        # Monitor step execution
        with monitor.monitor_step(step.step_id, step.description):
            # Get input row count
            input_rows = 0
            if current_data and hasattr(current_data, "data"):
                if isinstance(current_data.data, pl.DataFrame):
                    input_rows = len(current_data.data)
            
            # Execute step
            result = step.execute(current_data or TableDataContract(tables={}), context)
            
            # Get output row count
            output_rows = 0
            if result.output and hasattr(result.output, "data"):
                if isinstance(result.output.data, pl.DataFrame):
                    output_rows = len(result.output.data)
            
            # Record metrics
            monitor.record_metric("rows_processed", output_rows, step.step_id)
            monitor.record_metric("execution_time", result.execution_time, step.step_id)
            
            # Check for performance issues
            if result.execution_time > 10:
                monitor.record_metric("slow_step", 1, step.step_id)
            
            # Update current data
            if result.success:
                current_data = result.output
    
    # Complete monitoring
    monitor.complete_pipeline(status="completed")
    
    # Get monitoring summary
    summary = monitor.get_summary()
    console.print("\n[bold]Monitoring Summary:[/bold]")
    console.print(f"  Total Metrics: {summary['total_metrics']}")
    console.print(f"  Steps Tracked: {summary['steps_tracked']}")
    console.print(f"  Alerts: {summary.get('alerts', 0)}")
    console.print(f"  Critical Alerts: {summary.get('critical_alerts', 0)}")
    
    # Display execution history
    if monitor.execution_history:
        history = monitor.execution_history.get_execution_summary(limit=5)
        console.print("\n[cyan]Recent Executions:[/cyan]")
        console.print(history)
    
    return current_data


def example_error_recovery_pipeline():
    """
    Example demonstrating error handling and recovery strategies.
    """
    console.print("[bold cyan]Error Recovery Pipeline Example[/bold cyan]\n")
    
    # Initialize components
    db = FIA("path/to/fia.db")
    config = EstimatorConfig(estimator="tpa", eval_type="VOL")
    
    # Create error recovery engine
    recovery_engine = ErrorRecoveryEngine(
        enable_checkpointing=True,
        enable_rollback=True,
        checkpoint_dir=Path.home() / ".pyfia" / "checkpoints"
    )
    
    # Start pipeline tracking
    recovery_engine.start_pipeline(
        pipeline_id="tpa_with_recovery",
        execution_id=f"exec_{int(time.time())}"
    )
    
    # Build pipeline with potential failure points
    steps = [
        LoadTreeDataStep(db, evalid=231720),
        ApplyTreeDomainStep(tree_domain="STATUSCD == 1"),
        CalculateTPAStep(),
        AggregateToPlotStep(value_columns=["TPA"]),
        ApplyStratificationStep(db, evalid=231720),
        CalculatePopulationTotalsStep(),
        FormatOutputStep()
    ]
    
    # Execute with error handling
    context = ExecutionContext(db, config)
    current_data = None
    
    for i, step in enumerate(steps):
        console.print(f"\n[yellow]Executing step {i+1}/{len(steps)}: {step.step_id}[/yellow]")
        
        try:
            # Save checkpoint before risky operations
            if i > 0 and current_data:
                recovery_engine.save_checkpoint(
                    step_id=f"before_{step.step_id}",
                    data=current_data,
                    execution_id=context.execution_id
                )
            
            # Register rollback action
            recovery_engine.register_rollback(
                rollback_func=lambda: console.print(f"Rolling back {step.step_id}"),
                description=f"Rollback {step.step_id}"
            )
            
            # Execute step
            result = step.execute(
                current_data or TableDataContract(tables={}),
                context
            )
            
            if result.success:
                current_data = result.output
                console.print(f"  [green]✓ Step completed successfully[/green]")
            else:
                raise result.error or Exception(f"Step {step.step_id} failed")
                
        except Exception as e:
            console.print(f"  [red]✗ Step failed: {e}[/red]")
            
            # Handle error
            recovery_action, error_context = recovery_engine.handle_error(
                error=e,
                step_id=step.step_id,
                execution_context=context,
                input_data=current_data
            )
            
            console.print(f"  [cyan]Recovery strategy: {recovery_action.strategy.value}[/cyan]")
            
            # Execute recovery
            if recovery_action.strategy == RecoveryStrategy.RETRY:
                console.print("  [yellow]Retrying step...[/yellow]")
                # Retry logic would go here
                
            elif recovery_action.strategy == RecoveryStrategy.CHECKPOINT:
                console.print("  [yellow]Restoring from checkpoint...[/yellow]")
                checkpoint_data = recovery_engine.checkpoint_manager.load_checkpoint(
                    step_id=f"before_{step.step_id}",
                    execution_id=context.execution_id
                )
                if checkpoint_data:
                    current_data = checkpoint_data
                    console.print("  [green]✓ Checkpoint restored[/green]")
                    
            elif recovery_action.strategy == RecoveryStrategy.SKIP:
                console.print("  [yellow]Skipping failed step[/yellow]")
                continue
                
            elif recovery_action.strategy == RecoveryStrategy.ABORT:
                console.print("  [red]Aborting pipeline[/red]")
                
                # Perform rollback
                console.print("\n[yellow]Performing rollback...[/yellow]")
                rollback_results = recovery_engine.perform_rollback()
                for result in rollback_results:
                    console.print(f"  {result}")
                
                break
    
    # Display error report
    recovery_engine.display_error_report()
    
    return current_data


def example_optimized_pipeline():
    """
    Example demonstrating pipeline optimization techniques.
    """
    console.print("[bold cyan]Pipeline Optimization Example[/bold cyan]\n")
    
    # Initialize components
    db = FIA("path/to/fia.db")
    config = EstimatorConfig(
        estimator="area",
        eval_type="VOL",
        by_forest_type=True
    )
    
    # Build unoptimized pipeline
    steps = [
        LoadTreeDataStep(db, evalid=231720),
        LoadConditionDataStep(db, evalid=231720),
        LoadPlotDataStep(db, evalid=231720),
        ApplyTreeDomainStep(tree_domain="STATUSCD == 1"),
        ApplyAreaDomainStep(area_domain="COND_STATUS_CD == 1"),
        JoinTreeConditionStep(),
        JoinWithPlotStep(),
        CalculateAreaStep(),
        AggregateByForestTypeStep(),
        ApplyStratificationStep(db, evalid=231720),
        CalculateVarianceStep(),
        CalculatePopulationTotalsStep(),
        FormatOutputStep()
    ]
    
    console.print(f"[yellow]Original pipeline: {len(steps)} steps[/yellow]")
    
    # Create optimizer
    optimizer = PipelineOptimizer(
        optimization_level=OptimizationLevel.AGGRESSIVE,
        enable_fusion=True,
        enable_pushdown=True,
        enable_caching=True,
        enable_locality=True
    )
    
    # Add optimization hints
    optimizer.add_hint(OptimizationHint(
        hint_type="force_cache",
        target="*Stratification*",
        priority=10
    ))
    
    optimizer.add_hint(OptimizationHint(
        hint_type="no_fusion",
        target="*Variance*",
        priority=5
    ))
    
    # Optimize pipeline
    console.print("\n[cyan]Optimizing pipeline...[/cyan]")
    optimized_steps, optimization_result = optimizer.optimize_pipeline(
        steps=steps,
        config=config,
        show_report=True
    )
    
    # Compare performance with A/B testing
    console.print("\n[cyan]Running A/B test...[/cyan]")
    
    ab_tester = PipelineABTester()
    
    # Create test variants
    variants = {
        "original": steps,
        "optimized": optimized_steps,
        "cached_only": optimizer.cache_optimizer.optimize(steps, config) if optimizer.cache_optimizer else steps
    }
    
    # Run A/B test
    context = ExecutionContext(db, config)
    test_data = TableDataContract(tables={})
    
    test_results = ab_tester.test_variants(
        variants=variants,
        db=db,
        config=config,
        context=context,
        test_data=test_data,
        iterations=3
    )
    
    # Display cache statistics
    if optimizer.cache_optimizer:
        cache_stats = optimizer.get_cache_stats()
        console.print("\n[bold]Cache Statistics:[/bold]")
        console.print(f"  Cache Entries: {cache_stats['cache_entries']}")
        console.print(f"  Hit Rate: {cache_stats['hit_rate']:.1%}")
        console.print(f"  Most Accessed: {cache_stats['most_accessed']}")
    
    return optimized_steps


def example_graceful_degradation():
    """
    Example demonstrating graceful degradation capabilities.
    """
    console.print("[bold cyan]Graceful Degradation Example[/bold cyan]\n")
    
    # Define steps with graceful degradation
    @GracefulDegradation.with_default(pl.DataFrame())
    def risky_calculation(data: pl.DataFrame) -> pl.DataFrame:
        """Calculation that might fail."""
        # This could raise an exception
        return data.select(pl.col("NONEXISTENT_COLUMN") * 2)
    
    @GracefulDegradation.with_reduced_functionality(
        reduced_func=lambda data: data.select(pl.col("DIA").alias("SIMPLE_CALC"))
    )
    def complex_calculation(data: pl.DataFrame) -> pl.DataFrame:
        """Complex calculation with simpler fallback."""
        # Complex calculation that might fail
        return data.select(
            (pl.col("DIA") ** 2 * pl.col("HT") * 0.005454).alias("COMPLEX_CALC")
        )
    
    # Test graceful degradation
    test_data = pl.DataFrame({
        "DIA": [10.0, 15.0, 20.0],
        "HT": [60.0, 70.0, 80.0]
    })
    
    console.print("[yellow]Testing risky calculation with default fallback...[/yellow]")
    result1 = risky_calculation(test_data)
    console.print(f"  Result: {result1.shape if not result1.is_empty() else 'Empty DataFrame (fallback)'}")
    
    console.print("\n[yellow]Testing complex calculation with reduced functionality...[/yellow]")
    result2 = complex_calculation(test_data)
    console.print(f"  Result columns: {result2.columns}")
    
    return result2


def main():
    """Run all advanced pipeline examples."""
    console.print("[bold magenta]pyFIA Phase 4: Advanced Pipeline Orchestration Examples[/bold magenta]\n")
    console.print("=" * 80)
    
    examples = [
        ("Advanced Orchestration", example_advanced_orchestration),
        ("Pipeline Validation", example_validation_pipeline),
        ("Pipeline Monitoring", example_monitored_pipeline),
        ("Error Recovery", example_error_recovery_pipeline),
        ("Pipeline Optimization", example_optimized_pipeline),
        ("Graceful Degradation", example_graceful_degradation)
    ]
    
    for name, example_func in examples:
        console.print(f"\n[bold]Running: {name}[/bold]")
        console.print("-" * 40)
        
        try:
            result = example_func()
            if result:
                console.print(f"\n[green]✓ {name} completed successfully[/green]")
        except Exception as e:
            console.print(f"\n[red]✗ {name} failed: {e}[/red]")
        
        console.print("\n" + "=" * 80)
    
    console.print("\n[bold green]All examples completed![/bold green]")


if __name__ == "__main__":
    main()