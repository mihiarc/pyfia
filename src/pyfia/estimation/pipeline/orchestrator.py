"""
Advanced Pipeline Orchestration Engine for pyFIA Phase 4.

This module provides sophisticated execution control, dependency resolution,
parallel execution, and advanced pipeline management capabilities.
"""

import asyncio
import concurrent.futures
import inspect
import threading
import time
import uuid
from collections import defaultdict, deque
from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import Enum
from functools import lru_cache, wraps
from typing import (
    Any, Callable, Dict, List, Optional, Set, Tuple, Type, Union
)

import polars as pl
from pydantic import BaseModel, Field
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeElapsedColumn
from rich.table import Table

from .core import (
    PipelineStep, ExecutionContext, StepResult, StepStatus,
    ExecutionMode, PipelineException, EstimatorConfig
)
from .contracts import DataContract
from ...core import FIA


# === Enums ===

class DependencyType(str, Enum):
    """Type of dependency between steps."""
    DATA = "data"  # Output of one step is input to another
    CONDITIONAL = "conditional"  # Step depends on condition from another
    RESOURCE = "resource"  # Steps share a resource (e.g., cache)
    ORDERING = "ordering"  # Explicit ordering constraint


class ExecutionStrategy(str, Enum):
    """Pipeline execution strategy."""
    EAGER = "eager"  # Execute immediately
    LAZY = "lazy"  # Defer execution until needed
    ADAPTIVE = "adaptive"  # Adapt based on resource availability
    BATCH = "batch"  # Batch multiple operations


class RetryStrategy(str, Enum):
    """Retry strategy for failed steps."""
    NONE = "none"
    FIXED = "fixed"
    EXPONENTIAL = "exponential"
    LINEAR = "linear"


# === Data Classes ===

@dataclass
class StepDependency:
    """Represents a dependency between pipeline steps."""
    
    source_step_id: str
    target_step_id: str
    dependency_type: DependencyType
    is_optional: bool = False
    condition: Optional[Callable[[StepResult], bool]] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def is_satisfied(self, results: Dict[str, StepResult]) -> bool:
        """Check if dependency is satisfied."""
        if self.source_step_id not in results:
            return self.is_optional
            
        result = results[self.source_step_id]
        
        # Check basic completion
        if not result.success:
            return self.is_optional
            
        # Check condition if provided
        if self.condition:
            return self.condition(result)
            
        return True


@dataclass
class ExecutionPlan:
    """Execution plan for pipeline steps."""
    
    # Step execution order
    execution_order: List[List[str]]  # Groups of steps that can run in parallel
    dependencies: Dict[str, Set[StepDependency]]
    
    # Resource allocation
    resource_allocation: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    
    # Optimization hints
    can_parallelize: Dict[str, bool] = field(default_factory=dict)
    estimated_runtime: Dict[str, float] = field(default_factory=dict)
    memory_requirements: Dict[str, int] = field(default_factory=dict)
    
    # Execution metadata
    created_at: float = field(default_factory=time.time)
    plan_version: str = "1.0"
    
    def get_ready_steps(self, completed: Set[str]) -> Set[str]:
        """Get steps that are ready to execute."""
        ready = set()
        
        for group in self.execution_order:
            for step_id in group:
                if step_id in completed:
                    continue
                    
                # Check if all dependencies are satisfied
                step_deps = self.dependencies.get(step_id, set())
                if all(dep.source_step_id in completed for dep in step_deps):
                    ready.add(step_id)
                    
            # Only consider steps from current group
            if ready:
                break
                
        return ready


@dataclass
class RetryConfig:
    """Configuration for retry logic."""
    
    strategy: RetryStrategy = RetryStrategy.EXPONENTIAL
    max_retries: int = 3
    base_delay: float = 1.0  # seconds
    max_delay: float = 60.0  # seconds
    jitter: bool = True
    retry_on: Optional[Set[Type[Exception]]] = None
    
    def calculate_delay(self, attempt: int) -> float:
        """Calculate delay for retry attempt."""
        import random
        
        if self.strategy == RetryStrategy.NONE:
            return 0
        elif self.strategy == RetryStrategy.FIXED:
            delay = self.base_delay
        elif self.strategy == RetryStrategy.EXPONENTIAL:
            delay = min(self.base_delay * (2 ** attempt), self.max_delay)
        elif self.strategy == RetryStrategy.LINEAR:
            delay = min(self.base_delay * (attempt + 1), self.max_delay)
        else:
            delay = self.base_delay
            
        # Add jitter to avoid thundering herd
        if self.jitter:
            delay = delay * (0.5 + random.random())
            
        return delay


# === Dependency Resolver ===

class DependencyResolver:
    """Resolves and validates step dependencies."""
    
    def __init__(self):
        self.dependencies: Dict[str, Set[StepDependency]] = defaultdict(set)
        self.reverse_dependencies: Dict[str, Set[str]] = defaultdict(set)
        
    def add_dependency(self, dependency: StepDependency) -> None:
        """Add a dependency between steps."""
        self.dependencies[dependency.target_step_id].add(dependency)
        self.reverse_dependencies[dependency.source_step_id].add(dependency.target_step_id)
        
    def infer_dependencies(self, steps: List[PipelineStep]) -> None:
        """Infer dependencies from step contracts."""
        for i in range(len(steps) - 1):
            current_step = steps[i]
            next_step = steps[i + 1]
            
            # Check if output contract matches input contract
            current_output = current_step.get_output_contract()
            next_input = next_step.get_input_contract()
            
            # Create data dependency if contracts align
            if self._contracts_compatible(current_output, next_input):
                dep = StepDependency(
                    source_step_id=current_step.step_id,
                    target_step_id=next_step.step_id,
                    dependency_type=DependencyType.DATA,
                    metadata={"inferred": True}
                )
                self.add_dependency(dep)
    
    def _contracts_compatible(self, output_contract: Type, input_contract: Type) -> bool:
        """Check if output contract is compatible with input contract."""
        # Direct match
        if output_contract == input_contract:
            return True
            
        # Check inheritance
        if isinstance(output_contract, type) and isinstance(input_contract, type):
            if issubclass(output_contract, input_contract):
                return True
                
        return False
    
    def topological_sort(self, step_ids: Set[str]) -> List[List[str]]:
        """
        Perform topological sort to determine execution order.
        
        Returns groups of steps that can be executed in parallel.
        """
        # Build adjacency list
        graph = defaultdict(set)
        in_degree = defaultdict(int)
        
        for step_id in step_ids:
            in_degree[step_id] = 0
            
        for target_id, deps in self.dependencies.items():
            if target_id not in step_ids:
                continue
            for dep in deps:
                if dep.source_step_id in step_ids:
                    graph[dep.source_step_id].add(target_id)
                    in_degree[target_id] += 1
        
        # Kahn's algorithm with level tracking
        queue = deque([sid for sid in step_ids if in_degree[sid] == 0])
        result = []
        
        while queue:
            # Process all nodes at current level (can run in parallel)
            current_level = list(queue)
            result.append(current_level)
            queue.clear()
            
            for node in current_level:
                for neighbor in graph[node]:
                    in_degree[neighbor] -= 1
                    if in_degree[neighbor] == 0:
                        queue.append(neighbor)
        
        # Check for cycles
        if sum(len(level) for level in result) != len(step_ids):
            raise PipelineException("Circular dependency detected in pipeline")
            
        return result
    
    def validate_dependencies(self, steps: Dict[str, PipelineStep]) -> List[str]:
        """Validate all dependencies are satisfiable."""
        issues = []
        
        for target_id, deps in self.dependencies.items():
            if target_id not in steps:
                issues.append(f"Unknown target step: {target_id}")
                continue
                
            for dep in deps:
                if dep.source_step_id not in steps:
                    if not dep.is_optional:
                        issues.append(f"Missing required dependency: {dep.source_step_id} -> {target_id}")
                        
        return issues
    
    def get_execution_plan(self, steps: Dict[str, PipelineStep]) -> ExecutionPlan:
        """Generate optimized execution plan."""
        step_ids = set(steps.keys())
        execution_order = self.topological_sort(step_ids)
        
        # Determine which steps can be parallelized
        can_parallelize = {}
        for group in execution_order:
            for step_id in group:
                # Steps in same group can run in parallel
                can_parallelize[step_id] = len(group) > 1
        
        return ExecutionPlan(
            execution_order=execution_order,
            dependencies=dict(self.dependencies),
            can_parallelize=can_parallelize
        )


# === Parallel Executor ===

class ParallelExecutor:
    """Executes pipeline steps in parallel."""
    
    def __init__(
        self,
        max_workers: int = 4,
        use_processes: bool = False,
        timeout: Optional[float] = None
    ):
        """
        Initialize parallel executor.
        
        Parameters
        ----------
        max_workers : int
            Maximum number of parallel workers
        use_processes : bool
            Use processes instead of threads
        timeout : Optional[float]
            Global timeout for execution
        """
        self.max_workers = max_workers
        self.use_processes = use_processes
        self.timeout = timeout
        self.console = Console()
        
        # Execution state
        self._executor: Optional[concurrent.futures.Executor] = None
        self._futures: Dict[str, concurrent.futures.Future] = {}
        self._lock = threading.Lock()
        
    @contextmanager
    def _get_executor(self):
        """Get executor context."""
        if self.use_processes:
            executor = concurrent.futures.ProcessPoolExecutor(max_workers=self.max_workers)
        else:
            executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers)
            
        try:
            yield executor
        finally:
            executor.shutdown(wait=True)
    
    def execute_group(
        self,
        steps: Dict[str, PipelineStep],
        step_ids: List[str],
        context: ExecutionContext,
        current_outputs: Dict[str, Any]
    ) -> Dict[str, StepResult]:
        """
        Execute a group of steps in parallel.
        
        Parameters
        ----------
        steps : Dict[str, PipelineStep]
            All pipeline steps
        step_ids : List[str]
            IDs of steps to execute in parallel
        context : ExecutionContext
            Execution context
        current_outputs : Dict[str, Any]
            Current outputs from previous steps
            
        Returns
        -------
        Dict[str, StepResult]
            Results from executed steps
        """
        results = {}
        
        if len(step_ids) == 1:
            # Single step - execute directly
            step_id = step_ids[0]
            step = steps[step_id]
            input_data = current_outputs.get(step_id, current_outputs.get("__last__"))
            result = step.execute(input_data, context)
            results[step_id] = result
            
        else:
            # Multiple steps - execute in parallel
            with self._get_executor() as executor:
                futures = {}
                
                for step_id in step_ids:
                    step = steps[step_id]
                    input_data = current_outputs.get(step_id, current_outputs.get("__last__"))
                    
                    # Submit task
                    future = executor.submit(step.execute, input_data, context)
                    futures[step_id] = future
                
                # Wait for completion
                done, pending = concurrent.futures.wait(
                    futures.values(),
                    timeout=self.timeout,
                    return_when=concurrent.futures.ALL_COMPLETED
                )
                
                # Collect results
                for step_id, future in futures.items():
                    try:
                        if future in done:
                            results[step_id] = future.result()
                        else:
                            # Timeout occurred
                            results[step_id] = StepResult(
                                output=None,
                                status=StepStatus.FAILED,
                                error=TimeoutError(f"Step {step_id} timed out")
                            )
                    except Exception as e:
                        results[step_id] = StepResult(
                            output=None,
                            status=StepStatus.FAILED,
                            error=e
                        )
        
        return results


# === Advanced Orchestration Engine ===

class AdvancedOrchestrator:
    """
    Advanced pipeline orchestration engine with sophisticated execution control.
    
    Provides dependency resolution, parallel execution, retry logic, checkpointing,
    and comprehensive monitoring capabilities.
    """
    
    def __init__(
        self,
        pipeline_id: Optional[str] = None,
        execution_strategy: ExecutionStrategy = ExecutionStrategy.ADAPTIVE,
        max_parallel_steps: int = 4,
        enable_checkpointing: bool = True,
        enable_monitoring: bool = True,
        retry_config: Optional[RetryConfig] = None
    ):
        """
        Initialize advanced orchestrator.
        
        Parameters
        ----------
        pipeline_id : Optional[str]
            Unique pipeline identifier
        execution_strategy : ExecutionStrategy
            Strategy for executing steps
        max_parallel_steps : int
            Maximum steps to run in parallel
        enable_checkpointing : bool
            Whether to enable checkpointing
        enable_monitoring : bool
            Whether to enable monitoring
        retry_config : Optional[RetryConfig]
            Retry configuration
        """
        self.pipeline_id = pipeline_id or f"pipeline_{uuid.uuid4().hex[:8]}"
        self.execution_strategy = execution_strategy
        self.max_parallel_steps = max_parallel_steps
        self.enable_checkpointing = enable_checkpointing
        self.enable_monitoring = enable_monitoring
        self.retry_config = retry_config or RetryConfig()
        
        # Components
        self.dependency_resolver = DependencyResolver()
        self.parallel_executor = ParallelExecutor(max_workers=max_parallel_steps)
        self.console = Console()
        
        # State
        self.steps: Dict[str, PipelineStep] = {}
        self.execution_plan: Optional[ExecutionPlan] = None
        self.checkpoints: Dict[str, Any] = {}
        self.execution_history: List[Dict[str, Any]] = []
        
    def add_step(
        self,
        step: PipelineStep,
        depends_on: Optional[List[str]] = None,
        condition: Optional[Callable[[StepResult], bool]] = None
    ) -> "AdvancedOrchestrator":
        """
        Add a step with optional dependencies.
        
        Parameters
        ----------
        step : PipelineStep
            Step to add
        depends_on : Optional[List[str]]
            IDs of steps this depends on
        condition : Optional[Callable[[StepResult], bool]]
            Condition for execution
            
        Returns
        -------
        AdvancedOrchestrator
            Self for chaining
        """
        self.steps[step.step_id] = step
        
        # Add explicit dependencies
        if depends_on:
            for dep_id in depends_on:
                dep = StepDependency(
                    source_step_id=dep_id,
                    target_step_id=step.step_id,
                    dependency_type=DependencyType.ORDERING,
                    condition=condition
                )
                self.dependency_resolver.add_dependency(dep)
        
        return self
    
    def add_conditional_step(
        self,
        step: PipelineStep,
        condition: Callable[[ExecutionContext], bool],
        depends_on: Optional[List[str]] = None
    ) -> "AdvancedOrchestrator":
        """
        Add a step that only executes if condition is met.
        
        Parameters
        ----------
        step : PipelineStep
            Step to add
        condition : Callable[[ExecutionContext], bool]
            Condition to check
        depends_on : Optional[List[str]]
            Dependencies
            
        Returns
        -------
        AdvancedOrchestrator
            Self for chaining
        """
        # Wrap the step with condition checking
        original_should_skip = step.should_skip
        
        def enhanced_should_skip(context: ExecutionContext) -> bool:
            if not condition(context):
                return True
            return original_should_skip(context)
        
        step.should_skip = enhanced_should_skip
        
        return self.add_step(step, depends_on=depends_on)
    
    def _retry_step(
        self,
        step: PipelineStep,
        input_data: Any,
        context: ExecutionContext
    ) -> StepResult:
        """Execute a step with retry logic."""
        last_error = None
        
        for attempt in range(self.retry_config.max_retries + 1):
            try:
                result = step.execute(input_data, context)
                
                if result.success:
                    if attempt > 0:
                        result.add_warning(f"Step succeeded after {attempt} retries")
                    return result
                    
                # Check if we should retry this error
                if self.retry_config.retry_on:
                    if not any(isinstance(result.error, exc_type) 
                              for exc_type in self.retry_config.retry_on):
                        return result
                
                last_error = result.error
                
            except Exception as e:
                last_error = e
            
            # Calculate retry delay
            if attempt < self.retry_config.max_retries:
                delay = self.retry_config.calculate_delay(attempt)
                self.console.print(
                    f"[yellow]Retrying step {step.step_id} after {delay:.1f}s "
                    f"(attempt {attempt + 1}/{self.retry_config.max_retries})[/yellow]"
                )
                time.sleep(delay)
        
        # All retries failed
        return StepResult(
            output=None,
            status=StepStatus.FAILED,
            error=last_error or Exception(f"Step failed after {self.retry_config.max_retries} retries")
        )
    
    def _save_checkpoint(self, step_id: str, data: Any) -> None:
        """Save a checkpoint for recovery."""
        if not self.enable_checkpointing:
            return
            
        self.checkpoints[step_id] = {
            "data": data,
            "timestamp": time.time(),
            "step_id": step_id
        }
    
    def _load_checkpoint(self, step_id: str) -> Optional[Any]:
        """Load a checkpoint if available."""
        if not self.enable_checkpointing:
            return None
            
        checkpoint = self.checkpoints.get(step_id)
        if checkpoint:
            return checkpoint["data"]
        return None
    
    def _show_progress(self, execution_plan: ExecutionPlan) -> Progress:
        """Create progress display."""
        progress = Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeElapsedColumn(),
            console=self.console
        )
        
        # Add task for overall progress
        total_steps = sum(len(group) for group in execution_plan.execution_order)
        main_task = progress.add_task("[cyan]Pipeline Execution", total=total_steps)
        
        return progress
    
    def execute(
        self,
        db: FIA,
        config: EstimatorConfig,
        initial_input: Optional[DataContract] = None,
        show_progress: bool = True
    ) -> Tuple[Any, ExecutionContext]:
        """
        Execute the pipeline with advanced orchestration.
        
        Parameters
        ----------
        db : FIA
            FIA database connection
        config : EstimatorConfig
            Estimation configuration
        initial_input : Optional[DataContract]
            Initial input data
        show_progress : bool
            Whether to show progress
            
        Returns
        -------
        Tuple[Any, ExecutionContext]
            Final output and execution context
        """
        # Initialize context
        context = ExecutionContext(db, config, execution_id=self.pipeline_id)
        
        # Build execution plan
        self.dependency_resolver.infer_dependencies(list(self.steps.values()))
        self.execution_plan = self.dependency_resolver.get_execution_plan(self.steps)
        
        # Validate dependencies
        issues = self.dependency_resolver.validate_dependencies(self.steps)
        if issues:
            raise PipelineException(f"Dependency validation failed: {'; '.join(issues)}")
        
        # Initialize tracking
        current_outputs: Dict[str, Any] = {}
        if initial_input:
            current_outputs["__initial__"] = initial_input
            current_outputs["__last__"] = initial_input
        
        completed_steps: Set[str] = set()
        
        # Execute with progress tracking
        progress = None
        if show_progress and self.enable_monitoring:
            progress = self._show_progress(self.execution_plan)
            progress.start()
            main_task = list(progress.tasks)[0].id
        
        try:
            # Execute each group of steps
            for group_idx, step_group in enumerate(self.execution_plan.execution_order):
                
                # Check if we can parallelize this group
                if len(step_group) > 1 and self.execution_strategy != ExecutionStrategy.EAGER:
                    # Execute in parallel
                    group_results = self.parallel_executor.execute_group(
                        self.steps,
                        step_group,
                        context,
                        current_outputs
                    )
                else:
                    # Execute sequentially
                    group_results = {}
                    for step_id in step_group:
                        step = self.steps[step_id]
                        
                        # Check for checkpoint
                        checkpoint_data = self._load_checkpoint(step_id)
                        if checkpoint_data:
                            group_results[step_id] = StepResult(
                                output=checkpoint_data,
                                status=StepStatus.COMPLETED
                            )
                            if progress:
                                progress.advance(main_task)
                            continue
                        
                        # Get input data
                        input_data = current_outputs.get(step_id, current_outputs.get("__last__"))
                        
                        # Execute with retry
                        result = self._retry_step(step, input_data, context)
                        group_results[step_id] = result
                        
                        # Update progress
                        if progress:
                            progress.advance(main_task)
                
                # Process results
                for step_id, result in group_results.items():
                    context.add_step_result(step_id, result)
                    
                    if result.success:
                        current_outputs[step_id] = result.output
                        current_outputs["__last__"] = result.output
                        completed_steps.add(step_id)
                        
                        # Save checkpoint
                        self._save_checkpoint(step_id, result.output)
                    
                    elif not self.steps[step_id].skip_on_error:
                        raise PipelineException(
                            f"Step {step_id} failed: {result.error}",
                            step_id=step_id,
                            cause=result.error
                        )
            
            # Record execution in history
            self.execution_history.append({
                "execution_id": context.execution_id,
                "timestamp": time.time(),
                "duration": context.total_execution_time,
                "steps_completed": len(completed_steps),
                "steps_total": len(self.steps),
                "success": not context.has_errors
            })
            
            return current_outputs.get("__last__"), context
            
        finally:
            if progress:
                progress.stop()
    
    def visualize_execution_plan(self) -> None:
        """Display the execution plan."""
        if not self.execution_plan:
            self.console.print("[yellow]No execution plan available[/yellow]")
            return
        
        table = Table(title=f"Execution Plan: {self.pipeline_id}")
        table.add_column("Group", style="cyan")
        table.add_column("Steps", style="green")
        table.add_column("Parallel", style="yellow")
        table.add_column("Dependencies", style="magenta")
        
        for idx, group in enumerate(self.execution_plan.execution_order):
            parallel = "Yes" if len(group) > 1 else "No"
            
            for step_id in group:
                deps = self.execution_plan.dependencies.get(step_id, set())
                dep_str = ", ".join(d.source_step_id for d in deps) or "None"
                
                table.add_row(
                    str(idx + 1),
                    step_id,
                    parallel,
                    dep_str
                )
        
        self.console.print(table)
    
    def get_execution_summary(self) -> Dict[str, Any]:
        """Get summary of executions."""
        if not self.execution_history:
            return {"message": "No executions yet"}
        
        total_executions = len(self.execution_history)
        successful = sum(1 for e in self.execution_history if e["success"])
        avg_duration = sum(e["duration"] for e in self.execution_history) / total_executions
        
        return {
            "pipeline_id": self.pipeline_id,
            "total_executions": total_executions,
            "successful_executions": successful,
            "success_rate": successful / total_executions,
            "average_duration": avg_duration,
            "last_execution": self.execution_history[-1] if self.execution_history else None,
            "checkpoints_saved": len(self.checkpoints)
        }


# === Conditional Execution Support ===

class ConditionalExecutor:
    """Handles conditional execution of pipeline steps."""
    
    @staticmethod
    def when(condition: Callable[[ExecutionContext], bool]) -> Callable:
        """Decorator for conditional step execution."""
        def decorator(func: Callable) -> Callable:
            @wraps(func)
            def wrapper(self, input_data: Any, context: ExecutionContext) -> Any:
                if condition(context):
                    return func(self, input_data, context)
                else:
                    # Skip execution, pass through input
                    return input_data
            return wrapper
        return decorator
    
    @staticmethod
    def unless(condition: Callable[[ExecutionContext], bool]) -> Callable:
        """Decorator for negative conditional execution."""
        def decorator(func: Callable) -> Callable:
            @wraps(func)
            def wrapper(self, input_data: Any, context: ExecutionContext) -> Any:
                if not condition(context):
                    return func(self, input_data, context)
                else:
                    return input_data
            return wrapper
        return decorator
    
    @staticmethod
    def if_config(config_key: str, expected_value: Any = True) -> Callable:
        """Execute if config value matches."""
        def condition(context: ExecutionContext) -> bool:
            value = getattr(context.config, config_key, None)
            return value == expected_value
        return ConditionalExecutor.when(condition)
    
    @staticmethod
    def if_data_exists(data_key: str) -> Callable:
        """Execute if context data exists."""
        def condition(context: ExecutionContext) -> bool:
            return context.get_context_data(data_key) is not None
        return ConditionalExecutor.when(condition)


# === Step Skipping Support ===

class SkipStrategy:
    """Strategies for skipping steps dynamically."""
    
    @staticmethod
    def skip_on_small_data(threshold: int = 1000) -> Callable[[ExecutionContext], bool]:
        """Skip if data is below threshold."""
        def should_skip(context: ExecutionContext) -> bool:
            last_result = list(context.step_results.values())[-1] if context.step_results else None
            if last_result and hasattr(last_result.output, "data"):
                if isinstance(last_result.output.data, pl.DataFrame):
                    return len(last_result.output.data) < threshold
            return False
        return should_skip
    
    @staticmethod
    def skip_on_cache_hit(cache_key: str) -> Callable[[ExecutionContext], bool]:
        """Skip if cache contains key."""
        def should_skip(context: ExecutionContext) -> bool:
            return context.get_context_data(f"cache_{cache_key}") is not None
        return should_skip
    
    @staticmethod
    def skip_after_time(max_seconds: float) -> Callable[[ExecutionContext], bool]:
        """Skip if pipeline has been running too long."""
        def should_skip(context: ExecutionContext) -> bool:
            return context.total_execution_time > max_seconds
        return should_skip


# === Export public API ===

__all__ = [
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
    "SkipStrategy"
]