"""
Extension points and middleware for the pipeline framework.

This module provides extensibility mechanisms for customizing pipeline
behavior including custom steps, conditional execution, middleware,
and plugin systems for adding new estimation types.
"""

import time
from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, List, Optional, Type, Union
from concurrent.futures import ThreadPoolExecutor, as_completed
import warnings

import polars as pl

from ...core import FIA
from ..config import EstimatorConfig
from ..lazy_evaluation import LazyFrameWrapper
from ..caching import MemoryCache

from .core import (
    PipelineStep,
    ExecutionContext,
    DataContract,
    StepResult,
    StepStatus,
    PipelineException,
    StepValidationError,
    TInput,
    TOutput
)


# === Custom Step Base Classes ===

class CustomStep(PipelineStep[TInput, TOutput]):
    """
    Base class for user-defined custom pipeline steps.
    
    Provides a simplified interface for creating custom steps
    with built-in validation and error handling.
    """
    
    def __init__(
        self,
        step_function: Callable[[TInput, ExecutionContext], TOutput],
        input_contract: Type[TInput],
        output_contract: Type[TOutput],
        validation_function: Optional[Callable[[TInput, ExecutionContext], None]] = None,
        **kwargs
    ):
        """
        Initialize custom step.
        
        Parameters
        ----------
        step_function : Callable
            Function that implements the step logic
        input_contract : Type[TInput]
            Input data contract type
        output_contract : Type[TOutput]
            Output data contract type
        validation_function : Optional[Callable]
            Optional custom validation function
        """
        super().__init__(**kwargs)
        self._step_function = step_function
        self._input_contract = input_contract
        self._output_contract = output_contract
        self._validation_function = validation_function
    
    def get_input_contract(self) -> Type[TInput]:
        """Get input contract."""
        return self._input_contract
    
    def get_output_contract(self) -> Type[TOutput]:
        """Get output contract."""
        return self._output_contract
    
    def validate_input(self, input_data: TInput, context: ExecutionContext) -> None:
        """Validate input using custom validation if provided."""
        super().validate_input(input_data, context)
        
        if self._validation_function:
            try:
                self._validation_function(input_data, context)
            except Exception as e:
                raise StepValidationError(
                    f"Custom validation failed: {e}",
                    step_id=self.step_id
                ) from e
    
    def execute_step(self, input_data: TInput, context: ExecutionContext) -> TOutput:
        """Execute the custom step function."""
        try:
            return self._step_function(input_data, context)
        except Exception as e:
            raise PipelineException(
                f"Custom step execution failed: {e}",
                step_id=self.step_id,
                cause=e
            ) from e


class ParameterizedStep(PipelineStep[TInput, TOutput]):
    """
    Base class for steps with configurable parameters.
    
    Allows steps to be parameterized at runtime based on
    configuration or execution context.
    """
    
    def __init__(
        self,
        parameter_resolver: Callable[[ExecutionContext], Dict[str, Any]],
        **kwargs
    ):
        """
        Initialize parameterized step.
        
        Parameters
        ----------
        parameter_resolver : Callable
            Function that resolves parameters from execution context
        """
        super().__init__(**kwargs)
        self._parameter_resolver = parameter_resolver
        self._resolved_parameters: Optional[Dict[str, Any]] = None
    
    def resolve_parameters(self, context: ExecutionContext) -> Dict[str, Any]:
        """
        Resolve parameters for this execution.
        
        Parameters
        ----------
        context : ExecutionContext
            Execution context
            
        Returns
        -------
        Dict[str, Any]
            Resolved parameters
        """
        if self._resolved_parameters is None:
            self._resolved_parameters = self._parameter_resolver(context)
        return self._resolved_parameters
    
    @abstractmethod
    def execute_with_parameters(
        self, 
        input_data: TInput, 
        context: ExecutionContext, 
        parameters: Dict[str, Any]
    ) -> TOutput:
        """
        Execute step with resolved parameters.
        
        Parameters
        ----------
        input_data : TInput
            Input data
        context : ExecutionContext
            Execution context
        parameters : Dict[str, Any]
            Resolved parameters
            
        Returns
        -------
        TOutput
            Step output
        """
        pass
    
    def execute_step(self, input_data: TInput, context: ExecutionContext) -> TOutput:
        """Execute step with parameter resolution."""
        parameters = self.resolve_parameters(context)
        return self.execute_with_parameters(input_data, context, parameters)


class ConditionalStep(PipelineStep[TInput, TOutput]):
    """
    Step that conditionally executes based on configuration.
    
    Allows pipeline steps to be skipped or alternate implementations
    to be used based on runtime conditions.
    """
    
    def __init__(
        self,
        condition: Callable[[ExecutionContext], bool],
        step: PipelineStep[TInput, TOutput],
        fallback_step: Optional[PipelineStep[TInput, TOutput]] = None,
        **kwargs
    ):
        """
        Initialize conditional step.
        
        Parameters
        ----------
        condition : Callable
            Function that determines if step should execute
        step : PipelineStep
            Step to execute if condition is True
        fallback_step : Optional[PipelineStep]
            Step to execute if condition is False
        """
        super().__init__(**kwargs)
        self._condition = condition
        self._primary_step = step
        self._fallback_step = fallback_step
    
    def get_input_contract(self) -> Type[TInput]:
        """Get input contract from primary step."""
        return self._primary_step.get_input_contract()
    
    def get_output_contract(self) -> Type[TOutput]:
        """Get output contract from primary step."""
        return self._primary_step.get_output_contract()
    
    def should_skip(self, context: ExecutionContext) -> bool:
        """Check if step should be skipped."""
        if super().should_skip(context):
            return True
        
        # If condition is False and no fallback, skip
        return not self._condition(context) and self._fallback_step is None
    
    def execute_step(self, input_data: TInput, context: ExecutionContext) -> TOutput:
        """Execute appropriate step based on condition."""
        if self._condition(context):
            return self._primary_step.execute_step(input_data, context)
        elif self._fallback_step:
            return self._fallback_step.execute_step(input_data, context)
        else:
            # Pass through input unchanged
            return input_data


class ParallelStep(PipelineStep[TInput, TOutput]):
    """
    Step that executes multiple sub-steps in parallel.
    
    Useful for independent operations that can be parallelized
    for better performance.
    """
    
    def __init__(
        self,
        steps: List[PipelineStep],
        combine_function: Callable[[List[Any], ExecutionContext], TOutput],
        max_workers: Optional[int] = None,
        **kwargs
    ):
        """
        Initialize parallel step.
        
        Parameters
        ----------
        steps : List[PipelineStep]
            Steps to execute in parallel
        combine_function : Callable
            Function to combine results from parallel steps
        max_workers : Optional[int]
            Maximum number of worker threads
        """
        super().__init__(**kwargs)
        self._steps = steps
        self._combine_function = combine_function
        self._max_workers = max_workers or min(4, len(steps))
    
    def get_input_contract(self) -> Type[TInput]:
        """Get input contract from first step."""
        if self._steps:
            return self._steps[0].get_input_contract()
        return DataContract
    
    def get_output_contract(self) -> Type[TOutput]:
        """Get output contract (must be specified by combine_function)."""
        return DataContract
    
    def execute_step(self, input_data: TInput, context: ExecutionContext) -> TOutput:
        """Execute steps in parallel."""
        if not self._steps:
            return input_data
        
        results = []
        
        with ThreadPoolExecutor(max_workers=self._max_workers) as executor:
            # Submit all steps for execution
            future_to_step = {
                executor.submit(step.execute_step, input_data, context): step
                for step in self._steps
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_step):
                step = future_to_step[future]
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    raise PipelineException(
                        f"Parallel step {step.step_id} failed: {e}",
                        step_id=self.step_id,
                        cause=e
                    )
        
        # Combine results
        return self._combine_function(results, context)


# === Middleware System ===

class PipelineMiddleware(ABC):
    """
    Abstract base class for pipeline middleware.
    
    Middleware can intercept and modify pipeline execution
    for cross-cutting concerns like caching, logging, etc.
    """
    
    @abstractmethod
    def before_step(
        self, 
        step: PipelineStep, 
        input_data: DataContract, 
        context: ExecutionContext
    ) -> DataContract:
        """
        Process before step execution.
        
        Parameters
        ----------
        step : PipelineStep
            Step about to execute
        input_data : DataContract
            Input data for the step
        context : ExecutionContext
            Execution context
            
        Returns
        -------
        DataContract
            Potentially modified input data
        """
        pass
    
    @abstractmethod
    def after_step(
        self, 
        step: PipelineStep, 
        result: StepResult, 
        context: ExecutionContext
    ) -> StepResult:
        """
        Process after step execution.
        
        Parameters
        ----------
        step : PipelineStep
            Step that executed
        result : StepResult
            Step execution result
        context : ExecutionContext
            Execution context
            
        Returns
        -------
        StepResult
            Potentially modified result
        """
        pass


class CachingMiddleware(PipelineMiddleware):
    """
    Middleware for caching step results.
    
    Provides sophisticated caching with TTL, size limits,
    and cache invalidation strategies.
    """
    
    def __init__(
        self,
        cache: Optional[MemoryCache] = None,
        cache_key_function: Optional[Callable[[PipelineStep, DataContract], str]] = None,
        ttl_seconds: int = 300
    ):
        """
        Initialize caching middleware.
        
        Parameters
        ----------
        cache : Optional[MemoryCache]
            Cache instance to use
        cache_key_function : Optional[Callable]
            Function to generate cache keys
        ttl_seconds : int
            Time-to-live for cache entries
        """
        self.cache = cache or MemoryCache(max_size_mb=256, max_entries=100)
        self.cache_key_function = cache_key_function or self._default_cache_key
        self.ttl_seconds = ttl_seconds
    
    def _default_cache_key(self, step: PipelineStep, input_data: DataContract) -> str:
        """Generate default cache key."""
        return f"{step.step_id}_{hash(str(input_data))}"
    
    def before_step(
        self, 
        step: PipelineStep, 
        input_data: DataContract, 
        context: ExecutionContext
    ) -> DataContract:
        """Check cache before step execution."""
        # Check if result is cached
        cache_key = self.cache_key_function(step, input_data)
        cached_result = self.cache.get(cache_key)
        
        if cached_result is not None:
            # Store cached result in context for retrieval in after_step
            context.set_context_data(f"cache_hit_{step.step_id}", cached_result)
        
        return input_data
    
    def after_step(
        self, 
        step: PipelineStep, 
        result: StepResult, 
        context: ExecutionContext
    ) -> StepResult:
        """Cache result after step execution."""
        cache_hit_key = f"cache_hit_{step.step_id}"
        
        if context.get_context_data(cache_hit_key) is not None:
            # Use cached result
            cached_output = context.get_context_data(cache_hit_key)
            result.output = cached_output
            result.add_debug_info("cache_hit", True)
        elif result.success:
            # Cache the new result
            cache_key = self.cache_key_function(step, result.output)
            self.cache.put(cache_key, result.output, ttl_seconds=self.ttl_seconds)
            result.add_debug_info("cache_stored", True)
        
        return result


class LoggingMiddleware(PipelineMiddleware):
    """
    Middleware for logging step execution.
    
    Provides detailed logging of step execution including
    timing, memory usage, and error information.
    """
    
    def __init__(self, log_level: str = "INFO", include_data_info: bool = True):
        """
        Initialize logging middleware.
        
        Parameters
        ----------
        log_level : str
            Logging level
        include_data_info : bool
            Whether to include data size/type information
        """
        self.log_level = log_level
        self.include_data_info = include_data_info
    
    def before_step(
        self, 
        step: PipelineStep, 
        input_data: DataContract, 
        context: ExecutionContext
    ) -> DataContract:
        """Log before step execution."""
        step_info = f"Starting step: {step.step_id} ({step.__class__.__name__})"
        
        if self.include_data_info:
            # Get data size information
            data_info = self._get_data_info(input_data)
            step_info += f" - Input: {data_info}"
        
        # Use warnings for logging (could integrate with proper logging system)
        if self.log_level in ["DEBUG", "INFO"]:
            warnings.warn(step_info, UserWarning)
        
        return input_data
    
    def after_step(
        self, 
        step: PipelineStep, 
        result: StepResult, 
        context: ExecutionContext
    ) -> StepResult:
        """Log after step execution.""" 
        step_info = f"Completed step: {step.step_id} - Status: {result.status.value}"
        step_info += f" - Time: {result.execution_time:.2f}s"
        
        if result.records_processed:
            step_info += f" - Records: {result.records_processed:,}"
        
        if result.error:
            step_info += f" - Error: {result.error}"
        
        if result.warnings:
            step_info += f" - Warnings: {len(result.warnings)}"
        
        if self.log_level in ["DEBUG", "INFO"]:
            warnings.warn(step_info, UserWarning)
        
        return result
    
    def _get_data_info(self, data: DataContract) -> str:
        """Get information about data size and type."""
        info_parts = []
        
        for field_name, field_value in data.model_dump().items():
            if isinstance(field_value, (pl.DataFrame, pl.LazyFrame, LazyFrameWrapper)):
                if isinstance(field_value, LazyFrameWrapper):
                    frame = field_value.frame
                else:
                    frame = field_value
                
                if isinstance(frame, pl.LazyFrame):
                    info_parts.append(f"{field_name}=LazyFrame")
                else:
                    info_parts.append(f"{field_name}=DataFrame({len(frame)} rows)")
        
        return ", ".join(info_parts) if info_parts else "No data"


class ProfilingMiddleware(PipelineMiddleware):
    """
    Middleware for performance profiling.
    
    Collects detailed performance metrics including
    memory usage, CPU time, and I/O statistics.
    """
    
    def __init__(self, collect_memory_stats: bool = True):
        """
        Initialize profiling middleware.
        
        Parameters
        ----------
        collect_memory_stats : bool
            Whether to collect memory usage statistics
        """
        self.collect_memory_stats = collect_memory_stats
        self.step_profiles: Dict[str, Dict[str, Any]] = {}
    
    def before_step(
        self, 
        step: PipelineStep, 
        input_data: DataContract, 
        context: ExecutionContext
    ) -> DataContract:
        """Start profiling before step execution."""
        profile_data = {
            "start_time": time.time(),
            "start_memory": self._get_memory_usage() if self.collect_memory_stats else None
        }
        
        self.step_profiles[step.step_id] = profile_data
        return input_data
    
    def after_step(
        self, 
        step: PipelineStep, 
        result: StepResult, 
        context: ExecutionContext
    ) -> StepResult:
        """Complete profiling after step execution."""
        if step.step_id in self.step_profiles:
            profile_data = self.step_profiles[step.step_id]
            
            # Update timing
            profile_data["end_time"] = time.time()
            profile_data["duration"] = profile_data["end_time"] - profile_data["start_time"]
            
            # Update memory usage
            if self.collect_memory_stats:
                profile_data["end_memory"] = self._get_memory_usage()
                if profile_data["start_memory"] is not None:
                    profile_data["memory_delta"] = (
                        profile_data["end_memory"] - profile_data["start_memory"]
                    )
            
            # Add profiling info to result
            result.add_debug_info("profile", profile_data)
            
            # Update result metrics
            result.execution_time = profile_data["duration"]
            if self.collect_memory_stats and profile_data.get("memory_delta"):
                result.memory_used = profile_data["memory_delta"]
        
        return result
    
    def _get_memory_usage(self) -> int:
        """Get current memory usage in bytes."""
        try:
            import psutil
            import os
            process = psutil.Process(os.getpid())
            return process.memory_info().rss
        except ImportError:
            return 0
    
    def get_profiling_summary(self) -> Dict[str, Any]:
        """Get summary of profiling data."""
        total_time = sum(
            profile.get("duration", 0) 
            for profile in self.step_profiles.values()
        )
        
        total_memory = sum(
            profile.get("memory_delta", 0) 
            for profile in self.step_profiles.values()
            if profile.get("memory_delta", 0) > 0
        )
        
        return {
            "total_execution_time": total_time,
            "total_memory_used": total_memory,
            "step_count": len(self.step_profiles),
            "average_step_time": total_time / len(self.step_profiles) if self.step_profiles else 0,
            "step_details": self.step_profiles
        }


class ValidationMiddleware(PipelineMiddleware):
    """
    Middleware for additional data validation.
    
    Provides comprehensive validation of data contracts
    and business rule enforcement.
    """
    
    def __init__(
        self,
        validation_rules: Optional[Dict[str, Callable[[DataContract], bool]]] = None,
        strict_mode: bool = False
    ):
        """
        Initialize validation middleware.
        
        Parameters
        ----------
        validation_rules : Optional[Dict[str, Callable]]
            Custom validation rules by step type
        strict_mode : bool
            Whether to fail on validation warnings
        """
        self.validation_rules = validation_rules or {}
        self.strict_mode = strict_mode
    
    def before_step(
        self, 
        step: PipelineStep, 
        input_data: DataContract, 
        context: ExecutionContext
    ) -> DataContract:
        """Validate input before step execution."""
        # Apply step-specific validation rules
        step_type = step.__class__.__name__
        if step_type in self.validation_rules:
            rule = self.validation_rules[step_type]
            try:
                is_valid = rule(input_data)
                if not is_valid:
                    message = f"Validation rule failed for step {step.step_id}"
                    if self.strict_mode:
                        raise StepValidationError(message, step_id=step.step_id)
                    else:
                        warnings.warn(message, UserWarning)
            except Exception as e:
                if self.strict_mode:
                    raise StepValidationError(
                        f"Validation rule error for step {step.step_id}: {e}",
                        step_id=step.step_id
                    ) from e
                else:
                    warnings.warn(
                        f"Validation rule error for step {step.step_id}: {e}",
                        UserWarning
                    )
        
        return input_data
    
    def after_step(
        self, 
        step: PipelineStep, 
        result: StepResult, 
        context: ExecutionContext
    ) -> StepResult:
        """Validate output after step execution."""
        if result.success:
            # Basic output validation
            try:
                result.output.model_validate(result.output.model_dump())
                result.add_debug_info("output_validation", "passed")
            except Exception as e:
                message = f"Output validation failed for step {step.step_id}: {e}"
                if self.strict_mode:
                    result.error = StepValidationError(message, step_id=step.step_id)
                    result.status = StepStatus.FAILED
                else:
                    result.add_warning(message)
        
        return result