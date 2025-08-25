"""
Core abstractions for the pyFIA pipeline framework.

This module provides the foundational classes and interfaces for building
composable, type-safe estimation pipelines in pyFIA.
"""

import time
import uuid
from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import Enum
from typing import (
    Any, 
    Dict, 
    Generic, 
    List, 
    Optional, 
    Type, 
    TypeVar, 
    Union, 
    Callable,
    Iterator,
    Protocol
)
import warnings

import polars as pl
from pydantic import BaseModel, Field, ConfigDict

from ...core import FIA
from ..config import EstimatorConfig
from ..lazy_evaluation import LazyFrameWrapper


# === Type Variables ===

TInput = TypeVar("TInput", bound=BaseModel)
TOutput = TypeVar("TOutput", bound=BaseModel)
TConfig = TypeVar("TConfig", bound=EstimatorConfig)


# === Enums ===

class StepStatus(str, Enum):
    """Status of a pipeline step during execution."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


class ExecutionMode(str, Enum):
    """Execution mode for pipelines."""
    SEQUENTIAL = "sequential"
    PARALLEL = "parallel"
    STREAMING = "streaming"
    ADAPTIVE = "adaptive"


# === Exceptions ===

class PipelineException(Exception):
    """Base exception for pipeline-related errors."""
    
    def __init__(self, message: str, step_id: Optional[str] = None, cause: Optional[Exception] = None):
        super().__init__(message)
        self.step_id = step_id
        self.cause = cause


class StepValidationError(PipelineException):
    """Raised when step validation fails."""
    pass


class DataContractViolation(PipelineException):
    """Raised when data contract validation fails."""
    pass


# === Import Data Contracts ===

# Import all data contracts from the separate contracts module
from .contracts import (
    DataContract,
    RawTablesContract as TableDataContract,  # Alias for backward compatibility
    FilteredDataContract,
    JoinedDataContract,
    ValuedDataContract,
    PlotEstimatesContract,
    StratifiedEstimatesContract,
    PopulationEstimatesContract,
    FormattedOutputContract,
)


# === Step Result ===

@dataclass
class StepResult(Generic[TOutput]):
    """
    Result of executing a pipeline step.
    
    Contains the output data, execution metadata, and any warnings or errors.
    """
    
    # Core result data
    output: TOutput
    status: StepStatus
    
    # Execution metadata
    execution_time: float = 0.0
    memory_used: Optional[int] = None  # bytes
    records_processed: Optional[int] = None
    
    # Error handling
    error: Optional[Exception] = None
    warnings: List[str] = field(default_factory=list)
    
    # Debugging info
    debug_info: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def success(self) -> bool:
        """Whether the step completed successfully."""
        return self.status == StepStatus.COMPLETED and self.error is None
    
    def add_warning(self, message: str) -> None:
        """Add a warning message."""
        self.warnings.append(message)
    
    def add_debug_info(self, key: str, value: Any) -> None:
        """Add debug information."""
        self.debug_info[key] = value


# === Execution Context ===

class ExecutionContext:
    """
    Runtime context for pipeline execution.
    
    Manages execution state, configuration, caching, and error handling
    across pipeline steps.
    """
    
    def __init__(
        self,
        db: FIA,
        config: EstimatorConfig,
        execution_id: Optional[str] = None,
        debug: bool = False
    ):
        """
        Initialize execution context.
        
        Parameters
        ----------
        db : FIA
            FIA database connection
        config : EstimatorConfig
            Estimation configuration
        execution_id : Optional[str]
            Unique execution identifier
        debug : bool
            Whether to enable debug mode
        """
        self.db = db
        self.config = config
        self.execution_id = execution_id or str(uuid.uuid4())
        self.debug = debug
        
        # Execution state
        self.start_time = time.time()
        self.step_results: Dict[str, StepResult] = {}
        self.context_data: Dict[str, Any] = {}
        
        # Error tracking
        self.errors: List[Exception] = []
        self.warnings: List[str] = []
        
        # Performance tracking
        self.memory_usage: List[Tuple[str, int]] = []  # (step_id, memory_bytes)
        self.timing_data: Dict[str, float] = {}
    
    def add_step_result(self, step_id: str, result: StepResult) -> None:
        """Add a step result to the context."""
        self.step_results[step_id] = result
        self.timing_data[step_id] = result.execution_time
        
        if result.error:
            self.errors.append(result.error)
        
        self.warnings.extend(result.warnings)
    
    def get_step_output(self, step_id: str) -> Any:
        """Get output from a specific step."""
        if step_id not in self.step_results:
            raise ValueError(f"No result found for step: {step_id}")
        return self.step_results[step_id].output
    
    def set_context_data(self, key: str, value: Any) -> None:
        """Set context data accessible to all steps."""
        self.context_data[key] = value
    
    def get_context_data(self, key: str, default: Any = None) -> Any:
        """Get context data."""
        return self.context_data.get(key, default)
    
    @property
    def total_execution_time(self) -> float:
        """Total execution time so far."""
        return time.time() - self.start_time
    
    @property
    def has_errors(self) -> bool:
        """Whether any errors have occurred."""
        return len(self.errors) > 0
    
    @property
    def execution_summary(self) -> Dict[str, Any]:
        """Summary of execution metrics."""
        return {
            "execution_id": self.execution_id,
            "total_time": self.total_execution_time,
            "steps_completed": len([r for r in self.step_results.values() if r.success]),
            "steps_failed": len([r for r in self.step_results.values() if not r.success]),
            "total_warnings": len(self.warnings),
            "total_errors": len(self.errors),
            "step_timing": self.timing_data
        }


# === Pipeline Step Protocol ===

class StepExecutor(Protocol):
    """Protocol for step execution."""
    
    def execute(self, input_data: TInput, context: ExecutionContext) -> StepResult[TOutput]:
        """Execute the step with given input and context."""
        ...


# === Base Pipeline Step ===

class PipelineStep(ABC, Generic[TInput, TOutput]):
    """
    Abstract base class for all pipeline steps.
    
    Provides the framework for composable, type-safe pipeline operations
    with built-in validation, error handling, and performance tracking.
    """
    
    def __init__(
        self, 
        step_id: Optional[str] = None,
        description: Optional[str] = None,
        skip_on_error: bool = False,
        timeout_seconds: Optional[float] = None
    ):
        """
        Initialize pipeline step.
        
        Parameters
        ----------
        step_id : Optional[str]
            Unique identifier for this step
        description : Optional[str]
            Human-readable description
        skip_on_error : bool
            Whether to skip this step if previous steps failed
        timeout_seconds : Optional[float]
            Maximum execution time
        """
        self.step_id = step_id or f"{self.__class__.__name__}_{uuid.uuid4().hex[:8]}"
        self.description = description or self.__class__.__doc__ or "Pipeline step"
        self.skip_on_error = skip_on_error
        self.timeout_seconds = timeout_seconds
        
        # Execution state
        self.status = StepStatus.PENDING
        self.start_time: Optional[float] = None
        self.end_time: Optional[float] = None
    
    @abstractmethod
    def get_input_contract(self) -> Type[TInput]:
        """Get the input data contract for this step."""
        pass
    
    @abstractmethod
    def get_output_contract(self) -> Type[TOutput]:
        """Get the output data contract for this step."""
        pass
    
    @abstractmethod
    def execute_step(self, input_data: TInput, context: ExecutionContext) -> TOutput:
        """
        Execute the step logic.
        
        Parameters
        ----------
        input_data : TInput
            Input data matching the input contract
        context : ExecutionContext
            Execution context
            
        Returns
        -------
        TOutput
            Output data matching the output contract
        """
        pass
    
    def validate_input(self, input_data: TInput, context: ExecutionContext) -> None:
        """
        Validate input data before execution.
        
        Parameters
        ----------
        input_data : TInput
            Input data to validate
        context : ExecutionContext
            Execution context
            
        Raises
        ------
        StepValidationError
            If validation fails
        """
        # Validate input contract
        expected_type = self.get_input_contract()
        if not isinstance(input_data, expected_type):
            raise StepValidationError(
                f"Input data type mismatch. Expected {expected_type.__name__}, "
                f"got {type(input_data).__name__}",
                step_id=self.step_id
            )
        
        # Validate data frames if present
        for field_name, field_value in input_data.model_dump().items():
            if isinstance(field_value, (pl.DataFrame, pl.LazyFrame, LazyFrameWrapper)):
                try:
                    input_data.validate_schema(field_value)
                except DataContractViolation as e:
                    raise StepValidationError(
                        f"Schema validation failed for field {field_name}: {e}",
                        step_id=self.step_id
                    ) from e
    
    def validate_output(self, output_data: TOutput, context: ExecutionContext) -> None:
        """
        Validate output data after execution.
        
        Parameters
        ----------
        output_data : TOutput
            Output data to validate
        context : ExecutionContext
            Execution context
            
        Raises
        ------
        StepValidationError
            If validation fails
        """
        # Validate output contract
        expected_type = self.get_output_contract()
        if not isinstance(output_data, expected_type):
            raise StepValidationError(
                f"Output data type mismatch. Expected {expected_type.__name__}, "
                f"got {type(output_data).__name__}",
                step_id=self.step_id
            )
    
    def should_skip(self, context: ExecutionContext) -> bool:
        """
        Determine if this step should be skipped.
        
        Parameters
        ----------
        context : ExecutionContext
            Execution context
            
        Returns
        -------
        bool
            Whether to skip this step
        """
        if self.skip_on_error and context.has_errors:
            return True
        return False
    
    @contextmanager
    def execution_timer(self) -> Iterator[None]:
        """Context manager for timing step execution."""
        self.start_time = time.time()
        try:
            yield
        finally:
            self.end_time = time.time()
    
    def execute(self, input_data: TInput, context: ExecutionContext) -> StepResult[TOutput]:
        """
        Execute the step with full lifecycle management.
        
        Parameters
        ----------
        input_data : TInput
            Input data
        context : ExecutionContext
            Execution context
            
        Returns
        -------
        StepResult[TOutput]
            Step execution result
        """
        # Check if step should be skipped
        if self.should_skip(context):
            return StepResult(
                output=input_data,  # Pass through input
                status=StepStatus.SKIPPED
            )
        
        # Initialize result
        result = StepResult(
            output=input_data,  # Default fallback
            status=StepStatus.PENDING
        )
        
        try:
            # Update status
            self.status = StepStatus.RUNNING
            result.status = StepStatus.RUNNING
            
            with self.execution_timer():
                # Validate input
                self.validate_input(input_data, context)
                
                # Execute step
                output_data = self.execute_step(input_data, context)
                
                # Validate output
                self.validate_output(output_data, context)
                
                # Update result
                result.output = output_data
                result.status = StepStatus.COMPLETED
                self.status = StepStatus.COMPLETED
                
        except Exception as e:
            # Handle errors
            result.error = e
            result.status = StepStatus.FAILED
            self.status = StepStatus.FAILED
            
            if context.debug:
                result.add_debug_info("traceback", str(e))
        
        finally:
            # Record timing
            if self.start_time and self.end_time:
                result.execution_time = self.end_time - self.start_time
            
            # Add step metadata
            result.add_debug_info("step_id", self.step_id)
            result.add_debug_info("step_description", self.description)
        
        return result
    
    def __str__(self) -> str:
        """String representation of the step."""
        return f"{self.__class__.__name__}(id={self.step_id})"
    
    def __repr__(self) -> str:
        """Detailed string representation."""
        return (
            f"{self.__class__.__name__}("
            f"step_id='{self.step_id}', "
            f"status={self.status.value}, "
            f"description='{self.description}'"
            f")"
        )


# === Estimation Pipeline ===

class EstimationPipeline:
    """
    Main pipeline orchestrator for composable FIA estimation workflows.
    
    Manages step execution, data flow validation, error handling, and
    performance optimization across the entire estimation process.
    """
    
    def __init__(
        self,
        pipeline_id: Optional[str] = None,
        description: Optional[str] = None,
        execution_mode: ExecutionMode = ExecutionMode.SEQUENTIAL,
        fail_fast: bool = True,
        enable_caching: bool = True,
        debug: bool = False
    ):
        """
        Initialize estimation pipeline.
        
        Parameters
        ----------
        pipeline_id : Optional[str]
            Unique pipeline identifier
        description : Optional[str]
            Pipeline description
        execution_mode : ExecutionMode
            How to execute steps (sequential, parallel, etc.)
        fail_fast : bool
            Whether to stop on first error
        enable_caching : bool
            Whether to enable step result caching
        debug : bool
            Whether to enable debug mode
        """
        self.pipeline_id = pipeline_id or f"pipeline_{uuid.uuid4().hex[:8]}"
        self.description = description or "FIA Estimation Pipeline"
        self.execution_mode = execution_mode
        self.fail_fast = fail_fast
        self.enable_caching = enable_caching
        self.debug = debug
        
        # Pipeline steps
        self.steps: List[PipelineStep] = []
        self.step_cache: Dict[str, Any] = {}
        
        # Execution state
        self.context: Optional[ExecutionContext] = None
    
    def add_step(self, step: PipelineStep) -> "EstimationPipeline":
        """
        Add a step to the pipeline.
        
        Parameters
        ----------
        step : PipelineStep
            Step to add
            
        Returns
        -------
        EstimationPipeline
            Self for method chaining
        """
        self.steps.append(step)
        return self
    
    def insert_step(self, index: int, step: PipelineStep) -> "EstimationPipeline":
        """
        Insert a step at specific position.
        
        Parameters
        ----------
        index : int
            Position to insert at
        step : PipelineStep
            Step to insert
            
        Returns
        -------
        EstimationPipeline
            Self for method chaining
        """
        self.steps.insert(index, step)
        return self
    
    def remove_step(self, step_id: str) -> "EstimationPipeline":
        """
        Remove a step by ID.
        
        Parameters
        ----------
        step_id : str
            ID of step to remove
            
        Returns
        -------
        EstimationPipeline
            Self for method chaining
        """
        self.steps = [s for s in self.steps if s.step_id != step_id]
        return self
    
    def validate_pipeline(self) -> List[str]:
        """
        Validate the pipeline configuration.
        
        Returns
        -------
        List[str]
            List of validation issues (empty if valid)
        """
        issues = []
        
        if not self.steps:
            issues.append("Pipeline has no steps")
            return issues
        
        # Check data contract compatibility
        for i in range(len(self.steps) - 1):
            current_step = self.steps[i]
            next_step = self.steps[i + 1]
            
            current_output = current_step.get_output_contract()
            next_input = next_step.get_input_contract()
            
            # Check if output can be used as input for next step
            # This is a simplified check - could be more sophisticated
            if current_output != next_input:
                # Allow some flexibility in contracts
                if not (hasattr(current_output, "__origin__") and 
                       hasattr(next_input, "__origin__")):
                    issues.append(
                        f"Data contract mismatch between {current_step.step_id} "
                        f"(outputs {current_output.__name__}) and {next_step.step_id} "
                        f"(expects {next_input.__name__})"
                    )
        
        # Check for duplicate step IDs
        step_ids = [s.step_id for s in self.steps]
        duplicate_ids = [sid for sid in step_ids if step_ids.count(sid) > 1]
        if duplicate_ids:
            issues.append(f"Duplicate step IDs: {duplicate_ids}")
        
        return issues
    
    def execute(
        self, 
        db: FIA, 
        config: EstimatorConfig,
        initial_input: Optional[DataContract] = None
    ) -> pl.DataFrame:
        """
        Execute the complete pipeline.
        
        Parameters
        ----------
        db : FIA
            FIA database connection
        config : EstimatorConfig
            Estimation configuration
        initial_input : Optional[DataContract]
            Initial input data (if None, starts with empty TableDataContract)
            
        Returns
        -------
        pl.DataFrame
            Final pipeline output
            
        Raises
        ------
        PipelineException
            If pipeline execution fails
        """
        # Validate pipeline first
        issues = self.validate_pipeline()
        if issues:
            raise PipelineException(f"Pipeline validation failed: {'; '.join(issues)}")
        
        # Initialize execution context
        self.context = ExecutionContext(db, config, debug=self.debug)
        
        try:
            # Set up initial input
            if initial_input is None:
                current_output = TableDataContract(tables={})
            else:
                current_output = initial_input
            
            # Execute steps sequentially (for now - could add parallel execution later)
            for step in self.steps:
                if self.fail_fast and self.context.has_errors:
                    raise PipelineException(
                        f"Pipeline stopped due to previous errors. "
                        f"Failed at step: {step.step_id}"
                    )
                
                # Check cache if enabled
                cache_key = f"{step.step_id}_{hash(str(current_output))}"
                if self.enable_caching and cache_key in self.step_cache:
                    cached_result = self.step_cache[cache_key]
                    result = StepResult(
                        output=cached_result,
                        status=StepStatus.COMPLETED
                    )
                    result.add_debug_info("cache_hit", True)
                else:
                    # Execute step
                    result = step.execute(current_output, self.context)
                    
                    # Cache result if successful
                    if self.enable_caching and result.success:
                        self.step_cache[cache_key] = result.output
                
                # Add result to context
                self.context.add_step_result(step.step_id, result)
                
                # Update current output for next step
                if result.success:
                    current_output = result.output
                elif not step.skip_on_error:
                    raise PipelineException(
                        f"Step {step.step_id} failed: {result.error}",
                        step_id=step.step_id,
                        cause=result.error
                    )
            
            # Final output should be a FormattedOutputContract
            if isinstance(current_output, FormattedOutputContract):
                return current_output.data
            else:
                # If not formatted, try to convert
                if hasattr(current_output, "data"):
                    if isinstance(current_output.data, pl.DataFrame):
                        return current_output.data
                    elif hasattr(current_output.data, "collect"):
                        return current_output.data.collect()
                
                raise PipelineException(
                    "Pipeline did not produce a valid final output. "
                    "Make sure the last step produces FormattedOutputContract."
                )
                
        except Exception as e:
            if isinstance(e, PipelineException):
                raise
            else:
                raise PipelineException(
                    f"Unexpected error in pipeline execution: {e}",
                    cause=e
                ) from e
        
        finally:
            # Log execution summary if debug enabled
            if self.debug and self.context:
                summary = self.context.execution_summary
                warnings.warn(
                    f"Pipeline execution completed: {summary}",
                    category=UserWarning
                )
    
    def get_execution_summary(self) -> Optional[Dict[str, Any]]:
        """Get execution summary if pipeline has been run."""
        if self.context:
            return self.context.execution_summary
        return None
    
    def visualize_pipeline(self) -> str:
        """
        Generate a text visualization of the pipeline.
        
        Returns
        -------
        str
            Pipeline visualization
        """
        lines = [f"Pipeline: {self.pipeline_id}"]
        lines.append(f"Description: {self.description}")
        lines.append(f"Mode: {self.execution_mode.value}")
        lines.append(f"Steps: {len(self.steps)}")
        lines.append("")
        
        for i, step in enumerate(self.steps, 1):
            status_icon = {
                StepStatus.PENDING: "⏳",
                StepStatus.RUNNING: "🔄", 
                StepStatus.COMPLETED: "✅",
                StepStatus.FAILED: "❌",
                StepStatus.SKIPPED: "⏭️"
            }.get(step.status, "❓")
            
            lines.append(f"{i:2d}. {status_icon} {step.step_id}")
            lines.append(f"    {step.description}")
            
            input_contract = step.get_input_contract().__name__
            output_contract = step.get_output_contract().__name__
            lines.append(f"    {input_contract} → {output_contract}")
            lines.append("")
        
        return "\n".join(lines)
    
    def __repr__(self) -> str:
        """Detailed string representation."""
        return (
            f"EstimationPipeline("
            f"id='{self.pipeline_id}', "
            f"steps={len(self.steps)}, "
            f"mode={self.execution_mode.value}"
            f")"
        )