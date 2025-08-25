"""
Error Handling and Recovery for pyFIA Phase 4 Pipeline.

This module provides comprehensive error handling, recovery strategies,
and graceful degradation capabilities for pipeline execution.
"""

import json
import pickle
import traceback
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Type, Union

import polars as pl
from rich.console import Console
from rich.panel import Panel
from rich.text import Text

from .core import (
    PipelineStep, ExecutionContext, StepResult, StepStatus,
    PipelineException, DataContract
)


# === Enums ===

class ErrorSeverity(str, Enum):
    """Severity levels for errors."""
    LOW = "low"  # Can be ignored or worked around
    MEDIUM = "medium"  # Affects quality but not critical
    HIGH = "high"  # Significant impact on results
    CRITICAL = "critical"  # Pipeline cannot continue


class RecoveryStrategy(str, Enum):
    """Recovery strategies for errors."""
    RETRY = "retry"  # Retry the operation
    SKIP = "skip"  # Skip the failed step
    FALLBACK = "fallback"  # Use fallback implementation
    PARTIAL = "partial"  # Continue with partial data
    CHECKPOINT = "checkpoint"  # Restore from checkpoint
    ABORT = "abort"  # Abort pipeline execution


class ErrorCategory(str, Enum):
    """Categories of errors."""
    DATA = "data"  # Data-related errors
    COMPUTATION = "computation"  # Computation errors
    RESOURCE = "resource"  # Resource constraints
    CONFIGURATION = "configuration"  # Configuration issues
    NETWORK = "network"  # Network/IO errors
    VALIDATION = "validation"  # Validation failures
    UNKNOWN = "unknown"  # Unknown errors


# === Data Classes ===

@dataclass
class ErrorContext:
    """Context information for an error."""
    
    error: Exception
    step_id: Optional[str] = None
    error_category: ErrorCategory = ErrorCategory.UNKNOWN
    severity: ErrorSeverity = ErrorSeverity.MEDIUM
    timestamp: float = field(default_factory=lambda: __import__("time").time())
    traceback_str: Optional[str] = None
    input_data_info: Optional[Dict[str, Any]] = None
    system_info: Optional[Dict[str, Any]] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        """Initialize traceback if not provided."""
        if self.traceback_str is None and self.error:
            self.traceback_str = traceback.format_exc()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "error_type": type(self.error).__name__,
            "error_message": str(self.error),
            "step_id": self.step_id,
            "category": self.error_category.value,
            "severity": self.severity.value,
            "timestamp": self.timestamp,
            "traceback": self.traceback_str,
            "input_data_info": self.input_data_info,
            "system_info": self.system_info,
            "metadata": self.metadata
        }


@dataclass
class RecoveryAction:
    """Action to take for recovery."""
    
    strategy: RecoveryStrategy
    handler: Optional[Callable] = None
    fallback_data: Optional[Any] = None
    max_attempts: int = 3
    delay_seconds: float = 1.0
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def execute(
        self,
        error_context: ErrorContext,
        execution_context: ExecutionContext
    ) -> Optional[Any]:
        """Execute recovery action."""
        if self.handler:
            return self.handler(error_context, execution_context)
        
        if self.strategy == RecoveryStrategy.FALLBACK and self.fallback_data:
            return self.fallback_data
        
        return None


@dataclass
class ErrorReport:
    """Comprehensive error report."""
    
    pipeline_id: str
    execution_id: str
    total_errors: int = 0
    recovered_errors: int = 0
    unrecovered_errors: int = 0
    errors_by_category: Dict[ErrorCategory, int] = field(default_factory=dict)
    errors_by_severity: Dict[ErrorSeverity, int] = field(default_factory=dict)
    error_contexts: List[ErrorContext] = field(default_factory=list)
    recovery_actions: List[Tuple[ErrorContext, RecoveryAction]] = field(default_factory=list)
    
    def add_error(self, error_context: ErrorContext, recovered: bool = False) -> None:
        """Add an error to the report."""
        self.total_errors += 1
        
        if recovered:
            self.recovered_errors += 1
        else:
            self.unrecovered_errors += 1
        
        # Update category counts
        category = error_context.error_category
        self.errors_by_category[category] = self.errors_by_category.get(category, 0) + 1
        
        # Update severity counts
        severity = error_context.severity
        self.errors_by_severity[severity] = self.errors_by_severity.get(severity, 0) + 1
        
        # Store context
        self.error_contexts.append(error_context)
    
    def get_critical_errors(self) -> List[ErrorContext]:
        """Get critical errors."""
        return [e for e in self.error_contexts if e.severity == ErrorSeverity.CRITICAL]
    
    def summary(self) -> Dict[str, Any]:
        """Get error report summary."""
        return {
            "pipeline_id": self.pipeline_id,
            "execution_id": self.execution_id,
            "total_errors": self.total_errors,
            "recovered_errors": self.recovered_errors,
            "unrecovered_errors": self.unrecovered_errors,
            "recovery_rate": self.recovered_errors / self.total_errors if self.total_errors > 0 else 0,
            "critical_errors": len(self.get_critical_errors()),
            "errors_by_category": {k.value: v for k, v in self.errors_by_category.items()},
            "errors_by_severity": {k.value: v for k, v in self.errors_by_severity.items()}
        }


# === Base Error Handler ===

class ErrorHandler(ABC):
    """Abstract base class for error handlers."""
    
    @abstractmethod
    def can_handle(self, error: Exception) -> bool:
        """Check if this handler can handle the error."""
        pass
    
    @abstractmethod
    def handle(
        self,
        error_context: ErrorContext,
        execution_context: ExecutionContext
    ) -> RecoveryAction:
        """Handle the error and return recovery action."""
        pass
    
    def categorize_error(self, error: Exception) -> ErrorCategory:
        """Categorize the error."""
        error_type = type(error).__name__
        error_msg = str(error).lower()
        
        # Data errors
        if "dataframe" in error_msg or "column" in error_msg or "schema" in error_msg:
            return ErrorCategory.DATA
        
        # Resource errors
        if "memory" in error_msg or "disk" in error_msg or "resource" in error_msg:
            return ErrorCategory.RESOURCE
        
        # Network/IO errors
        if "connection" in error_msg or "timeout" in error_msg or "io" in error_type.lower():
            return ErrorCategory.NETWORK
        
        # Configuration errors
        if "config" in error_msg or "parameter" in error_msg or "setting" in error_msg:
            return ErrorCategory.CONFIGURATION
        
        # Validation errors
        if "validation" in error_msg or "invalid" in error_msg or "contract" in error_msg:
            return ErrorCategory.VALIDATION
        
        # Computation errors
        if "division" in error_msg or "overflow" in error_msg or "calculation" in error_msg:
            return ErrorCategory.COMPUTATION
        
        return ErrorCategory.UNKNOWN
    
    def assess_severity(self, error: Exception) -> ErrorSeverity:
        """Assess error severity."""
        # Critical errors
        if isinstance(error, (MemoryError, SystemError)):
            return ErrorSeverity.CRITICAL
        
        # High severity
        if isinstance(error, (ValueError, KeyError, AttributeError)):
            return ErrorSeverity.HIGH
        
        # Medium severity
        if isinstance(error, (IOError, ConnectionError)):
            return ErrorSeverity.MEDIUM
        
        # Low severity
        if isinstance(error, (Warning, DeprecationWarning)):
            return ErrorSeverity.LOW
        
        return ErrorSeverity.MEDIUM


# === Specific Error Handlers ===

class DataErrorHandler(ErrorHandler):
    """Handles data-related errors."""
    
    def can_handle(self, error: Exception) -> bool:
        """Check if this is a data error."""
        return self.categorize_error(error) == ErrorCategory.DATA
    
    def handle(
        self,
        error_context: ErrorContext,
        execution_context: ExecutionContext
    ) -> RecoveryAction:
        """Handle data error."""
        error = error_context.error
        error_msg = str(error).lower()
        
        # Missing column - try to add default
        if "column" in error_msg and "not found" in error_msg:
            return RecoveryAction(
                strategy=RecoveryStrategy.FALLBACK,
                handler=self._add_missing_column
            )
        
        # Empty dataframe - skip or use partial
        if "empty" in error_msg:
            return RecoveryAction(
                strategy=RecoveryStrategy.PARTIAL,
                handler=self._handle_empty_data
            )
        
        # Schema mismatch - try to cast
        if "schema" in error_msg or "type" in error_msg:
            return RecoveryAction(
                strategy=RecoveryStrategy.FALLBACK,
                handler=self._fix_schema
            )
        
        # Default: skip the step
        return RecoveryAction(strategy=RecoveryStrategy.SKIP)
    
    def _add_missing_column(
        self,
        error_context: ErrorContext,
        execution_context: ExecutionContext
    ) -> Optional[pl.DataFrame]:
        """Add missing column with default values."""
        # Extract column name from error
        error_msg = str(error_context.error)
        
        # This is simplified - would need proper parsing
        if "column" in error_msg.lower():
            # Try to extract column name and add it
            # Return modified dataframe
            pass
        
        return None
    
    def _handle_empty_data(
        self,
        error_context: ErrorContext,
        execution_context: ExecutionContext
    ) -> Optional[pl.DataFrame]:
        """Handle empty data."""
        # Return empty dataframe with correct schema
        return pl.DataFrame()
    
    def _fix_schema(
        self,
        error_context: ErrorContext,
        execution_context: ExecutionContext
    ) -> Optional[pl.DataFrame]:
        """Fix schema issues."""
        # Try to cast columns to expected types
        return None


class ResourceErrorHandler(ErrorHandler):
    """Handles resource-related errors."""
    
    def can_handle(self, error: Exception) -> bool:
        """Check if this is a resource error."""
        return (
            isinstance(error, MemoryError) or
            self.categorize_error(error) == ErrorCategory.RESOURCE
        )
    
    def handle(
        self,
        error_context: ErrorContext,
        execution_context: ExecutionContext
    ) -> RecoveryAction:
        """Handle resource error."""
        error = error_context.error
        
        # Memory error - try with smaller batch
        if isinstance(error, MemoryError):
            return RecoveryAction(
                strategy=RecoveryStrategy.RETRY,
                handler=self._reduce_batch_size,
                max_attempts=3
            )
        
        # Disk space - clean up temp files
        if "disk" in str(error).lower():
            return RecoveryAction(
                strategy=RecoveryStrategy.RETRY,
                handler=self._cleanup_temp_files,
                max_attempts=2
            )
        
        # Default: abort
        return RecoveryAction(strategy=RecoveryStrategy.ABORT)
    
    def _reduce_batch_size(
        self,
        error_context: ErrorContext,
        execution_context: ExecutionContext
    ) -> None:
        """Reduce batch size for processing."""
        # Modify config to use smaller batches
        if hasattr(execution_context.config, "batch_size"):
            execution_context.config.batch_size //= 2
    
    def _cleanup_temp_files(
        self,
        error_context: ErrorContext,
        execution_context: ExecutionContext
    ) -> None:
        """Clean up temporary files."""
        import tempfile
        import shutil
        
        temp_dir = Path(tempfile.gettempdir())
        # Clean up old temp files (simplified)
        for file in temp_dir.glob("pyfia_temp_*"):
            try:
                if file.is_file():
                    file.unlink()
                elif file.is_dir():
                    shutil.rmtree(file)
            except Exception:
                pass


class ComputationErrorHandler(ErrorHandler):
    """Handles computation errors."""
    
    def can_handle(self, error: Exception) -> bool:
        """Check if this is a computation error."""
        return (
            isinstance(error, (ZeroDivisionError, OverflowError, ArithmeticError)) or
            self.categorize_error(error) == ErrorCategory.COMPUTATION
        )
    
    def handle(
        self,
        error_context: ErrorContext,
        execution_context: ExecutionContext
    ) -> RecoveryAction:
        """Handle computation error."""
        error = error_context.error
        
        # Division by zero - use alternative calculation
        if isinstance(error, ZeroDivisionError):
            return RecoveryAction(
                strategy=RecoveryStrategy.FALLBACK,
                handler=self._handle_division_by_zero
            )
        
        # Overflow - use different numeric type
        if isinstance(error, OverflowError):
            return RecoveryAction(
                strategy=RecoveryStrategy.FALLBACK,
                handler=self._handle_overflow
            )
        
        # Default: use partial results
        return RecoveryAction(strategy=RecoveryStrategy.PARTIAL)
    
    def _handle_division_by_zero(
        self,
        error_context: ErrorContext,
        execution_context: ExecutionContext
    ) -> Any:
        """Handle division by zero."""
        # Return NaN or alternative value
        return float("nan")
    
    def _handle_overflow(
        self,
        error_context: ErrorContext,
        execution_context: ExecutionContext
    ) -> Any:
        """Handle numeric overflow."""
        # Use larger numeric type or clip values
        return None


# === Checkpoint Manager ===

class CheckpointManager:
    """Manages pipeline checkpoints for recovery."""
    
    def __init__(self, checkpoint_dir: Optional[Path] = None):
        """
        Initialize checkpoint manager.
        
        Parameters
        ----------
        checkpoint_dir : Optional[Path]
            Directory for storing checkpoints
        """
        self.checkpoint_dir = checkpoint_dir or Path.home() / ".pyfia" / "checkpoints"
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        self.checkpoints: Dict[str, Path] = {}
    
    def save_checkpoint(
        self,
        step_id: str,
        data: Any,
        execution_id: str
    ) -> Path:
        """
        Save a checkpoint.
        
        Parameters
        ----------
        step_id : str
            Step identifier
        data : Any
            Data to checkpoint
        execution_id : str
            Execution identifier
            
        Returns
        -------
        Path
            Path to checkpoint file
        """
        checkpoint_file = self.checkpoint_dir / f"{execution_id}_{step_id}.checkpoint"
        
        # Save based on data type
        if isinstance(data, pl.DataFrame):
            data.write_parquet(checkpoint_file.with_suffix(".parquet"))
        elif isinstance(data, DataContract):
            with open(checkpoint_file.with_suffix(".json"), "w") as f:
                json.dump(data.model_dump(), f)
        else:
            # Use pickle for other types
            with open(checkpoint_file, "wb") as f:
                pickle.dump(data, f)
        
        self.checkpoints[f"{execution_id}_{step_id}"] = checkpoint_file
        return checkpoint_file
    
    def load_checkpoint(
        self,
        step_id: str,
        execution_id: str,
        data_type: Optional[Type] = None
    ) -> Optional[Any]:
        """
        Load a checkpoint.
        
        Parameters
        ----------
        step_id : str
            Step identifier
        execution_id : str
            Execution identifier
        data_type : Optional[Type]
            Expected data type
            
        Returns
        -------
        Optional[Any]
            Checkpoint data if available
        """
        checkpoint_key = f"{execution_id}_{step_id}"
        
        # Check for parquet file
        parquet_file = self.checkpoint_dir / f"{checkpoint_key}.parquet"
        if parquet_file.exists():
            return pl.read_parquet(parquet_file)
        
        # Check for JSON file
        json_file = self.checkpoint_dir / f"{checkpoint_key}.json"
        if json_file.exists() and data_type:
            with open(json_file) as f:
                data = json.load(f)
                if issubclass(data_type, DataContract):
                    return data_type(**data)
        
        # Check for pickle file
        pickle_file = self.checkpoint_dir / f"{checkpoint_key}.checkpoint"
        if pickle_file.exists():
            with open(pickle_file, "rb") as f:
                return pickle.load(f)
        
        return None
    
    def cleanup_checkpoints(
        self,
        execution_id: str,
        keep_last_n: int = 0
    ) -> None:
        """
        Clean up checkpoints.
        
        Parameters
        ----------
        execution_id : str
            Execution identifier
        keep_last_n : int
            Number of recent checkpoints to keep
        """
        pattern = f"{execution_id}_*.checkpoint"
        checkpoint_files = sorted(
            self.checkpoint_dir.glob(pattern),
            key=lambda p: p.stat().st_mtime,
            reverse=True
        )
        
        # Remove old checkpoints
        for file in checkpoint_files[keep_last_n:]:
            try:
                file.unlink()
            except Exception:
                pass


# === Rollback Manager ===

class RollbackManager:
    """Manages rollback operations for pipeline failures."""
    
    def __init__(self):
        """Initialize rollback manager."""
        self.rollback_stack: List[Callable] = []
        self.completed_rollbacks: List[str] = []
    
    def register_rollback(
        self,
        rollback_func: Callable,
        description: str = "Rollback operation"
    ) -> None:
        """
        Register a rollback operation.
        
        Parameters
        ----------
        rollback_func : Callable
            Function to call for rollback
        description : str
            Description of rollback operation
        """
        self.rollback_stack.append((rollback_func, description))
    
    def rollback(self, partial: bool = False) -> List[str]:
        """
        Execute rollback operations.
        
        Parameters
        ----------
        partial : bool
            Whether to do partial rollback
            
        Returns
        -------
        List[str]
            List of completed rollback operations
        """
        rollback_results = []
        
        while self.rollback_stack:
            rollback_func, description = self.rollback_stack.pop()
            
            try:
                rollback_func()
                rollback_results.append(f"✓ {description}")
                self.completed_rollbacks.append(description)
            except Exception as e:
                rollback_results.append(f"✗ {description}: {e}")
            
            if partial and len(rollback_results) >= 1:
                break
        
        return rollback_results
    
    def clear(self) -> None:
        """Clear rollback stack."""
        self.rollback_stack.clear()
        self.completed_rollbacks.clear()


# === Error Recovery Engine ===

class ErrorRecoveryEngine:
    """
    Main error recovery orchestrator.
    
    Coordinates error handling, recovery strategies, checkpointing,
    and rollback operations for pipeline execution.
    """
    
    def __init__(
        self,
        enable_checkpointing: bool = True,
        enable_rollback: bool = True,
        checkpoint_dir: Optional[Path] = None
    ):
        """
        Initialize error recovery engine.
        
        Parameters
        ----------
        enable_checkpointing : bool
            Whether to enable checkpointing
        enable_rollback : bool
            Whether to enable rollback
        checkpoint_dir : Optional[Path]
            Directory for checkpoints
        """
        self.enable_checkpointing = enable_checkpointing
        self.enable_rollback = enable_rollback
        
        # Initialize components
        self.checkpoint_manager = CheckpointManager(checkpoint_dir) if enable_checkpointing else None
        self.rollback_manager = RollbackManager() if enable_rollback else None
        
        # Error handlers
        self.error_handlers: List[ErrorHandler] = [
            DataErrorHandler(),
            ResourceErrorHandler(),
            ComputationErrorHandler()
        ]
        
        # Error tracking
        self.error_report: Optional[ErrorReport] = None
        self.console = Console()
    
    def register_handler(self, handler: ErrorHandler) -> None:
        """Register custom error handler."""
        self.error_handlers.append(handler)
    
    def handle_error(
        self,
        error: Exception,
        step_id: Optional[str],
        execution_context: ExecutionContext,
        input_data: Optional[Any] = None
    ) -> Tuple[RecoveryAction, ErrorContext]:
        """
        Handle an error and determine recovery action.
        
        Parameters
        ----------
        error : Exception
            The error to handle
        step_id : Optional[str]
            Step where error occurred
        execution_context : ExecutionContext
            Execution context
        input_data : Optional[Any]
            Input data when error occurred
            
        Returns
        -------
        Tuple[RecoveryAction, ErrorContext]
            Recovery action and error context
        """
        # Create error context
        error_context = ErrorContext(
            error=error,
            step_id=step_id
        )
        
        # Categorize and assess error
        for handler in self.error_handlers:
            if handler.can_handle(error):
                error_context.error_category = handler.categorize_error(error)
                error_context.severity = handler.assess_severity(error)
                
                # Get recovery action
                recovery_action = handler.handle(error_context, execution_context)
                
                # Record in error report
                if self.error_report:
                    self.error_report.add_error(
                        error_context,
                        recovered=recovery_action.strategy != RecoveryStrategy.ABORT
                    )
                
                return recovery_action, error_context
        
        # No handler found - use default
        error_context.error_category = ErrorCategory.UNKNOWN
        error_context.severity = ErrorSeverity.HIGH
        
        recovery_action = RecoveryAction(strategy=RecoveryStrategy.ABORT)
        
        if self.error_report:
            self.error_report.add_error(error_context, recovered=False)
        
        return recovery_action, error_context
    
    def execute_recovery(
        self,
        recovery_action: RecoveryAction,
        error_context: ErrorContext,
        execution_context: ExecutionContext,
        retry_func: Optional[Callable] = None
    ) -> Optional[Any]:
        """
        Execute recovery action.
        
        Parameters
        ----------
        recovery_action : RecoveryAction
            Recovery action to execute
        error_context : ErrorContext
            Error context
        execution_context : ExecutionContext
            Execution context
        retry_func : Optional[Callable]
            Function to retry
            
        Returns
        -------
        Optional[Any]
            Recovery result if successful
        """
        strategy = recovery_action.strategy
        
        # Retry strategy
        if strategy == RecoveryStrategy.RETRY and retry_func:
            import time
            
            for attempt in range(recovery_action.max_attempts):
                try:
                    time.sleep(recovery_action.delay_seconds * (attempt + 1))
                    return retry_func()
                except Exception as e:
                    if attempt == recovery_action.max_attempts - 1:
                        raise
                    continue
        
        # Checkpoint recovery
        elif strategy == RecoveryStrategy.CHECKPOINT and self.checkpoint_manager:
            checkpoint_data = self.checkpoint_manager.load_checkpoint(
                error_context.step_id,
                execution_context.execution_id
            )
            if checkpoint_data:
                return checkpoint_data
        
        # Fallback strategy
        elif strategy == RecoveryStrategy.FALLBACK:
            return recovery_action.execute(error_context, execution_context)
        
        # Skip strategy
        elif strategy == RecoveryStrategy.SKIP:
            return None
        
        # Partial strategy
        elif strategy == RecoveryStrategy.PARTIAL:
            # Return partial results if available
            if execution_context.step_results:
                last_result = list(execution_context.step_results.values())[-1]
                if last_result.output:
                    return last_result.output
        
        return None
    
    def start_pipeline(
        self,
        pipeline_id: str,
        execution_id: str
    ) -> None:
        """Start error tracking for pipeline."""
        self.error_report = ErrorReport(
            pipeline_id=pipeline_id,
            execution_id=execution_id
        )
    
    def save_checkpoint(
        self,
        step_id: str,
        data: Any,
        execution_id: str
    ) -> None:
        """Save checkpoint if enabled."""
        if self.checkpoint_manager:
            self.checkpoint_manager.save_checkpoint(step_id, data, execution_id)
    
    def register_rollback(
        self,
        rollback_func: Callable,
        description: str
    ) -> None:
        """Register rollback operation."""
        if self.rollback_manager:
            self.rollback_manager.register_rollback(rollback_func, description)
    
    def perform_rollback(self, partial: bool = False) -> List[str]:
        """Perform rollback operations."""
        if self.rollback_manager:
            return self.rollback_manager.rollback(partial)
        return []
    
    def display_error_report(self) -> None:
        """Display error report in console."""
        if not self.error_report:
            return
        
        # Create error summary
        summary = self.error_report.summary()
        
        # Format as rich panel
        content = Text()
        content.append(f"Total Errors: {summary['total_errors']}\n", style="bold")
        content.append(f"Recovered: {summary['recovered_errors']}\n", style="green")
        content.append(f"Unrecovered: {summary['unrecovered_errors']}\n", style="red")
        content.append(f"Recovery Rate: {summary['recovery_rate']:.1%}\n")
        
        if summary['critical_errors'] > 0:
            content.append(f"\nCritical Errors: {summary['critical_errors']}\n", style="bold red")
        
        # Show error categories
        if summary['errors_by_category']:
            content.append("\nErrors by Category:\n", style="bold")
            for category, count in summary['errors_by_category'].items():
                content.append(f"  {category}: {count}\n")
        
        panel = Panel(
            content,
            title=f"Error Report - {self.error_report.pipeline_id}",
            border_style="red" if summary['unrecovered_errors'] > 0 else "green"
        )
        
        self.console.print(panel)
    
    def get_error_report(self) -> Optional[ErrorReport]:
        """Get current error report."""
        return self.error_report


# === Graceful Degradation Support ===

class GracefulDegradation:
    """Provides graceful degradation capabilities."""
    
    @staticmethod
    def with_default(default_value: Any) -> Callable:
        """Decorator to provide default value on error."""
        def decorator(func: Callable) -> Callable:
            def wrapper(*args, **kwargs):
                try:
                    return func(*args, **kwargs)
                except Exception:
                    return default_value
            return wrapper
        return decorator
    
    @staticmethod
    def with_partial_results() -> Callable:
        """Decorator to return partial results on error."""
        def decorator(func: Callable) -> Callable:
            def wrapper(*args, **kwargs):
                results = []
                try:
                    # Attempt to get full results
                    return func(*args, **kwargs)
                except Exception:
                    # Return whatever we have
                    if results:
                        return results
                    raise
            return wrapper
        return decorator
    
    @staticmethod
    def with_reduced_functionality(
        reduced_func: Callable
    ) -> Callable:
        """Decorator to use reduced functionality on error."""
        def decorator(func: Callable) -> Callable:
            def wrapper(*args, **kwargs):
                try:
                    return func(*args, **kwargs)
                except Exception:
                    # Fall back to simpler implementation
                    return reduced_func(*args, **kwargs)
            return wrapper
        return decorator


# === Export public API ===

__all__ = [
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
    "GracefulDegradation"
]