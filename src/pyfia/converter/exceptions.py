"""
Custom exceptions for FIA converter operations.

This module provides specific exception types for different failure modes
in the conversion process, enabling better error handling and recovery.
"""

from pathlib import Path
from typing import Any, Dict, Optional


class ConversionError(Exception):
    """Base exception for conversion errors."""

    def __init__(self, message: str, context: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.context = context or {}


class SourceReadError(ConversionError):
    """Error reading from source database."""

    def __init__(
        self,
        source_path: Path,
        table_name: str,
        original_error: Exception,
        context: Optional[Dict[str, Any]] = None
    ):
        self.source_path = source_path
        self.table_name = table_name
        self.original_error = original_error

        message = f"Failed to read {table_name} from {source_path}: {original_error}"
        super().__init__(message, context)


class SchemaCompatibilityError(ConversionError):
    """Schema compatibility issues between source and target."""

    def __init__(
        self,
        table_name: str,
        details: str,
        source_schema: Optional[Dict] = None,
        target_schema: Optional[Dict] = None
    ):
        self.table_name = table_name
        self.details = details
        self.source_schema = source_schema
        self.target_schema = target_schema

        context = {
            "source_schema": source_schema,
            "target_schema": target_schema
        }

        message = f"Schema compatibility error for {table_name}: {details}"
        super().__init__(message, context)


class InsertionError(ConversionError):
    """Error inserting data into target database."""

    def __init__(
        self,
        table_name: str,
        batch_info: Dict[str, Any],
        original_error: Exception
    ):
        self.table_name = table_name
        self.batch_info = batch_info
        self.original_error = original_error

        context = {
            "batch_info": batch_info,
            "original_error": str(original_error)
        }

        message = f"Failed to insert data into {table_name}: {original_error}"
        super().__init__(message, context)


class ValidationError(ConversionError):
    """Data validation error during conversion."""

    def __init__(
        self,
        table_name: str,
        validation_type: str,
        failed_checks: list,
        context: Optional[Dict[str, Any]] = None
    ):
        self.table_name = table_name
        self.validation_type = validation_type
        self.failed_checks = failed_checks

        message = f"Validation failed for {table_name} ({validation_type}): {len(failed_checks)} checks failed"
        super().__init__(message, context)


class StateConflictError(ConversionError):
    """Conflict when merging data from multiple states."""

    def __init__(
        self,
        conflicting_states: list,
        conflict_details: Dict[str, Any],
        resolution_strategy: Optional[str] = None
    ):
        self.conflicting_states = conflicting_states
        self.conflict_details = conflict_details
        self.resolution_strategy = resolution_strategy

        context = {
            "conflict_details": conflict_details,
            "resolution_strategy": resolution_strategy
        }

        message = f"State conflict between states {conflicting_states}"
        super().__init__(message, context)


class ResourceError(ConversionError):
    """Resource-related errors (memory, disk, etc.)."""

    def __init__(
        self,
        resource_type: str,
        threshold_exceeded: str,
        current_usage: Optional[str] = None
    ):
        self.resource_type = resource_type
        self.threshold_exceeded = threshold_exceeded
        self.current_usage = current_usage

        context = {
            "resource_type": resource_type,
            "threshold_exceeded": threshold_exceeded,
            "current_usage": current_usage
        }

        message = f"Resource limit exceeded for {resource_type}: {threshold_exceeded}"
        if current_usage:
            message += f" (current: {current_usage})"

        super().__init__(message, context)


class CheckpointError(ConversionError):
    """Error during checkpoint save/restore operations."""

    def __init__(self, operation: str, checkpoint_path: Path, original_error: Exception):
        self.operation = operation
        self.checkpoint_path = checkpoint_path
        self.original_error = original_error

        context = {
            "operation": operation,
            "checkpoint_path": str(checkpoint_path),
            "original_error": str(original_error)
        }

        message = f"Checkpoint {operation} failed for {checkpoint_path}: {original_error}"
        super().__init__(message, context)
