"""
FIA SQLite to DuckDB Converter Module.

This module provides tools for converting FIA DataMart SQLite state-level
databases into optimized DuckDB format for improved analytical performance.

Key Features:
- Single state conversion with optimization
- Multi-state database merging
- Incremental updates
- Data validation and integrity checks
- Error recovery and checkpointing
- Progress tracking
"""

from .models import (
    ConversionMetadata,
    ConversionResult,
    ConverterConfig,
    OptimizedSchema,
    UpdateResult,
    ValidationResult,
)
from .schema_loader import FIASchemaLoader, get_schema_loader
from .schema_optimizer import SchemaOptimizer
from .sqlite_to_duckdb import FIAConverter
from .state_merger import StateMerger
from .validation import DataValidator

__all__ = [
    # Main converter class
    "FIAConverter",
    # Component classes
    "SchemaOptimizer",
    "StateMerger",
    "DataValidator",
    "FIASchemaLoader",
    "get_schema_loader",
    # Configuration and result models
    "ConverterConfig",
    "ConversionResult",
    "UpdateResult",
    "ValidationResult",
    "ConversionMetadata",
    "OptimizedSchema",
]
