"""
Database interfaces and utilities for pyFIA.

This module provides:
- Query interfaces for DuckDB and other database engines
- Schema mapping utilities
- FIA database documentation and memory aids
"""

from .query_interface import DuckDBQueryInterface
from .schema_mapper import *

__all__ = [
    "DuckDBQueryInterface",
] 