"""
Database backend implementations for pyFIA.

This module provides the DuckDB database backend for FIA data access.
"""

from pathlib import Path
from typing import Any, Union

from .base import DatabaseBackend, QueryResult
from .duckdb_backend import DuckDBBackend

__all__ = [
    "DatabaseBackend",
    "DuckDBBackend",
    "QueryResult",
    "create_backend",
]


def create_backend(db_path: Union[str, Path], **kwargs: Any) -> DatabaseBackend:
    """
    Create a DuckDB database backend.

    Parameters
    ----------
    db_path : Union[str, Path]
        Path to the DuckDB database file
    **kwargs : Any
        Additional backend configuration options:
        - read_only: bool, default True
        - memory_limit: str, e.g., "8GB"
        - threads: int

    Returns
    -------
    DatabaseBackend
        DuckDB backend instance

    Examples
    --------
    >>> backend = create_backend("path/to/database.duckdb")

    >>> # With memory limit
    >>> backend = create_backend(
    ...     "path/to/database.duckdb",
    ...     memory_limit="8GB",
    ...     threads=4
    ... )
    """
    return DuckDBBackend(Path(db_path), **kwargs)
