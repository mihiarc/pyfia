"""
Database backend implementations for pyFIA.

This module provides database backends for FIA data access:
- DuckDBBackend: Local DuckDB file access
- MotherDuckBackend: Cloud-based MotherDuck access
"""

from pathlib import Path
from typing import Any, Optional, Union

from .base import DatabaseBackend, QueryResult
from .duckdb_backend import DuckDBBackend
from .motherduck_backend import MotherDuckBackend

__all__ = [
    "DatabaseBackend",
    "DuckDBBackend",
    "MotherDuckBackend",
    "QueryResult",
    "create_backend",
    "create_motherduck_backend",
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


def create_motherduck_backend(
    database: str,
    motherduck_token: Optional[str] = None,
    **kwargs: Any,
) -> MotherDuckBackend:
    """
    Create a MotherDuck database backend.

    Parameters
    ----------
    database : str
        Name of the MotherDuck database (e.g., 'fia_ga')
    motherduck_token : Optional[str]
        MotherDuck authentication token. If not provided, uses
        MOTHERDUCK_TOKEN environment variable.
    **kwargs : Any
        Additional backend configuration options

    Returns
    -------
    MotherDuckBackend
        MotherDuck backend instance

    Examples
    --------
    >>> backend = create_motherduck_backend("fia_ga")

    >>> # With explicit token
    >>> backend = create_motherduck_backend(
    ...     "fia_ga",
    ...     motherduck_token="your_token_here"
    ... )
    """
    return MotherDuckBackend(database, motherduck_token=motherduck_token, **kwargs)
