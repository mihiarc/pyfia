"""
Database backend implementations for pyFIA.

This module provides database backends for FIA data access:
- DuckDBBackend: Local DuckDB file access
- MotherDuckBackend: Cloud-based MotherDuck access
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

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


def create_backend(db_path: str | Path, **kwargs: Any) -> DatabaseBackend:
    """
    Create a database backend (DuckDB or MotherDuck).

    Parameters
    ----------
    db_path : str | Path
        Path to the database. Supports:
        - Local file path: "path/to/database.duckdb"
        - MotherDuck: "md:database_name" or "motherduck:database_name"
    **kwargs : Any
        Additional backend configuration options:
        - read_only: bool, default True
        - memory_limit: str, e.g., "8GB"
        - threads: int
        - motherduck_token: str (for MotherDuck connections)

    Returns
    -------
    DatabaseBackend
        DuckDB or MotherDuck backend instance

    Examples
    --------
    >>> backend = create_backend("path/to/database.duckdb")

    >>> # MotherDuck connection
    >>> backend = create_backend("md:fia_ga_eval2023")

    >>> # With memory limit
    >>> backend = create_backend(
    ...     "path/to/database.duckdb",
    ...     memory_limit="8GB",
    ...     threads=4
    ... )
    """
    db_str = str(db_path)

    # Check for MotherDuck prefix
    if db_str.startswith("md:") or db_str.startswith("motherduck:"):
        # Extract database name from prefix
        if db_str.startswith("md:"):
            database = db_str[3:]
        else:
            database = db_str[11:]

        motherduck_token = kwargs.pop("motherduck_token", None)
        return MotherDuckBackend(database, motherduck_token=motherduck_token, **kwargs)

    return DuckDBBackend(Path(db_path), **kwargs)


def create_motherduck_backend(
    database: str,
    motherduck_token: str | None = None,
    **kwargs: Any,
) -> MotherDuckBackend:
    """
    Create a MotherDuck database backend.

    Parameters
    ----------
    database : str
        Name of the MotherDuck database (e.g., 'fia_ga')
    motherduck_token : str | None
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
