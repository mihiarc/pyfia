"""
Database backend implementations for pyFIA.

This module provides database backend abstraction to support multiple
database engines (DuckDB and SQLite) while maintaining a consistent API.
"""

from pathlib import Path
from typing import Any, Optional, Union

from .base import DatabaseBackend, QueryResult
from .duckdb_backend import DuckDBBackend
from .sqlite_backend import SQLiteBackend

__all__ = [
    "DatabaseBackend",
    "DuckDBBackend",
    "SQLiteBackend",
    "QueryResult",
    "create_backend",
    "detect_engine",
]


def detect_engine(db_path: Union[str, Path]) -> str:
    """
    Auto-detect database engine type.

    Args:
        db_path: Path to database file

    Returns:
        Engine type ('duckdb' or 'sqlite')

    Raises:
        ValueError: If engine type cannot be determined
    """
    import sqlite3
    from pathlib import Path

    import duckdb

    db_path = Path(db_path)

    # Try DuckDB first
    try:
        conn = duckdb.connect(str(db_path), read_only=True)
        conn.execute("SELECT 1").fetchone()
        conn.close()
        return "duckdb"
    except Exception:
        pass

    # Try SQLite
    try:
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master LIMIT 1")
        cursor.fetchone()
        conn.close()
        return "sqlite"
    except Exception:
        pass

    raise ValueError(f"Could not determine database type for {db_path}")


def create_backend(
    db_path: Union[str, Path], engine: Optional[str] = None, **kwargs: Any
) -> DatabaseBackend:
    """
    Factory function to create appropriate database backend.

    Args:
        db_path: Path to the database file
        engine: Database engine ('duckdb' or 'sqlite'). If None, auto-detect.
        **kwargs: Additional backend-specific configuration options:
            - For DuckDB: read_only, memory_limit, threads
            - For SQLite: timeout, check_same_thread

    Returns:
        Appropriate DatabaseBackend implementation

    Raises:
        ValueError: If engine type cannot be determined or is unsupported

    Examples:
        >>> # Auto-detect engine
        >>> backend = create_backend("path/to/database.duckdb")

        >>> # Explicitly specify SQLite with options
        >>> backend = create_backend(
        ...     "path/to/database.db",
        ...     engine="sqlite",
        ...     timeout=60.0
        ... )

        >>> # DuckDB with memory limit
        >>> backend = create_backend(
        ...     "path/to/database.duckdb",
        ...     memory_limit="8GB",
        ...     threads=4
        ... )
    """
    db_path = Path(db_path)

    # Auto-detect engine if not specified
    if engine is None:
        engine = detect_engine(db_path)

    # Create appropriate backend
    if engine.lower() == "duckdb":
        return DuckDBBackend(db_path, **kwargs)
    elif engine.lower() == "sqlite":
        return SQLiteBackend(db_path, **kwargs)
    else:
        raise ValueError(f"Unsupported database engine: {engine}")
