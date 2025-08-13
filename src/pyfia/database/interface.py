"""
Database interface layer for pyFIA.

This module provides a flexible database abstraction that supports both
DuckDB and SQLite backends while maintaining consistent behavior.
"""

import logging
import sqlite3
from abc import ABC, abstractmethod
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Union

import duckdb
import polars as pl
from pydantic import BaseModel, ConfigDict, Field, field_validator
from rich.console import Console

# Set up logging
logger = logging.getLogger(__name__)
console = Console()


class QueryResult(BaseModel):
    """Result of a database query."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    data: pl.DataFrame
    row_count: int = Field(default=0)
    execution_time_ms: Optional[float] = Field(default=None)

    def __init__(self, **data: Any) -> None:
        """Initialize query result and calculate row count."""
        super().__init__(**data)
        if "row_count" not in data and hasattr(self.data, "shape"):
            self.row_count = self.data.shape[0]


class ConnectionConfig(BaseModel):
    """Configuration for database connections."""

    db_path: Path
    read_only: bool = Field(default=True)
    timeout: Optional[float] = Field(default=30.0)
    memory_limit: Optional[str] = Field(default=None)
    threads: Optional[int] = Field(default=None)

    @field_validator("db_path")
    @classmethod
    def validate_db_path(cls, v: Path) -> Path:
        """Ensure database path exists."""
        if not v.exists():
            raise FileNotFoundError(f"Database not found: {v}")
        return v


class QueryInterface(ABC):
    """
    Abstract base class for database query interfaces.

    This class defines the contract that all database implementations
    must follow to ensure consistent behavior across different backends.
    """

    def __init__(self, config: ConnectionConfig):
        """
        Initialize the query interface.

        Args:
            config: Database connection configuration
        """
        self.config = config
        self._connection: Optional[Any] = None
        self._schema_cache: Dict[str, Dict[str, str]] = {}

    @abstractmethod
    def connect(self) -> None:
        """Establish database connection."""
        pass

    @abstractmethod
    def disconnect(self) -> None:
        """Close database connection."""
        pass

    @abstractmethod
    def execute_query(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        fetch_as_polars: bool = True,
    ) -> Union[QueryResult, Any]:
        """
        Execute a SQL query.

        Args:
            query: SQL query string
            params: Optional query parameters
            fetch_as_polars: If True, return results as Polars DataFrame

        Returns:
            QueryResult with Polars DataFrame or raw results
        """
        pass

    @abstractmethod
    def get_table_schema(self, table_name: str) -> Dict[str, str]:
        """
        Get schema information for a table.

        Args:
            table_name: Name of the table

        Returns:
            Dictionary mapping column names to SQL types
        """
        pass

    @abstractmethod
    def table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists in the database.

        Args:
            table_name: Name of the table

        Returns:
            True if table exists, False otherwise
        """
        pass

    def read_table(
        self,
        table_name: str,
        columns: Optional[List[str]] = None,
        where: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> pl.DataFrame:
        """
        Read a table with optional filtering.

        Args:
            table_name: Name of the table to read
            columns: Optional list of columns to select
            where: Optional WHERE clause (without 'WHERE' keyword)
            limit: Optional row limit

        Returns:
            Polars DataFrame with the results
        """
        # Build query
        if columns:
            col_str = ", ".join(columns)
            query = f"SELECT {col_str} FROM {table_name}"
        else:
            query = f"SELECT * FROM {table_name}"

        if where:
            query += f" WHERE {where}"

        if limit:
            query += f" LIMIT {limit}"

        result = self.execute_query(query)
        return result.data

    @contextmanager
    def transaction(self) -> Iterator[None]:
        """
        Context manager for database transactions.

        Yields:
            None

        Raises:
            Exception: If transaction fails
        """
        if not self._connection:
            self.connect()

        try:
            yield
            if hasattr(self._connection, "commit"):
                self._connection.commit()
        except Exception as e:
            if hasattr(self._connection, "rollback"):
                self._connection.rollback()
            logger.error(f"Transaction failed: {e}")
            raise

    def __enter__(self) -> "QueryInterface":
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit."""
        self.disconnect()


class DuckDBInterface(QueryInterface):
    """DuckDB implementation of the query interface."""

    def connect(self) -> None:
        """Establish DuckDB connection."""
        if self._connection is not None:
            return

        connect_kwargs = {
            "database": str(self.config.db_path),
            "read_only": self.config.read_only,
        }

        # Add optional configuration
        config_options = {}
        if self.config.memory_limit:
            config_options["memory_limit"] = self.config.memory_limit
        if self.config.threads:
            config_options["threads"] = self.config.threads
        # Note: lock_timeout is not supported in all DuckDB versions

        if config_options:
            connect_kwargs["config"] = config_options

        try:
            self._connection = duckdb.connect(**connect_kwargs)
            logger.info(f"Connected to DuckDB database: {self.config.db_path}")
        except Exception as e:
            logger.error(f"Failed to connect to DuckDB: {e}")
            raise

    def disconnect(self) -> None:
        """Close DuckDB connection."""
        if self._connection is not None:
            try:
                self._connection.close()
                self._connection = None
                logger.info("Disconnected from DuckDB database")
            except Exception as e:
                logger.error(f"Error closing DuckDB connection: {e}")

    def execute_query(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        fetch_as_polars: bool = True,
    ) -> Union[QueryResult, Any]:
        """Execute a SQL query on DuckDB."""
        if not self._connection:
            self.connect()

        import time

        start_time = time.time()

        try:
            if params:
                # DuckDB uses $parameter_name syntax for parameters
                for key, value in params.items():
                    query = query.replace(f":{key}", f"${key}")
                result = self._connection.execute(query, params)
            else:
                result = self._connection.execute(query)

            if fetch_as_polars:
                df = result.pl()
                execution_time = (time.time() - start_time) * 1000
                return QueryResult(data=df, execution_time_ms=execution_time)
            else:
                return result

        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            logger.debug(f"Query: {query}")
            if params:
                logger.debug(f"Parameters: {params}")
            raise

    def get_table_schema(self, table_name: str) -> Dict[str, str]:
        """Get schema information for a DuckDB table."""
        if table_name in self._schema_cache:
            return self._schema_cache[table_name]

        if not self._connection:
            self.connect()

        try:
            result = self._connection.execute(f"DESCRIBE {table_name}").fetchall()
            schema = {row[0]: row[1] for row in result}
            self._schema_cache[table_name] = schema
            return schema
        except Exception as e:
            logger.error(f"Failed to get schema for table {table_name}: {e}")
            raise

    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in DuckDB."""
        if not self._connection:
            self.connect()

        try:
            result = self._connection.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
                [table_name],
            ).fetchone()
            return result[0] > 0 if result else False
        except Exception:
            return False


class SQLiteInterface(QueryInterface):
    """SQLite implementation of the query interface."""

    def connect(self) -> None:
        """Establish SQLite connection."""
        if self._connection is not None:
            return

        try:
            self._connection = sqlite3.connect(
                str(self.config.db_path),
                timeout=self.config.timeout or 30.0,
                check_same_thread=False,
            )

            # Enable some optimizations
            self._connection.execute("PRAGMA journal_mode=WAL")
            self._connection.execute("PRAGMA synchronous=NORMAL")
            self._connection.execute("PRAGMA cache_size=10000")
            self._connection.execute("PRAGMA temp_store=MEMORY")

            logger.info(f"Connected to SQLite database: {self.config.db_path}")
        except Exception as e:
            logger.error(f"Failed to connect to SQLite: {e}")
            raise

    def disconnect(self) -> None:
        """Close SQLite connection."""
        if self._connection is not None:
            try:
                self._connection.close()
                self._connection = None
                logger.info("Disconnected from SQLite database")
            except Exception as e:
                logger.error(f"Error closing SQLite connection: {e}")

    def execute_query(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        fetch_as_polars: bool = True,
    ) -> Union[QueryResult, Any]:
        """Execute a SQL query on SQLite."""
        if not self._connection:
            self.connect()

        import time

        start_time = time.time()

        try:
            if fetch_as_polars:
                # Use Polars' read_database for better performance
                if params:
                    # Convert dict params to list for SQLite
                    # SQLite uses ? placeholders, so we need to convert :name to ?
                    param_list = []
                    modified_query = query
                    for key, value in params.items():
                        modified_query = modified_query.replace(f":{key}", "?")
                        param_list.append(value)
                    df = pl.read_database(modified_query, self._connection, param_list)
                else:
                    df = pl.read_database(query, self._connection)

                execution_time = (time.time() - start_time) * 1000
                return QueryResult(data=df, execution_time_ms=execution_time)
            else:
                cursor = self._connection.cursor()
                if params:
                    # Convert named params to positional for SQLite
                    param_list = []
                    modified_query = query
                    for key, value in params.items():
                        modified_query = modified_query.replace(f":{key}", "?")
                        param_list.append(value)
                    cursor.execute(modified_query, param_list)
                else:
                    cursor.execute(query)
                return cursor

        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            logger.debug(f"Query: {query}")
            if params:
                logger.debug(f"Parameters: {params}")
            raise

    def get_table_schema(self, table_name: str) -> Dict[str, str]:
        """Get schema information for a SQLite table."""
        if table_name in self._schema_cache:
            return self._schema_cache[table_name]

        if not self._connection:
            self.connect()

        try:
            cursor = self._connection.cursor()
            cursor.execute(f"PRAGMA table_info({table_name})")
            rows = cursor.fetchall()
            # PRAGMA columns: cid, name, type, notnull, dflt_value, pk
            schema = {row[1]: (row[2] or "TEXT") for row in rows}
            self._schema_cache[table_name] = schema
            return schema
        except Exception as e:
            logger.error(f"Failed to get schema for table {table_name}: {e}")
            raise

    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in SQLite."""
        if not self._connection:
            self.connect()

        try:
            cursor = self._connection.cursor()
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                [table_name],
            )
            return cursor.fetchone() is not None
        except Exception:
            return False


def create_interface(
    db_path: Union[str, Path],
    engine: Optional[str] = None,
    **kwargs: Any,
) -> QueryInterface:
    """
    Factory function to create appropriate database interface.

    Args:
        db_path: Path to the database file
        engine: Database engine ('duckdb' or 'sqlite'). If None, auto-detect.
        **kwargs: Additional configuration options

    Returns:
        Appropriate QueryInterface implementation

    Raises:
        ValueError: If engine type cannot be determined
    """
    db_path = Path(db_path)

    # Auto-detect engine if not specified
    if engine is None:
        if db_path.suffix.lower() in [".duckdb", ".db", ".ddb"]:
            # Try to determine by attempting connections
            try:
                # Try DuckDB first
                conn = duckdb.connect(str(db_path), read_only=True)
                conn.execute("SELECT 1").fetchone()
                conn.close()
                engine = "duckdb"
            except Exception:
                # Try SQLite
                try:
                    conn = sqlite3.connect(str(db_path))
                    cursor = conn.cursor()
                    cursor.execute("SELECT name FROM sqlite_master LIMIT 1")
                    cursor.fetchone()
                    conn.close()
                    engine = "sqlite"
                except Exception:
                    raise ValueError(
                        f"Could not determine database type for {db_path}"
                    )
        else:
            raise ValueError(
                f"Cannot determine database engine from extension: {db_path.suffix}"
            )

    # Create configuration
    config = ConnectionConfig(db_path=db_path, **kwargs)

    # Create appropriate interface
    if engine.lower() == "duckdb":
        return DuckDBInterface(config)
    elif engine.lower() == "sqlite":
        return SQLiteInterface(config)
    else:
        raise ValueError(f"Unsupported database engine: {engine}")