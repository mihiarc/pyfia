"""
Abstract base class for database backends.

This module defines the interface that all database backends must implement
to ensure consistent behavior across different database engines.
"""

from abc import ABC, abstractmethod
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Union

import polars as pl
from pydantic import BaseModel, ConfigDict, Field


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


class DatabaseBackend(ABC):
    """
    Abstract base class for database backends.

    This class defines the contract that all database implementations
    must follow to ensure consistent behavior across different backends.
    """

    def __init__(self, db_path: Union[str, Path], **kwargs: Any):
        """
        Initialize database backend.

        Parameters
        ----------
        db_path : Union[str, Path]
            Path to database file
        **kwargs : Any
            Backend-specific configuration options
        """
        self.db_path = Path(db_path)
        if not self.db_path.exists():
            raise FileNotFoundError(f"Database not found: {db_path}")

        self._connection: Optional[Any] = None
        self._schema_cache: Dict[str, Dict[str, str]] = {}
        self._kwargs = kwargs

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
    ) -> pl.DataFrame:
        """
        Execute a SQL query and return results as Polars DataFrame.

        Parameters
        ----------
        query : str
            SQL query string
        params : Optional[Dict[str, Any]], optional
            Optional query parameters

        Returns
        -------
        pl.DataFrame
            Polars DataFrame with query results
        """
        pass

    @abstractmethod
    def get_table_schema(self, table_name: str) -> Dict[str, str]:
        """
        Get schema information for a table.

        Parameters
        ----------
        table_name : str
            Name of the table

        Returns
        -------
        Dict[str, str]
            Dictionary mapping column names to SQL types
        """
        pass

    @abstractmethod
    def table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists in the database.

        Parameters
        ----------
        table_name : str
            Name of the table

        Returns
        -------
        bool
            True if table exists, False otherwise
        """
        pass

    @abstractmethod
    def describe_table(self, table_name: str) -> List[tuple]:
        """
        Get table description for schema detection.

        Parameters
        ----------
        table_name : str
            Name of the table

        Returns
        -------
        List[tuple]
            List of tuples with column information
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

        Parameters
        ----------
        table_name : str
            Name of the table to read
        columns : Optional[List[str]], optional
            Optional list of columns to select
        where : Optional[str], optional
            Optional WHERE clause (without 'WHERE' keyword)
        limit : Optional[int], optional
            Optional row limit

        Returns
        -------
        pl.DataFrame
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

        return self.execute_query(query)

    @contextmanager
    def transaction(self) -> Iterator[None]:
        """
        Context manager for database transactions.

        Yields
        ------
        None

        Raises
        ------
        Exception
            If transaction fails
        """
        if not self._connection:
            self.connect()

        try:
            yield
            if hasattr(self._connection, "commit"):
                self._connection.commit()  # type: ignore[union-attr]
        except Exception:
            if hasattr(self._connection, "rollback"):
                self._connection.rollback()  # type: ignore[union-attr]
            raise

    def __enter__(self) -> "DatabaseBackend":
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit."""
        self.disconnect()

    @property
    def is_connected(self) -> bool:
        """Check if database is connected."""
        return self._connection is not None
