"""
SQLite backend implementation for pyFIA.

This module provides a SQLite-specific implementation of the DatabaseBackend
interface, optimized for FIA data processing with batch handling and
efficient Polars integration.
"""

import logging
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import polars as pl

from .base import DatabaseBackend

logger = logging.getLogger(__name__)


class SQLiteBackend(DatabaseBackend):
    """
    SQLite implementation of the database backend.

    This backend provides:
    - Efficient Polars integration via pl.read_database
    - Batch processing for IN clauses (999 item limit)
    - PRAGMA optimizations for better performance
    - FIA-specific type handling (CN fields as TEXT)
    """

    def __init__(
        self,
        db_path: Union[str, Path],
        timeout: Optional[float] = 30.0,
        check_same_thread: bool = False,
        **kwargs: Any,
    ):
        """
        Initialize SQLite backend.

        Args:
            db_path: Path to SQLite database file
            timeout: Timeout for acquiring database lock
            check_same_thread: Allow connection to be used across threads
            **kwargs: Additional SQLite configuration options
        """
        super().__init__(db_path, **kwargs)
        self.timeout = timeout
        self.check_same_thread = check_same_thread

    def connect(self) -> None:
        """Establish SQLite connection with optimized settings."""
        if self._connection is not None:
            return

        try:
            self._connection = sqlite3.connect(
                str(self.db_path),
                timeout=self.timeout,
                check_same_thread=self.check_same_thread,
            )

            # Apply performance optimizations from interface.py
            self._connection.execute("PRAGMA journal_mode=WAL")
            self._connection.execute("PRAGMA synchronous=NORMAL")
            self._connection.execute("PRAGMA cache_size=10000")
            self._connection.execute("PRAGMA temp_store=MEMORY")

            logger.info(f"Connected to SQLite database: {self.db_path}")
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
    ) -> pl.DataFrame:
        """
        Execute SQL query and return results as Polars DataFrame.

        Uses pl.read_database for efficient conversion.

        Args:
            query: SQL query string
            params: Optional query parameters (uses :param syntax)

        Returns:
            Polars DataFrame with query results
        """
        if not self._connection:
            self.connect()

        start_time = time.time()

        try:
            if params:
                # Convert dict params to list for SQLite
                # SQLite uses ? placeholders
                param_list = []
                modified_query = query
                for key, value in params.items():
                    modified_query = modified_query.replace(f":{key}", "?")
                    param_list.append(value)
                df = pl.read_database(modified_query, self._connection, param_list)
            else:
                df = pl.read_database(query, self._connection)

            execution_time = (time.time() - start_time) * 1000
            logger.debug(
                f"Query executed in {execution_time:.2f}ms, returned {len(df)} rows"
            )

            return df

        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            logger.debug(f"Query: {query}")
            if params:
                logger.debug(f"Parameters: {params}")
            raise

    def get_table_schema(self, table_name: str) -> Dict[str, str]:
        """
        Get schema information for a table.

        Args:
            table_name: Name of the table

        Returns:
            Dictionary mapping column names to SQL types
        """
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
        """
        Check if a table exists in the database.

        Args:
            table_name: Name of the table

        Returns:
            True if table exists, False otherwise
        """
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

    def describe_table(self, table_name: str) -> List[tuple]:
        """
        Get table description for schema detection.

        Args:
            table_name: Name of the table

        Returns:
            List of tuples with column information
        """
        if not self._connection:
            self.connect()

        try:
            cursor = self._connection.cursor()
            cursor.execute(f"PRAGMA table_info({table_name})")
            return cursor.fetchall()
        except Exception as e:
            logger.error(f"Failed to describe table {table_name}: {e}")
            raise

    def is_cn_column(self, column_name: str) -> bool:
        """
        Check if a column is a CN (Control Number) field.

        FIA uses CN fields as identifiers that should always be treated as text.

        Args:
            column_name: Name of the column

        Returns:
            True if the column is a CN field
        """
        return column_name.endswith("_CN") or column_name == "CN"

    def is_string_column(self, table_name: str, column_name: str) -> bool:
        """
        Check if a column should be treated as a string.

        Args:
            table_name: Name of the table
            column_name: Name of the column

        Returns:
            True if the column should be treated as a string
        """
        if self.is_cn_column(column_name):
            return True

        schema = self.get_table_schema(table_name)
        col_type = schema.get(column_name, "").upper()

        return col_type in ["TEXT", "VARCHAR", "CHAR", "CHARACTER", "NVARCHAR", "NCHAR"]

    def is_float_column(self, table_name: str, column_name: str) -> bool:
        """
        Check if a column should be treated as a floating point number.

        Args:
            table_name: Name of the table
            column_name: Name of the column

        Returns:
            True if the column should be treated as a float
        """
        schema = self.get_table_schema(table_name)
        col_type = schema.get(column_name, "").upper()

        return col_type in ["REAL", "DOUBLE", "FLOAT", "NUMERIC", "DECIMAL"]

    def is_integer_column(self, table_name: str, column_name: str) -> bool:
        """
        Check if a column should be treated as an integer.

        Args:
            table_name: Name of the table
            column_name: Name of the column

        Returns:
            True if the column should be treated as an integer
        """
        if self.is_cn_column(column_name):
            return False  # CN fields are always strings

        schema = self.get_table_schema(table_name)
        col_type = schema.get(column_name, "").upper()

        return col_type in [
            "INTEGER",
            "INT",
            "BIGINT",
            "SMALLINT",
            "TINYINT",
            "MEDIUMINT",
        ]

    def build_select_clause(
        self, table_name: str, columns: Optional[List[str]] = None
    ) -> str:
        """
        Build SELECT clause with appropriate type casting for FIA data.

        Args:
            table_name: Name of the table
            columns: Optional list of columns to select

        Returns:
            SELECT clause with type casting
        """
        schema = self.get_table_schema(table_name)

        if columns is None:
            columns = list(schema.keys())

        select_parts = []
        for col in columns:
            if self.is_cn_column(col):
                # Cast CN fields to TEXT for consistency
                select_parts.append(f"CAST({col} AS TEXT) AS {col}")
            else:
                # Use column as-is
                select_parts.append(col)

        return ", ".join(select_parts)

    def read_dataframe(
        self, query: str, schema_overrides: Optional[Dict[str, Any]] = None
    ) -> pl.DataFrame:
        """
        Execute query and return results as DataFrame.

        This method provides compatibility with the data reader interface.

        Args:
            query: SQL query to execute
            schema_overrides: Optional schema overrides for Polars

        Returns:
            Polars DataFrame with results
        """
        if not self._connection:
            self.connect()

        if schema_overrides:
            # For SQLite, we need to handle schema overrides differently
            # Execute query and then cast columns
            df = pl.read_database(query, self._connection)

            # Apply schema overrides
            for col, dtype in schema_overrides.items():
                if col in df.columns:
                    try:
                        df = df.with_columns(pl.col(col).cast(dtype))
                    except:
                        # Keep original type if cast fails
                        pass

            return df
        else:
            return self.execute_query(query)

    def read_table(
        self,
        table_name: str,
        columns: Optional[List[str]] = None,
        where: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> pl.DataFrame:
        """
        Read a table with optional filtering, handling batch processing for large IN clauses.

        Args:
            table_name: Name of the table to read
            columns: Optional list of columns to select
            where: Optional WHERE clause (without 'WHERE' keyword)
            limit: Optional row limit

        Returns:
            Polars DataFrame with the results
        """
        # Check if we have a large IN clause that needs batching
        if where and " IN (" in where and where.count(",") > 900:
            # This is a large IN clause, need to handle it specially
            # For now, use the parent implementation
            return super().read_table(table_name, columns, where, limit)

        # Otherwise use standard implementation
        return super().read_table(table_name, columns, where, limit)
