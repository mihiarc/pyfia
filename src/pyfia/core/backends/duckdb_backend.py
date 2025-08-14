"""
DuckDB backend implementation for pyFIA.

This module provides a DuckDB-specific implementation of the DatabaseBackend
interface, optimized for FIA data processing with native Polars integration.
"""

import logging
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import duckdb
import polars as pl

from .base import DatabaseBackend

logger = logging.getLogger(__name__)


class DuckDBBackend(DatabaseBackend):
    """
    DuckDB implementation of the database backend.

    This backend provides:
    - Native DuckDB-to-Polars conversion via result.pl()
    - Configurable memory limits and thread counts
    - FIA-specific type handling (CN fields as TEXT)
    - Optimized for analytical queries on columnar data
    """

    def __init__(
        self,
        db_path: Union[str, Path],
        read_only: bool = True,
        memory_limit: Optional[str] = None,
        threads: Optional[int] = None,
        **kwargs: Any,
    ):
        """
        Initialize DuckDB backend.

        Args:
            db_path: Path to DuckDB database file
            read_only: Open database in read-only mode
            memory_limit: Memory limit for DuckDB (e.g., '4GB')
            threads: Number of threads for DuckDB to use
            **kwargs: Additional DuckDB configuration options
        """
        super().__init__(db_path, **kwargs)
        self.read_only = read_only
        self.memory_limit = memory_limit
        self.threads = threads

    def connect(self) -> None:
        """Establish DuckDB connection with optimized settings."""
        if self._connection is not None:
            return

        connect_kwargs = {
            "database": str(self.db_path),
            "read_only": self.read_only,
        }

        # Add configuration options
        config_options = {}
        if self.memory_limit:
            config_options["memory_limit"] = self.memory_limit
        if self.threads:
            config_options["threads"] = self.threads

        if config_options:
            connect_kwargs["config"] = config_options

        try:
            self._connection = duckdb.connect(**connect_kwargs)
            logger.info(f"Connected to DuckDB database: {self.db_path}")
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
    ) -> pl.DataFrame:
        """
        Execute SQL query and return results as Polars DataFrame.

        Uses native DuckDB result.pl() for efficient conversion.

        Args:
            query: SQL query string
            params: Optional query parameters (uses $param syntax)

        Returns:
            Polars DataFrame with query results
        """
        if not self._connection:
            self.connect()

        start_time = time.time()

        try:
            if params:
                # DuckDB uses $parameter_name syntax
                for key, value in params.items():
                    query = query.replace(f":{key}", f"${key}")
                result = self._connection.execute(query, params)
            else:
                result = self._connection.execute(query)

            # Native DuckDB to Polars conversion
            df = result.pl()

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
            result = self._connection.execute(f"DESCRIBE {table_name}").fetchall()
            schema = {row[0]: row[1] for row in result}
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
            result = self._connection.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
                [table_name],
            ).fetchone()
            return result[0] > 0 if result else False
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
            return self._connection.execute(f"DESCRIBE {table_name}").fetchall()
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

        return col_type in ["VARCHAR", "TEXT", "CHAR", "STRING"]

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

        return col_type in ["DOUBLE", "FLOAT", "REAL", "DECIMAL", "NUMERIC"]

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

        return col_type in ["INTEGER", "BIGINT", "INT", "SMALLINT", "TINYINT"]

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
                # Always cast CN fields to VARCHAR
                select_parts.append(f"CAST({col} AS VARCHAR) AS {col}")
            else:
                # Use column as-is
                select_parts.append(col)

        return ", ".join(select_parts)

    def read_dataframe(self, query: str, **kwargs) -> pl.DataFrame:
        """
        Execute query and return results as DataFrame.

        This method provides compatibility with the data reader interface.

        Args:
            query: SQL query to execute
            **kwargs: Additional arguments (ignored for DuckDB)

        Returns:
            Polars DataFrame with results
        """
        return self.execute_query(query)
