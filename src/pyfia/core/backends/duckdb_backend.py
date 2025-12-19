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
    - Spatial extension support for polygon clipping
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

        Parameters
        ----------
        db_path : Union[str, Path]
            Path to DuckDB database file
        read_only : bool
            Open database in read-only mode
        memory_limit : Optional[str]
            Memory limit for DuckDB (e.g., '4GB')
        threads : Optional[int]
            Number of threads for DuckDB to use
        **kwargs : Any
            Additional DuckDB configuration options
        """
        super().__init__(db_path, **kwargs)
        self.read_only = read_only
        self.memory_limit = memory_limit
        self.threads = threads
        self._spatial_loaded = False

    def connect(self) -> None:
        """Establish DuckDB connection with optimized settings."""
        if self._connection is not None:
            return

        connect_kwargs = {
            "database": str(self.db_path),
            "read_only": self.read_only,
        }

        # Add configuration options
        config_options: dict[str, Any] = {}
        if self.memory_limit:
            config_options["memory_limit"] = self.memory_limit
        if self.threads:
            config_options["threads"] = self.threads

        if config_options:
            connect_kwargs["config"] = config_options

        try:
            self._connection = duckdb.connect(**connect_kwargs)  # type: ignore[arg-type]
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

        Parameters
        ----------
        query : str
            SQL query string
        params : Optional[Dict[str, Any]]
            Optional query parameters (uses $param syntax)

        Returns
        -------
        pl.DataFrame
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
                result = self._connection.execute(query, params)  # type: ignore[union-attr]
            else:
                result = self._connection.execute(query)  # type: ignore[union-attr]

            # Native DuckDB to Polars conversion
            df: pl.DataFrame = result.pl()

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

        Parameters
        ----------
        table_name : str
            Name of the table

        Returns
        -------
        Dict[str, str]
            Dictionary mapping column names to SQL types
        """
        if table_name in self._schema_cache:
            return self._schema_cache[table_name]

        if not self._connection:
            self.connect()

        try:
            result = self._connection.execute(  # type: ignore[union-attr]
                f"DESCRIBE {table_name}"
            ).fetchall()
            schema = {row[0]: row[1] for row in result}
            self._schema_cache[table_name] = schema
            return schema
        except Exception as e:
            logger.error(f"Failed to get schema for table {table_name}: {e}")
            raise

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
        if not self._connection:
            self.connect()

        try:
            result = self._connection.execute(  # type: ignore[union-attr]
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
                [table_name],
            ).fetchone()
            return result[0] > 0 if result else False
        except Exception:
            return False

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
        if not self._connection:
            self.connect()

        try:
            return self._connection.execute(  # type: ignore[union-attr, no-any-return]
                f"DESCRIBE {table_name}"
            ).fetchall()
        except Exception as e:
            logger.error(f"Failed to describe table {table_name}: {e}")
            raise

    def is_cn_column(self, column_name: str) -> bool:
        """
        Check if a column is a CN (Control Number) field.

        FIA uses CN fields as identifiers that should always be treated as text.

        Parameters
        ----------
        column_name : str
            Name of the column

        Returns
        -------
        bool
            True if the column is a CN field
        """
        return column_name.endswith("_CN") or column_name == "CN"

    def is_string_column(self, table_name: str, column_name: str) -> bool:
        """
        Check if a column should be treated as a string.

        Parameters
        ----------
        table_name : str
            Name of the table
        column_name : str
            Name of the column

        Returns
        -------
        bool
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

        Parameters
        ----------
        table_name : str
            Name of the table
        column_name : str
            Name of the column

        Returns
        -------
        bool
            True if the column should be treated as a float
        """
        schema = self.get_table_schema(table_name)
        col_type = schema.get(column_name, "").upper()

        return col_type in ["DOUBLE", "FLOAT", "REAL", "DECIMAL", "NUMERIC"]

    def is_integer_column(self, table_name: str, column_name: str) -> bool:
        """
        Check if a column should be treated as an integer.

        Parameters
        ----------
        table_name : str
            Name of the table
        column_name : str
            Name of the column

        Returns
        -------
        bool
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

        Parameters
        ----------
        table_name : str
            Name of the table
        columns : Optional[List[str]]
            Optional list of columns to select

        Returns
        -------
        str
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

        Parameters
        ----------
        query : str
            SQL query to execute
        **kwargs : Any
            Additional arguments (ignored for DuckDB)

        Returns
        -------
        pl.DataFrame
            Polars DataFrame with results
        """
        return self.execute_query(query)

    def load_spatial_extension(self) -> None:
        """
        Load the DuckDB spatial extension.

        This method installs (if needed) and loads the spatial extension,
        enabling spatial functions like ST_Read, ST_Intersects, ST_Point, etc.

        Raises
        ------
        SpatialExtensionError
            If the spatial extension cannot be loaded.
        """
        if self._spatial_loaded:
            return

        if not self._connection:
            self.connect()

        try:
            # Install spatial extension (no-op if already installed)
            self._connection.execute("INSTALL spatial")  # type: ignore[union-attr]
            # Load the extension
            self._connection.execute("LOAD spatial")  # type: ignore[union-attr]
            self._spatial_loaded = True
            logger.info("DuckDB spatial extension loaded successfully")
        except Exception as e:
            from ..exceptions import SpatialExtensionError

            logger.error(f"Failed to load spatial extension: {e}")
            raise SpatialExtensionError(str(e))

    def execute_spatial_query(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> pl.DataFrame:
        """
        Execute a spatial SQL query.

        This method ensures the spatial extension is loaded before executing
        the query, enabling use of spatial functions like ST_Read, ST_Intersects, etc.

        Parameters
        ----------
        query : str
            SQL query string with spatial functions
        params : Optional[Dict[str, Any]]
            Optional query parameters

        Returns
        -------
        pl.DataFrame
            Polars DataFrame with query results
        """
        self.load_spatial_extension()
        return self.execute_query(query, params)
