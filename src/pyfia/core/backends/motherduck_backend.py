"""
MotherDuck backend implementation for pyFIA.

This module provides a MotherDuck-specific implementation of the DatabaseBackend
interface, enabling cloud-based FIA data access via MotherDuck's serverless DuckDB.
"""

import logging
import os
import time
from typing import Any, Dict, List, Optional

import duckdb
import polars as pl

from .base import DatabaseBackend

logger = logging.getLogger(__name__)


class MotherDuckBackend(DatabaseBackend):
    """
    MotherDuck implementation of the database backend.

    This backend provides:
    - Connection to MotherDuck cloud databases
    - Native DuckDB-to-Polars conversion via result.pl()
    - FIA-specific type handling (CN fields as VARCHAR)
    - Optimized for analytical queries on columnar data

    MotherDuck stores FIA CN (Control Number) columns as DOUBLE due to their
    large values (>1e22). This backend casts them to VARCHAR for consistent
    string-based joins, matching the local DuckDB behavior.
    """

    def __init__(
        self,
        database: str,
        motherduck_token: Optional[str] = None,
        **kwargs: Any,
    ):
        """
        Initialize MotherDuck backend.

        Parameters
        ----------
        database : str
            Name of the MotherDuck database (e.g., 'fia_ga')
        motherduck_token : Optional[str]
            MotherDuck authentication token. If not provided, uses
            MOTHERDUCK_TOKEN environment variable.
        **kwargs : Any
            Additional configuration options
        """
        # Don't pass db_path to super since we don't have a file path
        self.database = database
        self.motherduck_token = motherduck_token or os.environ.get("MOTHERDUCK_TOKEN")
        self._connection: Optional[duckdb.DuckDBPyConnection] = None
        self._schema_cache: Dict[str, Dict[str, str]] = {}

        if not self.motherduck_token:
            raise ValueError(
                "MotherDuck token required. Set MOTHERDUCK_TOKEN environment "
                "variable or pass motherduck_token parameter."
            )

    def connect(self) -> None:
        """Establish MotherDuck connection."""
        if self._connection is not None:
            return

        try:
            # Connect to MotherDuck using md: protocol
            connection_string = f"md:{self.database}?motherduck_token={self.motherduck_token}"
            self._connection = duckdb.connect(connection_string)
            logger.info(f"Connected to MotherDuck database: {self.database}")
        except Exception as e:
            logger.error(f"Failed to connect to MotherDuck: {e}")
            raise

    def disconnect(self) -> None:
        """Close MotherDuck connection."""
        if self._connection is not None:
            try:
                self._connection.close()
                self._connection = None
                logger.info("Disconnected from MotherDuck database")
            except Exception as e:
                logger.error(f"Error closing MotherDuck connection: {e}")

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

        MotherDuck stores CN columns as DOUBLE (due to large values >1e22).
        This method casts CN columns to VARCHAR for consistent string-based joins.

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
                # Cast CN fields to VARCHAR for consistent joins
                # MotherDuck stores these as DOUBLE which can overflow
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
            Additional arguments (ignored)

        Returns
        -------
        pl.DataFrame
            Polars DataFrame with results
        """
        return self.execute_query(query)
