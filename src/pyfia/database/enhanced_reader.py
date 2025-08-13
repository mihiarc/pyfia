"""
Enhanced FIA data reader using the database interface layer.

This module provides a high-level data reader that leverages the
flexible database interface to work with both DuckDB and SQLite backends.
"""

import logging
from pathlib import Path
from typing import Dict, List, Literal, Optional, Union, overload

import polars as pl
from pydantic import BaseModel, Field

from .interface import QueryInterface, create_interface

logger = logging.getLogger(__name__)


class TableReadConfig(BaseModel):
    """Configuration for table reading operations."""

    table_name: str
    columns: Optional[List[str]] = Field(default=None)
    where: Optional[str] = Field(default=None)
    limit: Optional[int] = Field(default=None)
    batch_size: int = Field(default=1000, description="Batch size for large IN clauses")


class EnhancedFIADataReader:
    """
    Enhanced FIA data reader that works with multiple database backends.

    This reader provides:
    - Automatic backend detection and interface selection
    - Efficient batch processing for large queries
    - Lazy evaluation support
    - Consistent handling of FIA-specific data types
    - Built-in optimizations for common FIA query patterns
    """

    def __init__(
        self,
        db_path: Union[str, Path],
        engine: Optional[str] = None,
        lazy_by_default: bool = True,
        **kwargs,
    ):
        """
        Initialize the enhanced data reader.

        Args:
            db_path: Path to the FIA database
            engine: Database engine ('duckdb' or 'sqlite'). Auto-detected if None.
            lazy_by_default: If True, return LazyFrames by default
            **kwargs: Additional configuration for the database interface
        """
        self.db_path = Path(db_path)
        self.engine = engine
        self.lazy_by_default = lazy_by_default

        # Create database interface
        self._interface = create_interface(db_path, engine=engine, **kwargs)

        # Cache for table schemas and metadata
        self._table_cache: Dict[str, pl.LazyFrame] = {}
        self._evalid_cache: Dict[str, List[int]] = {}

    def __enter__(self) -> "EnhancedFIADataReader":
        """Context manager entry."""
        self._interface.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self._interface.disconnect()

    @property
    def interface(self) -> QueryInterface:
        """Get the underlying database interface."""
        return self._interface

    @overload
    def read_table(
        self,
        table_name: str,
        columns: Optional[List[str]] = None,
        where: Optional[str] = None,
        lazy: Literal[False] = False,
    ) -> pl.DataFrame: ...

    @overload
    def read_table(
        self,
        table_name: str,
        columns: Optional[List[str]] = None,
        where: Optional[str] = None,
        lazy: Literal[True] = True,
    ) -> pl.LazyFrame: ...

    def read_table(
        self,
        table_name: str,
        columns: Optional[List[str]] = None,
        where: Optional[str] = None,
        lazy: Optional[bool] = None,
    ) -> Union[pl.DataFrame, pl.LazyFrame]:
        """
        Read a table from the FIA database.

        Args:
            table_name: Name of the table to read
            columns: Optional list of columns to select
            where: Optional WHERE clause (without 'WHERE' keyword)
            lazy: If True, return LazyFrame; if False, return DataFrame

        Returns:
            Polars DataFrame or LazyFrame
        """
        if lazy is None:
            lazy = self.lazy_by_default

        # Read from database
        df = self._interface.read_table(
            table_name=table_name,
            columns=columns,
            where=where,
        )

        # Handle FIA-specific data type conversions
        df = self._standardize_fia_types(df, table_name)

        return df.lazy() if lazy else df

    def _standardize_fia_types(self, df: pl.DataFrame, table_name: str) -> pl.DataFrame:
        """
        Standardize FIA-specific data types across backends.

        Args:
            df: DataFrame to standardize
            table_name: Name of the source table

        Returns:
            DataFrame with standardized types
        """
        # Handle CN fields consistently (always as strings)
        for col in df.columns:
            if col.endswith("_CN") or col == "CN":
                if df[col].dtype != pl.Utf8:
                    df = df.with_columns(pl.col(col).cast(pl.Utf8))

        # Handle EVALID as integer
        if "EVALID" in df.columns and df["EVALID"].dtype != pl.Int64:
            df = df.with_columns(pl.col("EVALID").cast(pl.Int64))

        # Handle state codes as integers
        if "STATECD" in df.columns and df["STATECD"].dtype != pl.Int64:
            df = df.with_columns(pl.col("STATECD").cast(pl.Int64))

        return df

    def read_filtered_data(
        self,
        table_name: str,
        filter_column: str,
        filter_values: List[Union[str, int]],
        columns: Optional[List[str]] = None,
        batch_size: int = 900,
    ) -> pl.DataFrame:
        """
        Read data filtered by a list of values with automatic batching.

        This method handles large IN clauses by batching them to avoid
        database limitations (especially important for SQLite).

        Args:
            table_name: Name of the table to read
            filter_column: Column to filter on
            filter_values: List of values to filter by
            columns: Optional list of columns to select
            batch_size: Size of each batch for IN clauses

        Returns:
            Combined DataFrame with all matching records
        """
        if not filter_values:
            return pl.DataFrame()

        dfs = []

        # Process in batches
        for i in range(0, len(filter_values), batch_size):
            batch = filter_values[i : i + batch_size]

            # Format values for SQL
            if isinstance(batch[0], str):
                value_str = ", ".join(f"'{v}'" for v in batch)
            else:
                value_str = ", ".join(str(v) for v in batch)

            where_clause = f"{filter_column} IN ({value_str})"

            df = self.read_table(
                table_name=table_name,
                columns=columns,
                where=where_clause,
                lazy=False,
            )

            if not df.is_empty():
                dfs.append(df)

        # Combine all batches
        if dfs:
            return pl.concat(dfs, how="diagonal")
        else:
            return pl.DataFrame()

    def read_evalid_data(
        self, evalid: Union[int, List[int]], include_pop_tables: bool = True
    ) -> Dict[str, pl.DataFrame]:
        """
        Read all data for specified EVALID(s).

        This is a high-level method that loads a complete set of FIA data
        filtered by evaluation ID, following the same pattern as the original
        FIADataReader.

        Args:
            evalid: Single EVALID or list of EVALIDs
            include_pop_tables: If True, include population tables

        Returns:
            Dictionary with all relevant tables
        """
        if isinstance(evalid, int):
            evalid = [evalid]

        result = {}

        # Read population tables if requested
        if include_pop_tables:
            # POP_EVAL
            result["pop_eval"] = self.read_filtered_data(
                "POP_EVAL", "EVALID", evalid
            )

            # POP_PLOT_STRATUM_ASSGN
            ppsa = self.read_filtered_data(
                "POP_PLOT_STRATUM_ASSGN",
                "EVALID",
                evalid,
                columns=["PLT_CN", "STRATUM_CN", "EVALID"],
            )
            result["pop_plot_stratum_assgn"] = ppsa

            if not ppsa.is_empty():
                # Get unique stratum CNs
                stratum_cns = ppsa["STRATUM_CN"].unique().to_list()
                result["pop_stratum"] = self.read_filtered_data(
                    "POP_STRATUM", "CN", stratum_cns
                )

                # Get estimation unit CNs
                if not result["pop_stratum"].is_empty():
                    estn_unit_cns = (
                        result["pop_stratum"]["ESTN_UNIT_CN"].unique().to_list()
                    )
                    result["pop_estn_unit"] = self.read_filtered_data(
                        "POP_ESTN_UNIT", "CN", estn_unit_cns
                    )
                else:
                    result["pop_estn_unit"] = pl.DataFrame()
            else:
                result["pop_stratum"] = pl.DataFrame()
                result["pop_estn_unit"] = pl.DataFrame()

        # Read plot data
        if include_pop_tables and not ppsa.is_empty():
            plot_cns = ppsa["PLT_CN"].unique().to_list()
            result["plot"] = self.read_filtered_data("PLOT", "CN", plot_cns)

            # Read associated tree and condition data
            if not result["plot"].is_empty():
                result["tree"] = self.read_filtered_data("TREE", "PLT_CN", plot_cns)
                result["cond"] = self.read_filtered_data("COND", "PLT_CN", plot_cns)
            else:
                result["tree"] = pl.DataFrame()
                result["cond"] = pl.DataFrame()
        else:
            result["plot"] = pl.DataFrame()
            result["tree"] = pl.DataFrame()
            result["cond"] = pl.DataFrame()

        return result

    def get_table_info(self, table_name: str) -> Dict[str, any]:
        """
        Get information about a table including schema and row count.

        Args:
            table_name: Name of the table

        Returns:
            Dictionary with table information
        """
        schema = self._interface.get_table_schema(table_name)

        # Get row count
        result = self._interface.execute_query(
            f"SELECT COUNT(*) as row_count FROM {table_name}"
        )
        row_count = result.data["row_count"][0]

        return {
            "table_name": table_name,
            "exists": self._interface.table_exists(table_name),
            "schema": schema,
            "column_count": len(schema),
            "row_count": row_count,
        }

    def list_tables(self) -> List[str]:
        """
        List all tables in the database.

        Returns:
            List of table names
        """
        # Query varies by backend
        if hasattr(self._interface, "_connection") and self._interface._connection:
            if "DuckDB" in type(self._interface).__name__:
                query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
            else:
                query = "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"

            result = self._interface.execute_query(query)
            return result.data.to_series(0).to_list()
        else:
            return []

    def execute_custom_query(
        self, query: str, params: Optional[Dict[str, any]] = None
    ) -> pl.DataFrame:
        """
        Execute a custom SQL query.

        Args:
            query: SQL query string
            params: Optional query parameters

        Returns:
            Query results as DataFrame
        """
        result = self._interface.execute_query(query, params=params)
        return result.data