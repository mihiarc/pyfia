"""
Optimized data reading utilities for pyFIA.

This module provides high-performance functions for reading FIA data
from DuckDB and SQLite databases using Polars lazy evaluation.
"""

import logging
from pathlib import Path
from typing import Dict, List, Literal, Optional, Union, overload

import polars as pl

from .backends import DatabaseBackend, create_backend

logger = logging.getLogger(__name__)


class FIADataReader:
    """
    Optimized reader for FIA databases.

    This class provides efficient methods for reading FIA data with:
    - Support for both DuckDB and SQLite backends
    - Lazy evaluation for memory efficiency
    - Column selection to minimize data transfer
    - Type-aware schema handling for FIA's VARCHAR CN fields
    - Automatic database type detection
    """

    def __init__(
        self, db_path: Union[str, Path], engine: Optional[str] = None, **backend_kwargs
    ):
        """
        Initialize data reader.

        Parameters
        ----------
        db_path : str or Path
            Path to FIA database (DuckDB or SQLite).
        engine : str, optional
            Database engine ('duckdb' or 'sqlite'). If None, auto-detect.
        **backend_kwargs
            Additional backend-specific options:
                - For DuckDB: read_only, memory_limit, threads
                - For SQLite: timeout, check_same_thread
        """
        self.db_path = Path(db_path)
        if not self.db_path.exists():
            raise FileNotFoundError(f"Database not found: {db_path}")

        # Create backend using factory function
        self._backend: DatabaseBackend = create_backend(
            db_path, engine=engine, **backend_kwargs
        )

        # Connect to database
        self._backend.connect()

        # Cache for table schemas (delegate to backend)
        self._schemas = self._backend._schema_cache

    def __del__(self):
        """Close database connection if open."""
        try:
            if hasattr(self, "_backend") and self._backend:
                self._backend.disconnect()
        except Exception:
            # Avoid raising during garbage collection
            pass

    def get_table_schema(self, table_name: str) -> Dict[str, str]:
        """
        Get schema information for a table.

        Parameters
        ----------
        table_name : str
            Name of the table.

        Returns
        -------
        dict of str to str
            Dictionary mapping column names to SQL types.
        """
        return self._backend.get_table_schema(table_name)

    def _is_cn_column(self, column_name: str) -> bool:
        """
        Check if a column is a CN (Control Number) field.

        Parameters
        ----------
        column_name : str
            Name of the column.

        Returns
        -------
        bool
            True if the column is a CN field.
        """
        return self._backend.is_cn_column(column_name)  # type: ignore[attr-defined, no-any-return]

    def _is_string_column(self, table_name: str, column_name: str) -> bool:
        """
        Check if a column should be treated as a string.

        Parameters
        ----------
        table_name : str
            Name of the table.
        column_name : str
            Name of the column.

        Returns
        -------
        bool
            True if the column should be treated as a string.
        """
        return self._backend.is_string_column(table_name, column_name)  # type: ignore[attr-defined, no-any-return]

    def _is_float_column(self, table_name: str, column_name: str) -> bool:
        """
        Check if a column should be treated as a floating point number.

        Parameters
        ----------
        table_name : str
            Name of the table.
        column_name : str
            Name of the column.

        Returns
        -------
        bool
            True if the column should be treated as a float.
        """
        return self._backend.is_float_column(table_name, column_name)  # type: ignore[attr-defined, no-any-return]

    def _is_integer_column(self, table_name: str, column_name: str) -> bool:
        """
        Check if a column should be treated as an integer.

        Parameters
        ----------
        table_name : str
            Name of the table.
        column_name : str
            Name of the column.

        Returns
        -------
        bool
            True if the column should be treated as an integer.
        """
        return self._backend.is_integer_column(table_name, column_name)  # type: ignore[attr-defined, no-any-return]

    def _build_select_clause(
        self, table_name: str, columns: Optional[List[str]] = None
    ) -> str:
        """
        Build SELECT clause with appropriate type casting.

        Parameters
        ----------
        table_name : str
            Name of the table.
        columns : list of str, optional
            Optional list of columns to select.

        Returns
        -------
        str
            SELECT clause with type casting.
        """
        return self._backend.build_select_clause(table_name, columns)  # type: ignore[attr-defined, no-any-return]

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
        lazy: bool = True,
    ) -> Union[pl.DataFrame, pl.LazyFrame]:
        """
        Read a table from the FIA database.

        Parameters
        ----------
        table_name : str
            Name of the table to read.
        columns : list of str, optional
            Optional list of columns to select.
        where : str, optional
            Optional WHERE clause (without 'WHERE' keyword).
        lazy : bool, default True
            If True, return LazyFrame; if False, return DataFrame.

        Returns
        -------
        pl.DataFrame or pl.LazyFrame
            Polars DataFrame or LazyFrame.
        """
        # Build SELECT clause with appropriate type casting
        select_clause = self._build_select_clause(table_name, columns)
        query = f"SELECT {select_clause} FROM {table_name}"

        if where:
            query += f" WHERE {where}"

        # Execute query using backend
        df: pl.DataFrame = self._backend.read_dataframe(query)  # type: ignore[attr-defined]

        # Post-process to convert types
        for col in df.columns:
            if self._is_integer_column(table_name, col):
                # Convert integer columns back to Int64
                try:
                    df = df.with_columns(pl.col(col).cast(pl.Int64))
                except (
                    pl.exceptions.ComputeError,
                    pl.exceptions.InvalidOperationError,
                ):
                    pass  # Keep as string if conversion fails

        # Return as lazy frame or DataFrame based on lazy parameter
        if lazy:
            return df.lazy()
        return df

    def read_plot_data(self, evalid: List[int]) -> pl.DataFrame:
        """
        Read PLOT data filtered by EVALID.

        Parameters
        ----------
        evalid : list of int
            List of EVALID values to filter by.

        Returns
        -------
        pl.DataFrame
            DataFrame with plot data.
        """
        # First get plot CNs from assignments
        evalid_str = ", ".join(str(e) for e in evalid)
        ppsa = self.read_table(
            "POP_PLOT_STRATUM_ASSGN",
            columns=["PLT_CN", "STRATUM_CN", "EVALID"],
            where=f"EVALID IN ({evalid_str})",
            lazy=True,
        )

        # Get unique plot CNs
        plot_cns = ppsa.select("PLT_CN").unique().collect()["PLT_CN"].to_list()

        # Read plots
        if plot_cns:
            # SQLite has limits on IN clause size, so batch if needed
            batch_size = 900
            plot_dfs = []

            for i in range(0, len(plot_cns), batch_size):
                batch = plot_cns[i : i + batch_size]
                cn_str = ", ".join(f"'{cn}'" for cn in batch)

                df = self.read_table("PLOT", where=f"CN IN ({cn_str})", lazy=True)
                plot_dfs.append(df)

            plots = pl.concat(plot_dfs).collect() if plot_dfs else pl.DataFrame()
        else:
            plots = pl.DataFrame()

        # Add EVALID information
        if not plots.is_empty():
            plots = (
                plots.lazy()
                .join(
                    ppsa.select(["PLT_CN", "STRATUM_CN", "EVALID"]),
                    left_on="CN",
                    right_on="PLT_CN",
                    how="left",
                )
                .collect()
            )

        return plots

    def read_tree_data(self, plot_cns: List[str]) -> pl.DataFrame:
        """
        Read TREE data for specified plots.

        Parameters
        ----------
        plot_cns : list of str
            List of plot CNs to get trees for.

        Returns
        -------
        pl.DataFrame
            DataFrame with tree data.
        """
        if not plot_cns:
            return pl.DataFrame()

        # Batch process due to SQLite IN clause limits
        batch_size = 900
        tree_dfs = []

        for i in range(0, len(plot_cns), batch_size):
            batch = plot_cns[i : i + batch_size]
            cn_str = ", ".join(f"'{cn}'" for cn in batch)

            df = self.read_table("TREE", where=f"PLT_CN IN ({cn_str})", lazy=True)
            tree_dfs.append(df)

        return pl.concat(tree_dfs).collect() if tree_dfs else pl.DataFrame()

    def read_cond_data(self, plot_cns: List[str]) -> pl.DataFrame:
        """
        Read COND data for specified plots.

        Parameters
        ----------
        plot_cns : list of str
            List of plot CNs to get conditions for.

        Returns
        -------
        pl.DataFrame
            DataFrame with condition data.
        """
        if not plot_cns:
            return pl.DataFrame()

        # Batch process due to SQLite IN clause limits
        batch_size = 900
        cond_dfs = []

        for i in range(0, len(plot_cns), batch_size):
            batch = plot_cns[i : i + batch_size]
            cn_str = ", ".join(f"'{cn}'" for cn in batch)

            df = self.read_table("COND", where=f"PLT_CN IN ({cn_str})", lazy=True)
            cond_dfs.append(df)

        return pl.concat(cond_dfs).collect() if cond_dfs else pl.DataFrame()

    def read_pop_tables(self, evalid: List[int]) -> Dict[str, pl.DataFrame]:
        """
        Read population estimation tables for specified EVALIDs.

        Parameters
        ----------
        evalid : list of int
            List of EVALID values.

        Returns
        -------
        dict of str to pl.DataFrame
            Dictionary with population tables.
        """
        evalid_str = ", ".join(str(e) for e in evalid)

        # Read POP_EVAL
        pop_eval = self.read_table(
            "POP_EVAL", where=f"EVALID IN ({evalid_str})", lazy=True
        ).collect()

        # Read POP_PLOT_STRATUM_ASSGN
        ppsa = self.read_table(
            "POP_PLOT_STRATUM_ASSGN", where=f"EVALID IN ({evalid_str})", lazy=True
        ).collect()

        # Get unique stratum CNs
        if not ppsa.is_empty():
            stratum_cns = ppsa.select("STRATUM_CN").unique()["STRATUM_CN"].to_list()
            stratum_cn_str = ", ".join(f"'{cn}'" for cn in stratum_cns)

            # Read POP_STRATUM
            pop_stratum = self.read_table(
                "POP_STRATUM", where=f"CN IN ({stratum_cn_str})", lazy=True
            ).collect()

            # Get estimation unit CNs
            estn_unit_cns = (
                pop_stratum.select("ESTN_UNIT_CN").unique()["ESTN_UNIT_CN"].to_list()
            )
            estn_unit_cn_str = ", ".join(f"'{cn}'" for cn in estn_unit_cns)

            # Read POP_ESTN_UNIT
            pop_estn_unit = self.read_table(
                "POP_ESTN_UNIT", where=f"CN IN ({estn_unit_cn_str})", lazy=True
            ).collect()
        else:
            pop_stratum = pl.DataFrame()
            pop_estn_unit = pl.DataFrame()

        return {
            "pop_eval": pop_eval,
            "pop_plot_stratum_assgn": ppsa,
            "pop_stratum": pop_stratum,
            "pop_estn_unit": pop_estn_unit,
        }

    def read_evalid_data(
        self, evalid: Union[int, List[int]]
    ) -> Dict[str, pl.DataFrame]:
        """
        Read all data for specified EVALID(s).

        This is the main method for loading a complete set of FIA data
        filtered by evaluation ID.

        Parameters
        ----------
        evalid : int or list of int
            Single EVALID or list of EVALIDs.

        Returns
        -------
        dict of str to pl.DataFrame
            Dictionary with all relevant tables.
        """
        if isinstance(evalid, int):
            evalid = [evalid]

        # Read population tables first
        pop_tables = self.read_pop_tables(evalid)

        # Read plot data
        plots = self.read_plot_data(evalid)
        plot_cns = plots["CN"].to_list() if not plots.is_empty() else []

        # Read associated data
        trees = self.read_tree_data(plot_cns)
        conds = self.read_cond_data(plot_cns)

        return {"plot": plots, "tree": trees, "cond": conds, **pop_tables}
