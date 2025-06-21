"""
Optimized data reading utilities for pyFIA.

This module provides high-performance functions for reading FIA data
from SQLite or DuckDB databases using Polars lazy evaluation.
"""

import polars as pl
from pathlib import Path
from typing import Union, List, Optional, Dict, Tuple, overload, Literal
import sqlite3
import duckdb


class FIADataReader:
    """
    Optimized reader for FIA databases (SQLite and DuckDB).
    
    This class provides efficient methods for reading FIA data with:
    - Support for both SQLite and DuckDB backends
    - Lazy evaluation for memory efficiency
    - Column selection to minimize data transfer
    - Type-aware schema handling for FIA's VARCHAR CN fields
    """
    
    def __init__(self, db_path: Union[str, Path], engine: str = "sqlite"):
        """
        Initialize data reader.
        
        Args:
            db_path: Path to FIA database
            engine: Database engine ("sqlite" or "duckdb")
        """
        self.db_path = Path(db_path)
        if not self.db_path.exists():
            raise FileNotFoundError(f"Database not found: {db_path}")
        
        self.engine = engine.lower()
        if self.engine not in ["sqlite", "duckdb"]:
            raise ValueError(f"Unsupported engine: {engine}. Use 'sqlite' or 'duckdb'")
        
        # Cache for table schemas
        self._schemas: Dict[str, Dict[str, str]] = {}
        
        # DuckDB connection (kept open for performance)
        self._duckdb_conn = None
        if self.engine == "duckdb":
            self._duckdb_conn = duckdb.connect(str(self.db_path), read_only=True)
    
    def __del__(self):
        """Close DuckDB connection if open."""
        if self._duckdb_conn:
            self._duckdb_conn.close()
    
    def get_table_schema(self, table_name: str) -> Dict[str, str]:
        """
        Get schema information for a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Dictionary mapping column names to SQL types
        """
        if table_name in self._schemas:
            return self._schemas[table_name]
        
        if self.engine == "sqlite":
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(f"PRAGMA table_info({table_name})")
                schema = {row[1]: row[2] for row in cursor.fetchall()}
                self._schemas[table_name] = schema
        else:  # duckdb
            result = self._duckdb_conn.execute(f"DESCRIBE {table_name}").fetchall()
            schema = {row[0]: row[1] for row in result}
            self._schemas[table_name] = schema
            
        return schema
    
    @overload
    def read_table(self, 
                   table_name: str,
                   columns: Optional[List[str]] = None,
                   where: Optional[str] = None,
                   lazy: Literal[False] = False) -> pl.DataFrame: ...
    
    @overload
    def read_table(self, 
                   table_name: str,
                   columns: Optional[List[str]] = None,
                   where: Optional[str] = None,
                   lazy: Literal[True] = True) -> pl.LazyFrame: ...
    
    def read_table(self, 
                   table_name: str,
                   columns: Optional[List[str]] = None,
                   where: Optional[str] = None,
                   lazy: bool = True) -> Union[pl.DataFrame, pl.LazyFrame]:
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
        # Build query
        if columns:
            col_str = ", ".join(columns)
            query = f"SELECT {col_str} FROM {table_name}"
        else:
            query = f"SELECT * FROM {table_name}"
        
        if where:
            query += f" WHERE {where}"
        
        # Execute query based on engine
        if self.engine == "sqlite":
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                
                # Get column names and types
                col_names = [desc[0] for desc in cursor.description]
                
                # Fetch all rows
                rows = cursor.fetchall()
                
                if rows:
                    # Convert to dictionary format for better type handling
                    data_dict: Dict[str, List] = {col: [] for col in col_names}
                    for row in rows:
                        for i, val in enumerate(row):
                            data_dict[col_names[i]].append(val)
                    
                    # Create DataFrame with explicit null handling
                    df = pl.DataFrame(data_dict)
                else:
                    # Empty dataframe
                    df = pl.DataFrame(schema={col: pl.Object for col in col_names})
        else:  # duckdb
            # DuckDB can directly read into Polars DataFrame
            df = self._duckdb_conn.execute(query).pl()
        
        # Handle CN fields consistently
        schema = self.get_table_schema(table_name)
        for col in df.columns:
            if col.endswith('_CN') or col == 'CN':
                # In DuckDB, CN fields might be BIGINT - convert to string for consistency
                if self.engine == "duckdb":
                    df = df.with_columns(pl.col(col).cast(pl.Utf8))
                elif schema.get(col, '').startswith('VARCHAR'):
                    df = df.with_columns(pl.col(col).cast(pl.Utf8))
        
        return df.lazy() if lazy else df
    
    def read_plot_data(self, evalid: List[int]) -> pl.DataFrame:
        """
        Read PLOT data filtered by EVALID.
        
        Args:
            evalid: List of EVALID values to filter by
            
        Returns:
            DataFrame with plot data
        """
        # First get plot CNs from assignments
        evalid_str = ", ".join(str(e) for e in evalid)
        ppsa = self.read_table(
            'POP_PLOT_STRATUM_ASSGN',
            columns=['PLT_CN', 'STRATUM_CN', 'EVALID'],
            where=f"EVALID IN ({evalid_str})",
            lazy=False
        )
        
        # Get unique plot CNs
        plot_cns = ppsa.select('PLT_CN').unique()['PLT_CN'].to_list()
        
        # Read plots
        if plot_cns:
            # SQLite has limits on IN clause size, so batch if needed
            batch_size = 900
            plot_dfs = []
            
            for i in range(0, len(plot_cns), batch_size):
                batch = plot_cns[i:i + batch_size]
                cn_str = ", ".join(f"'{cn}'" for cn in batch)
                
                df = self.read_table(
                    'PLOT',
                    where=f"CN IN ({cn_str})",
                    lazy=False
                )
                plot_dfs.append(df)
            
            plots = pl.concat(plot_dfs, how="diagonal") if plot_dfs else pl.DataFrame()
        else:
            plots = pl.DataFrame()
        
        # Add EVALID information
        if not plots.is_empty():
            plots = plots.join(
                ppsa.select(['PLT_CN', 'STRATUM_CN', 'EVALID']),
                left_on='CN',
                right_on='PLT_CN',
                how='left'
            )
        
        return plots
    
    def read_tree_data(self, plot_cns: List[str]) -> pl.DataFrame:
        """
        Read TREE data for specified plots.
        
        Args:
            plot_cns: List of plot CNs to get trees for
            
        Returns:
            DataFrame with tree data
        """
        if not plot_cns:
            return pl.DataFrame()
        
        # Batch process due to SQLite IN clause limits
        batch_size = 900
        tree_dfs = []
        
        for i in range(0, len(plot_cns), batch_size):
            batch = plot_cns[i:i + batch_size]
            cn_str = ", ".join(f"'{cn}'" for cn in batch)
            
            df = self.read_table(
                'TREE',
                where=f"PLT_CN IN ({cn_str})",
                lazy=False
            )
            tree_dfs.append(df)
        
        return pl.concat(tree_dfs, how="diagonal") if tree_dfs else pl.DataFrame()
    
    def read_cond_data(self, plot_cns: List[str]) -> pl.DataFrame:
        """
        Read COND data for specified plots.
        
        Args:
            plot_cns: List of plot CNs to get conditions for
            
        Returns:
            DataFrame with condition data
        """
        if not plot_cns:
            return pl.DataFrame()
        
        # Batch process due to SQLite IN clause limits
        batch_size = 900
        cond_dfs = []
        
        for i in range(0, len(plot_cns), batch_size):
            batch = plot_cns[i:i + batch_size]
            cn_str = ", ".join(f"'{cn}'" for cn in batch)
            
            df = self.read_table(
                'COND',
                where=f"PLT_CN IN ({cn_str})",
                lazy=False
            )
            cond_dfs.append(df)
        
        return pl.concat(cond_dfs, how="diagonal") if cond_dfs else pl.DataFrame()
    
    def read_pop_tables(self, evalid: List[int]) -> Dict[str, pl.DataFrame]:
        """
        Read population estimation tables for specified EVALIDs.
        
        Args:
            evalid: List of EVALID values
            
        Returns:
            Dictionary with population tables
        """
        evalid_str = ", ".join(str(e) for e in evalid)
        
        # Read POP_EVAL
        pop_eval = self.read_table(
            'POP_EVAL',
            where=f"EVALID IN ({evalid_str})",
            lazy=False
        )
        
        # Read POP_PLOT_STRATUM_ASSGN
        ppsa = self.read_table(
            'POP_PLOT_STRATUM_ASSGN',
            where=f"EVALID IN ({evalid_str})",
            lazy=False
        )
        
        # Get unique stratum CNs
        if not ppsa.is_empty():
            stratum_cns = ppsa.select('STRATUM_CN').unique()['STRATUM_CN'].to_list()
            stratum_cn_str = ", ".join(f"'{cn}'" for cn in stratum_cns)
            
            # Read POP_STRATUM
            pop_stratum = self.read_table(
                'POP_STRATUM',
                where=f"CN IN ({stratum_cn_str})",
                lazy=False
            )
            
            # Get estimation unit CNs
            estn_unit_cns = pop_stratum.select('ESTN_UNIT_CN').unique()['ESTN_UNIT_CN'].to_list()
            estn_unit_cn_str = ", ".join(f"'{cn}'" for cn in estn_unit_cns)
            
            # Read POP_ESTN_UNIT
            pop_estn_unit = self.read_table(
                'POP_ESTN_UNIT',
                where=f"CN IN ({estn_unit_cn_str})",
                lazy=False
            )
        else:
            pop_stratum = pl.DataFrame()
            pop_estn_unit = pl.DataFrame()
        
        return {
            'pop_eval': pop_eval,
            'pop_plot_stratum_assgn': ppsa,
            'pop_stratum': pop_stratum,
            'pop_estn_unit': pop_estn_unit
        }
    
    def read_evalid_data(self, evalid: Union[int, List[int]]) -> Dict[str, pl.DataFrame]:
        """
        Read all data for specified EVALID(s).
        
        This is the main method for loading a complete set of FIA data
        filtered by evaluation ID.
        
        Args:
            evalid: Single EVALID or list of EVALIDs
            
        Returns:
            Dictionary with all relevant tables
        """
        if isinstance(evalid, int):
            evalid = [evalid]
        
        # Read population tables first
        pop_tables = self.read_pop_tables(evalid)
        
        # Read plot data
        plots = self.read_plot_data(evalid)
        plot_cns = plots['CN'].to_list() if not plots.is_empty() else []
        
        # Read associated data
        trees = self.read_tree_data(plot_cns)
        conds = self.read_cond_data(plot_cns)
        
        return {
            'plot': plots,
            'tree': trees,
            'cond': conds,
            **pop_tables
        }