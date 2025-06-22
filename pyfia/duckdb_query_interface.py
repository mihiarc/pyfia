"""
DuckDB query interface for pyFIA with LangChain integration support.

This module provides a query interface designed for natural language queries
and integration with LangChain agents.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import duckdb
import polars as pl


@dataclass
class TableInfo:
    """Information about a database table."""
    name: str
    columns: List[Dict[str, str]]
    row_count: int
    description: Optional[str] = None
    sample_data: Optional[pl.DataFrame] = None


class DuckDBQueryInterface:
    """
    Query interface for DuckDB FIA databases optimized for LangChain integration.

    This class provides:
    - Natural language-friendly metadata access
    - Query validation and safety features
    - Schema introspection for LLM context
    - Query result formatting for agents
    """

    def __init__(self, db_path: Union[str, Path]):
        """
        Initialize DuckDB query interface.

        Args:
            db_path: Path to DuckDB database
        """
        self.db_path = Path(db_path)
        if not self.db_path.exists():
            raise FileNotFoundError(f"Database not found: {db_path}")

        # Keep connection open for performance
        self.conn = duckdb.connect(str(self.db_path), read_only=True)

        # Cache for table metadata
        self._table_info_cache: Dict[str, TableInfo] = {}

        # FIA table descriptions for LLM context
        self.table_descriptions = {
            "PLOT": "Forest inventory plot locations and measurements",
            "TREE": "Individual tree measurements including species, diameter, height",
            "COND": "Forest condition class data including forest type and stand age",
            "POP_EVAL": "Population evaluation definitions with temporal boundaries",
            "POP_STRATUM": "Stratification and expansion factors for statistical estimation",
            "POP_PLOT_STRATUM_ASSGN": "Links plots to evaluations via strata",
            "POP_ESTN_UNIT": "Estimation unit definitions and areas",
            "REF_SPECIES": "Reference table for tree species codes and names",
            "REF_FOREST_TYPE": "Reference table for forest type codes and descriptions"
        }

    def __del__(self):
        """Close database connection."""
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()

    def get_database_schema(self) -> Dict[str, TableInfo]:
        """
        Get complete database schema with table metadata.

        Returns:
            Dictionary mapping table names to TableInfo objects
        """
        # Get all tables
        tables = self.conn.execute("SHOW TABLES").pl()['name'].to_list()

        schema_info = {}
        for table in tables:
            if table not in self._table_info_cache:
                self._table_info_cache[table] = self._get_table_info(table)
            schema_info[table] = self._table_info_cache[table]

        return schema_info

    def _get_table_info(self, table_name: str) -> TableInfo:
        """Get detailed information about a table."""
        # Get column information
        columns_df = self.conn.execute(f"DESCRIBE {table_name}").pl()
        columns = [
            {
                "name": row[0],
                "type": row[1],
                "nullable": row[2] == "YES"
            }
            for row in columns_df.iter_rows()
        ]

        # Get row count
        row_count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]

        # Get sample data (first 5 rows)
        sample_data = None
        if row_count > 0:
            sample_data = self.conn.execute(f"SELECT * FROM {table_name} LIMIT 5").pl()

        return TableInfo(
            name=table_name,
            columns=columns,
            row_count=row_count,
            description=self.table_descriptions.get(table_name),
            sample_data=sample_data
        )

    def get_table_summary(self, table_name: str) -> str:
        """
        Get a natural language summary of a table for LLM context.

        Args:
            table_name: Name of the table

        Returns:
            Natural language description of the table
        """
        info = self._get_table_info(table_name)

        summary = f"Table '{table_name}'"
        if info.description:
            summary += f": {info.description}"
        summary += f"\n- Rows: {info.row_count:,}"
        summary += f"\n- Columns ({len(info.columns)}):"

        for col in info.columns[:10]:  # Show first 10 columns
            summary += f"\n  - {col['name']} ({col['type']})"

        if len(info.columns) > 10:
            summary += f"\n  ... and {len(info.columns) - 10} more columns"

        return summary

    def execute_query(self, query: str, limit: Optional[int] = None) -> pl.DataFrame:
        """
        Execute a SQL query safely.

        Args:
            query: SQL query to execute
            limit: Optional row limit for safety

        Returns:
            Query results as Polars DataFrame
        """
        # Add limit if specified and not already in query
        if limit and "LIMIT" not in query.upper():
            query = f"{query} LIMIT {limit}"

        try:
            result = self.conn.execute(query).pl()
            return result
        except Exception as e:
            raise ValueError(f"Query execution failed: {str(e)}")

    def validate_query(self, query: str) -> Dict[str, Any]:
        """
        Validate a SQL query without executing it.

        Args:
            query: SQL query to validate

        Returns:
            Dictionary with validation results
        """
        try:
            # Use EXPLAIN to validate query
            self.conn.execute(f"EXPLAIN {query}")
            return {
                "valid": True,
                "message": "Query is valid",
                "estimated_cost": None  # Could parse EXPLAIN output for cost
            }
        except Exception as e:
            return {
                "valid": False,
                "message": str(e),
                "estimated_cost": None
            }

    def get_evalid_info(self, evalid: Optional[int] = None) -> pl.DataFrame:
        """
        Get evaluation information for understanding data organization.

        Args:
            evalid: Optional specific EVALID to filter by

        Returns:
            DataFrame with evaluation information
        """
        query = """
        SELECT
            pe.EVALID,
            pe.EVAL_DESCR,
            pe.STATECD,
            pe.START_INVYR,
            pe.END_INVYR,
            pet.EVAL_TYP,
            COUNT(DISTINCT ppsa.PLT_CN) as plot_count
        FROM POP_EVAL pe
        LEFT JOIN POP_EVAL_TYP pet ON pe.CN = pet.EVAL_CN
        LEFT JOIN POP_PLOT_STRATUM_ASSGN ppsa ON pe.EVALID = ppsa.EVALID
        """

        if evalid:
            query += f" WHERE pe.EVALID = {evalid}"

        query += " GROUP BY pe.EVALID, pe.EVAL_DESCR, pe.STATECD, pe.START_INVYR, pe.END_INVYR, pet.EVAL_TYP"
        query += " ORDER BY pe.EVALID DESC"

        return self.execute_query(query)

    def get_natural_language_context(self) -> str:
        """
        Get database context formatted for LLM understanding.

        Returns:
            Natural language description of database structure
        """
        context = "FIA Database Structure:\n\n"

        # Add key concept explanations
        context += "Key Concepts:\n"
        context += "- EVALID: Evaluation ID that groups statistically valid plot measurements\n"
        context += "- CN: Control Number - unique identifier for records (stored as VARCHAR)\n"
        context += "- Plots contain Trees and Conditions\n"
        context += "- Population tables (POP_*) contain stratification and expansion factors\n\n"

        # Add table summaries
        context += "Main Tables:\n"
        priority_tables = ["PLOT", "TREE", "COND", "POP_EVAL", "POP_STRATUM"]

        for table in priority_tables:
            try:
                context += f"\n{self.get_table_summary(table)}\n"
            except:
                pass

        return context

    def format_results_for_llm(self, df: pl.DataFrame, max_rows: int = 10) -> str:
        """
        Format query results for LLM consumption.

        Args:
            df: Results DataFrame
            max_rows: Maximum rows to include

        Returns:
            Formatted string representation
        """
        if df.is_empty():
            return "No results found."

        # Get shape info
        result = f"Results: {len(df)} rows, {len(df.columns)} columns\n\n"

        # Show column info
        result += "Columns: " + ", ".join(df.columns) + "\n\n"

        # Show data sample
        sample_df = df.head(max_rows)
        result += "Data sample:\n"
        result += str(sample_df)

        if len(df) > max_rows:
            result += f"\n\n... and {len(df) - max_rows} more rows"

        return result


# Helper function for LangChain tool creation
def create_duckdb_query_tool(db_path: str):
    """
    Create a LangChain-compatible tool for querying DuckDB.

    Args:
        db_path: Path to DuckDB database

    Returns:
        Function that can be used as a LangChain tool
    """
    interface = DuckDBQueryInterface(db_path)

    def query_fia_database(query: str, limit: int = 100) -> str:
        """
        Execute SQL query on FIA DuckDB database.

        Args:
            query: SQL query to execute
            limit: Maximum rows to return (default: 100)

        Returns:
            Formatted query results
        """
        try:
            # Validate query first
            validation = interface.validate_query(query)
            if not validation["valid"]:
                return f"Query validation failed: {validation['message']}"

            # Execute query
            results = interface.execute_query(query, limit=limit)

            # Format results
            return interface.format_results_for_llm(results)
        except Exception as e:
            return f"Query execution failed: {str(e)}"

    # Add metadata for LangChain
    query_fia_database.description = f"""
    Query the FIA DuckDB database using SQL.

    Database context:
    {interface.get_natural_language_context()}

    Example queries:
    - SELECT COUNT(*) FROM TREE WHERE SPCD = 316  -- Count loblolly pine trees
    - SELECT EVALID, COUNT(*) as plot_count FROM POP_PLOT_STRATUM_ASSGN GROUP BY EVALID
    - SELECT * FROM PLOT WHERE STATECD = 37 LIMIT 10  -- Get NC plots
    """

    return query_fia_database
