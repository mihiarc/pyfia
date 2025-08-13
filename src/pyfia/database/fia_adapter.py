"""
Adapter to integrate the new database interface with the existing FIA class.

This module shows how the existing FIA class can be adapted to use the
flexible database interface while maintaining backward compatibility.
"""

import warnings
from pathlib import Path
from typing import Dict, List, Optional, Union

import polars as pl

from ..core.fia import FIA
from .enhanced_reader import EnhancedFIADataReader


class FIAWithInterface(FIA):
    """
    Extended FIA class that uses the flexible database interface.

    This class demonstrates how to integrate the new database interface
    while maintaining compatibility with existing code.
    """

    def __init__(self, db_path: Union[str, Path], engine: Optional[str] = None):
        """
        Initialize FIA database with flexible backend support.

        Args:
            db_path: Path to FIA database (DuckDB or SQLite)
            engine: Database engine ('duckdb' or 'sqlite'). Auto-detected if None.
        """
        # Initialize parent class
        super().__init__(db_path, engine="duckdb")  # Parent always expects duckdb

        # Override the reader with our enhanced version
        self._reader = EnhancedFIADataReader(db_path, engine=engine)
        self._actual_engine = self._reader.engine or "auto-detected"

    def __enter__(self):
        """Context manager entry."""
        self._reader.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self._reader.__exit__(exc_type, exc_val, exc_tb)

    def load_table(
        self, table_name: str, columns: Optional[List[str]] = None
    ) -> pl.LazyFrame:
        """
        Load a table from the FIA database as a lazy frame.

        This method overrides the parent to use the flexible interface.

        Args:
            table_name: Name of the FIA table to load
            columns: Optional list of columns to load (None loads all)

        Returns:
            Polars LazyFrame of the table
        """
        # Build WHERE clause if state filter is active
        where_clause = None
        if self.state_filter and table_name in ["PLOT", "COND", "TREE"]:
            state_list = ", ".join(str(s) for s in self.state_filter)
            where_clause = f"STATECD IN ({state_list})"

        # Use the enhanced reader
        df = self._reader.read_table(
            table_name,
            columns=columns,
            where=where_clause,
            lazy=True,  # Always return lazy for compatibility
        )

        # Store in tables cache
        self.tables[table_name] = df

        return df

    def get_database_info(self) -> Dict[str, any]:
        """
        Get information about the connected database.

        Returns:
            Dictionary with database information
        """
        tables = self._reader.list_tables()
        
        info = {
            "path": str(self.db_path),
            "engine": self._actual_engine,
            "table_count": len(tables),
            "tables": tables,
        }

        # Get row counts for main tables
        main_tables = ["PLOT", "TREE", "COND", "POP_EVAL"]
        row_counts = {}
        
        for table in main_tables:
            if table in tables:
                table_info = self._reader.get_table_info(table)
                row_counts[table] = table_info["row_count"]
        
        info["row_counts"] = row_counts
        
        return info

    def execute_custom_query(
        self, query: str, params: Optional[Dict[str, any]] = None
    ) -> pl.DataFrame:
        """
        Execute a custom SQL query on the database.

        Args:
            query: SQL query string
            params: Optional query parameters

        Returns:
            Query results as DataFrame
        """
        return self._reader.execute_custom_query(query, params)

    def check_backend_compatibility(self) -> Dict[str, bool]:
        """
        Check which operations are supported by the current backend.

        Returns:
            Dictionary of feature support flags
        """
        backend_type = type(self._reader.interface).__name__

        # All backends support basic operations
        features = {
            "basic_queries": True,
            "parameterized_queries": True,
            "transactions": True,
            "lazy_evaluation": True,
        }

        # Backend-specific features
        if "DuckDB" in backend_type:
            features.update({
                "spatial_functions": True,
                "window_functions": True,
                "advanced_aggregations": True,
                "parallel_execution": True,
            })
        elif "SQLite" in backend_type:
            features.update({
                "spatial_functions": False,  # Unless spatialite is loaded
                "window_functions": True,  # SQLite 3.25+
                "advanced_aggregations": True,
                "parallel_execution": False,
            })

        return features


def migrate_to_interface(fia_instance: FIA) -> FIAWithInterface:
    """
    Helper function to migrate an existing FIA instance to use the interface.

    Args:
        fia_instance: Existing FIA instance

    Returns:
        New FIAWithInterface instance with same settings
    """
    # Create new instance
    new_instance = FIAWithInterface(fia_instance.db_path)

    # Copy over state
    new_instance.evalid = fia_instance.evalid
    new_instance.most_recent = fia_instance.most_recent
    new_instance.state_filter = fia_instance.state_filter
    new_instance.tables = fia_instance.tables.copy()

    return new_instance


# Example usage functions
def example_basic_usage():
    """Example of basic usage with the interface-enabled FIA class."""
    # Works with both SQLite and DuckDB
    with FIAWithInterface("path/to/fia.db") as db:
        # Get database info
        info = db.get_database_info()
        print(f"Connected to {info['engine']} database with {info['table_count']} tables")

        # Standard FIA operations work as before
        db.clip_by_state(37, most_recent=True)
        tpa_results = db.tpa(treeDomain="STATUSCD == 1")

        # New capability: custom queries
        custom_results = db.execute_custom_query(
            """
            SELECT STATECD, COUNT(DISTINCT PLT_CN) as plot_count
            FROM TREE
            WHERE DIA > :min_dia
            GROUP BY STATECD
            """,
            params={"min_dia": 20.0}
        )

        return tpa_results, custom_results


def example_backend_specific():
    """Example showing backend-specific optimizations."""
    db_path = "path/to/fia.duckdb"
    
    with FIAWithInterface(db_path, engine="duckdb") as db:
        # Check supported features
        features = db.check_backend_compatibility()
        
        if features["spatial_functions"]:
            # Use spatial functions if available
            results = db.execute_custom_query(
                """
                SELECT 
                    STATECD,
                    COUNT(*) as plot_count,
                    ST_Envelope(ST_Union(ST_Point(LON, LAT))) as bbox
                FROM PLOT
                WHERE LON IS NOT NULL AND LAT IS NOT NULL
                GROUP BY STATECD
                """
            )
        else:
            # Fallback for backends without spatial support
            results = db.execute_custom_query(
                """
                SELECT 
                    STATECD,
                    COUNT(*) as plot_count,
                    MIN(LON) as min_lon,
                    MAX(LON) as max_lon,
                    MIN(LAT) as min_lat,
                    MAX(LAT) as max_lat
                FROM PLOT
                WHERE LON IS NOT NULL AND LAT IS NOT NULL
                GROUP BY STATECD
                """
            )
        
        return results