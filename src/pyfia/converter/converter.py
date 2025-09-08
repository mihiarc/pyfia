"""
SQLite to DuckDB converter for FIA databases.

Simple, straightforward implementation that leverages DuckDB's native
sqlite_scanner extension for efficient conversion without unnecessary abstractions.
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import duckdb
import sqlite3

logger = logging.getLogger(__name__)


def convert_sqlite_to_duckdb(
    source_path: Path,
    target_path: Path,
    state_code: Optional[int] = None,
    append: bool = False,
    tables: Optional[List[str]] = None,
    show_progress: bool = False
) -> Dict[str, int]:
    """
    Convert SQLite FIA database to DuckDB format.
    
    This function leverages DuckDB's native sqlite_scanner extension for
    efficient data transfer with automatic compression.
    
    Parameters
    ----------
    source_path : Path
        Path to source SQLite database
    target_path : Path
        Path to target DuckDB database
    state_code : Optional[int]
        State FIPS code to add as column (useful for multi-state databases)
    append : bool
        If True, append to existing DuckDB database
    tables : Optional[List[str]]
        Specific tables to convert (None = all tables)
    show_progress : bool
        Show progress messages
        
    Returns
    -------
    Dict[str, int]
        Dictionary mapping table names to row counts
        
    Examples
    --------
    >>> # Convert single state
    >>> convert_sqlite_to_duckdb(
    ...     Path("NC_FIA.db"),
    ...     Path("north_carolina.duckdb"),
    ...     state_code=37
    ... )
    
    >>> # Append another state
    >>> convert_sqlite_to_duckdb(
    ...     Path("SC_FIA.db"),
    ...     Path("carolinas.duckdb"),
    ...     state_code=45,
    ...     append=True
    ... )
    """
    # Validate source exists
    if not source_path.exists():
        raise FileNotFoundError(f"Source SQLite file not found: {source_path}")
    
    # Determine connection mode
    if append and target_path.exists():
        mode = "r+"
        logger.info(f"Appending to existing DuckDB: {target_path}")
    else:
        mode = "w"
        logger.info(f"Creating new DuckDB: {target_path}")
    
    # Connect to DuckDB
    conn = duckdb.connect(str(target_path), read_only=False)
    
    try:
        # Install and load sqlite_scanner extension
        conn.execute("INSTALL sqlite_scanner")
        conn.execute("LOAD sqlite_scanner")
        
        # Get list of tables from SQLite
        if tables is None:
            sqlite_conn = sqlite3.connect(str(source_path))
            cursor = sqlite_conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
            )
            tables = [row[0] for row in cursor.fetchall()]
            sqlite_conn.close()
            
            if show_progress:
                print(f"Found {len(tables)} tables to convert")
        
        # Track row counts
        row_counts = {}
        
        # Convert each table
        for table_name in tables:
            if show_progress:
                print(f"Converting table: {table_name}")
            
            try:
                if append and _table_exists(conn, table_name):
                    # Append to existing table
                    if state_code is not None:
                        # Add state code column during append
                        conn.execute(f"""
                            INSERT INTO {table_name}
                            SELECT *, {state_code} AS STATE_ADDED
                            FROM sqlite_scan('{source_path}', '{table_name}')
                        """)
                    else:
                        conn.execute(f"""
                            INSERT INTO {table_name}
                            SELECT * FROM sqlite_scan('{source_path}', '{table_name}')
                        """)
                else:
                    # Create new table
                    if state_code is not None:
                        # Add state code column during creation
                        conn.execute(f"""
                            CREATE TABLE {table_name} AS
                            SELECT *, {state_code} AS STATE_ADDED
                            FROM sqlite_scan('{source_path}', '{table_name}')
                        """)
                    else:
                        conn.execute(f"""
                            CREATE TABLE {table_name} AS
                            SELECT * FROM sqlite_scan('{source_path}', '{table_name}')
                        """)
                
                # Get row count
                count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                row_counts[table_name] = count
                
                if show_progress:
                    print(f"  ✓ {table_name}: {count:,} rows")
                    
            except Exception as e:
                logger.warning(f"Failed to convert table {table_name}: {e}")
                row_counts[table_name] = 0
        
        # Checkpoint to ensure data is written
        conn.execute("CHECKPOINT")
        
        if show_progress:
            total_rows = sum(row_counts.values())
            print(f"\nConversion complete: {len(row_counts)} tables, {total_rows:,} total rows")
        
        return row_counts
        
    finally:
        conn.close()


def merge_states(
    source_paths: List[Path],
    state_codes: List[int],
    target_path: Path,
    tables: Optional[List[str]] = None,
    show_progress: bool = True
) -> Dict[str, Dict[str, int]]:
    """
    Merge multiple state SQLite databases into single DuckDB database.
    
    Parameters
    ----------
    source_paths : List[Path]
        List of SQLite database paths
    state_codes : List[int]
        Corresponding state FIPS codes
    target_path : Path
        Target DuckDB database path
    tables : Optional[List[str]]
        Specific tables to merge (None = all)
    show_progress : bool
        Show progress messages
        
    Returns
    -------
    Dict[str, Dict[str, int]]
        Nested dict: {state_code: {table_name: row_count}}
        
    Examples
    --------
    >>> # Create Southeast regional database
    >>> merge_states(
    ...     [Path("NC_FIA.db"), Path("SC_FIA.db"), Path("GA_FIA.db")],
    ...     [37, 45, 13],
    ...     Path("southeast.duckdb")
    ... )
    """
    if len(source_paths) != len(state_codes):
        raise ValueError("Number of source paths must match number of state codes")
    
    results = {}
    
    for i, (source_path, state_code) in enumerate(zip(source_paths, state_codes)):
        if show_progress:
            print(f"\n[{i+1}/{len(source_paths)}] Processing state {state_code}: {source_path.name}")
        
        # First state creates database, rest append
        append = i > 0
        
        row_counts = convert_sqlite_to_duckdb(
            source_path=source_path,
            target_path=target_path,
            state_code=state_code,
            append=append,
            tables=tables,
            show_progress=show_progress
        )
        
        results[str(state_code)] = row_counts
    
    if show_progress:
        print(f"\n✓ Merged {len(source_paths)} states into {target_path}")
    
    return results


def append_state(
    source_path: Path,
    target_path: Path,
    state_code: int,
    dedupe: bool = False,
    dedupe_keys: Optional[List[str]] = None,
    show_progress: bool = True
) -> Dict[str, int]:
    """
    Append a state to existing DuckDB database with optional deduplication.
    
    Parameters
    ----------
    source_path : Path
        SQLite database to append
    target_path : Path
        Existing DuckDB database
    state_code : int
        State FIPS code
    dedupe : bool
        Remove duplicates if True
    dedupe_keys : Optional[List[str]]
        Columns to use for deduplication (e.g., ["CN"])
    show_progress : bool
        Show progress messages
        
    Returns
    -------
    Dict[str, int]
        Table names to row counts
        
    Examples
    --------
    >>> # Append with deduplication
    >>> append_state(
    ...     Path("FL_FIA_updated.db"),
    ...     Path("southeast.duckdb"),
    ...     state_code=12,
    ...     dedupe=True,
    ...     dedupe_keys=["CN"]
    ... )
    """
    if not target_path.exists():
        raise FileNotFoundError(f"Target database does not exist: {target_path}")
    
    if dedupe and not dedupe_keys:
        dedupe_keys = ["CN"]  # Default to CN column for deduplication
    
    if show_progress:
        print(f"Appending state {state_code} from {source_path.name}")
        if dedupe:
            print(f"  Deduplication enabled using keys: {dedupe_keys}")
    
    if dedupe:
        # Append with deduplication
        return _append_with_deduplication(
            source_path, target_path, state_code, dedupe_keys, show_progress
        )
    else:
        # Simple append
        return convert_sqlite_to_duckdb(
            source_path=source_path,
            target_path=target_path,
            state_code=state_code,
            append=True,
            show_progress=show_progress
        )


def _table_exists(conn: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    """Check if table exists in DuckDB database."""
    result = conn.execute("""
        SELECT COUNT(*) 
        FROM information_schema.tables 
        WHERE table_name = ?
    """, [table_name]).fetchone()
    return result[0] > 0


def _append_with_deduplication(
    source_path: Path,
    target_path: Path,
    state_code: int,
    dedupe_keys: List[str],
    show_progress: bool
) -> Dict[str, int]:
    """
    Append with deduplication using temporary tables.
    
    This is more complex but handles updating existing state data.
    """
    conn = duckdb.connect(str(target_path), read_only=False)
    
    try:
        # Install sqlite_scanner
        conn.execute("INSTALL sqlite_scanner")
        conn.execute("LOAD sqlite_scanner")
        
        # Get tables from source
        sqlite_conn = sqlite3.connect(str(source_path))
        cursor = sqlite_conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
        tables = [row[0] for row in cursor.fetchall()]
        sqlite_conn.close()
        
        row_counts = {}
        
        for table_name in tables:
            if show_progress:
                print(f"  Processing {table_name} with deduplication...")
            
            try:
                # Create temporary table with new data
                temp_table = f"{table_name}_temp_{state_code}"
                conn.execute(f"""
                    CREATE TEMPORARY TABLE {temp_table} AS
                    SELECT *, {state_code} AS STATE_ADDED
                    FROM sqlite_scan('{source_path}', '{table_name}')
                """)
                
                if _table_exists(conn, table_name):
                    # Delete existing records with matching keys
                    key_conditions = " AND ".join([
                        f"{table_name}.{key} = {temp_table}.{key}"
                        for key in dedupe_keys
                    ])
                    
                    conn.execute(f"""
                        DELETE FROM {table_name}
                        WHERE EXISTS (
                            SELECT 1 FROM {temp_table}
                            WHERE {key_conditions}
                        )
                    """)
                    
                    # Insert new/updated records
                    conn.execute(f"""
                        INSERT INTO {table_name}
                        SELECT * FROM {temp_table}
                    """)
                else:
                    # Create table from temp
                    conn.execute(f"""
                        CREATE TABLE {table_name} AS
                        SELECT * FROM {temp_table}
                    """)
                
                # Get final count
                count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                row_counts[table_name] = count
                
                # Drop temp table
                conn.execute(f"DROP TABLE {temp_table}")
                
                if show_progress:
                    print(f"    ✓ {count:,} rows after deduplication")
                    
            except Exception as e:
                logger.warning(f"Failed to process table {table_name}: {e}")
                row_counts[table_name] = 0
        
        conn.execute("CHECKPOINT")
        return row_counts
        
    finally:
        conn.close()


def get_database_info(db_path: Path) -> Dict[str, any]:
    """
    Get information about a DuckDB database.
    
    Parameters
    ----------
    db_path : Path
        Path to DuckDB database
        
    Returns
    -------
    Dict[str, any]
        Database information including tables, row counts, and size
    """
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")
    
    conn = duckdb.connect(str(db_path), read_only=True)
    
    try:
        # Get table information
        tables = conn.execute("""
            SELECT 
                table_name,
                column_count,
                estimated_size
            FROM duckdb_tables()
            ORDER BY table_name
        """).fetchall()
        
        # Get row counts for each table
        table_info = {}
        total_rows = 0
        
        for table_name, col_count, est_size in tables:
            row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            table_info[table_name] = {
                "columns": col_count,
                "rows": row_count,
                "estimated_size": est_size
            }
            total_rows += row_count
        
        # Get file size
        file_size = db_path.stat().st_size
        
        return {
            "path": str(db_path),
            "file_size_mb": file_size / 1024 / 1024,
            "tables": table_info,
            "total_tables": len(tables),
            "total_rows": total_rows
        }
        
    finally:
        conn.close()