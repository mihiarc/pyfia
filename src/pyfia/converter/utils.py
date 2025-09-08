"""
Utility functions for SQLite to DuckDB conversion.

Simple utilities including YAML schema loading without unnecessary abstractions.
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional, Any
import yaml

import duckdb
import sqlite3

logger = logging.getLogger(__name__)


def load_fia_schema(schema_dir: Optional[Path] = None) -> Dict[str, Dict[str, Any]]:
    """
    Load FIA table schemas from YAML files.
    
    The YAML schemas are the official source of truth for FIA table definitions.
    We use them for validation and ensuring consistency.
    
    Parameters
    ----------
    schema_dir : Optional[Path]
        Directory containing YAML schema files
        If None, uses default location
        
    Returns
    -------
    Dict[str, Dict[str, Any]]
        Dictionary mapping table names to their schema definitions
    """
    if schema_dir is None:
        # Default to schemas directory in converter module
        schema_dir = Path(__file__).parent / "schemas"
    
    if not schema_dir.exists():
        logger.warning(f"Schema directory not found: {schema_dir}")
        return {}
    
    schemas = {}
    
    # Load all YAML files in schema directory
    for yaml_file in schema_dir.glob("*.yaml"):
        try:
            with open(yaml_file, 'r') as f:
                schema_data = yaml.safe_load(f)
                
                # Extract table name from filename or schema content
                table_name = yaml_file.stem.upper()
                
                # Store schema
                schemas[table_name] = schema_data
                
                logger.debug(f"Loaded schema for table: {table_name}")
                
        except Exception as e:
            logger.warning(f"Failed to load schema from {yaml_file}: {e}")
    
    return schemas


def validate_table_schema(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    expected_schema: Dict[str, Any]
) -> List[str]:
    """
    Validate that a table matches expected FIA schema.
    
    Simple validation without over-engineering - just check that
    required columns exist with appropriate types.
    
    Parameters
    ----------
    conn : duckdb.DuckDBPyConnection
        DuckDB connection
    table_name : str
        Table name to validate
    expected_schema : Dict[str, Any]
        Expected schema from YAML
        
    Returns
    -------
    List[str]
        List of validation issues (empty if valid)
    """
    issues = []
    
    try:
        # Get actual table schema from DuckDB
        actual_cols = conn.execute(f"""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = '{table_name}'
        """).fetchall()
        
        actual_schema = {col: dtype for col, dtype in actual_cols}
        
        # Check for required columns from YAML schema
        if "columns" in expected_schema:
            for col_def in expected_schema["columns"]:
                col_name = col_def.get("name", "").upper()
                
                if col_name and col_name not in actual_schema:
                    issues.append(f"Missing required column: {col_name}")
        
        # Don't be overly strict about types - DuckDB handles conversions well
        
    except Exception as e:
        issues.append(f"Failed to validate schema: {e}")
    
    return issues


def get_sqlite_tables(sqlite_path: Path) -> List[str]:
    """
    Get list of tables from SQLite database.
    
    Parameters
    ----------
    sqlite_path : Path
        Path to SQLite database
        
    Returns
    -------
    List[str]
        List of table names
    """
    if not sqlite_path.exists():
        raise FileNotFoundError(f"SQLite database not found: {sqlite_path}")
    
    conn = sqlite3.connect(str(sqlite_path))
    try:
        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )
        return [row[0] for row in cursor.fetchall()]
    finally:
        conn.close()


def get_duckdb_tables(duckdb_path: Path) -> List[str]:
    """
    Get list of tables from DuckDB database.
    
    Parameters
    ----------
    duckdb_path : Path
        Path to DuckDB database
        
    Returns
    -------
    List[str]
        List of table names
    """
    if not duckdb_path.exists():
        return []
    
    conn = duckdb.connect(str(duckdb_path), read_only=True)
    try:
        result = conn.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'main'
            ORDER BY table_name
        """).fetchall()
        return [row[0] for row in result]
    finally:
        conn.close()


def compare_databases(
    source_path: Path,
    target_path: Path
) -> Dict[str, Any]:
    """
    Compare source SQLite and target DuckDB databases.
    
    Simple comparison for verification without over-engineering.
    
    Parameters
    ----------
    source_path : Path
        Source SQLite database
    target_path : Path
        Target DuckDB database
        
    Returns
    -------
    Dict[str, Any]
        Comparison results
    """
    source_tables = set(get_sqlite_tables(source_path))
    target_tables = set(get_duckdb_tables(target_path))
    
    # Compare table lists
    common_tables = source_tables & target_tables
    missing_tables = source_tables - target_tables
    extra_tables = target_tables - source_tables
    
    # Compare row counts for common tables
    row_count_comparison = {}
    
    if common_tables:
        sqlite_conn = sqlite3.connect(str(source_path))
        duck_conn = duckdb.connect(str(target_path), read_only=True)
        
        try:
            for table in common_tables:
                # Get SQLite count
                sqlite_count = sqlite_conn.execute(
                    f"SELECT COUNT(*) FROM {table}"
                ).fetchone()[0]
                
                # Get DuckDB count
                duck_count = duck_conn.execute(
                    f"SELECT COUNT(*) FROM {table}"
                ).fetchone()[0]
                
                row_count_comparison[table] = {
                    "sqlite": sqlite_count,
                    "duckdb": duck_count,
                    "difference": duck_count - sqlite_count
                }
        finally:
            sqlite_conn.close()
            duck_conn.close()
    
    # Calculate file sizes
    source_size = source_path.stat().st_size if source_path.exists() else 0
    target_size = target_path.stat().st_size if target_path.exists() else 0
    
    return {
        "source_tables": len(source_tables),
        "target_tables": len(target_tables),
        "common_tables": len(common_tables),
        "missing_tables": list(missing_tables),
        "extra_tables": list(extra_tables),
        "row_counts": row_count_comparison,
        "source_size_mb": source_size / 1024 / 1024,
        "target_size_mb": target_size / 1024 / 1024,
        "compression_ratio": source_size / target_size if target_size > 0 else 0
    }


def create_state_filter(state_code: int) -> str:
    """
    Create SQL filter for state-specific data.
    
    Parameters
    ----------
    state_code : int
        State FIPS code
        
    Returns
    -------
    str
        SQL WHERE clause for state filtering
    """
    # Most FIA tables use STATECD column
    return f"STATECD = {state_code}"


def estimate_conversion_time(
    source_path: Path,
    mb_per_second: float = 50.0
) -> float:
    """
    Estimate conversion time based on file size.
    
    Parameters
    ----------
    source_path : Path
        Source SQLite database
    mb_per_second : float
        Estimated conversion speed in MB/s
        
    Returns
    -------
    float
        Estimated time in seconds
    """
    if not source_path.exists():
        return 0.0
    
    size_mb = source_path.stat().st_size / 1024 / 1024
    return size_mb / mb_per_second


def format_size(bytes_size: int) -> str:
    """
    Format byte size as human-readable string.
    
    Parameters
    ----------
    bytes_size : int
        Size in bytes
        
    Returns
    -------
    str
        Formatted size string
    """
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if bytes_size < 1024.0:
            return f"{bytes_size:.2f} {unit}"
        bytes_size /= 1024.0
    return f"{bytes_size:.2f} PB"


def format_duration(seconds: float) -> str:
    """
    Format duration as human-readable string.
    
    Parameters
    ----------
    seconds : float
        Duration in seconds
        
    Returns
    -------
    str
        Formatted duration string
    """
    if seconds < 60:
        return f"{seconds:.1f} seconds"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.1f} minutes"
    else:
        hours = seconds / 3600
        return f"{hours:.1f} hours"


def print_summary(
    source_path: Path,
    target_path: Path,
    row_counts: Dict[str, int],
    start_time: float,
    end_time: float
) -> None:
    """
    Print conversion summary.
    
    Parameters
    ----------
    source_path : Path
        Source SQLite database
    target_path : Path
        Target DuckDB database
    row_counts : Dict[str, int]
        Table row counts
    start_time : float
        Start timestamp
    end_time : float
        End timestamp
    """
    duration = end_time - start_time
    total_rows = sum(row_counts.values())
    
    # Get file sizes
    source_size = source_path.stat().st_size
    target_size = target_path.stat().st_size
    compression_ratio = source_size / target_size if target_size > 0 else 0
    
    print("\n" + "=" * 60)
    print("CONVERSION SUMMARY")
    print("=" * 60)
    print(f"Source: {source_path.name}")
    print(f"Target: {target_path.name}")
    print(f"Tables: {len(row_counts)}")
    print(f"Total Rows: {total_rows:,}")
    print(f"Duration: {format_duration(duration)}")
    print(f"Speed: {total_rows / duration:,.0f} rows/sec")
    print("-" * 60)
    print(f"Source Size: {format_size(source_size)}")
    print(f"Target Size: {format_size(target_size)}")
    print(f"Compression: {compression_ratio:.2f}x")
    print("=" * 60)