"""
SQLite to DuckDB converter for FIA databases.

Simple, efficient conversion leveraging DuckDB's native sqlite_scanner extension.
No unnecessary abstractions, just straightforward functionality.

Main Functions:
- convert_sqlite_to_duckdb(): Convert single SQLite database to DuckDB
- merge_states(): Merge multiple state databases into one DuckDB
- append_state(): Append a state to existing DuckDB database
- get_database_info(): Get information about a DuckDB database

Examples
--------
>>> from pyfia.converter import convert_sqlite_to_duckdb, merge_states
>>>
>>> # Convert single state
>>> convert_sqlite_to_duckdb(
...     Path("NC_FIA.db"),
...     Path("north_carolina.duckdb"),
...     state_code=37
... )
>>>
>>> # Create multi-state database
>>> merge_states(
...     [Path("NC_FIA.db"), Path("SC_FIA.db"), Path("GA_FIA.db")],
...     [37, 45, 13],
...     Path("southeast.duckdb")
... )
"""

from .converter import (
    append_state,
    convert_sqlite_to_duckdb,
    get_database_info,
    merge_states,
)
from .utils import (
    compare_databases,
    format_duration,
    format_size,
    get_duckdb_tables,
    get_sqlite_tables,
    load_fia_schema,
    print_summary,
    validate_table_schema,
)

__version__ = "2.0.0"

__all__ = [
    # Main conversion functions
    "convert_sqlite_to_duckdb",
    "merge_states",
    "append_state",
    "get_database_info",
    # Utility functions
    "load_fia_schema",
    "validate_table_schema",
    "get_sqlite_tables",
    "get_duckdb_tables",
    "compare_databases",
    "format_size",
    "format_duration",
    "print_summary",
]
