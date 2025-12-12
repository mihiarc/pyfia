# Data Conversion

Functions for converting and merging FIA databases.

## Overview

PyFIA works best with DuckDB databases. These utilities convert SQLite FIA databases to DuckDB format.

```python
import pyfia

# Convert single state
pyfia.convert_sqlite_to_duckdb("GA_FIA.db", "georgia.duckdb")

# Merge multiple states
pyfia.merge_state_databases(
    ["GA_FIA.db", "SC_FIA.db", "NC_FIA.db"],
    "southeast.duckdb"
)
```

## Functions

### convert_sqlite_to_duckdb

Convert a SQLite FIA database to DuckDB format.

::: pyfia.convert_sqlite_to_duckdb
    options:
      show_root_heading: true
      show_source: true

**Example:**

```python
# Basic conversion
result = pyfia.convert_sqlite_to_duckdb(
    "GA_FIADB_ENTIRE.db",
    "georgia.duckdb"
)
print(f"Converted {sum(result.values())} total rows")

# With options
result = pyfia.convert_sqlite_to_duckdb(
    "GA_FIADB_ENTIRE.db",
    "georgia.duckdb",
    compression_level="high",
    validation_level="comprehensive"
)
```

### merge_state_databases

Merge multiple state databases into one.

::: pyfia.merge_state_databases
    options:
      show_root_heading: true
      show_source: true

**Example:**

```python
# Merge southeast states
result = pyfia.merge_state_databases(
    [
        "GA_FIA.db",
        "SC_FIA.db",
        "NC_FIA.db",
        "FL_FIA.db"
    ],
    "southeast.duckdb"
)

# Check results
for state, tables in result.items():
    total = sum(tables.values())
    print(f"{state}: {total:,} rows")
```

### append_to_database

Append data to an existing database.

::: pyfia.append_to_database
    options:
      show_root_heading: true
      show_source: true

**Example:**

```python
# Append updated data
result = pyfia.append_to_database(
    "georgia.duckdb",      # Target
    "GA_FIA_update.db",    # Source
    dedupe=True            # Remove duplicates
)

# Or with FIA instance
db = pyfia.FIA("georgia.duckdb")
result = pyfia.append_to_database(
    db,
    "GA_FIA_update.db",
    dedupe=True,
    dedupe_keys=["CN", "INVYR"]
)
```

## Workflow

### Creating a Regional Database

```python
import pyfia
from pathlib import Path

# Source databases
sources = list(Path("./raw_data").glob("*_FIA.db"))

# Merge all
pyfia.merge_state_databases(
    sources,
    "regional.duckdb"
)

# Verify
db = pyfia.FIA("regional.duckdb")
states = db.load_table("PLOT").select("STATECD").unique().collect()
print(f"States included: {states['STATECD'].to_list()}")
```

### Updating Existing Database

```python
# Check current data
db = pyfia.FIA("georgia.duckdb")
current_years = db.load_table("SURVEY").select("INVYR").unique().collect()
print(f"Current years: {current_years['INVYR'].to_list()}")

# Append new data
pyfia.append_to_database(
    "georgia.duckdb",
    "GA_FIA_2024.db",
    dedupe=True
)

# Verify update
updated_years = db.load_table("SURVEY").select("INVYR").unique().collect()
print(f"Updated years: {updated_years['INVYR'].to_list()}")
```
