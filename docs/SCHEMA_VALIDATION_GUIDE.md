# FIA Database Schema Validation Guide

## Problem Summary

When appending Georgia SQLite data to nfi_south.duckdb, the operation failed due to schema mismatches. The SQLite database from FIA DataMart has different column structures than our DuckDB database, causing these specific issues:

### Schema Differences Found

1. **Extra CN Column**: Georgia SQLite has a `CN` (Control Number) column in many tables that doesn't exist in the target DuckDB
2. **Column Count Mismatch**: Tables like PLOT, TREE, COND have different numbers of columns (e.g., PLOT has 64 columns in SQLite but 63 in DuckDB)
3. **Reference Tables**: Many reference tables (REF_*) don't have CN columns and use different primary keys

## Root Cause

The existing nfi_south.duckdb was likely created with an older version of the converter that didn't include all columns from the source SQLite databases. The YAML schemas in `src/pyfia/converter/schemas/` define the expected structure, but the actual DuckDB database doesn't match these schemas.

## Solution Approaches

### Option 1: Convert Georgia Separately Then Merge (RECOMMENDED)

```python
from pyfia.converter import convert_sqlite_to_duckdb, merge_states

# Step 1: Convert Georgia to its own DuckDB file
convert_sqlite_to_duckdb(
    source_path=Path("data/SQLite_FIADB_GA.db"),
    target_path=Path("data/georgia.duckdb"),
    state_code=13,
    show_progress=True
)

# Step 2: Recreate regional database with all states
merge_states(
    source_paths=[
        Path("data/georgia.duckdb"),
        Path("data/alabama.duckdb"),  # Convert these first if needed
        Path("data/florida.duckdb"),
        Path("data/oklahoma.duckdb"),
        Path("data/texas.duckdb")
    ],
    target_path=Path("data/nfi_south_v2.duckdb"),
    show_progress=True
)
```

### Option 2: Rebuild Database from SQLite Sources

If you have all the original SQLite files:

```python
from pyfia.converter import merge_states

# Convert and merge all at once
merge_states(
    source_paths=[
        Path("data/SQLite_FIADB_GA.db"),
        Path("data/SQLite_FIADB_AL.db"),
        Path("data/SQLite_FIADB_FL.db"),
        Path("data/SQLite_FIADB_OK.db"),
        Path("data/SQLite_FIADB_TX.db")
    ],
    state_codes=[13, 1, 12, 40, 48],
    target_path=Path("data/nfi_south_new.duckdb"),
    show_progress=True
)
```

## Schema Validation Requirements

### Pre-Conversion Checks

1. **Verify YAML Schema Compatibility**
   - Check that source SQLite tables match expected YAML schemas
   - Log any column differences for documentation

2. **Target Database Schema Check**
   - Before appending, verify target database schema matches YAML definitions
   - Prevent silent data loss from column mismatches

3. **CN Column Handling**
   - The CN column is critical for FIA data integrity
   - It should be preserved in DuckDB conversions
   - Used for deduplication when merging states

### Implementation Requirements

```python
def validate_schema_compatibility(source_db, target_db, table_name):
    """
    Validate that source and target schemas are compatible for append.
    
    Returns:
        - compatible: bool
        - missing_in_target: list of columns
        - missing_in_source: list of columns
        - type_mismatches: dict of column -> (source_type, target_type)
    """
    # Implementation should:
    # 1. Get column info from both databases
    # 2. Compare column names and types
    # 3. Return detailed mismatch information
    # 4. Log warnings for non-critical differences
    # 5. Raise errors for critical incompatibilities
```

## Critical Tables for Validation

These tables MUST have matching schemas for proper FIA analysis:

1. **PLOT** - Plot locations and metadata
2. **COND** - Forest conditions 
3. **TREE** - Individual tree records
4. **POP_EVAL** - Evaluation definitions (needs CN column)
5. **POP_PLOT_STRATUM_ASSGN** - Plot-stratum assignments
6. **POP_STRATUM** - Stratum definitions
7. **POP_ESTN_UNIT** - Estimation unit definitions

## Error Prevention

### Before Any Append Operation

1. Check schema compatibility for all critical tables
2. Verify CN column exists in source and target for tables that need it
3. Ensure state code (STATECD) column exists in data tables
4. Validate that no data will be silently dropped

### Error Messages Should Include

- Specific table name
- Column count mismatch details
- Missing column names
- Suggested resolution (convert separately, rebuild, etc.)

## Testing Schema Validation

```python
import pytest
from pyfia.converter import validate_schema_compatibility

def test_schema_validation_catches_mismatches():
    """Test that schema validation catches column mismatches."""
    # Create test databases with different schemas
    # Verify validation catches the differences
    # Ensure helpful error messages are generated
    pass

def test_cn_column_preservation():
    """Test that CN columns are preserved in conversion."""
    # Convert a table with CN column
    # Verify CN values are maintained
    # Check deduplication works with CN
    pass
```

## Lessons Learned

1. **Always validate schemas before append operations**
2. **The YAML schemas are the source of truth** - databases should match them
3. **CN columns are critical** for FIA data integrity and deduplication
4. **Convert first, then merge** is safer than direct append when schemas might differ
5. **Document schema versions** to track changes over time

## Recommended Fix for Current Issue

Since the existing nfi_south.duckdb has an incompatible schema:

1. Convert Georgia SQLite to a new DuckDB file
2. Either:
   - Start fresh by converting all states and merging
   - Or extract data from existing nfi_south.duckdb and re-merge

This ensures all states have consistent, complete schemas matching the YAML definitions.