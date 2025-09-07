# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview
pyFIA is a Python library implementing the R rFIA package functionality for analyzing USDA Forest Inventory and Analysis (FIA) data. It provides exact statistical compatibility with rFIA while leveraging modern Python data science tools like Polars and DuckDB for high-performance data processing.

## Development Setup

### Installation
```bash
# Install with uv in development mode
uv venv
source .venv/bin/activate  # or .venv\Scripts\activate on Windows
uv pip install -e .[dev]

# Setup pre-commit hooks (if needed)
pre-commit install
```

### Essential Commands

#### Testing
```bash
# Run all tests
uv run pytest

# Run specific test file
uv run pytest tests/test_area.py

# Run with coverage
uv run pytest --cov=pyfia --cov-report=html

# Run property-based tests (longer)
uv run pytest tests/test_property_based.py -v
```

#### Code Quality
```bash
# Format code
uv run ruff format

# Lint code  
uv run ruff check --fix

# Type checking
uv run mypy src/pyfia/

# Run all pre-commit hooks
uv run pre-commit run --all-files
```

#### Documentation
```bash
# Build documentation locally
uv run mkdocs serve

# Build for deployment
uv run mkdocs build
```

## Architecture

### Core Components

**FIA Database Class (`pyfia.core.fia.FIA`)**
- Main entry point for database connections and EVALID-based filtering
- Supports both DuckDB and SQLite backends transparently
- Automatic backend detection or explicit engine selection
- Provides context manager support for automatic connection handling
- Key methods: `clip_by_evalid()`, `clip_by_state()`, `prepare_estimation_data()`

**Estimation Functions (`pyfia.estimation/`)**
- Statistical estimation following FIA methodology and rFIA compatibility
- Functions: `area()`, `biomass()`, `volume()`, `tpa()`, `mortality()`, `growth()`
- All support domain filtering, grouping, and proper variance calculations

**Data Reader (`pyfia.core.data_reader.FIADataReader`)**
- Efficient data loading with WHERE clause support
- Supports both DuckDB and SQLite backends through abstraction layer
- Automatic backend detection based on file extension/content
- Lazy evaluation for memory efficiency
- Automatic schema detection and column validation
- Backend-specific optimizations (DuckDB: columnar storage, SQLite: PRAGMA settings)

**Filters (`pyfia.filters/`)**
- Domain filtering, EVALID management, plot joins
- Statistical grouping and classification utilities

### Database Structure
- Supports both DuckDB and SQLite backends with automatic detection
- DuckDB recommended for efficient large-scale data processing
- SQLite supported for compatibility with FIA DataMart downloads
- EVALID-based filtering ensures statistically valid plot groupings
- Supports multiple evaluation types: VOL (volume), GRM (growth/removal/mortality), CHNG (change)
- State-level filtering applied at database level for performance

### Statistical Methodology
- Design-based estimation following Bechtold & Patterson (2005)
- Post-stratified estimation with proper variance calculation
- Temporal methods: TI (temporally indifferent), annual, SMA, LMA, EMA
- Ratio-of-means estimators for per-acre values

## Key Dependencies and Patterns

### Required Dependencies
- **Polars**: Primary dataframe library for in-memory operations
- **DuckDB**: Database engine for efficient data processing  
- **Pydantic v2**: Data validation and settings management
- **Rich**: Terminal output formatting
- **ConnectorX**: Fast database connectivity

### Code Patterns
- Use Polars LazyFrame for memory efficiency
- Apply Pydantic v2 for all data validation
- Follow rFIA naming conventions (e.g., `bySpecies`, `treeDomain`)
- Context managers for database connections
- Property-based testing with Hypothesis

### Testing Patterns
- Comprehensive fixtures in `tests/conftest.py` with sample FIA database
- Property-based tests for statistical accuracy
- Integration tests with real FIA data structures
- Performance benchmarks comparing to rFIA

## FIA-Specific Knowledge

### EVALID System
- 6-digit codes identifying statistically valid plot groupings
- Format: SSYYTT where:
  - SS = State FIPS code (e.g., 40 for Oklahoma)
  - YY = Inventory year (e.g., 23 for 2023)
  - TT = Evaluation type code (00, 01, 03, 07, 09, etc.)
- **CRITICAL**: Only ONE EVALID should be used per estimation to prevent overcounting
- Must filter by EVALID for proper statistical estimates
- Use `most_recent=True` for latest evaluations by default

### EVAL_TYP Values in POP_EVAL_TYP Table
The evaluation type codes in EVALID don't directly map to EVAL_TYP values:
- **EXPALL**: All data types (typically EVALID type 00) - recommended for area estimation
- **EXPVOL**: Volume/biomass data (typically EVALID type 01)
- **EXPGROW**: Growth data
- **EXPMORT**: Mortality data
- **EXPREMV**: Removal data
- **EXPCHNG**: Change data (typically EVALID type 03)
- **EXPDWM**: Down woody materials (typically EVALID type 07)
- **EXPINV**: Inventory data (typically EVALID type 09)

For area estimation, use EVALIDs with EXPALL (type 00) as they include all plots

### Domain Filtering
- `treeDomain`: SQL-like conditions for tree-level filtering (e.g., "STATUSCD == 1")
- `areaDomain`: Conditions for area/condition-level filtering
- `plotDomain`: Plot-level filtering conditions

### Common Species Codes
- 131: Loblolly pine, 110: Virginia pine, 833: Chestnut oak, 802: White oak
- Use REF_SPECIES table for species name lookups

### Estimation Units and Strata
- Population organized by estimation units and strata
- Proper variance calculation requires stratification
- Use POP_PLOT_STRATUM_ASSGN for plot assignments

## Example Usage Patterns

### Basic Analysis
```python
from pyfia import FIA, tpa, volume, area

# Auto-detect backend (DuckDB or SQLite)
with FIA("path/to/fia.duckdb") as db:  # or .db for SQLite
    # Filter to North Carolina most recent volume evaluation
    db.clip_by_state(37, most_recent=True)
    
    # Get estimates
    tpa_results = tpa(db, treeDomain="STATUSCD == 1")
    vol_results = volume(db, bySpecies=True)
    area_results = area(db, landType='timber')

# Explicit backend selection
db_sqlite = FIA("path/to/fia.db", engine="sqlite")
db_duckdb = FIA("path/to/fia.duckdb", engine="duckdb")

# Method chaining pattern works with both backends
db = FIA("path/to/fia.db").clip_by_state([37, 45]).clip_most_recent("VOL")
results = volume(db, treeDomain="DIA >= 10.0", bySpecies=True)
```

## Database Conversion and Management

### SQLite to DuckDB Converter

pyFIA includes a comprehensive converter for transforming FIA DataMart SQLite databases to DuckDB format. The converter uses FIA standard schemas directly without type "optimizations", ensuring data integrity while leveraging DuckDB's automatic compression.

#### Simple Conversion (Single State)
```python
from pyfia import convert_sqlite_to_duckdb

# Convert a single state database
result = convert_sqlite_to_duckdb(
    source_path="SQLite_FIADB_OK.db",
    target_path="oklahoma.duckdb",
    state_code=40  # Oklahoma FIPS code
)

print(f"Converted {result.stats.source_records_processed:,} records")
print(f"Compression ratio: {result.compression_ratio:.2f}x")
```

#### Building Multi-State Databases

##### Option 1: Merge Multiple States at Once
```python
from pyfia import merge_state_databases

# Create a multi-state database from multiple SQLite files
merge_state_databases(
    source_paths=["SQLite_FIADB_OK.db", "SQLite_FIADB_TX.db", "SQLite_FIADB_AL.db"],
    target_path="nfi_south.duckdb",
    state_codes=[40, 48, 1]  # Oklahoma, Texas, Alabama
)
```

##### Option 2: Build Incrementally with Append
```python
from pyfia import convert_sqlite_to_duckdb, append_to_database

# Step 1: Create initial database with first state
convert_sqlite_to_duckdb(
    source_path="SQLite_FIADB_OK.db",
    target_path="nfi_south.duckdb",
    state_code=40
)

# Step 2: Append additional states
append_to_database(
    target_db="nfi_south.duckdb",
    source_path="SQLite_FIADB_TX.db",
    state_code=48,
    dedupe=False  # No deduplication needed for new states
)

# Step 3: Continue appending
append_to_database(
    target_db="nfi_south.duckdb",
    source_path="SQLite_FIADB_AL.db",
    state_code=1
)
```

#### Handling Updates and Deduplication

```python
# When appending updated data for an existing state, use deduplication
append_to_database(
    target_db="nfi_south.duckdb",
    source_path="SQLite_FIADB_TX_2024_update.db",
    state_code=48,
    dedupe=True,
    dedupe_keys=["CN"]  # Remove duplicates based on CN field
)
```

#### Advanced Configuration

```python
from pyfia.converter import FIAConverter, ConverterConfig
from pathlib import Path

# Create custom configuration
config = ConverterConfig(
    source_dir=Path("/data/fia/sqlite"),
    target_path=Path("/data/fia/regional.duckdb"),
    compression_level="medium",    # Compression: none, low, medium, high
    validation_level="none",       # Validation: none, basic, standard, comprehensive
    append_mode=True,              # True for append, False for new database
    dedupe_on_append=False,        # Remove duplicates when appending
    dedupe_keys=["CN"],           # Fields to check for duplicates
    batch_size=100_000,           # Records per batch
    show_progress=True            # Show progress bars
)

# Use converter directly for more control
converter = FIAConverter(config)

# Convert/append a state
result = converter.convert_state(
    sqlite_path=Path("SQLite_FIADB_GA.db"),
    state_code=13,  # Georgia
    target_path=Path("/data/fia/regional.duckdb")
)

print(f"Processed: {result.stats.source_records_processed:,} records")
print(f"Written: {result.stats.target_records_written:,} records")
print(f"Time: {result.stats.total_time:.1f} seconds")
```

#### Practical Examples

##### Example 1: Create Regional Database
```python
from pyfia import convert_sqlite_to_duckdb, append_to_database

# Create Southern region database
states = {
    "SQLite_FIADB_AL.db": 1,   # Alabama
    "SQLite_FIADB_GA.db": 13,  # Georgia  
    "SQLite_FIADB_FL.db": 12,  # Florida
    "SQLite_FIADB_MS.db": 28,  # Mississippi
    "SQLite_FIADB_LA.db": 22   # Louisiana
}

# Start with first state
first_state = list(states.items())[0]
convert_sqlite_to_duckdb(
    source_path=first_state[0],
    target_path="nfi_south.duckdb",
    state_code=first_state[1]
)

# Append remaining states
for sqlite_file, state_code in list(states.items())[1:]:
    append_to_database(
        target_db="nfi_south.duckdb",
        source_path=sqlite_file,
        state_code=state_code
    )
```

##### Example 2: Update Existing State Data
```python
# Replace existing state data with fresh download
# First, check what's currently in the database
from pyfia import FIA

with FIA("nfi_south.duckdb") as db:
    db_conn = db.conn
    current_plots = db_conn.execute(
        "SELECT COUNT(*) FROM PLOT WHERE STATECD = 48"
    ).fetchone()[0]
    print(f"Current Texas plots: {current_plots:,}")

# Append with deduplication to update
append_to_database(
    target_db="nfi_south.duckdb",
    source_path="SQLite_FIADB_TX_latest.db",
    state_code=48,
    dedupe=True,
    dedupe_keys=["CN"]
)
```

#### Performance Benefits
- **5-10x faster** analytical queries using DuckDB's columnar storage
- **30-50% smaller** database size through automatic compression (typically 5-6x compression ratio)
- **True append mode** adds new states without removing existing data
- **Memory efficiency** with streaming processing and batch operations

#### Key Features
- **FIA Standard Schemas**: Uses official FIA data types from YAML definitions
- **Automatic Compression**: DuckDB applies optimal compression regardless of declared types
- **Multi-state Support**: Build regional or national databases incrementally
- **Deduplication**: Optional duplicate removal when updating existing data
- **Progress Tracking**: Rich progress bars show conversion status

#### Best Practices

1. **For New Databases**: Use `convert_sqlite_to_duckdb()` for the first state
2. **For Adding States**: Use `append_to_database()` with `dedupe=False`
3. **For Updates**: Use `append_to_database()` with `dedupe=True` and specify `dedupe_keys`
4. **For Performance**: Set `validation_level="none"` during conversion if data is trusted
5. **For Large Datasets**: Adjust `batch_size` based on available memory

#### Checking Database Contents

```python
# Quick check: Which states are in the database?
import duckdb

with duckdb.connect("nfi_south.duckdb", read_only=True) as conn:
    states = conn.execute("""
        SELECT 
            STATECD as state_code,
            COUNT(*) as plot_count
        FROM PLOT
        GROUP BY STATECD
        ORDER BY STATECD
    """).fetchall()
    
    print("States in database:")
    for state_code, count in states:
        print(f"  State {state_code}: {count:,} plots")

# More detailed check with state names
from pyfia import FIA

with FIA("nfi_south.duckdb") as db:
    conn = db.reader.conn if hasattr(db, 'reader') else duckdb.connect("nfi_south.duckdb", read_only=True)
    
    # State FIPS codes to names mapping
    state_names = {
        1: "Alabama", 2: "Alaska", 4: "Arizona", 5: "Arkansas", 6: "California",
        8: "Colorado", 9: "Connecticut", 10: "Delaware", 12: "Florida", 13: "Georgia",
        15: "Hawaii", 16: "Idaho", 17: "Illinois", 18: "Indiana", 19: "Iowa",
        20: "Kansas", 21: "Kentucky", 22: "Louisiana", 23: "Maine", 24: "Maryland",
        25: "Massachusetts", 26: "Michigan", 27: "Minnesota", 28: "Mississippi",
        29: "Missouri", 30: "Montana", 31: "Nebraska", 32: "Nevada", 33: "New Hampshire",
        34: "New Jersey", 35: "New Mexico", 36: "New York", 37: "North Carolina",
        38: "North Dakota", 39: "Ohio", 40: "Oklahoma", 41: "Oregon", 42: "Pennsylvania",
        44: "Rhode Island", 45: "South Carolina", 46: "South Dakota", 47: "Tennessee",
        48: "Texas", 49: "Utah", 50: "Vermont", 51: "Virginia", 53: "Washington",
        54: "West Virginia", 55: "Wisconsin", 56: "Wyoming"
    }
    
    result = conn.execute("""
        SELECT 
            STATECD,
            COUNT(DISTINCT CN) as plots,
            COUNT(DISTINCT INVYR) as years,
            MIN(INVYR) as earliest_year,
            MAX(INVYR) as latest_year
        FROM PLOT
        GROUP BY STATECD
        ORDER BY STATECD
    """).fetchall()
    
    print("\nDetailed state inventory:")
    for state_code, plots, years, min_year, max_year in result:
        name = state_names.get(state_code, f"Unknown ({state_code})")
        print(f"  {name}: {plots:,} plots, {years} inventory years ({min_year}-{max_year})")
```

#### Troubleshooting

```python
# If append shows 0 records processed, use the converter directly:
from pyfia.converter import FIAConverter, ConverterConfig

config = ConverterConfig(
    source_dir=Path("."),
    target_path=Path("database.duckdb"),
    append_mode=True,
    show_progress=True,
    validation_level="none"
)

converter = FIAConverter(config)
result = converter.convert_state(
    sqlite_path=Path("state_data.db"),
    state_code=state_fips_code,
    target_path=Path("database.duckdb")
)
```