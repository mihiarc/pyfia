# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

> **📚 Technical Reference**: For comprehensive FIA methodology, terminology, and formulas, see [`fia_technical_context.md`](./fia_technical_context.md)

## Project Overview
pyFIA is a Python library for analyzing USDA Forest Inventory and Analysis (FIA) data. It provides statistically valid estimation methods while leveraging modern Python data science tools like Polars and DuckDB for high-performance data processing.

## Core Design Principles

### Simplicity First
- **Avoid over-engineering**: No unnecessary design patterns (Strategy, Factory, Builder, etc.)
- **Direct functions over complex hierarchies**: Use simple functions where possible
- **YAGNI (You Aren't Gonna Need It)**: Don't add abstractions for hypothetical future needs
- **Simple parameter passing**: Direct parameters instead of configuration objects
- **Flat structure**: Avoid deep nesting of directories and modules

### Code Metrics
After recent refactoring, the codebase is significantly simplified:
- **Estimation module**: Reduced from 13,500 to 2,000 lines (85% reduction)
- **Converter module**: Reduced from 4,761 to 901 lines (81% reduction)  
- **Filtering module**: Reduced from 3,159 to 2,994 lines (5% reduction)

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

### Module Structure
```
pyfia/
├── core/               # Core database and reader functionality
│   ├── fia.py         # Main FIA database class
│   └── data_reader.py # Efficient data loading
├── estimation/         # Simplified estimation module (2,000 lines total)
│   ├── base.py        # BaseEstimator with Template Method pattern (400 lines)
│   └── estimators/    # Individual estimators (~300 lines each)
│       ├── area.py
│       ├── biomass.py
│       ├── growth.py
│       ├── mortality.py
│       ├── tpa.py
│       └── volume.py
├── converter/          # Simplified converter (901 lines total)
│   ├── converter.py   # Direct DuckDB conversion (452 lines)
│   └── utils.py       # Helper functions (383 lines)
├── filtering/          # Domain filtering and indicators
│   ├── core/parser.py # Centralized domain expression parser
│   ├── tree/filters.py
│   ├── area/filters.py
│   └── indicators/    # Simple functions, no Strategy pattern
└── constants/          # FIA constants and standard values
```

### Core Components

**FIA Database Class (`pyfia.core.fia.FIA`)**
- Main entry point for database connections and EVALID-based filtering
- Supports both DuckDB and SQLite backends transparently
- Automatic backend detection or explicit engine selection
- Provides context manager support for automatic connection handling
- Key methods: `clip_by_evalid()`, `clip_by_state()`, `prepare_estimation_data()`

**Estimation Functions (`pyfia.estimation/`)**
- Simple, direct API functions: `area()`, `biomass()`, `volume()`, `tpa()`, `mortality()`, `growth()`
- Each estimator is ~300 lines of straightforward code
- BaseEstimator uses Template Method pattern for consistent workflow
- No unnecessary abstractions or complex inheritance hierarchies
- All support domain filtering, grouping, and proper variance calculations

**Data Reader (`pyfia.core.data_reader.FIADataReader`)**
- Efficient data loading with WHERE clause support
- Supports both DuckDB and SQLite backends through abstraction layer
- Automatic backend detection based on file extension/content
- Lazy evaluation for memory efficiency
- Backend-specific optimizations (DuckDB: columnar storage, SQLite: PRAGMA settings)

**Filters (`pyfia.filtering/`)**
- Simple functions for domain filtering and classification
- No over-engineered Strategy patterns or complex abstractions
- Direct Polars expressions for efficient filtering
- Centralized domain expression parser

**Converter (`pyfia.converter/`)**
- Leverages DuckDB's native sqlite_scanner extension
- Simple functions: `convert_sqlite_to_duckdb()`, `merge_states()`, `append_state()`
- No pipelines, strategies, or complex configuration objects
- YAML schemas preserved as source of truth for FIA table definitions

### Database Structure
- Supports both DuckDB and SQLite backends with automatic detection
- DuckDB recommended for efficient large-scale data processing
- SQLite supported for compatibility with FIA DataMart downloads
- EVALID-based filtering ensures statistically valid plot groupings
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
- Use Polars LazyFrame for memory efficiency when appropriate
- Apply Pydantic v2 for settings and configuration only (not for data)
- Follow FIA standard naming conventions in public APIs
- Use simple functions over classes where possible
- Direct parameter passing instead of configuration objects
- Context managers for database connections

### Anti-Patterns to Avoid
- Don't create Strategy, Factory, or Builder patterns without clear need
- Don't add abstraction layers for hypothetical flexibility
- Don't create deep directory nesting (max 3 levels)
- Don't use complex inheritance hierarchies
- Don't create wrapper classes that just pass through calls

### Testing Patterns
- **ALWAYS use real FIA data for testing** when possible (just like using real API calls)
- Integration tests with actual FIA databases (georgia.duckdb, nfi_south.duckdb)
- Mock databases must include complete table structures (including GRM tables)
- Comprehensive fixtures in `tests/conftest.py` with proper FIA database structure
- Property-based tests for statistical accuracy against known FIA values
- Performance benchmarks for optimization validation

## FIA-Specific Knowledge

### EVALID System - Critical for Proper FIA Estimation

#### What is EVALID?
EVALIDs are 6-digit codes that identify statistically valid plot groupings for FIA estimation. They ensure proper expansion factors and prevent double-counting of plots.

#### EVALID Format
- Format: **SSYYTT** where:
  - **SS** = State FIPS code (e.g., 40 for Oklahoma, 48 for Texas)
  - **YY** = Inventory year (last 2 digits, e.g., 23 for 2023)
  - **TT** = Evaluation type code (00, 01, 03, 07, 09, etc.)
- Example: **482300** = Texas, 2023, All Area evaluation (type 00)

#### Critical Rules for EVALID Usage
1. **NEVER mix EVALIDs** - Use only ONE EVALID per estimation to prevent overcounting
2. **EVALIDs are stored in multiple tables**:
   - `POP_EVAL` - Master evaluation definitions
   - `POP_PLOT_STRATUM_ASSGN` - Links plots to EVALIDs
   - `POP_EVAL_TYP` - Defines evaluation types (EXPALL, EXPVOL, etc.)
3. **pyFIA does NOT automatically filter by EVALID** - You must explicitly select EVALIDs or use helper methods

#### Evaluation Types (EVAL_TYP in POP_EVAL_TYP Table)
The evaluation type codes determine which plots are included:
- **EXPALL**: All area estimation plots (typically EVALID ending in 00)
  - **REQUIRED for area estimation** - includes all plots needed for unbiased area estimates
  - Most recent and comprehensive evaluation
- **EXPVOL**: Volume/biomass plots (typically EVALID ending in 01)
  - Subset of plots with tree measurements
  - Use for volume, biomass, carbon estimates
- **EXPGROW**: Growth estimation plots
- **EXPMORT**: Mortality estimation plots
- **EXPREMV**: Removal estimation plots
- **EXPCHNG**: Change estimation plots (typically EVALID ending in 03)
- **EXPDWM**: Down woody materials plots (typically EVALID ending in 07)
- **EXPINV**: Inventory plots (typically EVALID ending in 09)

#### How to Select the Right EVALID

```python
# Method 1: Use pyFIA's helper methods (RECOMMENDED)
with FIA("data/nfi_south.duckdb") as db:
    # Get most recent EXPALL evaluation for area estimation
    db.clip_most_recent(eval_type="EXPALL")
    
    # Or get most recent EXPVOL evaluation for volume estimation
    db.clip_most_recent(eval_type="EXPVOL")

# Method 2: Query available EVALIDs directly
import duckdb
with duckdb.connect("data/nfi_south.duckdb") as conn:
    # Find all EXPALL evaluations for a state
    query = """
    SELECT DISTINCT 
        pe.EVALID,
        pe.EVAL_DESCR,
        pet.EVAL_TYP,
        COUNT(DISTINCT ppsa.PLT_CN) as plot_count
    FROM POP_EVAL pe
    JOIN POP_EVAL_TYP pet ON pe.CN = pet.EVAL_CN
    JOIN POP_PLOT_STRATUM_ASSGN ppsa ON pe.EVALID = ppsa.EVALID
    WHERE pe.STATECD = 48  -- Texas
      AND pet.EVAL_TYP = 'EXPALL'
    GROUP BY pe.EVALID, pe.EVAL_DESCR, pet.EVAL_TYP
    ORDER BY pe.EVALID DESC
    LIMIT 1
    """
    result = conn.execute(query).fetchone()
    best_evalid = result[0]  # Use this EVALID

# Method 3: Manually specify EVALID
db.clip_by_evalid([482300])  # Texas 2023 EXPALL
```

#### Common EVALID Pitfalls and Solutions

**Problem 1: Mixing multiple EVALIDs**
```python
# WRONG - This will double-count plots!
db.clip_by_evalid([482300, 482200])  # Two different Texas evaluations

# CORRECT - Use only one EVALID
db.clip_by_evalid([482300])  # Single evaluation
```

**Problem 2: Using wrong evaluation type**
```python
# WRONG for area estimation
db.clip_most_recent(eval_type="EXPVOL")  # Missing many plots!

# CORRECT for area estimation
db.clip_most_recent(eval_type="EXPALL")  # All plots included
```

**Problem 3: Not filtering by EVALID at all**
```python
# WRONG - May include incompatible plot groupings
results = area(db)  # No EVALID filtering!

# CORRECT - Filter first, then estimate
db.clip_most_recent(eval_type="EXPALL")
results = area(db)
```

#### Checking EVALID Coverage

```python
# See which EVALIDs are in your database
query = """
SELECT 
    pe.STATECD,
    pe.EVALID,
    pe.EVAL_DESCR,
    pet.EVAL_TYP,
    pe.START_INVYR,
    pe.END_INVYR,
    COUNT(DISTINCT ppsa.PLT_CN) as plot_count
FROM POP_EVAL pe
JOIN POP_EVAL_TYP pet ON pe.CN = pet.EVAL_CN
LEFT JOIN POP_PLOT_STRATUM_ASSGN ppsa ON pe.EVALID = ppsa.EVALID
GROUP BY pe.STATECD, pe.EVALID, pe.EVAL_DESCR, pet.EVAL_TYP, 
         pe.START_INVYR, pe.END_INVYR
ORDER BY pe.STATECD, pet.EVAL_TYP, pe.EVALID DESC
"""
```

#### Special Handling for Texas

Texas is unique in the FIA system and requires special handling:

1. **Regional vs Full State Evaluations**:
   - Texas has separate East and West regional evaluations
   - Full state evaluations (LOCATION_NM = "Texas") should be preferred over regional ones
   - East Texas only evaluations (LOCATION_NM = "Texas(EAST)") cover longitude -96.42 to -93.55
   - Full Texas evaluations cover longitude -106.63 to -93.55 (includes both East and West)

2. **Duplicate Data Issues**:
   - **CRITICAL**: Texas data contains exact duplicate rows in both POP_PLOT_STRATUM_ASSGN and POP_STRATUM tables
   - Each plot-stratum assignment appears exactly 2x in POP_PLOT_STRATUM_ASSGN
   - Each stratum appears exactly 2x in POP_STRATUM
   - pyFIA automatically deduplicates these tables to prevent overcounting

3. **EVALID Selection for Texas**:
   ```python
   # pyFIA automatically handles Texas specially
   db = FIA("path/to/database.duckdb")
   db.clip_most_recent(eval_type="ALL")
   # For Texas, this will select the full state evaluation (e.g., 482200)
   # even if a more recent East-only evaluation exists (e.g., 482320)
   ```

4. **Manual Texas Handling**:
   ```python
   # If you need to manually select Texas evaluations
   import duckdb
   
   # Find the best Texas EXPALL evaluation (full state preferred)
   query = """
   SELECT EVALID, LOCATION_NM, END_INVYR
   FROM POP_EVAL pe
   JOIN POP_EVAL_TYP pet ON pe.CN = pet.EVAL_CN
   WHERE pe.STATECD = 48 AND pet.EVAL_TYP = 'EXPALL'
     AND pe.LOCATION_NM = 'Texas'  -- Full state only
   ORDER BY pe.END_INVYR DESC
   LIMIT 1
   """
   ```

5. **Expected Values**:
   - Texas total land area: ~170 million acres
   - Texas forestland (typical): 40-45 million acres
   - If you get values >100 million acres, check for duplicate counting

### Domain Filtering
- `tree_domain`: SQL-like conditions for tree-level filtering (e.g., "STATUSCD == 1")
- `area_domain`: Conditions for area/condition-level filtering
- `plot_domain`: Plot-level filtering conditions

### Common Species Codes
- 131: Loblolly pine, 110: Virginia pine, 833: Chestnut oak, 802: White oak
- Use REF_SPECIES table for species name lookups

### Estimation Units and Strata
- Population organized by estimation units and strata
- Proper variance calculation requires stratification
- Use POP_PLOT_STRATUM_ASSGN for plot assignments

### Growth-Removal-Mortality (GRM) Tables

#### Critical for Mortality, Growth, and Removals Estimation
The FIA Growth-Removal-Mortality methodology requires specialized GRM tables that track individual tree components across remeasurement periods. These tables are **required** for mortality, growth, and removals estimation.

#### Required GRM Tables
- **TREE_GRM_COMPONENT**: Contains component-level data for each tree
  - Key fields: `TRE_CN`, `PLT_CN`, `DIA_BEGIN`, `DIA_MIDPT`, `DIA_END`
  - Component fields: `SUBP_COMPONENT_*` (e.g., MORTALITY1, MORTALITY2, CUT, SURVIVOR, INGROWTH)
  - TPA fields: `SUBP_TPAMORT_UNADJ_*`, `SUBP_TPAREMV_UNADJ_*`, `SUBP_TPAGROW_UNADJ_*`
  - Adjustment type: `SUBP_SUBPTYP_GRM_*` (0=None, 1=SUBP, 2=MICR, 3=MACR)
  
- **TREE_GRM_MIDPT**: Contains tree measurements at remeasurement midpoint
  - Key fields: `TRE_CN`, `DIA`, `SPCD`, `STATUSCD`
  - Measurement fields: `VOLCFNET`, `DRYBIO_BOLE`, `DRYBIO_BRANCH`, etc.

#### Component Types
Components in TREE_GRM_COMPONENT identify what happened to each tree:
- **SURVIVOR**: Trees alive at both measurements
- **MORTALITY1**, **MORTALITY2**: Trees that died between measurements
- **CUT**: Trees removed through harvest
- **DIVERSION**: Trees removed for non-forest use
- **INGROWTH**: New trees that grew into measurable size

#### Land Type and Tree Type Column Naming
GRM columns follow a naming pattern based on land type and tree type:
```python
# Column naming pattern: SUBP_{METRIC}_{TREE_TYPE}_{LAND_TYPE}
# Example: SUBP_TPAMORT_UNADJ_GS_FOREST
#   - TPAMORT_UNADJ = Trees per acre mortality (unadjusted)
#   - GS = Growing stock trees
#   - FOREST = Forest land

# Tree types: GS (growing stock), AL (all live)
# Land types: FOREST, TIMBER
```

#### Adjustment Factors (SUBPTYP_GRM)
The SUBPTYP_GRM field indicates which adjustment factor to apply:
- **0**: No adjustment (trees not sampled)
- **1**: SUBP adjustment (subplot, typically 5.0-24.0" DBH)
- **2**: MICR adjustment (microplot, typically <5.0" DBH)
- **3**: MACR adjustment (macroplot, typically ≥24.0" DBH)

This differs from standard tree adjustment which uses diameter breakpoints directly.

#### Checking for GRM Tables
```python
import duckdb

with duckdb.connect("your_database.duckdb", read_only=True) as conn:
    grm_tables = conn.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_name LIKE '%GRM%'
    """).fetchall()
    
    if not grm_tables:
        print("WARNING: GRM tables not found - mortality/growth/removals will not work")
    else:
        print(f"GRM tables available: {[t[0] for t in grm_tables]}")
```

## Example Usage Patterns

### Basic Analysis
```python
from pyfia import FIA, tpa, volume, area

# Auto-detect backend (DuckDB or SQLite)
with FIA("path/to/fia.duckdb") as db:  # or .db for SQLite
    # Filter to North Carolina most recent volume evaluation
    db.clip_by_state(37, most_recent=True)
    
    # Get estimates - simple, direct function calls
    tpa_results = tpa(db, tree_domain="STATUSCD == 1")
    vol_results = volume(db, by_species=True)
    area_results = area(db, land_type='timber')

# Explicit backend selection
db_sqlite = FIA("path/to/fia.db", engine="sqlite")
db_duckdb = FIA("path/to/fia.duckdb", engine="duckdb")

# Method chaining pattern works with both backends
db = FIA("path/to/fia.db").clip_by_state([37, 45]).clip_most_recent("VOL")
results = volume(db, tree_domain="DIA >= 10.0", by_species=True)
```

### Volume Estimation

The `volume()` function calculates tree volume (in cubic feet) with proper FIA expansion factors:

```python
from pyfia import FIA, volume

# Connect to database and filter to state
with FIA("nfi_south.duckdb") as db:
    # Filter to specific state (e.g., Texas = 48)
    db.clip_by_state(48, most_recent=True, eval_type="EXPVOL")
    
    # Basic volume estimation on forestland
    vol_results = volume(
        db,
        land_type="forest",     # "forest" or "timber"
        tree_type="live",       # "live", "dead", "gs", or "all"
        vol_type="net"          # "net", "gross", "sound", or "sawlog"
    )
    
    # Volume by species
    vol_by_species = volume(
        db,
        land_type="forest",
        tree_type="live",
        by_species=True         # Group by species code (SPCD)
    )
    
    # Volume with custom filters
    vol_large_trees = volume(
        db,
        tree_domain="DIA >= 20.0",     # Custom tree-level filter
        area_domain="SLOPE < 30",      # Custom area-level filter
        land_type="timber"
    )
    
    # Volume by custom grouping
    vol_by_forest_type = volume(
        db,
        grp_by="FORTYPCD",      # Group by forest type code
        land_type="forest"
    )
```

**Note**: All estimation functions use simple, direct parameters - no complex configuration objects.

**Direct SQL Alternative**: For complex queries or debugging, you can also use direct SQL:

```python
import duckdb

with duckdb.connect("nfi_south.duckdb", read_only=True) as conn:
    # Calculate volume with FIA expansion factors
    query = """
    SELECT 
        SUM(CAST(tree.TPA_UNADJ AS DOUBLE) * 
            CAST(tree.VOLCFNET AS DOUBLE) * 
            CAST(pop_stratum.EXPNS AS DOUBLE) * 
            CAST(CASE 
                WHEN tree.DIA < 5.0 THEN pop_stratum.ADJ_FACTOR_MICR
                WHEN tree.DIA < COALESCE(plot.MACRO_BREAKPOINT_DIA, 9999) 
                    THEN pop_stratum.ADJ_FACTOR_SUBP
                ELSE pop_stratum.ADJ_FACTOR_MACR
            END AS DOUBLE)
        ) as total_volume
    FROM pop_stratum
    JOIN pop_plot_stratum_assgn ON (pop_plot_stratum_assgn.STRATUM_CN = pop_stratum.CN)
    JOIN plot ON (pop_plot_stratum_assgn.PLT_CN = plot.CN)
    JOIN cond ON (cond.PLT_CN = plot.CN)
    JOIN tree ON (tree.PLT_CN = cond.PLT_CN AND tree.CONDID = cond.CONDID)
    WHERE tree.STATUSCD = 1 AND cond.COND_STATUS_CD = 1
        AND plot.STATECD = 48  -- Texas
    """
    
    result = conn.execute(query).fetchone()
    total_volume = result[0]
```

## Database Conversion and Management

### SQLite to DuckDB Converter

pyFIA includes a simple, efficient converter for transforming FIA DataMart SQLite databases to DuckDB format. The converter leverages DuckDB's native sqlite_scanner extension for direct, high-performance conversion.

#### Simple Conversion (Single State)
```python
from pyfia.converter import convert_sqlite_to_duckdb

# One-line conversion - no configuration objects needed
row_counts = convert_sqlite_to_duckdb(
    source_path=Path("SQLite_FIADB_OK.db"),
    target_path=Path("oklahoma.duckdb"),
    state_code=40,  # Oklahoma FIPS code
    show_progress=True
)

print(f"Converted {len(row_counts)} tables")
print(f"Total rows: {sum(row_counts.values()):,}")
```

#### Building Multi-State Databases

##### Option 1: Merge Multiple States at Once
```python
from pyfia.converter import merge_states

# Create a multi-state database from multiple SQLite files
results = merge_states(
    source_paths=[
        Path("SQLite_FIADB_OK.db"), 
        Path("SQLite_FIADB_TX.db"), 
        Path("SQLite_FIADB_AL.db")
    ],
    state_codes=[40, 48, 1],  # Oklahoma, Texas, Alabama
    target_path=Path("nfi_south.duckdb"),
    show_progress=True
)

print(f"Merged {len(results)} states into regional database")
```

##### Option 2: Build Incrementally with Append
```python
from pyfia.converter import convert_sqlite_to_duckdb, append_state

# Step 1: Create initial database with first state
convert_sqlite_to_duckdb(
    source_path=Path("SQLite_FIADB_OK.db"),
    target_path=Path("nfi_south.duckdb"),
    state_code=40
)

# Step 2: Append additional states
append_state(
    source_path=Path("SQLite_FIADB_TX.db"),
    target_path=Path("nfi_south.duckdb"),
    state_code=48,
    dedupe=False  # No deduplication needed for new states
)

# Step 3: Continue appending
append_state(
    source_path=Path("SQLite_FIADB_AL.db"),
    target_path=Path("nfi_south.duckdb"),
    state_code=1
)
```

#### Handling Updates and Deduplication

```python
# When appending updated data for an existing state, use deduplication
append_state(
    source_path=Path("SQLite_FIADB_TX_2024_update.db"),
    target_path=Path("nfi_south.duckdb"),
    state_code=48,
    dedupe=True,
    dedupe_keys=["CN"]  # Remove duplicates based on CN field
)
```

#### Utility Functions

```python
from pyfia.converter import get_database_info, compare_databases

# Get information about a DuckDB database
info = get_database_info(Path("nfi_south.duckdb"))
print(f"Database size: {info['file_size_mb']:.2f} MB")
print(f"Total tables: {info['total_tables']}")
print(f"Total rows: {info['total_rows']:,}")

# Compare source and target databases
comparison = compare_databases(
    source_path=Path("SQLite_FIADB_OK.db"),
    target_path=Path("oklahoma.duckdb")
)
print(f"Compression ratio: {comparison['compression_ratio']:.2f}x")
```

#### Practical Examples

##### Example 1: Create Regional Database
```python
from pyfia.converter import convert_sqlite_to_duckdb, append_state

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
    source_path=Path(first_state[0]),
    target_path=Path("nfi_south.duckdb"),
    state_code=first_state[1]
)

# Append remaining states
for sqlite_file, state_code in list(states.items())[1:]:
    append_state(
        source_path=Path(sqlite_file),
        target_path=Path("nfi_south.duckdb"),
        state_code=state_code
    )
```

##### Example 2: Update Existing State Data
```python
# Check what's currently in the database
from pyfia import FIA

with FIA("nfi_south.duckdb") as db:
    conn = db.conn if hasattr(db, 'conn') else db.reader.conn
    current_plots = conn.execute(
        "SELECT COUNT(*) FROM PLOT WHERE STATECD = 48"
    ).fetchone()[0]
    print(f"Current Texas plots: {current_plots:,}")

# Append with deduplication to update
append_state(
    source_path=Path("SQLite_FIADB_TX_latest.db"),
    target_path=Path("nfi_south.duckdb"),
    state_code=48,
    dedupe=True,
    dedupe_keys=["CN"]
)
```

#### Performance Benefits
- **5-10x faster** analytical queries using DuckDB's columnar storage
- **5-6x compression ratio** through automatic compression
- **True append mode** adds new states without removing existing data
- **Memory efficiency** with streaming processing

#### Key Features
- **Simple API**: Direct functions, no complex configuration objects
- **FIA Standard Schemas**: Uses official FIA data types from YAML definitions
- **Automatic Compression**: DuckDB applies optimal compression
- **Multi-state Support**: Build regional or national databases incrementally
- **Deduplication**: Optional duplicate removal when updating existing data

#### Best Practices

1. **For New Databases**: Use `convert_sqlite_to_duckdb()` for the first state
2. **For Adding States**: Use `append_state()` with `dedupe=False`
3. **For Updates**: Use `append_state()` with `dedupe=True` and specify `dedupe_keys`
4. **For Large Datasets**: Tables are processed individually with progress tracking

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
from pyfia.constants import StateCodes

with duckdb.connect("nfi_south.duckdb", read_only=True) as conn:
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
        name = StateCodes.CODE_TO_NAME.get(state_code, f"Unknown ({state_code})")
        print(f"  {name}: {plots:,} plots, {years} inventory years ({min_year}-{max_year})")
```

## Refactoring Guidelines

### When to Simplify

If you encounter code with these characteristics, consider simplifying:

1. **Deep nesting**: More than 3 levels of directory/module nesting
2. **Unnecessary patterns**: Strategy, Factory, Builder patterns without clear benefit
3. **Pass-through layers**: Classes/functions that just forward calls
4. **Configuration objects**: Complex configs for simple parameter passing
5. **Abstract base classes**: When a simple function would suffice

### How to Simplify

1. **Replace class hierarchies with functions** when there's no state to maintain
2. **Use direct parameters** instead of configuration objects
3. **Flatten directory structures** to maximum 3 levels
4. **Remove abstraction layers** that don't add value
5. **Combine related small files** into cohesive modules

### Example of Good Simplification

**Before** (over-engineered):
```python
class ConfigBuilder:
    def with_state(self, state): ...
    def with_options(self, options): ...
    def build(self): return Config(...)

class AbstractProcessor(ABC):
    @abstractmethod
    def process(self): pass

class ConcreteProcessor(AbstractProcessor):
    def __init__(self, config: Config): ...
    def process(self): ...

# Usage
config = ConfigBuilder().with_state(37).with_options({...}).build()
processor = ConcreteProcessor(config)
result = processor.process()
```

**After** (simple):
```python
def process_data(state: int, **options):
    # Direct implementation
    return result

# Usage
result = process_data(state=37, option1=value1, option2=value2)
```

## Documentation Standards

### Public API Documentation Guidelines

All public-facing API functions in pyFIA must follow NumPy-style docstrings with comprehensive documentation. The `mortality()` function serves as the reference implementation for documentation standards.

#### Required Docstring Sections (in order)

1. **Summary Line**: One-line description of what the function does
2. **Extended Summary**: 2-4 lines explaining the approach and methodology
3. **Parameters**: Detailed parameter descriptions with types and valid values
4. **Returns**: Complete description of return values with types
5. **See Also**: Related functions and reference material
6. **Notes**: Technical details, algorithms, and important information
7. **Warnings** (if applicable): Known limitations or caveats
8. **Raises** (if applicable): Exceptions that may be raised
9. **Examples**: Multiple practical examples showing common use cases

#### Parameter Documentation Best Practices

For parameters that accept column names (like `grp_by`):
- List common valid values with descriptions
- Reference external documentation for complete lists
- Use format: `parameter_name : type, optional/default value`
- Include value enumerations in curly braces for limited choices: `{'option1', 'option2'}`

Example from mortality():
```python
grp_by : str or list of str, optional
    Column name(s) to group results by. Can be any column from the 
    FIA tables used in the estimation (PLOT, COND, TREE_GRM_COMPONENT,
    TREE_GRM_MIDPT). Common grouping columns include:
    
    - 'FORTYPCD': Forest type code
    - 'OWNGRPCD': Ownership group (10=National Forest, 20=Other Federal,
      30=State/Local, 40=Private)
    - 'STATECD': State FIPS code
    ...
```

#### Returns Section Format

Structure return documentation clearly with field names, types, and descriptions:
```python
Returns
-------
pl.DataFrame
    Mortality estimates with the following columns:
    
    - **MORT_ACRE** : float
        Annual mortality per acre in units specified by 'measure'
    - **MORT_TOTAL** : float (if totals=True)
        Total annual mortality expanded to population level
    ...
```

#### Examples Section Guidelines

- Start each example with a descriptive header (not a comment)
- Show practical, runnable code
- Include brief output examples where helpful
- Progress from simple to complex use cases

#### See Also Section

Link to related functions and reference materials:
- Related estimation functions in pyFIA
- Relevant constant definitions
- Reference table utilities
- External documentation sources

#### Notes Section

Include:
- Algorithm descriptions
- Valid column sources for grouping parameters
- References to official documentation
- Technical implementation details
- Database table requirements

### Documentation Maintenance

When adding new public API functions:
1. Follow the mortality() function as a template
2. Ensure all required sections are present and in correct order
3. Document all parameters thoroughly with types and valid values
4. Provide multiple practical examples
5. Cross-reference related functions in See Also
6. Keep line length under 88 characters for readability

## Important Notes

- **No backward compatibility needed**: When refactoring, don't maintain old APIs
- **YAML schemas are essential**: Keep YAML schemas as source of truth for FIA tables
- **Performance over abstractions**: Choose direct, fast implementations
- **Readability matters**: Clear, simple code is better than clever abstractions
- **Documentation completeness**: Follow mortality() function as the gold standard for API documentation