# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview
pyFIA is a Python library implementing the R rFIA package functionality for analyzing USDA Forest Inventory and Analysis (FIA) data. It provides exact statistical compatibility with rFIA while leveraging modern Python data science tools like Polars and DuckDB for high-performance data processing.

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
- Follow rFIA naming conventions in public APIs
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

## Important Notes

- **No backward compatibility needed**: When refactoring, don't maintain old APIs
- **YAML schemas are essential**: Keep YAML schemas as source of truth for FIA tables
- **Performance over abstractions**: Choose direct, fast implementations
- **Readability matters**: Clear, simple code is better than clever abstractions