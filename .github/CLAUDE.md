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

# Setup pre-commit hooks
uv run pyfia-setup-precommit
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
- Must filter by EVALID for proper statistical estimates
- Use `most_recent=True` for latest evaluations by default

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

## Database Conversion

### SQLite to DuckDB Converter

pyFIA includes a comprehensive converter for transforming FIA DataMart SQLite databases to optimized DuckDB format:

#### CLI Usage
```bash
# Convert single state database
uv run pyfia-convert convert --source OR_FIA.db --target oregon.duckdb --state-code 41

# Merge multiple states
uv run pyfia-convert merge --source-dir /data/fia/ --target pacific_states.duckdb --states "6,41,53"

# Validate converted database
uv run pyfia-convert validate --database oregon.duckdb --validation comprehensive

# Get database information
uv run pyfia-convert info --database oregon.duckdb
```

#### Programmatic Usage
```python
from pyfia.converter import FIAConverter, ConverterConfig

# Single state conversion
config = ConverterConfig(
    source_dir=Path("/data/fia/sqlite"),
    target_path=Path("/data/fia/oregon.duckdb"),
    compression_level="medium",
    validation_level="standard"
)

converter = FIAConverter(config)
result = converter.convert_state(
    sqlite_path=Path("/data/fia/sqlite/OR_FIA.db"),
    state_code=41
)

print(result.summary())
```

#### Performance Benefits
- **5-10x faster** analytical queries using DuckDB's columnar storage
- **30-50% smaller** database size through compression and optimization
- **Optimized indexing** for common FIA query patterns (EVALID, STATECD, spatial)
- **Memory efficiency** with lazy evaluation and batch processing

#### Conversion Features
- **Schema optimization**: Automatic data type optimization for FIA columns
- **Multi-state merging**: Intelligent conflict resolution for boundary plots
- **Data validation**: Comprehensive integrity checks and statistical validation
- **Error recovery**: Checkpointing and rollback capabilities
- **Progress tracking**: Rich progress bars and detailed reporting

#### Configuration Options
```python
config = ConverterConfig(  
    batch_size=100_000,           # Records per batch
    parallel_workers=4,           # Parallel processing threads
    memory_limit="4GB",           # DuckDB memory limit
    compression_level="medium",   # none, low, medium, high, adaptive
    validation_level="standard",  # none, basic, standard, comprehensive
    create_indexes=True,          # Create optimized indexes
    show_progress=True           # Display progress bars
)
```