# Getting Started

This guide will help you install PyFIA and run your first analysis.

## Installation

### Using pip

```bash
pip install pyfia
```

### Using uv (recommended)

```bash
uv pip install pyfia
```

### Development Installation

```bash
git clone https://github.com/mihiarc/pyfia.git
cd pyfia
uv venv && source .venv/bin/activate
uv pip install -e .[dev]
```

## Prerequisites

### FIA Database

PyFIA requires a DuckDB database containing FIA data. You can either:

1. **Download pre-converted databases** from [FIA DataMart](https://apps.fs.usda.gov/fia/datamart/datamart.html)
2. **Convert SQLite databases** using PyFIA's conversion utilities

```python
import pyfia

# Convert a SQLite FIA database to DuckDB
pyfia.convert_sqlite_to_duckdb(
    "GA_FIADB_ENTIRE.db",  # Source SQLite
    "georgia.duckdb"        # Target DuckDB
)
```

## Quick Start

### 1. Connect to Database

```python
import pyfia

# Create database connection
db = pyfia.FIA("georgia.duckdb")
```

### 2. Filter to Your Region

```python
# Filter to Georgia, most recent evaluation
db.clip_by_state("GA")

# Or filter by specific EVALID
db.clip_by_evalid([132403, 132404])
```

### 3. Run an Analysis

```python
# Calculate forest area
forest_area = pyfia.area(db, land_type="forest")
print(forest_area)

# Calculate volume by species
volume = pyfia.volume(db, grp_by="SPCD")
print(volume)

# Get mortality estimates
mortality = pyfia.mortality(db, measure="volume")
print(mortality)
```

### 4. Add Descriptive Names

```python
# Join species names to results
volume_with_names = pyfia.join_species_names(volume, db)
print(volume_with_names)
```

## Understanding Results

All estimation functions return a Polars DataFrame with:

| Column | Description |
|--------|-------------|
| `estimate` | Point estimate |
| `variance` | Estimated variance |
| `se` | Standard error |
| `cv` | Coefficient of variation (%) |
| `ci_lower` | Lower 95% confidence bound |
| `ci_upper` | Upper 95% confidence bound |

## Configuration

PyFIA uses environment variables for default settings:

```bash
# Set default database path
export PYFIA_DATABASE_PATH=/path/to/fia.duckdb

# Set database engine (duckdb or sqlite)
export PYFIA_DATABASE_ENGINE=duckdb
```

Or configure programmatically:

```python
from pyfia import settings

settings.database_path = "/path/to/fia.duckdb"
settings.max_threads = 8
```

## Next Steps

- **[User Guide](guides/index.md)**: Learn about domain filtering and advanced workflows
- **[API Reference](api/index.md)**: Detailed function documentation
- **[Examples](examples.md)**: Real-world analysis examples

## Getting Help

- **Documentation**: [https://mihiarc.github.io/pyfia/](https://mihiarc.github.io/pyfia/)
- **Issues**: [GitHub Issues](https://github.com/mihiarc/pyfia/issues)
- **Discussions**: [GitHub Discussions](https://github.com/mihiarc/pyfia/discussions)
