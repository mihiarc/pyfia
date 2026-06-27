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

## Getting FIA Data

PyFIA can download FIA data directly from the USDA Forest Service DataMart:

```python
from pyfia import download

# Download Georgia FIA data
db_path = download("GA")
print(f"Downloaded to: {db_path}")
```

This downloads the data, converts it to DuckDB format, and returns the path to the database file.

### Download Options

```python
# Download multiple states (merged into single database)
db_path = download(["GA", "FL", "SC"])

# Download to a specific directory
db_path = download("GA", dir="./data")

# Download only specific tables
db_path = download("GA", tables=["PLOT", "TREE", "COND"])
```

For more details, see the [Downloading Data](guides/downloading.md) guide.

## Quick Start

### 1. Download and Connect

```python
from pyfia import download, FIA

# Download and open in one step
db = FIA.from_download("GA")

# Or download separately
db_path = download("GA")
db = FIA(db_path)
```

### 2. Filter to Your Region

```python
# Filter to Georgia, most recent evaluation
db.clip_by_state("GA")
db.clip_most_recent()

# Or filter by specific EVALID
db.clip_by_evalid([132403, 132404])
```

!!! warning "Always filter to a single evaluation before estimating"

    FIA databases contain multiple overlapping evaluations (EVALIDs) for each state.
    If you skip `clip_most_recent()` or `clip_by_evalid()`, plots will be counted
    multiple times and **your estimates will be wrong** — totals can be inflated by
    10-60x.

    **Rule of thumb:** Always call `db.clip_most_recent()` after `clip_by_state()`.
    For GRM estimates (growth, mortality, removals), use `eval_type="GRM"`:

    ```python
    db.clip_most_recent(eval_type="GRM")
    ```

    See the [FIA Technical Context](fia_technical_context.md) for details on the
    EVALID system and why this matters for statistical validity.

### 3. Run an Analysis

```python
import pyfia

# Calculate forest area
forest_area = pyfia.area(db, land_type="forest")
print(forest_area)

# Calculate volume by species
volume = pyfia.volume(db, grp_by="SPCD")
print(volume)

# Get mortality estimates (GRM estimates need GRM evaluation type)
db.clip_most_recent(eval_type="GRM")
mort = pyfia.mortality(db, measure="volume")
print(mort)
```

### 4. Add Descriptive Names

```python
# Join species names to results
volume_with_names = pyfia.join_species_names(volume, db)
print(volume_with_names)
```

## Complete Example

```python
from pyfia import download, FIA, area, volume

# Download Rhode Island data (smallest state, good for testing)
db_path = download("RI")

with FIA(db_path) as db:
    db.clip_most_recent()

    # Forest area
    forest_area = area(db, land_type="forest")
    print("Forest Area:")
    print(forest_area)

    # Volume by species
    vol = volume(db, grp_by="SPCD")
    print("\nVolume by Species:")
    print(vol.head(10))
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

- **[Downloading Data](guides/downloading.md)**: Learn about download options and caching
- **[User Guide](guides/index.md)**: Domain filtering and advanced workflows
- **[API Reference](api/index.md)**: Detailed function documentation
- **[Examples](examples.md)**: Real-world analysis examples

## Getting Help

- **Documentation**: [https://mihiarc.github.io/pyfia/](https://mihiarc.github.io/pyfia/)
- **Issues**: [GitHub Issues](https://github.com/mihiarc/pyfia/issues)
- **Discussions**: [GitHub Discussions](https://github.com/mihiarc/pyfia/discussions)
