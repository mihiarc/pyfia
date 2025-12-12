# Downloading FIA Data

PyFIA can download FIA data directly from the USDA Forest Service [FIA DataMart](https://apps.fs.usda.gov/fia/datamart/datamart.html), similar to rFIA's `getFIA()` function in R.

## Quick Start

```python
from pyfia import download, FIA, area

# Download Georgia FIA data
db_path = download("GA")

# Use immediately
with FIA(db_path) as db:
    db.clip_most_recent()
    result = area(db, land_type="forest")
    print(result)
```

## Basic Usage

### Download a Single State

```python
from pyfia import download

# Download Georgia data (saves to ~/.pyfia/data/)
db_path = download("GA")
print(f"Downloaded to: {db_path}")
```

### Download Multiple States

```python
# Download and merge multiple states into a single database
db_path = download(["GA", "FL", "SC"])
```

### Specify Download Directory

```python
# Download to a specific directory
db_path = download("GA", dir="./data")
```

## Download Options

### Common Tables Only (Default)

By default, PyFIA downloads only the tables required for analysis functions:

```python
# Downloads ~20 essential tables (PLOT, TREE, COND, etc.)
db_path = download("GA", common=True)  # This is the default
```

### All Tables

```python
# Download all available tables (larger download)
db_path = download("GA", common=False)
```

### Specific Tables

```python
# Download only the tables you need
db_path = download("GA", tables=["PLOT", "TREE", "COND", "SURVEY"])
```

## Caching

PyFIA caches downloaded data to avoid re-downloading:

```python
# First call downloads from FIA DataMart
db_path = download("GA")

# Subsequent calls use cached data
db_path = download("GA")  # Returns cached path instantly
```

### Force Re-download

```python
# Re-download even if cached data exists
db_path = download("GA", force=True)
```

### Manage Cache

```python
from pyfia.downloader import clear_cache, cache_info

# View cache information
info = cache_info()
print(f"Cache size: {info['total_size_mb']:.1f} MB")
print(f"Cached states: {info['states']}")

# Clear old cache entries
cleared = clear_cache(older_than_days=90)
print(f"Cleared {cleared} entries")

# Clear cache for a specific state
clear_cache(state="GA", delete_files=True)
```

## FIA Class Integration

### Using `from_download()`

The `FIA` class provides a convenience method to download and open in one step:

```python
from pyfia import FIA

# Download and open Georgia data
db = FIA.from_download("GA")

# Ready to use immediately
db.clip_most_recent()
result = db.get_plots().collect()
```

### Using with Context Manager

```python
from pyfia import download, FIA

with FIA(download("GA")) as db:
    db.clip_most_recent()
    # Run analyses...
```

## Common Tables

When `common=True` (default), PyFIA downloads these tables:

| Table | Description |
|-------|-------------|
| PLOT | Plot-level data |
| TREE | Tree measurements |
| COND | Condition data |
| SUBPLOT | Subplot data |
| SEEDLING | Seedling data |
| SURVEY | Survey metadata |
| POP_EVAL | Population evaluations |
| POP_EVAL_GRP | Evaluation groups |
| POP_EVAL_TYP | Evaluation types |
| POP_STRATUM | Stratum definitions |
| POP_ESTN_UNIT | Estimation units |
| POP_PLOT_STRATUM_ASSGN | Plot stratum assignments |
| TREE_GRM_COMPONENT | Growth/removal/mortality components |
| TREE_GRM_MIDPT | GRM midpoint values |
| TREE_GRM_BEGIN | GRM beginning values |
| And more... | Additional tables for complete analysis |

## Download Performance

!!! tip "Download Times"
    Download times vary by state size and internet connection:

    - **Small states** (RI, DE): ~1-2 minutes
    - **Medium states** (GA, NC): ~5-10 minutes
    - **Large states** (CA, TX): ~15-30 minutes

!!! warning "Disk Space"
    Some states have large TREE tables. Ensure sufficient disk space:

    - Small states: ~50-200 MB
    - Medium states: ~500 MB - 1 GB
    - Large states: 2-5 GB

## Comparison with rFIA

| Feature | rFIA (R) | PyFIA (Python) |
|---------|----------|----------------|
| Download state | `getFIA(states='GA')` | `download("GA")` |
| Multiple states | `getFIA(states=c('GA','FL'))` | `download(["GA", "FL"])` |
| Common tables | `common=TRUE` | `common=True` |
| Specific tables | `tables=c('PLOT')` | `tables=["PLOT"]` |
| Save location | `dir='/path'` | `dir="/path"` |
| Output format | R data.frame | DuckDB database |

## Complete Example

```python
from pyfia import download, FIA, area, volume

# Download Rhode Island data (smallest state, good for testing)
db_path = download("RI")

# Analyze forest area and volume
with FIA(db_path) as db:
    db.clip_most_recent()

    # Forest area by ownership
    forest_area = area(db, land_type="forest", grp_by="OWNGRPCD")
    print("Forest Area by Ownership:")
    print(forest_area)

    # Volume by species
    vol = volume(db, grp_by="SPCD")
    print("\nVolume by Species:")
    print(vol.head(10))
```

## Troubleshooting

### Network Errors

If downloads fail due to network issues:

```python
# Increase timeout and retry
from pyfia.downloader import DataMartClient

client = DataMartClient(timeout=600, max_retries=5)
# Use client directly for more control
```

### Incomplete Downloads

If a download is interrupted:

```python
# Force re-download to get fresh data
db_path = download("GA", force=True)
```

### Cache Issues

If cached data seems corrupted:

```python
from pyfia.downloader import clear_cache

# Clear specific state cache
clear_cache(state="GA", delete_files=True)

# Re-download
db_path = download("GA")
```

## API Reference

For complete API documentation, see the [Data Download API Reference](../api/downloader.md).
