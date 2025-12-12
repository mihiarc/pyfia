# Enhancement Plan: FIA Data Download Feature

> **Goal**: Enable pyFIA users to download FIA data directly from the FIA DataMart, similar to rFIA's `getFIA()` function.

## Status: Implemented

This feature has been implemented and is available in pyFIA.

---

## Summary

pyFIA now includes programmatic download capabilities that retrieve data from the [FIA DataMart](https://apps.fs.usda.gov/fia/datamart/datamart.html), matching the UX of rFIA in R.

---

## API Design

### Primary Interface

```python
from pyfia import download

# Download single state (returns path to DuckDB)
db_path = download("GA")

# Download multiple states (merged into single DuckDB)
db_path = download(["GA", "FL", "SC"])

# Download to specific directory
db_path = download("GA", dir="./data")

# Download only common tables (default=True)
db_path = download("GA", common=True)

# Download specific tables
db_path = download("GA", tables=["PLOT", "TREE", "COND"])
```

### Full Function Signature

```python
def download(
    states: Union[str, List[str]],
    dir: Optional[Union[str, Path]] = None,
    common: bool = True,
    tables: Optional[List[str]] = None,
    force: bool = False,
    show_progress: bool = True,
    use_cache: bool = True,
) -> Path:
    """
    Download FIA data from the FIA DataMart.

    Parameters
    ----------
    states : str or list of str
        State abbreviations (e.g., 'GA', 'NC').
        Supports multiple states: ['GA', 'FL', 'SC']
    dir : str or Path, optional
        Directory to save downloaded data. Defaults to ~/.pyfia/data/
    common : bool, default True
        If True, download only tables required for pyFIA functions.
        If False, download all available tables.
    tables : list of str, optional
        Specific tables to download. Overrides `common` parameter.
    force : bool, default False
        If True, re-download even if files exist locally.
    show_progress : bool, default True
        Show download progress bars (uses rich).
    use_cache : bool, default True
        Use cached downloads if available.

    Returns
    -------
    Path
        Path to the DuckDB database file.

    Raises
    ------
    StateNotFoundError
        If invalid state code provided.
    DownloadError
        If download fails (network issues, file not found).
    """
```

### Integration with FIA Class

```python
from pyfia import FIA

# New class method for convenience
db = FIA.from_download("GA")

# Use immediately
with FIA(download("GA")) as db:
    db.clip_most_recent()
    result = area(db)
```

---

## Design Decisions

### DuckDB-Only Output

The downloader outputs only DuckDB format. This simplifies the API and aligns with pyFIA's architecture:

- **DuckDB is pyFIA's native format**: All pyFIA functions expect DuckDB
- **No conversion needed**: Users don't need to specify format or convert later
- **Simpler caching**: Only one format to cache and track
- **Better UX**: `download("GA")` returns a ready-to-use database

### CSV Download Pipeline

Data is downloaded as CSV from the FIA DataMart (individual table ZIP files), then converted to DuckDB:

```
Download ZIPs → Extract CSVs → Convert to DuckDB → Clean up temp files
```

This approach allows:
- Selective table downloads (`common=True` or specific tables)
- Adding STATE_ADDED column during conversion
- Multi-state merging into single database

---

## Technical Architecture

### Module Structure

```
src/pyfia/
├── downloader/
│   ├── __init__.py            # Public API: download()
│   ├── client.py              # DataMartClient - HTTP download logic
│   ├── tables.py              # Table definitions (COMMON_TABLES, etc.)
│   ├── cache.py               # Cache management, checksums
│   └── exceptions.py          # DownloadError, etc.
├── core/
│   └── fia.py                 # FIA.from_download() class method
└── __init__.py                # Export download function
```

### FIA DataMart URL Structure

```
Base URL: https://apps.fs.usda.gov/fia/datamart/CSV/

State data:     {STATE}_{TABLE}.zip  (e.g., GA_PLOT.zip)
Reference:      REF_{TABLE}.zip      (e.g., REF_SPECIES.zip)
Entire US:      {TABLE}.zip          (e.g., PLOT.zip)
```

### Common Tables (required for analysis)

```python
COMMON_TABLES = [
    "COND",                    # Condition data
    "COND_DWM_CALC",          # Down woody material calculations
    "INVASIVE_SUBPLOT_SPP",   # Invasive species
    "PLOT",                   # Plot data
    "POP_ESTN_UNIT",          # Population estimation units
    "POP_EVAL",               # Population evaluations
    "POP_EVAL_GRP",           # Population evaluation groups
    "POP_EVAL_TYP",           # Population evaluation types
    "POP_PLOT_STRATUM_ASSGN", # Plot stratum assignments
    "POP_STRATUM",            # Stratum definitions
    "SUBPLOT",                # Subplot data
    "TREE",                   # Tree data (largest table)
    "TREE_GRM_COMPONENT",     # Growth/removal/mortality components
    "TREE_GRM_MIDPT",         # GRM midpoint values
    "TREE_GRM_BEGIN",         # GRM beginning values
    "SUBP_COND_CHNG_MTRX",   # Subplot condition change matrix
    "SEEDLING",               # Seedling data
    "SURVEY",                 # Survey metadata
    "SUBP_COND",              # Subplot condition
    "P2VEG_SUBP_STRUCTURE",   # Vegetation structure
]
```

---

## Caching

The download cache tracks:
- Downloaded DuckDB files by state
- Download timestamps
- MD5 checksums
- Cache age (warns if >90 days old)

```python
# Cache is checked automatically
db_path = download("GA")  # Uses cache if available

# Force re-download
db_path = download("GA", force=True)

# Clear cache
from pyfia.downloader import clear_cache
clear_cache(older_than_days=90)
```

---

## Comparison with rFIA

| Feature | rFIA | pyFIA |
|---------|------|-------|
| Download states | `getFIA(states='GA')` | `download("GA")` |
| Multiple states | `getFIA(states=c('GA','FL'))` | `download(["GA", "FL"])` |
| Common tables only | `common=TRUE` | `common=True` |
| Specific tables | `tables=c('PLOT')` | `tables=["PLOT"]` |
| Save to disk | `dir='/path'` | `dir="/path"` |
| Output format | R data.frame | DuckDB |

---

## Quick Start

```python
from pyfia import download, FIA, area

# Download Georgia FIA data (downloads to ~/.pyfia/data/)
db_path = download("GA")
print(f"Downloaded to: {db_path}")

# Analyze immediately
with FIA(db_path) as db:
    db.clip_most_recent()
    result = area(db, land_type="forest")
    print(result)
```

---

## References

- [rFIA Documentation](https://doserlab.com/files/rfia/reference/getfia)
- [FIA DataMart](https://apps.fs.usda.gov/fia/datamart/datamart.html)
- [FIESTA Package](https://usdaforestservice.github.io/FIESTA/articles/FIESTA_tutorial_DB.html)
