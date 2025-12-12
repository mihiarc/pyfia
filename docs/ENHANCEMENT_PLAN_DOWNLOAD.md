# Enhancement Plan: FIA Data Download Feature

> **Goal**: Enable pyFIA users to download FIA data directly from the FIA DataMart, similar to rFIA's `getFIA()` function.

## Executive Summary

pyFIA currently requires users to manually download FIA SQLite databases from the [FIA DataMart](https://apps.fs.usda.gov/fia/datamart/datamart.html). This enhancement will add programmatic download capabilities, reducing friction and matching the UX of rFIA in R.

---

## Research Findings

### How rFIA Does It

The rFIA package provides `getFIA()` which:
- Downloads state-level CSV ZIP files from `https://apps.fs.usda.gov/fia/datamart/CSV/`
- URL pattern: `{STATE}_{TABLE}.zip` (e.g., `GA_PLOT.zip`, `NC_TREE.zip`)
- Supports parallel downloads via `nCores` parameter
- Has `common=TRUE` to download only essential tables (conserves bandwidth)
- Can download reference tables with `states='REF'`
- Merges multi-state data automatically

### FIA DataMart URL Structure

```
Base URL: https://apps.fs.usda.gov/fia/datamart/CSV/

State data:     {STATE}_{TABLE}.zip  (e.g., GA_PLOT.zip)
Reference:      REF_{TABLE}.zip      (e.g., REF_SPECIES.zip)
Entire US:      {TABLE}.zip          (e.g., PLOT.zip)

Archives:
- CSV_FIADB_ENTIRE.zip        (~10GB)
- FIADB_REFERENCE.zip         (all reference tables)
- SQLite_FIADB_ENTIRE.zip     (~10GB SQLite database)

State SQLite:
https://apps.fs.usda.gov/fia/datamart/Databases/{STATE}.zip
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

REFERENCE_TABLES = [
    "REF_SPECIES",
    "REF_FOREST_TYPE",
    "REF_FOREST_TYPE_GROUP",
    "REF_CITATION",
    # ... etc
]
```

---

## Proposed API Design

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

# Download reference tables
ref_path = download("REF")

# Convenience function for entire US (with warning about size)
db_path = download_entire_us(dir="./data")
```

### Full Function Signature

```python
def download(
    states: Union[str, List[str]],
    dir: Optional[Union[str, Path]] = None,
    common: bool = True,
    tables: Optional[List[str]] = None,
    format: Literal["duckdb", "sqlite", "csv"] = "duckdb",
    force: bool = False,
    show_progress: bool = True,
) -> Path:
    """
    Download FIA data from the FIA DataMart.

    Parameters
    ----------
    states : str or list of str
        State abbreviations (e.g., 'GA', 'NC') or 'REF' for reference tables.
        Supports multiple states: ['GA', 'FL', 'SC']
    dir : str or Path, optional
        Directory to save downloaded data. Defaults to ~/.pyfia/data/
    common : bool, default True
        If True, download only tables required for pyFIA functions.
        If False, download all available tables.
    tables : list of str, optional
        Specific tables to download. Overrides `common` parameter.
    format : {'duckdb', 'sqlite', 'csv'}, default 'duckdb'
        Output format. DuckDB is recommended for pyFIA workflows.
    force : bool, default False
        If True, re-download even if files exist locally.
    show_progress : bool, default True
        Show download progress bars (uses rich).

    Returns
    -------
    Path
        Path to the downloaded/converted database file.

    Raises
    ------
    ValueError
        If invalid state code provided.
    DownloadError
        If download fails (network issues, file not found).

    Examples
    --------
    >>> from pyfia import download, FIA
    >>>
    >>> # Download Georgia data
    >>> db_path = download("GA")
    >>>
    >>> # Use immediately with pyFIA
    >>> with FIA(db_path) as db:
    ...     db.clip_most_recent()
    ...     result = area(db)
    """
```

### Integration with FIA Class

```python
from pyfia import FIA

# New class method for convenience
db = FIA.from_download("GA")

# Or with existing instance
db = FIA.download_and_open("GA")
```

---

## Technical Architecture

### Module Structure

```
src/pyfia/
├── downloader/                 # NEW MODULE
│   ├── __init__.py            # Public API: download(), download_entire_us()
│   ├── client.py              # DataMartClient - HTTP download logic
│   ├── tables.py              # Table definitions (COMMON_TABLES, etc.)
│   ├── cache.py               # Cache management, checksums
│   └── exceptions.py          # DownloadError, etc.
├── core/
│   └── fia.py                 # Add FIA.from_download() class method
└── __init__.py                # Export download function
```

### Key Classes

```python
# downloader/client.py
class DataMartClient:
    """
    HTTP client for FIA DataMart downloads.

    Similar pattern to EVALIDatorClient but for file downloads.
    """
    BASE_URL = "https://apps.fs.usda.gov/fia/datamart/CSV/"
    SQLITE_URL = "https://apps.fs.usda.gov/fia/datamart/Databases/"

    def __init__(
        self,
        timeout: int = 300,
        chunk_size: int = 1024 * 1024,  # 1MB chunks
    ):
        self.timeout = timeout
        self.chunk_size = chunk_size
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "pyFIA/1.0 (download client)"
        })

    def download_table(
        self,
        state: str,
        table: str,
        dest_dir: Path,
        show_progress: bool = True,
    ) -> Path:
        """Download a single table ZIP file."""
        ...

    def download_state_sqlite(
        self,
        state: str,
        dest_dir: Path,
    ) -> Path:
        """Download pre-built state SQLite database."""
        ...


# downloader/cache.py
@dataclass
class CachedDownload:
    """Metadata for a cached download."""
    state: str
    table: str
    path: Path
    downloaded_at: datetime
    size_bytes: int
    checksum: str  # MD5 or SHA256

class DownloadCache:
    """Manages cached downloads with metadata."""

    def __init__(self, cache_dir: Path):
        self.cache_dir = cache_dir
        self.metadata_file = cache_dir / "downloads.json"

    def get_cached(self, state: str, table: str) -> Optional[Path]:
        """Return cached file path if exists and valid."""
        ...

    def add_to_cache(self, state: str, table: str, path: Path) -> None:
        """Add download to cache with metadata."""
        ...

    def clear_cache(self, older_than: Optional[timedelta] = None) -> None:
        """Clear cached downloads."""
        ...
```

### Settings Integration

```python
# core/settings.py - additions
class PyFIASettings(BaseSettings):
    # ... existing settings ...

    # Download settings
    download_cache_dir: Path = Field(
        default=Path.home() / ".pyfia" / "data",
        description="Directory for downloaded FIA data"
    )
    download_timeout: int = Field(
        default=300,
        description="Download timeout in seconds"
    )
    download_chunk_size: int = Field(
        default=1024 * 1024,
        description="Download chunk size in bytes"
    )
    download_verify_checksums: bool = Field(
        default=True,
        description="Verify file checksums after download"
    )
```

---

## Implementation Plan

### Phase 1: Core Download Client (Priority: High)

**Files to create:**
1. `src/pyfia/downloader/__init__.py` - Public API
2. `src/pyfia/downloader/client.py` - HTTP download logic
3. `src/pyfia/downloader/tables.py` - Table definitions
4. `src/pyfia/downloader/exceptions.py` - Custom exceptions

**Key functionality:**
- Download single table ZIP files
- Extract CSV from ZIP
- Progress bar with rich
- Retry logic for network failures
- State code validation

### Phase 2: CSV to DuckDB Pipeline (Priority: High)

**Integration:**
- Leverage existing `converter.convert_sqlite_to_duckdb()`
- Add new `csv_to_duckdb()` function
- Handle multiple CSVs → single DuckDB

**Flow:**
```
Download ZIPs → Extract CSVs → Convert to DuckDB → Clean up temp files
```

### Phase 3: Caching Layer (Priority: Medium)

**Features:**
- Track downloaded files with metadata
- Skip re-downloads if fresh
- Checksum verification
- Cache expiration policy
- CLI command to clear cache

### Phase 4: Multi-State Merging (Priority: Medium)

**Leverage existing:**
- `converter.merge_states()` - already handles this
- Add state_code column during import
- Handle duplicate CNs across states

### Phase 5: FIA Class Integration (Priority: Low)

**Convenience methods:**
```python
@classmethod
def from_download(cls, states, **kwargs):
    db_path = download(states, **kwargs)
    return cls(db_path)
```

---

## Error Handling

```python
# downloader/exceptions.py
class DownloadError(Exception):
    """Base exception for download errors."""
    pass

class StateNotFoundError(DownloadError):
    """Invalid state code provided."""
    pass

class TableNotFoundError(DownloadError):
    """Requested table not available for state."""
    pass

class NetworkError(DownloadError):
    """Network-related download failure."""
    pass

class ChecksumError(DownloadError):
    """Downloaded file failed checksum verification."""
    pass
```

---

## Progress Reporting

Using `rich` (already a dependency):

```python
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn

def download_with_progress(url: str, dest: Path, description: str) -> None:
    with Progress(
        SpinnerColumn(),
        TextColumn("[bold blue]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
    ) as progress:
        task = progress.add_task(description, total=file_size)

        with requests.get(url, stream=True) as response:
            with open(dest, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
                    progress.update(task, advance=len(chunk))
```

---

## Testing Strategy

### Unit Tests
- State code validation
- URL construction
- Cache hit/miss logic
- Checksum verification

### Integration Tests (with real downloads)
- Download small reference table
- Download single state (use smallest state)
- Multi-state merge
- CSV → DuckDB conversion pipeline

### Mock Tests
- Network failure handling
- Retry logic
- Progress reporting

```python
# tests/test_downloader.py
def test_download_reference_tables():
    """Download reference tables (small, fast test)."""
    path = download("REF", tables=["REF_SPECIES"])
    assert path.exists()

def test_download_state_common_tables():
    """Download common tables for Rhode Island (smallest state)."""
    path = download("RI", common=True)
    with FIA(path) as db:
        assert db.table_exists("PLOT")
        assert db.table_exists("TREE")
```

---

## User Documentation

### Quick Start

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

### CLI Integration (Future)

```bash
# Download via command line
pyfia download GA --dir ./data
pyfia download GA FL SC --common
pyfia cache clear  # Clear download cache
pyfia cache info   # Show cache statistics
```

---

## Comparison with rFIA

| Feature | rFIA | pyFIA (proposed) |
|---------|------|------------------|
| Download states | `getFIA(states='GA')` | `download("GA")` |
| Multiple states | `getFIA(states=c('GA','FL'))` | `download(["GA", "FL"])` |
| Common tables only | `common=TRUE` | `common=True` |
| Specific tables | `tables=c('PLOT')` | `tables=["PLOT"]` |
| Save to disk | `dir='/path'` | `dir="/path"` |
| Parallel download | `nCores=4` | Planned (Phase 6) |
| Reference tables | `states='REF'` | `"REF"` |
| Output format | R data.frame | DuckDB (recommended) |
| Auto-conversion | N/A (R native) | CSV → DuckDB |

---

## Timeline Estimate

| Phase | Effort | Dependencies |
|-------|--------|--------------|
| Phase 1: Core client | 2-3 days | None |
| Phase 2: DuckDB pipeline | 1-2 days | Phase 1 |
| Phase 3: Caching | 1 day | Phase 1 |
| Phase 4: Multi-state | 1 day | Phase 2 |
| Phase 5: FIA integration | 0.5 day | Phase 2 |
| Documentation | 1 day | All phases |
| **Total** | **~8 days** | |

---

## Open Questions

1. **SQLite vs CSV downloads?**
   - FIA DataMart provides pre-built state SQLite files
   - These are faster to download (single file)
   - But CSV allows table selection
   - **Recommendation**: Support both, default to SQLite for simplicity

2. **Parallel downloads?**
   - rFIA supports `nCores` for parallel table downloads
   - Could use `concurrent.futures` in Python
   - **Recommendation**: Defer to Phase 6, single-threaded is fine initially

3. **Cache invalidation?**
   - How often does FIA DataMart update?
   - Monthly? Quarterly? Annually?
   - **Recommendation**: Store download date, warn if >90 days old

4. **Disk space warnings?**
   - Some states (CA, TX) have very large TREE tables
   - **Recommendation**: Check available disk space, warn if insufficient

---

## References

- [rFIA Documentation](https://doserlab.com/files/rfia/reference/getfia)
- [FIA DataMart](https://apps.fs.usda.gov/fia/datamart/datamart.html)
- [FIESTA Package](https://usdaforestservice.github.io/FIESTA/articles/FIESTA_tutorial_DB.html)
- [fia-py-api](https://github.com/ashki23/fia-py-api)

---

## Summary

This enhancement brings pyFIA to feature parity with rFIA for data acquisition:

```python
# rFIA (R)
ct <- getFIA(states='CT', dir='/path/to/save')

# pyFIA (Python) - proposed
db_path = download("CT", dir="/path/to/save")
```

The implementation follows pyFIA's design principles:
- **Simple API**: `download("GA")` not `FIADownloaderFactory.create().download()`
- **Leverages existing code**: Uses converter module for DuckDB conversion
- **Follows EVALIDatorClient pattern**: Consistent HTTP client design
- **Rich progress**: Uses existing rich dependency for UX
