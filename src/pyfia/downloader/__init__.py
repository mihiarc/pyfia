"""
FIA Data Download Module.

This module provides functionality to download FIA data directly from the
USDA Forest Service FIA DataMart, similar to rFIA's getFIA() function in R.

Downloads CSV files from FIA DataMart and converts them to DuckDB format
for use with pyFIA.

Examples
--------
>>> from pyfia import download
>>>
>>> # Download Georgia data (returns path to DuckDB database)
>>> db_path = download("GA")
>>>
>>> # Download multiple states (merged into single database)
>>> db_path = download(["GA", "FL", "SC"])
>>>
>>> # Download to specific directory
>>> db_path = download("GA", dir="./data")
>>>
>>> # Download only common tables (default)
>>> db_path = download("GA", common=True)

References
----------
- FIA DataMart: https://apps.fs.usda.gov/fia/datamart/datamart.html
- rFIA Package: https://doserlab.com/files/rfia/
"""

import logging
import tempfile
from pathlib import Path

from rich.console import Console

from pyfia.downloader.cache import DownloadCache
from pyfia.downloader.client import DataMartClient
from pyfia.downloader.exceptions import (
    ChecksumError,
    DownloadError,
    InsufficientSpaceError,
    NetworkError,
    StateNotFoundError,
    TableNotFoundError,
)
from pyfia.downloader.tables import (
    ALL_TABLES,
    COMMON_TABLES,
    REFERENCE_TABLES,
    STATE_FIPS_CODES,
    VALID_STATE_CODES,
    get_state_fips,
    get_tables_for_download,
    validate_state_code,
)

logger = logging.getLogger(__name__)
console = Console()

__all__ = [
    # Main function
    "download",
    # Client
    "DataMartClient",
    # Cache
    "DownloadCache",
    # Exceptions
    "DownloadError",
    "StateNotFoundError",
    "TableNotFoundError",
    "NetworkError",
    "ChecksumError",
    "InsufficientSpaceError",
    # Tables
    "COMMON_TABLES",
    "REFERENCE_TABLES",
    "ALL_TABLES",
    "VALID_STATE_CODES",
    "STATE_FIPS_CODES",
    # Utilities
    "validate_state_code",
    "get_state_fips",
    "get_tables_for_download",
]


def _get_default_data_dir() -> Path:
    """Get the default data directory."""
    from pyfia.core.settings import settings

    return settings.cache_dir.parent / "data"


def _convert_csvs_to_duckdb(
    csv_dir: Path,
    output_path: Path,
    state_code: int | None = None,
    show_progress: bool = True,
) -> Path:
    """
    Convert downloaded CSV files to DuckDB format.

    Parameters
    ----------
    csv_dir : Path
        Directory containing CSV files.
    output_path : Path
        Path for the output DuckDB file.
    state_code : int, optional
        State FIPS code to add as column.
    show_progress : bool
        Show progress messages.

    Returns
    -------
    Path
        Path to the created DuckDB file.
    """
    import duckdb

    csv_files = list(csv_dir.glob("*.csv")) + list(csv_dir.glob("*.CSV"))

    if not csv_files:
        raise DownloadError(f"No CSV files found in {csv_dir}")

    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect(str(output_path))

    try:
        for csv_file in csv_files:
            # Extract table name from filename (e.g., GA_PLOT.csv -> PLOT)
            name_parts = csv_file.stem.split("_")
            if len(name_parts) >= 2:
                table_name = "_".join(
                    name_parts[1:]
                )  # Handle names like TREE_GRM_COMPONENT
            else:
                table_name = name_parts[0]

            table_name = table_name.upper()

            if show_progress:
                console.print(f"  Converting {table_name}...", end=" ")

            try:
                if state_code is not None:
                    conn.execute(f"""
                        CREATE TABLE IF NOT EXISTS {table_name} AS
                        SELECT *, {state_code} AS STATE_ADDED
                        FROM read_csv_auto('{csv_file}', header=true, ignore_errors=true)
                    """)
                else:
                    conn.execute(f"""
                        CREATE TABLE IF NOT EXISTS {table_name} AS
                        SELECT * FROM read_csv_auto('{csv_file}', header=true, ignore_errors=true)
                    """)

                row_result = conn.execute(
                    f"SELECT COUNT(*) FROM {table_name}"
                ).fetchone()
                row_count = row_result[0] if row_result else 0
                if show_progress:
                    console.print(f"[green]{row_count:,} rows[/green]")

            except Exception as e:
                if show_progress:
                    console.print(f"[red]FAILED[/red] ({e})")
                logger.warning(f"Failed to convert {table_name}: {e}")

        conn.execute("CHECKPOINT")

    finally:
        conn.close()

    return output_path


def download(
    states: str | list[str],
    dir: str | Path | None = None,
    common: bool = True,
    tables: list[str] | None = None,
    force: bool = False,
    show_progress: bool = True,
    use_cache: bool = True,
) -> Path:
    """
    Download FIA data from the FIA DataMart.

    This function downloads FIA data for one or more states from the USDA
    Forest Service FIA DataMart, similar to rFIA's getFIA() function.
    Data is automatically converted to DuckDB format for use with pyFIA.

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
        Show download progress bars.
    use_cache : bool, default True
        Use cached downloads if available.

    Returns
    -------
    Path
        Path to the DuckDB database file.

    Raises
    ------
    StateNotFoundError
        If an invalid state code is provided.
    TableNotFoundError
        If a requested table is not available.
    NetworkError
        If download fails due to network issues.
    DownloadError
        For other download-related errors.

    Examples
    --------
    >>> from pyfia import download
    >>>
    >>> # Download Georgia data
    >>> db_path = download("GA")
    >>>
    >>> # Download multiple states merged into one database
    >>> db_path = download(["GA", "FL", "SC"])
    >>>
    >>> # Download only specific tables
    >>> db_path = download("GA", tables=["PLOT", "TREE", "COND"])
    >>>
    >>> # Use with pyFIA immediately
    >>> from pyfia import FIA, area
    >>> with FIA(download("GA")) as db:
    ...     db.clip_most_recent()
    ...     result = area(db)

    Notes
    -----
    - Large states (CA, TX) may have TREE tables >1GB compressed
    - First download may take several minutes depending on connection
    - Downloaded data is cached locally to avoid re-downloading
    """
    # Normalize states to list
    if isinstance(states, str):
        states = [states]

    # Validate all state codes
    validated_states = [validate_state_code(s) for s in states]

    # Set default directory
    if dir is None:
        data_dir = _get_default_data_dir()
    else:
        data_dir = Path(dir).expanduser()

    data_dir.mkdir(parents=True, exist_ok=True)

    # Create client and cache
    client = DataMartClient()
    cache = DownloadCache(data_dir / ".cache")

    # Handle single state vs multi-state
    if len(validated_states) == 1:
        return _download_single_state(
            state=validated_states[0],
            data_dir=data_dir,
            client=client,
            cache=cache,
            common=common,
            tables=tables,
            force=force,
            show_progress=show_progress,
            use_cache=use_cache,
        )
    else:
        return _download_multi_state(
            states=validated_states,
            data_dir=data_dir,
            client=client,
            cache=cache,
            common=common,
            tables=tables,
            force=force,
            show_progress=show_progress,
            use_cache=use_cache,
        )


def _download_single_state(
    state: str,
    data_dir: Path,
    client: DataMartClient,
    cache: DownloadCache,
    common: bool,
    tables: list[str] | None,
    force: bool,
    show_progress: bool,
    use_cache: bool,
) -> Path:
    """Download FIA data for a single state."""
    state_dir = data_dir / state.lower()
    duckdb_path = state_dir / f"{state.lower()}.duckdb"

    # Check cache for existing download (unless force=True)
    if use_cache and not force:
        cached_path = cache.get_cached(state)
        if cached_path and cached_path.exists():
            if show_progress:
                console.print(
                    f"[bold green]Using cached data for {state}[/bold green]: {cached_path}"
                )

                # Warn if cache is old
                cached_entry = cache._metadata.get(cache._get_cache_key(state))
                if cached_entry and cached_entry.is_stale:
                    console.print(
                        f"[yellow]Warning: Cached data is {cached_entry.age_days:.0f} days old. "
                        f"Use force=True to re-download.[/yellow]"
                    )

            return cached_path

    if show_progress:
        console.print(f"\n[bold]Downloading FIA data for {state}[/bold]")
        console.print(f"Data directory: {state_dir}")

    # Remove existing file if force=True
    if force and duckdb_path.exists():
        duckdb_path.unlink()

    # Use temp directory for CSV downloads
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        csv_dir = temp_path / "csv"
        csv_dir.mkdir()

        # Download CSVs
        if show_progress:
            console.print("\n[bold]Downloading CSV files...[/bold]")

        downloaded = client.download_tables(
            state,
            tables=tables,
            common=common,
            dest_dir=csv_dir,
            show_progress=show_progress,
        )

        if not downloaded:
            raise DownloadError(f"No tables downloaded for {state}")

        # Convert to DuckDB
        if show_progress:
            console.print("\n[bold]Converting to DuckDB...[/bold]")

        # Get state FIPS code
        state_code = None
        try:
            state_code = get_state_fips(state)
        except ValueError:
            pass

        _convert_csvs_to_duckdb(
            csv_dir, duckdb_path, state_code=state_code, show_progress=show_progress
        )

    if use_cache:
        cache.add_to_cache(state, duckdb_path)

    if show_progress:
        size_mb = duckdb_path.stat().st_size / (1024 * 1024)
        console.print(
            f"\n[bold green]Download complete![/bold green] "
            f"Database: {duckdb_path} ({size_mb:.1f} MB)"
        )

    return duckdb_path


def _download_multi_state(
    states: list[str],
    data_dir: Path,
    client: DataMartClient,
    cache: DownloadCache,
    common: bool,
    tables: list[str] | None,
    force: bool,
    show_progress: bool,
    use_cache: bool,
) -> Path:
    """Download and merge FIA data for multiple states."""
    # Create merged database name
    states_suffix = "_".join(sorted(states)).lower()
    merged_name = f"merged_{states_suffix}"

    merged_dir = data_dir / "merged"
    merged_dir.mkdir(parents=True, exist_ok=True)
    output_path = merged_dir / f"{merged_name}.duckdb"

    # Check cache
    cache_key = f"MERGED_{states_suffix.upper()}"
    if use_cache and not force:
        cached_path = cache.get_cached(cache_key)
        if cached_path and cached_path.exists():
            if show_progress:
                console.print(
                    f"[bold green]Using cached merged data[/bold green]: {cached_path}"
                )
            return cached_path

    if show_progress:
        console.print(f"\n[bold]Downloading and merging {len(states)} states[/bold]")
        console.print(f"States: {', '.join(states)}")

    # Download each state and merge
    import duckdb

    # Remove existing output if force
    if output_path.exists() and force:
        output_path.unlink()

    conn = duckdb.connect(str(output_path))

    try:
        for i, state in enumerate(states, 1):
            if show_progress:
                console.print(
                    f"\n[bold][{i}/{len(states)}] Processing {state}...[/bold]"
                )

            # Download state
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_path = Path(temp_dir)
                csv_dir = temp_path / "csv"
                csv_dir.mkdir()

                # Download CSVs
                downloaded = client.download_tables(
                    state,
                    tables=tables,
                    common=common,
                    dest_dir=csv_dir,
                    show_progress=show_progress,
                )

                if not downloaded:
                    logger.warning(f"No tables downloaded for {state}, skipping")
                    continue

                # Get state FIPS code
                state_code = get_state_fips(state)

                # Import into DuckDB
                csv_files = list(csv_dir.glob("*.csv")) + list(csv_dir.glob("*.CSV"))

                for csv_file in csv_files:
                    # Extract table name
                    name_parts = csv_file.stem.split("_")
                    if len(name_parts) >= 2:
                        table_name = "_".join(name_parts[1:])
                    else:
                        table_name = name_parts[0]
                    table_name = table_name.upper()

                    try:
                        # Check if table exists
                        existing_result = conn.execute(
                            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
                            [table_name],
                        ).fetchone()
                        existing = existing_result[0] if existing_result else 0

                        if existing > 0:
                            # Append to existing table
                            conn.execute(f"""
                                INSERT INTO {table_name}
                                SELECT *, {state_code} AS STATE_ADDED
                                FROM read_csv_auto('{csv_file}', header=true, ignore_errors=true)
                            """)
                        else:
                            # Create new table
                            conn.execute(f"""
                                CREATE TABLE {table_name} AS
                                SELECT *, {state_code} AS STATE_ADDED
                                FROM read_csv_auto('{csv_file}', header=true, ignore_errors=true)
                            """)

                    except Exception as e:
                        logger.warning(
                            f"Failed to import {table_name} for {state}: {e}"
                        )

        conn.execute("CHECKPOINT")

    finally:
        conn.close()

    if use_cache:
        cache.add_to_cache(cache_key, output_path)

    if show_progress:
        size_mb = output_path.stat().st_size / (1024 * 1024)
        console.print(
            f"\n[bold green]Merge complete![/bold green] "
            f"Database: {output_path} ({size_mb:.1f} MB)"
        )

    return output_path


def clear_cache(
    older_than_days: int | None = None,
    state: str | None = None,
    delete_files: bool = False,
) -> int:
    """
    Clear the download cache.

    Parameters
    ----------
    older_than_days : int, optional
        Only clear entries older than this many days.
    state : str, optional
        Only clear entries for this state.
    delete_files : bool, default False
        If True, also delete the cached files from disk.

    Returns
    -------
    int
        Number of cache entries cleared.
    """
    from datetime import timedelta

    data_dir = _get_default_data_dir()
    cache = DownloadCache(data_dir / ".cache")

    older_than = timedelta(days=older_than_days) if older_than_days else None

    return cache.clear_cache(
        older_than=older_than, state=state, delete_files=delete_files
    )


def cache_info() -> dict:
    """
    Get information about the download cache.

    Returns
    -------
    dict
        Cache statistics including size, file count, etc.
    """
    data_dir = _get_default_data_dir()
    cache = DownloadCache(data_dir / ".cache")
    return cache.get_cache_info()
