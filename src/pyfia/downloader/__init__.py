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

from __future__ import annotations

import logging
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING

from rich.console import Console

from pyfia.constants.fiadb_schema import COLUMN_TYPES, FIADB_VERSION
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
from pyfia.validation import sanitize_sql_path, validate_sql_identifier

if TYPE_CHECKING:
    import duckdb

logger = logging.getLogger(__name__)
console = Console()

# Reference tables that pyfia.reference and the estimators read (species,
# forest types, owner groups, survey units). A built database missing any of
# these is treated as a failed download rather than silently cached (#86).
REQUIRED_REFERENCE_TABLES = (
    "REF_SPECIES",
    "REF_SPECIES_GROUP",
    "REF_FOREST_TYPE",
    "REF_FOREST_TYPE_GROUP",
    "REF_OWNGRPCD",
    "REF_UNIT",
)

# DOUBLE holds every integer of smaller magnitude exactly.
_EXACT_INTEGER_LIMIT = 2**53

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


def _missing_reference_tables(db_path: Path) -> list[str]:
    """Return required reference tables absent or empty in a built database.

    Used to detect a database that was assembled after a failed reference-table
    download, so it is never silently cached as valid (#86).

    Parameters
    ----------
    db_path : Path
        Path to the built DuckDB database.

    Returns
    -------
    list of str
        Names of required reference tables that are missing or empty. Empty
        list means all required reference tables are present and non-empty.
    """
    import duckdb

    missing: list[str] = []
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        existing = {
            row[0].upper()
            for row in conn.execute(
                "SELECT table_name FROM information_schema.tables"
            ).fetchall()
        }
        for table in REQUIRED_REFERENCE_TABLES:
            if table.upper() not in existing:
                missing.append(table)
                continue
            count = conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()
            if not count or not count[0]:
                missing.append(table)
    finally:
        conn.close()
    return missing


def _verify_reference_tables_or_discard(db_path: Path, label: str) -> None:
    """Raise (and delete ``db_path``) if required reference tables are missing.

    A failed reference-table download must not produce a database that is
    cached and later breaks ``by_species`` / name-join operations. The partial
    database is removed so a retry rebuilds it cleanly.
    """
    missing = _missing_reference_tables(db_path)
    if missing:
        db_path.unlink(missing_ok=True)
        raise DownloadError(
            f"Reference tables missing from the downloaded database for "
            f"{label}: {', '.join(missing)}. This usually means the "
            f"reference-table download failed (check network / DataMart "
            f"availability). The incomplete database was discarded; "
            f"please retry the download."
        )


def _table_name(csv_file: Path, state: str | None) -> tuple[str, bool]:
    """
    FIADB table name of a DataMart CSV, and whether it is a state table.

    ``GA_PLOT.csv`` is table PLOT of state GA. National files keep their name
    (``REF_SPECIES.csv``, ``EVALIDATOR_POP_ESTIMATE.csv``, ``BEGINEND.csv``).
    """
    stem = csv_file.stem.upper()
    prefix = f"{state.upper()}_" if state else None
    if prefix and stem.startswith(prefix):
        return stem[len(prefix) :], True
    return stem, False


def _import_csv(
    conn: duckdb.DuckDBPyConnection,
    csv_file: Path,
    table: str,
    state_code: int | None = None,
) -> int:
    """
    Load one DataMart CSV into ``table`` with FIADB's column types.

    The file is read as text and each column is cast to its type in
    :data:`pyfia.constants.fiadb_schema.COLUMN_TYPES`. An integer column is
    cast through DOUBLE and only when every value is a whole number, so
    ``693.0`` loads as 693 and ``693.5`` fails. Rows that repeat the header,
    which some DataMart files contain, are dropped with a warning. Columns and
    tables outside the schema load as VARCHAR, which is lossless, with a
    warning. If ``table`` exists the rows are appended by column name.

    Parameters
    ----------
    conn : duckdb.DuckDBPyConnection
        Connection to the database being built.
    csv_file : Path
        The CSV file.
    table : str
        FIADB table name.
    state_code : int, optional
        State FIPS code, stored in a ``STATE_ADDED`` column.

    Returns
    -------
    int
        Rows loaded.

    Raises
    ------
    DownloadError
        If a value doesn't parse as its column's type, or the rows can't be
        appended to the existing table.
    """
    import duckdb

    safe_table = validate_sql_identifier(table, "table name")
    path = sanitize_sql_path(csv_file)
    source = f"read_csv('{path}', header=true, all_varchar=true)"
    header = [
        validate_sql_identifier(row[0], "column name")
        for row in conn.execute(f"DESCRIBE SELECT * FROM {source}").fetchall()
    ]
    declared = COLUMN_TYPES.get(safe_table)
    if declared is None:
        logger.warning(
            f"{safe_table} is not in the FIADB schema ({FIADB_VERSION}); "
            "its columns load as VARCHAR"
        )

    select: list[str] = []
    unknown: list[str] = []
    for col in header:
        col_type = (declared or {}).get(col)
        if col_type is None:
            col_type = "VARCHAR"
            if declared is not None:
                unknown.append(col)
        quoted = f'"{col}"'
        if col_type == "BIGINT":
            value = f"CAST({quoted} AS DOUBLE)"
            select.append(
                f"CASE WHEN {quoted} IS NULL OR ({value} = trunc({value}) "
                f"AND abs({value}) < {_EXACT_INTEGER_LIMIT}) "
                f"THEN CAST({value} AS BIGINT) "
                f"ELSE error('{safe_table}.{col} holds a value that is not an "
                f"integer: ' || {quoted}) END AS {quoted}"
            )
        elif col_type == "VARCHAR":
            select.append(quoted)
        else:
            select.append(f"CAST({quoted} AS {col_type}) AS {quoted}")
    if unknown:
        logger.warning(
            f"{safe_table}: columns outside the FIADB schema ({FIADB_VERSION}) "
            f"load as VARCHAR: {', '.join(unknown)}"
        )
    if state_code is not None:
        select.append(f"{int(state_code)} AS STATE_ADDED")

    repeats_header = " AND ".join(f"\"{c}\" = '{c}'" for c in header)
    first = f'"{header[0]}"'
    repeated = conn.execute(
        f"SELECT count(*) FROM {source} WHERE {first} = '{header[0]}'"
    ).fetchone()
    if repeated and repeated[0]:
        logger.warning(
            f"{csv_file.name}: dropped {repeated[0]} row(s) repeating the header"
        )
    query = (
        f"SELECT {', '.join(select)} FROM {source} "
        f"WHERE NOT coalesce({repeats_header}, false)"
    )
    exists = conn.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
        [safe_table],
    ).fetchone()
    try:
        if exists and exists[0]:
            result = conn.execute(f'INSERT INTO "{safe_table}" BY NAME {query}')
        else:
            result = conn.execute(f'CREATE TABLE "{safe_table}" AS {query}')
        count = result.fetchone()
    except duckdb.Error as e:
        raise DownloadError(
            f"Failed to import {safe_table} from {csv_file.name}: {e}"
        ) from e
    return int(count[0]) if count else 0


def _convert_csvs_to_duckdb(
    csv_dir: Path,
    output_path: Path,
    state_code: int | None = None,
    state: str | None = None,
    show_progress: bool = True,
) -> Path:
    """
    Convert downloaded CSV files to a DuckDB database.

    Each file loads with :func:`_import_csv`. If any file fails, the partial
    database is deleted and the error is raised.

    Parameters
    ----------
    csv_dir : Path
        Directory containing CSV files.
    output_path : Path
        Path for the output DuckDB file.
    state_code : int, optional
        State FIPS code, stored in a ``STATE_ADDED`` column of the state tables.
    state : str, optional
        State abbreviation, the prefix of the state tables' file names.
    show_progress : bool
        Show progress messages.

    Returns
    -------
    Path
        Path to the created DuckDB file.

    Raises
    ------
    DownloadError
        If there are no CSV files or one fails to import.
    """
    import duckdb

    csv_files = sorted(list(csv_dir.glob("*.csv")) + list(csv_dir.glob("*.CSV")))

    if not csv_files:
        raise DownloadError(f"No CSV files found in {csv_dir}")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(output_path))
    try:
        for csv_file in csv_files:
            table, is_state_table = _table_name(csv_file, state)
            if show_progress:
                console.print(f"  Converting {table}...", end=" ")
            rows = _import_csv(
                conn, csv_file, table, state_code if is_state_table else None
            )
            if show_progress:
                console.print(f"[green]{rows:,} rows[/green]")
        conn.execute("CHECKPOINT")
    except BaseException:
        conn.close()
        output_path.unlink(missing_ok=True)
        raise
    conn.close()
    return output_path


def _download_state_csvs(
    client: DataMartClient,
    state: str,
    tables: list[str] | None,
    common: bool,
    dest_dir: Path,
    show_progress: bool,
) -> dict[str, Path]:
    """
    Download a state's tables as CSV files.

    All tables (``common=False`` and no ``tables``) come from the state's
    DataMart archive, so they are whatever DataMart currently publishes; a
    subset is downloaded table by table.
    """
    if tables is None and not common:
        return client.download_state_archive(
            state, dest_dir, show_progress=show_progress
        )
    return client.download_tables(
        state,
        tables=tables,
        common=common,
        dest_dir=dest_dir,
        show_progress=show_progress,
    )


def _warn_on_fiadb_version(db_path: Path) -> None:
    """Warn when the database's FIADB release differs from the schema's."""
    import duckdb

    with duckdb.connect(str(db_path), read_only=True) as conn:
        row = conn.execute(
            "SELECT VERSION FROM REF_FIADB_VERSION ORDER BY CREATED_DATE DESC LIMIT 1"
        ).fetchone()
    if row and row[0] != FIADB_VERSION:
        logger.warning(
            f"The database is {row[0]}, but pyFIA's column types come from "
            f"{FIADB_VERSION}. Columns FIADB added since load as VARCHAR; "
            "regenerate the types with scripts/gen_fiadb_schema.py."
        )


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

        downloaded = _download_state_csvs(
            client, state, tables, common, csv_dir, show_progress
        )

        if not downloaded:
            raise DownloadError(f"No tables downloaded for {state}")

        # Also download the reference tables (state-independent)
        if show_progress:
            console.print("\n[bold]Downloading reference tables...[/bold]")

        try:
            client.download_reference_tables(
                dest_dir=csv_dir, show_progress=show_progress
            )
        except Exception as e:
            # Logged here for the root cause; the post-build verification below
            # is what makes a missing-reference-table build fatal (#86).
            logger.warning(f"Failed to download reference tables: {e}")

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
            csv_dir,
            duckdb_path,
            state_code=state_code,
            state=state,
            show_progress=show_progress,
        )

    # Fail loudly (and discard) rather than caching a database that is missing
    # its reference tables.
    _verify_reference_tables_or_discard(duckdb_path, state)
    _warn_on_fiadb_version(duckdb_path)

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
                downloaded = _download_state_csvs(
                    client, state, tables, common, csv_dir, show_progress
                )

                if not downloaded:
                    raise DownloadError(f"No tables downloaded for {state}")

                state_code = get_state_fips(state)
                csv_files = sorted(
                    list(csv_dir.glob("*.csv")) + list(csv_dir.glob("*.CSV"))
                )
                for csv_file in csv_files:
                    table, is_state_table = _table_name(csv_file, state)
                    _import_csv(
                        conn, csv_file, table, state_code if is_state_table else None
                    )

        # Download and add reference tables (once for all states)
        if show_progress:
            console.print("\n[bold]Downloading reference tables...[/bold]")

        with tempfile.TemporaryDirectory() as ref_temp_dir:
            ref_tables = client.download_reference_tables(
                dest_dir=Path(ref_temp_dir), show_progress=show_progress
            )
            for table_name, csv_path in sorted(ref_tables.items()):
                _import_csv(conn, csv_path, table_name)

        conn.execute("CHECKPOINT")
    except BaseException:
        conn.close()
        output_path.unlink(missing_ok=True)
        raise
    conn.close()

    # Fail loudly (and discard) rather than caching a database that is missing
    # its reference tables.
    _verify_reference_tables_or_discard(output_path, cache_key)
    _warn_on_fiadb_version(output_path)

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
