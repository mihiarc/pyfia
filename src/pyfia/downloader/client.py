"""
HTTP client for downloading FIA data from the DataMart.

This module provides the DataMartClient class for downloading FIA data
from the USDA Forest Service FIA DataMart.

References
----------
- FIA DataMart: https://apps.fs.usda.gov/fia/datamart/datamart.html
- CSV Downloads: https://apps.fs.usda.gov/fia/datamart/CSV/
"""

import hashlib
import logging
import shutil
import tempfile
import zipfile
from pathlib import Path

import requests
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)

from pyfia.downloader.exceptions import (
    DownloadError,
    NetworkError,
    TableNotFoundError,
)
from pyfia.downloader.tables import (
    REFERENCE_TABLES,
    get_tables_for_download,
    validate_state_code,
)

logger = logging.getLogger(__name__)

# FIA DataMart URLs
DATAMART_CSV_BASE = "https://apps.fs.usda.gov/fia/datamart/CSV/"


class DataMartClient:
    """
    HTTP client for FIA DataMart downloads.

    This client handles downloading CSV files from the FIA DataMart,
    with support for progress bars, retries, and checksum verification.

    Parameters
    ----------
    timeout : int, default 300
        Request timeout in seconds.
    chunk_size : int, default 1048576
        Download chunk size in bytes (default 1MB).
    max_retries : int, default 3
        Maximum number of retry attempts for failed downloads.

    Examples
    --------
    >>> client = DataMartClient()
    >>> path = client.download_table("GA", "PLOT", Path("./data"))
    >>> print(f"Downloaded to: {path}")
    """

    def __init__(
        self,
        timeout: int = 300,
        chunk_size: int = 1024 * 1024,
        max_retries: int = 3,
    ):
        self.timeout = timeout
        self.chunk_size = chunk_size
        self.max_retries = max_retries
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "pyFIA/1.0 (download client)"})

    def _build_csv_url(self, state: str, table: str) -> str:
        """
        Build the URL for a CSV table download.

        Parameters
        ----------
        state : str
            State abbreviation (e.g., 'GA') or 'REF' for reference tables.
        table : str
            Table name (e.g., 'PLOT', 'TREE').

        Returns
        -------
        str
            Full URL for the ZIP file download.
        """
        state = state.upper()
        table = table.upper()

        if state == "REF":
            # Reference tables: REF_SPECIES.zip
            filename = f"{table}.zip"
        else:
            # State tables: GA_PLOT.zip
            filename = f"{state}_{table}.zip"

        return f"{DATAMART_CSV_BASE}{filename}"

    def _download_file(
        self,
        url: str,
        dest_path: Path,
        description: str | None = None,
        show_progress: bool = True,
    ) -> Path:
        """
        Download a file from URL to destination path.

        Parameters
        ----------
        url : str
            URL to download from.
        dest_path : Path
            Destination file path.
        description : str, optional
            Description for progress bar.
        show_progress : bool, default True
            Show download progress bar.

        Returns
        -------
        Path
            Path to the downloaded file.

        Raises
        ------
        NetworkError
            If the download fails after all retries.
        """
        last_error = None

        for attempt in range(self.max_retries):
            try:
                response = self.session.get(
                    url, stream=True, timeout=self.timeout, allow_redirects=True
                )

                if response.status_code == 404:
                    raise TableNotFoundError(dest_path.stem)

                response.raise_for_status()

                # Get file size if available
                total_size = int(response.headers.get("content-length", 0))

                # Ensure parent directory exists
                dest_path.parent.mkdir(parents=True, exist_ok=True)

                if show_progress and total_size > 0:
                    with Progress(
                        SpinnerColumn(),
                        TextColumn("[bold blue]{task.description}"),
                        BarColumn(),
                        DownloadColumn(),
                        TransferSpeedColumn(),
                        TimeRemainingColumn(),
                    ) as progress:
                        task = progress.add_task(
                            description or dest_path.name, total=total_size
                        )

                        with open(dest_path, "wb") as f:
                            for chunk in response.iter_content(
                                chunk_size=self.chunk_size
                            ):
                                if chunk:
                                    f.write(chunk)
                                    progress.update(task, advance=len(chunk))
                else:
                    # No progress bar (small file or progress disabled)
                    with open(dest_path, "wb") as f:
                        for chunk in response.iter_content(chunk_size=self.chunk_size):
                            if chunk:
                                f.write(chunk)

                logger.debug(f"Downloaded {url} to {dest_path}")
                return dest_path

            except requests.exceptions.RequestException as e:
                last_error = e
                logger.warning(f"Download attempt {attempt + 1} failed: {e}")
                if attempt < self.max_retries - 1:
                    continue
                raise NetworkError(
                    f"Failed to download after {self.max_retries} attempts: {e}",
                    url=url,
                    status_code=getattr(e.response, "status_code", None)
                    if hasattr(e, "response")
                    else None,
                ) from last_error

        # Should not reach here, but just in case
        raise NetworkError(f"Download failed: {last_error}", url=url)

    def _extract_zip(
        self, zip_path: Path, extract_dir: Path, show_progress: bool = True
    ) -> list[Path]:
        """
        Extract a ZIP file to a directory.

        Parameters
        ----------
        zip_path : Path
            Path to the ZIP file.
        extract_dir : Path
            Directory to extract files to.
        show_progress : bool, default True
            Show extraction progress.

        Returns
        -------
        list of Path
            List of extracted file paths.
        """
        extracted_files = []

        with zipfile.ZipFile(zip_path, "r") as zf:
            members = zf.namelist()

            if show_progress:
                from rich.console import Console

                console = Console()
                with console.status(f"[bold blue]Extracting {zip_path.name}..."):
                    for member in members:
                        zf.extract(member, extract_dir)
                        extracted_files.append(extract_dir / member)
            else:
                for member in members:
                    zf.extract(member, extract_dir)
                    extracted_files.append(extract_dir / member)

        return extracted_files

    def download_table(
        self,
        state: str,
        table: str,
        dest_dir: Path,
        show_progress: bool = True,
    ) -> Path:
        """
        Download a single FIA table for a state.

        Parameters
        ----------
        state : str
            State abbreviation (e.g., 'GA') or 'REF' for reference tables.
        table : str
            Table name (e.g., 'PLOT', 'TREE').
        dest_dir : Path
            Directory to save the extracted CSV file.
        show_progress : bool, default True
            Show download progress bar.

        Returns
        -------
        Path
            Path to the extracted CSV file.

        Raises
        ------
        StateNotFoundError
            If the state code is invalid.
        TableNotFoundError
            If the table is not found for the state.
        NetworkError
            If the download fails.

        Examples
        --------
        >>> client = DataMartClient()
        >>> csv_path = client.download_table("GA", "PLOT", Path("./data"))
        """
        state = validate_state_code(state)
        table = table.upper()

        url = self._build_csv_url(state, table)
        logger.info(f"Downloading {state}_{table} from {url}")

        # Download to temp file
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            zip_filename = f"{state}_{table}.zip" if state != "REF" else f"{table}.zip"
            zip_path = temp_path / zip_filename

            try:
                self._download_file(
                    url,
                    zip_path,
                    description=f"{state}_{table}",
                    show_progress=show_progress,
                )
            except TableNotFoundError:
                raise TableNotFoundError(table, state)

            # Extract CSV
            extracted = self._extract_zip(zip_path, temp_path, show_progress=False)

            # Find the CSV file
            csv_files = [f for f in extracted if f.suffix.lower() == ".csv"]
            if not csv_files:
                raise DownloadError(f"No CSV file found in {zip_filename}", url=url)

            # Move to destination
            dest_dir.mkdir(parents=True, exist_ok=True)
            csv_file = csv_files[0]
            dest_path = dest_dir / csv_file.name

            shutil.move(str(csv_file), str(dest_path))

            logger.info(f"Extracted {table} to {dest_path}")
            return dest_path

    def download_tables(
        self,
        state: str,
        tables: list[str] | None = None,
        common: bool = True,
        dest_dir: Path | None = None,
        show_progress: bool = True,
    ) -> dict[str, Path]:
        """
        Download multiple FIA tables for a state.

        Parameters
        ----------
        state : str
            State abbreviation (e.g., 'GA') or 'REF' for reference tables.
        tables : list of str, optional
            Specific tables to download. If None, uses common or all tables.
        common : bool, default True
            If tables is None, download only common tables (True) or all (False).
        dest_dir : Path, optional
            Directory to save files. Defaults to ~/.pyfia/data/{state}/csv/
        show_progress : bool, default True
            Show download progress.

        Returns
        -------
        dict
            Mapping of table names to downloaded file paths.

        Examples
        --------
        >>> client = DataMartClient()
        >>> paths = client.download_tables("GA", common=True)
        >>> print(f"Downloaded {len(paths)} tables")
        """
        state = validate_state_code(state)

        # Determine tables to download
        if state == "REF":
            tables_to_download = tables or REFERENCE_TABLES
        else:
            tables_to_download = get_tables_for_download(common=common, tables=tables)

        # Set default destination
        if dest_dir is None:
            from pyfia.core.settings import settings

            dest_dir = settings.cache_dir.parent / "data" / state.lower() / "csv"

        dest_dir.mkdir(parents=True, exist_ok=True)

        downloaded = {}
        failed = []

        if show_progress:
            from rich.console import Console

            console = Console()
            console.print(
                f"\n[bold]Downloading {len(tables_to_download)} tables for {state}[/bold]\n"
            )

        for i, table in enumerate(tables_to_download, 1):
            if show_progress:
                from rich.console import Console

                console = Console()
                console.print(f"[{i}/{len(tables_to_download)}] {table}...", end=" ")

            try:
                path = self.download_table(
                    state, table, dest_dir, show_progress=show_progress
                )
                downloaded[table] = path
                if show_progress:
                    console.print("[green]OK[/green]")
            except (TableNotFoundError, NetworkError) as e:
                failed.append((table, str(e)))
                if show_progress:
                    console.print(f"[red]FAILED[/red] ({e})")
                logger.warning(f"Failed to download {state}_{table}: {e}")

        if show_progress:
            from rich.console import Console

            console = Console()
            console.print(
                f"\n[bold green]Downloaded {len(downloaded)}/{len(tables_to_download)} tables[/bold green]"
            )
            if failed:
                console.print(
                    f"[yellow]Failed: {', '.join(t for t, _ in failed)}[/yellow]"
                )

        return downloaded

    def get_file_checksum(self, file_path: Path) -> str:
        """
        Calculate MD5 checksum of a file.

        Parameters
        ----------
        file_path : Path
            Path to the file.

        Returns
        -------
        str
            MD5 hexdigest of the file.
        """
        md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                md5.update(chunk)
        return md5.hexdigest()

    def check_url_exists(self, url: str) -> bool:
        """
        Check if a URL exists (HEAD request).

        Parameters
        ----------
        url : str
            URL to check.

        Returns
        -------
        bool
            True if URL exists (status 200), False otherwise.
        """
        try:
            response = self.session.head(url, timeout=30, allow_redirects=True)
            return bool(response.status_code == 200)
        except requests.exceptions.RequestException:
            return False
