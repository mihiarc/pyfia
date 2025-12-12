"""
Download cache management for FIA data.

This module provides caching functionality to avoid re-downloading
FIA data that has already been retrieved.
"""

import json
import logging
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class CachedDownload:
    """
    Metadata for a cached download.

    Parameters
    ----------
    state : str
        State abbreviation or cache key (e.g., 'GA', 'MERGED_FL_GA_SC').
    path : str
        Path to the cached DuckDB file.
    downloaded_at : str
        ISO format timestamp of when the file was downloaded.
    size_bytes : int
        File size in bytes.
    checksum : str
        MD5 checksum of the file.
    """

    state: str
    path: str
    downloaded_at: str
    size_bytes: int
    checksum: str

    @property
    def age_days(self) -> float:
        """Get the age of this cached download in days."""
        downloaded = datetime.fromisoformat(self.downloaded_at)
        return (datetime.now() - downloaded).total_seconds() / 86400

    @property
    def is_stale(self) -> bool:
        """Check if this download is older than 90 days."""
        return self.age_days > 90


class DownloadCache:
    """
    Manages cached FIA data downloads with metadata tracking.

    The cache stores metadata about downloaded DuckDB files including timestamps,
    checksums, and file locations. This allows skipping downloads for
    files that are already present and valid.

    Parameters
    ----------
    cache_dir : Path
        Directory for cache storage.

    Examples
    --------
    >>> cache = DownloadCache(Path("~/.pyfia/cache"))
    >>> if not cache.get_cached("GA"):
    ...     # Download the file
    ...     cache.add_to_cache("GA", downloaded_path)
    """

    METADATA_FILE = "download_metadata.json"

    def __init__(self, cache_dir: Path):
        self.cache_dir = Path(cache_dir).expanduser()
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.metadata_file = self.cache_dir / self.METADATA_FILE
        self._metadata: dict[str, CachedDownload] = {}
        self._load_metadata()

    def _get_cache_key(self, state: str) -> str:
        """Generate a unique cache key for a state or merged dataset."""
        return state.upper()

    def _load_metadata(self) -> None:
        """Load metadata from disk."""
        if self.metadata_file.exists():
            try:
                with open(self.metadata_file) as f:
                    data = json.load(f)
                    # Handle both old format (with 'format' and 'table' fields) and new format
                    self._metadata = {}
                    for k, v in data.items():
                        # Remove deprecated fields if present
                        v.pop("format", None)
                        v.pop("table", None)
                        self._metadata[k] = CachedDownload(**v)
            except (json.JSONDecodeError, TypeError) as e:
                logger.warning(f"Failed to load cache metadata: {e}")
                self._metadata = {}
        else:
            self._metadata = {}

    def _save_metadata(self) -> None:
        """Save metadata to disk."""
        data = {k: asdict(v) for k, v in self._metadata.items()}
        with open(self.metadata_file, "w") as f:
            json.dump(data, f, indent=2)

    def get_cached(self, state: str, max_age_days: float | None = None) -> Path | None:
        """
        Get the path to a cached DuckDB file if it exists and is valid.

        Parameters
        ----------
        state : str
            State abbreviation or cache key (e.g., 'GA', 'MERGED_FL_GA_SC').
        max_age_days : float, optional
            Maximum age in days to consider cache valid.
            Defaults to None (no age limit).

        Returns
        -------
        Path or None
            Path to the cached DuckDB file, or None if not found/invalid.
        """
        key = self._get_cache_key(state)

        if key not in self._metadata:
            return None

        cached = self._metadata[key]
        path = Path(cached.path)

        # Check file exists
        if not path.exists():
            logger.debug(f"Cached file no longer exists: {path}")
            del self._metadata[key]
            self._save_metadata()
            return None

        # Check age if specified
        if max_age_days is not None and cached.age_days > max_age_days:
            logger.debug(f"Cached file too old: {cached.age_days:.1f} days")
            return None

        return path

    def add_to_cache(
        self,
        state: str,
        path: Path,
        checksum: str | None = None,
    ) -> None:
        """
        Add a downloaded DuckDB file to the cache.

        Parameters
        ----------
        state : str
            State abbreviation or cache key (e.g., 'GA', 'MERGED_FL_GA_SC').
        path : Path
            Path to the downloaded DuckDB file.
        checksum : str, optional
            MD5 checksum of the file. Calculated if not provided.
        """
        key = self._get_cache_key(state)
        path = Path(path)

        if not path.exists():
            raise FileNotFoundError(f"Cannot cache non-existent file: {path}")

        # Calculate checksum if not provided
        if checksum is None:
            import hashlib

            md5 = hashlib.md5()
            with open(path, "rb") as f:
                for chunk in iter(lambda: f.read(8192), b""):
                    md5.update(chunk)
            checksum = md5.hexdigest()

        cached = CachedDownload(
            state=state.upper(),
            path=str(path.absolute()),
            downloaded_at=datetime.now().isoformat(),
            size_bytes=path.stat().st_size,
            checksum=checksum,
        )

        self._metadata[key] = cached
        self._save_metadata()
        logger.debug(f"Added to cache: {key} -> {path}")

    def remove_from_cache(self, state: str) -> bool:
        """
        Remove an entry from the cache.

        Parameters
        ----------
        state : str
            State abbreviation or cache key.

        Returns
        -------
        bool
            True if entry was removed, False if not found.
        """
        key = self._get_cache_key(state)

        if key in self._metadata:
            del self._metadata[key]
            self._save_metadata()
            return True
        return False

    def clear_cache(
        self,
        older_than: timedelta | None = None,
        state: str | None = None,
        delete_files: bool = False,
    ) -> int:
        """
        Clear cached entries.

        Parameters
        ----------
        older_than : timedelta, optional
            Only clear entries older than this. If None, clear all.
        state : str, optional
            Only clear entries for this state.
        delete_files : bool, default False
            If True, also delete the cached files from disk.

        Returns
        -------
        int
            Number of entries cleared.
        """
        to_remove = []
        cutoff_days = older_than.total_seconds() / 86400 if older_than else None

        for key, cached in self._metadata.items():
            # Filter by state
            if state and cached.state != state.upper():
                continue

            # Filter by age
            if cutoff_days and cached.age_days <= cutoff_days:
                continue

            to_remove.append(key)

            # Delete file if requested
            if delete_files:
                path = Path(cached.path)
                if path.exists():
                    try:
                        path.unlink()
                        logger.debug(f"Deleted cached file: {path}")
                    except OSError as e:
                        logger.warning(f"Failed to delete {path}: {e}")

        # Remove from metadata
        for key in to_remove:
            del self._metadata[key]

        if to_remove:
            self._save_metadata()

        return len(to_remove)

    def get_cache_info(self) -> dict:
        """
        Get information about the cache.

        Returns
        -------
        dict
            Cache statistics including total size, file count, etc.
        """
        total_size = 0
        file_count = 0
        states = set()
        stale_count = 0

        for cached in self._metadata.values():
            path = Path(cached.path)
            if path.exists():
                total_size += cached.size_bytes
                file_count += 1
            states.add(cached.state)
            if cached.is_stale:
                stale_count += 1

        return {
            "cache_dir": str(self.cache_dir),
            "total_entries": len(self._metadata),
            "valid_files": file_count,
            "total_size_mb": total_size / (1024 * 1024),
            "states": sorted(states),
            "stale_entries": stale_count,
        }

    def list_cached(self, state: str | None = None) -> list[CachedDownload]:
        """
        List cached downloads.

        Parameters
        ----------
        state : str, optional
            Filter by state.

        Returns
        -------
        list of CachedDownload
            List of cached download metadata.
        """
        results = []
        for cached in self._metadata.values():
            if state and cached.state != state.upper():
                continue
            results.append(cached)

        return sorted(results, key=lambda x: x.state)
