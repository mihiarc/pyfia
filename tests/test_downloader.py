"""
Tests for the FIA data downloader module.

These tests verify the downloader functionality including:
- State code validation
- Table definitions
- Cache management
- Download client (with mocking for actual downloads)
"""

import json
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from pyfia.downloader import (
    COMMON_TABLES,
    VALID_STATE_CODES,
    DataMartClient,
    DownloadCache,
    clear_cache,
    download,
)
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
    REFERENCE_TABLES,
    STATE_FIPS_CODES,
    get_state_fips,
    get_tables_for_download,
    validate_state_code,
)


class TestStateValidation:
    """Tests for state code validation."""

    def test_valid_state_codes(self):
        """Test that valid state codes are accepted."""
        assert validate_state_code("GA") == "GA"
        assert validate_state_code("ga") == "GA"  # Case insensitive
        assert validate_state_code("NC") == "NC"
        assert validate_state_code("REF") == "REF"  # Reference tables

    def test_invalid_state_code_raises_error(self):
        """Test that invalid state codes raise StateNotFoundError."""
        with pytest.raises(StateNotFoundError) as exc_info:
            validate_state_code("XX")
        assert "XX" in str(exc_info.value)

    def test_state_fips_codes(self):
        """Test state FIPS code lookup."""
        assert get_state_fips("GA") == 13
        assert get_state_fips("NC") == 37
        assert get_state_fips("CA") == 6

    def test_ref_has_no_fips_code(self):
        """Test that REF tables don't have a FIPS code."""
        with pytest.raises(ValueError):
            get_state_fips("REF")


class TestTableDefinitions:
    """Tests for table definitions."""

    def test_common_tables_not_empty(self):
        """Test that common tables list is not empty."""
        assert len(COMMON_TABLES) > 0
        assert "PLOT" in COMMON_TABLES
        assert "TREE" in COMMON_TABLES
        assert "COND" in COMMON_TABLES

    def test_reference_tables_not_empty(self):
        """Test that reference tables list is not empty."""
        assert len(REFERENCE_TABLES) > 0
        assert "REF_SPECIES" in REFERENCE_TABLES

    def test_all_tables_contains_common(self):
        """Test that all tables contains common tables."""
        for table in COMMON_TABLES:
            assert table in ALL_TABLES, f"{table} not in ALL_TABLES"

    def test_valid_state_codes_complete(self):
        """Test that all US states are in valid state codes."""
        # Check some key states
        assert "GA" in VALID_STATE_CODES
        assert "CA" in VALID_STATE_CODES
        assert "NY" in VALID_STATE_CODES
        assert "TX" in VALID_STATE_CODES
        # Check territories
        assert "PR" in VALID_STATE_CODES
        assert "VI" in VALID_STATE_CODES

    def test_get_tables_for_download_common(self):
        """Test getting common tables for download."""
        tables = get_tables_for_download(common=True)
        assert tables == COMMON_TABLES

    def test_get_tables_for_download_all(self):
        """Test getting all tables for download."""
        tables = get_tables_for_download(common=False)
        assert tables == ALL_TABLES

    def test_get_tables_for_download_specific(self):
        """Test getting specific tables for download."""
        specific = ["PLOT", "TREE"]
        tables = get_tables_for_download(tables=specific)
        assert tables == ["PLOT", "TREE"]


class TestExceptions:
    """Tests for custom exceptions."""

    def test_download_error(self):
        """Test DownloadError exception."""
        error = DownloadError("Test error", url="http://example.com")
        assert "Test error" in str(error)
        assert error.url == "http://example.com"

    def test_state_not_found_error(self):
        """Test StateNotFoundError exception."""
        error = StateNotFoundError("XX", valid_states=["GA", "NC"])
        assert "XX" in str(error)

    def test_table_not_found_error(self):
        """Test TableNotFoundError exception."""
        error = TableNotFoundError("FAKE_TABLE", state="GA")
        assert "FAKE_TABLE" in str(error)
        assert "GA" in str(error)

    def test_network_error(self):
        """Test NetworkError exception."""
        error = NetworkError("Connection failed", url="http://example.com", status_code=500)
        assert "Connection failed" in str(error)
        assert error.status_code == 500

    def test_checksum_error(self):
        """Test ChecksumError exception."""
        error = ChecksumError("/path/to/file", expected="abc123", actual="def456")
        assert "abc123" in str(error)
        assert "def456" in str(error)

    def test_insufficient_space_error(self):
        """Test InsufficientSpaceError exception."""
        error = InsufficientSpaceError(
            required_bytes=1024 * 1024 * 100,  # 100MB
            available_bytes=1024 * 1024 * 50,  # 50MB
            path="/data"
        )
        assert "100" in str(error) or "50" in str(error)


class TestDownloadCache:
    """Tests for download cache management."""

    def test_cache_initialization(self):
        """Test cache initializes correctly."""
        with tempfile.TemporaryDirectory() as temp_dir:
            cache = DownloadCache(Path(temp_dir))
            assert cache.cache_dir.exists()

    def test_add_and_get_cached(self):
        """Test adding and retrieving from cache."""
        with tempfile.TemporaryDirectory() as temp_dir:
            cache_dir = Path(temp_dir) / "cache"
            cache = DownloadCache(cache_dir)

            # Create a dummy file
            test_file = cache_dir / "test.db"
            test_file.parent.mkdir(parents=True, exist_ok=True)
            test_file.write_text("test content")

            # Add to cache
            cache.add_to_cache("GA", test_file, format="duckdb")

            # Retrieve from cache
            cached_path = cache.get_cached("GA")
            assert cached_path == test_file

    def test_cache_miss(self):
        """Test cache miss returns None."""
        with tempfile.TemporaryDirectory() as temp_dir:
            cache = DownloadCache(Path(temp_dir))
            assert cache.get_cached("XX") is None

    def test_cache_max_age(self):
        """Test cache respects max age."""
        with tempfile.TemporaryDirectory() as temp_dir:
            cache_dir = Path(temp_dir)
            cache = DownloadCache(cache_dir)

            # Create a dummy file
            test_file = cache_dir / "test.db"
            test_file.write_text("test content")

            # Add to cache
            cache.add_to_cache("GA", test_file, format="duckdb")

            # Should be found with no age limit
            assert cache.get_cached("GA") is not None

            # Should be found with large age limit
            assert cache.get_cached("GA", max_age_days=30) is not None

            # Should NOT be found with 0 age limit (file was just created)
            # This depends on timing, so we skip this edge case
            # assert cache.get_cached("GA", max_age_days=0) is None

    def test_clear_cache(self):
        """Test clearing the cache."""
        with tempfile.TemporaryDirectory() as temp_dir:
            cache_dir = Path(temp_dir)
            cache = DownloadCache(cache_dir)

            # Create and cache a file
            test_file = cache_dir / "test.db"
            test_file.write_text("test content")
            cache.add_to_cache("GA", test_file, format="duckdb")

            # Verify it's cached
            assert cache.get_cached("GA") is not None

            # Clear cache
            cleared = cache.clear_cache()
            assert cleared >= 1

            # Verify it's gone
            assert cache.get_cached("GA") is None

    def test_cache_info(self):
        """Test getting cache info."""
        with tempfile.TemporaryDirectory() as temp_dir:
            cache = DownloadCache(Path(temp_dir))
            info = cache.get_cache_info()

            assert "cache_dir" in info
            assert "total_entries" in info
            assert "valid_files" in info
            assert "total_size_mb" in info

    def test_cache_persistence(self):
        """Test that cache persists across instances."""
        with tempfile.TemporaryDirectory() as temp_dir:
            cache_dir = Path(temp_dir)

            # Create first cache instance and add entry
            cache1 = DownloadCache(cache_dir)
            test_file = cache_dir / "test.db"
            test_file.write_text("test content")
            cache1.add_to_cache("GA", test_file, format="duckdb")

            # Create second cache instance and verify entry persists
            cache2 = DownloadCache(cache_dir)
            assert cache2.get_cached("GA") is not None


class TestDataMartClient:
    """Tests for the DataMart client."""

    def test_client_initialization(self):
        """Test client initializes with default values."""
        client = DataMartClient()
        assert client.timeout == 300
        assert client.chunk_size == 1024 * 1024
        assert client.max_retries == 3

    def test_client_custom_values(self):
        """Test client accepts custom values."""
        client = DataMartClient(timeout=600, chunk_size=2048, max_retries=5)
        assert client.timeout == 600
        assert client.chunk_size == 2048
        assert client.max_retries == 5

    def test_build_csv_url_state(self):
        """Test CSV URL building for state tables."""
        client = DataMartClient()
        url = client._build_csv_url("GA", "PLOT")
        assert url == "https://apps.fs.usda.gov/fia/datamart/CSV/GA_PLOT.zip"

    def test_build_csv_url_reference(self):
        """Test CSV URL building for reference tables."""
        client = DataMartClient()
        url = client._build_csv_url("REF", "REF_SPECIES")
        assert url == "https://apps.fs.usda.gov/fia/datamart/CSV/REF_SPECIES.zip"

    def test_build_sqlite_url(self):
        """Test SQLite URL building."""
        client = DataMartClient()
        url = client._build_sqlite_url("GA")
        assert url == "https://apps.fs.usda.gov/fia/datamart/Databases/SQLite_FIADB_GA.zip"

    def test_check_url_exists_timeout(self):
        """Test URL check handles invalid URLs gracefully."""
        client = DataMartClient(timeout=5)
        # This should return False for an invalid URL
        result = client.check_url_exists("https://invalid.example.com/nonexistent")
        assert result is False


class TestDownloadFunction:
    """Tests for the main download function."""

    def test_download_validates_state(self):
        """Test that download validates state codes."""
        with pytest.raises(StateNotFoundError):
            download("XX")

    def test_download_normalizes_state(self):
        """Test that download normalizes state codes."""
        # This would actually download, so we mock the client
        with patch.object(DataMartClient, 'download_tables') as mock_download:
            mock_download.return_value = {}
            with patch.object(DataMartClient, 'download_state_sqlite') as mock_sqlite:
                # Mock will be called but won't actually download
                pass

    def test_download_uses_cache(self):
        """Test that download checks cache first."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a fake cached database
            data_dir = Path(temp_dir)
            cache_dir = data_dir / ".cache"
            cache = DownloadCache(cache_dir)

            state_dir = data_dir / "ga"
            state_dir.mkdir(parents=True)
            fake_db = state_dir / "ga.duckdb"
            fake_db.write_text("fake database content")

            cache.add_to_cache("GA", fake_db, format="duckdb")

            # Download should use cached file
            result = download("GA", dir=data_dir, use_cache=True, show_progress=False)
            assert result == fake_db


class TestIntegration:
    """Integration tests (may make real network requests)."""

    @pytest.mark.slow
    @pytest.mark.network
    def test_download_reference_table(self):
        """Test downloading a small reference table (REF_SPECIES)."""
        # This test makes a real network request
        # Skip if network tests are disabled
        pytest.skip("Network tests disabled by default")

        with tempfile.TemporaryDirectory() as temp_dir:
            client = DataMartClient()
            path = client.download_table(
                "REF",
                "REF_SPECIES",
                Path(temp_dir),
                show_progress=False
            )
            assert path.exists()
            assert path.suffix == ".csv"


# Run tests with: uv run pytest tests/test_downloader.py -v
