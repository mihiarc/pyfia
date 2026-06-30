"""Unit tests for cache atomicity and integrity verification (#88).

Covers atomic metadata writes, size/checksum verification in get_cached(),
and atomic download writes (temp file + replace, with cleanup on failure).
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from pyfia.downloader.cache import DownloadCache


@pytest.fixture
def cache(tmp_path):
    return DownloadCache(tmp_path / "cache")


def _make_file(path, data: bytes = b"duckdb-bytes"):
    path.write_bytes(data)
    return path


class TestGetCachedIntegrity:
    def test_valid_file_returned(self, cache, tmp_path):
        f = _make_file(tmp_path / "GA.duckdb")
        cache.add_to_cache("GA", f)
        assert cache.get_cached("GA") == f

    def test_size_mismatch_treated_as_invalid(self, cache, tmp_path):
        f = _make_file(tmp_path / "GA.duckdb", b"original-content")
        cache.add_to_cache("GA", f)
        # Truncate the file behind the cache's back (simulates partial write).
        f.write_bytes(b"short")
        assert cache.get_cached("GA") is None

    def test_checksum_mismatch_with_verify(self, cache, tmp_path):
        f = _make_file(tmp_path / "GA.duckdb", b"original-content")
        cache.add_to_cache("GA", f)
        # Same length, different content — only a checksum check catches this.
        f.write_bytes(b"corrupted-conten")
        assert len(b"corrupted-conten") == len(b"original-content")
        assert cache.get_cached("GA", verify_checksum=True) is None
        # Without checksum verification the size check passes it through.
        assert cache.get_cached("GA") == f


class TestAtomicMetadataWrite:
    def test_metadata_is_valid_json_and_no_tmp_left(self, cache, tmp_path):
        f = _make_file(tmp_path / "GA.duckdb")
        cache.add_to_cache("GA", f)
        # Metadata file exists, parses, and no .tmp sidecar remains.
        assert cache.metadata_file.exists()
        json.loads(cache.metadata_file.read_text())
        assert not cache.metadata_file.with_name(
            cache.metadata_file.name + ".tmp"
        ).exists()

    def test_metadata_reloads_in_new_instance(self, cache, tmp_path):
        f = _make_file(tmp_path / "GA.duckdb")
        cache.add_to_cache("GA", f)
        reloaded = DownloadCache(cache.cache_dir)
        assert reloaded.get_cached("GA") == f


class TestAtomicDownload:
    def _client(self):
        from pyfia.downloader.client import DataMartClient

        client = DataMartClient()
        client.session = MagicMock()
        return client

    def _response(self, chunks, status=200):
        resp = MagicMock()
        resp.status_code = status
        resp.headers = {"content-length": str(sum(len(c) for c in chunks))}
        resp.raise_for_status = MagicMock()
        resp.iter_content = MagicMock(return_value=iter(chunks))
        return resp

    def test_successful_download_writes_dest_no_part(self, tmp_path):
        client = self._client()
        client.session.get.return_value = self._response([b"abc", b"def"])
        dest = tmp_path / "TREE.csv"
        out = client._download_file(
            "http://example/TREE.csv", dest, show_progress=False
        )
        assert out == dest
        assert dest.read_bytes() == b"abcdef"
        assert not dest.with_name("TREE.csv.part").exists()

    def test_interrupted_download_leaves_no_dest_or_part(self, tmp_path):
        client = self._client()

        def boom():
            yield b"abc"
            raise ConnectionError("connection dropped")

        resp = self._response([])
        resp.iter_content = MagicMock(return_value=boom())
        # max_retries defaults >1; force a single attempt so the error surfaces.
        client.max_retries = 1
        client.session.get.return_value = resp

        dest = tmp_path / "TREE.csv"
        with pytest.raises(Exception):
            client._download_file("http://example/TREE.csv", dest, show_progress=False)
        # Neither a truncated dest nor a leftover .part file should remain.
        assert not dest.exists()
        assert not dest.with_name("TREE.csv.part").exists()
