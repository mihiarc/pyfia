"""Reference-table integrity guard for downloaded databases (#86).

A failed reference-table download must not silently produce — and cache — a
database missing a reference table that pyFIA reads.
"""

from __future__ import annotations

import duckdb
import pytest

from pyfia.downloader import (
    REQUIRED_REFERENCE_TABLES,
    _missing_reference_tables,
    _verify_reference_tables_or_discard,
)
from pyfia.downloader.exceptions import DownloadError


def _make_db(path, table_rows: dict[str, int]):
    """Create a DuckDB file with the given reference tables and row counts."""
    conn = duckdb.connect(str(path))
    try:
        for name, n in table_rows.items():
            # CTAS from range(n): table always exists; n=0 -> empty table.
            conn.execute(f'CREATE TABLE "{name}" AS SELECT * FROM range({n})')
    finally:
        conn.close()
    return path


def _full_db(path):
    return _make_db(path, {t: 3 for t in REQUIRED_REFERENCE_TABLES})


class TestMissingReferenceTables:
    def test_none_missing_when_all_present(self, tmp_path):
        db = _full_db(tmp_path / "ga.duckdb")
        assert _missing_reference_tables(db) == []

    def test_absent_table_reported(self, tmp_path):
        *present, absent = REQUIRED_REFERENCE_TABLES
        db = _make_db(tmp_path / "ga.duckdb", {t: 3 for t in present})
        assert _missing_reference_tables(db) == [absent]

    def test_empty_table_reported(self, tmp_path):
        *present, empty = REQUIRED_REFERENCE_TABLES
        db = _make_db(tmp_path / "ga.duckdb", {**{t: 3 for t in present}, empty: 0})
        assert _missing_reference_tables(db) == [empty]

    def test_required_tables_are_ones_datamart_serves(self):
        # FIADB_REFERENCE.zip has no REF_STATE; requiring it failed every
        # download.
        assert "REF_STATE" not in REQUIRED_REFERENCE_TABLES


class TestVerifyOrDiscard:
    def test_passes_silently_when_complete(self, tmp_path):
        db = _full_db(tmp_path / "ga.duckdb")
        _verify_reference_tables_or_discard(db, "GA")  # no raise
        assert db.exists()

    def test_raises_and_discards_when_incomplete(self, tmp_path):
        db = _make_db(tmp_path / "ga.duckdb", {"REF_SPECIES": 3})
        with pytest.raises(DownloadError) as exc:
            _verify_reference_tables_or_discard(db, "GA")
        msg = str(exc.value)
        assert all(t in msg for t in REQUIRED_REFERENCE_TABLES[1:])
        assert "GA" in msg
        assert "retry" in msg.lower()
        # Partial database is removed so a retry rebuilds cleanly.
        assert not db.exists()
