"""
End-to-end integration tests for FIA data download.

These tests make real network requests to the FIA DataMart and verify
the complete download → convert → analyze pipeline works correctly.

Run with: uv run pytest tests/integration/test_download_e2e.py -v
"""

import tempfile
from pathlib import Path

import polars as pl
import pytest

from pyfia import FIA, area, download
from pyfia.downloader import (
    DataMartClient,
)
from pyfia.downloader.tables import get_state_fips


class TestDownloadE2E:
    """End-to-end tests for FIA data downloads using Rhode Island (smallest state)."""

    @pytest.fixture
    def temp_data_dir(self):
        """Create a temporary directory for test downloads."""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield Path(temp_dir)

    def test_download_single_table_csv(self, temp_data_dir):
        """Test downloading a single CSV table for RI."""
        client = DataMartClient(timeout=120)

        # Download just the SURVEY table (very small)
        csv_path = client.download_table(
            state="RI",
            table="SURVEY",
            dest_dir=temp_data_dir,
            show_progress=True,
        )

        assert csv_path.exists()
        assert csv_path.suffix == ".csv"

        # Verify we can read it with polars
        df = pl.read_csv(csv_path)
        assert len(df) > 0
        assert "STATECD" in df.columns

        # Verify it's Rhode Island data (STATECD = 44)
        state_codes = df["STATECD"].unique().to_list()
        assert 44 in state_codes, f"Expected Rhode Island (44), got {state_codes}"

    def test_download_state_duckdb(self, temp_data_dir):
        """Test downloading RI data and converting to DuckDB."""
        # Download with common tables only (faster)
        db_path = download(
            states="RI",
            dir=temp_data_dir,
            common=True,
            force=True,
            show_progress=True,
        )

        assert db_path.exists()
        assert db_path.suffix == ".duckdb"

        # Verify we can open it with FIA class
        with FIA(db_path) as db:
            # Check that key tables exist
            plot_df = db.load_table("PLOT").collect()
            assert len(plot_df) > 0

            tree_df = db.load_table("TREE").collect()
            assert len(tree_df) > 0

            cond_df = db.load_table("COND").collect()
            assert len(cond_df) > 0

            # Verify STATE_ADDED column was added
            assert "STATE_ADDED" in plot_df.columns
            ri_fips = get_state_fips("RI")
            assert plot_df["STATE_ADDED"].unique().to_list() == [ri_fips]

    def test_download_specific_tables(self, temp_data_dir):
        """Test downloading only specific tables."""
        # Download only PLOT and SURVEY tables
        db_path = download(
            states="RI",
            dir=temp_data_dir,
            tables=["PLOT", "SURVEY"],
            force=True,
            show_progress=True,
        )

        assert db_path.exists()

        # Verify only requested tables were created
        import duckdb

        conn = duckdb.connect(str(db_path), read_only=True)
        tables = conn.execute(
            "SELECT table_name FROM information_schema.tables"
        ).fetchall()
        table_names = [t[0] for t in tables]
        conn.close()

        assert "PLOT" in table_names
        assert "SURVEY" in table_names
        # TREE should NOT be in there since we didn't request it
        assert "TREE" not in table_names

    def test_download_with_cache(self, temp_data_dir):
        """Test that caching works correctly."""
        # First download
        db_path1 = download(
            states="RI",
            dir=temp_data_dir,
            tables=["SURVEY"],  # Small table for speed
            force=True,
            show_progress=True,
            use_cache=True,
        )

        # Record file modification time
        mtime1 = db_path1.stat().st_mtime

        # Second download should use cache
        db_path2 = download(
            states="RI",
            dir=temp_data_dir,
            tables=["SURVEY"],
            force=False,  # Don't force re-download
            show_progress=True,
            use_cache=True,
        )

        # Should return same path
        assert db_path1 == db_path2

        # File should not have been modified
        mtime2 = db_path2.stat().st_mtime
        assert mtime1 == mtime2, "Cache should have prevented re-download"

    def test_download_force_redownload(self, temp_data_dir):
        """Test that force=True re-downloads even with cache."""
        # First download
        db_path1 = download(
            states="RI",
            dir=temp_data_dir,
            tables=["SURVEY"],
            force=True,
            show_progress=True,
            use_cache=True,
        )

        mtime1 = db_path1.stat().st_mtime

        # Force re-download
        import time

        time.sleep(0.1)  # Ensure different timestamp

        db_path2 = download(
            states="RI",
            dir=temp_data_dir,
            tables=["SURVEY"],
            force=True,  # Force re-download
            show_progress=True,
            use_cache=True,
        )

        # File should have been re-created
        mtime2 = db_path2.stat().st_mtime
        assert mtime2 > mtime1, "force=True should re-download"

    def test_from_download_convenience_method(self, temp_data_dir):
        """Test FIA.from_download() convenience method."""
        # Use from_download to download and open in one step
        db = FIA.from_download(
            states="RI",
            dir=temp_data_dir,
            tables=["PLOT", "COND", "SURVEY"],
            force=True,
            show_progress=True,
        )

        # Verify we have a working FIA instance
        assert isinstance(db, FIA)
        assert db.db_path.exists()

        # Can load tables
        plot_df = db.load_table("PLOT").collect()
        assert len(plot_df) > 0


class TestDownloadAnalysisPipeline:
    """Test the complete download → analyze pipeline."""

    @pytest.fixture
    def temp_data_dir(self):
        """Create a temporary directory for test downloads."""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield Path(temp_dir)

    def test_download_and_estimate_area(self, temp_data_dir):
        """Test downloading RI data and running area estimation.

        Note: RI is a small state and may not have all evaluation types.
        We find available evaluations first before running estimation.
        """
        from pyfia.core.exceptions import NoEVALIDError

        # Download Rhode Island data
        db_path = download(
            states="RI",
            dir=temp_data_dir,
            common=True,
            force=True,
            show_progress=True,
        )

        # Run area estimation
        with FIA(db_path) as db:
            # Find available evaluations first
            available = db.find_evalid()

            # Try EXPALL first, fall back to any available eval type
            try:
                db.clip_most_recent(eval_type="EXPALL")
            except NoEVALIDError:
                # EXPALL may not be available in RI, use most recent of any type
                if len(available) > 0:
                    # find_evalid() returns a list of EVALIDs, get the max
                    most_recent_evalid = max(available)
                    db.clip_by_evalid(most_recent_evalid)
                else:
                    pytest.skip("No evaluations available in downloaded RI data")

            # Estimate forest area
            result = area(db, land_type="forest")

            # Verify we got results
            assert isinstance(result, pl.DataFrame)
            assert len(result) > 0

            # Check expected columns
            assert "TOTAL" in result.columns or any(
                "AREA" in c.upper() for c in result.columns
            )

            # Rhode Island is small but should have some forest
            # Total area (all sampled land) is about 700k-800k acres
            # Forest area is about 350k-400k acres
            total_col = [c for c in result.columns if "TOTAL" in c.upper()][0]
            total_area = result[total_col][0]

            # Sanity check: RI area should be positive and reasonable
            # (Total sampled land ~ 700k-800k acres, forest ~ 350k-400k acres)
            assert 100_000 < total_area < 2_000_000, (
                f"RI area {total_area:,.0f} seems wrong"
            )


class TestReferenceTableDownload:
    """Test downloading reference tables."""

    @pytest.fixture
    def temp_data_dir(self):
        """Create a temporary directory for test downloads."""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield Path(temp_dir)

    @pytest.mark.skip(
        reason="Reference tables are bundled in FIADB_REFERENCE.zip, not individual files"
    )
    def test_download_reference_species(self, temp_data_dir):
        """Test downloading REF_SPECIES table.

        Note: FIA DataMart bundles all reference tables in FIADB_REFERENCE.zip
        rather than providing individual files like REF_SPECIES.zip.
        This test is skipped until we implement reference bundle download.
        """
        client = DataMartClient(timeout=120)

        csv_path = client.download_table(
            state="REF",
            table="REF_SPECIES",
            dest_dir=temp_data_dir,
            show_progress=True,
        )

        assert csv_path.exists()

        # Verify contents
        df = pl.read_csv(csv_path)
        assert len(df) > 100  # Should have many species

        # Check for expected columns
        assert "SPCD" in df.columns  # Species code
        assert "COMMON_NAME" in df.columns or "GENUS" in df.columns


# Run with: uv run pytest tests/integration/test_download_e2e.py -v -s
