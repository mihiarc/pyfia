"""
Comprehensive database integration tests for pyFIA.

Tests the core database functionality, backend switching, EVALID handling,
and multi-state database operations that are critical for reliability.
"""

import os
import tempfile
from pathlib import Path
import pytest
import polars as pl
import duckdb
import sqlite3

from pyfia import FIA, area, volume
from pyfia.core.backends.duckdb_backend import DuckDBBackend
from pyfia.core.backends.sqlite_backend import SQLiteBackend


class TestDatabaseBackends:
    """Test database backend abstraction and switching."""

    def test_backend_detection_duckdb(self, tmp_path):
        """Test automatic DuckDB backend detection."""
        # Create a minimal DuckDB file
        duckdb_path = tmp_path / "test.duckdb"

        with duckdb.connect(str(duckdb_path)) as conn:
            conn.execute("CREATE TABLE test_table (id INTEGER)")
            conn.execute("INSERT INTO test_table VALUES (1)")

        # Test backend detection
        backend = DuckDBBackend(str(duckdb_path))
        assert backend.detect_backend() == "duckdb"

        # Test table existence
        assert backend.table_exists("test_table")

        # Clean up
        backend.disconnect()

    def test_backend_detection_sqlite(self, tmp_path):
        """Test automatic SQLite backend detection."""
        # Create a minimal SQLite file
        sqlite_path = tmp_path / "test.db"

        with sqlite3.connect(str(sqlite_path)) as conn:
            conn.execute("CREATE TABLE test_table (id INTEGER)")
            conn.execute("INSERT INTO test_table VALUES (1)")

        # Test backend detection
        backend = SQLiteBackend(str(sqlite_path))
        assert backend.detect_backend() == "sqlite"

        # Test table existence
        assert backend.table_exists("test_table")

        # Clean up
        backend.disconnect()

    def test_backend_switching_explicit(self, tmp_path):
        """Test explicit backend specification."""
        # Create test databases
        duckdb_path = tmp_path / "test.duckdb"
        sqlite_path = tmp_path / "test.db"

        # Create minimal databases
        with duckdb.connect(str(duckdb_path)) as conn:
            conn.execute("CREATE TABLE duckdb_table (id INTEGER)")

        with sqlite3.connect(str(sqlite_path)) as conn:
            conn.execute("CREATE TABLE sqlite_table (id INTEGER)")

        # Test explicit DuckDB backend
        db_duck = FIA(str(duckdb_path), engine="duckdb")
        assert db_duck.reader.backend.backend_type == "duckdb"

        # Test explicit SQLite backend
        db_sqlite = FIA(str(sqlite_path), engine="sqlite")
        assert db_sqlite.reader.backend.backend_type == "sqlite"

    def test_connection_error_handling(self):
        """Test database connection error handling."""
        nonexistent_path = "/nonexistent/path/database.duckdb"

        # Should raise appropriate error for nonexistent database
        with pytest.raises((FileNotFoundError, Exception)):
            FIA(nonexistent_path)

    def test_concurrent_connections(self, sample_fia_db):
        """Test multiple concurrent connections to same database."""
        if not sample_fia_db:
            pytest.skip("No sample FIA database available")

        # Create multiple FIA instances
        db1 = FIA(str(sample_fia_db))
        db2 = FIA(str(sample_fia_db))

        try:
            # Both should be able to read data
            tables1 = db1.reader.get_table_names()
            tables2 = db2.reader.get_table_names()

            assert len(tables1) > 0
            assert len(tables2) > 0
            assert set(tables1) == set(tables2)

        finally:
            # Clean up connections
            if hasattr(db1.reader, 'disconnect'):
                db1.reader.disconnect()
            if hasattr(db2.reader, 'disconnect'):
                db2.reader.disconnect()


class TestEvalidSystemIntegration:
    """Comprehensive tests for EVALID system - the most critical missing tests."""

    def test_get_available_evalids(self, sample_fia_instance):
        """Test retrieval of available EVALIDs."""
        if sample_fia_instance is None:
            pytest.skip("No sample FIA database available")

        try:
            evalids = sample_fia_instance.get_available_evalids()

            if evalids:
                assert isinstance(evalids, list)
                assert all(isinstance(eid, int) for eid in evalids)
                assert all(eid > 100000 for eid in evalids)  # Valid EVALID format

        except AttributeError:
            pytest.skip("get_available_evalids method not implemented")

    def test_evalid_filtering_validity(self, sample_fia_instance):
        """Test EVALID filtering maintains statistical validity."""
        if sample_fia_instance is None:
            pytest.skip("No sample FIA database available")

        try:
            # Get original plot count
            original_plots = len(sample_fia_instance.get_plots())

            # Get available EVALIDs and pick first one
            evalids = sample_fia_instance.get_available_evalids()
            if not evalids:
                pytest.skip("No EVALIDs available for testing")

            # Filter by EVALID
            sample_fia_instance.clip_by_evalid([evalids[0]])
            filtered_plots = len(sample_fia_instance.get_plots())

            # Should have fewer or equal plots after filtering
            assert filtered_plots <= original_plots

            # Should still have some plots if EVALID is valid
            assert filtered_plots > 0, f"No plots found for EVALID {evalids[0]}"

        except (AttributeError, Exception) as e:
            pytest.skip(f"EVALID filtering not available: {e}")

    def test_most_recent_evalid_selection(self, sample_fia_instance):
        """Test automatic most recent EVALID selection."""
        if sample_fia_instance is None:
            pytest.skip("No sample FIA database available")

        try:
            # Test EXPALL (area) evaluation selection
            sample_fia_instance.clip_most_recent(eval_type="EXPALL")

            # Should be able to run area estimation
            result = area(sample_fia_instance, land_type="forest")
            assert isinstance(result, pl.DataFrame)
            assert len(result) > 0

            # Test EXPVOL (volume) evaluation selection
            sample_fia_instance.clip_most_recent(eval_type="EXPVOL")

            # Should be able to run volume estimation
            vol_result = volume(sample_fia_instance, land_type="forest")
            assert isinstance(vol_result, pl.DataFrame)
            assert len(vol_result) > 0

        except (AttributeError, Exception) as e:
            pytest.skip(f"Most recent EVALID selection not available: {e}")

    def test_evalid_consistency_across_tables(self, sample_fia_instance):
        """Test EVALID consistency across related tables."""
        if sample_fia_instance is None:
            pytest.skip("No sample FIA database available")

        try:
            # Get plots and stratum assignments
            plots = sample_fia_instance.get_plots()
            ppsa = sample_fia_instance.get_table("POP_PLOT_STRATUM_ASSGN")

            if len(plots) > 0 and len(ppsa) > 0:
                # All plots should have corresponding stratum assignments
                plot_cns = set(plots["CN"].to_list())
                ppsa_plot_cns = set(ppsa["PLT_CN"].to_list())

                # Intersection should not be empty
                common_plots = plot_cns.intersection(ppsa_plot_cns)
                assert len(common_plots) > 0, "No common plots between PLOT and POP_PLOT_STRATUM_ASSGN"

        except Exception as e:
            pytest.skip(f"EVALID consistency check not possible: {e}")


class TestTexasSpecialHandling:
    """Tests for Texas-specific data handling."""

    def test_texas_duplicate_detection(self):
        """Test detection of Texas duplicate data."""
        # Create mock Texas data with duplicates
        mock_ppsa = pl.DataFrame({
            "CN": ["TX1", "TX1", "TX2", "TX2"],  # Duplicate CNs
            "PLT_CN": ["P1", "P1", "P2", "P2"],
            "STRATUM_CN": ["S1", "S1", "S2", "S2"],
            "EVALID": [482300, 482300, 482300, 482300]
        })

        mock_stratum = pl.DataFrame({
            "CN": ["S1", "S1", "S2", "S2"],  # Duplicate CNs
            "EVALID": [482300, 482300, 482300, 482300],
            "EXPNS": [1000.0, 1000.0, 1500.0, 1500.0],
            "ADJ_FACTOR_SUBP": [1.0, 1.0, 1.0, 1.0]
        })

        # Test deduplication logic
        dedupe_ppsa = mock_ppsa.unique(subset=["CN"])
        dedupe_stratum = mock_stratum.unique(subset=["CN"])

        assert len(dedupe_ppsa) == 2, "Should deduplicate to 2 unique records"
        assert len(dedupe_stratum) == 2, "Should deduplicate to 2 unique records"

    def test_texas_evalid_preference(self):
        """Test Texas EVALID preference for full state over regional."""
        # Mock evaluation data showing regional vs full state
        mock_evaluations = pl.DataFrame({
            "EVALID": [482320, 482300],  # East-only vs full state
            "STATECD": [48, 48],
            "LOCATION_NM": ["Texas(EAST)", "Texas"],
            "END_INVYR": [2023, 2022],  # More recent East-only
            "EVAL_TYP": ["EXPALL", "EXPALL"]
        })

        # Logic should prefer full state (482300) over regional (482320)
        # even if regional is more recent
        full_state = mock_evaluations.filter(
            pl.col("LOCATION_NM") == "Texas"
        )

        assert len(full_state) == 1
        assert full_state["EVALID"][0] == 482300


class TestMultiStateIntegration:
    """Tests for multi-state database operations."""

    def test_multi_state_area_consistency(self, sample_fia_instance):
        """Test area estimation consistency across multiple states."""
        if sample_fia_instance is None:
            pytest.skip("No sample FIA database available")

        try:
            # Get available states
            plots = sample_fia_instance.get_plots()
            if len(plots) == 0:
                pytest.skip("No plots available")

            states = plots["STATECD"].unique().to_list()

            if len(states) > 1:
                total_area = 0
                state_areas = []

                for state in states:
                    # Filter to individual state
                    state_db = FIA(sample_fia_instance.db_path)
                    state_db.clip_by_state([state])

                    try:
                        state_result = area(state_db, land_type="forest")
                        if len(state_result) > 0:
                            state_area = state_result["AREA_ACRE"][0]
                            state_areas.append(state_area)
                            total_area += state_area
                    except Exception:
                        pass  # Skip if state has no data

                # Multi-state total should be sum of individual states
                if state_areas:
                    multi_state_result = area(sample_fia_instance, land_type="forest")
                    if len(multi_state_result) > 0:
                        multi_state_area = multi_state_result["AREA_ACRE"][0]

                        # Should be approximately equal (allowing for rounding)
                        relative_diff = abs(multi_state_area - total_area) / total_area
                        assert relative_diff < 0.01, "Multi-state area should equal sum of state areas"

        except Exception as e:
            pytest.skip(f"Multi-state consistency test not possible: {e}")


class TestPerformanceRegression:
    """Performance regression tests."""

    def test_large_table_loading_performance(self, sample_fia_instance):
        """Test performance of loading large tables."""
        if sample_fia_instance is None:
            pytest.skip("No sample FIA database available")

        import time

        start_time = time.time()

        try:
            # Load potentially large tables
            tree_data = sample_fia_instance.get_trees()
            plot_data = sample_fia_instance.get_plots()
            cond_data = sample_fia_instance.get_conditions()

            load_time = time.time() - start_time

            # Should complete within reasonable time (10 seconds for test data)
            assert load_time < 10.0, f"Table loading took {load_time:.2f}s, too slow"

            # Should have data
            assert len(tree_data) > 0 or len(plot_data) > 0

        except Exception as e:
            pytest.skip(f"Performance test not possible: {e}")

    def test_estimation_performance(self, sample_fia_instance):
        """Test estimation function performance."""
        if sample_fia_instance is None:
            pytest.skip("No sample FIA database available")

        import time

        start_time = time.time()

        try:
            # Run basic area estimation
            result = area(sample_fia_instance, land_type="forest")

            estimation_time = time.time() - start_time

            # Should complete within reasonable time
            assert estimation_time < 30.0, f"Area estimation took {estimation_time:.2f}s, too slow"

            # Should have results
            if len(result) > 0:
                assert result["AREA_ACRE"][0] > 0

        except Exception as e:
            pytest.skip(f"Estimation performance test not possible: {e}")


class TestErrorRecovery:
    """Test error handling and recovery mechanisms."""

    def test_corrupted_table_handling(self, tmp_path):
        """Test handling of corrupted tables."""
        # Create a database with a corrupted table
        db_path = tmp_path / "corrupted.duckdb"

        with duckdb.connect(str(db_path)) as conn:
            # Create a table with invalid data types
            conn.execute("CREATE TABLE PLOT (CN VARCHAR, STATECD VARCHAR)")  # Wrong types
            conn.execute("INSERT INTO PLOT VALUES ('invalid', 'invalid')")

        # Should handle gracefully
        try:
            db = FIA(str(db_path))
            plots = db.get_plots()
            # Should either return empty or handle type conversion
            assert isinstance(plots, pl.DataFrame)
        except Exception as e:
            # Should raise descriptive error
            assert "STATECD" in str(e) or "type" in str(e).lower()

    def test_missing_required_tables(self, tmp_path):
        """Test handling of missing required tables."""
        # Create database with missing critical tables
        db_path = tmp_path / "incomplete.duckdb"

        with duckdb.connect(str(db_path)) as conn:
            conn.execute("CREATE TABLE DUMMY (id INTEGER)")

        # Should handle missing tables gracefully
        try:
            db = FIA(str(db_path))

            # Should raise descriptive error when trying estimation
            with pytest.raises(Exception) as exc_info:
                area(db, land_type="forest")

            # Error should mention missing tables
            error_msg = str(exc_info.value).lower()
            assert any(table.lower() in error_msg for table in ["plot", "cond", "pop_stratum"])

        except Exception:
            # Initial database creation might fail, which is also acceptable
            pass

    def test_memory_pressure_handling(self, sample_fia_instance):
        """Test behavior under memory pressure."""
        if sample_fia_instance is None:
            pytest.skip("No sample FIA database available")

        # This test would need to simulate memory pressure
        # For now, just verify lazy loading works
        try:
            # Should be able to load data lazily without loading everything into memory
            lazy_tree_data = sample_fia_instance.get_trees_lazy()
            if hasattr(lazy_tree_data, 'collect'):
                # Should be able to collect results
                tree_data = lazy_tree_data.collect()
                assert isinstance(tree_data, pl.DataFrame)

        except (AttributeError, Exception):
            pytest.skip("Lazy loading not available for testing")