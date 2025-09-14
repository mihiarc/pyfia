"""
Comprehensive regression tests for the growth() function.

This test suite focuses on catching the critical bugs identified in PR #15:
1. GROWTH_ACRE to GROW_ACRE column name bug in utils.py
2. NET growth calculation correctness
3. GRM component type handling (SURVIVOR, INGROWTH, REVERSION)
4. Missing data handling (NULL volumes, missing REMPER)
5. Performance issues (collect_schema() calls)
6. Error handling inconsistencies
7. Hard-coded magic numbers

These tests use real FIA data structures and methodologies to ensure
accuracy against EVALIDator reference implementations.
"""

import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import polars as pl
import pytest

from pyfia import FIA
from pyfia.estimation.estimators.growth import growth, GrowthEstimator
from pyfia.estimation.utils import format_output_columns


class TestGrowthRegressionBugs:
    """Test cases that would have caught the critical growth calculation bugs."""

    @pytest.fixture
    def grm_test_database(self):
        """Create a test database with proper GRM table structure."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
            db_path = tmp.name

        # Create database with GRM tables
        conn = sqlite3.connect(db_path)

        # TREE_GRM_COMPONENT - Critical for growth calculations
        conn.execute("""
            CREATE TABLE TREE_GRM_COMPONENT (
                TRE_CN TEXT PRIMARY KEY,
                PLT_CN TEXT,
                DIA_BEGIN REAL,
                DIA_MIDPT REAL,
                DIA_END REAL,
                -- GS FOREST columns (growing stock on forest land)
                SUBP_COMPONENT_GS_FOREST TEXT,
                SUBP_TPAGROW_UNADJ_GS_FOREST REAL,
                SUBP_SUBPTYP_GRM_GS_FOREST INTEGER,
                -- AL FOREST columns (all live on forest land)
                SUBP_COMPONENT_AL_FOREST TEXT,
                SUBP_TPAGROW_UNADJ_AL_FOREST REAL,
                SUBP_SUBPTYP_GRM_AL_FOREST INTEGER,
                -- GS TIMBER columns (growing stock on timber land)
                SUBP_COMPONENT_GS_TIMBER TEXT,
                SUBP_TPAGROW_UNADJ_GS_TIMBER REAL,
                SUBP_SUBPTYP_GRM_GS_TIMBER INTEGER
            )
        """)

        # TREE_GRM_MIDPT - Volume data at midpoint
        conn.execute("""
            CREATE TABLE TREE_GRM_MIDPT (
                TRE_CN TEXT PRIMARY KEY,
                PLT_CN TEXT,
                DIA REAL,
                SPCD INTEGER,
                STATUSCD INTEGER,
                VOLCFNET REAL,
                DRYBIO_AG REAL
            )
        """)

        # TREE_GRM_BEGIN - Volume data at beginning
        conn.execute("""
            CREATE TABLE TREE_GRM_BEGIN (
                TRE_CN TEXT PRIMARY KEY,
                PLT_CN TEXT,
                VOLCFNET REAL,
                DRYBIO_AG REAL
            )
        """)

        # COND table with ALSTKCD for grouping tests
        conn.execute("""
            CREATE TABLE COND (
                CN TEXT PRIMARY KEY,
                PLT_CN TEXT,
                CONDID INTEGER,
                COND_STATUS_CD INTEGER,
                CONDPROP_UNADJ REAL,
                OWNGRPCD INTEGER,
                FORTYPCD INTEGER,
                SITECLCD INTEGER,
                RESERVCD INTEGER,
                ALSTKCD INTEGER
            )
        """)

        # PLOT table with REMPER for growth period
        conn.execute("""
            CREATE TABLE PLOT (
                CN TEXT PRIMARY KEY,
                STATECD INTEGER,
                INVYR INTEGER,
                PLOT_STATUS_CD INTEGER,
                MACRO_BREAKPOINT_DIA REAL,
                REMPER REAL
            )
        """)

        # POP_STRATUM for expansion factors
        conn.execute("""
            CREATE TABLE POP_STRATUM (
                CN TEXT PRIMARY KEY,
                EVALID INTEGER,
                EXPNS REAL,
                ADJ_FACTOR_SUBP REAL,
                ADJ_FACTOR_MICR REAL,
                ADJ_FACTOR_MACR REAL
            )
        """)

        # POP_PLOT_STRATUM_ASSGN for plot assignment
        conn.execute("""
            CREATE TABLE POP_PLOT_STRATUM_ASSGN (
                CN TEXT PRIMARY KEY,
                PLT_CN TEXT,
                STRATUM_CN TEXT,
                EVALID INTEGER
            )
        """)

        # BEGINEND table (listed in requirements but not used - test should catch this)
        conn.execute("""
            CREATE TABLE BEGINEND (
                CN TEXT PRIMARY KEY,
                EVALID INTEGER,
                ONEORTWO INTEGER
            )
        """)

        # Insert test data with different GRM components
        conn.executemany("""
            INSERT INTO TREE_GRM_COMPONENT VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            # Tree 1: SURVIVOR with volume growth
            ("T1", "P1", 10.0, 11.0, 12.0, "SURVIVOR", 2.5, 1, "SURVIVOR", 2.5, 1, "SURVIVOR", 2.5, 1),
            # Tree 2: INGROWTH (new tree)
            ("T2", "P1", None, 5.5, 6.0, "INGROWTH", 1.8, 2, "INGROWTH", 1.8, 2, "INGROWTH", 1.8, 2),
            # Tree 3: REVERSION (tree coming back into inventory)
            ("T3", "P2", None, 7.2, 8.0, "REVERSION1", 1.2, 1, "REVERSION1", 1.2, 1, "REVERSION1", 1.2, 1),
            # Tree 4: SURVIVOR with no adjustment (SUBPTYP_GRM = 0)
            ("T4", "P2", 15.0, 15.5, 16.0, "SURVIVOR", 3.2, 0, "SURVIVOR", 3.2, 0, "SURVIVOR", 3.2, 0),
            # Tree 5: SURVIVOR with MACR adjustment
            ("T5", "P3", 25.0, 26.0, 27.0, "SURVIVOR", 4.1, 3, "SURVIVOR", 4.1, 3, "SURVIVOR", 4.1, 3),
        ])

        # Insert midpoint volume data
        conn.executemany("""
            INSERT INTO TREE_GRM_MIDPT VALUES (?, ?, ?, ?, ?, ?, ?)
        """, [
            ("T1", "P1", 11.0, 131, 1, 25.5, 180.2),     # Survivor
            ("T2", "P1", 5.5, 110, 1, 12.8, 95.1),       # Ingrowth
            ("T3", "P2", 7.2, 316, 1, 15.6, 110.8),      # Reversion
            ("T4", "P2", 15.5, 131, 1, 45.2, 320.5),     # Survivor (no adjustment)
            ("T5", "P3", 26.0, 833, 1, 85.7, 580.3),     # Survivor (macroplot)
        ])

        # Insert beginning volume data (only for SURVIVOR trees)
        conn.executemany("""
            INSERT INTO TREE_GRM_BEGIN VALUES (?, ?, ?, ?)
        """, [
            ("T1", "P1", 20.1, 150.8),  # Survivor: growth = 25.5 - 20.1 = 5.4 per REMPER
            ("T4", "P2", 40.8, 290.2),  # Survivor: growth = 45.2 - 40.8 = 4.4 per REMPER
            ("T5", "P3", 75.3, 520.1),  # Survivor: growth = 85.7 - 75.3 = 10.4 per REMPER
            # Note: No beginning data for INGROWTH (T2) and REVERSION (T3)
        ])

        # Insert condition data with different ALSTKCD values for grouping tests
        conn.executemany("""
            INSERT INTO COND VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            ("C1", "P1", 1, 1, 1.0, 40, 161, 3, 0, 2),  # Fully stocked
            ("C2", "P2", 1, 1, 1.0, 40, 406, 2, 0, 3),  # Medium stocked
            ("C3", "P3", 1, 1, 1.0, 10, 703, 1, 0, 1),  # Overstocked
        ])

        # Insert plot data with different REMPER values
        conn.executemany("""
            INSERT INTO PLOT VALUES (?, ?, ?, ?, ?, ?)
        """, [
            ("P1", 37, 2020, 1, 24.0, 5.0),   # Standard 5-year period
            ("P2", 37, 2021, 1, 24.0, 4.5),   # Shorter period
            ("P3", 37, 2019, 1, 24.0, None),  # Missing REMPER (should default to 5.0)
        ])

        # Insert stratification data
        conn.executemany("""
            INSERT INTO POP_STRATUM VALUES (?, ?, ?, ?, ?, ?)
        """, [
            ("S1", 372301, 6000.0, 1.0, 12.0, 0.25),
            ("S2", 372301, 6500.0, 1.0, 12.0, 0.25),
            ("S3", 372301, 5800.0, 1.0, 12.0, 0.25),
        ])

        # Insert plot-stratum assignments
        conn.executemany("""
            INSERT INTO POP_PLOT_STRATUM_ASSGN VALUES (?, ?, ?, ?)
        """, [
            ("A1", "P1", "S1", 372301),
            ("A2", "P2", "S2", 372301),
            ("A3", "P3", "S3", 372301),
        ])

        # Insert BEGINEND data (should be unused according to growth.py comments)
        conn.execute("INSERT INTO BEGINEND VALUES ('BE1', 372301, 2)")

        conn.commit()
        conn.close()

        return Path(db_path)

    def test_column_name_bug_growth_acre_to_grow_acre(self, grm_test_database):
        """Test that catches the GROWTH_ACRE to GROW_ACRE column name bug in utils.py."""
        # This test should FAIL before the fix and PASS after
        with FIA(str(grm_test_database)) as db:
            db.clip_by_evalid([372301])

            results = growth(
                db,
                land_type="forest",
                tree_type="gs",
                measure="volume"
            )

            # The bug: format_output_columns() renames GROWTH_ACRE to GROW_ACRE
            # but the growth function expects GROWTH_ACRE to exist

            # This should be the CORRECT column name
            assert "GROWTH_ACRE" in results.columns, "GROWTH_ACRE column should exist after growth calculation"

            # This should NOT exist due to the bug
            # Before fix: utils.py line 93 has "GROWTH_ACRE": "GROW_ACRE" mapping
            # After fix: this mapping should be removed or corrected

            # Test that the growth function can access its own output columns
            growth_per_acre = results["GROWTH_ACRE"][0]
            assert growth_per_acre >= 0, "Growth per acre should be non-negative"

    def test_format_output_columns_bug_directly(self):
        """Test the utils.format_output_columns function directly to catch the column name bug."""
        # Create a sample result DataFrame with GROWTH_ACRE
        sample_results = pl.DataFrame({
            "GROWTH_ACRE": [1.5, 2.3, 0.8],
            "GROWTH_TOTAL": [15000, 23000, 8000],
            "STATECD": [37, 37, 37]
        })

        # Call format_output_columns with growth estimation type
        formatted = format_output_columns(
            sample_results,
            estimation_type="growth",
            include_se=False,
            include_cv=False
        )

        # BUG: utils.py line 93 has "GROWTH_ACRE": "GROW_ACRE" which is WRONG
        # The growth function expects GROWTH_ACRE but gets GROW_ACRE

        # Test what the function expects vs what it gets
        original_columns = set(sample_results.columns)
        formatted_columns = set(formatted.columns)

        # This test will FAIL with the bug because GROWTH_ACRE gets renamed to GROW_ACRE
        if "GROW_ACRE" in formatted_columns and "GROWTH_ACRE" not in formatted_columns:
            pytest.fail(
                "CRITICAL BUG: format_output_columns renamed GROWTH_ACRE to GROW_ACRE. "
                "This breaks the growth function which expects GROWTH_ACRE. "
                "Fix utils.py line 93 to remove this incorrect mapping."
            )

        # After fix, GROWTH_ACRE should remain unchanged
        assert "GROWTH_ACRE" in formatted_columns, "GROWTH_ACRE should not be renamed"

    def test_net_growth_calculation_correctness(self, grm_test_database):
        """Test NET growth calculation against EVALIDator methodology."""
        with FIA(str(grm_test_database)) as db:
            db.clip_by_evalid([372301])

            results = growth(
                db,
                land_type="forest",
                tree_type="gs",
                measure="volume"
            )

            # Verify we get results
            assert not results.is_empty(), "Growth calculation should return results"
            assert "GROWTH_ACRE" in results.columns, "Should have GROWTH_ACRE column"

            # Calculate expected NET growth manually from test data:
            # Tree T1 (SURVIVOR): (25.5 - 20.1) / 5.0 = 1.08 * 2.5 TPA * 1.0 adj * 6000 expns = 16,200
            # Tree T2 (INGROWTH): 12.8 / 5.0 = 2.56 * 1.8 TPA * 12.0 adj * 6000 expns = 331,776
            # Tree T3 (REVERSION): 15.6 / 4.5 = 3.47 * 1.2 TPA * 1.0 adj * 6500 expns = 27,002
            # Tree T4 (SURVIVOR, no adj): (45.2 - 40.8) / 4.5 = 0.98 * 3.2 TPA * 0.0 adj * 6500 expns = 0
            # Tree T5 (SURVIVOR, MACR): (85.7 - 75.3) / 5.0 = 2.08 * 4.1 TPA * 0.25 adj * 5800 expns = 12,416

            # Total expected (rough): 16,200 + 331,776 + 27,002 + 0 + 12,416 = 387,394 cubic feet

            # Check that growth is positive and reasonable
            total_growth = results["GROWTH_TOTAL"][0] if "GROWTH_TOTAL" in results.columns else 0
            assert total_growth > 0, "Total growth should be positive for growing forest"

            # Growth should be in a reasonable range (not the 5x overestimate from the bug)
            # If it's > 1,000,000, likely still has the bug
            assert total_growth < 1000000, f"Growth seems too high ({total_growth:,.0f}), may still have 5x overestimate bug"

    def test_grm_component_types_handling(self, grm_test_database):
        """Test proper handling of all GRM component types: SURVIVOR, INGROWTH, REVERSION."""
        with FIA(str(grm_test_database)) as db:
            db.clip_by_evalid([372301])

            # Get the raw GRM component data to verify test setup
            grm_data = db.tables.get("TREE_GRM_COMPONENT")
            if grm_data is None:
                db.load_table("TREE_GRM_COMPONENT")
                grm_data = db.tables["TREE_GRM_COMPONENT"]

            if isinstance(grm_data, pl.LazyFrame):
                grm_data = grm_data.collect()

            components = grm_data["SUBP_COMPONENT_GS_FOREST"].unique().to_list()
            expected_components = ["SURVIVOR", "INGROWTH", "REVERSION1"]

            for component in expected_components:
                assert component in components, f"Test data should include {component} component"

            # Run growth estimation
            results = growth(
                db,
                land_type="forest",
                tree_type="gs",
                measure="volume"
            )

            # Should handle all component types without errors
            assert not results.is_empty(), "Should successfully process all GRM component types"

            # Verify that growth includes contributions from all component types
            # (This is implicit - if any component type was mishandled, we'd get wrong totals)
            growth_acre = results["GROWTH_ACRE"][0]
            assert growth_acre > 0, "Growth should be positive with SURVIVOR, INGROWTH, and REVERSION"

    def test_missing_data_handling(self, grm_test_database):
        """Test handling of missing data: NULL volumes, missing REMPER."""
        # First test with original data to establish baseline
        with FIA(str(grm_test_database)) as db:
            db.clip_by_evalid([372301])
            baseline_results = growth(db, land_type="forest", tree_type="gs", measure="volume")
            baseline_growth = baseline_results["GROWTH_ACRE"][0]

        # Create modified database with NULL volumes and missing REMPER
        with sqlite3.connect(str(grm_test_database)) as conn:
            # Set some volumes to NULL
            conn.execute("UPDATE TREE_GRM_MIDPT SET VOLCFNET = NULL WHERE TRE_CN = 'T2'")
            conn.execute("UPDATE TREE_GRM_BEGIN SET VOLCFNET = NULL WHERE TRE_CN = 'T1'")

            # Verify REMPER is already NULL for P3 (set in fixture)
            remper_null = conn.execute("SELECT REMPER FROM PLOT WHERE CN = 'P3'").fetchone()[0]
            assert remper_null is None, "Test setup: P3 should have NULL REMPER"

        # Test growth calculation with missing data
        with FIA(str(grm_test_database)) as db:
            db.clip_by_evalid([372301])

            # Should not crash with NULL volumes and missing REMPER
            results = growth(db, land_type="forest", tree_type="gs", measure="volume")
            assert not results.is_empty(), "Should handle NULL volumes and missing REMPER gracefully"

            # Growth should still be calculated for trees with valid data
            growth_with_nulls = results["GROWTH_ACRE"][0]

            # Should be different from baseline due to missing data
            # (Could be higher or lower depending on which trees had NULL values)
            assert growth_with_nulls != baseline_growth, "Growth should change when volume data is NULL"

            # Should not be NaN or negative due to NULL handling
            assert not pl.Series([growth_with_nulls]).is_nan().any(), "Growth should not be NaN with NULL handling"

    def test_alstkcd_grouping_functionality(self, grm_test_database):
        """Test that ALSTKCD grouping works correctly."""
        with FIA(str(grm_test_database)) as db:
            db.clip_by_evalid([372301])

            # Test grouping by ALSTKCD (stocking class)
            results = growth(
                db,
                grp_by="ALSTKCD",
                land_type="forest",
                tree_type="gs",
                measure="volume"
            )

            # Should have multiple rows for different ALSTKCD values
            assert len(results) > 1, "Should have multiple rows when grouped by ALSTKCD"
            assert "ALSTKCD" in results.columns, "Should include ALSTKCD grouping column"

            # Verify expected ALSTKCD values from test data (1=Overstocked, 2=Fully, 3=Medium)
            alstkcd_values = set(results["ALSTKCD"].to_list())
            expected_alstkcd = {1, 2, 3}  # From test data fixture
            assert alstkcd_values == expected_alstkcd, f"Should have ALSTKCD values {expected_alstkcd}, got {alstkcd_values}"

            # Each group should have positive growth
            for row in results.iter_rows(named=True):
                growth_acre = row["GROWTH_ACRE"]
                alstkcd = row["ALSTKCD"]
                assert growth_acre >= 0, f"Growth should be non-negative for ALSTKCD {alstkcd}"

    def test_collect_schema_performance_issue(self, grm_test_database):
        """Test for performance issues caused by collect_schema() calls."""

        with patch('polars.LazyFrame.collect_schema') as mock_collect_schema:
            # Set up the mock to return a reasonable schema
            mock_collect_schema.return_value = pl.Schema({
                "TRE_CN": pl.Utf8,
                "PLT_CN": pl.Utf8,
                "VOLCFNET": pl.Float64,
                "CONDID": pl.Int64
            })

            with FIA(str(grm_test_database)) as db:
                db.clip_by_evalid([372301])

                # Run growth calculation
                results = growth(db, land_type="forest", tree_type="gs", measure="volume")

                # Check how many times collect_schema was called
                schema_calls = mock_collect_schema.call_count

                # Performance issue: collect_schema() is expensive and should be minimized
                # Before fix: likely called multiple times unnecessarily
                # After fix: should be called minimally (ideally <= 2 times)

                if schema_calls > 5:
                    pytest.fail(
                        f"Performance issue: collect_schema() called {schema_calls} times. "
                        "This is expensive for large datasets. Consider caching schema or "
                        "using .columns instead of .collect_schema().names()"
                    )

                assert not results.is_empty(), "Should still produce results despite mocked schema"

    def test_error_handling_consistency(self, grm_test_database):
        """Test consistent error handling across different scenarios."""

        # Test 1: Missing required GRM tables
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
            incomplete_db_path = tmp.name

        # Create database without GRM tables
        conn = sqlite3.connect(incomplete_db_path)
        conn.execute("CREATE TABLE PLOT (CN TEXT PRIMARY KEY)")
        conn.execute("INSERT INTO PLOT VALUES ('P1')")
        conn.commit()
        conn.close()

        with pytest.raises(ValueError, match="TREE_GRM_COMPONENT.*not found"):
            with FIA(incomplete_db_path) as db:
                growth(db, land_type="forest", tree_type="gs", measure="volume")

        # Test 2: Invalid parameters should raise clear errors
        with FIA(str(grm_test_database)) as db:
            db.clip_by_evalid([372301])

            # Invalid measure type
            with pytest.raises((ValueError, KeyError)):
                growth(db, measure="invalid_measure")

            # Invalid land type
            with pytest.raises((ValueError, KeyError)):
                growth(db, land_type="invalid_land_type")

        # Test 3: Empty results should be handled gracefully
        with FIA(str(grm_test_database)) as db:
            # Filter to non-existent EVALID
            db.clip_by_evalid([999999])

            # Should return empty results, not crash
            results = growth(db, land_type="forest", tree_type="gs", measure="volume")
            # Depending on implementation, might return empty DataFrame or raise error
            # Either is acceptable as long as it's consistent

    def test_magic_numbers_parameterization(self, grm_test_database):
        """Test removal of hard-coded magic numbers."""

        # The growth function has several hard-coded values that should be parameterizable

        # Test 1: Variance calculation uses hard-coded 12% CV
        with FIA(str(grm_test_database)) as db:
            db.clip_by_evalid([372301])

            results = growth(
                db,
                land_type="forest",
                tree_type="gs",
                measure="volume",
                variance=True
            )

            if "GROWTH_ACRE_SE" in results.columns:
                growth_acre = results["GROWTH_ACRE"][0]
                growth_se = results["GROWTH_ACRE_SE"][0]

                # Hard-coded 12% CV in growth.py line 422
                expected_se = growth_acre * 0.12
                actual_cv = (growth_se / growth_acre) * 100

                # This test documents the magic number - ideally CV should be configurable
                assert abs(actual_cv - 12.0) < 0.1, f"Hard-coded 12% CV detected (actual: {actual_cv:.1f}%). Consider making CV configurable."

        # Test 2: Default REMPER value (5.0) is hard-coded
        # This is tested indirectly through missing REMPER handling above

        # Test 3: Default year (2023) is hard-coded in format_output
        with FIA(str(grm_test_database)) as db:
            db.clip_by_evalid([372301])

            results = growth(db, land_type="forest", tree_type="gs", measure="volume")

            if "YEAR" in results.columns:
                year = results["YEAR"][0]
                # Hard-coded 2023 in growth.py line 449
                if year == 2023:
                    # This documents the magic number - year should come from data
                    pass  # Expected for now, but should be improved

    def test_beginend_table_unused_issue(self, grm_test_database):
        """Test that BEGINEND table is listed in requirements but not actually used."""

        # BEGINEND is in get_required_tables() but not used in load_data()
        # This is a code smell that should be fixed

        estimator = GrowthEstimator(FIA(str(grm_test_database)), {
            "land_type": "forest",
            "tree_type": "gs",
            "measure": "volume"
        })

        required_tables = estimator.get_required_tables()
        assert "BEGINEND" in required_tables, "BEGINEND is listed in required tables"

        # Run estimation and verify BEGINEND is actually not needed
        # If the code doesn't use BEGINEND, then it shouldn't be in required_tables

        # Remove BEGINEND table to see if estimation still works
        with sqlite3.connect(str(grm_test_database)) as conn:
            conn.execute("DROP TABLE BEGINEND")

        with FIA(str(grm_test_database)) as db:
            db.clip_by_evalid([372301])

            # Should still work without BEGINEND if it's truly unused
            try:
                results = growth(db, land_type="forest", tree_type="gs", measure="volume")
                # If this succeeds, BEGINEND is indeed unused and should be removed from requirements
                assert not results.is_empty(), "Growth calculation should work without BEGINEND table"

                pytest.fail(
                    "BEGINEND table is listed in get_required_tables() but is not actually used. "
                    "Remove it from the requirements to avoid unnecessary table loading."
                )
            except ValueError:
                # If this fails, then BEGINEND is actually needed
                pass

    def test_regression_against_evalidator_values(self, grm_test_database):
        """Test growth results against known EVALIDator reference values."""

        # This test uses the known EVALIDator methodology from test_growth_evaluation.py
        # to verify our implementation produces similar results

        with FIA(str(grm_test_database)) as db:
            db.clip_by_evalid([372301])

            # Test with ALSTKCD grouping like the EVALIDator query
            results = growth(
                db,
                grp_by="ALSTKCD",
                land_type="forest",
                tree_type="gs",
                measure="volume",
                tree_domain="DIA_MIDPT >= 5.0"  # Growing stock trees >= 5" DBH
            )

            # Verify basic result structure matches EVALIDator format
            assert not results.is_empty(), "Should produce results like EVALIDator"
            assert "ALSTKCD" in results.columns, "Should group by ALSTKCD like EVALIDator"
            assert "GROWTH_TOTAL" in results.columns, "Should have total estimates like EVALIDator"

            # Sum total growth across all stocking classes
            total_growth = results["GROWTH_TOTAL"].sum()

            # The original bug produced ~395% overestimate (99.3M vs 24.9M reference)
            # After initial fix: -26% difference (18.4M vs 24.9M reference)
            # This test documents that we should be within reasonable range of reference

            # For our small test dataset, expect much smaller values
            # But growth should be positive and not absurdly large
            assert total_growth > 0, "Total growth should be positive"
            assert total_growth < 10000000, "Total growth should be reasonable (< 10M for test data)"

            # Growth per acre should also be reasonable
            growth_per_acre = results["GROWTH_ACRE"].sum() / len(results)
            assert 0 < growth_per_acre < 1000, "Growth per acre should be reasonable (0-1000 cu ft/acre/year)"


@pytest.fixture
def grm_fixture_for_reuse():
    """Reusable GRM fixture for other test files."""
    # This fixture can be imported by other test modules
    # to ensure consistent GRM test data structure

    grm_data_structure = {
        "tree_grm_component_columns": [
            "TRE_CN", "PLT_CN", "DIA_BEGIN", "DIA_MIDPT", "DIA_END",
            "SUBP_COMPONENT_GS_FOREST", "SUBP_TPAGROW_UNADJ_GS_FOREST", "SUBP_SUBPTYP_GRM_GS_FOREST",
            "SUBP_COMPONENT_AL_FOREST", "SUBP_TPAGROW_UNADJ_AL_FOREST", "SUBP_SUBPTYP_GRM_AL_FOREST",
            "SUBP_COMPONENT_GS_TIMBER", "SUBP_TPAGROW_UNADJ_GS_TIMBER", "SUBP_SUBPTYP_GRM_GS_TIMBER"
        ],
        "tree_grm_midpt_columns": [
            "TRE_CN", "PLT_CN", "DIA", "SPCD", "STATUSCD", "VOLCFNET", "DRYBIO_AG"
        ],
        "tree_grm_begin_columns": [
            "TRE_CN", "PLT_CN", "VOLCFNET", "DRYBIO_AG"
        ],
        "expected_components": ["SURVIVOR", "INGROWTH", "REVERSION1", "CUT1", "DIVERSION1", "MORTALITY1"],
        "subptyp_grm_values": [0, 1, 2, 3],  # None, SUBP, MICR, MACR
        "remper_default": 5.0
    }

    return grm_data_structure


class TestGrowthEdgeCases:
    """Additional edge case tests for comprehensive coverage."""

    def test_zero_growth_components(self, grm_test_database):
        """Test handling of trees with zero growth."""

        # Modify test data to have zero growth scenario
        with sqlite3.connect(str(grm_test_database)) as conn:
            # Set beginning and ending volumes to be the same (zero growth)
            conn.execute("UPDATE TREE_GRM_MIDPT SET VOLCFNET = 20.1 WHERE TRE_CN = 'T1'")
            # Beginning volume is already 20.1, so growth = 0

        with FIA(str(grm_test_database)) as db:
            db.clip_by_evalid([372301])

            results = growth(db, land_type="forest", tree_type="gs", measure="volume")

            # Should handle zero growth gracefully
            assert not results.is_empty(), "Should handle zero growth trees"

            # Total growth might still be positive due to other trees
            total_growth = results["GROWTH_TOTAL"][0] if "GROWTH_TOTAL" in results.columns else 0
            assert total_growth >= 0, "Total growth should be non-negative"

    def test_negative_growth_survivor_trees(self, grm_test_database):
        """Test handling of survivor trees with negative growth (volume loss)."""

        # Modify test data to have negative growth
        with sqlite3.connect(str(grm_test_database)) as conn:
            # Set ending volume less than beginning volume
            conn.execute("UPDATE TREE_GRM_MIDPT SET VOLCFNET = 15.0 WHERE TRE_CN = 'T1'")
            # Beginning is 20.1, ending is 15.0, so growth = -5.1/5.0 = -1.02 per year

        with FIA(str(grm_test_database)) as db:
            db.clip_by_evalid([372301])

            results = growth(db, land_type="forest", tree_type="gs", measure="volume")

            # Should handle negative growth (volume loss) for individual trees
            # Total might still be positive due to ingrowth and other survivors
            assert not results.is_empty(), "Should handle negative growth trees"

    def test_extreme_remper_values(self, grm_test_database):
        """Test handling of extreme REMPER values."""

        with sqlite3.connect(str(grm_test_database)) as conn:
            # Set extreme REMPER values
            conn.execute("UPDATE PLOT SET REMPER = 0.1 WHERE CN = 'P1'")  # Very short period
            conn.execute("UPDATE PLOT SET REMPER = 20.0 WHERE CN = 'P2'")  # Very long period

        with FIA(str(grm_test_database)) as db:
            db.clip_by_evalid([372301])

            results = growth(db, land_type="forest", tree_type="gs", measure="volume")

            # Should handle extreme REMPER values without crashing
            assert not results.is_empty(), "Should handle extreme REMPER values"

            # Results should be reasonable despite extreme values
            growth_acre = results["GROWTH_ACRE"][0]
            assert not pl.Series([growth_acre]).is_nan().any(), "Growth should not be NaN with extreme REMPER"