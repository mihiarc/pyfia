"""
Tests for growth calculation methodology against EVALIDator reference.

This module tests the growth calculation against the EVALIDator SQL methodology
shown in test_growth_evaluation.py. It focuses on ensuring the NET growth
calculation follows the correct FIA methodology.

Critical aspects tested:
1. NET growth = (Ending - Beginning) / REMPER for SURVIVOR
2. NET growth = Ending / REMPER for INGROWTH and REVERSION
3. Component-based logic matching EVALIDator query
4. SUBPTYP_GRM adjustment factor application
5. Volume change calculations by component type
"""

import sqlite3
import tempfile
from pathlib import Path

import polars as pl
import pytest

from pyfia import FIA
from pyfia.estimation.estimators.growth import growth


class TestGrowthEVALIDatorMethodology:
    """Test growth calculations against EVALIDator reference methodology."""

    @pytest.fixture
    def evalidator_test_database(self):
        """Create test database matching EVALIDator query structure."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
            db_path = tmp.name

        conn = sqlite3.connect(db_path)

        # Create tables matching EVALIDator query expectations
        # This reproduces the complex EVALIDator SQL from test_growth_evaluation.py

        # TREE_GRM_COMPONENT with specific test data
        conn.execute("""
            CREATE TABLE TREE_GRM_COMPONENT (
                TRE_CN TEXT PRIMARY KEY,
                PLT_CN TEXT,
                DIA_BEGIN REAL,
                DIA_MIDPT REAL,
                DIA_END REAL,
                SUBP_COMPONENT_GS_FOREST TEXT,
                SUBP_TPAGROW_UNADJ_GS_FOREST REAL,
                SUBP_SUBPTYP_GRM_GS_FOREST INTEGER
            )
        """)

        # TREE_GRM_MIDPT - corresponds to TREE in EVALIDator query
        conn.execute("""
            CREATE TABLE TREE_GRM_MIDPT (
                TRE_CN TEXT PRIMARY KEY,
                PLT_CN TEXT,
                DIA REAL,
                SPCD INTEGER,
                STATUSCD INTEGER,
                VOLCFNET REAL
            )
        """)

        # TREE_GRM_BEGIN - corresponds to TRE_BEGIN in EVALIDator
        conn.execute("""
            CREATE TABLE TREE_GRM_BEGIN (
                TRE_CN TEXT PRIMARY KEY,
                PLT_CN TEXT,
                VOLCFNET REAL
            )
        """)

        # BEGINEND table (EVALIDator uses this for ONEORTWO logic)
        conn.execute("""
            CREATE TABLE BEGINEND (
                CN TEXT PRIMARY KEY,
                EVALID INTEGER,
                ONEORTWO INTEGER
            )
        """)

        # Other required tables
        conn.execute("""
            CREATE TABLE COND (
                CN TEXT PRIMARY KEY,
                PLT_CN TEXT,
                CONDID INTEGER,
                COND_STATUS_CD INTEGER,
                CONDPROP_UNADJ REAL,
                ALSTKCD INTEGER
            )
        """)

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

        conn.execute("""
            CREATE TABLE POP_PLOT_STRATUM_ASSGN (
                CN TEXT PRIMARY KEY,
                PLT_CN TEXT,
                STRATUM_CN TEXT,
                EVALID INTEGER
            )
        """)

        # Insert test data that matches EVALIDator expectations
        # This data is designed to test specific growth calculation scenarios

        # Test case 1: SURVIVOR tree with clear volume growth
        # EVALIDator: (ending - beginning) / REMPER for SURVIVOR
        conn.execute("""
            INSERT INTO TREE_GRM_COMPONENT VALUES
            ('SURV1', 'P1', 10.0, 11.0, 12.0, 'SURVIVOR', 2.5, 1)
        """)
        conn.execute("""
            INSERT INTO TREE_GRM_MIDPT VALUES
            ('SURV1', 'P1', 11.0, 131, 1, 25.0)
        """)
        conn.execute("""
            INSERT INTO TREE_GRM_BEGIN VALUES
            ('SURV1', 'P1', 20.0)
        """)

        # Test case 2: INGROWTH tree (no beginning volume)
        # EVALIDator: ending / REMPER for INGROWTH
        conn.execute("""
            INSERT INTO TREE_GRM_COMPONENT VALUES
            ('INGR1', 'P1', NULL, 5.5, 6.0, 'INGROWTH', 1.8, 2)
        """)
        conn.execute("""
            INSERT INTO TREE_GRM_MIDPT VALUES
            ('INGR1', 'P1', 5.5, 110, 1, 12.0)
        """)

        # Test case 3: REVERSION tree
        # EVALIDator: ending / REMPER for REVERSION
        conn.execute("""
            INSERT INTO TREE_GRM_COMPONENT VALUES
            ('REVR1', 'P2', NULL, 7.2, 8.0, 'REVERSION1', 1.2, 1)
        """)
        conn.execute("""
            INSERT INTO TREE_GRM_MIDPT VALUES
            ('REVR1', 'P2', 7.2, 316, 1, 15.0)
        """)

        # Test case 4: SURVIVOR with zero adjustment (SUBPTYP_GRM = 0)
        conn.execute("""
            INSERT INTO TREE_GRM_COMPONENT VALUES
            ('SURV2', 'P2', 15.0, 15.5, 16.0, 'SURVIVOR', 3.2, 0)
        """)
        conn.execute("""
            INSERT INTO TREE_GRM_MIDPT VALUES
            ('SURV2', 'P2', 15.5, 131, 1, 45.0)
        """)
        conn.execute("""
            INSERT INTO TREE_GRM_BEGIN VALUES
            ('SURV2', 'P2', 40.0)
        """)

        # Condition data with ALSTKCD for grouping (matches EVALIDator query)
        conn.executemany("""
            INSERT INTO COND VALUES (?, ?, ?, ?, ?, ?)
        """, [
            ("C1", "P1", 1, 1, 1.0, 2),  # Fully stocked
            ("C2", "P2", 1, 1, 1.0, 3),  # Medium stocked
        ])

        # Plot data with REMPER
        conn.executemany("""
            INSERT INTO PLOT VALUES (?, ?, ?, ?, ?, ?)
        """, [
            ("P1", 13, 2020, 1, 24.0, 5.0),  # Georgia, 5-year period
            ("P2", 13, 2021, 1, 24.0, 5.0),  # Georgia, 5-year period
        ])

        # Stratification data (matches EVALIDator EVALID)
        conn.executemany("""
            INSERT INTO POP_STRATUM VALUES (?, ?, ?, ?, ?, ?)
        """, [
            ("S1", 132303, 6000.0, 1.0, 12.0, 0.25),
            ("S2", 132303, 6500.0, 1.0, 12.0, 0.25),
        ])

        # Plot assignments
        conn.executemany("""
            INSERT INTO POP_PLOT_STRATUM_ASSGN VALUES (?, ?, ?, ?)
        """, [
            ("A1", "P1", "S1", 132303),
            ("A2", "P2", "S2", 132303),
        ])

        # BEGINEND data (EVALIDator uses ONEORTWO = 2 for ending volume approach)
        conn.execute("INSERT INTO BEGINEND VALUES ('BE1', 132303, 2)")

        conn.commit()
        conn.close()

        return Path(db_path)

    def test_net_growth_calculation_survivor_trees(self, evalidator_test_database):
        """
        Test NET growth calculation for SURVIVOR trees: (Ending - Beginning) / REMPER

        This matches EVALIDator methodology for SURVIVOR components.
        """
        with FIA(str(evalidator_test_database)) as db:
            db.clip_by_evalid([132303])

            # Get raw data to verify our test setup
            grm_component = db.tables.get("TREE_GRM_COMPONENT")
            if grm_component is None:
                db.load_table("TREE_GRM_COMPONENT")
                grm_component = db.tables["TREE_GRM_COMPONENT"]

            if isinstance(grm_component, pl.LazyFrame):
                grm_component = grm_component.collect()

            # Verify we have SURVIVOR trees in test data
            survivor_trees = grm_component.filter(pl.col("SUBP_COMPONENT_GS_FOREST") == "SURVIVOR")
            assert len(survivor_trees) >= 1, "Should have SURVIVOR trees in test data"

            # Run growth calculation
            results = growth(
                db,
                land_type="forest",
                tree_type="gs",
                measure="volume"
            )

            assert not results.is_empty(), "Should produce growth results for SURVIVOR trees"

            # For our test data:
            # SURV1: (25.0 - 20.0) / 5.0 = 1.0 annual growth per tree
            #        1.0 * 2.5 TPA * 1.0 adj * 6000 expns = 15,000 cubic feet total
            # SURV2: (45.0 - 40.0) / 5.0 = 1.0 annual growth per tree
            #        1.0 * 3.2 TPA * 0.0 adj * 6500 expns = 0 (no adjustment)

            # Expected total: ~15,000 cubic feet (only SURV1 contributes)
            total_growth = results["GROWTH_TOTAL"].sum() if "GROWTH_TOTAL" in results.columns else 0

            # Should be positive and in reasonable range
            assert total_growth > 0, "SURVIVOR trees should produce positive growth"
            assert 10000 <= total_growth <= 20000, f"SURVIVOR growth should be ~15,000, got {total_growth:,.0f}"

    def test_net_growth_calculation_ingrowth_trees(self, evalidator_test_database):
        """
        Test NET growth calculation for INGROWTH trees: Ending / REMPER

        INGROWTH trees have no beginning volume, so growth = ending volume / REMPER
        """
        with FIA(str(evalidator_test_database)) as db:
            db.clip_by_evalid([132303])

            # Verify test setup has INGROWTH trees
            db.load_table("TREE_GRM_COMPONENT")
            grm_component = db.tables["TREE_GRM_COMPONENT"]
            if isinstance(grm_component, pl.LazyFrame):
                grm_component = grm_component.collect()

            ingrowth_trees = grm_component.filter(pl.col("SUBP_COMPONENT_GS_FOREST") == "INGROWTH")
            assert len(ingrowth_trees) >= 1, "Should have INGROWTH trees in test data"

            # For INGROWTH, beginning volume should be NULL or 0
            db.load_table("TREE_GRM_BEGIN")
            begin_data = db.tables["TREE_GRM_BEGIN"]
            if isinstance(begin_data, pl.LazyFrame):
                begin_data = begin_data.collect()

            ingrowth_begin = begin_data.filter(pl.col("TRE_CN") == "INGR1")
            assert len(ingrowth_begin) == 0, "INGROWTH trees should not have beginning volume data"

            # Run growth calculation
            results = growth(db, land_type="forest", tree_type="gs", measure="volume")

            # For our test data (INGR1):
            # INGROWTH: 12.0 / 5.0 = 2.4 annual growth per tree
            #           2.4 * 1.8 TPA * 12.0 adj * 6000 expns = 311,040 cubic feet

            total_growth = results["GROWTH_TOTAL"].sum() if "GROWTH_TOTAL" in results.columns else 0
            assert total_growth > 100000, "INGROWTH should contribute significant growth due to high microplot adjustment"

    def test_net_growth_calculation_reversion_trees(self, evalidator_test_database):
        """
        Test NET growth calculation for REVERSION trees: Ending / REMPER

        REVERSION trees (like INGROWTH) have no beginning volume.
        """
        with FIA(str(evalidator_test_database)) as db:
            db.clip_by_evalid([132303])

            # Verify REVERSION trees in test data
            db.load_table("TREE_GRM_COMPONENT")
            grm_component = db.tables["TREE_GRM_COMPONENT"]
            if isinstance(grm_component, pl.LazyFrame):
                grm_component = grm_component.collect()

            reversion_trees = grm_component.filter(pl.col("SUBP_COMPONENT_GS_FOREST").str.starts_with("REVERSION"))
            assert len(reversion_trees) >= 1, "Should have REVERSION trees in test data"

            results = growth(db, land_type="forest", tree_type="gs", measure="volume")

            # For our test data (REVR1):
            # REVERSION1: 15.0 / 5.0 = 3.0 annual growth per tree
            #             3.0 * 1.2 TPA * 1.0 adj * 6500 expns = 23,400 cubic feet

            total_growth = results["GROWTH_TOTAL"].sum() if "GROWTH_TOTAL" in results.columns else 0
            assert total_growth > 0, "REVERSION trees should contribute positive growth"

    def test_component_filtering_growth_only(self, evalidator_test_database):
        """
        Test that only growth components are included, not mortality or removals.

        Growth calculation should filter to: SURVIVOR, INGROWTH, REVERSION*
        Should exclude: MORTALITY*, CUT*, DIVERSION*
        """
        # Add mortality and removal components to test filtering
        conn = sqlite3.connect(str(evalidator_test_database))

        # Add trees that should be excluded from growth
        conn.execute("""
            INSERT INTO TREE_GRM_COMPONENT VALUES
            ('MORT1', 'P1', 12.0, 12.0, 12.0, 'MORTALITY1', 1.5, 1)
        """)
        conn.execute("""
            INSERT INTO TREE_GRM_COMPONENT VALUES
            ('CUT1', 'P2', 14.0, 14.0, 14.0, 'CUT1', 2.0, 1)
        """)
        conn.execute("""
            INSERT INTO TREE_GRM_MIDPT VALUES
            ('MORT1', 'P1', 12.0, 131, 2, 30.0)
        """)
        conn.execute("""
            INSERT INTO TREE_GRM_MIDPT VALUES
            ('CUT1', 'P2', 14.0, 110, 1, 35.0)
        """)

        conn.commit()
        conn.close()

        with FIA(str(evalidator_test_database)) as db:
            db.clip_by_evalid([132303])

            # Load component data to verify our test setup
            db.load_table("TREE_GRM_COMPONENT")
            grm_component = db.tables["TREE_GRM_COMPONENT"]
            if isinstance(grm_component, pl.LazyFrame):
                grm_component = grm_component.collect()

            all_components = grm_component["SUBP_COMPONENT_GS_FOREST"].unique().to_list()

            # Should have growth and non-growth components
            assert "SURVIVOR" in all_components, "Test setup should have SURVIVOR"
            assert "INGROWTH" in all_components, "Test setup should have INGROWTH"
            assert "MORTALITY1" in all_components, "Test setup should have MORTALITY1"
            assert "CUT1" in all_components, "Test setup should have CUT1"

            # Run growth calculation
            results = growth(db, land_type="forest", tree_type="gs", measure="volume")

            # Growth calculation should only include growth components
            # The mortality and cut trees should not contribute to growth totals

            # If filtering is working correctly, total growth should match our previous calculations
            # (only from SURVIVOR, INGROWTH, REVERSION trees)
            total_growth = results["GROWTH_TOTAL"].sum() if "GROWTH_TOTAL" in results.columns else 0
            assert total_growth > 0, "Should have positive growth from growth components only"

            # If mortality/cut trees were incorrectly included, totals would be much higher
            # This is an indirect test but helps verify component filtering

    def test_subptyp_grm_adjustment_factors(self, evalidator_test_database):
        """
        Test SUBPTYP_GRM adjustment factor application matches EVALIDator.

        EVALIDator logic:
        - SUBPTYP_GRM = 0: adjustment = 0 (not sampled)
        - SUBPTYP_GRM = 1: adjustment = ADJ_FACTOR_SUBP
        - SUBPTYP_GRM = 2: adjustment = ADJ_FACTOR_MICR
        - SUBPTYP_GRM = 3: adjustment = ADJ_FACTOR_MACR
        """
        with FIA(str(evalidator_test_database)) as db:
            db.clip_by_evalid([132303])

            # Verify our test data has different SUBPTYP_GRM values
            db.load_table("TREE_GRM_COMPONENT")
            grm_component = db.tables["TREE_GRM_COMPONENT"]
            if isinstance(grm_component, pl.LazyFrame):
                grm_component = grm_component.collect()

            subptyp_values = set(grm_component["SUBP_SUBPTYP_GRM_GS_FOREST"].to_list())
            expected_subptyp = {0, 1, 2}  # From our test data
            assert subptyp_values >= expected_subptyp, f"Should have various SUBPTYP_GRM values: {subptyp_values}"

            # Get stratification data to verify adjustment factors
            db.load_table("POP_STRATUM")
            stratum_data = db.tables["POP_STRATUM"]
            if isinstance(stratum_data, pl.LazyFrame):
                stratum_data = stratum_data.collect()

            # Our test data has:
            # ADJ_FACTOR_SUBP = 1.0, ADJ_FACTOR_MICR = 12.0, ADJ_FACTOR_MACR = 0.25

            results = growth(db, land_type="forest", tree_type="gs", measure="volume")
            total_growth = results["GROWTH_TOTAL"].sum() if "GROWTH_TOTAL" in results.columns else 0

            # Expected contributions:
            # SURV1: SUBPTYP_GRM=1 -> ADJ_FACTOR_SUBP=1.0 -> contributes normally
            # INGR1: SUBPTYP_GRM=2 -> ADJ_FACTOR_MICR=12.0 -> contributes 12x (microplot)
            # REVR1: SUBPTYP_GRM=1 -> ADJ_FACTOR_SUBP=1.0 -> contributes normally
            # SURV2: SUBPTYP_GRM=0 -> no adjustment -> contributes 0

            # The MICR adjustment should make INGR1 the dominant contributor
            assert total_growth > 100000, "High total growth indicates microplot adjustment is working"

    def test_evalidator_oneortwo_handling(self, evalidator_test_database):
        """
        Test handling of BEGINEND.ONEORTWO logic (if implemented).

        EVALIDator uses BEGINEND.ONEORTWO to decide between beginning vs ending volume approach.
        Our implementation may use a simplified approach.
        """
        with FIA(str(evalidator_test_database)) as db:
            db.clip_by_evalid([132303])

            # Check if BEGINEND table is actually used
            # (According to growth.py comments, it's listed but not used)

            # Verify BEGINEND exists in our test data
            db.load_table("BEGINEND")
            beginend_data = db.tables["BEGINEND"]
            if isinstance(beginend_data, pl.LazyFrame):
                beginend_data = beginend_data.collect()

            assert len(beginend_data) > 0, "BEGINEND table should have data"
            oneortwo_value = beginend_data["ONEORTWO"][0]
            assert oneortwo_value == 2, "Test data should have ONEORTWO = 2 (ending volume approach)"

            # Run growth calculation
            results = growth(db, land_type="forest", tree_type="gs", measure="volume")

            # The current implementation should work regardless of BEGINEND.ONEORTWO
            # because it uses a simplified NET growth approach
            assert not results.is_empty(), "Growth calculation should work with ONEORTWO = 2"

    def test_remper_division_accuracy(self, evalidator_test_database):
        """
        Test that annual growth is correctly calculated by dividing by REMPER.

        All volume changes should be divided by REMPER to get annual rates.
        """
        with FIA(str(evalidator_test_database)) as db:
            db.clip_by_evalid([132303])

            # Our test data uses REMPER = 5.0 years
            # So volume changes should be divided by 5.0 to get annual growth

            results = growth(db, land_type="forest", tree_type="gs", measure="volume")

            # Check if we can isolate individual tree contributions
            # (This is difficult without access to intermediate calculations)

            # Instead, verify the overall magnitude is reasonable for annual growth
            growth_per_acre = results["GROWTH_ACRE"].sum() / len(results) if len(results) > 0 else 0

            # For healthy forest, expect annual growth of 10-200 cu ft/acre/year
            # Our calculations should be in this range
            assert 1 <= growth_per_acre <= 1000, f"Annual growth per acre should be reasonable: {growth_per_acre:.1f} cu ft/acre/year"

    def test_alstkcd_grouping_matches_evalidator(self, evalidator_test_database):
        """
        Test ALSTKCD grouping matches EVALIDator query structure.

        EVALIDator groups by ALSTKCD with specific labels.
        """
        with FIA(str(evalidator_test_database)) as db:
            db.clip_by_evalid([132303])

            # Group by ALSTKCD like the EVALIDator query
            results = growth(
                db,
                grp_by="ALSTKCD",
                land_type="forest",
                tree_type="gs",
                measure="volume"
            )

            # Should have multiple rows for different stocking classes
            assert len(results) >= 2, "Should have multiple ALSTKCD groups"
            assert "ALSTKCD" in results.columns, "Should include ALSTKCD grouping column"

            # Our test data has ALSTKCD values 2 and 3
            alstkcd_values = set(results["ALSTKCD"].to_list())
            expected_alstkcd = {2, 3}  # From test data
            assert alstkcd_values == expected_alstkcd, f"Should have ALSTKCD {expected_alstkcd}, got {alstkcd_values}"

            # Each group should have reasonable growth values
            for row in results.iter_rows(named=True):
                alstkcd = row["ALSTKCD"]
                growth_acre = row["GROWTH_ACRE"]
                growth_total = row.get("GROWTH_TOTAL", 0)

                assert growth_acre >= 0, f"Growth per acre should be non-negative for ALSTKCD {alstkcd}"
                assert growth_total >= 0, f"Total growth should be non-negative for ALSTKCD {alstkcd}"

            # Sum across groups should match overall total
            total_by_groups = results["GROWTH_TOTAL"].sum() if "GROWTH_TOTAL" in results.columns else 0

            # Compare with ungrouped calculation
            ungrouped_results = growth(db, land_type="forest", tree_type="gs", measure="volume")
            total_ungrouped = ungrouped_results["GROWTH_TOTAL"][0] if "GROWTH_TOTAL" in ungrouped_results.columns else 0

            # Should be approximately equal (within rounding)
            if total_ungrouped > 0:
                relative_diff = abs(total_by_groups - total_ungrouped) / total_ungrouped
                assert relative_diff < 0.01, f"Grouped vs ungrouped totals should match: {total_by_groups:,.0f} vs {total_ungrouped:,.0f}"

    def test_growth_methodology_regression_check(self, evalidator_test_database):
        """
        Regression test to ensure growth methodology doesn't change unexpectedly.

        This test documents the expected behavior and will catch regressions.
        """
        with FIA(str(evalidator_test_database)) as db:
            db.clip_by_evalid([132303])

            results = growth(db, land_type="forest", tree_type="gs", measure="volume")

            # Document expected structure
            expected_columns = {"GROWTH_ACRE", "GROWTH_TOTAL", "AREA_TOTAL", "N_PLOTS"}
            actual_columns = set(results.columns)

            missing_columns = expected_columns - actual_columns
            if missing_columns:
                pytest.fail(f"Missing expected columns: {missing_columns}. This indicates a regression in output format.")

            # Document expected value ranges based on our test data
            if not results.is_empty():
                growth_acre = results["GROWTH_ACRE"][0]
                growth_total = results["GROWTH_TOTAL"][0] if "GROWTH_TOTAL" in results.columns else 0

                # These ranges are based on our specific test data calculations
                # If the methodology changes, these assertions will fail and need updating
                assert 0 < growth_acre < 1000, f"Growth per acre regression: expected 0-1000, got {growth_acre}"
                assert 0 < growth_total < 1000000, f"Total growth regression: expected 0-1M, got {growth_total}"

                # The previous bug caused ~5x overestimate, so this catches that regression
                if growth_total > 500000:
                    pytest.fail(f"Possible 5x overestimate regression: total growth = {growth_total:,.0f}")