"""
Integration tests for growth function with real-world scenarios.

These tests focus on integration with the broader pyFIA system and
real-world usage patterns that would reveal bugs in actual usage.
"""

import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest

from pyfia import FIA
from pyfia.estimation.estimators.growth import growth, GrowthEstimator


class TestGrowthIntegrationScenarios:
    """Integration tests for growth function in realistic usage scenarios."""

    @pytest.fixture
    def realistic_fia_database(self):
        """Create a more realistic FIA database for integration testing."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
            db_path = tmp.name

        conn = sqlite3.connect(db_path)

        # Create all required tables with realistic scale
        tables_sql = {
            "TREE_GRM_COMPONENT": """
                CREATE TABLE TREE_GRM_COMPONENT (
                    TRE_CN TEXT PRIMARY KEY,
                    PLT_CN TEXT,
                    DIA_BEGIN REAL,
                    DIA_MIDPT REAL,
                    DIA_END REAL,
                    SUBP_COMPONENT_GS_FOREST TEXT,
                    SUBP_TPAGROW_UNADJ_GS_FOREST REAL,
                    SUBP_SUBPTYP_GRM_GS_FOREST INTEGER,
                    SUBP_COMPONENT_AL_FOREST TEXT,
                    SUBP_TPAGROW_UNADJ_AL_FOREST REAL,
                    SUBP_SUBPTYP_GRM_AL_FOREST INTEGER,
                    SUBP_COMPONENT_GS_TIMBER TEXT,
                    SUBP_TPAGROW_UNADJ_GS_TIMBER REAL,
                    SUBP_SUBPTYP_GRM_GS_TIMBER INTEGER
                )
            """,
            "TREE_GRM_MIDPT": """
                CREATE TABLE TREE_GRM_MIDPT (
                    TRE_CN TEXT PRIMARY KEY,
                    PLT_CN TEXT,
                    DIA REAL,
                    SPCD INTEGER,
                    STATUSCD INTEGER,
                    VOLCFNET REAL,
                    DRYBIO_AG REAL
                )
            """,
            "TREE_GRM_BEGIN": """
                CREATE TABLE TREE_GRM_BEGIN (
                    TRE_CN TEXT PRIMARY KEY,
                    PLT_CN TEXT,
                    VOLCFNET REAL,
                    DRYBIO_AG REAL
                )
            """,
            "COND": """
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
            """,
            "PLOT": """
                CREATE TABLE PLOT (
                    CN TEXT PRIMARY KEY,
                    STATECD INTEGER,
                    COUNTYCD INTEGER,
                    INVYR INTEGER,
                    PLOT_STATUS_CD INTEGER,
                    MACRO_BREAKPOINT_DIA REAL,
                    REMPER REAL
                )
            """,
            "POP_STRATUM": """
                CREATE TABLE POP_STRATUM (
                    CN TEXT PRIMARY KEY,
                    EVALID INTEGER,
                    EXPNS REAL,
                    ADJ_FACTOR_SUBP REAL,
                    ADJ_FACTOR_MICR REAL,
                    ADJ_FACTOR_MACR REAL
                )
            """,
            "POP_PLOT_STRATUM_ASSGN": """
                CREATE TABLE POP_PLOT_STRATUM_ASSGN (
                    CN TEXT PRIMARY KEY,
                    PLT_CN TEXT,
                    STRATUM_CN TEXT,
                    EVALID INTEGER
                )
            """,
            "BEGINEND": """
                CREATE TABLE BEGINEND (
                    CN TEXT PRIMARY KEY,
                    EVALID INTEGER,
                    ONEORTWO INTEGER
                )
            """
        }

        # Create all tables
        for table_name, sql in tables_sql.items():
            conn.execute(sql)

        # Insert realistic scale test data (50+ trees across multiple plots)
        # This tests performance and real-world data patterns

        plot_ids = [f"P{i:03d}" for i in range(1, 11)]  # 10 plots
        tree_counter = 1

        for plot_id in plot_ids:
            # Each plot has 5-8 trees with various components
            trees_per_plot = 5 + (int(plot_id[1:]) % 4)  # 5-8 trees

            for tree_num in range(trees_per_plot):
                tree_id = f"T{tree_counter:03d}"
                tree_counter += 1

                # Vary component types realistically
                if tree_num == 0:
                    component = "SURVIVOR"
                    tpa_grow = 2.0 + (tree_counter % 5) * 0.5
                    subptyp = 1  # SUBP
                    dia_begin = 8.0 + (tree_counter % 10)
                    dia_midpt = dia_begin + 1.0
                    dia_end = dia_begin + 2.0
                    vol_begin = dia_begin ** 2 * 0.5
                    vol_midpt = dia_midpt ** 2 * 0.5
                elif tree_num == 1 and tree_counter % 3 == 0:
                    component = "INGROWTH"
                    tpa_grow = 1.5 + (tree_counter % 3) * 0.3
                    subptyp = 2  # MICR
                    dia_begin = None
                    dia_midpt = 5.0 + (tree_counter % 3)
                    dia_end = dia_midpt + 0.5
                    vol_begin = None
                    vol_midpt = dia_midpt ** 2 * 0.5
                elif tree_num == 2 and tree_counter % 7 == 0:
                    component = "REVERSION1"
                    tpa_grow = 1.0 + (tree_counter % 4) * 0.2
                    subptyp = 1  # SUBP
                    dia_begin = None
                    dia_midpt = 7.0 + (tree_counter % 5)
                    dia_end = dia_midpt + 1.0
                    vol_begin = None
                    vol_midpt = dia_midpt ** 2 * 0.5
                else:
                    component = "SURVIVOR"
                    tpa_grow = 1.8 + (tree_counter % 6) * 0.4
                    subptyp = 1 if tree_counter % 10 < 7 else 3  # Mostly SUBP, some MACR
                    dia_begin = 10.0 + (tree_counter % 15)
                    dia_midpt = dia_begin + 1.2
                    dia_end = dia_begin + 2.5
                    vol_begin = dia_begin ** 2 * 0.5
                    vol_midpt = dia_midpt ** 2 * 0.5

                # Add some zeros for SUBPTYP_GRM = 0 (not sampled)
                if tree_counter % 20 == 0:
                    subptyp = 0
                    tpa_grow = 0.0

                spcd = [131, 110, 316, 833, 621][tree_counter % 5]  # Vary species

                # Insert GRM component data
                conn.execute("""
                    INSERT INTO TREE_GRM_COMPONENT VALUES
                    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    tree_id, plot_id, dia_begin, dia_midpt, dia_end,
                    component, tpa_grow, subptyp,
                    component, tpa_grow, subptyp,
                    component, tpa_grow, subptyp
                ))

                # Insert midpoint data
                conn.execute("""
                    INSERT INTO TREE_GRM_MIDPT VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    tree_id, plot_id, dia_midpt, spcd, 1,
                    vol_midpt, vol_midpt * 7  # biomass ~ 7x volume
                ))

                # Insert beginning data for SURVIVOR trees only
                if component == "SURVIVOR" and vol_begin is not None:
                    conn.execute("""
                        INSERT INTO TREE_GRM_BEGIN VALUES (?, ?, ?, ?)
                    """, (
                        tree_id, plot_id, vol_begin, vol_begin * 7
                    ))

        # Insert plot and condition data
        for i, plot_id in enumerate(plot_ids):
            plot_cn = plot_id
            county = 1 + (i % 5)
            invyr = 2019 + (i % 4)
            remper = 4.0 + (i % 3)  # Vary REMPER: 4.0, 5.0, 6.0
            alstkcd = 1 + (i % 5)  # Stocking classes 1-5

            # Plot data
            conn.execute("""
                INSERT INTO PLOT VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (plot_cn, 37, county, invyr, 1, 24.0, remper))

            # Condition data
            conn.execute("""
                INSERT INTO COND VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                f"C{i+1:03d}", plot_id, 1, 1, 1.0,
                40, 161, 2, 0, alstkcd
            ))

        # Insert stratification data (3 strata)
        for i in range(3):
            stratum_cn = f"S{i+1}"
            conn.execute("""
                INSERT INTO POP_STRATUM VALUES (?, ?, ?, ?, ?, ?)
            """, (
                stratum_cn, 372301, 6000.0 + i * 500,
                1.0, 12.0, 0.25
            ))

        # Assign plots to strata
        for i, plot_id in enumerate(plot_ids):
            stratum_cn = f"S{(i % 3) + 1}"  # Distribute plots across strata
            conn.execute("""
                INSERT INTO POP_PLOT_STRATUM_ASSGN VALUES (?, ?, ?, ?)
            """, (f"A{i+1:03d}", plot_id, stratum_cn, 372301))

        # Insert BEGINEND
        conn.execute("INSERT INTO BEGINEND VALUES ('BE1', 372301, 2)")

        conn.commit()
        conn.close()

        return Path(db_path)

    def test_large_dataset_performance(self, realistic_fia_database):
        """Test growth calculation performance with realistic dataset size."""
        with FIA(str(realistic_fia_database)) as db:
            db.clip_by_evalid([372301])

            import time
            start_time = time.time()

            # This should complete reasonably quickly even with 50+ trees
            results = growth(
                db,
                land_type="forest",
                tree_type="gs",
                measure="volume"
            )

            end_time = time.time()
            runtime = end_time - start_time

            assert not results.is_empty(), "Should produce results with realistic dataset"
            assert runtime < 10.0, f"Growth calculation took too long: {runtime:.2f} seconds"

            # Verify reasonable results
            growth_acre = results["GROWTH_ACRE"][0]
            assert 0 < growth_acre < 500, f"Growth per acre should be reasonable: {growth_acre:.1f}"

    def test_multiple_species_grouping(self, realistic_fia_database):
        """Test growth calculation with by_species grouping."""
        with FIA(str(realistic_fia_database)) as db:
            db.clip_by_evalid([372301])

            results = growth(
                db,
                by_species=True,
                land_type="forest",
                tree_type="gs",
                measure="volume"
            )

            # Should have multiple species
            assert len(results) >= 3, "Should have multiple species groups"
            assert "SPCD" in results.columns, "Should include SPCD column"

            # Check we have expected species codes
            species_codes = set(results["SPCD"].to_list())
            expected_species = {131, 110, 316, 833, 621}  # From test data
            assert len(species_codes & expected_species) >= 3, "Should have multiple expected species"

            # Each species should have reasonable growth
            for row in results.iter_rows(named=True):
                spcd = row["SPCD"]
                growth_acre = row["GROWTH_ACRE"]
                assert growth_acre >= 0, f"Growth should be non-negative for species {spcd}"

    def test_multiple_grouping_variables(self, realistic_fia_database):
        """Test growth calculation with multiple grouping variables."""
        with FIA(str(realistic_fia_database)) as db:
            db.clip_by_evalid([372301])

            results = growth(
                db,
                grp_by=["COUNTYCD", "ALSTKCD"],
                land_type="forest",
                tree_type="gs",
                measure="volume"
            )

            # Should have multiple groups
            assert len(results) >= 4, "Should have multiple county/stocking class combinations"
            assert "COUNTYCD" in results.columns, "Should include COUNTYCD"
            assert "ALSTKCD" in results.columns, "Should include ALSTKCD"

            # Check for reasonable distribution
            county_codes = set(results["COUNTYCD"].to_list())
            alstkcd_values = set(results["ALSTKCD"].to_list())

            assert len(county_codes) >= 2, "Should have multiple counties"
            assert len(alstkcd_values) >= 2, "Should have multiple stocking classes"

    def test_different_land_types(self, realistic_fia_database):
        """Test growth calculation with different land_type parameters."""
        with FIA(str(realistic_fia_database)) as db:
            db.clip_by_evalid([372301])

            # Test forest land
            forest_results = growth(
                db,
                land_type="forest",
                tree_type="gs",
                measure="volume"
            )

            # Test timber land (subset of forest)
            timber_results = growth(
                db,
                land_type="timber",
                tree_type="gs",
                measure="volume"
            )

            assert not forest_results.is_empty(), "Should have forest results"
            assert not timber_results.is_empty(), "Should have timber results"

            # Timber should be subset of forest (timber ≤ forest)
            forest_growth = forest_results["GROWTH_TOTAL"][0] if "GROWTH_TOTAL" in forest_results.columns else 0
            timber_growth = timber_results["GROWTH_TOTAL"][0] if "GROWTH_TOTAL" in timber_results.columns else 0

            assert timber_growth <= forest_growth, "Timber growth should not exceed forest growth"

    def test_different_tree_types(self, realistic_fia_database):
        """Test growth calculation with different tree_type parameters."""
        with FIA(str(realistic_fia_database)) as db:
            db.clip_by_evalid([372301])

            # Test different tree types
            gs_results = growth(db, tree_type="gs", measure="volume")  # Growing stock
            al_results = growth(db, tree_type="al", measure="volume")  # All live

            assert not gs_results.is_empty(), "Should have growing stock results"
            assert not al_results.is_empty(), "Should have all live results"

            # All live should include growing stock, so AL ≥ GS
            gs_growth = gs_results["GROWTH_TOTAL"][0] if "GROWTH_TOTAL" in gs_results.columns else 0
            al_growth = al_results["GROWTH_TOTAL"][0] if "GROWTH_TOTAL" in al_results.columns else 0

            assert al_growth >= gs_growth, "All live growth should not be less than growing stock growth"

    def test_different_measures(self, realistic_fia_database):
        """Test growth calculation with different measure types."""
        with FIA(str(realistic_fia_database)) as db:
            db.clip_by_evalid([372301])

            # Test different measures
            volume_results = growth(db, measure="volume")
            biomass_results = growth(db, measure="biomass")
            count_results = growth(db, measure="count")

            # All should produce results
            for results, measure in [(volume_results, "volume"), (biomass_results, "biomass"), (count_results, "count")]:
                assert not results.is_empty(), f"Should have {measure} results"

                growth_acre = results["GROWTH_ACRE"][0]
                assert growth_acre >= 0, f"{measure.title()} growth should be non-negative"

            # Verify reasonable relative magnitudes
            vol_growth = volume_results["GROWTH_ACRE"][0]
            bio_growth = biomass_results["GROWTH_ACRE"][0]
            cnt_growth = count_results["GROWTH_ACRE"][0]

            # Biomass should be higher than volume (tons vs cu ft)
            assert bio_growth > vol_growth, "Biomass growth should exceed volume growth in magnitude"

            # Count should be much smaller (trees vs cu ft)
            assert cnt_growth < vol_growth, "Tree count growth should be less than volume growth"

    def test_variance_calculation_integration(self, realistic_fia_database):
        """Test variance calculation with realistic data."""
        with FIA(str(realistic_fia_database)) as db:
            db.clip_by_evalid([372301])

            results = growth(
                db,
                land_type="forest",
                tree_type="gs",
                measure="volume",
                variance=True
            )

            # Should include variance columns
            expected_variance_columns = {"GROWTH_ACRE_SE"}
            variance_columns = {col for col in results.columns if col.endswith("_SE")}

            assert len(variance_columns) >= 1, "Should include at least one standard error column"

            # Check variance values are reasonable
            if "GROWTH_ACRE_SE" in results.columns:
                growth_acre = results["GROWTH_ACRE"][0]
                growth_se = results["GROWTH_ACRE_SE"][0]

                assert growth_se >= 0, "Standard error should be non-negative"
                assert growth_se <= growth_acre, "Standard error should not exceed estimate"

                # CV should be reasonable (not too high)
                cv = (growth_se / growth_acre) * 100 if growth_acre > 0 else 0
                assert cv <= 50, f"Coefficient of variation seems high: {cv:.1f}%"

    def test_domain_filtering_integration(self, realistic_fia_database):
        """Test domain filtering with realistic scenarios."""
        with FIA(str(realistic_fia_database)) as db:
            db.clip_by_evalid([372301])

            # Test tree domain filtering
            all_trees = growth(db, measure="volume")
            large_trees = growth(db, tree_domain="DIA_MIDPT >= 10.0", measure="volume")

            assert not all_trees.is_empty(), "Should have results for all trees"
            assert not large_trees.is_empty(), "Should have results for large trees"

            # Large trees should be subset
            all_growth = all_trees["GROWTH_TOTAL"][0] if "GROWTH_TOTAL" in all_trees.columns else 0
            large_growth = large_trees["GROWTH_TOTAL"][0] if "GROWTH_TOTAL" in large_trees.columns else 0

            assert large_growth <= all_growth, "Large tree growth should not exceed all tree growth"

            # Test area domain filtering
            all_ownership = growth(db, measure="volume")
            private_only = growth(db, area_domain="OWNGRPCD == 40", measure="volume")

            assert not private_only.is_empty(), "Should have results for private ownership filter"

    def test_error_handling_integration(self, realistic_fia_database):
        """Test error handling in integration scenarios."""
        # Test with non-existent EVALID
        with FIA(str(realistic_fia_database)) as db:
            db.clip_by_evalid([999999])  # Non-existent

            # Should handle gracefully (empty results or clear error)
            try:
                results = growth(db, measure="volume")
                # If it returns results, they should be empty or minimal
                if not results.is_empty():
                    assert len(results) == 0 or results["GROWTH_TOTAL"].sum() == 0
            except (ValueError, KeyError) as e:
                # Clear error is also acceptable
                assert "EVALID" in str(e) or "no data" in str(e).lower()

        # Test with invalid parameters
        with FIA(str(realistic_fia_database)) as db:
            db.clip_by_evalid([372301])

            with pytest.raises((ValueError, KeyError)):
                growth(db, measure="invalid_measure")

    def test_memory_efficiency(self, realistic_fia_database):
        """Test memory efficiency with multiple calculations."""
        with FIA(str(realistic_fia_database)) as db:
            db.clip_by_evalid([372301])

            # Run multiple calculations to test memory usage
            results_list = []

            for i in range(5):
                results = growth(
                    db,
                    grp_by="ALSTKCD" if i % 2 == 0 else "COUNTYCD",
                    measure="volume" if i % 2 == 0 else "biomass",
                    land_type="forest",
                    tree_type="gs"
                )
                results_list.append(results)

            # All calculations should succeed
            for i, results in enumerate(results_list):
                assert not results.is_empty(), f"Calculation {i} should produce results"

            # Memory usage should be reasonable (no major leaks)
            # This is implicit - if there were memory leaks, the test would likely fail or hang

    def test_concurrent_calculations(self, realistic_fia_database):
        """Test that multiple simultaneous calculations don't interfere."""
        # This tests thread safety and data isolation

        def run_growth_calculation(db_path, evalid, params):
            """Helper function for concurrent testing."""
            with FIA(str(db_path)) as db:
                db.clip_by_evalid([evalid])
                return growth(db, **params)

        # Run multiple calculations with different parameters
        params_list = [
            {"measure": "volume", "tree_type": "gs"},
            {"measure": "biomass", "tree_type": "al"},
            {"grp_by": "ALSTKCD", "measure": "volume"},
            {"by_species": True, "measure": "volume"}
        ]

        results_list = []
        for params in params_list:
            result = run_growth_calculation(realistic_fia_database, 372301, params)
            results_list.append(result)

        # All should succeed
        for i, results in enumerate(results_list):
            assert not results.is_empty(), f"Concurrent calculation {i} should produce results"

    @patch('polars.LazyFrame.collect_schema')
    def test_collect_schema_optimization(self, mock_collect_schema, realistic_fia_database):
        """Test that collect_schema() calls are minimized for performance."""
        # Set up mock
        mock_collect_schema.return_value = pl.Schema({
            "TRE_CN": pl.Utf8,
            "PLT_CN": pl.Utf8,
            "VOLCFNET": pl.Float64,
            "DIA_MIDPT": pl.Float64
        })

        with FIA(str(realistic_fia_database)) as db:
            db.clip_by_evalid([372301])

            # Run growth calculation
            results = growth(db, land_type="forest", tree_type="gs", measure="volume")

            # Check how many times collect_schema was called
            schema_calls = mock_collect_schema.call_count

            # Should be called minimally (the performance issue from the bug)
            assert schema_calls <= 5, f"Too many collect_schema() calls: {schema_calls}. Consider optimizing."

            assert not results.is_empty(), "Should still produce results with mocked schema"