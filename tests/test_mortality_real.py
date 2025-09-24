"""
Real data validation tests for mortality estimation.

These tests validate mortality estimation against actual FIA database files and
compare results with published estimates from official FIA reports and EVALIDator.

Tests use fia.duckdb database with Georgia (STATECD=13) and South Carolina (STATECD=45) data.
Expected values should be obtained from EVALIDator (https://apps.fs.usda.gov/Evalidator/).
"""

import polars as pl
import pytest
from pathlib import Path

from pyfia import FIA
from pyfia.estimation import mortality


class TestMortalityRealData:
    """Real data validation tests for mortality estimation using fia.duckdb."""

    @pytest.fixture
    def fia_database_path(self):
        """Path to the real FIA DuckDB database."""
        db_path = Path("fia.duckdb")
        if not db_path.exists():
            pytest.skip("fia.duckdb not found - real data tests require this database")
        return str(db_path)

    def test_georgia_mortality_basic(self, fia_database_path):
        """
        Test Georgia total mortality estimation basic functionality.

        Validates that mortality function returns expected columns and reasonable values.
        """
        with FIA(fia_database_path) as db:
            db.clip_by_state(13, most_recent=True, eval_type="EXPMORT")  # Georgia mortality eval

            # Get basic mortality estimates
            result = mortality(db)

            assert isinstance(result, pl.DataFrame)
            assert "MORT_ACRE" in result.columns
            assert "MORT_ACRE_SE" in result.columns
            assert "MORT_ACRE_VAR" in result.columns
            assert "N_PLOTS" in result.columns
            assert len(result) == 1

            mort_per_acre = result["MORT_ACRE"][0]
            n_plots = result["N_PLOTS"][0]

            print(f"\n=== GEORGIA MORTALITY BASIC ===")
            print(f"Mortality per acre: {mort_per_acre:.2f} cubic feet/acre/year")
            print(f"Sample Size: {n_plots:,} plots")

            # Basic validation - mortality should be reasonable
            assert mort_per_acre > 0, f"Mortality {mort_per_acre:.2f} should be positive"
            assert n_plots > 100, f"Too few plots for reliable estimate: {n_plots}"

    def test_south_carolina_mortality_basic(self, fia_database_path):
        """
        Test South Carolina total mortality estimation basic functionality.
        """
        with FIA(fia_database_path) as db:
            db.clip_by_state(45, most_recent=True, eval_type="EXPMORT")  # South Carolina mortality eval

            # Get basic mortality estimates
            result = mortality(db)

            assert isinstance(result, pl.DataFrame)
            assert "MORT_ACRE" in result.columns
            assert "MORT_ACRE_SE" in result.columns
            assert "N_PLOTS" in result.columns
            assert len(result) == 1

            mort_per_acre = result["MORT_ACRE"][0]
            n_plots = result["N_PLOTS"][0]

            print(f"\n=== SOUTH CAROLINA MORTALITY BASIC ===")
            print(f"Mortality per acre: {mort_per_acre:.2f} cubic feet/acre/year")
            print(f"Sample Size: {n_plots:,} plots")

            # Basic validation
            assert mort_per_acre > 0, f"Mortality {mort_per_acre:.2f} should be positive"
            assert n_plots > 100, f"Too few plots for reliable estimate: {n_plots}"

    def test_georgia_mortality_by_species(self, fia_database_path):
        """Test Georgia mortality by species grouping."""
        with FIA(fia_database_path) as db:
            db.clip_by_state(13, most_recent=True, eval_type="EXPMORT")

            result = mortality(db, by_species=True)

            assert "SPCD" in result.columns
            assert len(result) > 10, f"Expected multiple species with mortality in Georgia"

            print(f"\nGeorgia Mortality by Species (top 5):")
            top_species = result.sort("MORT_ACRE", descending=True).head(5)

            total_mortality = 0
            for row in top_species.iter_rows(named=True):
                spcd = row["SPCD"]
                mort_acre = row["MORT_ACRE"]
                n_plots = row["N_PLOTS"]
                print(f"  Species {spcd}: {mort_acre:.2f} cu ft/acre/year (N_PLOTS={n_plots})")

                assert mort_acre >= 0  # Some species may have zero mortality
                assert n_plots > 0
                total_mortality += mort_acre

            # Top 5 species should have some mortality
            assert total_mortality > 0, f"Top 5 species have no mortality"

    def test_south_carolina_mortality_by_species(self, fia_database_path):
        """Test South Carolina mortality by species grouping."""
        with FIA(fia_database_path) as db:
            db.clip_by_state(45, most_recent=True, eval_type="EXPMORT")

            result = mortality(db, by_species=True)

            assert "SPCD" in result.columns
            assert len(result) > 10, f"Expected multiple species with mortality in South Carolina"

            print(f"\nSouth Carolina Mortality by Species (top 5):")
            top_species = result.sort("MORT_ACRE", descending=True).head(5)

            total_mortality = 0
            for row in top_species.iter_rows(named=True):
                spcd = row["SPCD"]
                mort_acre = row["MORT_ACRE"]
                n_plots = row["N_PLOTS"]
                print(f"  Species {spcd}: {mort_acre:.2f} cu ft/acre/year (N_PLOTS={n_plots})")

                assert mort_acre >= 0
                assert n_plots > 0
                total_mortality += mort_acre

            assert total_mortality > 0, f"Top 5 species have no mortality"

    def test_loblolly_pine_mortality(self, fia_database_path):
        """
        Test loblolly pine (SPCD=131) mortality in Georgia and South Carolina.

        Loblolly pine is a major commercial species in the Southeast.
        """
        for state_code, state_name in [(13, "Georgia"), (45, "South Carolina")]:
            with FIA(fia_database_path) as db:
                db.clip_by_state(state_code, most_recent=True, eval_type="EXPMORT")

                # Filter to loblolly pine
                result = mortality(
                    db,
                    tree_domain="SPCD == 131",
                    land_type="forest"
                )

                mort_per_acre = result["MORT_ACRE"][0] if len(result) > 0 else 0
                n_plots = result["N_PLOTS"][0] if len(result) > 0 else 0

                print(f"\n{state_name} Loblolly Pine Mortality: {mort_per_acre:.2f} cu ft/acre/year (N_PLOTS={n_plots})")

                # Loblolly should have some mortality
                assert mort_per_acre >= 0, f"{state_name} loblolly mortality {mort_per_acre:.2f} should be non-negative"
                if n_plots > 0:  # Only check if we have plots with loblolly mortality
                    assert mort_per_acre > 0, f"Expected some loblolly mortality when plots present"

    def test_mortality_by_forest_type(self, fia_database_path):
        """Test mortality grouped by forest type (FORTYPCD)."""
        for state_code, state_name in [(13, "Georgia"), (45, "South Carolina")]:
            with FIA(fia_database_path) as db:
                db.clip_by_state(state_code, most_recent=True, eval_type="EXPMORT")

                result = mortality(
                    db,
                    grp_by=["FORTYPCD"],
                    land_type="forest"
                )

                assert "FORTYPCD" in result.columns
                assert len(result) > 5, f"Expected multiple forest types in {state_name}"

                print(f"\n{state_name} Mortality by Forest Type (top 5):")
                top_types = result.sort("MORT_ACRE", descending=True).head(5)

                for row in top_types.iter_rows(named=True):
                    forest_type = row["FORTYPCD"]
                    mort_acre = row["MORT_ACRE"]
                    n_plots = row["N_PLOTS"]
                    print(f"  Forest Type {forest_type}: {mort_acre:.2f} cu ft/acre/year (N_PLOTS={n_plots})")

                    assert mort_acre >= 0
                    assert n_plots > 0

    def test_mortality_with_totals(self, fia_database_path):
        """Test mortality with total expansion to population level."""
        for state_code, state_name in [(13, "Georgia"), (45, "South Carolina")]:
            with FIA(fia_database_path) as db:
                db.clip_by_state(state_code, most_recent=True, eval_type="EXPMORT")

                # Get both per-acre and total estimates
                result = mortality(db, totals=True, land_type="forest")

                assert "MORT_ACRE" in result.columns
                assert "MORT_TOTAL" in result.columns
                assert "MORT_TOTAL_SE" in result.columns

                mort_acre = result["MORT_ACRE"][0]
                mort_total = result["MORT_TOTAL"][0]
                mort_total_se = result["MORT_TOTAL_SE"][0]
                n_plots = result["N_PLOTS"][0]

                sampling_error_pct = (mort_total_se / mort_total * 100) if mort_total > 0 else 0

                print(f"\n{state_name} Total Mortality:")
                print(f"  Per acre: {mort_acre:.2f} cu ft/acre/year")
                print(f"  Total: {mort_total:,.0f} cu ft/year")
                print(f"  Sampling Error: {sampling_error_pct:.2f}%")
                print(f"  N_PLOTS: {n_plots:,}")

                # Validate reasonable values
                assert mort_total > 1_000_000, f"{state_name} total mortality seems too low: {mort_total:,.0f}"
                assert 0.1 <= sampling_error_pct <= 20.0, f"Sampling error {sampling_error_pct:.2f}% outside reasonable range"

    def test_mortality_different_measures(self, fia_database_path):
        """Test mortality estimation with different measurement units."""
        with FIA(fia_database_path) as db:
            db.clip_by_state(13, most_recent=True, eval_type="EXPMORT")  # Georgia

            measures = ["volume", "basal_area", "biomass", "tpa"]

            print(f"\nGeorgia Mortality by Different Measures:")
            for measure in measures:
                result = mortality(db, measure=measure)

                mort_acre = result["MORT_ACRE"][0]
                n_plots = result["N_PLOTS"][0]

                # Get appropriate units
                units = {
                    "volume": "cu ft/acre/year",
                    "basal_area": "sq ft/acre/year",
                    "biomass": "tons/acre/year",
                    "tpa": "trees/acre/year"
                }[measure]

                print(f"  {measure.title()}: {mort_acre:.2f} {units} (N_PLOTS={n_plots})")

                # All measures should have positive mortality
                assert mort_acre > 0, f"No mortality for measure {measure}"
                assert n_plots > 100, f"Too few plots for {measure}"

    def test_mortality_by_ownership(self, fia_database_path):
        """Test mortality grouped by ownership."""
        with FIA(fia_database_path) as db:
            db.clip_by_state(13, most_recent=True, eval_type="EXPMORT")  # Georgia

            result = mortality(
                db,
                grp_by=["OWNGRPCD"],
                land_type="forest"
            )

            assert "OWNGRPCD" in result.columns

            print(f"\nGeorgia Mortality by Ownership:")
            ownership_names = {
                10: "National Forest",
                20: "Other Federal",
                30: "State/Local",
                40: "Private"
            }

            for row in result.iter_rows(named=True):
                own_code = row["OWNGRPCD"]
                mort_acre = row["MORT_ACRE"]
                n_plots = row["N_PLOTS"]
                own_name = ownership_names.get(own_code, f"Code {own_code}")

                print(f"  {own_name}: {mort_acre:.2f} cu ft/acre/year (N_PLOTS={n_plots})")

                assert mort_acre >= 0
                assert n_plots > 0

    def test_georgia_mortality_evalidator_comparison(self, fia_database_path):
        """
        Compare Georgia mortality results with EVALIDator - EXACT MATCH VALIDATION.

        EVALIDator Georgia results for average annual mortality of sawlog volume
        of sawtimber trees:
        - Total: 307,168,403 cubic feet
        - Non-zero plots: 924
        - Sampling error: 5.527%
        """
        EXPECTED_GA_SAWLOG_TOTAL = 307_168_403  # From EVALIDator
        EXPECTED_GA_SAWLOG_PLOTS = 924  # Non-zero plots
        EXPECTED_GA_SAWLOG_SE_PCT = 5.527  # Sampling error percentage
        TOLERANCE = 1000  # Allow 1000 cu ft tolerance for volume
        SE_TOLERANCE = 0.5  # Allow 0.5% tolerance for sampling error

        with FIA(fia_database_path) as db:
            # Use mortality evaluation type
            db.clip_by_state(13, most_recent=True, eval_type="EXPMORT")

            # Calculate sawlog volume mortality of sawtimber trees
            # Sawtimber trees: typically DIA >= 9.0 for softwoods, >= 11.0 for hardwoods
            # Sawlog volume: VOLCSNET (saw-log net volume)
            result = mortality(
                db,
                measure="sawlog",  # Use sawlog volume measure
                tree_type="sawtimber",  # Sawtimber trees only
                totals=True
            )

            actual_total = result["MORT_TOTAL"][0]
            actual_plots = result["N_PLOTS"][0]
            actual_se = result["MORT_TOTAL_SE"][0]
            actual_se_pct = (actual_se / actual_total * 100) if actual_total > 0 else 0

            print(f"\nGeorgia Sawlog Mortality EVALIDator Comparison:")
            print(f"Total Volume:")
            print(f"  pyFIA: {actual_total:,.0f} cu ft")
            print(f"  EVALIDator: {EXPECTED_GA_SAWLOG_TOTAL:,.0f} cu ft")
            print(f"  Difference: {abs(actual_total - EXPECTED_GA_SAWLOG_TOTAL):,.0f} cu ft")
            print(f"\nNon-zero Plots:")
            print(f"  pyFIA: {actual_plots:,}")
            print(f"  EVALIDator: {EXPECTED_GA_SAWLOG_PLOTS:,}")
            print(f"  Difference: {abs(actual_plots - EXPECTED_GA_SAWLOG_PLOTS)}")
            print(f"\nSampling Error %:")
            print(f"  pyFIA: {actual_se_pct:.3f}%")
            print(f"  EVALIDator: {EXPECTED_GA_SAWLOG_SE_PCT:.3f}%")
            print(f"  Difference: {abs(actual_se_pct - EXPECTED_GA_SAWLOG_SE_PCT):.3f}%")

            # Assert exact matches within tolerance
            assert abs(actual_total - EXPECTED_GA_SAWLOG_TOTAL) <= TOLERANCE, \
                f"Georgia sawlog mortality mismatch: pyFIA {actual_total:,.0f} vs EVALIDator {EXPECTED_GA_SAWLOG_TOTAL:,.0f}"

            assert actual_plots == EXPECTED_GA_SAWLOG_PLOTS, \
                f"Georgia plot count mismatch: pyFIA {actual_plots:,} vs EVALIDator {EXPECTED_GA_SAWLOG_PLOTS:,}"

            assert abs(actual_se_pct - EXPECTED_GA_SAWLOG_SE_PCT) <= SE_TOLERANCE, \
                f"Georgia sampling error mismatch: pyFIA {actual_se_pct:.3f}% vs EVALIDator {EXPECTED_GA_SAWLOG_SE_PCT:.3f}%"

            print(f"\n✓ All metrics match EVALIDator!")

    def test_comprehensive_mortality_summary_for_evalidator(self, fia_database_path):
        """
        Comprehensive mortality summary for both states - all values for EVALIDator comparison.

        This test outputs all the key mortality values that can be directly
        compared with EVALIDator results.

        Known EVALIDator values:
        - Georgia sawlog mortality: 307,168,403 cu ft, 924 plots, 5.527% SE
        """
        print("\n" + "="*80)
        print("COMPREHENSIVE MORTALITY SUMMARY FOR EVALIDATOR COMPARISON")
        print("="*80)
        print("Known EVALIDator values:")
        print("- Georgia sawlog mortality: 307,168,403 cu ft, 924 plots, 5.527% SE")
        print("="*80)

        for state_code, state_name in [(13, "Georgia"), (45, "South Carolina")]:
            print(f"\n{'='*20} {state_name.upper()} MORTALITY ESTIMATES {'='*20}")

            with FIA(fia_database_path) as db:
                # Use mortality evaluation
                db.clip_by_state(state_code, most_recent=True, eval_type="EXPMORT")

                # Get current EVALID for reporting
                import duckdb
                with duckdb.connect(fia_database_path, read_only=True) as conn:
                    evalid_query = """
                    SELECT DISTINCT pe.EVALID, pe.EVAL_DESCR
                    FROM POP_EVAL pe
                    JOIN POP_EVAL_TYP pet ON pe.CN = pet.EVAL_CN
                    WHERE pe.STATECD = ? AND pet.EVAL_TYP = 'EXPMORT'
                    ORDER BY pe.EVALID DESC
                    LIMIT 1
                    """
                    evalid_result = conn.execute(evalid_query, [state_code]).fetchone()
                    if evalid_result:
                        current_evalid = evalid_result[0]
                        print(f"Using EVALID: {current_evalid}")
                    else:
                        print(f"Warning: No EXPMORT evaluation found for {state_name}")
                        continue

                # Total forest mortality (volume)
                forest_result = mortality(db, land_type="forest", totals=True, measure="volume")
                forest_acre = forest_result["MORT_ACRE"][0]
                forest_total = forest_result["MORT_TOTAL"][0]
                forest_se = forest_result["MORT_TOTAL_SE"][0]
                forest_plots = forest_result["N_PLOTS"][0]
                forest_se_pct = (forest_se / forest_total * 100) if forest_total > 0 else 0

                # Timberland mortality
                timber_result = mortality(db, land_type="timber", totals=True, measure="volume")
                timber_acre = timber_result["MORT_ACRE"][0]
                timber_total = timber_result["MORT_TOTAL"][0]
                timber_se = timber_result["MORT_TOTAL_SE"][0]
                timber_plots = timber_result["N_PLOTS"][0]
                timber_se_pct = (timber_se / timber_total * 100) if timber_total > 0 else 0

                print(f"\nFOREST MORTALITY (Volume):")
                print(f"  Per acre: {forest_acre:.2f} cu ft/acre/year")
                print(f"  Total: {forest_total:,.0f} cu ft/year")
                print(f"  Sampling Error: {forest_se_pct:.3f}%")
                print(f"  Non-zero plots: {forest_plots:,}")

                print(f"\nTIMBERLAND MORTALITY (Volume):")
                print(f"  Per acre: {timber_acre:.2f} cu ft/acre/year")
                print(f"  Total: {timber_total:,.0f} cu ft/year")
                print(f"  Sampling Error: {timber_se_pct:.3f}%")
                print(f"  Non-zero plots: {timber_plots:,}")

                # Mortality by different measures
                print(f"\nMORTALITY BY MEASURE (Forest):")
                for measure in ["volume", "basal_area", "biomass", "tpa"]:
                    m_result = mortality(db, land_type="forest", totals=True, measure=measure)
                    m_acre = m_result["MORT_ACRE"][0]
                    m_total = m_result["MORT_TOTAL"][0]

                    units_acre = {
                        "volume": "cu ft/acre/year",
                        "basal_area": "sq ft/acre/year",
                        "biomass": "tons/acre/year",
                        "tpa": "trees/acre/year"
                    }[measure]

                    units_total = {
                        "volume": "cu ft/year",
                        "basal_area": "sq ft/year",
                        "biomass": "tons/year",
                        "tpa": "trees/year"
                    }[measure]

                    print(f"  {measure.title():12}: {m_acre:10.2f} {units_acre:20} Total: {m_total:15,.0f} {units_total}")

                # Loblolly pine mortality
                loblolly_result = mortality(
                    db,
                    tree_domain="SPCD == 131",
                    land_type="forest",
                    totals=True
                )
                lob_acre = loblolly_result["MORT_ACRE"][0]
                lob_total = loblolly_result["MORT_TOTAL"][0]
                lob_plots = loblolly_result["N_PLOTS"][0]

                print(f"\nLOBLOLLY PINE MORTALITY (SPCD=131):")
                print(f"  Per acre: {lob_acre:.2f} cu ft/acre/year")
                print(f"  Total: {lob_total:,.0f} cu ft/year")
                print(f"  Non-zero plots: {lob_plots:,}")

                print("-" * 80)

        print(f"\nINSTRUCTIONS FOR EVALIDATOR COMPARISON:")
        print("1. Go to https://apps.fs.usda.gov/Evalidator/")
        print("2. Select Georgia or South Carolina")
        print("3. Select mortality evaluation (EXPMORT type)")
        print("4. Run mortality estimates for:")
        print("   - Total forest mortality")
        print("   - Timberland mortality")
        print("   - Mortality by species or forest type")
        print("5. Compare EVALIDator totals with pyFIA results above")

    def test_mortality_temporal_methods(self, fia_database_path):
        """Test different temporal calculation methods for mortality."""
        with FIA(fia_database_path) as db:
            db.clip_by_state(13, most_recent=True, eval_type="EXPMORT")  # Georgia

            temporal_methods = ["TI", "ANNUAL", "SMA", "LMA"]  # EMA requires more complex setup

            print(f"\nGeorgia Mortality by Temporal Method:")
            for method in temporal_methods:
                try:
                    result = mortality(db, temporal_method=method)
                    mort_acre = result["MORT_ACRE"][0]
                    n_plots = result["N_PLOTS"][0]

                    print(f"  {method:8}: {mort_acre:.2f} cu ft/acre/year (N_PLOTS={n_plots})")

                    # All methods should produce positive mortality
                    assert mort_acre >= 0, f"Negative mortality for method {method}"

                except Exception as e:
                    print(f"  {method:8}: Not available - {str(e)}")

    def test_mortality_sampling_error_validation(self, fia_database_path):
        """
        Test that sampling error calculations are reasonable for mortality.

        This validates the standard error and coefficient of variation.
        """
        with FIA(fia_database_path) as db:
            db.clip_by_state(13, most_recent=True, eval_type="EXPMORT")  # Georgia

            result = mortality(db, totals=True, land_type="forest")

            # Check all variance-related columns
            assert "MORT_ACRE_SE" in result.columns
            assert "MORT_ACRE_VAR" in result.columns
            assert "MORT_TOTAL_SE" in result.columns
            assert "MORT_TOTAL_VAR" in result.columns

            mort_acre = result["MORT_ACRE"][0]
            mort_acre_se = result["MORT_ACRE_SE"][0]
            mort_total = result["MORT_TOTAL"][0]
            mort_total_se = result["MORT_TOTAL_SE"][0]

            # Calculate CV and sampling error percentages
            cv_acre = (mort_acre_se / mort_acre * 100) if mort_acre > 0 else 0
            cv_total = (mort_total_se / mort_total * 100) if mort_total > 0 else 0

            print(f"\nGeorgia Mortality Sampling Statistics:")
            print(f"  Per-acre estimate: {mort_acre:.2f} ± {mort_acre_se:.2f} (CV={cv_acre:.2f}%)")
            print(f"  Total estimate: {mort_total:,.0f} ± {mort_total_se:,.0f} (CV={cv_total:.2f}%)")

            # Validate reasonable CVs (typically 2-20% for state-level estimates)
            assert 0.5 <= cv_acre <= 30.0, f"Per-acre CV {cv_acre:.2f}% outside reasonable range"
            assert 0.5 <= cv_total <= 30.0, f"Total CV {cv_total:.2f}% outside reasonable range"

            # CVs should be similar for per-acre and total
            assert abs(cv_acre - cv_total) < 5.0, f"CV mismatch between per-acre ({cv_acre:.2f}%) and total ({cv_total:.2f}%)"


# Helper function for getting mortality evaluation info
def get_mortality_evaluation_info(db_path: str, state_code: int) -> dict:
    """
    Get information about available mortality evaluations for a state.

    Parameters
    ----------
    db_path : str
        Path to FIA database
    state_code : int
        FIA state code

    Returns
    -------
    dict
        Dictionary with evaluation information
    """
    import duckdb

    with duckdb.connect(db_path, read_only=True) as conn:
        query = """
        SELECT
            pe.EVALID,
            pe.EVAL_DESCR,
            pet.EVAL_TYP,
            pe.START_INVYR,
            pe.END_INVYR,
            COUNT(DISTINCT ppsa.PLT_CN) as plot_count
        FROM POP_EVAL pe
        JOIN POP_EVAL_TYP pet ON pe.CN = pet.EVAL_CN
        LEFT JOIN POP_PLOT_STRATUM_ASSGN ppsa ON pe.EVALID = ppsa.EVALID
        WHERE pe.STATECD = ? AND pet.EVAL_TYP = 'EXPMORT'
        GROUP BY pe.EVALID, pe.EVAL_DESCR, pet.EVAL_TYP, pe.START_INVYR, pe.END_INVYR
        ORDER BY pe.END_INVYR DESC
        """

        result = conn.execute(query, [state_code]).fetchall()

        if result:
            return {
                'evalid': result[0][0],
                'description': result[0][1],
                'eval_typ': result[0][2],
                'start_year': result[0][3],
                'end_year': result[0][4],
                'plot_count': result[0][5]
            }
        else:
            return None