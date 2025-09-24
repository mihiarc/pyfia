"""
Test mortality estimation with southern states test database.

Uses data/test_southern.duckdb which contains Georgia and South Carolina data
with GRM tables for mortality estimation.
"""

import polars as pl
import pytest
from pathlib import Path

from pyfia import FIA
from pyfia.estimation import mortality


class TestMortalitySouthern:
    """Test mortality estimation using southern states test database."""

    @pytest.fixture
    def db_path(self):
        """Path to test southern database."""
        path = Path("data/test_southern.duckdb")
        if not path.exists():
            pytest.skip(f"Test database not found at {path}")
        return str(path)

    def test_georgia_mortality_basic(self, db_path):
        """Test basic mortality estimation for Georgia."""
        with FIA(db_path) as db:
            # Filter to Georgia
            db.clip_by_state(13)

            # Get basic mortality estimates
            result = mortality(db, land_type="forest")

            print(f"\n=== Georgia Forest Mortality ===")
            print(f"Mortality per acre: {result['MORT_ACRE'][0]:.2f} cu ft/acre/year")
            print(f"Standard error: {result['MORT_ACRE_SE'][0]:.2f}")
            print(f"Non-zero plots: {result['N_PLOTS'][0]}")

            assert result["MORT_ACRE"][0] > 0, "Expected positive mortality"
            assert result["N_PLOTS"][0] > 0, "Expected some plots with mortality"

    def test_south_carolina_mortality_basic(self, db_path):
        """Test basic mortality estimation for South Carolina."""
        with FIA(db_path) as db:
            # Filter to South Carolina
            db.clip_by_state(45)

            # Get basic mortality estimates
            result = mortality(db, land_type="forest")

            print(f"\n=== South Carolina Forest Mortality ===")
            print(f"Mortality per acre: {result['MORT_ACRE'][0]:.2f} cu ft/acre/year")
            print(f"Standard error: {result['MORT_ACRE_SE'][0]:.2f}")
            print(f"Non-zero plots: {result['N_PLOTS'][0]}")

            assert result["MORT_ACRE"][0] > 0, "Expected positive mortality"
            assert result["N_PLOTS"][0] > 0, "Expected some plots with mortality"

    def test_mortality_by_species(self, db_path):
        """Test mortality by species grouping."""
        with FIA(db_path) as db:
            db.clip_by_state(13)  # Georgia

            result = mortality(db, by_species=True, land_type="forest")

            print(f"\n=== Georgia Mortality by Species (top 5) ===")
            top_species = result.sort("MORT_ACRE", descending=True).head(5)
            for row in top_species.iter_rows(named=True):
                print(f"Species {row['SPCD']}: {row['MORT_ACRE']:.2f} cu ft/acre/year (N={row['N_PLOTS']} plots)")

            assert len(result) > 0, "Expected at least some species with mortality"
            assert "SPCD" in result.columns, "Should have species code column"

    def test_mortality_different_measures(self, db_path):
        """Test mortality with different measurement units."""
        with FIA(db_path) as db:
            db.clip_by_state(13)  # Georgia

            measures = {
                "volume": "cu ft/acre/year",
                "basal_area": "sq ft/acre/year",
                "biomass": "tons/acre/year",
                "tpa": "trees/acre/year"
            }

            print(f"\n=== Georgia Mortality by Measure ===")
            for measure, units in measures.items():
                result = mortality(db, measure=measure, land_type="forest")
                mort_acre = result["MORT_ACRE"][0]
                print(f"{measure.title():12}: {mort_acre:.2f} {units}")

                assert mort_acre >= 0, f"Negative mortality for {measure}"

    def test_mortality_with_totals(self, db_path):
        """Test mortality with total expansion."""
        with FIA(db_path) as db:
            db.clip_by_state(13)  # Georgia

            result = mortality(db, totals=True, land_type="forest")

            print(f"\n=== Georgia Total Forest Mortality ===")
            print(f"Per acre: {result['MORT_ACRE'][0]:.2f} cu ft/acre/year")
            print(f"Total: {result['MORT_TOTAL'][0]:,.0f} cu ft/year")
            print(f"Total SE: {result['MORT_TOTAL_SE'][0]:,.0f} cu ft/year")
            print(f"Sampling Error %: {result['MORT_TOTAL_SE'][0]/result['MORT_TOTAL'][0]*100:.2f}%")

            assert "MORT_TOTAL" in result.columns, "Should have total column"
            assert "MORT_TOTAL_SE" in result.columns, "Should have total SE column"
            assert result["MORT_TOTAL"][0] > 0, "Expected positive total mortality"

    def test_mortality_sawlog_sawtimber(self, db_path):
        """Test sawlog volume mortality of sawtimber trees (EVALIDator comparison)."""
        with FIA(db_path) as db:
            db.clip_by_state(13)  # Georgia

            # Test sawlog volume of sawtimber trees
            result = mortality(
                db,
                measure="sawlog",
                tree_type="sawtimber",
                totals=True
            )

            print(f"\n=== Georgia Sawlog Mortality of Sawtimber ===")
            print(f"Per acre: {result['MORT_ACRE'][0]:.2f} cu ft/acre/year")
            if "MORT_TOTAL" in result.columns:
                print(f"Total: {result['MORT_TOTAL'][0]:,.0f} cu ft/year")
                print(f"Total SE: {result['MORT_TOTAL_SE'][0]:,.0f} cu ft/year")
                print(f"Sampling Error %: {result['MORT_TOTAL_SE'][0]/result['MORT_TOTAL'][0]*100:.2f}%")
            print(f"Non-zero plots: {result['N_PLOTS'][0]}")

            assert result["MORT_ACRE"][0] >= 0, "Expected non-negative sawlog mortality"
            assert result["N_PLOTS"][0] >= 0, "Expected non-negative plot count"

    def test_mortality_by_forest_type(self, db_path):
        """Test mortality grouped by forest type."""
        with FIA(db_path) as db:
            db.clip_by_state(13)  # Georgia

            result = mortality(db, grp_by=["FORTYPCD"], land_type="forest")

            print(f"\n=== Georgia Mortality by Forest Type (top 5) ===")
            if len(result) > 0:
                top_types = result.sort("MORT_ACRE", descending=True).head(5)
                for row in top_types.iter_rows(named=True):
                    print(f"Forest Type {row['FORTYPCD']}: {row['MORT_ACRE']:.2f} cu ft/acre/year (N={row['N_PLOTS']} plots)")

                assert "FORTYPCD" in result.columns, "Should have forest type column"
            else:
                print("No mortality data by forest type found")

    def test_mortality_variance_columns(self, db_path):
        """Test that all variance columns are present and reasonable."""
        with FIA(db_path) as db:
            db.clip_by_state(13)  # Georgia

            result = mortality(db, totals=True, land_type="forest")

            # Check required columns - mortality may not return VAR columns
            required_cols = [
                "MORT_ACRE", "MORT_ACRE_SE",
                "MORT_TOTAL", "MORT_TOTAL_SE",
                "N_PLOTS"
            ]

            print(f"\n=== Available columns: {result.columns}")

            for col in required_cols:
                assert col in result.columns, f"Missing required column: {col}"

            # Validate SE and CV
            mort_acre = result["MORT_ACRE"][0]
            mort_acre_se = result["MORT_ACRE_SE"][0]
            mort_total = result["MORT_TOTAL"][0]
            mort_total_se = result["MORT_TOTAL_SE"][0]

            # CV should be reasonable (typically 2-50% for mortality)
            cv_acre = (mort_acre_se / mort_acre * 100) if mort_acre > 0 else 0
            cv_total = (mort_total_se / mort_total * 100) if mort_total > 0 else 0

            print(f"\n=== Variance Statistics ===")
            print(f"Per-acre Estimate: {mort_acre:.2f} ± {mort_acre_se:.2f} (CV={cv_acre:.2f}%)")
            print(f"Total Estimate: {mort_total:,.0f} ± {mort_total_se:,.0f} (CV={cv_total:.2f}%)")

            assert 0.1 <= cv_acre <= 100, f"Per-acre CV {cv_acre:.2f}% outside reasonable range"
            assert 0.1 <= cv_total <= 100, f"Total CV {cv_total:.2f}% outside reasonable range"

            # CVs should be similar for per-acre and total
            assert abs(cv_acre - cv_total) < 5.0, f"CV mismatch between per-acre ({cv_acre:.2f}%) and total ({cv_total:.2f}%)"

    def test_mortality_comprehensive_output(self, db_path):
        """Generate comprehensive mortality output for both states."""
        print("\n" + "=" * 80)
        print("COMPREHENSIVE MORTALITY SUMMARY - SOUTHERN STATES TEST DATABASE")
        print("=" * 80)

        for state_code, state_name in [(13, "Georgia"), (45, "South Carolina")]:
            print(f"\n{'='*20} {state_name} MORTALITY {'='*20}")

            with FIA(db_path) as db:
                db.clip_by_state(state_code)

                # Basic forest mortality
                forest_result = mortality(db, land_type="forest", totals=True)
                print(f"\nFOREST MORTALITY (Volume):")
                print(f"  Per acre: {forest_result['MORT_ACRE'][0]:.2f} cu ft/acre/year")
                if "MORT_TOTAL" in forest_result.columns:
                    total = forest_result['MORT_TOTAL'][0]
                    se = forest_result['MORT_TOTAL_SE'][0]
                    print(f"  Total: {total:,.0f} cu ft/year")
                    print(f"  Sampling Error: {se/total*100:.2f}%")
                print(f"  Non-zero plots: {forest_result['N_PLOTS'][0]}")

                # Different measures
                print(f"\nMORTALITY BY MEASURE:")
                for measure in ["volume", "basal_area", "biomass", "tpa"]:
                    result = mortality(db, measure=measure, land_type="forest")
                    units = {
                        "volume": "cu ft/acre/year",
                        "basal_area": "sq ft/acre/year",
                        "biomass": "tons/acre/year",
                        "tpa": "trees/acre/year"
                    }[measure]
                    print(f"  {measure.title():12}: {result['MORT_ACRE'][0]:10.2f} {units}")

                # Top species
                species_result = mortality(db, by_species=True, land_type="forest")
                if len(species_result) > 0:
                    print(f"\nTOP MORTALITY BY SPECIES:")
                    top_species = species_result.sort("MORT_ACRE", descending=True).head(3)
                    for row in top_species.iter_rows(named=True):
                        print(f"  Species {row['SPCD']}: {row['MORT_ACRE']:.2f} cu ft/acre/year")

        print("\n" + "=" * 80)