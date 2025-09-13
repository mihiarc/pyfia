"""
Integration tests for mortality() function using real FIA data.

These tests use the actual Georgia FIA database to verify that the mortality
estimator works correctly with real GRM tables and produces valid results.
"""

import pytest
import polars as pl
from pathlib import Path
from pyfia import FIA, mortality
import duckdb


# Skip if real database not available
GEORGIA_DB = Path("data/georgia.duckdb")
pytestmark = pytest.mark.skipif(
    not GEORGIA_DB.exists(),
    reason="Georgia database not available for integration testing"
)


class TestMortalityIntegrationGeorgia:
    """Integration tests using real Georgia FIA database."""
    
    def test_mortality_real_data_basic(self):
        """Test basic mortality calculation with real Georgia data."""
        with FIA(str(GEORGIA_DB)) as db:
            # Use EVALID 132009 which has mortality data
            db.clip_by_evalid([132009])
            
            results = mortality(db, measure="volume", land_type="forest")
            
            assert isinstance(results, pl.DataFrame)
            assert len(results) > 0
            
            # Check that we get real values
            assert results["MORT_ACRE"][0] > 0
            assert results["MORT_TOTAL"][0] > 0
            assert results["N_DEAD_TREES"][0] > 0
            
            # Georgia should have thousands of mortality records
            assert results["N_DEAD_TREES"][0] > 1000
            
            # Verify reasonable mortality values (cu ft per acre)
            # Typical range is 10-50 cu ft/acre for annual mortality
            assert 1 < results["MORT_ACRE"][0] < 100
    
    def test_mortality_real_data_by_species(self):
        """Test mortality by species with real data."""
        with FIA(str(GEORGIA_DB)) as db:
            db.clip_by_evalid([132009])
            
            results = mortality(db, by_species=True, measure="volume")
            
            assert isinstance(results, pl.DataFrame)
            assert "SPCD" in results.columns
            assert len(results) > 5  # Georgia should have many species
            
            # Check top mortality species
            top_species = results.sort("MORT_ACRE", descending=True).head(5)
            
            # All top species should have positive mortality
            for row in top_species.iter_rows(named=True):
                assert row["MORT_ACRE"] > 0
                assert row["SPCD"] > 0
    
    def test_mortality_real_data_biomass(self):
        """Test biomass mortality calculation with real data."""
        with FIA(str(GEORGIA_DB)) as db:
            db.clip_by_evalid([132009])
            
            results = mortality(db, measure="biomass", land_type="forest")
            
            assert isinstance(results, pl.DataFrame)
            assert results["MEASURE"][0] == "BIOMASS"
            
            # Biomass mortality should be positive (tons per acre)
            assert results["MORT_ACRE"][0] > 0
            
            # Typical biomass mortality is 0.5-2 tons/acre/year
            assert 0.1 < results["MORT_ACRE"][0] < 10
    
    def test_mortality_real_data_count(self):
        """Test tree count mortality with real data."""
        with FIA(str(GEORGIA_DB)) as db:
            db.clip_by_evalid([132009])
            
            results = mortality(db, measure="count", land_type="forest")
            
            assert isinstance(results, pl.DataFrame)
            assert results["MEASURE"][0] == "COUNT"
            
            # Trees per acre mortality
            assert results["MORT_ACRE"][0] > 0
            
            # Typical mortality is 1-10 trees per acre per year
            assert 0.1 < results["MORT_ACRE"][0] < 50
    
    def test_mortality_real_data_by_forest_type(self):
        """Test mortality grouped by forest type with real data."""
        with FIA(str(GEORGIA_DB)) as db:
            db.clip_by_evalid([132009])
            
            results = mortality(db, grp_by="FORTYPCD", measure="volume")
            
            assert isinstance(results, pl.DataFrame)
            assert "FORTYPCD" in results.columns
            assert len(results) > 3  # Multiple forest types
            
            # Each forest type should have valid mortality
            for row in results.iter_rows(named=True):
                assert row["MORT_ACRE"] >= 0  # Some may have zero mortality
                assert row["FORTYPCD"] >= 0
    
    def test_mortality_real_data_with_variance(self):
        """Test variance calculation with real data."""
        with FIA(str(GEORGIA_DB)) as db:
            db.clip_by_evalid([132009])
            
            results = mortality(db, variance=True, measure="volume")
            
            assert isinstance(results, pl.DataFrame)
            assert "MORT_ACRE_SE" in results.columns
            assert "MORT_TOTAL_SE" in results.columns
            
            # Standard errors should be positive
            assert results["MORT_ACRE_SE"][0] > 0
            assert results["MORT_TOTAL_SE"][0] > 0
            
            # SE should be less than the estimate (reasonable CV)
            assert results["MORT_ACRE_SE"][0] < results["MORT_ACRE"][0]
    
    def test_mortality_real_data_timber_vs_forest(self):
        """Test mortality on timber vs forest land with real data."""
        with FIA(str(GEORGIA_DB)) as db:
            db.clip_by_evalid([132009])
            
            # Forest land mortality
            forest_results = mortality(db, land_type="forest", measure="volume")
            
            # Timber land mortality
            timber_results = mortality(db, land_type="timber", measure="volume")
            
            assert forest_results["LAND_TYPE"][0] == "FOREST"
            assert timber_results["LAND_TYPE"][0] == "TIMBER"
            
            # Both should have mortality
            assert forest_results["MORT_ACRE"][0] > 0
            assert timber_results["MORT_ACRE"][0] > 0
            
            # Forest typically includes more area than timber
            assert forest_results["AREA_TOTAL"][0] >= timber_results["AREA_TOTAL"][0]
    
    def test_mortality_real_data_comparison_with_sql(self):
        """Compare mortality results with direct SQL query."""
        with FIA(str(GEORGIA_DB)) as db:
            db.clip_by_evalid([132009])
            
            # Run mortality function
            func_results = mortality(db, measure="volume", land_type="forest")
            
            # Direct SQL query for comparison
            conn = duckdb.connect(str(GEORGIA_DB), read_only=True)
            sql_query = """
            SELECT 
                COUNT(DISTINCT tgc.TRE_CN) as tree_count,
                SUM(tgc.SUBP_TPAMORT_UNADJ_GS_FOREST * tgm.VOLCFNET) as total_mortality
            FROM TREE_GRM_COMPONENT tgc
            JOIN TREE_GRM_MIDPT tgm ON tgc.TRE_CN = tgm.TRE_CN
            JOIN POP_PLOT_STRATUM_ASSGN ppsa ON tgc.PLT_CN = ppsa.PLT_CN
            WHERE tgc.SUBP_COMPONENT_GS_FOREST LIKE 'MORTALITY%'
              AND tgc.SUBP_TPAMORT_UNADJ_GS_FOREST > 0
              AND ppsa.EVALID = 132009
            """
            
            sql_result = conn.execute(sql_query).fetchone()
            conn.close()
            
            # Function should process similar number of trees
            assert func_results["N_DEAD_TREES"][0] > 0
            assert sql_result[0] > 0
            
            # Results should be in same order of magnitude
            # (exact match not expected due to additional filtering/adjustments)
            assert func_results["N_DEAD_TREES"][0] > sql_result[0] * 0.5
            assert func_results["N_DEAD_TREES"][0] < sql_result[0] * 2.0


class TestMortalityIntegrationValidation:
    """Validate mortality estimates against known FIA values."""
    
    def test_mortality_reasonable_ranges(self):
        """Test that mortality values fall within reasonable ecological ranges."""
        with FIA(str(GEORGIA_DB)) as db:
            db.clip_by_evalid([132009])
            
            results = mortality(db, measure="volume", land_type="forest")
            
            # Annual mortality typically 0.5-2% of standing volume
            # For southeastern US forests, expect 10-50 cu ft/acre/year
            mort_per_acre = results["MORT_ACRE"][0]
            assert 5 < mort_per_acre < 100, f"Mortality {mort_per_acre} cu ft/acre outside expected range"
            
            # Total mortality for Georgia (millions of acres of forest)
            # Should be in millions of cubic feet
            total_mort = results["MORT_TOTAL"][0]
            assert total_mort > 1_000_000, f"Total mortality {total_mort} seems too low for Georgia"
    
    def test_mortality_species_distribution(self):
        """Test that species mortality distribution makes ecological sense."""
        with FIA(str(GEORGIA_DB)) as db:
            db.clip_by_evalid([132009])
            
            results = mortality(db, by_species=True, measure="volume")
            
            # Sort by mortality
            top_mortality = results.sort("MORT_ACRE", descending=True).head(10)
            
            # Common Georgia species should be in top mortality
            # (Pine species codes: 111, 121, 131, etc.)
            species_codes = top_mortality["SPCD"].to_list()
            
            # At least some pine species should be present
            pine_codes = [s for s in species_codes if 100 <= s <= 140]
            assert len(pine_codes) > 0, "No pine species in top mortality (unexpected for Georgia)"
            
            # Check mortality rates are reasonable for each species
            for row in top_mortality.iter_rows(named=True):
                # Species mortality should be positive but not extreme
                assert 0 < row["MORT_ACRE"] < 200
    
    def test_mortality_temporal_consistency(self):
        """Test that mortality estimates are temporally consistent."""
        with FIA(str(GEORGIA_DB)) as db:
            # Test multiple EVALIDs if available
            evalids_to_test = [132009, 132003, 132000]
            
            results_by_evalid = {}
            for evalid in evalids_to_test:
                try:
                    db_temp = FIA(str(GEORGIA_DB))
                    db_temp.clip_by_evalid([evalid])
                    results = mortality(db_temp, measure="volume")
                    if len(results) > 0 and results["N_DEAD_TREES"][0] > 0:
                        results_by_evalid[evalid] = results["MORT_ACRE"][0]
                except:
                    continue
            
            if len(results_by_evalid) > 1:
                # Mortality shouldn't vary wildly between evaluations
                values = list(results_by_evalid.values())
                max_mort = max(values)
                min_mort = min(values)
                
                # Should be within 3x of each other (allowing for disturbance events)
                assert max_mort < min_mort * 3, "Mortality varies too much between evaluations"


if __name__ == "__main__":
    # Run tests with verbose output
    pytest.main([__file__, "-v", "-s"])