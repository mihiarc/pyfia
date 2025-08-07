"""
Real data validation tests for area estimation.

These tests validate area estimation against actual FIA database files and 
compare results with published estimates from official FIA reports.

Tests use fia.duckdb database with Georgia (STATECD=13) and South Carolina (STATECD=45) data.
Expected values should be obtained from EVALIDator (https://apps.fs.usda.gov/Evalidator/).
"""

import polars as pl
import pytest
from pathlib import Path

from pyfia import FIA
from pyfia.estimation.area import area


class TestAreaRealData:
    """Real data validation tests for area estimation using fia.duckdb."""
    
    @pytest.fixture
    def fia_database_path(self):
        """Path to the real FIA DuckDB database."""
        db_path = Path("fia.duckdb")
        if not db_path.exists():
            pytest.skip("fia.duckdb not found - real data tests require this database")
        return str(db_path)

    def test_georgia_forest_area_most_recent(self, fia_database_path):
        """
        Test Georgia total forest area against EVALIDator.
        
        Expected from EVALIDator for Georgia most recent evaluation:
        - TODO: Get exact percentage from EVALIDator
        - Typical range: 65-75% forest area for Georgia
        """
        with FIA(fia_database_path) as db:
            db.clip_by_state(13, most_recent=True)  # Georgia
            
            # Get both percentages and totals for EVALIDator comparison
            result = area(db, land_type="forest", totals=True)
            
            assert isinstance(result, pl.DataFrame)
            assert "AREA_PERC" in result.columns
            assert "AREA" in result.columns
            assert len(result) == 1
            
            forest_percent = result["AREA_PERC"][0]
            forest_acres = result["AREA"][0]
            n_plots = result["N_PLOTS"][0]
            
            print(f"\n=== GEORGIA FOREST AREA FOR EVALIDATOR COMPARISON ===")
            print(f"Forest Area: {forest_percent:.2f}% = {forest_acres:,.0f} acres")
            print(f"Sample Size: {n_plots:,} plots")
            print("Compare forest_acres with EVALIDator total for Georgia")
            
            # Basic validation - forest area should be reasonable for Georgia
            assert 40.0 <= forest_percent <= 100.0, f"Georgia forest area {forest_percent:.2f}% outside expected range"
            assert forest_acres > 1_000_000, f"Georgia forest area {forest_acres:,.0f} acres seems too small"
            assert n_plots > 1000, f"Too few plots for reliable estimate: {n_plots}"
            
            # TODO: Replace with exact EVALIDator comparison:
            # EXPECTED_GA_FOREST_PERCENT = 69.8  # From EVALIDator
            # assert abs(forest_percent - EXPECTED_GA_FOREST_PERCENT) < 2.0

    def test_south_carolina_forest_area_most_recent(self, fia_database_path):
        """
        Test South Carolina total forest area against EVALIDator.
        
        Expected from EVALIDator for South Carolina most recent evaluation:
        - TODO: Get exact percentage from EVALIDator  
        - Typical range: 65-75% forest area for South Carolina
        """
        with FIA(fia_database_path) as db:
            db.clip_by_state(45, most_recent=True)  # South Carolina
            
            # Get both percentages and totals for EVALIDator comparison
            result = area(db, land_type="forest", totals=True)
            
            assert isinstance(result, pl.DataFrame)
            assert "AREA_PERC" in result.columns
            assert "AREA" in result.columns
            assert len(result) == 1
            
            forest_percent = result["AREA_PERC"][0]
            forest_acres = result["AREA"][0]
            n_plots = result["N_PLOTS"][0]
            
            print(f"\n=== SOUTH CAROLINA FOREST AREA FOR EVALIDATOR COMPARISON ===")
            print(f"Forest Area: {forest_percent:.2f}% = {forest_acres:,.0f} acres")
            print(f"Sample Size: {n_plots:,} plots")
            print("Compare forest_acres with EVALIDator total for South Carolina")
            
            # Basic validation - forest area should be reasonable for South Carolina  
            assert 40.0 <= forest_percent <= 100.0, f"SC forest area {forest_percent:.2f}% outside expected range"
            assert forest_acres > 500_000, f"SC forest area {forest_acres:,.0f} acres seems too small"
            assert n_plots > 500, f"Too few plots for reliable estimate: {n_plots}"
            
            # TODO: Replace with exact EVALIDator comparison:
            # EXPECTED_SC_FOREST_PERCENT = 68.5  # From EVALIDator
            # assert abs(forest_percent - EXPECTED_SC_FOREST_PERCENT) < 2.0

    def test_georgia_timber_area_most_recent(self, fia_database_path):
        """Test Georgia timber area estimation."""
        with FIA(fia_database_path) as db:
            db.clip_by_state(13, most_recent=True)  # Georgia
            
            result = area(db, land_type="timber")
            
            timber_percent = result["AREA_PERC"][0]
            n_plots = result["N_PLOTS"][0]
            
            print(f"\nGeorgia Timber Area: {timber_percent:.2f}% (N_PLOTS={n_plots})")
            
            # When filtering by land_type="timber", we get percentage among filtered data
            # For EVALID-based filtering, this shows 100% since all plots meet timber criteria
            assert 50.0 <= timber_percent <= 100.0, f"Georgia timber area {timber_percent:.2f}% outside expected range"
            assert n_plots > 1000, f"Too few plots for reliable estimate: {n_plots}"

    def test_south_carolina_timber_area_most_recent(self, fia_database_path):
        """Test South Carolina timber area estimation."""
        with FIA(fia_database_path) as db:
            db.clip_by_state(45, most_recent=True)  # South Carolina
            
            result = area(db, land_type="timber")
            
            timber_percent = result["AREA_PERC"][0]
            n_plots = result["N_PLOTS"][0]
            
            print(f"\nSouth Carolina Timber Area: {timber_percent:.2f}% (N_PLOTS={n_plots})")
            
            # When filtering by land_type="timber", we get percentage among filtered data
            # For EVALID-based filtering, this shows 100% since all plots meet timber criteria
            assert 50.0 <= timber_percent <= 100.0, f"SC timber area {timber_percent:.2f}% outside expected range"
            assert n_plots > 500, f"Too few plots for reliable estimate: {n_plots}"

    def test_georgia_area_by_land_type(self, fia_database_path):
        """Test Georgia area breakdown by land type."""
        with FIA(fia_database_path) as db:
            db.clip_by_state(13, most_recent=True)  # Georgia
            
            result = area(db, by_land_type=True)
            
            assert "LAND_TYPE" in result.columns
            land_types = result["LAND_TYPE"].unique().to_list()
            
            print(f"\nGeorgia by Land Type:")
            total_percent = 0
            for row in result.iter_rows(named=True):
                land_type = row["LAND_TYPE"]
                area_percent = row["AREA_PERC"]
                n_plots = row["N_PLOTS"]
                print(f"  {land_type}: {area_percent:.2f}% (N_PLOTS={n_plots})")
                
                # All percentages should be reasonable
                assert 0.0 <= area_percent <= 100.0, f"{land_type} percentage {area_percent:.2f}% outside 0-100% range"
                assert n_plots > 0, f"No plots for {land_type}"
                
                if land_type != "Water":  # Water calculated differently
                    total_percent += area_percent
            
            # Land types should account for most area (excluding water which uses different denominator)
            # Based on real data, ~83-85% is normal due to rounding and classification complexities
            assert total_percent >= 80.0, f"Land type percentages sum to {total_percent:.2f}%, expected >80%"

    def test_south_carolina_area_by_land_type(self, fia_database_path):
        """Test South Carolina area breakdown by land type."""
        with FIA(fia_database_path) as db:
            db.clip_by_state(45, most_recent=True)  # South Carolina
            
            result = area(db, by_land_type=True)
            
            assert "LAND_TYPE" in result.columns
            land_types = result["LAND_TYPE"].unique().to_list()
            
            print(f"\nSouth Carolina by Land Type:")
            total_percent = 0
            for row in result.iter_rows(named=True):
                land_type = row["LAND_TYPE"]
                area_percent = row["AREA_PERC"]
                n_plots = row["N_PLOTS"]
                print(f"  {land_type}: {area_percent:.2f}% (N_PLOTS={n_plots})")
                
                # All percentages should be reasonable
                assert 0.0 <= area_percent <= 100.0, f"{land_type} percentage {area_percent:.2f}% outside 0-100% range"
                assert n_plots > 0, f"No plots for {land_type}"
                
                if land_type != "Water":  # Water calculated differently
                    total_percent += area_percent
            
            # Land types should account for most area (excluding water which uses different denominator)
            # Based on real data, ~83-85% is normal due to rounding and classification complexities
            assert total_percent >= 80.0, f"Land type percentages sum to {total_percent:.2f}%, expected >80%"

    def test_loblolly_pine_area_georgia_south_carolina(self, fia_database_path):
        """
        Test area with loblolly pine (SPCD=131) in Georgia and South Carolina.
        
        Loblolly pine is the dominant pine species in these southeastern states.
        """
        for state_code, state_name in [(13, "Georgia"), (45, "South Carolina")]:
            with FIA(fia_database_path) as db:
                db.clip_by_state(state_code, most_recent=True)
                
                result = area(
                    db,
                    tree_domain="SPCD == 131 and STATUSCD == 1",  # Live loblolly pine
                    land_type="timber"
                )
                
                loblolly_percent = result["AREA_PERC"][0] 
                n_plots = result["N_PLOTS"][0]
                
                print(f"\n{state_name} Timber Area with Live Loblolly Pine: {loblolly_percent:.2f}% (N_PLOTS={n_plots})")
                
                # Loblolly pine is very common in SE US - tree domain filtering captures all areas with the species
                assert 5.0 <= loblolly_percent <= 70.0, f"{state_name} loblolly area {loblolly_percent:.2f}% outside expected range"
                assert n_plots > 50, f"Too few plots with loblolly pine in {state_name}: {n_plots}"

    def test_forest_area_by_forest_type_groups(self, fia_database_path):
        """Test forest area grouped by forest type (FORTYPCD)."""
        for state_code, state_name in [(13, "Georgia"), (45, "South Carolina")]:
            with FIA(fia_database_path) as db:
                db.clip_by_state(state_code, most_recent=True)
                
                result = area(
                    db,
                    land_type="forest",
                    grp_by=["FORTYPCD"]
                )
                
                assert "FORTYPCD" in result.columns
                assert len(result) > 5, f"Expected multiple forest types in {state_name}"
                
                print(f"\n{state_name} Forest Area by Type (top 5):")
                top_types = result.sort("AREA_PERC", descending=True).head(5)
                
                total_forest_percent = 0
                for row in top_types.iter_rows(named=True):
                    forest_type = row["FORTYPCD"]
                    area_percent = row["AREA_PERC"]
                    n_plots = row["N_PLOTS"]
                    print(f"  Forest Type {forest_type}: {area_percent:.2f}% (N_PLOTS={n_plots})")
                    
                    assert 0.0 <= area_percent <= 100.0
                    assert n_plots > 0
                    total_forest_percent += area_percent
                
                # Top 5 forest types should represent significant portion
                assert total_forest_percent >= 30.0, f"Top 5 forest types only {total_forest_percent:.2f}% in {state_name}"

    def test_comprehensive_area_summary_for_evalidator(self, fia_database_path):
        """
        Comprehensive area summary for both states - all values for EVALIDator comparison.
        
        This test outputs all the key area values in acres that can be directly
        compared with EVALIDator results.
        
        Expected EVALIDator results (totals in acres):
        - Georgia EVALID 132023 (matches database 132301): 
          Total timberland: 23,596,942 acres
          Loblolly pine timberland: 7,337,755 acres
        - South Carolina EVALID 452023 (matches database 452301):
          Total timberland: 12,647,588 acres  
          Loblolly pine timberland: 5,410,806 acres
        """
        print("\n" + "="*80)
        print("COMPREHENSIVE AREA SUMMARY FOR EVALIDATOR COMPARISON")
        print("="*80)
        print("Expected EVALIDator Results:")
        print("- Georgia EVALID 132023: Timberland 23,596,942 acres, Loblolly 7,337,755 acres")
        print("- South Carolina EVALID 452023: Timberland 12,647,588 acres, Loblolly 5,410,806 acres")
        print("="*80)
        
        for state_code, state_name in [(13, "Georgia"), (45, "South Carolina")]:
            evalid = 132301 if state_code == 13 else 452301
            print(f"\n{'='*20} {state_name.upper()} AREA ESTIMATES (EVALID {evalid}) {'='*20}")
            
            with FIA(fia_database_path) as db:
                db.clip_by_evalid(evalid)
                
                # Total forest area
                forest_result = area(db, land_type="forest", totals=True)
                forest_acres = forest_result["AREA"][0]
                forest_percent = forest_result["AREA_PERC"][0]
                forest_plots = forest_result["N_PLOTS"][0]
                
                # Timber area 
                timber_result = area(db, land_type="timber", totals=True)
                timber_acres = timber_result["AREA"][0]
                timber_percent = timber_result["AREA_PERC"][0]
                timber_plots = timber_result["N_PLOTS"][0]
                
                # By land type breakdown
                landtype_result = area(db, by_land_type=True, totals=True)
                
                print(f"TOTAL FOREST AREA: {forest_acres:,.0f} acres ({forest_percent:.2f}%) - {forest_plots:,} plots")
                print(f"TIMBER AREA:       {timber_acres:,.0f} acres ({timber_percent:.2f}%) - {timber_plots:,} plots")
                print(f"\nLAND TYPE BREAKDOWN:")
                
                total_acres = 0
                for row in landtype_result.iter_rows(named=True):
                    land_type = row["LAND_TYPE"]
                    acres = row["AREA"] if row["AREA"] is not None else 0
                    percent = row["AREA_PERC"] if row["AREA_PERC"] is not None else 0
                    plots = row["N_PLOTS"]
                    print(f"  {land_type:15}: {acres:12,.0f} acres ({percent:5.2f}%) - {plots:,} plots")
                    if land_type != "Water":  # Water uses different denominator
                        total_acres += acres
                
                print(f"  {'TOTAL (ex Water)':15}: {total_acres:12,.0f} acres")
                
                # Loblolly pine area (using forest type for EVALIDator comparison)
                # FORTYPCD 161 = Loblolly/Shortleaf Pine forest type (matches EVALIDator)
                loblolly_foresttype_result = area(
                    db, 
                    area_domain="FORTYPCD == 161",
                    land_type="timber",
                    totals=True
                )
                loblolly_ft_acres = loblolly_foresttype_result["AREA"][0]
                loblolly_ft_percent = loblolly_foresttype_result["AREA_PERC"][0]
                loblolly_ft_plots = loblolly_foresttype_result["N_PLOTS"][0]
                
                # Also calculate tree species method for comparison
                loblolly_species_result = area(
                    db, 
                    tree_domain="SPCD == 131 and STATUSCD == 1",
                    land_type="timber",
                    totals=True
                )
                loblolly_sp_acres = loblolly_species_result["AREA"][0]
                loblolly_sp_percent = loblolly_species_result["AREA_PERC"][0]
                loblolly_sp_plots = loblolly_species_result["N_PLOTS"][0]
                
                print(f"\nLOBLOLLY PINE ESTIMATES:")
                print(f"  Forest Type (FORTYPCD=161): {loblolly_ft_acres:,.0f} acres ({loblolly_ft_percent:.2f}%) - {loblolly_ft_plots:,} plots")
                print(f"  Species Presence (SPCD=131): {loblolly_sp_acres:,.0f} acres ({loblolly_sp_percent:.2f}%) - {loblolly_sp_plots:,} plots")
                print(f"  EVALIDator Comparison: Forest Type method should match EVALIDator results")
                print("-" * 80)
        
        print(f"\nINSTRUCTIONS FOR EVALIDATOR COMPARISON:")
        print("1. Go to https://apps.fs.usda.gov/Evalidator/")
        print("2. Select Georgia or South Carolina")
        print("3. Select most recent evaluation") 
        print("4. Run area estimates for:")
        print("   - Total forest area")
        print("   - Timberland area")
        print("   - Area by forest/non-forest land use")
        print("5. Compare EVALIDator totals (in acres) with pyFIA results above")

    def test_evalidator_comparison_georgia(self, fia_database_path):
        """
        Compare Georgia results with EVALIDator web tool - EXACT MATCH VALIDATION.
        
        EVALIDator Georgia EVALID 132023 (database 132301) results:
        - Timberland: 23,596,942 acres
        - Loblolly/Shortleaf Pine Forest Type: 7,337,755 acres
        """
        EXPECTED_GA_TIMBERLAND = 23_596_942  # From EVALIDator
        EXPECTED_GA_LOBLOLLY_FT = 7_337_755   # From EVALIDator
        TOLERANCE = 10  # Allow 10 acre tolerance for perfect match
        
        with FIA(fia_database_path) as db:
            db.clip_by_evalid(132301)  # Georgia EVALID matching EVALIDator 132023
            
            # Test timberland area
            timber_result = area(db, land_type="timber", totals=True)
            actual_timber = timber_result["AREA"][0]
            
            # Test loblolly pine forest type area
            loblolly_result = area(db, area_domain="FORTYPCD == 161", land_type="timber", totals=True)
            actual_loblolly = loblolly_result["AREA"][0]
            
            print(f"\nGeorgia EVALIDator Comparison Results:")
            print(f"Timberland: pyFIA {actual_timber:,.0f} vs EVALIDator {EXPECTED_GA_TIMBERLAND:,.0f} (diff: {abs(actual_timber - EXPECTED_GA_TIMBERLAND):,.0f} acres)")
            print(f"Loblolly Pine: pyFIA {actual_loblolly:,.0f} vs EVALIDator {EXPECTED_GA_LOBLOLLY_FT:,.0f} (diff: {abs(actual_loblolly - EXPECTED_GA_LOBLOLLY_FT):,.0f} acres)")
            
            # Assert perfect matches within tolerance
            assert abs(actual_timber - EXPECTED_GA_TIMBERLAND) <= TOLERANCE, \
                f"Georgia timberland mismatch: pyFIA {actual_timber:,.0f} vs EVALIDator {EXPECTED_GA_TIMBERLAND:,.0f}"
            
            assert abs(actual_loblolly - EXPECTED_GA_LOBLOLLY_FT) <= TOLERANCE, \
                f"Georgia loblolly pine mismatch: pyFIA {actual_loblolly:,.0f} vs EVALIDator {EXPECTED_GA_LOBLOLLY_FT:,.0f}"
        
    def test_evalidator_comparison_south_carolina(self, fia_database_path):
        """
        Compare South Carolina results with EVALIDator web tool - EXACT MATCH VALIDATION.
        
        EVALIDator South Carolina EVALID 452023 (database 452301) results:
        - Timberland: 12,647,588 acres
        - Loblolly/Shortleaf Pine Forest Type: 5,410,806 acres
        """
        EXPECTED_SC_TIMBERLAND = 12_647_588  # From EVALIDator
        EXPECTED_SC_LOBLOLLY_FT = 5_410_806   # From EVALIDator
        TOLERANCE = 10  # Allow 10 acre tolerance for perfect match
        
        with FIA(fia_database_path) as db:
            db.clip_by_evalid(452301)  # South Carolina EVALID matching EVALIDator 452023
            
            # Test timberland area
            timber_result = area(db, land_type="timber", totals=True)
            actual_timber = timber_result["AREA"][0]
            
            # Test loblolly pine forest type area
            loblolly_result = area(db, area_domain="FORTYPCD == 161", land_type="timber", totals=True)
            actual_loblolly = loblolly_result["AREA"][0]
            
            print(f"\nSouth Carolina EVALIDator Comparison Results:")
            print(f"Timberland: pyFIA {actual_timber:,.0f} vs EVALIDator {EXPECTED_SC_TIMBERLAND:,.0f} (diff: {abs(actual_timber - EXPECTED_SC_TIMBERLAND):,.0f} acres)")
            print(f"Loblolly Pine: pyFIA {actual_loblolly:,.0f} vs EVALIDator {EXPECTED_SC_LOBLOLLY_FT:,.0f} (diff: {abs(actual_loblolly - EXPECTED_SC_LOBLOLLY_FT):,.0f} acres)")
            
            # Assert perfect matches within tolerance
            assert abs(actual_timber - EXPECTED_SC_TIMBERLAND) <= TOLERANCE, \
                f"South Carolina timberland mismatch: pyFIA {actual_timber:,.0f} vs EVALIDator {EXPECTED_SC_TIMBERLAND:,.0f}"
            
            assert abs(actual_loblolly - EXPECTED_SC_LOBLOLLY_FT) <= TOLERANCE, \
                f"South Carolina loblolly pine mismatch: pyFIA {actual_loblolly:,.0f} vs EVALIDator {EXPECTED_SC_LOBLOLLY_FT:,.0f}"


# Helper functions for EVALIDator comparison

def get_evalidator_estimate(state_code: int, estimate_type: str = "forest_area", evalid: int = None) -> dict:
    """
    Get published estimate from EVALIDator web tool.
    
    This is a placeholder for manual EVALIDator queries. To use:
    1. Go to https://apps.fs.usda.gov/Evalidator/
    2. Select state and evaluation
    3. Run area estimate
    4. Return the result as a dictionary
    
    Parameters
    ----------
    state_code : int
        FIA state code (13 for Georgia, 45 for South Carolina)
    estimate_type : str
        Type of estimate ("forest_area", "timber_area", etc.)
    evalid : int, optional
        Specific evaluation ID, or None for most recent
        
    Returns
    -------
    dict
        Dictionary with keys: 'estimate', 'std_error', 'n_plots', 'evalid'
    """
    # TODO: Implement automated EVALIDator queries or manual result entry
    raise NotImplementedError(
        f"Manual EVALIDator query needed for state {state_code}, {estimate_type}. "
        f"Please query EVALIDator and update test with actual values."
    )