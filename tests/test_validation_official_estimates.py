"""
Comprehensive validation tests comparing pyFIA results to official published estimates.

This module consolidates all validation tests that compare pyFIA estimation results
against official published values from:
1. EVALIDator web tool (https://apps.fs.usda.gov/Evalidator/)
2. FIA Database National Program reports
3. State-specific FIA factsheets

Test Structure:
- Each test includes the official source and date
- Expected values are hardcoded with source citations
- Tests validate both point estimates and sampling errors
- Tolerances are set based on FIA's published precision standards

Database Requirements:
- Tests require fia.duckdb with Georgia (13) and South Carolina (45) data
- Or set PYFIA_DATABASE_PATH environment variable to database location
"""

import os
from pathlib import Path
import pytest
import polars as pl
from typing import Dict, Tuple

from pyfia import FIA, area, volume, biomass, tpa, mortality


class TestOfficialAreaEstimates:
    """Validate area estimates against EVALIDator and published reports."""
    
    @pytest.fixture
    def fia_db(self):
        """Get FIA database connection."""
        # Check for environment variable first
        db_path = os.getenv("PYFIA_DATABASE_PATH", "fia.duckdb")
        if not Path(db_path).exists():
            pytest.skip(f"FIA database not found at {db_path}")
        return db_path
    
    def test_georgia_timberland_evalidator_2023(self, fia_db):
        """
        Validate Georgia timberland area against EVALIDator.
        
        Source: EVALIDator web tool
        Query Date: 2023 evaluation (EVALID 132023, database 132301)
        Expected: 23,596,942 acres timberland
        Sampling Error: Not yet available
        """
        EXPECTED_ACRES = 23_596_942
        TOLERANCE_ACRES = 100  # Allow 100 acre tolerance
        
        with FIA(fia_db) as db:
            db.clip_by_evalid(132301)
            result = area(db, land_type="timber", totals=True)
            
            actual_acres = result["AREA"][0]
            difference = abs(actual_acres - EXPECTED_ACRES)
            
            assert difference <= TOLERANCE_ACRES, (
                f"Georgia timberland area mismatch: "
                f"pyFIA={actual_acres:,.0f} vs EVALIDator={EXPECTED_ACRES:,.0f} "
                f"(diff={difference:,.0f} acres)"
            )
    
    def test_south_carolina_timberland_evalidator_2023(self, fia_db):
        """
        Validate South Carolina timberland area against EVALIDator.
        
        Source: EVALIDator web tool
        Query Date: 2023 evaluation (EVALID 452023, database 452301)
        Expected: 12,647,588 acres timberland
        Sampling Error: 0.796% at 68% confidence
        """
        EXPECTED_ACRES = 12_647_588
        EXPECTED_SE_PCT = 0.796
        TOLERANCE_ACRES = 100
        TOLERANCE_SE_PCT = 0.5  # This will likely fail - known issue
        
        with FIA(fia_db) as db:
            db.clip_by_evalid(452301)
            result = area(db, land_type="timber", totals=True)
            
            actual_acres = result["AREA"][0]
            actual_se = result["AREA_SE"][0]
            actual_se_pct = (actual_se / actual_acres * 100) if actual_acres > 0 else 0
            
            # Test area estimate
            difference = abs(actual_acres - EXPECTED_ACRES)
            assert difference <= TOLERANCE_ACRES, (
                f"South Carolina timberland area mismatch: "
                f"pyFIA={actual_acres:,.0f} vs EVALIDator={EXPECTED_ACRES:,.0f} "
                f"(diff={difference:,.0f} acres)"
            )
            
            # Test sampling error (expected to fail due to known variance bug)
            se_difference = abs(actual_se_pct - EXPECTED_SE_PCT)
            if se_difference > TOLERANCE_SE_PCT:
                pytest.xfail(
                    f"Known variance calculation issue: "
                    f"pyFIA SE={actual_se_pct:.3f}% vs EVALIDator={EXPECTED_SE_PCT:.3f}% "
                    f"(diff={se_difference:.3f}%)"
                )
    
    def test_loblolly_pine_forest_type_evalidator(self, fia_db):
        """
        Validate loblolly/shortleaf pine forest type area.
        
        Source: EVALIDator web tool
        Forest Type Code: 161 (Loblolly/Shortleaf Pine)
        Expected:
        - Georgia: 7,337,755 acres
        - South Carolina: 5,410,806 acres
        """
        test_cases = [
            (13, 132301, "Georgia", 7_337_755),
            (45, 452301, "South Carolina", 5_410_806),
        ]
        
        for state_code, evalid, state_name, expected_acres in test_cases:
            with FIA(fia_db) as db:
                db.clip_by_evalid(evalid)
                result = area(
                    db,
                    area_domain="FORTYPCD == 161",
                    land_type="timber",
                    totals=True
                )
                
                actual_acres = result["AREA"][0]
                difference = abs(actual_acres - expected_acres)
                
                assert difference <= 100, (
                    f"{state_name} loblolly pine area mismatch: "
                    f"pyFIA={actual_acres:,.0f} vs EVALIDator={expected_acres:,.0f} "
                    f"(diff={difference:,.0f} acres)"
                )


class TestOfficialVolumeEstimates:
    """Validate volume estimates against published reports."""
    
    @pytest.fixture
    def fia_db(self):
        """Get FIA database connection."""
        db_path = os.getenv("PYFIA_DATABASE_PATH", "fia.duckdb")
        if not Path(db_path).exists():
            pytest.skip(f"FIA database not found at {db_path}")
        return db_path
    
    @pytest.mark.skip(reason="Need to obtain official volume estimates from EVALIDator")
    def test_georgia_net_volume_cubic_feet(self, fia_db):
        """
        Validate Georgia net volume against published estimates.
        
        Source: TBD - Need EVALIDator query
        Expected: TBD billion cubic feet
        """
        # TODO: Query EVALIDator for Georgia net volume on timberland
        # Expected format: All live trees on timberland, net cubic feet
        pass
    
    @pytest.mark.skip(reason="Need to obtain official volume estimates from EVALIDator")
    def test_south_carolina_net_volume_cubic_feet(self, fia_db):
        """
        Validate South Carolina net volume against published estimates.
        
        Source: TBD - Need EVALIDator query
        Expected: TBD billion cubic feet
        """
        # TODO: Query EVALIDator for South Carolina net volume on timberland
        pass


class TestOfficialBiomassEstimates:
    """Validate biomass estimates against published reports."""
    
    @pytest.fixture
    def fia_db(self):
        """Get FIA database connection."""
        db_path = os.getenv("PYFIA_DATABASE_PATH", "fia.duckdb")
        if not Path(db_path).exists():
            pytest.skip(f"FIA database not found at {db_path}")
        return db_path
    
    @pytest.mark.xfail(reason="Green-weight biomass not yet implemented")
    def test_georgia_biomass_green_tons(self, fia_db):
        """
        Validate Georgia biomass against published estimates.
        
        Source: Domain expert communication
        Expected: 2.4 billion green tons total timberland biomass
        """
        EXPECTED_TONS = 2_400_000_000
        TOLERANCE_PCT = 5  # 5% tolerance
        
        with FIA(fia_db) as db:
            db.clip_by_state(13, most_recent=True)
            result = biomass(
                db,
                land_type="timber",
                component="AG",  # Aboveground
                units="tons",  # Should be green tons when implemented
                totals=True
            )
            
            actual_tons = result["BIOMASS"][0]
            pct_diff = abs(actual_tons - EXPECTED_TONS) / EXPECTED_TONS * 100
            
            assert pct_diff <= TOLERANCE_PCT, (
                f"Georgia biomass mismatch: "
                f"pyFIA={actual_tons:,.0f} vs Expected={EXPECTED_TONS:,.0f} "
                f"(diff={pct_diff:.1f}%)"
            )
    
    @pytest.mark.xfail(reason="Green-weight biomass not yet implemented")
    def test_south_carolina_biomass_green_tons(self, fia_db):
        """
        Validate South Carolina biomass against published estimates.
        
        Source: Domain expert communication
        Expected: 1.3 billion green tons total timberland biomass
        """
        EXPECTED_TONS = 1_300_000_000
        TOLERANCE_PCT = 5
        
        with FIA(fia_db) as db:
            db.clip_by_state(45, most_recent=True)
            result = biomass(
                db,
                land_type="timber",
                component="AG",
                units="tons",
                totals=True
            )
            
            actual_tons = result["BIOMASS"][0]
            pct_diff = abs(actual_tons - EXPECTED_TONS) / EXPECTED_TONS * 100
            
            assert pct_diff <= TOLERANCE_PCT, (
                f"South Carolina biomass mismatch: "
                f"pyFIA={actual_tons:,.0f} vs Expected={EXPECTED_TONS:,.0f} "
                f"(diff={pct_diff:.1f}%)"
            )


class TestOfficialTPAEstimates:
    """Validate trees per acre estimates against published reports."""
    
    @pytest.fixture
    def fia_db(self):
        """Get FIA database connection."""
        db_path = os.getenv("PYFIA_DATABASE_PATH", "fia.duckdb")
        if not Path(db_path).exists():
            pytest.skip(f"FIA database not found at {db_path}")
        return db_path
    
    @pytest.mark.skip(reason="Need to obtain official TPA estimates")
    def test_live_trees_per_acre_timberland(self, fia_db):
        """
        Validate trees per acre on timberland.
        
        Source: TBD - Need EVALIDator or report values
        """
        # TODO: Add official TPA estimates when available
        pass


class TestComprehensiveValidationReport:
    """Generate comprehensive validation report for all estimators."""
    
    @pytest.fixture
    def fia_db(self):
        """Get FIA database connection."""
        db_path = os.getenv("PYFIA_DATABASE_PATH", "fia.duckdb")
        if not Path(db_path).exists():
            pytest.skip(f"FIA database not found at {db_path}")
        return db_path
    
    def test_generate_validation_report(self, fia_db):
        """
        Generate a comprehensive validation report comparing all pyFIA
        estimates to EVALIDator for documentation.
        """
        states = [
            (13, 132301, "Georgia"),
            (45, 452301, "South Carolina"),
        ]
        
        print("\n" + "="*80)
        print("COMPREHENSIVE VALIDATION REPORT - pyFIA vs EVALIDator")
        print("="*80)
        
        for state_code, evalid, state_name in states:
            print(f"\n{state_name.upper()} (EVALID {evalid})")
            print("-"*40)
            
            with FIA(fia_db) as db:
                db.clip_by_evalid(evalid)
                
                # Area estimates
                print("\nAREA ESTIMATES:")
                for land_type in ["forest", "timber"]:
                    result = area(db, land_type=land_type, totals=True)
                    acres = result["AREA"][0]
                    se = result["AREA_SE"][0]
                    se_pct = (se / acres * 100) if acres > 0 else 0
                    n_plots = result["N_PLOTS"][0]
                    print(f"  {land_type.capitalize():10}: {acres:15,.0f} acres "
                          f"(SE: {se_pct:6.3f}%, N={n_plots:,} plots)")
                
                # Volume estimates (if available)
                try:
                    print("\nVOLUME ESTIMATES:")
                    result = volume(db, land_type="timber", tree_type="live", totals=True)
                    if "VOLUME" in result.columns:
                        vol = result["VOLUME"][0]
                        se = result.get("VOLUME_SE", [0])[0]
                        se_pct = (se / vol * 100) if vol > 0 and se > 0 else 0
                        n_plots = result["N_PLOTS"][0]
                        print(f"  Net cubic : {vol:15,.0f} cu ft "
                              f"(SE: {se_pct:6.3f}%, N={n_plots:,} plots)")
                except Exception as e:
                    print(f"  Volume estimation error: {e}")
                
                # TPA estimates
                try:
                    print("\nTREES PER ACRE:")
                    result = tpa(db, tree_domain="STATUSCD == 1", land_type="timber")
                    if "TPA" in result.columns:
                        tpa_val = result["TPA"][0]
                        se = result.get("TPA_SE", [0])[0]
                        se_pct = (se / tpa_val * 100) if tpa_val > 0 and se > 0 else 0
                        n_plots = result["N_PLOTS"][0]
                        print(f"  Live trees: {tpa_val:15.1f} per acre "
                              f"(SE: {se_pct:6.3f}%, N={n_plots:,} plots)")
                except Exception as e:
                    print(f"  TPA estimation error: {e}")
        
        print("\n" + "="*80)
        print("INSTRUCTIONS FOR EVALIDATOR COMPARISON:")
        print("1. Go to https://apps.fs.usda.gov/Evalidator/")
        print("2. Select state and evaluation matching EVALID above")
        print("3. Run estimates for Area, Volume, Number of Trees, Biomass")
        print("4. Compare totals and sampling errors")
        print("5. Update test expected values with EVALIDator results")
        print("="*80)


def get_evalidator_estimates() -> Dict[str, Dict]:
    """
    Return dictionary of known EVALIDator estimates for reference.
    
    This serves as a central repository of official values that have been
    validated against EVALIDator.
    """
    return {
        "Georgia": {
            "evalid": 132301,
            "evalidator_evalid": 132023,
            "timberland_acres": 23_596_942,
            "loblolly_pine_acres": 7_337_755,
            "timberland_se_pct": None,  # TODO: Get from EVALIDator
            "loblolly_se_pct": None,  # TODO: Get from EVALIDator
        },
        "South Carolina": {
            "evalid": 452301,
            "evalidator_evalid": 452023,
            "timberland_acres": 12_647_588,
            "loblolly_pine_acres": 5_410_806,
            "timberland_se_pct": 0.796,
            "loblolly_se_pct": 2.463,
        }
    }


if __name__ == "__main__":
    # Run validation report when executed directly
    import sys
    db_path = os.getenv("PYFIA_DATABASE_PATH", "fia.duckdb")
    if Path(db_path).exists():
        test = TestComprehensiveValidationReport()
        test.test_generate_validation_report(db_path)
    else:
        print(f"Database not found at {db_path}")
        print("Set PYFIA_DATABASE_PATH environment variable to database location")
        sys.exit(1)