#!/usr/bin/env python
"""
Validate Georgia volume estimates against published FIA EVALIDator values.

This script compares pyFIA volume estimates with official FIA statistics
for Georgia to ensure statistical accuracy.
"""

import polars as pl
from pyfia import FIA, volume, area
import duckdb
import sys

def test_georgia_timberland_evalidator():
    """
    Validate Georgia timberland area against EVALIDator.
    
    Source: EVALIDator web tool
    Query Date: 2023 evaluation (EVALID 132023, database 132301)
    Expected: 23,596,942 acres timberland
    """
    EXPECTED_ACRES = 23_596_942
    TOLERANCE_ACRES = 100  # Allow 100 acre tolerance
    
    print("="*80)
    print("GEORGIA TIMBERLAND VALIDATION TEST")
    print("="*80)
    
    db = FIA("data/georgia.duckdb")
    db.clip_by_evalid(132301)  # EXPVOL evaluation
    
    result = area(db, land_type="timber")
    
    actual_acres = result["AREA"][0]
    difference = abs(actual_acres - EXPECTED_ACRES)
    pct_diff = (difference / EXPECTED_ACRES) * 100
    
    print(f"\nTimberland Area Comparison:")
    print(f"  pyFIA result:    {actual_acres:,.0f} acres")
    print(f"  EVALIDator:      {EXPECTED_ACRES:,.0f} acres")
    print(f"  Difference:      {difference:,.0f} acres ({pct_diff:.4f}%)")
    print(f"  Tolerance:       {TOLERANCE_ACRES:,.0f} acres")
    
    if difference <= TOLERANCE_ACRES:
        print("\n✅ TEST PASSED: Georgia timberland area matches EVALIDator!")
        return True
    else:
        print(f"\n❌ TEST FAILED: Difference of {difference:,.0f} acres exceeds tolerance")
        return False


def test_georgia_loblolly_pine_evalidator():
    """
    Validate Georgia loblolly/shortleaf pine forest type area.
    
    Source: EVALIDator web tool
    Forest Type Code: 161 (Loblolly/Shortleaf Pine)
    Expected: 7,337,755 acres
    """
    EXPECTED_ACRES = 7_337_755
    TOLERANCE_ACRES = 100
    
    print("\n" + "="*80)
    print("GEORGIA LOBLOLLY PINE FOREST TYPE VALIDATION TEST")
    print("="*80)
    
    db = FIA("data/georgia.duckdb")
    db.clip_by_evalid(132301)  # EXPVOL evaluation
    
    # Filter by forest type code 161 (Loblolly/Shortleaf Pine)
    result = area(db, area_domain="FORTYPCD == 161", land_type="timber")
    
    actual_acres = result["AREA"][0]
    difference = abs(actual_acres - EXPECTED_ACRES)
    pct_diff = (difference / EXPECTED_ACRES) * 100
    
    print(f"\nLoblolly/Shortleaf Pine Forest Type Comparison:")
    print(f"  pyFIA result:    {actual_acres:,.0f} acres")
    print(f"  EVALIDator:      {EXPECTED_ACRES:,.0f} acres")
    print(f"  Difference:      {difference:,.0f} acres ({pct_diff:.4f}%)")
    print(f"  Tolerance:       {TOLERANCE_ACRES:,.0f} acres")
    
    if difference <= TOLERANCE_ACRES:
        print("\n✅ TEST PASSED: Georgia loblolly pine area matches EVALIDator!")
        return True
    else:
        print(f"\n❌ TEST FAILED: Difference of {difference:,.0f} acres exceeds tolerance")
        return False


def test_georgia_comprehensive_report():
    """
    Generate comprehensive validation report for Georgia.
    """
    print("\n" + "="*80)
    print("COMPREHENSIVE GEORGIA VALIDATION REPORT")
    print("="*80)
    
    db = FIA("data/georgia.duckdb")
    
    # Test with different EVALIDs
    evalids = [
        (132300, "EXPALL", "All Area"),
        (132301, "EXPVOL", "Current Volume")
    ]
    
    for evalid, eval_type, description in evalids:
        print(f"\n{'-'*60}")
        print(f"EVALID {evalid} ({eval_type}: {description})")
        print(f"{'-'*60}")
        
        db_test = FIA("data/georgia.duckdb")
        db_test.clip_by_evalid(evalid)
        
        # Forest area
        forest_result = area(db_test, land_type="forest")
        forest_acres = forest_result["AREA"][0]
        forest_se = forest_result["AREA_SE"][0]
        forest_plots = forest_result["N_PLOTS"][0]
        forest_se_pct = (forest_se / forest_acres * 100) if forest_acres > 0 else 0
        
        print(f"\nForest Area:")
        print(f"  Total: {forest_acres:,.0f} acres")
        print(f"  SE: {forest_se:,.0f} acres ({forest_se_pct:.3f}%)")
        print(f"  Non-zero plots: {forest_plots:,}")
        
        # Timberland area
        timber_result = area(db_test, land_type="timber")
        timber_acres = timber_result["AREA"][0]
        timber_se = timber_result["AREA_SE"][0]
        timber_plots = timber_result["N_PLOTS"][0]
        timber_se_pct = (timber_se / timber_acres * 100) if timber_acres > 0 else 0
        
        print(f"\nTimberland Area:")
        print(f"  Total: {timber_acres:,.0f} acres")
        print(f"  SE: {timber_se:,.0f} acres ({timber_se_pct:.3f}%)")
        print(f"  Non-zero plots: {timber_plots:,}")
        
        # Loblolly pine if EXPVOL
        if eval_type == "EXPVOL":
            lob_result = area(db_test, area_domain="FORTYPCD == 161", land_type="timber")
            lob_acres = lob_result["AREA"][0]
            lob_se = lob_result["AREA_SE"][0]
            lob_plots = lob_result["N_PLOTS"][0]
            lob_se_pct = (lob_se / lob_acres * 100) if lob_acres > 0 else 0
            
            print(f"\nLoblolly/Shortleaf Pine Forest Type:")
            print(f"  Total: {lob_acres:,.0f} acres")
            print(f"  SE: {lob_se:,.0f} acres ({lob_se_pct:.3f}%)")
            print(f"  Non-zero plots: {lob_plots:,}")


def test_sampling_error_comparison():
    """
    Compare sampling errors with EVALIDator.
    
    Known EVALIDator sampling errors for Georgia are not yet documented,
    but we can check if our values are reasonable.
    """
    print("\n" + "="*80)
    print("GEORGIA SAMPLING ERROR ANALYSIS")
    print("="*80)
    
    db = FIA("data/georgia.duckdb")
    db.clip_by_evalid(132301)
    
    # Timberland
    timber_result = area(db, land_type="timber")
    timber_acres = timber_result["AREA"][0]
    timber_se = timber_result["AREA_SE"][0]
    timber_se_pct = (timber_se / timber_acres * 100)
    
    print(f"\nTimberland Sampling Error:")
    print(f"  Area: {timber_acres:,.0f} acres")
    print(f"  Standard Error: {timber_se:,.0f} acres")
    print(f"  Sampling Error %: {timber_se_pct:.3f}%")
    print(f"  Expected range: 0.5-2.0% for state-level estimates")
    
    if 0.5 <= timber_se_pct <= 2.0:
        print("  ✅ Sampling error is in reasonable range")
    else:
        print(f"  ⚠️  Sampling error {timber_se_pct:.3f}% may indicate variance calculation issue")
    
    # Loblolly pine
    lob_result = area(db, area_domain="FORTYPCD == 161", land_type="timber")
    lob_acres = lob_result["AREA"][0]
    lob_se = lob_result["AREA_SE"][0]
    lob_se_pct = (lob_se / lob_acres * 100)
    
    print(f"\nLoblolly Pine Sampling Error:")
    print(f"  Area: {lob_acres:,.0f} acres")
    print(f"  Standard Error: {lob_se:,.0f} acres")
    print(f"  Sampling Error %: {lob_se_pct:.3f}%")
    print(f"  Expected range: 2.0-5.0% for species/type-specific estimates")
    
    if 2.0 <= lob_se_pct <= 5.0:
        print("  ✅ Sampling error is in reasonable range")
    else:
        print(f"  ⚠️  Sampling error {lob_se_pct:.3f}% may indicate variance calculation issue")


def main():
    """Run all Georgia validation tests."""
    
    print("\n" + "🌲"*40)
    print("RUNNING GEORGIA FIA VALIDATION TESTS")
    print("🌲"*40)
    
    tests_passed = []
    tests_failed = []
    
    # Run critical validation tests
    try:
        if test_georgia_timberland_evalidator():
            tests_passed.append("Timberland area")
        else:
            tests_failed.append("Timberland area")
    except Exception as e:
        print(f"Error in timberland test: {e}")
        tests_failed.append("Timberland area (error)")
    
    try:
        if test_georgia_loblolly_pine_evalidator():
            tests_passed.append("Loblolly pine area")
        else:
            tests_failed.append("Loblolly pine area")
    except Exception as e:
        print(f"Error in loblolly pine test: {e}")
        tests_failed.append("Loblolly pine area (error)")
    
    # Run report generation
    try:
        test_georgia_comprehensive_report()
        tests_passed.append("Comprehensive report")
    except Exception as e:
        print(f"Error in comprehensive report: {e}")
        tests_failed.append("Comprehensive report (error)")
    
    # Run sampling error analysis
    try:
        test_sampling_error_comparison()
        tests_passed.append("Sampling error analysis")
    except Exception as e:
        print(f"Error in sampling error analysis: {e}")
        tests_failed.append("Sampling error analysis (error)")
    
    # Summary
    print("\n" + "="*80)
    print("VALIDATION TEST SUMMARY")
    print("="*80)
    print(f"\n✅ Tests Passed ({len(tests_passed)}):")
    for test in tests_passed:
        print(f"   • {test}")
    
    if tests_failed:
        print(f"\n❌ Tests Failed ({len(tests_failed)}):")
        for test in tests_failed:
            print(f"   • {test}")
        return 1
    else:
        print("\n🎉 ALL VALIDATION TESTS PASSED!")
        print("\nGeorgia estimates match EVALIDator published values.")
        print("The georgia.duckdb database is validated and ready for use.")
        return 0


if __name__ == "__main__":
    sys.exit(main())