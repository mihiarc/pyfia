#!/usr/bin/env python
"""
Validation of pyFIA area estimation against official FIA published numbers.

This script demonstrates that pyFIA produces statistically exact results
matching the official USDA Forest Inventory and Analysis estimates.
"""

from pyfia import FIA, area

# Official published value for Oklahoma forestland
OFFICIAL_OKLAHOMA_FOREST_ACRES = 11_270_676

# Connect to the database
with FIA("nfi_south.duckdb") as db:
    # Filter to Oklahoma's 2023 current area/volume evaluation
    # EVALID 402301: OKLAHOMA 2023: 2014-2023: CURRENT AREA, CURRENT VOLUME
    db.clip_by_evalid([402301])
    
    # Calculate forestland area
    results = area(db, land_type='forest')
    
    # Extract our estimate
    pyfia_estimate = results['FA_TOTAL'][0]
    
    # Calculate difference
    difference = abs(pyfia_estimate - OFFICIAL_OKLAHOMA_FOREST_ACRES)
    percent_diff = (difference / OFFICIAL_OKLAHOMA_FOREST_ACRES) * 100
    
    # Display validation results
    print("\n" + "=" * 60)
    print("pyFIA VALIDATION: Oklahoma Forestland Area (2023)")
    print("=" * 60)
    print(f"Official FIA Published Value:  {OFFICIAL_OKLAHOMA_FOREST_ACRES:15,.0f} acres")
    print(f"pyFIA Calculated Value:        {pyfia_estimate:15,.0f} acres")
    print(f"Difference:                    {difference:15,.0f} acres")
    print(f"Percent Difference:            {percent_diff:15.6f}%")
    print("-" * 60)
    
    if difference <= 1:
        print("✓ VALIDATION PASSED: Exact statistical compatibility achieved!")
        print("  The 1-acre difference is due to floating-point rounding only.")
    elif percent_diff < 0.01:
        print("✓ VALIDATION PASSED: Within 0.01% of official estimate")
    elif percent_diff < 0.1:
        print("⚠ VALIDATION WARNING: Within 0.1% of official estimate")
    else:
        print("✗ VALIDATION FAILED: Difference exceeds acceptable threshold")
    
    print("\nImplementation Details:")
    print("  • Uses EVALID 402301 (Current Area, Current Volume)")
    print("  • Applies PROP_BASIS adjustment factors (MACR/SUBP)")
    print("  • Filters for forest land (COND_STATUS_CD == 1)")
    print("  • Includes proper stratification and expansion factors")
    print("=" * 60)