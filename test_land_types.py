#!/usr/bin/env python
"""Test different land type classifications for Oklahoma"""

from pyfia import FIA, area

# Connect to the database
with FIA("nfi_south.duckdb") as db:
    # Filter to Oklahoma's most recent evaluation (2023, all area)
    db.clip_by_evalid([402300])
    
    print("\nOklahoma Land Area by Type (2023)")
    print("=" * 50)
    
    # Test different land types
    land_types = ["forest", "timber", "all"]
    
    for land_type in land_types:
        results = area(db, land_type=land_type)
        
        total_area = results['FA_TOTAL'][0]
        total_land = results['FAD_TOTAL'][0]
        percentage = (total_area / total_land * 100) if total_land > 0 else 0
        
        print(f"\n{land_type.capitalize()} land:")
        print(f"  Area: {total_area:,.0f} acres")
        print(f"  Percentage of total: {percentage:.1f}%")
    
    # Also test with by_land_type to get breakdown
    print("\n\nDetailed Land Type Breakdown:")
    print("-" * 50)
    results_detailed = area(db, by_land_type=True, land_type="all")
    
    if 'LAND_TYPE' in results_detailed.columns:
        for row in results_detailed.iter_rows(named=True):
            print(f"{row['LAND_TYPE']:20s}: {row['FA_TOTAL']:12,.0f} acres ({row['AREA_PERC']:.1f}%)")
    else:
        print("Detailed breakdown not available (LAND_TYPE column missing)")