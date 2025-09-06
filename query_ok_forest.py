#!/usr/bin/env python
"""Query total forestland area in Oklahoma from nfi_south.duckdb"""

from pyfia import FIA, area

# Connect to the database
with FIA("nfi_south.duckdb") as db:
    # Filter to Oklahoma's most recent evaluation (2023, current area/volume)
    # Using 402301 to match the SQL example
    db.clip_by_evalid([402301])
    
    # Get forestland area
    results = area(db, land_type='forest')
    
    # Display results
    print(f"\nOklahoma Forest Area Estimation (2023)")
    print("=" * 50)
    print(f"Total forest area: {results['FA_TOTAL'][0]:,.0f} acres")
    print(f"Total land area:   {results['FAD_TOTAL'][0]:,.0f} acres")
    
    # Calculate percentage if different
    if results['FAD_TOTAL'][0] > 0:
        forest_pct = (results['FA_TOTAL'][0] / results['FAD_TOTAL'][0]) * 100
        print(f"Forest percentage: {forest_pct:.1f}%")