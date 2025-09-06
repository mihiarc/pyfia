#!/usr/bin/env python
"""Query total forestland area in Oklahoma from nfi_south.duckdb"""

from pyfia import FIA, area

# Connect to the database
with FIA("nfi_south.duckdb") as db:
    # Filter to Oklahoma's most recent evaluation (2023, all area)
    db.clip_by_evalid([402300])
    
    # Get forestland area
    results = area(db, land_type='forest')
    
    # Display results
    print(f"\nTotal forestland area in Oklahoma (2023 evaluation):")
    print(f"{results['AREA_ESTIMATE'].iloc[0]:,.0f} acres")
    print(f"Standard error: {results['AREA_SE'].iloc[0]:,.0f} acres")
    print(f"Sampling error: {results['AREA_SE_PERC'].iloc[0]:.2f}%")