#!/usr/bin/env python
"""
Test updated growth function against published estimate.

Target: 2,473,614,987 cu ft for Georgia EVALID 132303
"""

import sys
sys.path.insert(0, '/home/mihiarc/pyfia/src')

from pyfia import FIA, growth

# Test with the database
db_path = "./data/test_southern.duckdb"

print("=" * 80)
print("TESTING UPDATED GROWTH FUNCTION")
print("=" * 80)
print("Target: 2,473,614,987 cu ft")
print("=" * 80)

# Connect to database and filter to Georgia
db = FIA(db_path)

# Filter to specific EVALID
db.clip_by_evalid([132303])

# Run growth estimation with the updated function
print("\nRunning growth estimation...")
results = growth(
    db,
    land_type="timber",  # Timberland only
    tree_type="gs",       # Growing stock
    measure="volume",     # Volume in cubic feet
    totals=True,
    variance=False
)

print("\nResults:")
if not results.is_empty():
    growth_per_acre = results['GROWTH_ACRE'][0]
    growth_total = results['GROWTH_TOTAL'][0] if 'GROWTH_TOTAL' in results.columns else None
    n_plots = results['N_PLOTS'][0] if 'N_PLOTS' in results.columns else None

    print(f"  Growth per acre: {growth_per_acre:.2f} cu ft/acre/year")
    if growth_total:
        print(f"  Total growth: {growth_total:,.0f} cu ft/year")
        print(f"  Target:       2,473,614,987 cu ft/year")

        diff = growth_total - 2473614987
        pct_diff = (diff / 2473614987) * 100
        print(f"  Difference:   {diff:+,.0f} ({pct_diff:+.2f}%)")

        if abs(pct_diff) < 2:
            print("\n✓ SUCCESS: Within 2% of target!")
        else:
            print(f"\n⚠ Note: {abs(pct_diff):.1f}% difference from target")

    if n_plots:
        print(f"  Number of plots: {n_plots}")

    # Show all columns for debugging
    print("\nAll result columns:")
    for col in results.columns:
        value = results[col][0]
        if isinstance(value, float):
            print(f"  {col}: {value:,.2f}")
        else:
            print(f"  {col}: {value}")
else:
    print("No results returned!")

# Clean up
db.close()

print("\n" + "=" * 80)
print("TEST COMPLETE")
print("=" * 80)