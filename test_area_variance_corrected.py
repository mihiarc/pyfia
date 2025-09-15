#!/usr/bin/env python
"""Test that the corrected area() function produces proper variance estimates."""

import polars as pl
from pyfia import FIA, area

# Test with Georgia data
db = FIA("data/georgia.duckdb")
db.clip_most_recent(eval_type="ALL")

print("=" * 80)
print("TESTING CORRECTED AREA() VARIANCE IMPLEMENTATION")
print("=" * 80)

# Get area estimates with variance
results = area(db, land_type="forest", variance=True)

print("\n1. RESULTS FROM CORRECTED area() FUNCTION:")
print(results)

# Extract key values
area_col = "AREA_TOTAL" if "AREA_TOTAL" in results.columns else "AREA"
if area_col in results.columns:
    area_total = results[area_col][0]
    print(f"\nForest area estimate: {area_total:,.0f} acres")
else:
    print("Warning: Area column not found in results")
    area_total = 0

if "AREA_SE" in results.columns:
    se = results["AREA_SE"][0]
    print(f"Standard error: {se:,.0f} acres")

    if area_total > 0:
        se_percent = 100 * se / area_total
        print(f"SE%: {se_percent:.3f}%")
elif "AREA_SE_PERCENT" in results.columns:
    se_percent = results["AREA_SE_PERCENT"][0]
    print(f"SE%: {se_percent:.3f}%")
    se = area_total * se_percent / 100
    print(f"Standard error: {se:,.0f} acres")
else:
    print("Warning: No SE columns found in results")

print("\n2. COMPARISON WITH EVALDATOR:")
print(f"   Target forest area: 24,172,679 acres")
print(f"   Target SE%: 0.563%")
print(f"   Target SE: 136,092 acres")

if area_total > 0:
    area_diff = (area_total - 24_172_679) / 24_172_679 * 100
    print(f"\n   Area difference: {area_diff:+.2f}%")

    if "AREA_SE" in results.columns or "AREA_SE_PERCENT" in results.columns:
        se_ratio = se_percent / 0.563
        print(f"   SE% ratio: {se_ratio:.2f}x")

        if se_ratio > 0.8 and se_ratio < 1.2:
            print("\n✅ SUCCESS: Variance calculation is within 20% of target!")
        else:
            print(f"\n⚠️  SE is {se_ratio:.2f}x the target - needs adjustment")

# Test with grouping to ensure variance works with groups
print("\n" + "=" * 80)
print("3. TESTING WITH GROUPING (BY OWNERSHIP):")

results_grouped = area(db, grp_by="OWNGRPCD", land_type="forest", variance=True)
print(f"\nNumber of ownership groups: {len(results_grouped)}")

# Show first few groups
print("\nFirst 3 ownership groups:")
for row in results_grouped.head(3).iter_rows(named=True):
    owngrp = row.get("OWNGRPCD", "?")
    area_val = row.get("AREA_TOTAL", 0)
    se_val = row.get("AREA_SE", 0)
    if area_val > 0:
        se_pct = 100 * se_val / area_val
    else:
        se_pct = row.get("AREA_SE_PERCENT", 0)

    print(f"   OWNGRPCD {owngrp}: {area_val:,.0f} acres, SE% = {se_pct:.3f}%")

print("\n" + "=" * 80)
print("SUMMARY:")
if se_ratio > 0.8 and se_ratio < 1.2:
    print("✅ Variance calculation is working correctly!")
    print("✅ SE% matches EVALIDator within acceptable range")
else:
    print(f"⚠️  Variance still needs adjustment (currently {se_ratio:.2f}x target)")
    print(f"   Current SE%: {se_percent:.3f}%")
    print(f"   Target SE%: 0.563%")