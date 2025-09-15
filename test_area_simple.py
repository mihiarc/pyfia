#!/usr/bin/env python
"""Simple test to verify area calculation is correct."""

import polars as pl
from pyfia import FIA, area

# Test with Georgia data
db = FIA("data/georgia.duckdb")
db.clip_most_recent(eval_type="ALL")

print(f"Selected EVALID: {db.evalid}")

# Calculate area using our function
results = area(db, land_type="forest")

print(f"\nResults from area() function:")
print(f"  Forest area: {results['AREA'][0]:,.0f} acres")
print(f"  SE: {results['AREA_SE'][0]:,.0f} acres")
print(f"  SE%: {results['AREA_SE_PERCENT'][0]:.3f}%")
print(f"  Plots: {results['N_PLOTS'][0]}")

# Now calculate manually to verify
db.load_table("COND", columns=["PLT_CN", "CONDID", "CONDPROP_UNADJ", "PROP_BASIS", "COND_STATUS_CD"])
db.load_table("POP_PLOT_STRATUM_ASSGN")
db.load_table("POP_STRATUM")

# Get forest conditions
cond = db.tables["COND"].filter(pl.col("COND_STATUS_CD") == 1).collect()

# Get stratification filtered by EVALID
ppsa = db.tables["POP_PLOT_STRATUM_ASSGN"].filter(
    pl.col("EVALID").is_in(db.evalid)
).collect()

strat = db.tables["POP_STRATUM"].filter(
    pl.col("EVALID").is_in(db.evalid)
).collect()

# Remove duplicates from strat (Georgia specific issue)
strat = strat.unique(subset=["CN"])

# Join
data = cond.join(ppsa, on="PLT_CN", how="inner")
data = data.join(strat, left_on="STRATUM_CN", right_on="CN", how="inner")

print(f"\nManual calculation:")
print(f"  Unique plots after joins: {data['PLT_CN'].n_unique()}")
print(f"  Total conditions: {len(data)}")

# Aggregate to plot level first
plot_data = data.group_by("PLT_CN").agg([
    pl.col("CONDPROP_UNADJ").sum().alias("forest_prop"),
    pl.first("EXPNS").alias("EXPNS")
])

print(f"  Plot forest proportion - mean: {plot_data['forest_prop'].mean():.3f}")

# Calculate total
manual_total = (plot_data["forest_prop"] * plot_data["EXPNS"]).sum()
print(f"  Manual total: {manual_total:,.0f} acres")

# Compare
diff = results['AREA'][0] - manual_total
diff_pct = 100 * diff / manual_total
print(f"\nDifference: {diff:,.0f} acres ({diff_pct:+.3f}%)")

# Now check the SE calculation
# The SE% should be around 0.563% based on EVALIDator
print(f"\nTarget SE% from EVALIDator: 0.563%")
print(f"Our SE%: {results['AREA_SE_PERCENT'][0]:.3f}%")
print(f"Ratio: {results['AREA_SE_PERCENT'][0] / 0.563:.2f}x too high")