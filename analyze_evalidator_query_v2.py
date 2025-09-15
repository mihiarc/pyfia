#!/usr/bin/env python
"""Analyze the EVALIDator query to understand the exact calculation method."""

import polars as pl
from pyfia import FIA

print("=" * 80)
print("ANALYZING EVALIDATOR QUERY FOR AREA CALCULATION")
print("=" * 80)

print("\n1. KEY DIFFERENCES IDENTIFIED FROM EVALIDATOR QUERY:")
print("-" * 50)

print("\na) CRITICAL INSIGHT - ADJUSTMENT FACTOR LOGIC:")
print("   EVALIDator uses COND.PROP_BASIS field:")
print("   - IF PROP_BASIS = 'MACR' → use ADJ_FACTOR_MACR")
print("   - ELSE → use ADJ_FACTOR_SUBP")
print("\n   ⚠️  This is different from tree-level adjustment!")
print("   ⚠️  For conditions, we use PROP_BASIS directly, not diameter breakpoints")

print("\nb) FILTERING SEQUENCE:")
print("   1. Filter to specific EVALID (132301 for Georgia)")
print("   2. Filter COND_STATUS_CD = 1 (forest)")
print("   3. Filter CONDPROP_UNADJ IS NOT NULL")
print("   4. Group by plot/condition first")
print("   5. Then aggregate with EXPNS")

# Load Georgia data
db = FIA("data/georgia.duckdb")

# Check available EVALIDs
print("\n2. CHECKING EVALIDS IN DATABASE:")
print("-" * 50)

# Get all EVALIDs
all_evalids_query = """
SELECT DISTINCT EVALID, COUNT(DISTINCT PLT_CN) as plot_count
FROM POP_PLOT_STRATUM_ASSGN
GROUP BY EVALID
ORDER BY EVALID DESC
"""

if hasattr(db, 'conn'):
    conn = db.conn
elif hasattr(db, '_reader') and hasattr(db._reader, 'conn'):
    conn = db._reader.conn
else:
    import duckdb
    conn = duckdb.connect("data/georgia.duckdb", read_only=True)

evalids_result = conn.execute(all_evalids_query).fetchall()
print("\nAvailable EVALIDs:")
for evalid, count in evalids_result[:5]:  # Show first 5
    print(f"  {evalid}: {count} plots")

# Use the most recent EXPALL
target_evalid = 132301  # From EVALIDator query
our_evalid = None

for evalid, _ in evalids_result:
    if evalid == target_evalid:
        our_evalid = target_evalid
        break

if our_evalid is None:
    # Use most recent if exact match not found
    our_evalid = evalids_result[0][0]
    print(f"\n⚠️  EVALIDator EVALID {target_evalid} not found, using {our_evalid}")
else:
    print(f"\n✓ Using EVALID {our_evalid} (matches EVALIDator)")

# Now load data with specific EVALID
db.clip_by_evalid([our_evalid])

# Load tables
db.load_table("COND", columns=["PLT_CN", "CONDID", "CONDPROP_UNADJ", "COND_STATUS_CD", "PROP_BASIS"])
db.load_table("POP_PLOT_STRATUM_ASSGN")
db.load_table("POP_STRATUM")

print("\n3. ANALYZING PROP_BASIS VALUES:")
print("-" * 50)

# Get data
cond = db.tables["COND"].filter(
    (pl.col("COND_STATUS_CD") == 1) &
    pl.col("CONDPROP_UNADJ").is_not_null()
).collect()

# Check PROP_BASIS distribution
prop_basis_counts = cond.group_by("PROP_BASIS").agg(pl.len().alias("count"))
print("\nPROP_BASIS distribution in forest conditions:")
print(prop_basis_counts)

# Get stratification data
ppsa = db.tables["POP_PLOT_STRATUM_ASSGN"].filter(pl.col("EVALID") == our_evalid).collect()
strat = db.tables["POP_STRATUM"].filter(pl.col("EVALID") == our_evalid).unique(subset=["CN"]).collect()

print(f"\nData for EVALID {our_evalid}:")
print(f"  Forest conditions: {len(cond)}")
print(f"  Plot-stratum assignments: {len(ppsa)}")
print(f"  Strata: {len(strat)}")

# Join data exactly as EVALIDator does
data = ppsa.join(cond, on="PLT_CN", how="inner")
data = data.join(strat, left_on="STRATUM_CN", right_on="CN", how="inner")

print(f"  After joins: {len(data)} condition records")

print("\n4. ADJUSTMENT FACTOR ANALYSIS:")
print("-" * 50)

# Check adjustment factors
adj_stats = strat.select([
    pl.col("ADJ_FACTOR_SUBP").mean().alias("mean_subp"),
    pl.col("ADJ_FACTOR_SUBP").min().alias("min_subp"),
    pl.col("ADJ_FACTOR_SUBP").max().alias("max_subp"),
    pl.col("ADJ_FACTOR_MACR").mean().alias("mean_macr"),
    pl.col("ADJ_FACTOR_MACR").min().alias("min_macr"),
    pl.col("ADJ_FACTOR_MACR").max().alias("max_macr"),
])
print("Adjustment factor statistics:")
print(adj_stats)

# In Georgia, all adjustment factors are 1.0 for SUBP and 0.0 for MACR
# This means no adjustment is actually happening!

print("\n5. AREA CALCULATION - MATCHING EVALIDATOR:")
print("-" * 50)

# Method 1: Exact EVALIDator logic
data = data.with_columns([
    pl.when(pl.col("PROP_BASIS") == "MACR")
      .then(pl.col("ADJ_FACTOR_MACR"))
      .otherwise(pl.col("ADJ_FACTOR_SUBP"))
      .alias("adjustment")
])

# Calculate ESTIMATED_VALUE per condition
data = data.with_columns([
    (pl.col("CONDPROP_UNADJ") * pl.col("adjustment")).alias("ESTIMATED_VALUE")
])

# Group by plot first (as EVALIDator does)
plot_level = data.group_by(["PLT_CN", "EXPNS"]).agg([
    pl.sum("ESTIMATED_VALUE").alias("plot_total")
])

# Then calculate final area
total_area = (plot_level["plot_total"] * plot_level["EXPNS"]).sum()
print(f"\nMethod 1 (EVALIDator exact): {total_area:,.0f} acres")

# Method 2: Without grouping by plot first
total_area_direct = (data["ESTIMATED_VALUE"] * data["EXPNS"]).sum()
print(f"Method 2 (Direct calculation): {total_area_direct:,.0f} acres")

# Method 3: Check our current implementation
from pyfia import area
results = area(db, land_type="forest", variance=False)
our_area = results["AREA"][0] if "AREA" in results.columns else results["AREA_TOTAL"][0]
print(f"Method 3 (pyFIA area()): {our_area:,.0f} acres")

print("\n6. COMPARISON WITH EVALDATOR TARGET:")
print("-" * 50)
target = 24_172_679
print(f"EVALIDator target: {target:,.0f} acres")
print(f"\nDifferences:")
print(f"  Method 1: {(total_area - target):+,.0f} ({(total_area - target) / target * 100:+.2f}%)")
print(f"  Method 2: {(total_area_direct - target):+,.0f} ({(total_area_direct - target) / target * 100:+.2f}%)")
print(f"  Method 3: {(our_area - target):+,.0f} ({(our_area - target) / target * 100:+.2f}%)")

# Check if there's an EVALID mismatch
if our_evalid != target_evalid:
    print(f"\n⚠️  EVALID MISMATCH:")
    print(f"  EVALIDator uses: {target_evalid}")
    print(f"  We're using: {our_evalid}")
    print("  This could explain the ~2% difference")

# Additional check: Are we missing any plots?
print("\n7. PLOT COVERAGE CHECK:")
print("-" * 50)

# Count unique plots in our calculation
unique_plots = data["PLT_CN"].n_unique()
print(f"Unique plots in calculation: {unique_plots}")

# Check how many forest plots exist in total
total_forest_query = """
SELECT COUNT(DISTINCT c.PLT_CN) as forest_plots
FROM COND c
JOIN POP_PLOT_STRATUM_ASSGN ppsa ON c.PLT_CN = ppsa.PLT_CN
WHERE c.COND_STATUS_CD = 1
  AND c.CONDPROP_UNADJ IS NOT NULL
  AND ppsa.EVALID = ?
"""
forest_count = conn.execute(total_forest_query, [our_evalid]).fetchone()[0]
print(f"Total forest plots for EVALID: {forest_count}")

if unique_plots != forest_count:
    print(f"⚠️  Missing {forest_count - unique_plots} plots in calculation!")

# Check for any conditions with PROP_BASIS = 'MACR'
macr_count = (data["PROP_BASIS"] == "MACR").sum()
print(f"\nConditions with PROP_BASIS='MACR': {macr_count}")
if macr_count > 0 and adj_stats["mean_macr"][0] == 0:
    print("⚠️  MACR conditions exist but ADJ_FACTOR_MACR = 0!")
    print("    This would zero out those conditions in EVALIDator")