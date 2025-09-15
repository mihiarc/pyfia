#!/usr/bin/env python
"""Analyze the EVALIDator query to understand the exact calculation method."""

import polars as pl
from pyfia import FIA

# Parse the EVALIDator query to understand key differences
print("=" * 80)
print("ANALYZING EVALIDATOR QUERY FOR AREA CALCULATION")
print("=" * 80)

print("\n1. KEY OBSERVATIONS FROM EVALIDATOR QUERY:")
print("-" * 40)

print("\na) ADJUSTMENT FACTOR LOGIC:")
print("   CASE COND.PROP_BASIS")
print("     WHEN 'MACR' THEN POP_STRATUM.ADJ_FACTOR_MACR")
print("     ELSE POP_STRATUM.ADJ_FACTOR_SUBP")
print("   END")
print("\n   ⚠️  EVALIDator uses PROP_BASIS from COND table, not plot-level breakpoints!")
print("   ⚠️  Only two cases: MACR uses ADJ_FACTOR_MACR, everything else uses ADJ_FACTOR_SUBP")

print("\nb) FILTERING:")
print("   - COND.COND_STATUS_CD = 1 (forest only)")
print("   - COND.CONDPROP_UNADJ IS NOT NULL")
print("   - Specific EVALID: pop_stratum.evalid = 132301")
print("   - Specific RSCD: pop_stratum.rscd = 33")

print("\nc) CALCULATION:")
print("   ESTIMATED_VALUE = SUM(CONDPROP_UNADJ * adjustment_factor)")
print("   FINAL = SUM(ESTIMATED_VALUE * EXPNS)")
print("   - Groups by plot first, then aggregates")

print("\n2. TESTING OUR IMPLEMENTATION VS EVALIDATOR METHOD:")
print("-" * 40)

# Load Georgia data
db = FIA("data/georgia.duckdb")
db.clip_most_recent(eval_type="ALL")

# Load tables
db.load_table("COND", columns=["PLT_CN", "CONDID", "CONDPROP_UNADJ", "COND_STATUS_CD", "PROP_BASIS"])
db.load_table("POP_PLOT_STRATUM_ASSGN")
db.load_table("POP_STRATUM")

# Get data
cond = db.tables["COND"].filter(
    (pl.col("COND_STATUS_CD") == 1) &
    pl.col("CONDPROP_UNADJ").is_not_null()
).collect()
ppsa = db.tables["POP_PLOT_STRATUM_ASSGN"].filter(pl.col("EVALID").is_in(db.evalid)).collect()
strat = db.tables["POP_STRATUM"].filter(pl.col("EVALID").is_in(db.evalid)).unique(subset=["CN"]).collect()

print(f"\nData loaded:")
print(f"  Forest conditions: {len(cond)}")
print(f"  Plot-stratum assignments: {len(ppsa)}")
print(f"  Strata: {len(strat)}")

# Check PROP_BASIS values
prop_basis_counts = cond.group_by("PROP_BASIS").agg(pl.count().alias("count"))
print(f"\nPROP_BASIS distribution in COND:")
print(prop_basis_counts)

# Join data
data = cond.join(ppsa, on="PLT_CN", how="inner")
data = data.join(strat, left_on="STRATUM_CN", right_on="CN", how="inner")

print(f"\nAfter joins: {len(data)} condition records")

# Method 1: Our current implementation (using PROP_BASIS)
# This should match EVALIDator exactly
data_method1 = data.with_columns([
    pl.when(pl.col("PROP_BASIS") == "MACR")
      .then(pl.col("ADJ_FACTOR_MACR"))
      .otherwise(pl.col("ADJ_FACTOR_SUBP"))
      .alias("adj_factor")
])

# Calculate area using EVALIDator method
result1 = data_method1.with_columns([
    (pl.col("CONDPROP_UNADJ") * pl.col("adj_factor") * pl.col("EXPNS")).alias("area_expanded")
])

total_area_method1 = result1["area_expanded"].sum()
print(f"\n3. AREA CALCULATION RESULTS:")
print(f"-" * 40)
print(f"Method 1 (EVALIDator exact logic): {total_area_method1:,.0f} acres")

# Method 2: Check what happens if we use ADJ_FACTOR_SUBP for everything
data_method2 = data.with_columns([
    pl.col("ADJ_FACTOR_SUBP").alias("adj_factor")
])

result2 = data_method2.with_columns([
    (pl.col("CONDPROP_UNADJ") * pl.col("adj_factor") * pl.col("EXPNS")).alias("area_expanded")
])

total_area_method2 = result2["area_expanded"].sum()
print(f"Method 2 (SUBP only): {total_area_method2:,.0f} acres")

# Method 3: Check if we're missing any adjustments
print(f"\n4. ADJUSTMENT FACTOR ANALYSIS:")
print(f"-" * 40)

# Check adjustment factor values
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

# Check if all adjustment factors are 1.0
all_one_subp = (strat["ADJ_FACTOR_SUBP"] == 1.0).all()
all_one_macr = (strat["ADJ_FACTOR_MACR"] == 1.0).all()
print(f"\nAll ADJ_FACTOR_SUBP = 1.0? {all_one_subp}")
print(f"All ADJ_FACTOR_MACR = 1.0? {all_one_macr}")

# Method 4: No adjustment factors (raw)
total_area_raw = (cond["CONDPROP_UNADJ"] *
                  data["EXPNS"].head(len(cond))).sum()
print(f"\nMethod 4 (No adjustment): {total_area_raw:,.0f} acres")

print(f"\n5. COMPARISON WITH TARGETS:")
print(f"-" * 40)
print(f"EVALIDator target: 24,172,679 acres")
print(f"Our result: {total_area_method1:,.0f} acres")
print(f"Difference: {(total_area_method1 - 24_172_679):,.0f} acres ({(total_area_method1 - 24_172_679) / 24_172_679 * 100:.2f}%)")

# Check for any NULL values that might affect calculation
print(f"\n6. DATA QUALITY CHECKS:")
print(f"-" * 40)
print(f"NULL CONDPROP_UNADJ: {cond['CONDPROP_UNADJ'].is_null().sum()}")
print(f"NULL EXPNS: {data['EXPNS'].is_null().sum()}")
print(f"NULL ADJ_FACTOR_SUBP: {data['ADJ_FACTOR_SUBP'].is_null().sum()}")
print(f"NULL ADJ_FACTOR_MACR: {data['ADJ_FACTOR_MACR'].is_null().sum()}")

# Check EVALID in our data
evalids = strat["EVALID"].unique()
print(f"\n7. EVALID CHECK:")
print(f"EVALIDs in our data: {evalids}")
print(f"EVALIDator uses: 132301 (Georgia 2023 EXPALL)")

# Try filtering to exact EVALID if different
if 132301 in evalids:
    ppsa_exact = db.tables["POP_PLOT_STRATUM_ASSGN"].filter(pl.col("EVALID") == 132301).collect()
    strat_exact = db.tables["POP_STRATUM"].filter(pl.col("EVALID") == 132301).unique(subset=["CN"]).collect()

    data_exact = cond.join(ppsa_exact, on="PLT_CN", how="inner")
    data_exact = data_exact.join(strat_exact, left_on="STRATUM_CN", right_on="CN", how="inner")

    data_exact = data_exact.with_columns([
        pl.when(pl.col("PROP_BASIS") == "MACR")
          .then(pl.col("ADJ_FACTOR_MACR"))
          .otherwise(pl.col("ADJ_FACTOR_SUBP"))
          .alias("adj_factor")
    ])

    total_exact = (data_exact["CONDPROP_UNADJ"] * data_exact["adj_factor"] * data_exact["EXPNS"]).sum()
    print(f"\nWith exact EVALID 132301: {total_exact:,.0f} acres")
    print(f"Difference from target: {(total_exact - 24_172_679) / 24_172_679 * 100:.2f}%")