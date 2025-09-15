#!/usr/bin/env python
"""Fix variance by including ALL plots, not just forest plots."""

import polars as pl
from pyfia import FIA

db = FIA("data/georgia.duckdb")
db.clip_most_recent(eval_type="ALL")

# Get ALL conditions, not just forest
db.load_table("COND", columns=["PLT_CN", "CONDID", "CONDPROP_UNADJ", "COND_STATUS_CD"])
db.load_table("POP_PLOT_STRATUM_ASSGN")
db.load_table("POP_STRATUM")

# Get ALL conditions
cond_all = db.tables["COND"].collect()
ppsa = db.tables["POP_PLOT_STRATUM_ASSGN"].filter(pl.col("EVALID").is_in(db.evalid)).collect()
strat = db.tables["POP_STRATUM"].filter(pl.col("EVALID").is_in(db.evalid)).unique(subset=["CN"]).collect()

print("Including ALL plots (forest and non-forest):")
print(f"Total conditions: {len(cond_all)}")
print(f"Unique plots: {cond_all['PLT_CN'].n_unique()}")

# Create a column for forest proportion (0 for non-forest conditions)
cond_all = cond_all.with_columns([
    pl.when(pl.col("COND_STATUS_CD") == 1)
      .then(pl.col("CONDPROP_UNADJ"))
      .otherwise(0.0)
      .alias("FOREST_PROP")
])

# Join with stratification
data = ppsa.join(cond_all, on="PLT_CN", how="left")  # LEFT join to keep all plots

# For plots with no conditions (shouldn't happen), set forest prop to 0
data = data.with_columns([
    pl.col("FOREST_PROP").fill_null(0.0)
])

print(f"\nAfter joining with ALL plots:")
print(f"Total rows: {len(data)}")
print(f"Unique plots: {data['PLT_CN'].n_unique()}")

# Aggregate to plot level
plot_data = data.group_by(["PLT_CN", "STRATUM_CN"]).agg([
    pl.col("FOREST_PROP").sum().alias("forest_prop_total"),
    pl.first("EVALID").alias("EVALID")
])

# Join with stratum data
plot_data = plot_data.join(strat, left_on="STRATUM_CN", right_on="CN", how="inner")

print(f"\nPlot-level data:")
print(f"Total plots: {len(plot_data)}")
print(f"Forest proportion stats:")
print(f"  Plots with forest (>0): {(plot_data['forest_prop_total'] > 0).sum()}")
print(f"  Plots without forest (=0): {(plot_data['forest_prop_total'] == 0).sum()}")

# Calculate total area
total_area = (plot_data["forest_prop_total"] * plot_data["EXPNS"]).sum()
print(f"\nTotal forest area: {total_area:,.0f} acres")

# Now calculate variance with ALL plots
# Aggregate to stratum level
strata_stats = plot_data.group_by("STRATUM_CN").agg([
    pl.count("PLT_CN").alias("n_h"),  # This should now match P2POINTCNT better
    pl.mean("forest_prop_total").alias("ybar_h"),
    pl.var("forest_prop_total", ddof=1).alias("s2_yh"),
    pl.first("EXPNS").alias("w_h"),
    pl.first("P2POINTCNT").alias("N_h")
])

print(f"\nStratum statistics (first 5):")
print(strata_stats.select(["n_h", "N_h", "ybar_h", "s2_yh", "w_h"]).head())

# Check if n_h now matches P2POINTCNT
n_match = (strata_stats["n_h"] == strata_stats["N_h"]).sum()
print(f"\nStrata where n_h = P2POINTCNT: {n_match}/{len(strata_stats)}")

# Calculate variance
# V(Ŷ) = Σ_h [w_h² × s²_yh / n_h]
variance_components = strata_stats.with_columns([
    pl.when(pl.col("n_h") > 0)
      .then(pl.col("w_h") ** 2 * pl.col("s2_yh") / pl.col("n_h"))
      .otherwise(0.0)
      .alias("v_h")
])

total_variance = variance_components["v_h"].sum()
se = total_variance ** 0.5
se_percent = 100 * se / total_area if total_area > 0 else 0

print(f"\nVariance calculation results:")
print(f"  Total variance: {total_variance:,.0f}")
print(f"  SE: {se:,.0f} acres")
print(f"  SE%: {se_percent:.3f}%")

print(f"\nComparison with EVALIDator:")
print(f"  EVALIDator SE%: 0.563%")
print(f"  Our SE%: {se_percent:.3f}%")
print(f"  Ratio: {se_percent / 0.563:.2f}x")

# Additional insight: what's the mean n_h?
print(f"\nSample sizes:")
print(f"  Mean n_h: {strata_stats['n_h'].mean():.1f}")
print(f"  Total n: {strata_stats['n_h'].sum()}")
print(f"  This should equal total PPSA plots: {len(ppsa)}")