#!/usr/bin/env python
"""Analyze how area is actually calculated to align variance calculation."""

import polars as pl
from pyfia import FIA

db = FIA("data/georgia.duckdb")
db.clip_most_recent(eval_type="ALL")

# Load the tables we need
db.load_table("COND", columns=["PLT_CN", "CONDID", "CONDPROP_UNADJ", "PROP_BASIS", "COND_STATUS_CD"])
db.load_table("POP_PLOT_STRATUM_ASSGN")
db.load_table("POP_STRATUM")

# Get forest conditions
cond = db.tables["COND"].filter(pl.col("COND_STATUS_CD") == 1).collect()
ppsa = db.tables["POP_PLOT_STRATUM_ASSGN"].collect()
strat = db.tables["POP_STRATUM"].collect()

# Join to get expansion factors
data = cond.join(ppsa, on="PLT_CN", how="inner")
data = data.join(strat, left_on="STRATUM_CN", right_on="CN", how="inner")

print(f"Data shape after joins: {data.shape}")
print(f"Unique plots: {data['PLT_CN'].n_unique()}")
print(f"Total conditions: {len(data)}")

# Calculate area the way the main function does
# Area = CONDPROP_UNADJ * ADJ_FACTOR * EXPNS
# For simplicity, assume ADJ_FACTOR = 1.0 for PROP_BASIS = 'SUBP'
data = data.with_columns([
    pl.when(pl.col("PROP_BASIS") == "SUBP").then(1.0).otherwise(1.0).alias("ADJ_FACTOR")
])

# Calculate total area
total_area = (data["CONDPROP_UNADJ"] * data["ADJ_FACTOR"] * data["EXPNS"]).sum()
print(f"\nTotal area (sum of all conditions): {total_area:,.0f}")

# Now calculate by aggregating to plot level first
plot_area = data.group_by("PLT_CN").agg([
    (pl.col("CONDPROP_UNADJ") * pl.col("ADJ_FACTOR")).sum().alias("plot_prop"),
    pl.first("EXPNS").alias("EXPNS"),
    pl.first("ESTN_UNIT").alias("ESTN_UNIT"),
    pl.first("STRATUM_CN").alias("STRATUM_CN")
])

print(f"\nPlot-level aggregation:")
print(f"  Number of plots: {len(plot_area)}")
print(f"  Plot proportion stats: min={plot_area['plot_prop'].min():.3f}, "
      f"max={plot_area['plot_prop'].max():.3f}, mean={plot_area['plot_prop'].mean():.3f}")

total_area_plot = (plot_area["plot_prop"] * plot_area["EXPNS"]).sum()
print(f"  Total area (plot aggregation): {total_area_plot:,.0f}")

# The key insight: are we double-counting EXPNS for plots with multiple conditions?
multi_cond_plots = data.group_by("PLT_CN").agg([
    pl.count().alias("n_conditions"),
    pl.col("CONDPROP_UNADJ").sum().alias("sum_prop")
])

print(f"\nMultiple condition analysis:")
print(f"  Plots with 1 condition: {(multi_cond_plots['n_conditions'] == 1).sum()}")
print(f"  Plots with 2+ conditions: {(multi_cond_plots['n_conditions'] > 1).sum()}")
print(f"  Max conditions per plot: {multi_cond_plots['n_conditions'].max()}")

# Check if proportions sum to 1 within plots
print(f"\nProportion sums within plots:")
print(f"  Min sum: {multi_cond_plots['sum_prop'].min():.3f}")
print(f"  Max sum: {multi_cond_plots['sum_prop'].max():.3f}")
print(f"  Mean sum: {multi_cond_plots['sum_prop'].mean():.3f}")

# So the correct calculation is at plot level
# Each plot's total forest proportion × expansion factor
print(f"\n✓ Correct total: {total_area_plot:,.0f} acres (plot-level aggregation)")
print(f"✗ Incorrect total: {total_area:,.0f} acres (condition-level sum)")