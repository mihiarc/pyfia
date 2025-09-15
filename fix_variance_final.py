#!/usr/bin/env python
"""Final attempt to fix variance calculation based on FIA methodology."""

import polars as pl
from pyfia import FIA

# The correct FIA variance formula for stratified total estimation is:
# V(Ŷ) = Σ_h [N_h² × (1 - f_h) × s²_yh / n_h]
#
# Where:
# - N_h = total number of population units in stratum h
# - n_h = number of sampled units in stratum h
# - f_h = n_h / N_h (sampling fraction)
# - s²_yh = sample variance of y values in stratum h
# - y values are the plot-level values (forest proportion × plot size)
#
# For FIA:
# - Each plot represents a fixed area (typically 1 acre sample = ~6000 acres represented)
# - EXPNS = acres represented per plot = N_h × acres_per_plot / n_h
# - So: N_h = EXPNS × n_h / acres_per_plot
#
# But we don't know acres_per_plot directly. However, we can reformulate:
# V(Ŷ) = Σ_h [EXPNS² × n_h × (1 - f_h) × s²_yh / n_h]
#       = Σ_h [EXPNS² × (1 - f_h) × s²_yh]
#
# If f_h is small (typical for FIA), (1 - f_h) ≈ 1, so:
# V(Ŷ) ≈ Σ_h [EXPNS² × s²_yh]
#
# Wait, that's not right either. Let me think...
#
# Actually, the issue is that y_i should be the expanded value for each plot:
# y_i = proportion_i × EXPNS_i
#
# Then the variance of these expanded values gives us the right scale.

db = FIA("data/georgia.duckdb")
db.clip_most_recent(eval_type="ALL")

# Get the data
db.load_table("COND", columns=["PLT_CN", "CONDID", "CONDPROP_UNADJ", "COND_STATUS_CD"])
db.load_table("POP_PLOT_STRATUM_ASSGN")
db.load_table("POP_STRATUM")

# Filter and join
cond = db.tables["COND"].filter(pl.col("COND_STATUS_CD") == 1).collect()
ppsa = db.tables["POP_PLOT_STRATUM_ASSGN"].filter(pl.col("EVALID").is_in(db.evalid)).collect()
strat = db.tables["POP_STRATUM"].filter(pl.col("EVALID").is_in(db.evalid)).unique(subset=["CN"]).collect()

data = cond.join(ppsa, on="PLT_CN", how="inner")
data = data.join(strat, left_on="STRATUM_CN", right_on="CN", how="inner")

# Aggregate to plot level
plot_data = data.group_by(["PLT_CN", "STRATUM_CN", "ESTN_UNIT"]).agg([
    pl.col("CONDPROP_UNADJ").sum().alias("forest_prop"),
    pl.first("EXPNS").alias("EXPNS"),
    pl.first("P2POINTCNT").alias("N_h_total")
])

print(f"Plot data shape: {plot_data.shape}")
print(f"Forest proportion stats: min={plot_data['forest_prop'].min():.3f}, "
      f"max={plot_data['forest_prop'].max():.3f}, mean={plot_data['forest_prop'].mean():.3f}")

# Calculate EXPANDED plot values (this is the key!)
plot_data = plot_data.with_columns([
    (pl.col("forest_prop") * pl.col("EXPNS")).alias("y_i_expanded")
])

print(f"\nExpanded plot values (y_i × EXPNS):")
print(f"  Min: {plot_data['y_i_expanded'].min():,.0f} acres")
print(f"  Max: {plot_data['y_i_expanded'].max():,.0f} acres")
print(f"  Mean: {plot_data['y_i_expanded'].mean():,.0f} acres")
print(f"  Total: {plot_data['y_i_expanded'].sum():,.0f} acres")

# Calculate stratum-level statistics using EXPANDED values
strata_stats = plot_data.group_by(["ESTN_UNIT", "STRATUM_CN"]).agg([
    pl.count("PLT_CN").alias("n_h"),
    pl.mean("y_i_expanded").alias("ybar_h_expanded"),
    pl.var("y_i_expanded", ddof=1).alias("s2_yh_expanded"),
    pl.first("EXPNS").alias("w_h"),
    pl.first("N_h_total").alias("N_h")
])

print(f"\nStratum statistics (first 5):")
print(strata_stats.select(["n_h", "ybar_h_expanded", "s2_yh_expanded", "w_h", "N_h"]).head(5))

# Calculate variance using the expanded values
# V(Ŷ) = Σ_h [n_h × s²(y_expanded)]
# But we need to account for the finite population correction and stratification

# The correct formula for stratified sampling with expansion factors is:
# V(Ŷ) = Σ_h [N_h² × (1 - n_h/N_h) × s²_yh / n_h]
# Where y_h are the per-unit values (proportions)
#
# But since we have y_expanded = y × EXPNS, and EXPNS = N_h × size / n_h:
# V(Ŷ_expanded) = Σ_h [(1 - n_h/N_h) × n_h × s²(y_expanded) / n_h]
#                = Σ_h [(1 - n_h/N_h) × s²(y_expanded)]

strata_stats = strata_stats.with_columns([
    pl.when(pl.col("N_h") > 0)
      .then(1 - pl.col("n_h") / pl.col("N_h"))
      .otherwise(1.0)
      .alias("fpc"),
    # For the expanded values, we don't need to multiply by N_h² again
    # because the expansion is already in the y values
    (pl.col("s2_yh_expanded") * (1 - pl.col("n_h") / pl.col("N_h")))
      .fill_null(0)
      .alias("var_h")
])

print(f"\nVariance components:")
print(strata_stats.select(["n_h", "N_h", "fpc", "s2_yh_expanded", "var_h"]).head(5))

# Hmm, fpc is 0 because n_h = N_h. Let's ignore FPC for now
# and use the standard formula without FPC:
strata_stats = strata_stats.with_columns([
    (pl.col("n_h") * pl.col("s2_yh_expanded") / pl.col("n_h")).alias("var_h_no_fpc")
])

total_variance = strata_stats["var_h_no_fpc"].sum()
se = total_variance ** 0.5
total_estimate = plot_data["y_i_expanded"].sum()
se_percent = 100 * se / total_estimate

print(f"\nFinal results:")
print(f"  Total estimate: {total_estimate:,.0f} acres")
print(f"  Variance: {total_variance:,.0f}")
print(f"  SE: {se:,.0f} acres")
print(f"  SE%: {se_percent:.3f}%")

print(f"\nTarget from EVALIDator: 0.563%")
print(f"Ratio: {se_percent / 0.563:.2f}x")