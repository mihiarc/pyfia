#!/usr/bin/env python
"""Test variance calculation using P1POINTCNT as population size."""

import polars as pl
from pyfia import FIA

db = FIA("data/georgia.duckdb")
db.clip_most_recent(eval_type="ALL")

# Get ALL plots (including non-forest)
db.load_table("COND", columns=["PLT_CN", "CONDID", "CONDPROP_UNADJ", "COND_STATUS_CD"])
db.load_table("POP_PLOT_STRATUM_ASSGN")
db.load_table("POP_STRATUM")

cond_all = db.tables["COND"].collect()
ppsa = db.tables["POP_PLOT_STRATUM_ASSGN"].filter(pl.col("EVALID").is_in(db.evalid)).collect()
strat = db.tables["POP_STRATUM"].filter(pl.col("EVALID").is_in(db.evalid)).unique(subset=["CN"]).collect()

# Create forest proportion (0 for non-forest)
cond_all = cond_all.with_columns([
    pl.when(pl.col("COND_STATUS_CD") == 1)
      .then(pl.col("CONDPROP_UNADJ"))
      .otherwise(0.0)
      .alias("FOREST_PROP")
])

# Join to get all plots
data = ppsa.join(cond_all, on="PLT_CN", how="left")
data = data.with_columns([pl.col("FOREST_PROP").fill_null(0.0)])

# Aggregate to plot level
plot_data = data.group_by(["PLT_CN", "STRATUM_CN"]).agg([
    pl.col("FOREST_PROP").sum().alias("forest_prop")
])

# Join with stratum data
plot_data = plot_data.join(strat, left_on="STRATUM_CN", right_on="CN")

# Calculate stratum statistics
strata_stats = plot_data.group_by("STRATUM_CN").agg([
    pl.count("PLT_CN").alias("n_h"),  # Sample size (P2 points)
    pl.mean("forest_prop").alias("ybar_h"),
    pl.var("forest_prop", ddof=1).alias("s2_yh"),
    pl.first("EXPNS").alias("w_h"),
    pl.first("P1POINTCNT").alias("N_h"),  # Population size (P1 points)
    pl.first("P2POINTCNT").alias("n_h_check")
])

print("Stratum statistics (first 5):")
print(strata_stats.select(["n_h", "n_h_check", "N_h", "ybar_h", "s2_yh", "w_h"]).head())

# Verify n_h matches P2POINTCNT
assert (strata_stats["n_h"] == strata_stats["n_h_check"]).all(), "n_h doesn't match P2POINTCNT!"

# Calculate total estimate
total_estimate = (strata_stats["ybar_h"] * strata_stats["w_h"] * strata_stats["n_h"]).sum()
print(f"\nTotal forest area: {total_estimate:,.0f} acres")

# Calculate variance using proper two-phase formula
# V(Ŷ) = Σ_h [N_h² × (1 - n_h/N_h) × s²_yh / n_h]
#
# But wait, EXPNS already includes N_h/n_h scaling:
# EXPNS = (N_h × acres_per_P1_point) / n_h
#
# So we can rewrite:
# V(Ŷ) = Σ_h [EXPNS² × n_h × (1 - n_h/N_h) × s²_yh / n_h]
#      = Σ_h [EXPNS² × (1 - n_h/N_h) × s²_yh]

strata_stats = strata_stats.with_columns([
    (1 - pl.col("n_h") / pl.col("N_h")).alias("fpc"),
    (pl.col("w_h") ** 2 * (1 - pl.col("n_h") / pl.col("N_h")) * pl.col("s2_yh")).alias("var_h")
])

print("\nVariance components (first 5):")
print(strata_stats.select(["n_h", "N_h", "fpc", "s2_yh", "var_h"]).head())

total_variance = strata_stats["var_h"].sum()
se = total_variance ** 0.5
se_percent = 100 * se / total_estimate

print(f"\nResults with proper two-phase variance:")
print(f"  Total variance: {total_variance:,.0f}")
print(f"  SE: {se:,.0f} acres")
print(f"  SE%: {se_percent:.3f}%")
print(f"  Total estimate: {total_estimate:,.0f} acres")

print(f"\nComparison with EVALIDator:")
print(f"  EVALIDator forest area: 24,172,679 acres")
print(f"  EVALIDator SE%: 0.563%")
print(f"  EVALIDator SE: {24_172_679 * 0.00563:,.0f} acres")

print(f"\nOur results:")
print(f"  Area difference: {(total_estimate - 24_172_679) / 24_172_679 * 100:+.2f}%")
print(f"  SE% ratio: {se_percent / 0.563:.2f}x")

# The SE should now be much closer!