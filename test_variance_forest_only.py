#!/usr/bin/env python
"""Calculate variance using only forest plots as you correctly pointed out."""

import polars as pl
from pyfia import FIA

db = FIA("data/georgia.duckdb")
db.clip_most_recent(eval_type="ALL")

# Get ONLY forest conditions
db.load_table("COND", columns=["PLT_CN", "CONDID", "CONDPROP_UNADJ", "COND_STATUS_CD"])
db.load_table("POP_PLOT_STRATUM_ASSGN")
db.load_table("POP_STRATUM")

# Filter to forest only
cond = db.tables["COND"].filter(pl.col("COND_STATUS_CD") == 1).collect()
ppsa = db.tables["POP_PLOT_STRATUM_ASSGN"].filter(pl.col("EVALID").is_in(db.evalid)).collect()
strat = db.tables["POP_STRATUM"].filter(pl.col("EVALID").is_in(db.evalid)).unique(subset=["CN"]).collect()

# Join forest conditions with stratification
data = cond.join(ppsa, on="PLT_CN", how="inner")

print(f"Forest plots only:")
print(f"  Total forest conditions: {len(data)}")
print(f"  Unique forest plots: {data['PLT_CN'].n_unique()}")

# Aggregate to plot level
plot_data = data.group_by(["PLT_CN", "STRATUM_CN"]).agg([
    pl.col("CONDPROP_UNADJ").sum().alias("forest_prop")
])

# Join with stratum data
plot_data = plot_data.join(strat, left_on="STRATUM_CN", right_on="CN")

# Calculate stratum statistics using ONLY forest plots
strata_stats = plot_data.group_by("STRATUM_CN").agg([
    pl.count("PLT_CN").alias("n_h"),  # Number of FOREST plots in stratum
    pl.mean("forest_prop").alias("ybar_h"),
    pl.var("forest_prop", ddof=1).alias("s2_yh"),
    pl.first("EXPNS").alias("w_h"),
    pl.first("P1POINTCNT").alias("N_h"),  # Total P1 points (population)
    pl.first("P2POINTCNT").alias("P2_total")  # Total P2 points (all types)
])

print(f"\nStratum statistics (first 5):")
cols_to_show = ["n_h", "P2_total", "N_h", "ybar_h", "s2_yh"]
print(strata_stats.select(cols_to_show).head())

print(f"\nKey insight:")
print(f"  n_h = number of FOREST plots only")
print(f"  P2_total = total P2 plots (forest + non-forest)")
print(f"  N_h = P1POINTCNT (total population)")

# Now the variance formula for estimating forest area:
# We're estimating the total of a domain (forest)
# The variance formula is:
# V(Ŷ) = Σ_h [N_h² × (1 - n_h/N_h) × s²_yh / n_h]
#
# But since n_h << N_h, the FPC ≈ 1
# And since EXPNS = N_h × acres_per_P1 / P2_total
# We need to be careful about the scaling

# Actually, for domain estimation, the formula is different
# We need to account for the fact that we're estimating over a subset
# The correct formula uses the P2_total for the sampling fraction

strata_stats = strata_stats.with_columns([
    # Use P2_total for the sampling fraction, not n_h
    (1 - pl.col("P2_total") / pl.col("N_h")).alias("fpc"),
    # But use n_h for the variance calculation
    pl.when(pl.col("n_h") > 0)
      .then(pl.col("w_h") ** 2 * (1 - pl.col("P2_total") / pl.col("N_h")) * pl.col("s2_yh") / pl.col("n_h"))
      .otherwise(0.0)
      .alias("var_h")
])

print(f"\nVariance components (first 5):")
print(strata_stats.select(["n_h", "P2_total", "N_h", "fpc", "var_h"]).head())

# Calculate total
total_estimate = (plot_data["forest_prop"] * plot_data["EXPNS"]).sum()
total_variance = strata_stats["var_h"].sum()
se = total_variance ** 0.5
se_percent = 100 * se / total_estimate

print(f"\nResults:")
print(f"  Total forest area: {total_estimate:,.0f} acres")
print(f"  Total variance: {total_variance:,.0f}")
print(f"  SE: {se:,.0f} acres")
print(f"  SE%: {se_percent:.3f}%")

print(f"\nTarget from EVALIDator:")
print(f"  SE%: 0.563%")
print(f"  SE: ~136,000 acres")
print(f"\nRatio: {se_percent / 0.563:.2f}x")

# Try another approach: maybe we need to scale by n_h/P2_total
print(f"\n--- Alternative calculation ---")
# The issue might be that EXPNS is defined per P2 plot (all plots)
# But we're only using forest plots
# So we need to adjust the expansion factor

strata_stats = strata_stats.with_columns([
    # Adjusted expansion for forest plots only
    (pl.col("w_h") * pl.col("P2_total") / pl.col("n_h")).alias("w_h_adjusted"),
])

strata_stats = strata_stats.with_columns([
    (pl.col("w_h_adjusted") ** 2 * (1 - pl.col("P2_total") / pl.col("N_h")) * pl.col("s2_yh") / pl.col("P2_total"))
      .alias("var_h_alt")
])

total_variance_alt = strata_stats["var_h_alt"].sum()
se_alt = total_variance_alt ** 0.5
se_percent_alt = 100 * se_alt / total_estimate

print(f"Alternative approach:")
print(f"  SE: {se_alt:,.0f} acres")
print(f"  SE%: {se_percent_alt:.3f}%")
print(f"  Ratio: {se_percent_alt / 0.563:.2f}x")