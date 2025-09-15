#!/usr/bin/env python
"""Test the correct variance formula based on FIA methodology.

Key insight: For domain estimation (forest area), we need to use the
variance formula for estimating a domain total, not a population mean.
"""

import polars as pl
from pyfia import FIA
import math

db = FIA("data/georgia.duckdb")
db.clip_most_recent(eval_type="ALL")

# Get data
db.load_table("COND", columns=["PLT_CN", "CONDID", "CONDPROP_UNADJ", "COND_STATUS_CD"])
db.load_table("POP_PLOT_STRATUM_ASSGN")
db.load_table("POP_STRATUM")

# Get ALL plots (including non-forest) for proper variance
ppsa = db.tables["POP_PLOT_STRATUM_ASSGN"].filter(pl.col("EVALID").is_in(db.evalid)).collect()
strat = db.tables["POP_STRATUM"].filter(pl.col("EVALID").is_in(db.evalid)).unique(subset=["CN"]).collect()

# Get forest conditions
cond = db.tables["COND"].collect()

# Create forest indicator (0 for non-forest, proportion for forest)
cond = cond.with_columns([
    pl.when(pl.col("COND_STATUS_CD") == 1)
      .then(pl.col("CONDPROP_UNADJ"))
      .otherwise(0.0)
      .alias("forest_prop")
])

# Join ALL plots with conditions (left join to keep non-forest plots)
data = ppsa.join(
    cond.group_by("PLT_CN").agg(pl.col("forest_prop").sum()),
    on="PLT_CN",
    how="left"
)
data = data.with_columns([pl.col("forest_prop").fill_null(0.0)])

print("=" * 80)
print("CORRECT FIA VARIANCE CALCULATION FOR DOMAIN ESTIMATION")
print("=" * 80)

print(f"\n1. DATA SUMMARY:")
print(f"   Total plots (ALL): {len(data)}")
print(f"   Forest plots: {(data['forest_prop'] > 0).sum()}")
print(f"   Non-forest plots: {(data['forest_prop'] == 0).sum()}")

# Join with stratum data
plot_data = data.join(strat, left_on="STRATUM_CN", right_on="CN")

# Aggregate to plot level (already done)
plot_level = plot_data.select([
    "PLT_CN", "STRATUM_CN", "forest_prop", "EXPNS",
    "P1POINTCNT", "P2POINTCNT", "ADJ_FACTOR_SUBP"
])

# Calculate stratum statistics using ALL plots
strata_stats = plot_level.group_by("STRATUM_CN").agg([
    pl.count("PLT_CN").alias("n_h"),  # ALL plots in stratum
    pl.mean("forest_prop").alias("ybar_h"),
    pl.var("forest_prop", ddof=1).alias("s2_yh"),
    pl.first("EXPNS").alias("w_h"),
    pl.first("P1POINTCNT").alias("N_h"),
    pl.first("P2POINTCNT").alias("n_h_check"),
    pl.first("ADJ_FACTOR_SUBP").alias("adj")
])

# Verify n_h matches P2POINTCNT
assert (strata_stats["n_h"] == strata_stats["n_h_check"]).all(), "n_h doesn't match P2POINTCNT"
print(f"\n2. VERIFICATION: n_h matches P2POINTCNT for all strata ✓")

# Calculate total area estimate
total_area = (plot_level["forest_prop"] * plot_level["EXPNS"] * plot_level["ADJ_FACTOR_SUBP"]).sum()
print(f"\n3. FOREST AREA ESTIMATE:")
print(f"   Total: {total_area:,.0f} acres")

# CORRECT VARIANCE FORMULA for domain estimation
# When estimating a domain total (subset of population):
# V(Ŷ_D) = Σ_h [N_h² × (1 - f_h) × s²_yDh / n_h]
#
# Where:
# - yDh values include zeros for non-domain units
# - n_h includes ALL sampled units (not just domain units)
# - s²_yDh is variance across ALL units (including zeros)
#
# Since EXPNS = N_h × acres_per_unit / n_h, and N_h >> n_h:
# V(Ŷ_D) ≈ Σ_h [EXPNS² × n_h × s²_yDh / n_h]
#        = Σ_h [EXPNS² × s²_yDh]

print(f"\n4. VARIANCE CALCULATION METHODS:")

# Method A: Simple formula (w² × s²)
variance_A = (strata_stats["w_h"] ** 2 * strata_stats["adj"] ** 2 * strata_stats["s2_yh"]).sum()
se_A = math.sqrt(variance_A)
se_pct_A = 100 * se_A / total_area
print(f"\n   Method A (w² × adj² × s²):")
print(f"   SE = {se_A:,.0f} acres")
print(f"   SE% = {se_pct_A:.4f}%")

# Method B: With finite population correction
fpc = 1 - strata_stats["n_h"] / strata_stats["N_h"]
variance_B = (strata_stats["w_h"] ** 2 * strata_stats["adj"] ** 2 * fpc * strata_stats["s2_yh"]).sum()
se_B = math.sqrt(variance_B)
se_pct_B = 100 * se_B / total_area
print(f"\n   Method B (with FPC):")
print(f"   SE = {se_B:,.0f} acres")
print(f"   SE% = {se_pct_B:.4f}%")
print(f"   Mean FPC = {fpc.mean():.6f}")

# Method C: Multiply by n_h (for when we're summing, not averaging)
variance_C = (strata_stats["w_h"] ** 2 * strata_stats["adj"] ** 2 * strata_stats["s2_yh"] * strata_stats["n_h"]).sum()
se_C = math.sqrt(variance_C)
se_pct_C = 100 * se_C / total_area
print(f"\n   Method C (w² × adj² × s² × n):")
print(f"   SE = {se_C:,.0f} acres")
print(f"   SE% = {se_pct_C:.4f}%")

# Method D: The key insight - we need to account for the two-phase design
# In two-phase sampling, the variance has an additional component
# The correct formula includes both phase 1 and phase 2 variance
# For now, let's try scaling by sqrt of total P2 plots
total_p2 = strata_stats["n_h"].sum()
variance_D = variance_A * total_p2
se_D = math.sqrt(variance_D)
se_pct_D = 100 * se_D / total_area
print(f"\n   Method D (Method A × n_total = {total_p2}):")
print(f"   SE = {se_D:,.0f} acres")
print(f"   SE% = {se_pct_D:.4f}%")

print(f"\n5. COMPARISON WITH EVALDATOR:")
target_se_pct = 0.563
print(f"   Target SE% = {target_se_pct}%")
print(f"   Target SE = {total_area * target_se_pct / 100:,.0f} acres")
print(f"\n   Method comparison (ratio to target):")
print(f"   Method A: {se_pct_A / target_se_pct:.3f}x")
print(f"   Method B: {se_pct_B / target_se_pct:.3f}x")
print(f"   Method C: {se_pct_C / target_se_pct:.3f}x")
print(f"   Method D: {se_pct_D / target_se_pct:.3f}x")

# Check some stratum details
print(f"\n6. STRATUM DETAILS (first 3):")
for row in strata_stats.head(3).iter_rows(named=True):
    print(f"\n   Stratum {row['STRATUM_CN'][:8]}...:")
    print(f"   n = {row['n_h']}, N = {row['N_h']:,}")
    print(f"   mean = {row['ybar_h']:.4f}, var = {row['s2_yh']:.6f}")
    print(f"   EXPNS = {row['w_h']:.2f}, ADJ = {row['adj']:.4f}")

# The missing piece might be related to the adjustment factors
print(f"\n7. ADJUSTMENT FACTOR ANALYSIS:")
print(f"   Unique ADJ_FACTOR_SUBP values: {strat['ADJ_FACTOR_SUBP'].unique()}")
print(f"   Mean ADJ_FACTOR_SUBP: {strat['ADJ_FACTOR_SUBP'].mean():.4f}")
print(f"   This might be the missing scaling factor!")