#!/usr/bin/env python
"""Understand the correct variance scaling for FIA two-phase sampling."""

import polars as pl
from pyfia import FIA
import math

db = FIA("data/georgia.duckdb")
db.clip_most_recent(eval_type="ALL")

# Get all the relevant tables
db.load_table("COND", columns=["PLT_CN", "CONDID", "CONDPROP_UNADJ", "COND_STATUS_CD"])
db.load_table("POP_PLOT_STRATUM_ASSGN")
db.load_table("POP_STRATUM")

# Get forest conditions
cond = db.tables["COND"].filter(pl.col("COND_STATUS_CD") == 1).collect()
ppsa = db.tables["POP_PLOT_STRATUM_ASSGN"].filter(pl.col("EVALID").is_in(db.evalid)).collect()
strat = db.tables["POP_STRATUM"].filter(pl.col("EVALID").is_in(db.evalid)).unique(subset=["CN"]).collect()

print("=" * 80)
print("UNDERSTANDING FIA TWO-PHASE SAMPLING STRUCTURE")
print("=" * 80)

# Join and aggregate to plot level
data = cond.join(ppsa, on="PLT_CN", how="inner")
plot_data = data.group_by(["PLT_CN", "STRATUM_CN"]).agg([
    pl.col("CONDPROP_UNADJ").sum().alias("forest_prop")
])
plot_data = plot_data.join(strat, left_on="STRATUM_CN", right_on="CN")

print(f"\n1. BASIC STATISTICS:")
print(f"   Forest plots (n): {len(plot_data)}")
print(f"   Unique strata: {plot_data['STRATUM_CN'].n_unique()}")

# Key insight: In FIA two-phase sampling:
# Phase 1: Remote sensing (photo points) - P1POINTCNT
# Phase 2: Field plots (subset of P1) - P2POINTCNT (which equals our n_h)

print(f"\n2. PHASE 1 vs PHASE 2:")
print(f"   Total P1 points (N): {strat['P1POINTCNT'].sum():,}")
print(f"   Total P2 points (n): {strat['P2POINTCNT'].sum():,}")
print(f"   Sampling rate: {strat['P2POINTCNT'].sum() / strat['P1POINTCNT'].sum():.6f}")

# Each P1 point represents 0.222395 acres
P1_ACRES = 0.222395
print(f"   Each P1 point = {P1_ACRES} acres")
print(f"   Total area = {strat['P1POINTCNT'].sum() * P1_ACRES:,.0f} acres")

# Calculate stratum statistics
strata_stats = plot_data.group_by("STRATUM_CN").agg([
    pl.count("PLT_CN").alias("n_h"),  # Forest plots in stratum
    pl.mean("forest_prop").alias("ybar_h"),
    pl.var("forest_prop", ddof=1).alias("s2_yh"),
    pl.first("EXPNS").alias("w_h"),
    pl.first("P1POINTCNT").alias("N_h"),
    pl.first("P2POINTCNT").alias("n_h_total")  # Total P2 points (forest + non-forest)
])

print(f"\n3. STRATUM-LEVEL ANALYSIS (first 5 strata):")
print(strata_stats.select(["n_h", "n_h_total", "N_h", "ybar_h", "s2_yh"]).head())

# The key realization:
# - n_h (forest plots) < n_h_total (all P2 plots)
# - N_h (P1 points) >> n_h_total (P2 points)
# - Variance should use n_h_total for sampling fraction, but n_h for calculation

print(f"\n4. VARIANCE CALCULATION APPROACHES:")

# Approach 1: Standard stratified variance (what we've been trying)
variance_1 = strata_stats.with_columns([
    (pl.col("w_h") ** 2 * pl.col("s2_yh") / pl.col("n_h")).alias("var_component")
])["var_component"].sum()
se_1 = math.sqrt(variance_1)
total_area = (plot_data["forest_prop"] * plot_data["EXPNS"]).sum()
se_pct_1 = 100 * se_1 / total_area

print(f"\n   Approach 1 (w²×s²/n):")
print(f"   SE = {se_1:,.0f} acres")
print(f"   SE% = {se_pct_1:.4f}%")

# Approach 2: With finite population correction using P1/P2
variance_2 = strata_stats.with_columns([
    (pl.col("w_h") ** 2 * (1 - pl.col("n_h_total") / pl.col("N_h")) *
     pl.col("s2_yh") / pl.col("n_h")).alias("var_component")
])["var_component"].sum()
se_2 = math.sqrt(variance_2)
se_pct_2 = 100 * se_2 / total_area

print(f"\n   Approach 2 (with FPC using P2/P1):")
print(f"   SE = {se_2:,.0f} acres")
print(f"   SE% = {se_pct_2:.4f}%")

# Approach 3: Scale by n_h_total/n_h ratio
# Theory: We're using only forest plots but variance should account for all plots
avg_ratio = (strata_stats["n_h_total"] / strata_stats["n_h"]).mean()
variance_3 = variance_1 * avg_ratio
se_3 = math.sqrt(variance_3)
se_pct_3 = 100 * se_3 / total_area

print(f"\n   Approach 3 (scaled by n_total/n_forest = {avg_ratio:.2f}):")
print(f"   SE = {se_3:,.0f} acres")
print(f"   SE% = {se_pct_3:.4f}%")

# Approach 4: Use n_h × s²_yh (not dividing by n_h)
# This is for when we're estimating a total, not a mean
variance_4 = strata_stats.with_columns([
    (pl.col("w_h") ** 2 * pl.col("s2_yh") * pl.col("n_h")).alias("var_component")
])["var_component"].sum()
se_4 = math.sqrt(variance_4)
se_pct_4 = 100 * se_4 / total_area

print(f"\n   Approach 4 (w²×s²×n - for totals):")
print(f"   SE = {se_4:,.0f} acres")
print(f"   SE% = {se_pct_4:.4f}%")

# What's the target?
target_se_pct = 0.563
target_se = total_area * target_se_pct / 100

print(f"\n5. COMPARISON WITH TARGET:")
print(f"   Target SE% = {target_se_pct}%")
print(f"   Target SE = {target_se:,.0f} acres")
print(f"\n   Our approaches vs target:")
print(f"   Approach 1: {se_pct_1 / target_se_pct:.2f}x")
print(f"   Approach 2: {se_pct_2 / target_se_pct:.2f}x")
print(f"   Approach 3: {se_pct_3 / target_se_pct:.2f}x")
print(f"   Approach 4: {se_pct_4 / target_se_pct:.2f}x")

# Calculate what scaling factor we need
needed_factor = target_se / se_1
print(f"\n6. REQUIRED SCALING:")
print(f"   Need to multiply SE by: {needed_factor:.1f}")
print(f"   Need to multiply variance by: {needed_factor**2:.1f}")

# Check if it's related to any obvious quantity
print(f"\n7. POSSIBLE SCALING RELATIONSHIPS:")
print(f"   sqrt(n_forest_total): {math.sqrt(len(plot_data)):.1f}")
print(f"   sqrt(n_P2_total): {math.sqrt(strat['P2POINTCNT'].sum()):.1f}")
print(f"   sqrt(mean(n_h)): {math.sqrt(strata_stats['n_h'].mean()):.1f}")
print(f"   P2/P1 ratio^(-1/2): {math.sqrt(strat['P1POINTCNT'].sum() / strat['P2POINTCNT'].sum()):.1f}")