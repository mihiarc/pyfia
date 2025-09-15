#!/usr/bin/env python
"""Analyze why our variance is 200x too small."""

import polars as pl
from pyfia import FIA
import math

db = FIA("data/georgia.duckdb")
db.clip_most_recent(eval_type="ALL")

# Get the stratification
db.load_table("POP_PLOT_STRATUM_ASSGN")
db.load_table("POP_STRATUM")
ppsa = db.tables["POP_PLOT_STRATUM_ASSGN"].filter(pl.col("EVALID").is_in(db.evalid)).collect()
strat = db.tables["POP_STRATUM"].filter(pl.col("EVALID").is_in(db.evalid)).unique(subset=["CN"]).collect()

print("Understanding the scale issue:")
print("=" * 70)

# Our calculated SE is 671 acres
our_se = 671
# Target SE is 0.563% of 24.17M = 136,092 acres
target_se = 136_092

ratio = target_se / our_se
print(f"Our SE: {our_se:,} acres")
print(f"Target SE: {target_se:,} acres")
print(f"Ratio: {ratio:.1f}x too small")
print(f"Variance ratio: {ratio**2:.1f}x too small")

# Check the expansion factors
print(f"\nExpansion factor (EXPNS) analysis:")
print(f"  Total plots: {len(ppsa)}")
print(f"  Sum of EXPNS: {strat['EXPNS'].sum():,.0f}")
print(f"  Mean EXPNS: {strat['EXPNS'].mean():,.0f}")

# The total land area of Georgia
georgia_area = strat['EXPNS'].sum()  # This might not be right
print(f"\nImplied total area: {georgia_area:,.0f} acres")

# But wait, EXPNS is per stratum, and we have multiple strata
# Let's check the actual total properly
strat_plot_counts = ppsa.group_by("STRATUM_CN").agg(pl.count().alias("n_plots"))
strat_with_counts = strat.join(strat_plot_counts, left_on="CN", right_on="STRATUM_CN")

# Total area = sum over strata of (EXPNS × n_plots_in_stratum)
strat_with_counts = strat_with_counts.with_columns([
    (pl.col("EXPNS") * pl.col("n_plots")).alias("stratum_total_area")
])

total_area_correct = strat_with_counts["stratum_total_area"].sum()
print(f"Correct total area: {total_area_correct:,.0f} acres")
print(f"Georgia's actual land area: ~37.3 million acres")

# So EXPNS × n_plots gives us the total area represented by each stratum
# This means EXPNS is acres per plot

print(f"\nKey insight:")
print(f"  EXPNS represents acres/plot")
print(f"  Total area = Σ(EXPNS × n_plots) = {total_area_correct:,.0f}")

# Now let's think about the variance formula
# For a proportion p with variance s²_p in a stratum with n plots,
# where each plot represents w acres:
#
# Total estimate = Σ(p̄ × w × n) = Σ(p̄ × total_acres_in_stratum)
#
# Variance of total = Σ[Var(p̄ × w × n)]
#                   = Σ[(w × n)² × Var(p̄)]
#                   = Σ[(w × n)² × s²_p / n]
#                   = Σ[w² × n × s²_p]
#
# But we're calculating: Σ[w² × s²_p / n]
#
# So we're off by a factor of n²!

print(f"\nVariance scaling issue:")
print(f"  We calculate: V = Σ[w² × s²_p / n]")
print(f"  Should be:    V = Σ[w² × n × s²_p]")
print(f"  Difference: n² factor")
print(f"  With mean n = {ppsa.group_by('STRATUM_CN').agg(pl.count().alias('n')).select('n').mean().item():.0f}")
print(f"  This gives factor of ~{ppsa.group_by('STRATUM_CN').agg(pl.count().alias('n')).select('n').mean().item()**2:.0f}x")

# Actually, let me reconsider. The standard stratified sampling formula is:
# V(Ŷ_st) = Σ[N_h² × (1 - f_h) × S²_h / n_h]
#
# Where N_h is the population size in stratum h
#
# In our case, N_h isn't the number of plots but the number of acres!
# So N_h = EXPNS × P2POINTCNT (total acres in stratum)
#
# But that doesn't make sense either...

print(f"\nActually, let's think differently:")
print(f"In FIA, each plot is a sample point representing a fixed area")
print(f"The variance formula for stratified sampling of area is:")
print(f"  V = Σ_h [(N_h)² × (1 - n_h/N_h) × s²_h / n_h]")
print(f"Where:")
print(f"  N_h = total number of possible sample points in stratum h")
print(f"  n_h = number of sampled points in stratum h")
print(f"  s²_h = variance of the forest proportion among sampled points")

# In our case, we have n_h = N_h (complete census within each stratum)
# So (1 - n_h/N_h) = 0, giving zero variance!
#
# This suggests that P2POINTCNT is NOT the population size but the sample size
# The true N_h must be much larger

# Let's estimate what N_h should be to get the right variance
# If SE should be 136,092 and we got 671, we need variance to be (136092/671)² = 41,000x larger
# This means we need (1 - n_h/N_h) ≈ 41,000 × (n_h/N_h²) ≈ 41,000/n_h
# So N_h/n_h ≈ sqrt(41,000) ≈ 200

print(f"\nTo get the right variance, we'd need:")
print(f"  N_h/n_h ≈ {math.sqrt(ratio**2):.0f}")
print(f"  This means the true population is ~{math.sqrt(ratio**2):.0f}x larger than our sample")

print(f"\nConclusion:")
print(f"  P2POINTCNT appears to be the SAMPLE size, not population size")
print(f"  We need the true population N_h for each stratum")
print(f"  Or we need a different variance formula that accounts for this")