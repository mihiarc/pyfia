#!/usr/bin/env python
"""Debug what we're using for number of observations in variance calculation."""

import polars as pl
from pyfia import FIA

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

print("=" * 70)
print("UNDERSTANDING THE DATA STRUCTURE")
print("=" * 70)

print(f"\n1. PPSA (Plot-Stratum Assignments):")
print(f"   Total rows: {len(ppsa)}")
print(f"   Unique plots: {ppsa['PLT_CN'].n_unique()}")
print(f"   Unique strata: {ppsa['STRATUM_CN'].n_unique()}")

print(f"\n2. COND (Conditions - forest only):")
print(f"   Total rows: {len(cond)}")
print(f"   Unique plots: {cond['PLT_CN'].n_unique()}")

# Join to see what we get
data = cond.join(ppsa, on="PLT_CN", how="inner")
print(f"\n3. After COND + PPSA join:")
print(f"   Total rows: {len(data)}")
print(f"   Unique plots: {data['PLT_CN'].n_unique()}")
print(f"   Note: {len(data)} rows but {data['PLT_CN'].n_unique()} unique plots")
print(f"   This means average {len(data) / data['PLT_CN'].n_unique():.1f} conditions per plot")

# Check conditions per plot
conds_per_plot = data.group_by("PLT_CN").agg(pl.count().alias("n_conditions"))
print(f"\n4. Conditions per plot distribution:")
print(f"   Plots with 1 condition: {(conds_per_plot['n_conditions'] == 1).sum()}")
print(f"   Plots with 2 conditions: {(conds_per_plot['n_conditions'] == 2).sum()}")
print(f"   Plots with 3+ conditions: {(conds_per_plot['n_conditions'] >= 3).sum()}")
print(f"   Max conditions: {conds_per_plot['n_conditions'].max()}")

# Now aggregate to plot level (what we do for variance)
plot_data = data.group_by(["PLT_CN", "STRATUM_CN", "ESTN_UNIT"]).agg([
    pl.col("CONDPROP_UNADJ").sum().alias("forest_prop"),
])

print(f"\n5. Plot-level data (after aggregation):")
print(f"   Total plots: {len(plot_data)}")
print(f"   Unique plots: {plot_data['PLT_CN'].n_unique()}")
print(f"   These should be the same: {len(plot_data) == plot_data['PLT_CN'].n_unique()}")

# Count plots per stratum (this is n_h)
plots_per_stratum = plot_data.group_by("STRATUM_CN").agg([
    pl.count("PLT_CN").alias("n_h_actual")
])

# Compare with P2POINTCNT
strat_comparison = plots_per_stratum.join(
    strat.select(["CN", "P2POINTCNT"]),
    left_on="STRATUM_CN",
    right_on="CN"
)

print(f"\n6. NUMBER OF OBSERVATIONS (n_h) per stratum:")
print("=" * 70)
print(f"{'Stratum':<20} {'n_h (actual plots)':<20} {'P2POINTCNT':<15} {'Match?':<10}")
print("-" * 70)

for row in strat_comparison.head(10).iter_rows():
    stratum = row[0][:10] + "..."  # Truncate for display
    n_h = row[1]
    p2 = row[2]
    match = "YES" if n_h == p2 else f"NO ({n_h}/{p2})"
    print(f"{stratum:<20} {n_h:<20} {p2:<15} {match:<10}")

# Overall statistics
print(f"\n7. SUMMARY:")
print(f"   Total plots in variance calculation: {plots_per_stratum['n_h_actual'].sum()}")
print(f"   Total P2POINTCNT: {strat['P2POINTCNT'].sum()}")
print(f"   Total forest conditions: {len(data)}")
print(f"   Unique forest plots: {data['PLT_CN'].n_unique()}")

# Check if we're using the right n for variance
print(f"\n8. VARIANCE FORMULA CHECK:")
print(f"   We should be using n_h = number of PLOTS per stratum")
print(f"   NOT number of conditions")
print(f"   Current n_h values range: {plots_per_stratum['n_h_actual'].min()} to {plots_per_stratum['n_h_actual'].max()}")
print(f"   Mean n_h: {plots_per_stratum['n_h_actual'].mean():.1f}")

# The key question: are we dividing by the right n?
print(f"\n9. KEY INSIGHT:")
total_plots = plots_per_stratum['n_h_actual'].sum()
total_conditions = len(data)
print(f"   If we use n = {total_plots} (plots), variance is divided by this")
print(f"   If we use n = {total_conditions} (conditions), variance is divided by this")
print(f"   Ratio: {total_conditions / total_plots:.2f}x")
print(f"\n   Since SE ∝ 1/√n, using conditions instead of plots would make SE")
print(f"   √{total_conditions / total_plots:.2f} = {(total_conditions / total_plots) ** 0.5:.2f}x smaller than it should be")