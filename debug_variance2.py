#!/usr/bin/env python
"""Debug the variance calculation more thoroughly."""

import polars as pl
from pyfia import FIA
from pyfia.estimation.estimators.area import AreaEstimator

# Create test estimator
db = FIA("data/georgia.duckdb")
db.clip_most_recent(eval_type="ALL")

config = {
    "land_type": "forest",
    "area_domain": None,
    "grp_by": None,
    "variance": True,
    "totals": True
}

estimator = AreaEstimator(db, config)

# Load and process data
data = estimator.load_data()
data = estimator.apply_filters(data)
data = estimator.calculate_values(data)
results = estimator.aggregate_results(data)

print("Initial results (before variance):")
print(results)
print(f"\nArea total: {results['AREA_TOTAL'][0]:,.0f}")

# Now look at the plot-condition data
print(f"\nPlot-condition data shape: {estimator.plot_condition_data.shape}")
print(f"Columns: {estimator.plot_condition_data.columns}")

# Calculate condition areas
cond_data = estimator.plot_condition_data.with_columns([
    (pl.col("AREA_VALUE").cast(pl.Float64) *
     pl.col("ADJ_FACTOR_AREA").cast(pl.Float64)).alias("h_ic")
])

print(f"\nSample condition areas (h_ic):")
print(cond_data.select(["PLT_CN", "AREA_VALUE", "ADJ_FACTOR_AREA", "h_ic", "EXPNS"]).head(10))

# Aggregate to plot level
plot_data = cond_data.group_by(
    ["PLT_CN", "ESTN_UNIT", "STRATUM_CN", "EXPNS"]
).agg([
    pl.sum("h_ic").alias("y_i")
])

print(f"\nPlot data after aggregation:")
print(f"Shape: {plot_data.shape}")
print(f"Sample rows:")
print(plot_data.head(10))
print(f"\ny_i statistics:")
print(f"  Min: {plot_data['y_i'].min()}")
print(f"  Max: {plot_data['y_i'].max()}")
print(f"  Mean: {plot_data['y_i'].mean()}")

# Calculate stratum statistics
strata_stats = plot_data.group_by(["ESTN_UNIT", "STRATUM_CN"]).agg([
    pl.count("PLT_CN").alias("n_h"),
    pl.mean("y_i").alias("ybar_h"),
    pl.var("y_i", ddof=1).alias("s2_yh"),
    pl.first("EXPNS").alias("w_h")
])

print(f"\nStratum statistics (first 10):")
print(strata_stats.head(10))

# Calculate total estimate the correct way
total_estimate_correct = (strata_stats["ybar_h"] * strata_stats["w_h"]).sum()
print(f"\nTotal estimate from strata: {total_estimate_correct:,.0f}")

# Compare with the actual area total
print(f"Area total from results: {results['AREA_TOTAL'][0]:,.0f}")
print(f"Ratio: {results['AREA_TOTAL'][0] / total_estimate_correct:.2f}")

# The issue might be that we're calculating at the wrong level
# Let's check the total expansion
total_expns = estimator.plot_condition_data["EXPNS"].sum()
print(f"\nTotal EXPNS (sum): {total_expns:,.0f}")
unique_expns = estimator.plot_condition_data.group_by("PLT_CN").agg(
    pl.first("EXPNS").alias("EXPNS")
)["EXPNS"].sum()
print(f"Total EXPNS (unique plots): {unique_expns:,.0f}")