#!/usr/bin/env python
"""Test variance formula directly with sample data."""

import polars as pl
import numpy as np

# Create sample data similar to what we have
data = pl.DataFrame({
    "PLT_CN": ["P1", "P2", "P3", "P4", "P5"],
    "STRATUM_CN": ["S1", "S1", "S1", "S1", "S1"],
    "y_i": [100.0, 200.0, 150.0, 300.0, 250.0],  # Volume per plot
    "x_i": [1.0, 0.9, 1.0, 0.8, 1.0],  # Area per plot
    "EXPNS": [1000.0, 1000.0, 1000.0, 1000.0, 1000.0]  # Expansion factor
})

print("Sample data:")
print(data)

# Calculate stratum statistics
strata_stats = data.group_by("STRATUM_CN").agg([
    pl.count("PLT_CN").alias("n_h"),
    pl.mean("y_i").alias("ybar_h"),
    pl.mean("x_i").alias("xbar_h"),
    pl.var("y_i", ddof=1).alias("s2_yh"),
    pl.var("x_i", ddof=1).alias("s2_xh"),
    pl.first("EXPNS").alias("w_h"),
    # Covariance
    (((pl.col("y_i") - pl.col("y_i").mean()) *
      (pl.col("x_i") - pl.col("x_i").mean())).sum() /
     (pl.len() - 1)).alias("cov_yxh")
])

print("\nStratum statistics:")
print(strata_stats)

# Calculate ratio
row = strata_stats[0]
total_y = row["ybar_h"][0] * row["w_h"][0] * row["n_h"][0]
total_x = row["xbar_h"][0] * row["w_h"][0] * row["n_h"][0]
ratio = total_y / total_x if total_x > 0 else 0

print(f"\nTotal Y: {total_y}")
print(f"Total X: {total_x}")
print(f"Ratio: {ratio}")

# Calculate variance component manually
s2_yh = row["s2_yh"][0]
s2_xh = row["s2_xh"][0]
cov_yxh = row["cov_yxh"][0]
w_h = row["w_h"][0]
n_h = row["n_h"][0]

print(f"\nVariance components:")
print(f"  s2_yh: {s2_yh}")
print(f"  s2_xh: {s2_xh}")
print(f"  cov_yxh: {cov_yxh}")

# Variance formula for ratio
v_h = (w_h ** 2) * (s2_yh + ratio**2 * s2_xh - 2 * ratio * cov_yxh) / n_h

print(f"\nVariance component (v_h): {v_h}")
print(f"Standard error: {v_h ** 0.5}")