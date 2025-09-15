#!/usr/bin/env python
"""Debug variance calculation step by step."""

import polars as pl
from pyfia import FIA
from pyfia.estimation.estimators.volume import VolumeEstimator

db_path = "data/nfi_south.duckdb"

with FIA(db_path) as db:
    db.clip_by_state(48, most_recent=True, eval_type="VOL")

    # Create estimator
    config = {
        "land_type": "forest",
        "tree_type": "live",
        "vol_type": "net",
        "totals": True
    }

    estimator = VolumeEstimator(db, config)
    results = estimator.estimate()

    # Now manually run through the variance calculation
    if estimator.plot_tree_data is not None:
        print("Step 1: Aggregate to plot-condition level")
        print("-" * 60)

        plot_cond_data = estimator.plot_tree_data.group_by(
            ["PLT_CN", "CONDID", "STRATUM_CN", "EXPNS", "CONDPROP_UNADJ"]
        ).agg([
            pl.sum("VOLUME_ADJ").alias("y_ic"),  # Volume per condition
        ])

        print(f"Plot-condition aggregation shape: {plot_cond_data.shape}")
        print(f"Sample rows:")
        print(plot_cond_data.head(5))

        # Check for nulls in aggregated data
        print(f"\nNull y_ic values: {plot_cond_data['y_ic'].is_null().sum()}")

        # Filter out nulls for further processing
        plot_cond_data = plot_cond_data.filter(pl.col("y_ic").is_not_null())
        print(f"After filtering nulls: {plot_cond_data.shape}")

        print("\n\nStep 2: Aggregate to plot level")
        print("-" * 60)

        plot_data = plot_cond_data.group_by(
            ["PLT_CN", "STRATUM_CN", "EXPNS"]
        ).agg([
            pl.sum("y_ic").alias("y_i"),  # Total volume per plot
            pl.sum("CONDPROP_UNADJ").alias("x_i")  # Total area per plot
        ])

        print(f"Plot aggregation shape: {plot_data.shape}")
        print(f"Sample rows:")
        print(plot_data.head(5))

        print(f"\nPlot-level statistics:")
        print(f"  Mean y_i (volume): {plot_data['y_i'].mean():.2f}")
        print(f"  Std y_i: {plot_data['y_i'].std():.2f}")
        print(f"  Mean x_i (area): {plot_data['x_i'].mean():.4f}")
        print(f"  Std x_i: {plot_data['x_i'].std():.4f}")

        print("\n\nStep 3: Calculate stratum statistics")
        print("-" * 60)

        strat_cols = ["STRATUM_CN"]
        strata_stats = plot_data.group_by(strat_cols).agg([
            pl.count("PLT_CN").alias("n_h"),
            pl.mean("y_i").alias("ybar_h"),
            pl.mean("x_i").alias("xbar_h"),
            pl.var("y_i", ddof=1).alias("s2_yh"),
            pl.var("x_i", ddof=1).alias("s2_xh"),
            pl.first("EXPNS").cast(pl.Float64).alias("w_h"),
            # Covariance
            ((pl.col("y_i") - pl.col("y_i").mean()) *
             (pl.col("x_i") - pl.col("x_i").mean())).mean().alias("cov_yxh")
        ])

        print(f"Stratum statistics shape: {strata_stats.shape}")
        print(f"First few strata:")
        print(strata_stats.head())

        # Check for nulls
        print(f"\nNull variance values: {strata_stats['s2_yh'].is_null().sum()}")

        # Handle nulls
        strata_stats = strata_stats.with_columns([
            pl.when(pl.col("s2_yh").is_null()).then(0.0).otherwise(pl.col("s2_yh")).alias("s2_yh"),
            pl.when(pl.col("s2_xh").is_null()).then(0.0).otherwise(pl.col("s2_xh")).alias("s2_xh"),
            pl.when(pl.col("cov_yxh").is_null()).then(0.0).otherwise(pl.col("cov_yxh")).alias("cov_yxh")
        ])

        print("\n\nStep 4: Calculate ratio and variance components")
        print("-" * 60)

        # Calculate totals
        total_y = (strata_stats["ybar_h"] * strata_stats["w_h"] * strata_stats["n_h"]).sum()
        total_x = (strata_stats["xbar_h"] * strata_stats["w_h"] * strata_stats["n_h"]).sum()
        ratio = total_y / total_x if total_x > 0 else 0

        print(f"Total Y (volume): {total_y:.2f}")
        print(f"Total X (area): {total_x:.4f}")
        print(f"Ratio (Y/X): {ratio:.2f}")

        # Calculate variance components
        variance_components = strata_stats.with_columns([
            (pl.col("w_h") ** 2 *
             (pl.col("s2_yh") +
              ratio ** 2 * pl.col("s2_xh") -
              2 * ratio * pl.col("cov_yxh")) /
             pl.col("n_h")
            ).alias("v_h")
        ])

        print(f"\nVariance components:")
        print(variance_components.select(["STRATUM_CN", "n_h", "s2_yh", "s2_xh", "v_h"]).head())

        total_variance = variance_components["v_h"].sum()
        print(f"\nTotal variance: {total_variance:.6f}")
        print(f"Standard error: {total_variance ** 0.5:.6f}")