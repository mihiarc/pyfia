#!/usr/bin/env python
"""Debug why variance components are NaN."""

import polars as pl
from pyfia import FIA
from pyfia.estimation.estimators.volume import VolumeEstimator

db_path = "data/nfi_south.duckdb"

with FIA(db_path) as db:
    db.clip_by_state(48, most_recent=True, eval_type="VOL")

    estimator = VolumeEstimator(db, {"land_type": "forest", "tree_type": "live", "vol_type": "net", "totals": True})
    estimator.estimate()

    # Manually run variance calculation
    plot_tree_data = estimator.plot_tree_data
    plot_cond_data = plot_tree_data.group_by(
        ["PLT_CN", "CONDID", "STRATUM_CN", "EXPNS", "CONDPROP_UNADJ"]
    ).agg([pl.sum("VOLUME_ADJ").alias("y_ic")])

    plot_data = plot_cond_data.group_by(["PLT_CN", "STRATUM_CN", "EXPNS"]).agg([
        pl.sum("y_ic").alias("y_i"),
        pl.sum("CONDPROP_UNADJ").cast(pl.Float64).alias("x_i")
    ])

    # Check one stratum in detail
    sample_stratum = plot_data["STRATUM_CN"][0]
    stratum_data = plot_data.filter(pl.col("STRATUM_CN") == sample_stratum)

    print(f"Sample stratum: {sample_stratum}")
    print(f"Number of plots: {len(stratum_data)}")
    print(f"y_i values (first 5): {stratum_data['y_i'].head(5).to_list()}")
    print(f"x_i values (first 5): {stratum_data['x_i'].head(5).to_list()}")

    # Check if x_i has any variance
    x_variance = stratum_data["x_i"].var(ddof=1)
    y_variance = stratum_data["y_i"].var(ddof=1)

    print(f"\nx_i variance: {x_variance}")
    print(f"y_i variance: {y_variance}")

    # Check if all x_i are the same value
    unique_x = stratum_data["x_i"].unique()
    print(f"\nUnique x_i values in stratum: {unique_x.to_list()[:10]}")
    print(f"Total unique x_i values: {len(unique_x)}")