#!/usr/bin/env python
"""Debug the volume variance calculation."""

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

    # Run estimation
    print("Running estimation...")
    results = estimator.estimate()

    # Check if data was preserved
    print(f"\nData preservation check:")
    print(f"  plot_tree_data is None: {estimator.plot_tree_data is None}")

    if estimator.plot_tree_data is not None:
        print(f"  plot_tree_data shape: {estimator.plot_tree_data.shape}")
        print(f"  plot_tree_data columns: {estimator.plot_tree_data.columns}")
        print(f"  First few rows:")
        print(estimator.plot_tree_data.head(3))

    print(f"\nResults:")
    print(results)