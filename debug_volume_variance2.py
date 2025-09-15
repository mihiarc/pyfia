#!/usr/bin/env python
"""Debug the volume variance calculation - check non-null values."""

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

    # Check data preservation
    if estimator.plot_tree_data is not None:
        print(f"\nData preservation analysis:")
        print(f"  Total rows: {len(estimator.plot_tree_data)}")

        # Check for null values
        null_counts = estimator.plot_tree_data.null_count()
        print(f"\nNull counts per column:")
        print(null_counts)

        # Check non-null VOLUME_ADJ
        non_null_volume = estimator.plot_tree_data.filter(pl.col("VOLUME_ADJ").is_not_null())
        print(f"\nRows with non-null VOLUME_ADJ: {len(non_null_volume)}")

        if len(non_null_volume) > 0:
            print(f"\nSample of non-null VOLUME_ADJ rows:")
            print(non_null_volume.head(5))

            # Check variance calculation on non-null data
            print(f"\nVolume statistics:")
            print(f"  Mean VOLUME_ADJ: {non_null_volume['VOLUME_ADJ'].mean():.2f}")
            print(f"  Std VOLUME_ADJ: {non_null_volume['VOLUME_ADJ'].std():.2f}")
            print(f"  Min VOLUME_ADJ: {non_null_volume['VOLUME_ADJ'].min():.2f}")
            print(f"  Max VOLUME_ADJ: {non_null_volume['VOLUME_ADJ'].max():.2f}")

    print(f"\nFinal Results:")
    print(f"  VOLCFNET_ACRE: {results['VOLCFNET_ACRE'][0]:.2f}")
    print(f"  VOLCFNET_ACRE_SE: {results['VOLCFNET_ACRE_SE'][0]:.4f}")

    # Check intermediate calculation
    if "VOLUME_ACRE_VARIANCE" in results.columns:
        print(f"  VOLUME_ACRE_VARIANCE: {results['VOLUME_ACRE_VARIANCE'][0]}")
    if "VOLUME_TOTAL_VARIANCE" in results.columns:
        print(f"  VOLUME_TOTAL_VARIANCE: {results['VOLUME_TOTAL_VARIANCE'][0]}")