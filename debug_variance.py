#!/usr/bin/env python
"""Debug the variance calculation to understand what's happening."""

import polars as pl
from pyfia import FIA, area
from pyfia.estimation.estimators.area import AreaEstimator

# Monkey-patch to add debug output
original_calc = AreaEstimator._calculate_variance_for_group

def debug_calc(self, plot_data, strat_cols):
    print(f"\n=== DEBUG: _calculate_variance_for_group ===")
    print(f"Plot data shape: {plot_data.shape}")
    print(f"Stratification columns: {strat_cols}")

    # Show first few rows
    print(f"\nFirst 5 rows of plot_data:")
    print(plot_data.head())

    # Calculate stratum statistics
    strata_stats = plot_data.group_by(strat_cols).agg([
        pl.count("PLT_CN").alias("n_h"),
        pl.mean("y_i").alias("ybar_h"),
        pl.var("y_i", ddof=1).alias("s2_yh"),
        pl.first("EXPNS").alias("w_h")
    ])

    print(f"\nStratum statistics:")
    print(strata_stats.head(10))

    # Call original
    result = original_calc(self, plot_data, strat_cols)

    print(f"\nVariance calculation result:")
    print(f"  Total variance: {result['variance']:,.2f}")
    print(f"  SE total: {result['se_total']:,.2f}")
    print(f"  SE percent: {result['se_percent']:.3f}%")
    print(f"  Estimate: {result['estimate']:,.0f}")

    return result

AreaEstimator._calculate_variance_for_group = debug_calc

# Test
db = FIA("data/georgia.duckdb")
db.clip_most_recent(eval_type="ALL")
results = area(db, land_type="forest")

print("\n=== FINAL RESULTS ===")
print(f"Area: {results['AREA'][0]:,.0f} acres")
print(f"SE: {results['AREA_SE'][0]:,.0f} acres")
print(f"SE%: {results['AREA_SE_PERCENT'][0]:.3f}%")