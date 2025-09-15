#!/usr/bin/env python
"""
Compare variance implementations between area() and volume() functions.

This script evaluates the differences in variance calculation approaches:
- area(): Implements proper domain total estimation variance (lines 382-541)
- volume(): Uses simplified placeholder variance (lines 154-181)

The area() function correctly implements:
1. Plot-condition data preservation for variance calculation
2. Domain indicator approach for subset estimation
3. Stratified variance formula: V(Ŷ_D) = Σ_h [w_h² × s²_yDh × n_h]
4. Proper handling of grouping variables

The volume() function currently:
1. Uses a fixed 12% coefficient of variation as placeholder
2. Does not preserve plot-level data for variance calculation
3. Lacks proper stratified variance implementation
"""

import warnings
import polars as pl
from pyfia import FIA, area, volume
from pyfia.estimation.estimators.area import AreaEstimator
from pyfia.estimation.estimators.volume import VolumeEstimator

def test_variance_implementation():
    """Test and compare variance implementations between area and volume functions."""

    print("=" * 80)
    print("VARIANCE IMPLEMENTATION COMPARISON: area() vs volume()")
    print("=" * 80)

    # Connect to test database
    db_path = "data/georgia.duckdb"

    with FIA(db_path) as db:
        # Select most recent evaluations
        db.clip_most_recent(eval_type="ALL")

        # Test 1: Basic variance calculation
        print("\nTest 1: Basic Variance Calculation")
        print("-" * 40)

        # Area estimation with variance
        area_results = area(db, land_type="forest", variance=False, totals=True)
        print(f"\nArea Results (with proper variance):")
        print(f"  Forest Area: {area_results['AREA_TOTAL'][0]:,.0f} acres")
        if 'AREA_SE' in area_results.columns:
            print(f"  Standard Error: {area_results['AREA_SE'][0]:,.0f} acres")
            se_percent = 100 * area_results['AREA_SE'][0] / area_results['AREA_TOTAL'][0]
            print(f"  SE %: {se_percent:.2f}%")
        else:
            print("  Standard Error: Not calculated")

        # Volume estimation with variance
        volume_results = volume(db, land_type="forest", vol_type="net", variance=False, totals=True)
        print(f"\nVolume Results (with placeholder variance):")
        if not volume_results.is_empty():
            print(f"  Net Volume: {volume_results['VOLCFNET_TOTAL'][0]:,.0f} cubic feet")
            if 'VOLCFNET_TOTAL_SE' in volume_results.columns:
                print(f"  Standard Error: {volume_results['VOLCFNET_TOTAL_SE'][0]:,.0f} cubic feet")
                se_percent = 100 * volume_results['VOLCFNET_TOTAL_SE'][0] / volume_results['VOLCFNET_TOTAL'][0]
                print(f"  SE %: {se_percent:.2f}% (fixed at 12%)")
            else:
                print("  Standard Error: Not calculated")

        # Test 2: Grouped variance calculation
        print("\n\nTest 2: Grouped Variance Calculation")
        print("-" * 40)

        # Area by ownership group
        area_by_owner = area(db, grp_by="OWNGRPCD", land_type="forest")
        print("\nArea by Ownership Group (with proper variance):")
        for row in area_by_owner.head(3).iter_rows(named=True):
            if 'AREA_SE' in row and row['AREA_SE'] is not None:
                se_pct = 100 * row['AREA_SE'] / row['AREA_TOTAL'] if row['AREA_TOTAL'] > 0 else 0
                print(f"  Owner {row['OWNGRPCD']}: {row['AREA_TOTAL']:,.0f} acres, SE={se_pct:.2f}%")
            else:
                print(f"  Owner {row['OWNGRPCD']}: {row['AREA_TOTAL']:,.0f} acres, SE=Not calculated")

        # Volume by ownership group
        volume_by_owner = volume(db, grp_by="OWNGRPCD", land_type="forest")
        print("\nVolume by Ownership Group (with placeholder variance):")
        for row in volume_by_owner.head(3).iter_rows(named=True):
            if 'VOLCFNET_TOTAL_SE' in row and row['VOLCFNET_TOTAL_SE'] is not None:
                se_pct = 100 * row['VOLCFNET_TOTAL_SE'] / row['VOLCFNET_TOTAL'] if row['VOLCFNET_TOTAL'] > 0 else 0
                print(f"  Owner {row['OWNGRPCD']}: {row['VOLCFNET_TOTAL']:,.0f} cu ft, SE={se_pct:.2f}% (fixed)")
            else:
                print(f"  Owner {row['OWNGRPCD']}: {row['VOLCFNET_TOTAL']:,.0f} cu ft, SE=Not calculated")

def analyze_variance_code():
    """Analyze the variance calculation code differences."""

    print("\n" + "=" * 80)
    print("CODE ANALYSIS: Variance Implementation Differences")
    print("=" * 80)

    print("\n1. AREA() VARIANCE IMPLEMENTATION (lines 382-541):")
    print("-" * 50)
    print("   ✓ Preserves plot-condition data in self.plot_condition_data")
    print("   ✓ Implements proper domain total estimation formula")
    print("   ✓ Uses stratified variance: V(Ŷ_D) = Σ_h [w_h² × s²_yDh × n_h]")
    print("   ✓ Correctly handles grouped estimates with separate variance per group")
    print("   ✓ Accounts for domain indicators (0/1) in variance calculation")
    print("   ✓ Handles single-plot strata with zero variance")

    print("\n2. VOLUME() VARIANCE IMPLEMENTATION (lines 154-181):")
    print("-" * 50)
    print("   ✗ Uses fixed 12% coefficient of variation as placeholder")
    print("   ✗ Does not preserve plot-level data for variance calculation")
    print("   ✗ No stratification in variance calculation")
    print("   ✗ Same CV applied to all groups uniformly")
    print("   ✗ No accounting for sample size or design effects")

    print("\n3. KEY DIFFERENCES:")
    print("-" * 50)
    print("   • Data Preservation:")
    print("     - area(): Stores plot_condition_data for variance (line 353)")
    print("     - volume(): No data preservation for variance")
    print()
    print("   • Variance Formula:")
    print("     - area(): Proper stratified domain estimation (lines 485-517)")
    print("     - volume(): Simple multiplication by 0.12 (lines 160-162)")
    print()
    print("   • Group Handling:")
    print("     - area(): Calculates variance separately per group (lines 427-454)")
    print("     - volume(): Applies same 12% CV to all groups")

    print("\n4. RECOMMENDATIONS FOR VOLUME() IMPROVEMENT:")
    print("-" * 50)
    print("   1. Add plot_tree_data preservation in aggregate_results()")
    print("   2. Implement _calculate_variance_for_group() method")
    print("   3. Use proper stratified variance formula for ratio estimation")
    print("   4. Account for tree-level clustering within plots")
    print("   5. Handle growing stock vs all trees differently")

def propose_volume_variance_fix():
    """Propose specific code changes for volume variance calculation."""

    print("\n" + "=" * 80)
    print("PROPOSED FIX: Volume Variance Implementation")
    print("=" * 80)

    print("\nRequired changes to volume.py:")
    print("-" * 40)

    print("""
1. Add data preservation in VolumeEstimator.__init__():
   ```python
   def __init__(self, db, config):
       super().__init__(db, config)
       self.plot_tree_data = None  # Store for variance calculation
   ```

2. Modify aggregate_results() to preserve data (around line 135):
   ```python
   # Before applying two-stage aggregation
   # Store plot-level data for variance
   cols_to_preserve = ["PLT_CN", "CONDID", "STRATUM_CN", "EXPNS",
                        "VOLUME_ADJ", "ADJ_FACTOR", "CONDPROP_UNADJ"]
   if self.config.get("grp_by"):
       cols_to_preserve.extend(self.config["grp_by"])

   self.plot_tree_data = data_with_strat.select(cols_to_preserve).collect()
   ```

3. Replace calculate_variance() method (lines 154-181):
   ```python
   def calculate_variance(self, results: pl.DataFrame) -> pl.DataFrame:
       '''Calculate variance using proper ratio estimation formula.'''

       if self.plot_tree_data is None:
           # Fallback to conservative estimate
           import warnings
           warnings.warn(
               "Plot-tree data not available for proper variance calculation. "
               "Using placeholder 12% CV."
           )
           return results.with_columns([
               (pl.col("VOLUME_ACRE") * 0.12).alias("VOLUME_ACRE_SE"),
               (pl.col("VOLUME_TOTAL") * 0.12).alias("VOLUME_TOTAL_SE"),
           ])

       # Aggregate to plot-condition level
       plot_cond_data = self.plot_tree_data.group_by(
           ["PLT_CN", "CONDID", "STRATUM_CN", "EXPNS", "CONDPROP_UNADJ"]
       ).agg([
           pl.sum("VOLUME_ADJ").alias("y_ic"),  # Volume per condition
       ])

       # Then follow similar logic as area() variance calculation
       # ... (implement stratified variance formula)
   ```

4. Add _calculate_variance_for_group() method similar to area():
   ```python
   def _calculate_variance_for_group(self, plot_data: pl.DataFrame,
                                     strat_cols: List[str]) -> dict:
       '''Calculate variance for ratio-of-means estimator.'''
       # Implement proper variance formula for ratio estimation
       # V(R) = (1/X̄²) * [V(Y) + R² * V(X) - 2R * Cov(Y,X)]
   ```
""")

    print("\n5. Testing Requirements:")
    print("-" * 40)
    print("   • Compare variance estimates with FIA EVALIDator")
    print("   • Test with different grouping variables")
    print("   • Verify variance increases with domain restrictions")
    print("   • Check that rare species have higher CVs")
    print("   • Validate against known FIA publications")

if __name__ == "__main__":
    # Run tests
    try:
        test_variance_implementation()
    except Exception as e:
        print(f"\nError during testing: {e}")
        print("Make sure georgia.duckdb is available in the data/ directory")

    # Analyze code differences
    analyze_variance_code()

    # Propose fixes
    propose_volume_variance_fix()

    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print("""
The area() function implements proper stratified variance calculation following
FIA methodology, while volume() currently uses a simplified placeholder (12% CV).

Key improvements needed for volume():
1. Preserve plot-level data during aggregation
2. Implement stratified variance formula for ratio estimation
3. Account for clustering of trees within plots
4. Handle grouped estimates with separate variance calculations

The area() implementation (lines 382-541) provides a good template that can be
adapted for volume and other metric estimators.
""")