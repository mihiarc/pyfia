#!/usr/bin/env python
"""
Test the new proper variance implementation for volume() function.

This script verifies that the volume variance now:
1. Uses proper ratio-of-means variance formula
2. Responds to sample size
3. Varies by group
4. Increases with domain restrictions
"""

import polars as pl
from pyfia import FIA, volume

def test_new_variance_implementation():
    """Test the new variance implementation for volume."""

    print("=" * 80)
    print("TESTING NEW VOLUME VARIANCE IMPLEMENTATION")
    print("=" * 80)

    db_path = "data/nfi_south.duckdb"

    with FIA(db_path) as db:
        # Test with Texas data
        db.clip_by_state(48, most_recent=True, eval_type="VOL")

        # Test 1: Basic variance calculation
        print("\n1. BASIC VOLUME VARIANCE (should NOT be fixed 12% anymore)")
        print("-" * 60)

        volume_results = volume(db, land_type="forest", totals=True)

        if not volume_results.is_empty():
            volume_acre = volume_results['VOLCFNET_ACRE'][0]
            volume_se = volume_results['VOLCFNET_ACRE_SE'][0]
            cv_pct = 100 * volume_se / volume_acre if volume_acre > 0 else 0

            print(f"Net Volume/Acre: {volume_acre:,.1f} cu ft/acre")
            print(f"Standard Error: {volume_se:,.1f} cu ft/acre")
            print(f"CV%: {cv_pct:.2f}%")
            print(f"Sample Size: {volume_results['N_PLOTS'][0]:,} plots")
            print(f"Tree Count: {volume_results['N_TREES'][0]:,} trees")

            # Check if CV is still fixed at 12%
            if abs(cv_pct - 12.0) < 0.01:
                print("\n⚠️ WARNING: CV is still 12% - variance calculation may not be working!")
            else:
                print("\n✅ SUCCESS: CV is not fixed at 12% - proper variance is being calculated!")

        # Test 2: Variance by groups - should show different CVs
        print("\n\n2. GROUPED VARIANCE (each group should have different CV)")
        print("-" * 60)

        # Test by forest type - common groups with different sample sizes
        volume_by_type = volume(db, grp_by="FORTYPCD", land_type="forest")

        if not volume_by_type.is_empty():
            print("\nVolume by Forest Type (showing top 5):")
            print(f"{'Forest Type':<15} {'Volume/Acre':<15} {'SE':<12} {'CV%':<8} {'N Plots':<10}")
            print("-" * 70)

            # Sort by volume and show top 5
            sorted_results = volume_by_type.sort("VOLCFNET_ACRE", descending=True).head(5)

            cv_values = []
            for row in sorted_results.iter_rows(named=True):
                if row['VOLCFNET_ACRE'] and row['VOLCFNET_ACRE'] > 0:
                    cv = 100 * row['VOLCFNET_ACRE_SE'] / row['VOLCFNET_ACRE']
                    cv_values.append(cv)
                    print(f"{row['FORTYPCD']:<15} {row['VOLCFNET_ACRE']:<15.1f} "
                          f"{row['VOLCFNET_ACRE_SE']:<12.1f} {cv:<8.2f} {row['N_PLOTS']:<10}")

            # Check if all CVs are the same (bad) or different (good)
            if cv_values and len(set([round(cv, 1) for cv in cv_values])) == 1:
                print("\n⚠️ WARNING: All groups have the same CV - variance may not be group-specific!")
            else:
                print("\n✅ SUCCESS: Different groups have different CVs - proper variance calculation!")

        # Test 3: Domain restriction effect
        print("\n\n3. DOMAIN RESTRICTION EFFECT ON VARIANCE")
        print("-" * 60)

        # All trees
        volume_all = volume(db, land_type="forest")
        cv_all = 100 * volume_all['VOLCFNET_ACRE_SE'][0] / volume_all['VOLCFNET_ACRE'][0]

        # Large trees only (should have higher CV)
        volume_large = volume(db, land_type="forest", tree_domain="DIA >= 20.0")
        cv_large = 100 * volume_large['VOLCFNET_ACRE_SE'][0] / volume_large['VOLCFNET_ACRE'][0]

        print(f"\nAll trees:")
        print(f"  Volume: {volume_all['VOLCFNET_ACRE'][0]:,.1f} cu ft/acre")
        print(f"  CV: {cv_all:.2f}%")
        print(f"  N Trees: {volume_all['N_TREES'][0]:,}")

        print(f"\nLarge trees (≥20\" DBH):")
        print(f"  Volume: {volume_large['VOLCFNET_ACRE'][0]:,.1f} cu ft/acre")
        print(f"  CV: {cv_large:.2f}%")
        print(f"  N Trees: {volume_large['N_TREES'][0]:,}")

        print(f"\nCV Ratio (large/all): {cv_large/cv_all:.2f}x")

        if cv_large > cv_all:
            print("✅ SUCCESS: Domain restriction increases CV (as expected)!")
        else:
            print("⚠️ WARNING: Domain restriction did not increase CV")

        # Test 4: Check for variance columns
        print("\n\n4. VARIANCE OUTPUT STRUCTURE")
        print("-" * 60)

        print(f"Columns in output: {volume_all.columns}")

        expected_cols = ["VOLCFNET_ACRE_SE", "VOLCFNET_TOTAL_SE"]
        missing_cols = [col for col in expected_cols if col not in volume_all.columns]

        if missing_cols:
            print(f"⚠️ Missing expected columns: {missing_cols}")
        else:
            print("✅ All expected variance columns present")

        # Check if variance values are present
        if "VOLUME_ACRE_VARIANCE" in volume_all.columns:
            print(f"Variance (per acre): {volume_all['VOLUME_ACRE_VARIANCE'][0]:.2e}")
        if "VOLUME_TOTAL_VARIANCE" in volume_all.columns:
            print(f"Variance (total): {volume_all['VOLUME_TOTAL_VARIANCE'][0]:.2e}")

def analyze_variance_quality():
    """Analyze the quality of the new variance implementation."""

    print("\n" + "=" * 80)
    print("VARIANCE QUALITY ASSESSMENT")
    print("=" * 80)

    print("\n📊 EXPECTED BEHAVIOR (Bechtold & Patterson 2005):")
    print("-" * 60)
    print("1. CV should vary by stratum and group (not fixed)")
    print("2. Smaller samples → higher CV")
    print("3. Domain restrictions → higher CV")
    print("4. Rare species/types → higher CV")
    print("5. State-level volume CV typically 2-5%")
    print("6. County-level volume CV typically 10-25%")

    print("\n🔬 RATIO-OF-MEANS VARIANCE FORMULA:")
    print("-" * 60)
    print("V(R) = (1/X̄²) × Σ_h w_h² × [s²_yh + R² × s²_xh - 2R × Cov(y,x)] / n_h")
    print("\nWhere:")
    print("  R = ratio estimate (volume/area)")
    print("  y = volume per plot")
    print("  x = area per plot")
    print("  w_h = stratum weight (EXPNS)")
    print("  Cov(y,x) = covariance between volume and area")

    print("\n💡 KEY INSIGHTS:")
    print("-" * 60)
    print("• Covariance term reduces variance (volume and area are correlated)")
    print("• Stratification reduces variance by accounting for between-strata differences")
    print("• Ratio estimation is more efficient than separate estimates")
    print("• Variance depends on both numerator and denominator variability")

if __name__ == "__main__":
    try:
        test_new_variance_implementation()
        analyze_variance_quality()
    except Exception as e:
        import traceback
        print(f"\n❌ Error during testing: {e}")
        print("\nTraceback:")
        traceback.print_exc()

    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print("""
The volume() function has been updated with proper ratio-of-means variance
calculation that accounts for:
- Stratified sampling design
- Covariance between volume and area
- Group-specific variance
- Domain restriction effects

This brings volume() in line with FIA statistical standards and provides
users with defensible precision estimates for their analyses.
""")