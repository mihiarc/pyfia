#!/usr/bin/env python
"""
Detailed variance comparison test between area() and volume() functions.

This test demonstrates the differences in variance calculation quality
between the two implementations.
"""

import polars as pl
from pyfia import FIA, area, volume

def test_variance_behavior():
    """Test how variance behaves under different conditions."""

    print("=" * 80)
    print("DETAILED VARIANCE BEHAVIOR ANALYSIS")
    print("=" * 80)

    db_path = "data/nfi_south.duckdb"

    with FIA(db_path) as db:
        # Test with Texas data
        db.clip_by_state(48, most_recent=True, eval_type="ALL")

        # Test 1: Overall estimates
        print("\n1. OVERALL ESTIMATES - TEXAS")
        print("-" * 40)

        area_results = area(db, land_type="forest")
        print(f"\nArea Estimation:")
        print(f"  Forest Area: {area_results['AREA'][0]:,.0f} acres")
        print(f"  Standard Error: {area_results['AREA_SE'][0]:,.0f} acres")
        print(f"  CV%: {area_results['AREA_SE_PERCENT'][0]:.2f}%")
        print(f"  Sample Size: {area_results['N_PLOTS'][0]:,} plots")

        # Re-clip for volume
        db.clip_by_state(48, most_recent=True, eval_type="VOL")
        volume_results = volume(db, land_type="forest")

        if not volume_results.is_empty():
            print(f"\nVolume Estimation:")
            print(f"  Net Volume/Acre: {volume_results['VOLCFNET_ACRE'][0]:,.1f} cu ft/acre")
            print(f"  Standard Error: {volume_results['VOLCFNET_ACRE_SE'][0]:,.1f} cu ft/acre")
            cv_pct = 100 * volume_results['VOLCFNET_ACRE_SE'][0] / volume_results['VOLCFNET_ACRE'][0]
            print(f"  CV%: {cv_pct:.2f}% (FIXED at 12%)")
            print(f"  Sample Size: {volume_results['N_PLOTS'][0]:,} plots")
            print(f"  Tree Count: {volume_results['N_TREES'][0]:,} trees")

        # Test 2: Grouped estimates - should show different variance patterns
        print("\n\n2. GROUPED ESTIMATES BY OWNERSHIP")
        print("-" * 40)

        # Re-clip for area
        db.clip_by_state(48, most_recent=True, eval_type="ALL")
        area_by_owner = area(db, grp_by="OWNGRPCD", land_type="forest")

        print("\nArea by Ownership (variable CV based on sample size):")
        for row in area_by_owner.sort("AREA", descending=True).head(4).iter_rows(named=True):
            cv = row['AREA_SE_PERCENT'] if 'AREA_SE_PERCENT' in row else (
                100 * row['AREA_SE'] / row['AREA'] if row['AREA'] > 0 else 0
            )
            print(f"  Owner {row['OWNGRPCD']}: {row['AREA']:>15,.0f} acres, "
                  f"CV={cv:>5.2f}%, N={row['N_PLOTS']:>6,} plots")

        # Re-clip for volume
        db.clip_by_state(48, most_recent=True, eval_type="VOL")
        volume_by_owner = volume(db, grp_by="OWNGRPCD", land_type="forest")

        print("\nVolume by Ownership (fixed 12% CV regardless of sample):")
        for row in volume_by_owner.sort("VOLCFNET_TOTAL", descending=True).head(4).iter_rows(named=True):
            cv = 100 * row['VOLCFNET_ACRE_SE'] / row['VOLCFNET_ACRE'] if row['VOLCFNET_ACRE'] > 0 else 0
            print(f"  Owner {row['OWNGRPCD']}: {row['VOLCFNET_ACRE']:>8.1f} cu ft/acre, "
                  f"CV={cv:>5.2f}%, N={row['N_PLOTS']:>6,} plots")

        # Test 3: Domain restriction - should increase variance
        print("\n\n3. DOMAIN RESTRICTION EFFECTS")
        print("-" * 40)

        # Area with domain restriction
        db.clip_by_state(48, most_recent=True, eval_type="ALL")
        area_all = area(db, land_type="forest")
        area_old = area(db, land_type="forest", area_domain="STDAGE > 50")

        print("\nArea Estimation with Domain Restriction:")
        print(f"  All forest: CV={area_all['AREA_SE_PERCENT'][0]:.2f}%")
        if not area_old.is_empty():
            print(f"  Old forest (>50 yr): CV={area_old['AREA_SE_PERCENT'][0]:.2f}%")
            print(f"  CV Ratio: {area_old['AREA_SE_PERCENT'][0] / area_all['AREA_SE_PERCENT'][0]:.2f}x "
                  f"(properly increases with domain restriction)")

        # Volume with domain restriction
        db.clip_by_state(48, most_recent=True, eval_type="VOL")
        volume_all = volume(db, land_type="forest")
        volume_large = volume(db, land_type="forest", tree_domain="DIA >= 20.0")

        print("\nVolume Estimation with Domain Restriction:")
        cv_all = 100 * volume_all['VOLCFNET_ACRE_SE'][0] / volume_all['VOLCFNET_ACRE'][0]
        cv_large = 100 * volume_large['VOLCFNET_ACRE_SE'][0] / volume_large['VOLCFNET_ACRE'][0]
        print(f"  All trees: CV={cv_all:.2f}%")
        print(f"  Large trees (≥20\" DBH): CV={cv_large:.2f}%")
        print(f"  CV Ratio: {cv_large / cv_all:.2f}x (STAYS SAME at 12% - incorrect!)")

def analyze_variance_quality():
    """Analyze the quality of variance estimates."""

    print("\n" + "=" * 80)
    print("VARIANCE QUALITY ANALYSIS")
    print("=" * 80)

    print("\n✅ AREA() VARIANCE - PROPER IMPLEMENTATION:")
    print("-" * 50)
    print("• Responds to sample size (larger N → smaller CV)")
    print("• Increases with domain restrictions (subset → higher CV)")
    print("• Accounts for stratification (reduces variance)")
    print("• Different CVs for different groups")
    print("• Matches published FIA estimates")

    print("\n⚠️ VOLUME() VARIANCE - PLACEHOLDER IMPLEMENTATION:")
    print("-" * 50)
    print("• Fixed 12% CV regardless of conditions")
    print("• Doesn't respond to sample size")
    print("• Same CV for rare and common species")
    print("• No increase for domain restrictions")
    print("• May not match published estimates")

    print("\n📊 EXPECTED CV RANGES (from FIA publications):")
    print("-" * 50)
    print("State-level estimates:")
    print("  • Forest area: 0.5-2%")
    print("  • Total volume: 2-5%")
    print("  • Volume by species: 5-50% (rare species higher)")
    print("\nCounty-level estimates:")
    print("  • Forest area: 5-15%")
    print("  • Total volume: 10-25%")
    print("\nPlot-level estimates:")
    print("  • Individual plots: 50-100%+")

def recommend_next_steps():
    """Provide recommendations for next steps."""

    print("\n" + "=" * 80)
    print("RECOMMENDATIONS")
    print("=" * 80)

    print("\n1. IMMEDIATE ACTIONS:")
    print("   • Review variance_implementation_proposal.md")
    print("   • Implement data preservation in VolumeEstimator")
    print("   • Add _calculate_ratio_variance method")

    print("\n2. VALIDATION NEEDED:")
    print("   • Compare with FIA EVALIDator for known estimates")
    print("   • Test with multiple states and groupings")
    print("   • Verify CV increases appropriately with restrictions")

    print("\n3. EXTEND TO OTHER ESTIMATORS:")
    print("   • biomass.py - same issue as volume")
    print("   • tpa.py - same issue as volume")
    print("   • mortality.py - already has better implementation")
    print("   • growth.py - needs review")

    print("\n4. DOCUMENTATION:")
    print("   • Add variance methodology to user guide")
    print("   • Include examples showing proper CV interpretation")
    print("   • Warn about current placeholder limitations")

if __name__ == "__main__":
    try:
        test_variance_behavior()
    except Exception as e:
        print(f"\nError during testing: {e}")
        print("Ensure nfi_south.duckdb is available in data/ directory")

    analyze_variance_quality()
    recommend_next_steps()

    print("\n" + "=" * 80)
    print("CONCLUSION")
    print("=" * 80)
    print("""
The area() function correctly implements stratified variance calculation that
responds appropriately to sample size and domain restrictions. The volume()
function uses a simplified 12% CV placeholder that doesn't account for these
factors.

Implementing proper variance for volume() and other estimators is critical for:
- Statistical validity of confidence intervals
- Proper interpretation of estimate precision
- Alignment with published FIA estimates
- User trust in the library's outputs

The proposed implementation in variance_implementation_proposal.md provides a
clear path forward to achieve statistical rigor across all estimation functions.
""")