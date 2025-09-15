#!/usr/bin/env python
"""
Test the fixed area() variance calculation with Georgia FIA data.

This script tests whether the implemented fix produces standard errors
that match EVALIDator's published estimates.
"""

import polars as pl
from pyfia import FIA, area
import warnings

# Published Georgia forestland area from FIA EVALIDator
PUBLISHED_GEORGIA = {
    "total_forestland_acres": 24_172_679,
    "sampling_error_percent": 0.563,  # At 67% confidence level
    "n_plots": 4842  # Non-zero (forested) plots
}

# Calculate SE from sampling error percentage
PUBLISHED_GEORGIA["forestland_se_acres"] = (
    PUBLISHED_GEORGIA["sampling_error_percent"] *
    PUBLISHED_GEORGIA["total_forestland_acres"] / 100
)
PUBLISHED_GEORGIA["forestland_se_percent"] = PUBLISHED_GEORGIA["sampling_error_percent"]


def test_area_variance():
    """Test the fixed variance calculation for area estimation."""

    print("=" * 70)
    print("TESTING FIXED AREA VARIANCE CALCULATION")
    print("=" * 70)

    db_path = "data/georgia.duckdb"

    try:
        with FIA(db_path) as db:
            print("\nLoading Georgia FIA database...")
            db.clip_most_recent(eval_type="ALL")

            if db.evalid:
                print(f"Using EVALID: {db.evalid}")

            # Test 1: Basic forest area with proper variance
            print("\n" + "=" * 50)
            print("TEST 1: Forest Area with Fixed Variance Calculation")
            print("=" * 50)

            results = area(db, land_type="forest")

            print(f"\nResults columns: {results.columns}")
            print(f"\nEstimation Results:")
            print(f"  Forest area: {results['AREA'][0]:,.0f} acres")

            if 'AREA_SE' in results.columns:
                print(f"  Standard error: {results['AREA_SE'][0]:,.0f} acres")

            if 'AREA_SE_PERCENT' in results.columns:
                print(f"  SE%: {results['AREA_SE_PERCENT'][0]:.3f}%")
            elif 'AREA_SE' in results.columns and 'AREA' in results.columns:
                se_pct = 100 * results['AREA_SE'][0] / results['AREA'][0]
                print(f"  SE% (calculated): {se_pct:.3f}%")

            if 'AREA_VARIANCE' in results.columns:
                print(f"  Variance: {results['AREA_VARIANCE'][0]:,.2f}")

            print(f"  Number of plots: {results['N_PLOTS'][0]:,}")

            # Compare with published
            print(f"\nPublished EVALIDator Estimates:")
            print(f"  Forest area: {PUBLISHED_GEORGIA['total_forestland_acres']:,} acres")
            print(f"  SE: {PUBLISHED_GEORGIA['forestland_se_acres']:,.0f} acres")
            print(f"  SE%: {PUBLISHED_GEORGIA['forestland_se_percent']:.3f}%")
            print(f"  N non-zero plots: {PUBLISHED_GEORGIA['n_plots']:,}")

            # Analysis
            print(f"\n" + "-" * 50)
            print("ANALYSIS:")

            area_diff = results['AREA'][0] - PUBLISHED_GEORGIA['total_forestland_acres']
            area_diff_pct = 100 * area_diff / PUBLISHED_GEORGIA['total_forestland_acres']
            print(f"  Area difference: {area_diff:,.0f} acres ({area_diff_pct:+.2f}%)")

            if 'AREA_SE_PERCENT' in results.columns:
                se_pct_calc = results['AREA_SE_PERCENT'][0]
            elif 'AREA_SE' in results.columns and 'AREA' in results.columns:
                se_pct_calc = 100 * results['AREA_SE'][0] / results['AREA'][0]
            else:
                se_pct_calc = None

            if se_pct_calc is not None:
                se_pct_diff = se_pct_calc - PUBLISHED_GEORGIA['forestland_se_percent']
                print(f"  SE% difference: {se_pct_diff:+.3f} percentage points")

                # Success criteria
                if abs(se_pct_diff) < 0.1:  # Within 0.1 percentage points
                    print("\n✅ SUCCESS: SE% is within 0.1 percentage points of published!")
                elif abs(se_pct_diff) < 0.2:
                    print("\n⚠️  CLOSE: SE% is within 0.2 percentage points of published")
                else:
                    print(f"\n❌ ISSUE: SE% differs by {abs(se_pct_diff):.3f} percentage points")

            # Test 2: Grouped estimates to verify each group has different SE
            print("\n" + "=" * 50)
            print("TEST 2: Grouped Estimates (by Ownership)")
            print("=" * 50)

            grouped_results = area(db, land_type="forest", grp_by="OWNGRPCD")

            print(f"\nOwnership Group Results:")
            print(f"{'Owner':<10} {'Area (acres)':<15} {'SE (acres)':<15} {'SE%':<10}")
            print("-" * 55)

            se_values = []
            for i in range(min(5, len(grouped_results))):
                own = grouped_results['OWNGRPCD'][i]
                area_val = grouped_results['AREA'][i]

                if 'AREA_SE' in grouped_results.columns:
                    se = grouped_results['AREA_SE'][i]
                    se_values.append(se)
                else:
                    se = None

                if 'AREA_SE_PERCENT' in grouped_results.columns:
                    se_pct = grouped_results['AREA_SE_PERCENT'][i]
                elif se and area_val > 0:
                    se_pct = 100 * se / area_val
                else:
                    se_pct = None

                if se is not None and se_pct is not None:
                    print(f"{own:<10} {area_val:>14,.0f} {se:>14,.0f} {se_pct:>9.3f}%")
                else:
                    print(f"{own:<10} {area_val:>14,.0f} {'N/A':>14} {'N/A':>9}")

            # Check if all SEs are different (not the bug we had before)
            if len(se_values) > 1:
                if len(set(se_values)) == 1:
                    print("\n❌ ERROR: All groups have identical SE - variance calculation bug!")
                else:
                    print("\n✅ SUCCESS: Each group has different SE as expected")

            # Test 3: Check internal consistency
            print("\n" + "=" * 50)
            print("TEST 3: Internal Consistency Checks")
            print("=" * 50)

            # Total of grouped areas should equal ungrouped total
            if len(grouped_results) > 0:
                grouped_total = grouped_results['AREA'].sum()
                ungrouped_total = results['AREA'][0]
                total_diff = abs(grouped_total - ungrouped_total)

                print(f"\nTotal area from groups: {grouped_total:,.0f} acres")
                print(f"Ungrouped total area: {ungrouped_total:,.0f} acres")
                print(f"Difference: {total_diff:,.0f} acres")

                if total_diff < 1000:  # Within 1000 acres
                    print("✅ Totals are consistent")
                else:
                    print("❌ Totals don't match!")

    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    # Suppress warnings for cleaner output
    warnings.filterwarnings("ignore")

    test_area_variance()

    print("\n" + "=" * 70)
    print("TEST COMPLETE")
    print("=" * 70)