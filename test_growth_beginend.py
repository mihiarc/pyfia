"""
Test growth estimation with BEGINEND cross-join methodology against Georgia EVALID 132303.

Expected results from EVALIDator:
- Georgia EVALID 132303 timberland growth
- Expected: 2,473,614,987 cu ft (gross growth)
- SE%: 1.283%
- Non-zero plots: 4,588
"""

from pyfia import FIA, growth

# Path to southern states database (includes Georgia with GRM_MIDPT)
DB_PATH = "data/test_southern.duckdb"

# Expected EVALIDator results
EXPECTED_TOTAL = 2_473_614_987  # cu ft
EXPECTED_SE_PCT = 1.283
EXPECTED_PLOTS = 4_588
EVALID = 132303

def test_growth_beginend():
    """Test growth estimation with BEGINEND cross-join."""
    print("=" * 80)
    print("Testing Growth Estimation with BEGINEND Cross-Join")
    print("=" * 80)

    with FIA(DB_PATH) as db:
        # Filter to Georgia EVALID 132303
        print(f"\nFiltering to Georgia EVALID {EVALID}...")
        db.clip_by_evalid([EVALID])

        # Estimate growth on timberland (GS = growing stock, TIMBER = timberland)
        print("\nEstimating growth on timberland...")
        results = growth(
            db,
            land_type="timber",
            tree_type="gs",
            measure="volume",
            totals=True,
            variance=True
        )

        if results.is_empty():
            print("\n❌ ERROR: No growth results returned!")
            return False

        # Debug: Print available columns
        print(f"\nAvailable columns: {results.columns}")
        print(f"\nFirst row:\n{results.head(1)}")

        # Extract results - check what columns are actually available
        growth_total = results["GROWTH_TOTAL"][0] if "GROWTH_TOTAL" in results.columns else None
        growth_acre = results["GROWTH_ACRE"][0] if "GROWTH_ACRE" in results.columns else None
        area_total = results["AREA_TOTAL"][0] if "AREA_TOTAL" in results.columns else None
        n_plots = results["N_PLOTS"][0] if "N_PLOTS" in results.columns else None

        print("\n" + "=" * 80)
        print("RESULTS")
        print("=" * 80)
        if growth_total:
            print(f"Total Growth:       {growth_total:,.0f} cu ft")
        if growth_acre:
            print(f"Growth per Acre:    {growth_acre:.2f} cu ft/acre")
        if area_total:
            print(f"Total Area:         {area_total:,.0f} acres")
        if n_plots:
            print(f"Number of Plots:    {n_plots:,}")

        print("\n" + "=" * 80)
        print("COMPARISON TO EVALIDator")
        print("=" * 80)
        print(f"Expected Total:     {EXPECTED_TOTAL:,.0f} cu ft")
        print(f"Actual Total:       {growth_total:,.0f} cu ft")
        print(f"Difference:         {growth_total - EXPECTED_TOTAL:,.0f} cu ft")
        print(f"Percent Difference: {((growth_total - EXPECTED_TOTAL) / EXPECTED_TOTAL * 100):.2f}%")

        print(f"\nExpected Plots:     {EXPECTED_PLOTS:,}")
        print(f"Actual Plots:       {n_plots:,}")

        # Check if within acceptable tolerance (±5%)
        tolerance = 0.05  # 5%
        pct_diff = abs((growth_total - EXPECTED_TOTAL) / EXPECTED_TOTAL)

        print("\n" + "=" * 80)
        if pct_diff <= tolerance:
            print(f"✅ SUCCESS: Within {tolerance*100}% of EVALIDator estimate!")
            print("=" * 80)
            return True
        else:
            print(f"⚠️  WARNING: Difference of {pct_diff*100:.2f}% exceeds {tolerance*100}% tolerance")
            print("=" * 80)
            return False

if __name__ == "__main__":
    success = test_growth_beginend()
    exit(0 if success else 1)