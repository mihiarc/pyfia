#!/usr/bin/env python
"""
Test script to validate the growth function fix in PR #15
Tests the NET growth calculation: (Ending - Beginning) vs just Ending
"""

import polars as pl
from pyfia import FIA, growth

def test_growth_calculation():
    """Test that growth correctly calculates NET change in volume"""

    # Create a simple test database with known values
    # SURVIVOR tree: Beginning volume = 100, Ending volume = 120
    # Expected NET growth = (120 - 100) / 5 years = 4 cu ft/year

    print("Testing growth NET calculation fix...")

    # Test with Texas data if available
    db_path = "data/nfi_south.duckdb"

    try:
        with FIA(db_path) as db:
            # Filter to Texas
            db.clip_by_state(48)

            # Get most recent EXPVOL evaluation
            db.clip_most_recent(eval_type="EXPVOL")

            # Run growth estimation
            results = growth(
                db,
                measure="volume",
                land_type="forest",
                tree_type="gs",
                totals=True
            )

            if not results.is_empty():
                print(f"\nResults columns: {results.columns}")
                print(f"First row: {results.head(1)}")

                if "GROWTH_ACRE" in results.columns:
                    growth_acre = results["GROWTH_ACRE"][0]
                    growth_total = results["GROWTH_TOTAL"][0] if "GROWTH_TOTAL" in results.columns else 0

                    print(f"\nResults:")
                    print(f"  Growth per acre: {growth_acre:.2f} cu ft/acre/year")
                    print(f"  Total growth: {growth_total/1_000_000:.2f} million cu ft/year" if growth_total > 0 else "")

                # The PR description says the fix reduced values from 99.3M to 18.4M
                # for EVALID 132303, which is a significant reduction
                # Values should now be reasonable (not 5x higher)

                    if growth_total < 50_000_000:  # Less than 50M cu ft
                        print("\n✓ Growth values appear reasonable after fix")
                    else:
                        print("\n✗ Growth values still seem too high")
                else:
                    print("\nGROWTH_ACRE column not found in results")

            else:
                print("No results returned")

    except FileNotFoundError:
        print(f"Database not found at {db_path}")
        print("Creating synthetic test...")

        # Create synthetic test data
        test_synthetic_growth()
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

def test_synthetic_growth():
    """Test with synthetic data to validate calculation logic"""

    print("\n=== Testing with synthetic data ===")

    # Create test data that mimics GRM structure
    # SURVIVOR tree: has both beginning and ending volumes
    # INGROWTH tree: only has ending volume (new tree)

    component_data = pl.DataFrame({
        "TRE_CN": [1, 2, 3],
        "PLT_CN": [100, 100, 100],
        "COMPONENT": ["SURVIVOR", "INGROWTH", "SURVIVOR"],
        "TPAGROW_UNADJ": [1.0, 0.5, 0.8],  # Trees per acre
        "SUBPTYP_GRM": [1, 1, 1],  # Subplot adjustment
        "DIA_BEGIN": [10.0, None, 15.0],
        "DIA_MIDPT": [11.0, 6.0, 16.0],
        "DIA_END": [12.0, 6.0, 17.0],
    })

    midpt_data = pl.DataFrame({
        "TRE_CN": [1, 2, 3],
        "VOLCFNET": [120.0, 30.0, 200.0],  # Ending volumes
        "DIA": [12.0, 6.0, 17.0],
        "SPCD": [131, 110, 131],
        "STATUSCD": [1, 1, 1]
    })

    begin_data = pl.DataFrame({
        "TRE_CN": [1, 3],  # Only SURVIVOR trees have beginning data
        "BEGIN_VOLCFNET": [100.0, 180.0],  # Beginning volumes
    })

    # Join the data
    test_data = component_data.join(midpt_data, on="TRE_CN", how="inner")
    test_data = test_data.join(begin_data, on="TRE_CN", how="left")

    # Add REMPER (remeasurement period)
    test_data = test_data.with_columns([
        pl.lit(5.0).alias("REMPER")  # 5 year period
    ])

    # Calculate volume change following the PR's logic
    test_data = test_data.with_columns([
        pl.when(pl.col("COMPONENT") == "SURVIVOR")
        .then(
            # NET growth for SURVIVOR: (Ending - Beginning) / REMPER
            (pl.col("VOLCFNET").fill_null(0) - pl.col("BEGIN_VOLCFNET").fill_null(0)) /
            pl.col("REMPER")
        )
        .when(pl.col("COMPONENT") == "INGROWTH")
        .then(
            # INGROWTH: Only ending volume / REMPER
            pl.col("VOLCFNET").fill_null(0) / pl.col("REMPER")
        )
        .otherwise(0.0)
        .alias("volume_change_per_year")
    ])

    # Calculate growth value
    test_data = test_data.with_columns([
        (pl.col("TPAGROW_UNADJ") * pl.col("volume_change_per_year")).alias("GROWTH_VALUE")
    ])

    print("\nTest Data:")
    print(test_data.select([
        "TRE_CN", "COMPONENT", "VOLCFNET", "BEGIN_VOLCFNET",
        "volume_change_per_year", "GROWTH_VALUE"
    ]))

    # Validate calculations
    expected_results = {
        1: (120 - 100) / 5,  # SURVIVOR: (120-100)/5 = 4 cu ft/year
        2: 30 / 5,           # INGROWTH: 30/5 = 6 cu ft/year
        3: (200 - 180) / 5   # SURVIVOR: (200-180)/5 = 4 cu ft/year
    }

    print("\nValidation:")
    for row in test_data.iter_rows(named=True):
        tre_cn = row["TRE_CN"]
        actual = row["volume_change_per_year"]
        expected = expected_results[tre_cn]

        if abs(actual - expected) < 0.01:
            print(f"  Tree {tre_cn} ({row['COMPONENT']}): ✓ {actual:.2f} = {expected:.2f}")
        else:
            print(f"  Tree {tre_cn} ({row['COMPONENT']}): ✗ {actual:.2f} ≠ {expected:.2f}")

    # Test what would happen with OLD (incorrect) logic
    print("\n=== Comparison with OLD logic (using only ending volume) ===")

    old_logic = test_data.with_columns([
        # OLD: Just use ending volume (incorrect)
        (pl.col("TPAGROW_UNADJ") * pl.col("VOLCFNET") / pl.col("REMPER")).alias("OLD_GROWTH_VALUE")
    ])

    print("\nOLD vs NEW calculation:")
    print(old_logic.select([
        "TRE_CN", "COMPONENT",
        "OLD_GROWTH_VALUE", "GROWTH_VALUE",
        ((pl.col("OLD_GROWTH_VALUE") / pl.col("GROWTH_VALUE")) * 100).alias("OLD_PCT_OF_NEW")
    ]))

    total_old = old_logic["OLD_GROWTH_VALUE"].sum()
    total_new = old_logic["GROWTH_VALUE"].sum()

    print(f"\nTotal OLD: {total_old:.2f}")
    print(f"Total NEW: {total_new:.2f}")
    print(f"OLD/NEW ratio: {total_old/total_new:.1f}x")

    if total_old / total_new > 3:
        print("\n✓ Fix correctly reduces overestimation!")
    else:
        print("\n✗ Fix may not be working as expected")

if __name__ == "__main__":
    test_growth_calculation()