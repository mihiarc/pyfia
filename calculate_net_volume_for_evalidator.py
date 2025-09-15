#!/usr/bin/env python
"""
Calculate net merchantable bole wood volume of live trees for comparison with EVALIDator.

EVALIDator query:
- Net merchantable bole wood volume of live trees (timber species at least 5 inches d.b.h.)
- In cubic feet
- On forest land
"""

import polars as pl
from pyfia import FIA, volume

def calculate_net_volume_for_texas():
    """Calculate net volume for Texas to compare with EVALIDator."""

    print("=" * 80)
    print("NET MERCHANTABLE BOLE WOOD VOLUME CALCULATION")
    print("For comparison with FIA EVALIDator")
    print("=" * 80)

    db_path = "data/nfi_south.duckdb"

    with FIA(db_path) as db:
        # Filter to Texas and most recent volume evaluation
        print("\n1. Database Setup")
        print("-" * 40)
        print("State: Texas (FIPS code 48)")
        print("Evaluation type: EXPVOL (volume plots)")

        db.clip_by_state(48, most_recent=True, eval_type="VOL")

        # Get evaluation info
        if db.evalid:
            print(f"Selected EVALID: {db.evalid}")

        # Calculate net volume of live trees on forestland
        print("\n2. Volume Calculation Parameters")
        print("-" * 40)
        print("Land type: forest (COND_STATUS_CD = 1)")
        print("Tree type: live (STATUSCD = 1)")
        print("Volume type: net (VOLCFNET)")
        print("Size threshold: ≥5 inches DBH (handled by FIA)")

        results = volume(
            db,
            land_type="forest",
            tree_type="live",
            vol_type="net",
            totals=True
        )

        print("\n3. RESULTS")
        print("=" * 60)

        if not results.is_empty():
            # Per-acre estimate
            volume_per_acre = results['VOLCFNET_ACRE'][0]
            print(f"\nPER-ACRE ESTIMATE:")
            print(f"  Net volume: {volume_per_acre:,.1f} cubic feet per acre")

            # Standard error if available
            if 'VOLCFNET_ACRE_SE' in results.columns:
                se_acre = results['VOLCFNET_ACRE_SE'][0]
                if not pl.DataFrame([se_acre])['column_0'].is_nan()[0] and se_acre > 0:
                    cv_acre = 100 * se_acre / volume_per_acre if volume_per_acre > 0 else 0
                    print(f"  Standard error: {se_acre:,.1f} cubic feet per acre")
                    print(f"  CV%: {cv_acre:.2f}%")

            # Total estimate
            total_volume = results['VOLCFNET_TOTAL'][0]
            print(f"\nTOTAL ESTIMATE:")
            print(f"  Total net volume: {total_volume:,.0f} cubic feet")
            print(f"  Total net volume: {total_volume/1e9:,.2f} billion cubic feet")

            # Sample size
            n_plots = results['N_PLOTS'][0]
            n_trees = results['N_TREES'][0]
            print(f"\nSAMPLE SIZE:")
            print(f"  Number of plots: {n_plots:,}")
            print(f"  Number of trees: {n_trees:,}")

            # Additional grouping by ownership for context
            print("\n4. VOLUME BY OWNERSHIP GROUP")
            print("-" * 60)

            volume_by_owner = volume(
                db,
                grp_by="OWNGRPCD",
                land_type="forest",
                tree_type="live",
                vol_type="net",
                totals=True
            )

            if not volume_by_owner.is_empty():
                print(f"\n{'Owner Group':<20} {'Volume/Acre':<15} {'Total (billion ft³)':<20} {'% of Total':<10}")
                print("-" * 70)

                total_vol = volume_by_owner['VOLCFNET_TOTAL'].sum()

                for row in volume_by_owner.sort('VOLCFNET_TOTAL', descending=True).iter_rows(named=True):
                    owner = row['OWNGRPCD']
                    vol_acre = row['VOLCFNET_ACRE']
                    vol_total = row['VOLCFNET_TOTAL']
                    pct = 100 * vol_total / total_vol if total_vol > 0 else 0

                    # Map ownership codes to names
                    owner_names = {
                        10: "National Forest",
                        20: "Other Federal",
                        30: "State/Local",
                        40: "Private"
                    }
                    owner_name = owner_names.get(owner, f"Code {owner}")

                    print(f"{owner_name:<20} {vol_acre:<15.1f} {vol_total/1e9:<20.2f} {pct:<10.1f}")

        print("\n" + "=" * 80)
        print("NOTES FOR EVALIDATOR COMPARISON:")
        print("-" * 80)
        print("1. This estimate includes all live trees ≥5\" DBH on forestland")
        print("2. Volume is net cubic feet (gross minus defects)")
        print("3. Forest land is defined as COND_STATUS_CD = 1")
        print("4. Results should match EVALIDator's 'Net volume of live trees'")
        print("5. Minor differences may occur due to:")
        print("   - Rounding in intermediate calculations")
        print("   - Different EVALID selection if multiple are available")
        print("   - Database version differences")

if __name__ == "__main__":
    calculate_net_volume_for_texas()