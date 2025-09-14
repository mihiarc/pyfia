#!/usr/bin/env python
"""
Simple test of TPA function with Georgia data.
"""

from pyfia import FIA, tpa

def main():
    print("Simple TPA Test - Georgia")
    print("-" * 40)

    # Load database and set EVALID
    db = FIA("data/georgia.duckdb")
    db.clip_most_recent(eval_type="VOL")
    print(f"Selected EVALID: {db.evalid}")

    # Manually load tables with all columns to work around column selection issue
    db.load_table("TREE", columns=None)
    db.load_table("COND", columns=None)
    db.load_table("PLOT", columns=None)

    # Now run TPA estimation
    print("\nCalculating TPA on forestland...")
    results = tpa(db, land_type="forest", totals=True)

    if not results.is_empty():
        print(f"\nResults:")
        print(f"  TPA: {results['TPA'][0]:.1f} trees/acre")
        print(f"  BAA: {results['BAA'][0]:.1f} sq ft/acre")
        print(f"  Standard Error:")
        print(f"    TPA SE: {results['TPA_SE'][0]:.1f} ({results['TPA_SE'][0]/results['TPA'][0]*100:.1f}% CV)")
        print(f"    BAA SE: {results['BAA_SE'][0]:.1f} ({results['BAA_SE'][0]/results['BAA'][0]*100:.1f}% CV)")
        print(f"  Sample size: {results['N_PLOTS'][0]:,} plots")
        print(f"  Trees measured: {results['N_TREES'][0]:,}")

        if "TPA_TOTAL" in results.columns:
            print(f"\n  Population totals:")
            print(f"    Total trees: {results['TPA_TOTAL'][0]:,.0f}")
            print(f"    Total basal area: {results['BAA_TOTAL'][0]:,.0f} sq ft")

    # Test by species
    print("\n\nTop 5 species by TPA:")
    results_species = tpa(db, by_species=True, land_type="forest")
    if not results_species.is_empty():
        top5 = results_species.sort("TPA", descending=True).head(5)
        for row in top5.iter_rows(named=True):
            spcd = int(row['SPCD']) if row['SPCD'] is not None else 0
            print(f"  Species {spcd:3d}: {row['TPA']:6.1f} TPA, {row['BAA']:5.1f} BAA")

    print("\n" + "="*40)
    print("Expected values for Georgia forestland:")
    print("  TPA: ~430-450 trees/acre")
    print("  BAA: ~100-110 sq ft/acre")
    print("="*40)

if __name__ == "__main__":
    main()