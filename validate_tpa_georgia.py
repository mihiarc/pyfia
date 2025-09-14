#!/usr/bin/env python
"""
Validate TPA function against published Georgia FIA estimates.

Published values from FIA EVALIDator and Georgia Forestry Commission reports.
"""

import warnings
import polars as pl
from pyfia import FIA, tpa, area

def main():
    print("="*70)
    print("TPA Function Validation - Georgia FIA Data")
    print("="*70)

    # Connect to Georgia database
    print("\n1. Loading Georgia FIA database...")
    db = FIA("data/georgia.duckdb")

    # Check available EVALIDs
    print("\n2. Checking available evaluations...")
    query = """
    SELECT DISTINCT
        pe.EVALID,
        pe.EVAL_DESCR,
        pet.EVAL_TYP,
        pe.END_INVYR,
        COUNT(DISTINCT ppsa.PLT_CN) as plot_count
    FROM POP_EVAL pe
    JOIN POP_EVAL_TYP pet ON pe.CN = pet.EVAL_CN
    JOIN POP_PLOT_STRATUM_ASSGN ppsa ON pe.EVALID = ppsa.EVALID
    WHERE pe.STATECD = 13  -- Georgia
    GROUP BY pe.EVALID, pe.EVAL_DESCR, pet.EVAL_TYP, pe.END_INVYR
    ORDER BY pet.EVAL_TYP, pe.END_INVYR DESC
    """

    # Access connection properly
    if hasattr(db, 'conn'):
        conn = db.conn
    elif hasattr(db, '_reader') and hasattr(db._reader, 'conn'):
        conn = db._reader.conn
    else:
        import duckdb
        conn = duckdb.connect("data/georgia.duckdb", read_only=True)

    evalids = conn.execute(query).fetchdf()
    print("\nAvailable EVALIDs for Georgia:")
    print(evalids.to_string())

    # Select most recent EXPVOL evaluation
    print("\n3. Selecting most recent volume evaluation...")
    db.clip_most_recent(eval_type="VOL")
    print(f"   Selected EVALID: {db.evalid}")

    # Get forest area for context
    print("\n4. Calculating forest area...")
    area_results = area(db, land_type="forest")
    if not area_results.is_empty():
        forest_area = area_results["AREA"][0] if "AREA" in area_results.columns else area_results["AREA_TOTAL"][0]
        print(f"   Total forest area: {forest_area:,.0f} acres")

    # Calculate TPA and BAA estimates
    print("\n5. Calculating TPA and BAA estimates...")

    # Overall estimates on forestland
    print("\n   a) Overall forestland estimates:")
    results_forest = tpa(db, land_type="forest", totals=True)

    if not results_forest.is_empty():
        tpa_val = results_forest["TPA"][0]
        baa_val = results_forest["BAA"][0]
        tpa_se = results_forest["TPA_SE"][0]
        baa_se = results_forest["BAA_SE"][0]
        n_plots = results_forest["N_PLOTS"][0]
        n_trees = results_forest["N_TREES"][0]

        print(f"      TPA: {tpa_val:.1f} ± {tpa_se:.1f} trees/acre")
        print(f"      BAA: {baa_val:.1f} ± {baa_se:.1f} sq ft/acre")
        print(f"      Sample size: {n_plots:,} plots, {n_trees:,} trees")
        print(f"      CV: {(tpa_se/tpa_val*100):.1f}% (TPA), {(baa_se/baa_val*100):.1f}% (BAA)")

    # Timberland estimates
    print("\n   b) Timberland estimates:")
    results_timber = tpa(db, land_type="timber", totals=True)

    if not results_timber.is_empty():
        tpa_val = results_timber["TPA"][0]
        baa_val = results_timber["BAA"][0]
        tpa_se = results_timber["TPA_SE"][0]
        baa_se = results_timber["BAA_SE"][0]

        print(f"      TPA: {tpa_val:.1f} ± {tpa_se:.1f} trees/acre")
        print(f"      BAA: {baa_val:.1f} ± {baa_se:.1f} sq ft/acre")

    # By species (top 5)
    print("\n   c) Top 5 species by TPA:")
    results_species = tpa(db, by_species=True, land_type="forest")

    if not results_species.is_empty():
        top_species = results_species.sort("TPA", descending=True).head(5)

        # Try to add species names
        species_codes = {
            131: "Loblolly pine",
            121: "Longleaf pine",
            111: "Slash pine",
            316: "Sweetgum",
            611: "Sweetbay",
            802: "White oak",
            833: "Southern red oak",
            827: "Laurel oak",
            837: "Post oak"
        }

        for row in top_species.iter_rows(named=True):
            spcd = row["SPCD"]
            species_name = species_codes.get(spcd, f"Species {spcd}")
            tpa_val = row["TPA"]
            baa_val = row["BAA"]
            print(f"      {species_name:20s}: {tpa_val:6.1f} TPA, {baa_val:5.1f} BAA")

    # By size class
    print("\n   d) By diameter size class:")
    results_size = tpa(db, by_size_class=True, land_type="forest")

    if not results_size.is_empty():
        results_size = results_size.sort("SIZE_CLASS")
        for row in results_size.head(10).iter_rows(named=True):
            size_class = row["SIZE_CLASS"]
            tpa_val = row["TPA"]
            baa_val = row["BAA"]
            print(f"      {size_class:2.0f}-{size_class+1.9:3.1f} inches: {tpa_val:6.1f} TPA, {baa_val:5.1f} BAA")

    # Large trees only
    print("\n   e) Large trees (≥10 inches DBH):")
    results_large = tpa(db, tree_domain="DIA >= 10.0", land_type="forest")

    if not results_large.is_empty():
        tpa_val = results_large["TPA"][0]
        baa_val = results_large["BAA"][0]
        print(f"      TPA: {tpa_val:.1f} trees/acre")
        print(f"      BAA: {baa_val:.1f} sq ft/acre")

    # Compare with published values
    print("\n" + "="*70)
    print("COMPARISON WITH PUBLISHED VALUES")
    print("="*70)

    print("""
    Published Georgia forestland statistics (2019-2021 evaluation):
    Source: FIA EVALIDator and Georgia Forestry Commission

    Forestland (2021):
    - Area: ~24.8 million acres
    - Average TPA: ~430-450 trees/acre (all live trees)
    - Average BAA: ~100-110 sq ft/acre
    - Dominant species: Loblolly pine (~40% of volume)

    Timberland (2021):
    - Area: ~23.6 million acres
    - Average TPA: ~440 trees/acre
    - Average BAA: ~105 sq ft/acre

    Notes:
    - Published values may differ due to:
      * Different evaluation years
      * Different EVALID selection
      * Post-stratification adjustments
      * Rounding and aggregation methods
    """)

    # Test small sample warning
    print("\n6. Testing small sample warning...")
    try:
        # Filter to a very small area to trigger warning
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            db_small = FIA("data/georgia.duckdb")
            db_small.clip_by_evalid([db.evalid[0]])
            # Add extreme filter to get small sample
            results_small = tpa(
                db_small,
                tree_domain="DIA > 50.0",  # Very large trees only
                area_domain="STDAGE > 100"  # Very old stands only
            )

            if w:
                print(f"   ✓ Warning triggered: '{w[0].message}'")
            else:
                print(f"   Sample size: {results_small['N_PLOTS'][0] if not results_small.is_empty() else 0} plots")
    except Exception as e:
        print(f"   Error in small sample test: {e}")

    print("\n" + "="*70)
    print("Validation Complete")
    print("="*70)

if __name__ == "__main__":
    main()