#!/usr/bin/env python
"""
Example demonstrating the enhanced mortality calculator for pyFIA.

This script shows how to use the new mortality estimation features
with various grouping options and variance calculations.
"""

from pyfia import FIA, mortality


def main():
    """Run mortality estimation examples."""
    # Initialize FIA database (adjust path as needed)
    db_path = "path/to/fia.duckdb"
    
    print("pyFIA Enhanced Mortality Calculator Demo")
    print("=" * 50)
    
    try:
        with FIA(db_path) as db:
            # Example 1: Basic mortality estimation
            print("\n1. Basic mortality estimation (trees per acre)")
            results = mortality(db)
            print(results.select(["MORTALITY_TPA", "MORTALITY_TPA_SE", "N_PLOTS"]))
            
            # Example 2: Mortality by species
            print("\n2. Mortality by species")
            results = mortality(db, by_species=True)
            print(results.select(["SPCD", "COMMON_NAME", "MORTALITY_TPA", "MORTALITY_TPA_SE"])
                  .sort("MORTALITY_TPA", descending=True)
                  .head(10))
            
            # Example 3: Mortality by ownership and agent
            print("\n3. Mortality by ownership group and mortality agent")
            results = mortality(
                db,
                by_ownership=True,
                by_agent=True,
                include_components=True
            )
            print(results.select([
                "OWNGRPCD", "OWNGRPNM", "AGENTCD", "AGENTNM",
                "MORTALITY_TPA", "MORTALITY_BA", "MORTALITY_VOL"
            ]).head(10))
            
            # Example 4: Mortality with multiple groupings and variance
            print("\n4. Detailed mortality with variance components")
            results = mortality(
                db,
                by_species=True,
                by_ownership=True,
                by_disturbance=True,
                variance=True,
                totals=True
            )
            print(results.columns)
            
            # Example 5: Mortality for specific domain
            print("\n5. Mortality for loblolly pine in forest land")
            results = mortality(
                db,
                tree_domain="SPCD == 131",  # Loblolly pine
                land_type="forest",
                by_agent=True,
                include_components=True
            )
            print(results.select([
                "AGENTCD", "AGENTNM", 
                "MORTALITY_TPA", "MORTALITY_BA", "MORTALITY_VOL",
                "N_PLOTS"
            ]))
            
            # Example 6: Compare growing stock vs all trees mortality
            print("\n6. Comparing growing stock vs all trees mortality")
            gs_mort = mortality(db, tree_class="growing_stock")
            all_mort = mortality(db, tree_class="all")
            
            print(f"Growing stock mortality: {gs_mort['MORTALITY_TPA'][0]:.2f} TPA")
            print(f"All trees mortality: {all_mort['MORTALITY_TPA'][0]:.2f} TPA")
            
    except Exception as e:
        print(f"Error: {e}")
        print("\nNote: Update db_path to point to your FIA database")


if __name__ == "__main__":
    main()