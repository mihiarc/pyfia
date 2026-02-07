#!/usr/bin/env python3
"""
Example of using the new MortalityConfig with pyFIA mortality estimation.

This script demonstrates how to use the enhanced Pydantic-based configuration
for mortality analysis with real FIA data.
"""

from pyfia import FIA, MortalityCalculator, MortalityConfig, mortality
import polars as pl


def example_basic_mortality(db_path: str):
    """Basic mortality estimation example."""
    print("=" * 60)
    print("Example 1: Basic Mortality Estimation")
    print("=" * 60)
    
    # Using the convenience function (backwards compatible)
    with FIA(db_path) as db:
        db.clip_by_state(13)  # Georgia
        
        # Traditional approach still works
        results = mortality(
            db,
            by_species=True,
            variance=True,
            totals=True
        )
        
        print("\nTop 5 species by mortality (TPA):")
        print(results.sort("MORTALITY_TPA", descending=True).head(5))
        

def example_advanced_config(db_path: str):
    """Advanced mortality estimation with new config."""
    print("\n" + "=" * 60)
    print("Example 2: Advanced Mortality with MortalityConfig")
    print("=" * 60)
    
    # Create a sophisticated configuration
    config = MortalityConfig(
        # Mortality calculation options
        mortality_type="both",  # Calculate both TPA and volume
        tree_class="timber",    # Focus on timber trees
        land_type="timber",     # Timber land only
        
        # Grouping options
        grp_by=["UNITCD", "COUNTYCD"],  # Geographic grouping
        by_species=True,                  # Group by species
        group_by_ownership=True,          # Include ownership
        group_by_agent=True,              # Include mortality agent
        
        # Domain filters
        tree_domain="DIA >= 10.0 AND STATUSCD == 2",  # Large dead trees
        area_domain="COND_STATUS_CD == 1",            # Forested conditions
        
        # Output options
        variance=True,           # Include variance calculations
        totals=True,            # Include total estimates
        include_components=True, # Include BA components
        
        # Variance method
        variance_method="ratio"  # Use ratio variance method
    )
    
    with FIA(db_path) as db:
        db.clip_by_state(13)  # Georgia
        
        # Use the calculator directly with new config
        calculator = MortalityCalculator(db, config)
        results = calculator.estimate()
        
        print(f"\nGrouping columns used: {config.get_grouping_columns()}")
        print(f"Output columns: {results.columns}")
        print(f"\nNumber of groups: {len(results)}")
        
        # Show summary by ownership
        if "OWNGRPCD" in results.columns:
            ownership_summary = (
                results
                .group_by("OWNGRPCD")
                .agg([
                    pl.sum("MORTALITY_TPA_TOTAL").alias("TOTAL_MORTALITY_TPA"),
                    pl.sum("MORTALITY_VOL_TOTAL").alias("TOTAL_MORTALITY_VOL")
                ])
                .sort("TOTAL_MORTALITY_TPA", descending=True)
            )
            print("\nMortality by ownership group:")
            print(ownership_summary)


def example_validation(db_path: str):
    """Demonstrate configuration validation."""
    print("\n" + "=" * 60)
    print("Example 3: Configuration Validation")
    print("=" * 60)
    
    # Example of validation in action
    try:
        # This will fail validation
        bad_config = MortalityConfig(
            mortality_type="volume",
            tree_type="live"  # Can't calculate mortality on live trees!
        )
    except ValueError as e:
        print(f"✓ Validation caught error: {e}")
    
    try:
        # This will also fail
        bad_config2 = MortalityConfig(
            tree_class="timber",
            land_type="forest"  # Timber class needs timber land type
        )
    except ValueError as e:
        print(f"✓ Validation caught error: {e}")
    
    # Show a valid timber configuration
    valid_config = MortalityConfig(
        mortality_type="volume",
        tree_type="dead",
        tree_class="timber",
        land_type="timber"
    )
    print(f"\n✓ Valid timber mortality config created successfully")
    print(f"  - Tree type: {valid_config.tree_type}")
    print(f"  - Tree class: {valid_config.tree_class}")
    print(f"  - Land type: {valid_config.land_type}")


def example_comparison(db_path: str):
    """Compare old and new configuration approaches."""
    print("\n" + "=" * 60)
    print("Example 4: Old vs New Configuration Approaches")
    print("=" * 60)
    
    with FIA(db_path) as db:
        db.clip_by_state(13)  # Georgia
        
        # Old approach - passing many parameters
        print("Old approach - function with many parameters:")
        results_old = mortality(
            db,
            by_species=True,
            by_ownership=True,
            by_agent=True,
            tree_domain="DIA >= 10.0",
            variance=True,
            totals=True
        )
        
        # New approach - structured configuration
        print("\nNew approach - structured configuration:")
        config = MortalityConfig(
            by_species=True,
            group_by_ownership=True,
            group_by_agent=True,
            tree_domain="DIA >= 10.0",
            variance=True,
            totals=True
        )
        
        calculator = MortalityCalculator(db, config)
        results_new = calculator.estimate()
        
        # Results should be identical
        print(f"\nOld approach result shape: {results_old.shape}")
        print(f"New approach result shape: {results_new.shape}")
        print(f"Results identical: {results_old.equals(results_new)}")


def main():
    """Run all examples."""
    # Update this path to your FIA database
    db_path = "/path/to/fia_georgia.duckdb"
    
    # Check if path needs updating
    import os
    if not os.path.exists(db_path):
        print("Please update db_path in the script to point to your FIA database.")
        print("Example: db_path = '/data/fia/georgia.duckdb'")
        return
    
    example_basic_mortality(db_path)
    example_advanced_config(db_path)
    example_validation(db_path)
    example_comparison(db_path)


if __name__ == "__main__":
    main()