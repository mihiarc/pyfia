#!/usr/bin/env python3
"""
Demonstration of the new MortalityConfig usage in pyFIA.

This script shows how to use the enhanced Pydantic-based configuration
for mortality estimation with proper validation and type safety.
"""

from pyfia import FIA, MortalityCalculator, MortalityConfig
import polars as pl


def main():
    """Demonstrate mortality configuration usage."""
    
    # Example database path (adjust as needed)
    db_path = "/path/to/fia.duckdb"
    
    # Example 1: Basic mortality configuration
    print("Example 1: Basic mortality estimation")
    basic_config = MortalityConfig(
        mortality_type="tpa",
        land_type="forest",
        tree_type="all",
        variance=True,
        totals=True
    )
    print(f"Basic config grouping columns: {basic_config.get_grouping_columns()}")
    print(f"Expected output columns: {basic_config.get_output_columns()}")
    print()
    
    # Example 2: Mortality by species and ownership
    print("Example 2: Mortality by species and ownership")
    species_config = MortalityConfig(
        mortality_type="both",  # Calculate both TPA and volume
        by_species=True,
        group_by_ownership=True,
        variance=True,
        totals=True,
        include_components=True  # Include BA components
    )
    print(f"Species config grouping columns: {species_config.get_grouping_columns()}")
    print(f"Expected output columns: {species_config.get_output_columns()}")
    print()
    
    # Example 3: Complex grouping with validation
    print("Example 3: Complex mortality grouping")
    complex_config = MortalityConfig(
        mortality_type="volume",
        grp_by=["STATECD", "UNITCD", "COUNTYCD"],
        by_species=True,
        group_by_species_group=True,
        group_by_agent=True,
        group_by_disturbance=True,
        tree_domain="DIA >= 10.0",
        area_domain="COND_STATUS_CD == 1",
        variance=True,
        totals=True,
        tree_class="timber"
    )
    print(f"Complex config grouping columns: {complex_config.get_grouping_columns()}")
    print(f"Expected output columns: {complex_config.get_output_columns()}")
    print()
    
    # Example 4: Validation examples
    print("Example 4: Configuration validation")
    
    try:
        # This will raise a validation error
        invalid_config = MortalityConfig(
            mortality_type="volume",
            tree_type="live",  # Can't calculate mortality on live trees!
            land_type="forest"
        )
    except ValueError as e:
        print(f"Validation error caught: {e}")
    
    try:
        # This will also raise a validation error
        invalid_config2 = MortalityConfig(
            tree_class="timber",
            land_type="forest"  # timber class requires timber land type
        )
    except ValueError as e:
        print(f"Validation error caught: {e}")
    
    # Example 5: Using with MortalityCalculator (if database available)
    print("\nExample 5: Using with MortalityCalculator")
    
    # Create a configuration for actual use
    calc_config = MortalityConfig(
        mortality_type="tpa",
        by_species=True,
        group_by_ownership=True,
        tree_domain="STATUSCD == 2",  # Dead trees
        variance=True,
        totals=True,
        variance_method="ratio"
    )
    
    # Show how it would be used (uncomment with real database)
    # with FIA(db_path) as db:
    #     db.clip_by_state(37)  # North Carolina
    #     calculator = MortalityCalculator(db, calc_config)
    #     results = calculator.estimate()
    #     print(results.head())
    
    # Example 6: Converting to legacy config for backwards compatibility
    print("\nExample 6: Backwards compatibility")
    legacy_config = calc_config.to_estimator_config()
    print(f"Legacy config type: {type(legacy_config)}")
    print(f"Legacy config grp_by: {legacy_config.grp_by}")
    print(f"Legacy config extra_params: {legacy_config.extra_params}")
    
    # Example 7: Domain expression validation
    print("\nExample 7: Domain expression validation")
    
    safe_config = MortalityConfig(
        tree_domain="DIA >= 10.0 AND STATUSCD == 2",
        area_domain="FORTYPCD IN (121, 122, 123)"
    )
    print("Safe domain expressions validated successfully")
    
    try:
        # This would be caught by validation
        dangerous_config = MortalityConfig(
            tree_domain="DIA >= 10; DROP TABLE TREE; --"
        )
    except ValueError as e:
        print(f"Dangerous SQL caught: {e}")


if __name__ == "__main__":
    main()