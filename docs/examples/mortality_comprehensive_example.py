#!/usr/bin/env python3
"""
Comprehensive example of mortality estimation in pyFIA.

This example demonstrates:
1. Basic mortality estimation
2. Mortality by species
3. Mortality with variance calculation
4. Using both MortalityConfig and parameter-based usage patterns
5. Grouping by multiple variables
6. Different mortality types (TPA and volume)
"""

import polars as pl
from pyfia import FIA, mortality
from pyfia.estimation.config import MortalityConfig


def main():
    """Run comprehensive mortality estimation examples."""
    # Initialize database (auto-detects DuckDB or SQLite)
    db_path = "path/to/fia.duckdb"  # or .db for SQLite
    
    print("=== PyFIA Mortality Estimation Examples ===\n")
    
    # Example 1: Basic mortality estimation (parameter-based)
    print("1. Basic Mortality Estimation (TPA)")
    print("-" * 40)
    
    with FIA(db_path) as db:
        # Clip to a specific state (e.g., Georgia = 13)
        db.clip_by_state(13, most_recent=True)
        
        # Basic mortality with default settings
        basic_mortality = mortality(db)
        
        print("Basic mortality results:")
        print(basic_mortality.select([
            "ESTN_UNIT_CN",
            "MORTALITY_PER_ACRE",
            "N_PLOTS",
            "N_TREES"
        ]).head())
        print()
    
    # Example 2: Mortality by species (parameter-based)
    print("2. Mortality by Species")
    print("-" * 40)
    
    with FIA(db_path) as db:
        db.clip_by_state(13, most_recent=True)
        
        # Mortality grouped by species
        species_mortality = mortality(
            db,
            by_species=True,
            variance=False  # Standard error by default
        )
        
        print("Top 5 species by mortality:")
        print(
            species_mortality
            .sort("MORTALITY_PER_ACRE", descending=True)
            .select([
                "SPCD",
                "MORTALITY_PER_ACRE",
                "SE_OF_ESTIMATE",
                "SE_OF_ESTIMATE_PCT",
                "N_PLOTS"
            ])
            .head()
        )
        print()
    
    # Example 3: Using MortalityConfig for complex analysis
    print("3. Complex Mortality Analysis with Config")
    print("-" * 40)
    
    with FIA(db_path) as db:
        db.clip_by_state(13, most_recent=True)
        
        # Create config for detailed mortality analysis
        config = MortalityConfig(
            # Grouping options
            by_species=True,
            group_by_ownership=True,
            group_by_agent=True,
            
            # Mortality types
            mortality_type="both",  # Both TPA and volume
            
            # Include components
            include_components=True,  # Include BA mortality
            
            # Output options
            totals=True,  # Include total estimates
            variance=True,  # Return variance instead of SE
            
            # Filters
            tree_type="dead",  # Mortality requires dead trees
            tree_class="all",  # All tree classes
            tree_domain="DIA >= 5.0",  # Only trees >= 5 inches
            land_type="forest"
        )
        
        detailed_mortality = mortality(db, config)
        
        print("Detailed mortality results shape:", detailed_mortality.shape)
        print("\nColumns:", detailed_mortality.columns)
        print("\nSample results:")
        print(
            detailed_mortality
            .filter(pl.col("SPCD").is_not_null())
            .select([
                "SPCD",
                "OWNGRPCD",
                "AGENTCD",
                "MORTALITY_TPA",
                "MORTALITY_VOL",
                "MORTALITY_BA",
                "N_PLOTS"
            ])
            .head(5)
        )
        print()
    
    # Example 4: Mixed usage - config with parameter overrides
    print("4. Mixed Usage Pattern (Config + Overrides)")
    print("-" * 40)
    
    with FIA(db_path) as db:
        db.clip_by_state(13, most_recent=True)
        
        # Base config
        base_config = MortalityConfig(
            mortality_type="tpa",
            by_species=True,
            variance=False,
            tree_type="dead"  # Required for mortality
        )
        
        # Override some parameters
        override_mortality = mortality(
            db,
            config=base_config,
            mortality_type="both",  # Override to get both TPA and volume
            by_ownership=True,  # Add ownership grouping
            variance=True  # Override to get variance
        )
        
        print("Override results columns:", override_mortality.columns)
        print()
    
    # Example 5: Custom grouping with grp_by
    print("5. Custom Grouping Variables")
    print("-" * 40)
    
    with FIA(db_path) as db:
        db.clip_by_state(13, most_recent=True)
        
        # Group by multiple custom variables
        custom_mortality = mortality(
            db,
            grp_by=["UNITCD", "COUNTYCD", "FORTYPCD"],
            mortality_type="tpa",
            tree_domain="STATUSCD == 2"  # Only dead trees
        )
        
        print("Custom grouping results:")
        print(
            custom_mortality
            .select([
                "UNITCD",
                "COUNTYCD", 
                "FORTYPCD",
                "MORTALITY_PER_ACRE",
                "N_PLOTS"
            ])
            .head(10)
        )
        print()
    
    # Example 6: Mortality by disturbance
    print("6. Mortality by Disturbance Type")
    print("-" * 40)
    
    with FIA(db_path) as db:
        db.clip_by_state(13, most_recent=True)
        
        # Analyze mortality by disturbance codes
        disturbance_config = MortalityConfig(
            group_by_disturbance=True,
            mortality_type="both",
            include_components=True
        )
        
        disturbance_mortality = mortality(db, disturbance_config)
        
        print("Mortality by disturbance codes:")
        print(
            disturbance_mortality
            .filter(pl.col("DSTRBCD1").is_not_null())
            .select([
                "DSTRBCD1",
                "MORTALITY_TPA",
                "MORTALITY_VOL", 
                "MORTALITY_BA",
                "N_PLOTS"
            ])
            .sort("MORTALITY_TPA", descending=True)
            .head(10)
        )
        print()
    
    # Example 7: Mortality trends (if multiple years available)
    print("7. Mortality Trends Analysis")
    print("-" * 40)
    
    with FIA(db_path) as db:
        # Don't use most_recent to get multiple years
        db.clip_by_state(13, most_recent=False)
        
        # Group by year and species
        trend_mortality = mortality(
            db,
            grp_by=["INVYR"],
            by_species=True,
            tree_domain="SPCD IN (131, 110, 833)"  # Specific species
        )
        
        print("Mortality trends by year and species:")
        print(
            trend_mortality
            .select([
                "INVYR",
                "SPCD",
                "MORTALITY_PER_ACRE",
                "SE_OF_ESTIMATE_PCT"
            ])
            .sort(["SPCD", "INVYR"])
            .head(15)
        )
        print()
    
    # Example 8: Harvest vs Natural mortality
    print("8. Harvest vs Natural Mortality")
    print("-" * 40)
    
    with FIA(db_path) as db:
        db.clip_by_state(13, most_recent=True)
        
        # Natural mortality only
        natural_config = MortalityConfig(
            by_species=True,
            include_natural=True,
            include_harvest=False,
            mortality_type="both"
        )
        
        natural_mortality = mortality(db, natural_config)
        
        # Harvest mortality only  
        harvest_config = MortalityConfig(
            by_species=True,
            include_natural=False,
            include_harvest=True,
            mortality_type="both"
        )
        
        harvest_mortality = mortality(db, harvest_config)
        
        print("Comparison of natural vs harvest mortality:")
        print("\nNatural mortality (top 5 species):")
        print(
            natural_mortality
            .sort("MORTALITY_TPA", descending=True)
            .select(["SPCD", "MORTALITY_TPA", "MORTALITY_VOL"])
            .head(5)
        )
        
        print("\nHarvest mortality (top 5 species):")
        print(
            harvest_mortality
            .sort("MORTALITY_TPA", descending=True)
            .select(["SPCD", "MORTALITY_TPA", "MORTALITY_VOL"])
            .head(5)
        )


if __name__ == "__main__":
    main()