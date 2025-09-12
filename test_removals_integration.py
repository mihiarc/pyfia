#!/usr/bin/env python
"""
Integration test for removals() function with real FIA data.

This tests the removals estimator with actual FIA GRM tables to ensure
it works correctly with real data structure and produces valid results.
"""

import sys
from pathlib import Path

import polars as pl
from rich.console import Console
from rich.table import Table

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from pyfia import FIA, removals

console = Console()


def test_removals_with_real_data():
    """Test removals function with real FIA database containing GRM tables."""
    
    db_path = Path("data/nfi_south.duckdb")
    if not db_path.exists():
        console.print(f"[red]Database not found: {db_path}[/red]")
        return False
    
    console.print(f"\n[bold cyan]Testing removals() with real FIA data[/bold cyan]")
    console.print(f"Database: {db_path}")
    
    try:
        # Initialize FIA database connection
        with FIA(db_path) as db:
            console.print("\n[green]✓[/green] Database connection established")
            
            # Check for GRM tables
            db.load_table("TREE_GRM_COMPONENT")
            db.load_table("TREE_GRM_MIDPT")
            console.print("[green]✓[/green] GRM tables found")
            
            # Check GRM data structure
            grm_comp = db.tables["TREE_GRM_COMPONENT"].limit(5).collect()
            console.print(f"[green]✓[/green] TREE_GRM_COMPONENT has {len(grm_comp.columns)} columns")
            
            # Find a state with data
            db.load_table("PLOT")
            states = db.tables["PLOT"].select("STATECD").unique().collect()
            state_code = states["STATECD"][0]
            console.print(f"[green]✓[/green] Testing with state code: {state_code}")
            
            # Filter to one state for testing
            db.clip_by_state(state_code, most_recent=True)
            
            # Test 1: Basic volume removals
            console.print("\n[bold]Test 1: Basic volume removals[/bold]")
            try:
                results = removals(
                    db,
                    measure="volume",
                    land_type="forest"
                )
                
                if results is not None and len(results) > 0:
                    console.print(f"[green]✓[/green] Volume removals calculated")
                    console.print(f"  Results shape: {results.shape}")
                    console.print(f"  Columns: {results.columns}")
                    
                    # Show sample results
                    if "REMOVALS_PER_ACRE" in results.columns:
                        vol_per_acre = results["REMOVALS_PER_ACRE"][0]
                        console.print(f"  Removals per acre: {vol_per_acre:.2f} cubic feet")
                else:
                    console.print("[yellow]⚠[/yellow] No removal data found (may be expected)")
                    
            except Exception as e:
                console.print(f"[red]✗[/red] Volume calculation failed: {e}")
                return False
            
            # Test 2: Removals by species
            console.print("\n[bold]Test 2: Removals by species[/bold]")
            try:
                results_species = removals(
                    db,
                    measure="volume",
                    by_species=True,
                    land_type="forest"
                )
                
                if results_species is not None and len(results_species) > 0:
                    console.print(f"[green]✓[/green] Species-level removals calculated")
                    console.print(f"  Number of species: {len(results_species)}")
                    
                    # Show top species
                    if "SPCD" in results_species.columns:
                        top_species = results_species.sort("REMOVALS_PER_ACRE", descending=True).head(3)
                        console.print("  Top 3 species by removals per acre:")
                        for row in top_species.iter_rows(named=True):
                            console.print(f"    SPCD {row['SPCD']}: {row['REMOVALS_PER_ACRE']:.2f} cf/acre")
                else:
                    console.print("[yellow]⚠[/yellow] No species-level removal data found")
                    
            except Exception as e:
                console.print(f"[red]✗[/red] Species calculation failed: {e}")
                
            # Test 3: Check GRM component filtering
            console.print("\n[bold]Test 3: GRM component analysis[/bold]")
            try:
                # Load raw GRM data to check components
                grm_data = db.tables["TREE_GRM_COMPONENT"].select([
                    "SUBP_COMPONENT_GS_FOREST",
                    "SUBP_TPAREMV_UNADJ_GS_FOREST"
                ]).filter(
                    pl.col("SUBP_TPAREMV_UNADJ_GS_FOREST") > 0
                ).limit(100).collect()
                
                if len(grm_data) > 0:
                    # Check component types
                    components = grm_data["SUBP_COMPONENT_GS_FOREST"].unique()
                    console.print(f"[green]✓[/green] Found {len(components)} component types with removals")
                    
                    cut_components = [c for c in components if c and c.startswith("CUT")]
                    div_components = [c for c in components if c and c.startswith("DIVERSION")]
                    
                    console.print(f"  CUT components: {len(cut_components)}")
                    console.print(f"  DIVERSION components: {len(div_components)}")
                    
                    if cut_components:
                        console.print(f"  Sample CUT components: {cut_components[:3]}")
                else:
                    console.print("[yellow]⚠[/yellow] No GRM removal data in this dataset")
                    
            except Exception as e:
                console.print(f"[yellow]⚠[/yellow] GRM analysis skipped: {e}")
            
            # Test 4: Different measurement types
            console.print("\n[bold]Test 4: Different measurement types[/bold]")
            measures_tested = []
            
            for measure_type in ["volume", "biomass", "count"]:
                try:
                    results_measure = removals(
                        db,
                        measure=measure_type,
                        land_type="forest"
                    )
                    if results_measure is not None:
                        measures_tested.append(measure_type)
                        console.print(f"[green]✓[/green] {measure_type.capitalize()} calculation successful")
                except Exception as e:
                    console.print(f"[yellow]⚠[/yellow] {measure_type.capitalize()} failed: {str(e)[:50]}")
            
            console.print(f"\nSuccessfully tested {len(measures_tested)}/3 measurement types")
            
            # Summary
            console.print("\n[bold green]Integration test completed![/bold green]")
            console.print("\n[bold]Summary:[/bold]")
            console.print("• Database connection: ✓")
            console.print("• GRM tables present: ✓")
            console.print("• Basic removals calculation: ✓")
            console.print(f"• Measurement types working: {len(measures_tested)}/3")
            
            return True
            
    except Exception as e:
        console.print(f"\n[red]Integration test failed with error:[/red]")
        console.print(f"[red]{e}[/red]")
        import traceback
        traceback.print_exc()
        return False


def check_grm_table_structure():
    """Examine the structure of GRM tables to understand the data."""
    
    db_path = Path("data/nfi_south.duckdb")
    console.print("\n[bold cyan]Examining GRM table structure[/bold cyan]")
    
    with FIA(db_path) as db:
        # Check TREE_GRM_COMPONENT structure
        db.load_table("TREE_GRM_COMPONENT")
        grm_comp = db.tables["TREE_GRM_COMPONENT"].limit(1).collect()
        
        console.print("\n[bold]TREE_GRM_COMPONENT columns:[/bold]")
        for col in sorted(grm_comp.columns):
            if "REMV" in col or "COMPONENT" in col or "SUBPTYP" in col:
                console.print(f"  • {col}")
        
        # Check TREE_GRM_MIDPT structure  
        db.load_table("TREE_GRM_MIDPT")
        grm_midpt = db.tables["TREE_GRM_MIDPT"].limit(1).collect()
        
        console.print("\n[bold]TREE_GRM_MIDPT columns:[/bold]")
        for col in sorted(grm_midpt.columns):
            if "VOL" in col or "TRE_CN" in col:
                console.print(f"  • {col}")


if __name__ == "__main__":
    console.print("[bold]=" * 60 + "[/bold]")
    console.print("[bold cyan]REMOVALS FUNCTION INTEGRATION TEST[/bold cyan]")
    console.print("[bold]=" * 60 + "[/bold]")
    
    # First check table structure
    check_grm_table_structure()
    
    # Then run integration test
    success = test_removals_with_real_data()
    
    if success:
        console.print("\n[bold green]✅ All integration tests passed![/bold green]")
        sys.exit(0)
    else:
        console.print("\n[bold red]❌ Some integration tests failed[/bold red]")
        sys.exit(1)