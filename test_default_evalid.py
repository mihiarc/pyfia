#!/usr/bin/env python
"""
Test that the area() function applies default EVALID filtering to prevent overcounting.
"""

from pyfia import FIA, area
from rich.console import Console

console = Console()

def test_default_evalid_filtering():
    """Test area() with and without explicit EVALID filtering."""
    
    console.print("\n[bold cyan]Testing Default EVALID Filtering in area() Function[/bold cyan]")
    console.print("=" * 60)
    
    # Test 1: Call area() without any EVALID filtering
    console.print("\n[yellow]Test 1: Calling area() without pre-filtering database[/yellow]")
    console.print("This should automatically apply most recent EVALID filter")
    
    with FIA("nfi_south.duckdb") as db:
        # Don't apply any filtering - let area() handle it
        # Filter only by state to have a reasonable dataset
        db.state_filter = [40]  # Oklahoma
        
        # Call area without any EVALID filtering
        results1 = area(db, totals=True)
        
        console.print("\nResults from area() with automatic EVALID filtering:")
        console.print(f"  Forest area: {results1['AREA'][0]:,.0f} acres")
        console.print(f"  Forest percentage: {results1['AREA_PERC'][0]:.2f}%")
        console.print(f"  Number of plots: {results1['N_PLOTS'][0]:,}")
    
    # Test 2: Explicitly filter to most recent EVALID first
    console.print("\n[yellow]Test 2: Pre-filtering database to most recent EVALID[/yellow]")
    
    with FIA("nfi_south.duckdb") as db:
        # Explicitly filter to most recent
        db.clip_by_state(40, most_recent=True)
        
        # Call area - should get same results as Test 1
        results2 = area(db, totals=True)
        
        console.print("\nResults from area() with explicit EVALID filtering:")
        console.print(f"  Forest area: {results2['AREA'][0]:,.0f} acres")
        console.print(f"  Forest percentage: {results2['AREA_PERC'][0]:.2f}%")
        console.print(f"  Number of plots: {results2['N_PLOTS'][0]:,}")
    
    # Compare results
    console.print("\n[bold]Comparison:[/bold]")
    
    area_match = abs(results1['AREA'][0] - results2['AREA'][0]) < 1
    perc_match = abs(results1['AREA_PERC'][0] - results2['AREA_PERC'][0]) < 0.01
    plots_match = results1['N_PLOTS'][0] == results2['N_PLOTS'][0]
    
    if area_match and perc_match and plots_match:
        console.print("[green]✓ Results match! Default EVALID filtering is working correctly.[/green]")
        console.print("  The area() function successfully prevents overcounting by")
        console.print("  defaulting to most recent EVALID when none is specified.")
    else:
        console.print("[red]✗ Results don't match![/red]")
        console.print(f"  Area difference: {abs(results1['AREA'][0] - results2['AREA'][0]):,.0f} acres")
        console.print(f"  Percentage difference: {abs(results1['AREA_PERC'][0] - results2['AREA_PERC'][0]):.2f}%")
        console.print(f"  Plot count difference: {abs(results1['N_PLOTS'][0] - results2['N_PLOTS'][0])}")
    
    # Test 3: Show what happens without any filtering (if we had access to all EVALIDs)
    console.print("\n[yellow]Test 3: Checking available EVALIDs in the database[/yellow]")
    
    with FIA("nfi_south.duckdb") as db:
        # Get connection to check EVALIDs
        conn = db._reader._backend._connection
        
        # Count total plots for Oklahoma across all EVALIDs
        all_evalids_query = """
            SELECT 
                COUNT(DISTINCT ppsa.PLT_CN) as total_plots,
                COUNT(DISTINCT ppsa.EVALID) as num_evalids,
                MIN(p.INVYR) as min_year,
                MAX(p.INVYR) as max_year
            FROM POP_PLOT_STRATUM_ASSGN ppsa
            JOIN PLOT p ON ppsa.PLT_CN = p.CN
            WHERE p.STATECD = 40
        """
        
        result = conn.execute(all_evalids_query).fetchone()
        total_plots, num_evalids, min_year, max_year = result
        
        console.print(f"\nOklahoma database statistics:")
        console.print(f"  Total unique plots (all EVALIDs): {total_plots:,}")
        console.print(f"  Number of EVALIDs: {num_evalids}")
        console.print(f"  Year range: {min_year} - {max_year}")
        console.print(f"  Plots used in estimate (most recent): {results1['N_PLOTS'][0]:,}")
        console.print(f"  Reduction factor: {results1['N_PLOTS'][0]/total_plots:.1%}")
        console.print(f"\n[dim]The default EVALID filtering prevents using all {total_plots:,} plots[/dim]")
        console.print(f"[dim]which would lead to overcounting across {num_evalids} evaluation periods.[/dim]")

if __name__ == "__main__":
    test_default_evalid_filtering()