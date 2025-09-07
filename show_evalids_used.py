#!/usr/bin/env python
"""
Show which EVALIDs are used when calling the area() function.
"""

from pyfia import FIA, area
from rich.console import Console
from rich.table import Table

def main():
    """Show EVALIDs used in area() calculation."""
    console = Console()
    
    console.print("\n[bold cyan]EVALIDs Used in area() Function Call[/bold cyan]")
    console.print("=" * 50)
    
    with FIA("nfi_south.duckdb") as db:
        # Filter to Oklahoma
        db.clip_by_state(40, most_recent=True)
        
        # Check what EVALIDs are set after filtering
        console.print("\n[yellow]After clip_by_state(40, most_recent=True):[/yellow]")
        
        if db.evalid:
            console.print(f"Number of EVALIDs: {len(db.evalid)}")
            console.print(f"EVALID values: {sorted(db.evalid)}")
            
            # Get more details about these EVALIDs from the database
            conn = db._reader._backend._connection
            evalid_str = ", ".join(str(e) for e in db.evalid)
            
            # Query to get EVALID details - check table structure first
            # First, let's see what columns POP_EVAL has
            try:
                # Try with common column names
                evalid_details = f"""
                    SELECT DISTINCT
                        ppsa.EVALID,
                        COUNT(DISTINCT ppsa.PLT_CN) as plot_count,
                        MIN(p.INVYR) as min_year,
                        MAX(p.INVYR) as max_year
                    FROM POP_PLOT_STRATUM_ASSGN ppsa
                    JOIN PLOT p ON ppsa.PLT_CN = p.CN
                    WHERE ppsa.EVALID IN ({evalid_str})
                    GROUP BY ppsa.EVALID
                    ORDER BY ppsa.EVALID
                """
            except:
                # Fallback query
                evalid_details = f"""
                    SELECT DISTINCT
                        EVALID,
                        COUNT(DISTINCT PLT_CN) as plot_count
                    FROM POP_PLOT_STRATUM_ASSGN
                    WHERE EVALID IN ({evalid_str})
                    GROUP BY EVALID
                    ORDER BY EVALID
                """
            
            results = conn.execute(evalid_details).fetchall()
            
            # Create table for EVALID details
            table = Table(title="EVALIDs Being Used", show_header=True, header_style="bold magenta")
            table.add_column("EVALID", style="cyan")
            table.add_column("Plots", style="magenta", justify="right")
            table.add_column("Min Year", style="green", justify="center")
            table.add_column("Max Year", style="blue", justify="center")
            
            total_plots = 0
            for row in results:
                if len(row) == 4:  # Full query worked
                    evalid, plots, min_year, max_year = row
                    table.add_row(
                        str(evalid),
                        f"{plots:,}" if plots else "0",
                        str(min_year) if min_year else "N/A",
                        str(max_year) if max_year else "N/A"
                    )
                else:  # Fallback query
                    evalid, plots = row
                    table.add_row(
                        str(evalid),
                        f"{plots:,}" if plots else "0",
                        "N/A",
                        "N/A"
                    )
                total_plots += plots if plots else 0
            
            console.print("\n")
            console.print(table)
            console.print(f"\n[bold]Total plots across all EVALIDs: {total_plots:,}[/bold]")
            
        else:
            console.print("[red]No EVALIDs set![/red]")
        
        # Now call area() and show results
        console.print("\n[yellow]Calling area() function:[/yellow]")
        results = area(db, totals=True)
        
        console.print(f"\nResults:")
        console.print(f"  Forest area: {results['AREA'][0]:,.0f} acres")
        console.print(f"  Forest percentage: {results['AREA_PERC'][0]:.2f}%")
        console.print(f"  Number of plots: {results['N_PLOTS'][0]:,}")
        
    # Now test what happens without pre-filtering
    console.print("\n" + "=" * 50)
    console.print("\n[yellow]Testing area() without pre-filtering:[/yellow]")
    
    with FIA("nfi_south.duckdb") as db:
        # Don't apply any EVALID filtering - just set state
        db.state_filter = [40]
        
        console.print("Before area() call:")
        console.print(f"  db.evalid = {db.evalid}")
        
        # Call area - it should apply default filtering
        results = area(db, totals=True)
        
        console.print("\nAfter area() call:")
        console.print(f"  db.evalid = {db.evalid}")
        console.print(f"\nResults:")
        console.print(f"  Forest area: {results['AREA'][0]:,.0f} acres")
        console.print(f"  Forest percentage: {results['AREA_PERC'][0]:.2f}%")
        console.print(f"  Number of plots: {results['N_PLOTS'][0]:,}")

if __name__ == "__main__":
    main()