#!/usr/bin/env python
"""
Test that area() uses only ONE EVALID to prevent overcounting.
"""

from pyfia import FIA, area
from rich.console import Console
from rich.table import Table

console = Console()

def test_single_evalid():
    """Verify area() uses only one EVALID."""
    
    console.print("\n[bold cyan]Testing Single EVALID Usage in area()[/bold cyan]")
    console.print("=" * 60)
    
    # Test with automatic EVALID selection
    console.print("\n[yellow]Test 1: Automatic EVALID selection (no pre-filtering)[/yellow]")
    
    with FIA("nfi_south.duckdb") as db:
        # Set state filter but no EVALID
        db.state_filter = [40]  # Oklahoma
        
        console.print(f"Before area() call: db.evalid = {db.evalid}")
        
        # Call area - should apply single EVALID
        results = area(db, totals=True)
        
        console.print(f"After area() call: db.evalid = {db.evalid}")
        
        if db.evalid and len(db.evalid) == 1:
            evalid = db.evalid[0]
            # Parse EVALID
            evalid_str = str(evalid)
            if len(evalid_str) >= 6:
                state = evalid_str[:2]
                year = evalid_str[2:4]
                eval_type = evalid_str[4:6]
                console.print(f"\n[green]✓ Using single EVALID: {evalid}[/green]")
                console.print(f"  State code: {state}")
                console.print(f"  Year: 20{year}")
                console.print(f"  Eval type: {eval_type} (00=VOL, 01=GRM, 03=CHNG)")
            else:
                console.print(f"\n[green]✓ Using single EVALID: {evalid}[/green]")
        else:
            console.print(f"\n[red]✗ Multiple EVALIDs used: {db.evalid}[/red]")
        
        console.print(f"\nResults:")
        console.print(f"  Forest area: {results['AREA'][0]:,.0f} acres")
        console.print(f"  Forest percentage: {results['AREA_PERC'][0]:.2f}%")
        console.print(f"  Number of plots: {results['N_PLOTS'][0]:,}")
    
    # Test with explicit clip_by_state
    console.print("\n[yellow]Test 2: With clip_by_state (should use single VOL EVALID)[/yellow]")
    
    with FIA("nfi_south.duckdb") as db:
        # Use clip_most_recent to explicitly get VOL evaluation
        db.clip_by_state(40).clip_most_recent("VOL")
        
        console.print(f"After clip_most_recent('VOL'): db.evalid = {db.evalid}")
        
        if db.evalid and len(db.evalid) == 1:
            console.print(f"[green]✓ Single EVALID set correctly[/green]")
        else:
            console.print(f"[yellow]⚠ Multiple EVALIDs: {db.evalid}[/yellow]")
        
        results = area(db, totals=True)
        
        console.print(f"\nResults:")
        console.print(f"  Forest area: {results['AREA'][0]:,.0f} acres")
        console.print(f"  Forest percentage: {results['AREA_PERC'][0]:.2f}%")
        console.print(f"  Number of plots: {results['N_PLOTS'][0]:,}")
    
    # Check what happens with each EVALID individually
    console.print("\n[yellow]Test 3: Checking each Oklahoma EVALID individually[/yellow]")
    
    with FIA("nfi_south.duckdb") as db:
        # Get all Oklahoma EVALIDs
        conn = db._reader._backend._connection
        oklahoma_evalids = conn.execute("""
            SELECT DISTINCT EVALID 
            FROM POP_PLOT_STRATUM_ASSGN ppsa
            JOIN PLOT p ON ppsa.PLT_CN = p.CN
            WHERE p.STATECD = 40
            ORDER BY EVALID
        """).fetchall()
        
        console.print(f"Found {len(oklahoma_evalids)} EVALIDs for Oklahoma")
        
        # Create table for results
        table = Table(title="Results by Individual EVALID", show_header=True)
        table.add_column("EVALID", style="cyan")
        table.add_column("Type", style="yellow")
        table.add_column("Plots", style="green", justify="right")
        table.add_column("Forest %", style="blue", justify="right")
        table.add_column("Forest Acres", style="magenta", justify="right")
        
        # Test first 5 EVALIDs
        for evalid_row in oklahoma_evalids[:5]:
            evalid = evalid_row[0]
            evalid_str = str(evalid)
            eval_type = evalid_str[4:6] if len(evalid_str) >= 6 else "??"
            
            db_test = FIA("nfi_south.duckdb")
            db_test.clip_by_evalid(evalid)
            
            try:
                results = area(db_test, totals=True)
                table.add_row(
                    str(evalid),
                    eval_type,
                    f"{results['N_PLOTS'][0]:,}",
                    f"{results['AREA_PERC'][0]:.2f}",
                    f"{results['AREA'][0]:,.0f}"
                )
            except Exception as e:
                table.add_row(str(evalid), eval_type, "Error", "-", str(e)[:30])
        
        console.print("\n")
        console.print(table)
        console.print("\n[dim]Note: Different EVALIDs may give different results due to[/dim]")
        console.print("[dim]different evaluation types and inventory periods.[/dim]")

if __name__ == "__main__":
    test_single_evalid()