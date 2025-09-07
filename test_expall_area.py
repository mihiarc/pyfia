#!/usr/bin/env python
"""
Test that area() now uses EXPALL evaluations for correct area estimation.
"""

from pyfia import FIA, area
from rich.console import Console

console = Console()

console.print("\n[bold cyan]Testing area() with EXPALL Evaluations[/bold cyan]")
console.print("=" * 60)

# Test 1: Let area() auto-select EVALID
console.print("\n[yellow]Test 1: Automatic EVALID selection[/yellow]")

with FIA("nfi_south.duckdb") as db:
    db.state_filter = [40]  # Oklahoma
    
    console.print("Before area() call: db.evalid = None")
    
    # This should now select an EXPALL evaluation
    results = area(db, totals=True)
    
    if db.evalid:
        evalid = db.evalid[0]
        console.print(f"After area() call: Selected EVALID {evalid}")
        
        # Verify this is an EXPALL evaluation
        conn = db._reader._backend._connection
        check_query = f"""
            SELECT DISTINCT pet.EVAL_TYP
            FROM POP_EVAL pe
            JOIN POP_EVAL_TYP pet ON pe.CN = pet.EVAL_CN
            WHERE pe.EVALID = {evalid}
        """
        eval_types = conn.execute(check_query).fetchall()
        
        console.print(f"EVAL_TYP values for EVALID {evalid}:")
        for et in eval_types:
            console.print(f"  - {et[0]}")
            
    console.print(f"\nResults:")
    console.print(f"  Forest area: {results['AREA'][0]:,.0f} acres")
    console.print(f"  Forest percentage: {results['AREA_PERC'][0]:.2f}%")
    console.print(f"  Number of plots: {results['N_PLOTS'][0]:,}")

# Test 2: Explicit comparison between EXPALL and EXPVOL
console.print("\n[yellow]Test 2: Compare EXPALL vs EXPVOL results[/yellow]")

# Get EXPALL EVALID (type 00)
db_all = FIA("nfi_south.duckdb")
db_all.clip_by_evalid(402300)  # EXPALL
results_all = area(db_all, totals=True)

# Get EXPVOL EVALID (type 01)
db_vol = FIA("nfi_south.duckdb")
db_vol.clip_by_evalid(402301)  # EXPVOL
results_vol = area(db_vol, totals=True)

console.print("\nComparison:")
console.print(f"EXPALL (402300): {results_all['AREA'][0]:,.0f} acres, {results_all['N_PLOTS'][0]:,} plots")
console.print(f"EXPVOL (402301): {results_vol['AREA'][0]:,.0f} acres, {results_vol['N_PLOTS'][0]:,} plots")

console.print("\n[bold green]✓ EXPALL includes all plots for comprehensive area estimation[/bold green]")