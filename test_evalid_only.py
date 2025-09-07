#!/usr/bin/env python
"""
Test using EVALID filtering alone without separate state filtering.
Since EVALID format is SSYYTT, the state is already encoded.
"""

from pyfia import FIA, area
from rich.console import Console
from rich.table import Table

console = Console()

console.print("\n[bold cyan]Testing EVALID-Only Filtering (No Separate State Filter)[/bold cyan]")
console.print("=" * 60)

# Method 1: Traditional approach with state filter
console.print("\n[yellow]Method 1: clip_by_state (with state filter + EVALID)[/yellow]")

with FIA("nfi_south.duckdb") as db:
    db.clip_by_state(40, most_recent=True)  # Oklahoma
    
    console.print(f"State filter: {db.state_filter}")
    console.print(f"EVALID filter: {db.evalid}")
    
    results1 = area(db, totals=True)
    
    console.print(f"\nResults:")
    console.print(f"  Forest area: {results1['AREA'][0]:,.0f} acres")
    console.print(f"  Forest percentage: {results1['AREA_PERC'][0]:.2f}%")
    console.print(f"  Number of plots: {results1['N_PLOTS'][0]:,}")

# Method 2: Direct EVALID filtering (cleaner approach)
console.print("\n[yellow]Method 2: Direct EVALID filtering (no state filter needed)[/yellow]")

with FIA("nfi_south.duckdb") as db:
    # Oklahoma's most recent EXPALL EVALID is 402300
    # The '40' prefix already identifies Oklahoma
    db.clip_by_evalid(402300)
    
    console.print(f"State filter: {db.state_filter}")
    console.print(f"EVALID filter: {db.evalid}")
    
    results2 = area(db, totals=True)
    
    console.print(f"\nResults:")
    console.print(f"  Forest area: {results2['AREA'][0]:,.0f} acres")
    console.print(f"  Forest percentage: {results2['AREA_PERC'][0]:.2f}%")
    console.print(f"  Number of plots: {results2['N_PLOTS'][0]:,}")

# Verify both methods give identical results
console.print("\n[bold]Comparison:[/bold]")
if (results1['AREA'][0] == results2['AREA'][0] and 
    results1['AREA_PERC'][0] == results2['AREA_PERC'][0] and
    results1['N_PLOTS'][0] == results2['N_PLOTS'][0]):
    console.print("[green]✓ Both methods produce identical results![/green]")
else:
    console.print("[red]✗ Results differ between methods[/red]")

# Show how to find the right EVALID for any state
console.print("\n[yellow]Finding the correct EVALID for any state:[/yellow]")

def find_state_evalid(db_path: str, state_code: int, eval_type: str = "ALL"):
    """Find the most recent EVALID for a state and evaluation type."""
    with FIA(db_path) as db:
        conn = db._reader._backend._connection
        
        # Query for most recent EVALID with desired eval_type
        query = f"""
            SELECT 
                pe.EVALID,
                pe.END_INVYR,
                COUNT(DISTINCT ppsa.PLT_CN) as plot_count
            FROM POP_EVAL pe
            JOIN POP_EVAL_TYP pet ON pe.CN = pet.EVAL_CN
            JOIN POP_PLOT_STRATUM_ASSGN ppsa ON pe.EVALID = ppsa.EVALID
            WHERE pe.STATECD = {state_code}
            AND pet.EVAL_TYP = 'EXP{eval_type}'
            GROUP BY pe.EVALID, pe.END_INVYR
            ORDER BY pe.END_INVYR DESC, pe.EVALID DESC
            LIMIT 1
        """
        
        result = conn.execute(query).fetchone()
        if result:
            evalid, year, plots = result
            return evalid, year, plots
        return None, None, None

# Find EVALIDs for different states
states = {
    40: "Oklahoma",
    48: "Texas", 
    1: "Alabama"
}

table = Table(title="Most Recent EXPALL EVALIDs by State")
table.add_column("State", style="cyan")
table.add_column("EVALID", style="green")
table.add_column("Year", style="yellow")
table.add_column("Plots", style="magenta", justify="right")

for state_code, state_name in states.items():
    evalid, year, plots = find_state_evalid("nfi_south.duckdb", state_code, "ALL")
    if evalid:
        table.add_row(state_name, str(evalid), str(year), f"{plots:,}")

console.print("\n")
console.print(table)

console.print("\n[bold green]Recommendation:[/bold green]")
console.print("Use direct EVALID filtering (e.g., db.clip_by_evalid(402300))")
console.print("This is simpler and ensures exactly one EVALID is used.")
console.print("The state code is already encoded in the EVALID!")