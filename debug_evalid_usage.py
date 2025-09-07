#!/usr/bin/env python
"""
Debug what EVALIDs are being used in oklahoma_area_api_direct.py
"""

from pyfia import FIA, area
from rich.console import Console

console = Console()

console.print("\n[bold cyan]Debugging EVALID Usage in area() Function[/bold cyan]")
console.print("=" * 60)

# Replicate what oklahoma_area_api_direct.py does
with FIA("nfi_south.duckdb") as db:
    # This is what the script does
    db.clip_by_state(40, most_recent=True)
    
    console.print(f"\nAfter clip_by_state(40, most_recent=True):")
    console.print(f"db.evalid = {db.evalid}")
    console.print(f"Number of EVALIDs: {len(db.evalid) if db.evalid else 0}")
    
    if db.evalid:
        # Check what types these are
        conn = db._reader._backend._connection
        for evalid in db.evalid[:5]:  # Show first 5
            evalid_str = str(evalid)
            state = evalid_str[:2] if len(evalid_str) >= 6 else "??"
            year = evalid_str[2:4] if len(evalid_str) >= 6 else "??"
            type_code = evalid_str[4:6] if len(evalid_str) >= 6 else "??"
            
            # Get EVAL_TYP
            query = f"""
                SELECT DISTINCT pet.EVAL_TYP
                FROM POP_EVAL pe
                JOIN POP_EVAL_TYP pet ON pe.CN = pet.EVAL_CN
                WHERE pe.EVALID = {evalid}
            """
            eval_types = conn.execute(query).fetchall()
            eval_types_str = ", ".join([et[0] for et in eval_types])
            
            console.print(f"  EVALID {evalid}: State={state}, Year=20{year}, Type={type_code}, EVAL_TYP={eval_types_str}")
    
    # Now call area
    console.print("\n[yellow]Calling area() with these EVALIDs:[/yellow]")
    results = area(db, totals=True)
    
    console.print(f"\nResults:")
    console.print(f"  Forest area: {results['AREA'][0]:,.0f} acres")
    console.print(f"  Forest percentage: {results['AREA_PERC'][0]:.2f}%")
    console.print(f"  Number of plots: {results['N_PLOTS'][0]:,}")
    
# Now test with single EVALID
console.print("\n" + "=" * 60)
console.print("\n[yellow]Testing with single EVALID (402300 - EXPALL):[/yellow]")

with FIA("nfi_south.duckdb") as db:
    db.clip_by_evalid(402300)  # Just one EVALID
    
    console.print(f"db.evalid = {db.evalid}")
    
    results = area(db, totals=True)
    
    console.print(f"\nResults:")
    console.print(f"  Forest area: {results['AREA'][0]:,.0f} acres")
    console.print(f"  Forest percentage: {results['AREA_PERC'][0]:.2f}%")
    console.print(f"  Number of plots: {results['N_PLOTS'][0]:,}")

console.print("\n[bold]Analysis:[/bold]")
console.print("The difference is because clip_by_state() returns multiple EVALIDs")
console.print("for different evaluation types, while we should use only ONE.")