#!/usr/bin/env python
"""
Inspect the actual EVAL_TYP values in the database.
"""

from pyfia import FIA
from rich.console import Console
from rich.table import Table

console = Console()

console.print("\n[bold cyan]Inspecting EVAL_TYP Values in Database[/bold cyan]")
console.print("=" * 60)

with FIA("nfi_south.duckdb") as db:
    conn = db._reader._backend._connection
    
    # First, check what columns POP_EVAL_TYP has
    console.print("\n[yellow]Columns in POP_EVAL_TYP table:[/yellow]")
    schema_query = """
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = 'POP_EVAL_TYP'
        ORDER BY ordinal_position
    """
    columns = conn.execute(schema_query).fetchall()
    for col in columns:
        console.print(f"  - {col[0]}")
    
    # Now check actual EVAL_TYP values for Oklahoma
    console.print("\n[yellow]Sample EVAL_TYP values for Oklahoma EVALIDs:[/yellow]")
    
    sample_query = """
        SELECT DISTINCT
            pe.EVALID,
            pe.STATECD,
            pe.END_INVYR,
            pet.EVAL_TYP,
            pet.EVAL_CN,
            COUNT(*) as count
        FROM POP_EVAL pe
        JOIN POP_EVAL_TYP pet ON pe.CN = pet.EVAL_CN
        WHERE pe.STATECD = 40
        AND pe.END_INVYR >= 2020
        GROUP BY pe.EVALID, pe.STATECD, pe.END_INVYR, pet.EVAL_TYP, pet.EVAL_CN
        ORDER BY pe.EVALID DESC, pet.EVAL_TYP
        LIMIT 30
    """
    
    results = conn.execute(sample_query).fetchall()
    
    table = Table(title="EVAL_TYP Values for Recent Oklahoma EVALIDs")
    table.add_column("EVALID", style="cyan")
    table.add_column("State", style="green")
    table.add_column("End Year", style="yellow")
    table.add_column("EVAL_TYP", style="magenta")
    table.add_column("Count", style="blue")
    
    for row in results:
        evalid, state, year, eval_typ, eval_cn, count = row
        table.add_row(
            str(evalid),
            str(state),
            str(year),
            eval_typ if eval_typ else "NULL",
            str(count)
        )
    
    console.print(table)
    
    # Check unique EVAL_TYP values across all Oklahoma
    console.print("\n[yellow]All unique EVAL_TYP values for Oklahoma:[/yellow]")
    
    unique_query = """
        SELECT DISTINCT pet.EVAL_TYP
        FROM POP_EVAL pe
        JOIN POP_EVAL_TYP pet ON pe.CN = pet.EVAL_CN
        WHERE pe.STATECD = 40
        AND pet.EVAL_TYP IS NOT NULL
        ORDER BY pet.EVAL_TYP
    """
    
    unique_types = conn.execute(unique_query).fetchall()
    
    console.print("Unique EVAL_TYP values found:")
    for typ in unique_types:
        console.print(f"  - '{typ[0]}'")
    
    # Now let's check what EVALID 402300 (VOL) actually has
    console.print("\n[yellow]Checking EVAL_TYP for EVALID 402300 (should be VOL):[/yellow]")
    
    check_402300 = """
        SELECT 
            pe.EVALID,
            pet.EVAL_TYP,
            COUNT(*) as count
        FROM POP_EVAL pe
        JOIN POP_EVAL_TYP pet ON pe.CN = pet.EVAL_CN
        WHERE pe.EVALID = 402300
        GROUP BY pe.EVALID, pet.EVAL_TYP
    """
    
    results_402300 = conn.execute(check_402300).fetchall()
    
    if results_402300:
        for evalid, eval_typ, count in results_402300:
            console.print(f"EVALID {evalid}: EVAL_TYP = '{eval_typ}' (count: {count})")
    else:
        console.print("No results found for EVALID 402300 in POP_EVAL_TYP")
    
    # Check if we can match on the prefix
    console.print("\n[yellow]Checking if EVAL_TYP values start with 'EXP':[/yellow]")
    
    prefix_check = """
        SELECT 
            pet.EVAL_TYP,
            COUNT(DISTINCT pe.EVALID) as evalid_count,
            MIN(pe.EVALID) as sample_evalid
        FROM POP_EVAL pe
        JOIN POP_EVAL_TYP pet ON pe.CN = pet.EVAL_CN
        WHERE pe.STATECD = 40
        AND pet.EVAL_TYP LIKE 'EXP%'
        GROUP BY pet.EVAL_TYP
        ORDER BY pet.EVAL_TYP
    """
    
    prefix_results = conn.execute(prefix_check).fetchall()
    
    if prefix_results:
        table2 = Table(title="EVAL_TYP Values Starting with 'EXP'")
        table2.add_column("EVAL_TYP", style="cyan")
        table2.add_column("EVALID Count", style="green")
        table2.add_column("Sample EVALID", style="yellow")
        
        for eval_typ, count, sample in prefix_results:
            table2.add_row(eval_typ, str(count), str(sample))
        
        console.print(table2)
    else:
        console.print("No EVAL_TYP values starting with 'EXP' found")