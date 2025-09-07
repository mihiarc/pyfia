#!/usr/bin/env python
"""
Check what EVALID types are available for Oklahoma.
"""

from pyfia import FIA
from rich.console import Console
from rich.table import Table

console = Console()

with FIA("nfi_south.duckdb") as db:
    conn = db._reader._backend._connection
    
    # Query to check EVALID types
    query = """
        SELECT 
            ppsa.EVALID,
            COUNT(DISTINCT ppsa.PLT_CN) as plot_count,
            MIN(p.INVYR) as min_year,
            MAX(p.INVYR) as max_year,
            CAST(SUBSTR(CAST(ppsa.EVALID AS VARCHAR), 5, 2) AS INTEGER) as eval_type_code
        FROM POP_PLOT_STRATUM_ASSGN ppsa
        JOIN PLOT p ON ppsa.PLT_CN = p.CN
        WHERE p.STATECD = 40
        GROUP BY ppsa.EVALID
        ORDER BY ppsa.EVALID DESC
        LIMIT 20
    """
    
    results = conn.execute(query).fetchall()
    
    console.print("\n[bold cyan]Most Recent Oklahoma EVALIDs[/bold cyan]")
    
    table = Table(show_header=True)
    table.add_column("EVALID", style="cyan")
    table.add_column("Year", style="green")
    table.add_column("Type", style="yellow")
    table.add_column("Type Name", style="magenta")
    table.add_column("Plots", style="blue", justify="right")
    table.add_column("Inv Years", style="white")
    
    # Type mapping
    type_names = {
        0: "VOL (Volume)",
        1: "GRM (Growth/Removal/Mortality)",
        3: "CHNG (Change)",
        7: "DWM (Down Woody Materials)",
        9: "REGEN (Regeneration)",
        20: "Unknown (20)",
        21: "Unknown (21)",
        23: "Unknown (23)",
        29: "Unknown (29)"
    }
    
    for row in results:
        evalid, plots, min_year, max_year, type_code = row
        evalid_str = str(evalid)
        year = evalid_str[2:4] if len(evalid_str) >= 6 else "??"
        
        table.add_row(
            str(evalid),
            f"20{year}",
            str(type_code),
            type_names.get(type_code, f"Type {type_code}"),
            f"{plots:,}",
            f"{min_year}-{max_year}"
        )
    
    console.print(table)
    
    # Check if there's a VOL type for recent years
    console.print("\n[yellow]Checking for most recent VOL evaluation:[/yellow]")
    
    vol_query = """
        SELECT 
            ppsa.EVALID,
            COUNT(DISTINCT ppsa.PLT_CN) as plot_count,
            MAX(p.INVYR) as max_year
        FROM POP_PLOT_STRATUM_ASSGN ppsa
        JOIN PLOT p ON ppsa.PLT_CN = p.CN
        WHERE p.STATECD = 40
        AND CAST(SUBSTR(CAST(ppsa.EVALID AS VARCHAR), 5, 2) AS INTEGER) = 0
        GROUP BY ppsa.EVALID
        ORDER BY ppsa.EVALID DESC
        LIMIT 1
    """
    
    vol_result = conn.execute(vol_query).fetchone()
    if vol_result:
        evalid, plots, max_year = vol_result
        console.print(f"Most recent VOL EVALID: {evalid} ({plots:,} plots, year {max_year})")
    else:
        console.print("No VOL evaluations found!")
    
    # Check what POP_EVAL_TYP says
    console.print("\n[yellow]Checking POP_EVAL_TYP table:[/yellow]")
    
    try:
        eval_typ_query = """
            SELECT DISTINCT
                pet.EVAL_TYP,
                COUNT(DISTINCT pe.EVALID) as evalid_count
            FROM POP_EVAL pe
            JOIN POP_EVAL_TYP pet ON pe.CN = pet.EVAL_CN
            WHERE pe.STATECD = 40
            GROUP BY pet.EVAL_TYP
        """
        
        eval_types = conn.execute(eval_typ_query).fetchall()
        
        console.print("Evaluation types in POP_EVAL_TYP:")
        for eval_type, count in eval_types:
            console.print(f"  {eval_type}: {count} EVALIDs")
    except Exception as e:
        console.print(f"Could not query POP_EVAL_TYP: {e}")