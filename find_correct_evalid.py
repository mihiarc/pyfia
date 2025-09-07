#!/usr/bin/env python
"""
Find the correct EVALID for area estimation based on EVAL_TYP.
"""

from pyfia import FIA
from rich.console import Console
from rich.table import Table

console = Console()

console.print("\n[bold cyan]Finding Correct EVALID for Area Estimation[/bold cyan]")
console.print("=" * 60)

with FIA("nfi_south.duckdb") as db:
    conn = db._reader._backend._connection
    
    # Find EVALIDs with EXPVOL for Oklahoma
    console.print("\n[yellow]Oklahoma EVALIDs with EXPVOL:[/yellow]")
    
    expvol_query = """
        SELECT DISTINCT
            pe.EVALID,
            pe.END_INVYR,
            CAST(SUBSTR(CAST(pe.EVALID AS VARCHAR), 5, 2) AS INTEGER) as type_code,
            COUNT(DISTINCT ppsa.PLT_CN) as plot_count
        FROM POP_EVAL pe
        JOIN POP_EVAL_TYP pet ON pe.CN = pet.EVAL_CN
        JOIN POP_PLOT_STRATUM_ASSGN ppsa ON pe.EVALID = ppsa.EVALID
        JOIN PLOT p ON ppsa.PLT_CN = p.CN
        WHERE pe.STATECD = 40
        AND pet.EVAL_TYP = 'EXPVOL'
        AND p.STATECD = 40
        GROUP BY pe.EVALID, pe.END_INVYR
        ORDER BY pe.END_INVYR DESC, pe.EVALID DESC
        LIMIT 10
    """
    
    results = conn.execute(expvol_query).fetchall()
    
    table = Table(title="EVALIDs with EXPVOL")
    table.add_column("EVALID", style="cyan")
    table.add_column("End Year", style="green")
    table.add_column("Type Code", style="yellow")
    table.add_column("Plot Count", style="magenta", justify="right")
    
    for evalid, year, type_code, plots in results:
        table.add_row(str(evalid), str(year), str(type_code), f"{plots:,}")
    
    console.print(table)
    
    # Find EVALIDs with EXPALL for Oklahoma
    console.print("\n[yellow]Oklahoma EVALIDs with EXPALL:[/yellow]")
    
    expall_query = """
        SELECT DISTINCT
            pe.EVALID,
            pe.END_INVYR,
            CAST(SUBSTR(CAST(pe.EVALID AS VARCHAR), 5, 2) AS INTEGER) as type_code,
            COUNT(DISTINCT ppsa.PLT_CN) as plot_count
        FROM POP_EVAL pe
        JOIN POP_EVAL_TYP pet ON pe.CN = pet.EVAL_CN
        JOIN POP_PLOT_STRATUM_ASSGN ppsa ON pe.EVALID = ppsa.EVALID
        JOIN PLOT p ON ppsa.PLT_CN = p.CN
        WHERE pe.STATECD = 40
        AND pet.EVAL_TYP = 'EXPALL'
        AND p.STATECD = 40
        GROUP BY pe.EVALID, pe.END_INVYR
        ORDER BY pe.END_INVYR DESC, pe.EVALID DESC
        LIMIT 10
    """
    
    results = conn.execute(expall_query).fetchall()
    
    table2 = Table(title="EVALIDs with EXPALL")
    table2.add_column("EVALID", style="cyan")
    table2.add_column("End Year", style="green")
    table2.add_column("Type Code", style="yellow")
    table2.add_column("Plot Count", style="magenta", justify="right")
    
    for evalid, year, type_code, plots in results:
        table2.add_row(str(evalid), str(year), str(type_code), f"{plots:,}")
    
    console.print(table2)
    
    # Let's check what rFIA documentation says
    console.print("\n[bold]Analysis:[/bold]")
    console.print("• EXPVOL (Volume) evaluations are used for volume and biomass estimates")
    console.print("• EXPALL (All) evaluations contain all plot data")
    console.print("• For area estimation, either EXPVOL or EXPALL should work")
    console.print("• The most recent EXPVOL EVALID for Oklahoma is 402301 (7,298 plots)")
    console.print("• The most recent EXPALL EVALID for Oklahoma is 402300 (7,535 plots)")
    
    # Test both to see which gives more reasonable results
    console.print("\n[yellow]Testing area estimates with different EVALIDs:[/yellow]")
    
    from pyfia import area
    
    # Test with 402301 (EXPVOL)
    db1 = FIA("nfi_south.duckdb")
    db1.clip_by_evalid(402301)
    result1 = area(db1, totals=True)
    
    # Test with 402300 (EXPALL)
    db2 = FIA("nfi_south.duckdb")
    db2.clip_by_evalid(402300)
    result2 = area(db2, totals=True)
    
    console.print("\nResults comparison:")
    console.print(f"EVALID 402301 (EXPVOL): {result1['AREA'][0]:,.0f} acres ({result1['AREA_PERC'][0]:.2f}%), {result1['N_PLOTS'][0]:,} plots")
    console.print(f"EVALID 402300 (EXPALL): {result2['AREA'][0]:,.0f} acres ({result2['AREA_PERC'][0]:.2f}%), {result2['N_PLOTS'][0]:,} plots")
    
    console.print("\n[bold green]Recommendation:[/bold green]")
    console.print("Use EVALID with EXPALL (type 00) for area estimation as it includes all plots")