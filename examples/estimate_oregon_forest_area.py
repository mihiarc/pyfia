#!/usr/bin/env python3
"""
Estimate the area of forestland in the state of Oregon using FIA data.

This script demonstrates how to use pyfia's area() function to calculate
forest area statistics for Oregon.
"""

from pathlib import Path
import polars as pl
from rich.console import Console
from rich.table import Table
from rich import print as rprint
from rich.progress import Progress, SpinnerColumn, TextColumn

from pyfia import FIA
from pyfia.estimation.area import area
from pyfia.filters.grouping import get_forest_type_group, get_ownership_group_name

console = Console()


def estimate_oregon_forest_area(db_path: str = "fia.duckdb"):
    """Estimate forest area in Oregon using FIA data."""
    
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        
        # Load FIA database with Oregon filter
        task = progress.add_task("[bold blue]Loading Oregon FIA data...[/bold blue]", total=None)
        db = FIA(db_path)
        # Use clip_by_state for efficient state-level filtering
        db.clip_by_state(state=41, most_recent=True)
        progress.update(task, completed=True)
        
        # 1. Total forest area in Oregon
        task = progress.add_task("[green]Calculating total forest area...[/green]", total=None)
        total_forest = area(db, totals=True)
        progress.update(task, completed=True)
        
        # 2. Area by land type categories
        task = progress.add_task("[green]Analyzing land type distribution...[/green]", total=None)
        by_land_type = area(db, by_land_type=True, totals=True)
        progress.update(task, completed=True)
        
        # 3. Timberland area
        task = progress.add_task("[green]Calculating timberland area...[/green]", total=None)
        timberland = area(db, land_type="timber", totals=True)
        progress.update(task, completed=True)
        
        # 4. Forest area by ownership
        task = progress.add_task("[green]Analyzing forest ownership...[/green]", total=None)
        by_ownership = area(db, grp_by=["OWNGRPCD"], totals=True)
        progress.update(task, completed=True)
        
        # 5. Forest area by forest type groups
        task = progress.add_task("[green]Analyzing forest type distribution...[/green]", total=None)
        # Use FORTYPCD which exists in COND table, then we'll group it ourselves
        by_forest_type = area(db, grp_by=["FORTYPCD"], totals=True)
        progress.update(task, completed=True)
        
    
    # Display all results
    console.print("\n[bold green]1. Total Forest Area in Oregon[/bold green]")
    
    if not total_forest.is_empty():
        row = total_forest.row(0, named=True)
        console.print(f"   Forest area: {row['AREA']:,.0f} acres ({row['AREA_PERC']:.1f}% of total land)")
        console.print(f"   Standard error: ±{row['AREA_PERC_SE']:.2f}%")
        console.print(f"   Number of plots: {row['N_PLOTS']:,}")
    
    # 2. Area by land type categories
    console.print("\n[bold green]2. Area by Land Type Categories[/bold green]")
    
    # Create a table for land type results
    table = Table(title="Oregon Land Type Distribution")
    table.add_column("Land Type", style="cyan")
    table.add_column("Acres", justify="right", style="green")
    table.add_column("Percentage", justify="right")
    table.add_column("Std Error (%)", justify="right")
    
    for row in by_land_type.iter_rows(named=True):
        table.add_row(
            row["LAND_TYPE"],
            f"{row['AREA']:,.0f}",
            f"{row['AREA_PERC']:.1f}%",
            f"±{row['AREA_PERC_SE']:.2f}"
        )
    
    console.print(table)
    
    # 3. Timberland area (productive, unreserved forest)
    console.print("\n[bold green]3. Timberland Area[/bold green]")
    
    if not timberland.is_empty():
        row = timberland.row(0, named=True)
        console.print(f"   Timberland area: {row['AREA']:,.0f} acres ({row['AREA_PERC']:.1f}% of total)")
        console.print(f"   Standard error: ±{row['AREA_PERC_SE']:.2f}%")
    
    # 4. Forest area by ownership group
    console.print("\n[bold green]4. Forest Area by Ownership[/bold green]")
    
    table2 = Table(title="Oregon Forest Ownership")
    table2.add_column("Ownership", style="cyan")
    table2.add_column("Forest Acres", justify="right", style="green")
    table2.add_column("Percentage", justify="right")
    table2.add_column("Std Error (%)", justify="right")
    
    for row in by_ownership.iter_rows(named=True):
        owner_name = get_ownership_group_name(row["OWNGRPCD"])
        table2.add_row(
            owner_name,
            f"{row['AREA']:,.0f}",
            f"{row['AREA_PERC']:.1f}%",
            f"±{row['AREA_PERC_SE']:.2f}"
        )
    
    console.print(table2)
    
    # 5. Forest area by major forest type groups
    console.print("\n[bold green]5. Top Forest Type Groups[/bold green]")
    
    # Add forest type group column using the imported function
    by_forest_type = by_forest_type.with_columns(
        pl.col("FORTYPCD").map_elements(get_forest_type_group, return_dtype=pl.Utf8).alias("FOREST_TYPE_GROUP")
    )
    
    # Group by forest type group and sum areas
    grouped_types = (
        by_forest_type
        .group_by("FOREST_TYPE_GROUP")
        .agg([
            pl.sum("AREA").alias("AREA"),
            pl.sum("AREA_PERC").alias("AREA_PERC")
        ])
        .sort("AREA", descending=True)
        .head(10)  # Show top 10 groups
    )
    
    table3 = Table(title="Top Forest Type Groups in Oregon")
    table3.add_column("Forest Type Group", style="cyan")
    table3.add_column("Forest Acres", justify="right", style="green")
    table3.add_column("Percentage", justify="right")
    
    for row in grouped_types.iter_rows(named=True):
        table3.add_row(
            row["FOREST_TYPE_GROUP"],
            f"{row['AREA']:,.0f}",
            f"{row['AREA_PERC']:.1f}%"
        )
    
    console.print(table3)
    
    console.print("\n[bold yellow]Analysis complete![/bold yellow]")


if __name__ == "__main__":
    import sys
    
    # Check if database path is provided as command line argument
    db_path = sys.argv[1] if len(sys.argv) > 1 else "fia.duckdb"
    
    try:
        estimate_oregon_forest_area(db_path)
    except Exception as e:
        console.print(f"\n[bold red]Error:[/bold red] {str(e)}")
        console.print("\n[yellow]Usage:[/yellow] python estimate_oregon_forest_area.py [path_to_fia_db]")
        console.print("\nIf no path is provided, the script will use 'fia.duckdb' in the current directory.")
        sys.exit(1)