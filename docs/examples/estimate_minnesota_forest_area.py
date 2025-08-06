#!/usr/bin/env python3
"""
Estimate forest area in Minnesota using FIA data.

This script demonstrates how to use pyfia's area() function to calculate
various forest area statistics for Minnesota, including area by forest type,
ownership, and land use categories.
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


def estimate_minnesota_forest_area(db_path: str = "fia.duckdb"):
    """Estimate forest area in Minnesota using FIA data."""
    
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        
        # Load FIA database with Minnesota filter
        task = progress.add_task("[bold blue]Loading Minnesota FIA data...[/bold blue]", total=None)
        db = FIA(db_path)
        # Use clip_by_state for efficient state-level filtering (Minnesota = 27)
        db.clip_by_state(state=27, most_recent=True)
        progress.update(task, completed=True)
        
        # 1. Total forest area
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
        
        # 6. Forest area by stand age classes
        task = progress.add_task("[green]Analyzing forest age distribution...[/green]", total=None)
        by_stand_age = area(db, grp_by=["STDAGE"], totals=True)
        progress.update(task, completed=True)
        
        # 7. Reserved forest area
        task = progress.add_task("[green]Calculating reserved forest area...[/green]", total=None)
        reserved_forest = area(db, area_domain="RESERVCD == 1", totals=True)
        progress.update(task, completed=True)
    
    # Display all results
    console.print("\n[bold blue]Minnesota Forest Area Analysis[/bold blue]\n")
    
    # 1. Total forest area
    console.print("[bold green]1. Total Forest Area[/bold green]")
    
    if not total_forest.is_empty():
        row = total_forest.row(0, named=True)
        console.print(f"   Forest area: {row['AREA']:,.0f} acres ({row['AREA_PERC']:.1f}% of total land)")
        console.print(f"   Standard error: ±{row['AREA_PERC_SE']:.2f}%")
        console.print(f"   Number of plots: {row['N_PLOTS']:,}")
    
    # 2. Area by land type categories
    console.print("\n[bold green]2. Area by Land Type Categories[/bold green]")
    
    table1 = Table(title="Minnesota Land Type Distribution")
    table1.add_column("Land Type", style="cyan")
    table1.add_column("Acres", justify="right", style="green")
    table1.add_column("Percentage", justify="right")
    table1.add_column("Std Error (%)", justify="right")
    
    for row in by_land_type.iter_rows(named=True):
        table1.add_row(
            row["LAND_TYPE"],
            f"{row['AREA']:,.0f}",
            f"{row['AREA_PERC']:.1f}%",
            f"±{row['AREA_PERC_SE']:.2f}"
        )
    
    console.print(table1)
    
    # 3. Timberland area
    console.print("\n[bold green]3. Timberland Area (Productive, Unreserved Forest)[/bold green]")
    
    if not timberland.is_empty():
        row = timberland.row(0, named=True)
        console.print(f"   Timberland area: {row['AREA']:,.0f} acres ({row['AREA_PERC']:.1f}% of total)")
        console.print(f"   Standard error: ±{row['AREA_PERC_SE']:.2f}%")
    
    # 4. Forest area by ownership
    console.print("\n[bold green]4. Forest Area by Ownership[/bold green]")
    
    table2 = Table(title="Minnesota Forest Ownership")
    table2.add_column("Ownership", style="cyan")
    table2.add_column("Forest Acres", justify="right", style="green")
    table2.add_column("Percentage", justify="right")
    table2.add_column("Std Error (%)", justify="right")
    
    # Sort by area descending
    by_ownership_sorted = by_ownership.sort("AREA", descending=True)
    
    for row in by_ownership_sorted.iter_rows(named=True):
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
    
    table3 = Table(title="Top Forest Type Groups in Minnesota")
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
    
    # 6. Forest area by stand age classes
    console.print("\n[bold green]6. Forest Area by Stand Age Classes[/bold green]")
    
    # Filter out null ages and create age classes
    by_stand_age_filtered = by_stand_age.filter(pl.col("STDAGE").is_not_null())
    
    # Create age classes
    by_stand_age_filtered = by_stand_age_filtered.with_columns(
        pl.when(pl.col("STDAGE") <= 20).then(pl.lit("0-20"))
        .when(pl.col("STDAGE") <= 40).then(pl.lit("21-40"))
        .when(pl.col("STDAGE") <= 60).then(pl.lit("41-60"))
        .when(pl.col("STDAGE") <= 80).then(pl.lit("61-80"))
        .when(pl.col("STDAGE") <= 100).then(pl.lit("81-100"))
        .otherwise(pl.lit("100+"))
        .alias("AGE_CLASS")
    )
    
    # Group by age class
    age_class_summary = (
        by_stand_age_filtered
        .group_by("AGE_CLASS")
        .agg([
            pl.sum("AREA").alias("AREA"),
            pl.sum("AREA_PERC").alias("AREA_PERC")
        ])
    )
    
    table4 = Table(title="Forest Area by Stand Age")
    table4.add_column("Age Class (years)", style="cyan")
    table4.add_column("Forest Acres", justify="right", style="green")
    table4.add_column("Percentage", justify="right")
    
    # Define custom sort order for age classes
    age_order = ["0-20", "21-40", "41-60", "61-80", "81-100", "100+"]
    
    for age_class in age_order:
        rows = age_class_summary.filter(pl.col("AGE_CLASS") == age_class)
        if not rows.is_empty():
            row = rows.row(0, named=True)
            table4.add_row(
                age_class,
                f"{row['AREA']:,.0f}",
                f"{row['AREA_PERC']:.1f}%"
            )
    
    console.print(table4)
    
    # 7. Reserved forest area
    console.print("\n[bold green]7. Reserved Forest Area[/bold green]")
    
    if not reserved_forest.is_empty():
        row = reserved_forest.row(0, named=True)
        console.print(f"   Reserved forest area: {row['AREA']:,.0f} acres ({row['AREA_PERC']:.1f}% of total)")
        console.print(f"   Standard error: ±{row['AREA_PERC_SE']:.2f}%")
        console.print(f"   (Areas reserved from timber production)")
    
    # Summary statistics
    console.print("\n[bold green]8. Summary Statistics[/bold green]")
    
    if not total_forest.is_empty() and not timberland.is_empty():
        total_forest_acres = total_forest.row(0, named=True)['AREA']
        timberland_acres = timberland.row(0, named=True)['AREA']
        reserved_acres = reserved_forest.row(0, named=True)['AREA'] if not reserved_forest.is_empty() else 0
        
        console.print(f"\n[yellow]Forest Land Categories:[/yellow]")
        console.print(f"   Total forest land: {total_forest_acres:,.0f} acres")
        console.print(f"   Timberland (productive, unreserved): {timberland_acres:,.0f} acres ({timberland_acres/total_forest_acres*100:.1f}%)")
        console.print(f"   Reserved forest: {reserved_acres:,.0f} acres ({reserved_acres/total_forest_acres*100:.1f}%)")
        console.print(f"   Other forest: {total_forest_acres - timberland_acres - reserved_acres:,.0f} acres")
    
    console.print("\n[bold yellow]Analysis complete![/bold yellow]")


if __name__ == "__main__":
    import sys
    
    # Check if database path is provided as command line argument
    db_path = sys.argv[1] if len(sys.argv) > 1 else "fia.duckdb"
    
    try:
        estimate_minnesota_forest_area(db_path)
    except Exception as e:
        console.print(f"\n[bold red]Error:[/bold red] {str(e)}")
        console.print("\n[yellow]Usage:[/yellow] python estimate_minnesota_forest_area.py [path_to_fia_db]")
        console.print("\nIf no path is provided, the script will use 'fia.duckdb' in the current directory.")
        sys.exit(1)