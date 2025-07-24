#!/usr/bin/env python3
"""
Estimate the net merchantable volume of timber in California by diameter class.

This script demonstrates how to use pyfia's volume() function to calculate
merchantable timber volume statistics for California, grouped by diameter classes.
"""

from pathlib import Path
import polars as pl
from rich.console import Console
from rich.table import Table
from rich import print as rprint
from rich.progress import Progress, SpinnerColumn, TextColumn

from pyfia import FIA
from pyfia.estimation.volume import volume
from pyfia.filters.grouping import get_ownership_group_name, get_forest_type_group

console = Console()


def estimate_california_merchantable_volume(db_path: str = "fia.duckdb"):
    """Estimate merchantable timber volume in California by diameter class."""
    
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        
        # Load FIA database with California filter
        task = progress.add_task("[bold blue]Loading California FIA data...[/bold blue]", total=None)
        db = FIA(db_path)
        # Use clip_by_state for efficient state-level filtering
        db.clip_by_state(state=6, most_recent=True)
        progress.update(task, completed=True)
        
        # 1. Total merchantable volume (growing stock on timberland)
        task = progress.add_task("[green]Calculating total merchantable volume...[/green]", total=None)
        total_volume = volume(
            db, 
            tree_type="gs",  # Growing stock only (merchantable)
            land_type="timber",  # Timberland only
            vol_type="net",  # Net volume
            totals=True
        )
        progress.update(task, completed=True)
        
        # 2. Merchantable volume by diameter class
        task = progress.add_task("[green]Analyzing volume by diameter class...[/green]", total=None)
        volume_by_diameter = volume(
            db,
            tree_type="gs",
            land_type="timber",
            vol_type="net",
            by_size_class=True,  # Group by diameter size class
            totals=True
        )
        progress.update(task, completed=True)
        
        # 3. Sawlog volume by diameter class (≥9" diameter)
        task = progress.add_task("[green]Calculating sawlog volume by diameter class...[/green]", total=None)
        sawlog_volume = volume(
            db,
            tree_type="gs",
            land_type="timber",
            vol_type="sawlog",  # Sawlog volume specifically
            by_size_class=True,
            totals=True
        )
        progress.update(task, completed=True)
        
        # 4. Volume by species (top 10)
        task = progress.add_task("[green]Analyzing volume by species...[/green]", total=None)
        volume_by_species = volume(
            db,
            tree_type="gs",
            land_type="timber",
            vol_type="net",
            by_species=True,
            totals=True
        )
        progress.update(task, completed=True)
        
        # 5. Volume by ownership and diameter class
        task = progress.add_task("[green]Analyzing volume by ownership...[/green]", total=None)
        volume_by_ownership = volume(
            db,
            tree_type="gs",
            land_type="timber",
            vol_type="net",
            grp_by=["OWNGRPCD"],
            by_size_class=True,
            totals=True
        )
        progress.update(task, completed=True)
    
    # Display all results
    console.print("\n[bold blue]California Merchantable Timber Volume Analysis[/bold blue]")
    console.print("[dim]All volumes are net cubic feet of growing stock on timberland[/dim]\n")
    
    # 1. Total merchantable volume
    console.print("[bold green]1. Total Merchantable Volume[/bold green]")
    
    if not total_volume.is_empty():
        row = total_volume.row(0, named=True)
        console.print(f"   Per acre: {row['VOLCFNET_ACRE']:,.1f} cubic feet/acre")
        console.print(f"   Standard error: ±{row['VOLCFNET_ACRE_SE']:.2f} cubic feet/acre")
        if 'VOL_TOTAL_BOLE_CF_ACRE' in row:
            console.print(f"   Total volume: {row['VOL_TOTAL_BOLE_CF_ACRE']:,.0f} cubic feet")
        console.print(f"   Number of plots: {row['nPlots_TREE']:,}")
    
    # 2. Volume by diameter class
    console.print("\n[bold green]2. Merchantable Volume by Diameter Class[/bold green]")
    
    table1 = Table(title="Net Cubic Foot Volume by Diameter Class")
    table1.add_column("Diameter Class", style="cyan")
    table1.add_column("Volume (ft³)", justify="right", style="green")
    table1.add_column("Per Acre", justify="right")
    table1.add_column("% of Total", justify="right")
    table1.add_column("Std Error", justify="right")
    
    # Calculate total for percentages
    total_vol = volume_by_diameter['VOL_TOTAL_BOLE_CF_ACRE'].sum() if not volume_by_diameter.is_empty() and 'VOL_TOTAL_BOLE_CF_ACRE' in volume_by_diameter.columns else 0
    
    # Define custom sort order for size classes
    size_class_order = ["1.0-4.9", "5.0-9.9", "10.0-19.9", "20.0-29.9", "30.0+"]
    
    # Sort by custom order
    for size_class in size_class_order:
        rows = volume_by_diameter.filter(pl.col("sizeClass") == size_class)
        if not rows.is_empty():
            row = rows.row(0, named=True)
            vol_total = row.get('VOL_TOTAL_BOLE_CF_ACRE', 0)
            percentage = (vol_total / total_vol * 100) if total_vol > 0 else 0
            table1.add_row(
                row["sizeClass"] + " inches",
                f"{vol_total:,.0f}" if vol_total else "-",
                f"{row['VOLCFNET_ACRE']:,.1f} ft³/ac",
                f"{percentage:.1f}%",
                f"±{row['VOLCFNET_ACRE_SE']:.2f}"
            )
    
    console.print(table1)
    
    # 3. Sawlog volume by diameter class
    console.print("\n[bold green]3. Sawlog Volume by Diameter Class (≥9\" DBH)[/bold green]")
    
    table2 = Table(title="Net Sawlog Volume by Diameter Class")
    table2.add_column("Diameter Class", style="cyan")
    table2.add_column("Sawlog Volume (ft³)", justify="right", style="green")
    table2.add_column("Per Acre", justify="right")
    table2.add_column("% of Sawlog", justify="right")
    
    # Calculate total sawlog volume
    total_sawlog = sawlog_volume['VOL_TOTAL_SAW_CF_ACRE'].sum() if not sawlog_volume.is_empty() and 'VOL_TOTAL_SAW_CF_ACRE' in sawlog_volume.columns else 0
    
    # Only show relevant size classes for sawlog (≥9")
    sawlog_classes = ["10.0-19.9", "20.0-29.9", "30.0+"]
    
    for size_class in sawlog_classes:
        rows = sawlog_volume.filter(pl.col("sizeClass") == size_class)
        if not rows.is_empty():
            row = rows.row(0, named=True)
            vol_total = row.get('VOL_TOTAL_SAW_CF_ACRE', 0)
            percentage = (vol_total / total_sawlog * 100) if total_sawlog > 0 else 0
            table2.add_row(
                row["sizeClass"] + " inches",
                f"{vol_total:,.0f}" if vol_total else "-",
                f"{row['VOLCSNET_ACRE']:,.1f} ft³/ac",
                f"{percentage:.1f}%"
            )
    
    console.print(table2)
    
    # 4. Top species by volume
    console.print("\n[bold green]4. Top 10 Species by Volume[/bold green]")
    
    # Sort by volume and get top 10
    vol_col = 'VOL_TOTAL_BOLE_CF_ACRE' if 'VOL_TOTAL_BOLE_CF_ACRE' in volume_by_species.columns else 'VOLCFNET_ACRE'
    top_species = volume_by_species.sort(vol_col, descending=True).head(10)
    
    # Recalculate total for species percentages
    species_total = volume_by_species[vol_col].sum() if vol_col in volume_by_species.columns else 0
    
    table3 = Table(title="Merchantable Volume by Species")
    table3.add_column("Species Code", style="cyan")
    table3.add_column("Volume (ft³)", justify="right", style="green")
    table3.add_column("Per Acre", justify="right")
    table3.add_column("% of Total", justify="right")
    
    for row in top_species.iter_rows(named=True):
        vol_total = row.get('VOL_TOTAL_BOLE_CF_ACRE', 0)
        percentage = (vol_total / species_total * 100) if species_total > 0 else 0
        table3.add_row(
            f"SPCD {row['SPCD']}",
            f"{vol_total:,.0f}" if vol_total else "-",
            f"{row['VOLCFNET_ACRE']:,.1f} ft³/ac",
            f"{percentage:.1f}%"
        )
    
    console.print(table3)
    
    # 5. Volume by ownership and diameter class
    console.print("\n[bold green]5. Volume by Ownership and Diameter Class[/bold green]")
    
    # Pivot data for better display
    ownership_pivot = (
        volume_by_ownership
        .with_columns(
            pl.col("OWNGRPCD").map_elements(get_ownership_group_name, return_dtype=pl.Utf8).alias("Owner")
        )
        .pivot(
            values="VOLCFNET_ACRE",
            index="sizeClass",
            on="Owner",
            aggregate_function="sum"
        )
    )
    
    table4 = Table(title="Volume per Acre by Ownership and Diameter (ft³/ac)")
    table4.add_column("Diameter Class", style="cyan")
    
    # Add columns for each ownership type
    owner_cols = [col for col in ownership_pivot.columns if col != "sizeClass"]
    for owner in owner_cols:
        table4.add_column(owner, justify="right")
    
    # Sort by size class order
    for size_class in size_class_order:
        rows = ownership_pivot.filter(pl.col("sizeClass") == size_class)
        if not rows.is_empty():
            row = rows.row(0, named=True)
            row_data = [row["sizeClass"] + " inches"]
            for owner in owner_cols:
                value = row.get(owner, 0)
                row_data.append(f"{value:,.1f}" if value else "-")
            table4.add_row(*row_data)
    
    console.print(table4)
    
    # Summary statistics
    console.print("\n[bold green]6. Summary Statistics[/bold green]")
    
    # Calculate average diameter distribution
    if not volume_by_diameter.is_empty():
        console.print("\n[yellow]Diameter Distribution of Merchantable Volume:[/yellow]")
        # Recalculate total using correct column
        dist_total = volume_by_diameter['VOL_TOTAL_BOLE_CF_ACRE'].sum() if 'VOL_TOTAL_BOLE_CF_ACRE' in volume_by_diameter.columns else volume_by_diameter['VOLCFNET_ACRE'].sum()
        
        for size_class in size_class_order:
            rows = volume_by_diameter.filter(pl.col("sizeClass") == size_class)
            if not rows.is_empty():
                row = rows.row(0, named=True)
                vol_value = row.get('VOL_TOTAL_BOLE_CF_ACRE', row.get('VOLCFNET_ACRE', 0))
                percentage = (vol_value / dist_total * 100) if dist_total > 0 else 0
                bar_length = int(percentage / 2)  # Scale to fit
                bar = "█" * bar_length
                console.print(f"   {row['sizeClass']:>10} inches: {bar} {percentage:.1f}%")
    
    console.print("\n[bold yellow]Analysis complete![/bold yellow]")


if __name__ == "__main__":
    import sys
    
    # Check if database path is provided as command line argument
    db_path = sys.argv[1] if len(sys.argv) > 1 else "fia.duckdb"
    
    try:
        estimate_california_merchantable_volume(db_path)
    except Exception as e:
        console.print(f"\n[bold red]Error:[/bold red] {str(e)}")
        console.print("\n[yellow]Usage:[/yellow] python estimate_california_volume_by_diameter.py [path_to_fia_db]")
        console.print("\nIf no path is provided, the script will use 'fia.duckdb' in the current directory.")
        sys.exit(1)