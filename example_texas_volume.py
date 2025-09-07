#!/usr/bin/env python
"""
Example: Calculate volume of trees on forestland in Texas using pyFIA.

This script demonstrates how to use the volume() function to estimate
tree volume from a multi-state FIA database.
"""

from pyfia import FIA, volume
from rich.console import Console
from rich.table import Table
from rich import print as rprint

console = Console()

def main():
    """Calculate volume estimates for Texas forestland."""
    
    # Path to the multi-state database
    db_path = "nfi_south.duckdb"
    
    console.print("\n[bold cyan]pyFIA Volume Estimation Example[/bold cyan]")
    console.print("=" * 50)
    
    try:
        # Open the database connection
        with FIA(db_path) as db:
            console.print(f"\n[green]✓[/green] Connected to database: {db_path}")
            
            # Filter to Texas (state code 48) and most recent evaluation
            # For volume estimation, we want EXPVOL evaluations
            db.clip_by_state(48, most_recent=True, eval_type="EXPVOL")
            console.print("[green]✓[/green] Filtered to Texas, most recent volume evaluation")
            
            # Calculate volume on forestland
            # COND_STATUS_CD == 1 indicates forestland
            console.print("\n[yellow]Calculating volume estimates...[/yellow]")
            
            # Basic volume estimate for all trees on forestland
            vol_results = volume(
                db,
                area_domain="COND_STATUS_CD == 1",  # Forestland only
                tree_domain="STATUSCD == 1"  # Live trees only
            )
            
            # Display basic results
            console.print("\n[bold]Total Volume on Texas Forestland:[/bold]")
            table = Table(show_header=True, header_style="bold magenta")
            table.add_column("Metric", style="cyan")
            table.add_column("Value", justify="right")
            table.add_column("Units", style="dim")
            
            table.add_row(
                "Total Volume",
                f"{vol_results['ESTIMATE'][0]:,.0f}",
                "cubic feet"
            )
            table.add_row(
                "Standard Error (%)",
                f"{vol_results['SE_PERCENT'][0]:.2f}",
                "%"
            )
            table.add_row(
                "Volume per Acre",
                f"{vol_results['ESTIMATE_per_acre'][0]:,.2f}",
                "cubic feet/acre"
            )
            table.add_row(
                "Number of Plots",
                f"{vol_results['N'][0]:,}",
                "plots"
            )
            
            console.print(table)
            
            # Volume by species (top 10)
            console.print("\n[yellow]Calculating volume by species...[/yellow]")
            vol_by_species = volume(
                db,
                area_domain="COND_STATUS_CD == 1",  # Forestland only
                tree_domain="STATUSCD == 1",  # Live trees only
                by_species=True
            )
            
            # Sort by volume and get top 10
            vol_by_species_sorted = vol_by_species.sort_values(
                'ESTIMATE', 
                descending=True
            ).head(10)
            
            # Display species results
            console.print("\n[bold]Top 10 Species by Volume on Texas Forestland:[/bold]")
            species_table = Table(show_header=True, header_style="bold magenta")
            species_table.add_column("Species Code", style="cyan")
            species_table.add_column("Volume (cubic feet)", justify="right")
            species_table.add_column("% of Total", justify="right", style="yellow")
            species_table.add_column("SE (%)", justify="right", style="dim")
            
            total_volume = vol_results['ESTIMATE'][0]
            
            for _, row in vol_by_species_sorted.iterrows():
                species_table.add_row(
                    str(int(row['SPCD'])),
                    f"{row['ESTIMATE']:,.0f}",
                    f"{(row['ESTIMATE'] / total_volume * 100):.1f}",
                    f"{row['SE_PERCENT']:.2f}"
                )
            
            console.print(species_table)
            
            # Volume by forest type
            console.print("\n[yellow]Calculating volume by forest type...[/yellow]")
            vol_by_forest_type = volume(
                db,
                area_domain="COND_STATUS_CD == 1",  # Forestland only
                tree_domain="STATUSCD == 1",  # Live trees only
                grp_by="FORTYPCD"  # Group by forest type code
            )
            
            # Sort by volume and get top 5
            vol_by_type_sorted = vol_by_forest_type.sort_values(
                'ESTIMATE',
                descending=True
            ).head(5)
            
            # Display forest type results
            console.print("\n[bold]Top 5 Forest Types by Volume:[/bold]")
            type_table = Table(show_header=True, header_style="bold magenta")
            type_table.add_column("Forest Type", style="cyan")
            type_table.add_column("Volume (cubic feet)", justify="right")
            type_table.add_column("Volume/Acre", justify="right", style="yellow")
            type_table.add_column("SE (%)", justify="right", style="dim")
            
            for _, row in vol_by_type_sorted.iterrows():
                type_table.add_row(
                    str(int(row['FORTYPCD'])),
                    f"{row['ESTIMATE']:,.0f}",
                    f"{row['ESTIMATE_per_acre']:,.1f}",
                    f"{row['SE_PERCENT']:.2f}"
                )
            
            console.print(type_table)
            
            # Volume by size class
            console.print("\n[yellow]Calculating volume by diameter class...[/yellow]")
            
            # Define diameter classes
            size_classes = [
                ("Small (5-10 inches)", "DIA >= 5.0 AND DIA < 10.0"),
                ("Medium (10-15 inches)", "DIA >= 10.0 AND DIA < 15.0"),
                ("Large (15-20 inches)", "DIA >= 15.0 AND DIA < 20.0"),
                ("Very Large (20+ inches)", "DIA >= 20.0")
            ]
            
            console.print("\n[bold]Volume by Diameter Class:[/bold]")
            size_table = Table(show_header=True, header_style="bold magenta")
            size_table.add_column("Size Class", style="cyan")
            size_table.add_column("Volume (cubic feet)", justify="right")
            size_table.add_column("% of Total", justify="right", style="yellow")
            
            for class_name, tree_filter in size_classes:
                vol_by_size = volume(
                    db,
                    area_domain="COND_STATUS_CD == 1",
                    tree_domain=f"STATUSCD == 1 AND {tree_filter}"
                )
                
                if not vol_by_size.empty:
                    size_table.add_row(
                        class_name,
                        f"{vol_by_size['ESTIMATE'][0]:,.0f}",
                        f"{(vol_by_size['ESTIMATE'][0] / total_volume * 100):.1f}"
                    )
            
            console.print(size_table)
            
            console.print("\n[green bold]✓ Analysis complete![/green bold]\n")
            
    except FileNotFoundError:
        console.print(f"\n[red]Error:[/red] Database file not found: {db_path}")
        console.print("Please ensure the nfi_south.duckdb file exists in the current directory.")
    except Exception as e:
        console.print(f"\n[red]Error:[/red] {str(e)}")
        raise

if __name__ == "__main__":
    main()