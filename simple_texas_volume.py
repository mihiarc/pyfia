#!/usr/bin/env python
"""
Simple example: Calculate volume of trees on forestland in Texas using pyFIA.

This script demonstrates the basic usage of the volume() function API
to estimate tree volume from the nfi_south.duckdb multi-state database.
"""

from pyfia import FIA, volume
from rich.console import Console
from rich.table import Table

console = Console()

def main():
    """Calculate basic volume estimates for Texas forestland."""
    
    # Path to the multi-state database
    db_path = "nfi_south.duckdb"
    
    console.print("\n[bold cyan]pyFIA Volume Estimation - Texas Forestland[/bold cyan]")
    console.print("=" * 60)
    
    try:
        # Open the database and filter to Texas
        with FIA(db_path) as db:
            console.print(f"[green]✓[/green] Connected to: {db_path}")
            
            # Filter to Texas (state code 48)
            # Using most recent evaluation for volume
            db.clip_by_state(48, most_recent=True, eval_type="EXPVOL")
            console.print("[green]✓[/green] Filtered to Texas (state code: 48)")
            console.print("[green]✓[/green] Using most recent volume evaluation (EXPVOL)")
            
            # ===== BASIC VOLUME ESTIMATION =====
            console.print("\n[bold yellow]1. Total Volume on Forestland[/bold yellow]")
            console.print("   Calculating volume for all live trees on forestland...")
            
            # Calculate volume with basic filters
            # Using default parameters where possible
            vol_total = volume(
                db,
                land_type="forest",  # Only forestland (COND_STATUS_CD == 1)
                tree_type="live",    # Only live trees (STATUSCD == 1)
                vol_type="net"       # Net volume (default)
            )
            
            # Display results
            if not vol_total.empty:
                table = Table(show_header=True, header_style="bold magenta")
                table.add_column("Metric", style="cyan", width=25)
                table.add_column("Value", justify="right", style="white")
                table.add_column("Units", style="dim")
                
                table.add_row(
                    "Total Net Volume",
                    f"{vol_total['ESTIMATE'][0]:,.0f}",
                    "cubic feet"
                )
                table.add_row(
                    "Standard Error",
                    f"{vol_total['SE_PERCENT'][0]:.2f}",
                    "percent"
                )
                table.add_row(
                    "Volume per Acre",
                    f"{vol_total['ESTIMATE_per_acre'][0]:,.2f}",
                    "cubic feet/acre"
                )
                table.add_row(
                    "Number of Plots",
                    f"{vol_total['N'][0]:,}",
                    "plots"
                )
                
                console.print(table)
            
            # ===== VOLUME BY SPECIES =====
            console.print("\n[bold yellow]2. Volume by Species (Top 5)[/bold yellow]")
            console.print("   Breaking down volume by tree species...")
            
            vol_species = volume(
                db,
                land_type="forest",
                tree_type="live",
                by_species=True,  # Group by species
                vol_type="net"
            )
            
            if not vol_species.empty:
                # Sort and get top 5 species
                top_species = vol_species.sort_values('ESTIMATE', descending=True).head(5)
                
                species_table = Table(show_header=True, header_style="bold magenta")
                species_table.add_column("Species Code", style="cyan")
                species_table.add_column("Volume (cubic ft)", justify="right")
                species_table.add_column("Per Acre", justify="right", style="yellow")
                species_table.add_column("SE %", justify="right", style="dim")
                
                for _, row in top_species.iterrows():
                    species_table.add_row(
                        str(int(row['SPCD'])),
                        f"{row['ESTIMATE']:,.0f}",
                        f"{row['ESTIMATE_per_acre']:,.1f}",
                        f"{row['SE_PERCENT']:.2f}"
                    )
                
                console.print(species_table)
            
            # ===== VOLUME WITH CUSTOM DOMAIN FILTERS =====
            console.print("\n[bold yellow]3. Large Tree Volume (DIA >= 20 inches)[/bold yellow]")
            console.print("   Filtering to large diameter trees...")
            
            vol_large = volume(
                db,
                land_type="forest",
                tree_type="live",
                tree_domain="DIA >= 20.0",  # Custom filter for large trees
                vol_type="net"
            )
            
            if not vol_large.empty:
                large_table = Table(show_header=True, header_style="bold magenta")
                large_table.add_column("Metric", style="cyan", width=25)
                large_table.add_column("Value", justify="right", style="white")
                large_table.add_column("Units", style="dim")
                
                # Calculate percentage of total
                pct_of_total = (vol_large['ESTIMATE'][0] / vol_total['ESTIMATE'][0]) * 100
                
                large_table.add_row(
                    "Large Tree Volume",
                    f"{vol_large['ESTIMATE'][0]:,.0f}",
                    "cubic feet"
                )
                large_table.add_row(
                    "Percent of Total",
                    f"{pct_of_total:.1f}",
                    "percent"
                )
                large_table.add_row(
                    "Per Acre",
                    f"{vol_large['ESTIMATE_per_acre'][0]:,.2f}",
                    "cubic feet/acre"
                )
                
                console.print(large_table)
            
            # ===== TIMBER LAND VOLUME =====
            console.print("\n[bold yellow]4. Volume on Timberland Only[/bold yellow]")
            console.print("   Comparing timberland vs all forestland...")
            
            vol_timber = volume(
                db,
                land_type="timber",  # Timberland only (more restrictive)
                tree_type="live",
                vol_type="net"
            )
            
            if not vol_timber.empty:
                timber_table = Table(show_header=True, header_style="bold magenta")
                timber_table.add_column("Land Type", style="cyan")
                timber_table.add_column("Volume (cubic ft)", justify="right")
                timber_table.add_column("Per Acre", justify="right", style="yellow")
                
                timber_table.add_row(
                    "All Forestland",
                    f"{vol_total['ESTIMATE'][0]:,.0f}",
                    f"{vol_total['ESTIMATE_per_acre'][0]:,.1f}"
                )
                timber_table.add_row(
                    "Timberland Only",
                    f"{vol_timber['ESTIMATE'][0]:,.0f}",
                    f"{vol_timber['ESTIMATE_per_acre'][0]:,.1f}"
                )
                
                console.print(timber_table)
                
                # Show difference
                timber_pct = (vol_timber['ESTIMATE'][0] / vol_total['ESTIMATE'][0]) * 100
                console.print(f"\n   [dim]Timberland represents {timber_pct:.1f}% of total forestland volume[/dim]")
            
            # ===== GROSS VS NET VOLUME =====
            console.print("\n[bold yellow]5. Gross vs Net Volume Comparison[/bold yellow]")
            console.print("   Comparing gross volume (including defects) vs net volume...")
            
            vol_gross = volume(
                db,
                land_type="forest",
                tree_type="live",
                vol_type="gross"  # Gross volume (includes defects)
            )
            
            if not vol_gross.empty:
                comparison_table = Table(show_header=True, header_style="bold magenta")
                comparison_table.add_column("Volume Type", style="cyan")
                comparison_table.add_column("Total (cubic ft)", justify="right")
                comparison_table.add_column("Per Acre", justify="right", style="yellow")
                
                comparison_table.add_row(
                    "Gross Volume",
                    f"{vol_gross['ESTIMATE'][0]:,.0f}",
                    f"{vol_gross['ESTIMATE_per_acre'][0]:,.1f}"
                )
                comparison_table.add_row(
                    "Net Volume",
                    f"{vol_total['ESTIMATE'][0]:,.0f}",
                    f"{vol_total['ESTIMATE_per_acre'][0]:,.1f}"
                )
                
                console.print(comparison_table)
                
                # Calculate cull percentage
                cull_volume = vol_gross['ESTIMATE'][0] - vol_total['ESTIMATE'][0]
                cull_pct = (cull_volume / vol_gross['ESTIMATE'][0]) * 100
                console.print(f"\n   [dim]Cull/defect represents {cull_pct:.1f}% of gross volume[/dim]")
            
            console.print("\n[green bold]✓ Analysis complete![/green bold]")
            console.print("\n[dim]Note: All estimates include standard errors for statistical validity.[/dim]")
            console.print("[dim]Use area_domain and tree_domain parameters for custom filtering.[/dim]\n")
            
    except FileNotFoundError:
        console.print(f"\n[red]Error:[/red] Database not found: {db_path}")
        console.print("Please ensure nfi_south.duckdb exists in the current directory.")
        console.print("\nTo create it, use:")
        console.print("  from pyfia import convert_sqlite_to_duckdb")
        console.print("  convert_sqlite_to_duckdb('SQLite_FIADB_TX.db', 'nfi_south.duckdb', 48)")
        
    except Exception as e:
        console.print(f"\n[red]Error:[/red] {str(e)}")
        import traceback
        console.print("\n[dim]Full traceback:[/dim]")
        traceback.print_exc()

if __name__ == "__main__":
    main()