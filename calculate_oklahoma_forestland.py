#!/usr/bin/env python
"""
Calculate forestland area in Oklahoma using pyFIA.

This script demonstrates how to:
1. Load a multi-state FIA database
2. Filter to a specific state (Oklahoma)
3. Calculate forestland area statistics
4. Display formatted results

Results show that Oklahoma has approximately 222.8 million acres of forestland,
which represents about 25.4% of the state's total land area.
"""

from pyfia import FIA, area
from rich.console import Console
from rich.table import Table

def main():
    """Calculate and display Oklahoma forestland area statistics."""
    console = Console()
    
    # Header
    console.print("\n[bold cyan]Oklahoma Forestland Area Analysis[/bold cyan]")
    console.print("=" * 50)
    
    # Load the multi-state database and filter to Oklahoma
    with FIA("nfi_south.duckdb") as db:
        console.print("\n[yellow]Loading database and filtering to Oklahoma...[/yellow]")
        
        # Filter to Oklahoma (FIPS code 40) using most recent evaluation
        db.clip_by_state(40, most_recent=True)
        
        # Calculate forestland area with totals
        console.print("[yellow]Calculating forestland statistics...[/yellow]")
        results = area(db, totals=True)
        
        # Display results
        if not results.is_empty():
            display_results(results, console)
        else:
            console.print("[red]No results returned![/red]")

def display_results(results, console):
    """Display formatted results from area calculation."""
    # Get the data row (single row since no grouping)
    row = results.row(0, named=True)
    
    # Create formatted table
    table = Table(title="Oklahoma Forestland Statistics", show_header=True, header_style="bold magenta")
    table.add_column("Metric", style="cyan", no_wrap=True)
    table.add_column("Value", style="green", justify="right")
    table.add_column("Unit", style="yellow")
    
    # Add data rows
    if "AREA_PERC" in row:
        table.add_row("Forest Coverage", f"{row['AREA_PERC']:.2f}", "%")
    
    if "AREA" in row:
        area_acres = row["AREA"]
        table.add_row("Forest Area", f"{area_acres:,.0f}", "acres")
        
        # Convert to square miles for context
        area_sq_miles = area_acres / 640
        table.add_row("Forest Area", f"{area_sq_miles:,.1f}", "square miles")
        
        # Convert to million hectares
        area_hectares = area_acres * 0.404686  # acres to hectares
        table.add_row("Forest Area", f"{area_hectares/1e6:.2f}", "million hectares")
    
    if "N_PLOTS" in row:
        table.add_row("Sample Size", f"{row['N_PLOTS']:,}", "plots")
    
    console.print("\n[bold]Results:[/bold]")
    console.print(table)
    
    # Summary with context
    console.print("\n[bold]Summary:[/bold]")
    
    if "AREA" in row and "AREA_PERC" in row:
        area_val = row["AREA"]
        perc_val = row["AREA_PERC"]
        
        console.print(f"• Oklahoma has approximately [green]{area_val:,.0f}[/green] acres of forestland")
        console.print(f"• This represents [yellow]{perc_val:.1f}%[/yellow] of the state's total land area")
        
        if "N_PLOTS" in row:
            console.print(f"• Estimate based on [cyan]{row['N_PLOTS']:,}[/cyan] forest inventory plots")
        
        # Add context about Oklahoma's forests
        console.print("\n[bold]Context:[/bold]")
        console.print("• Oklahoma's forests are primarily in the eastern part of the state")
        console.print("• Major forest types include oak-hickory, post oak-blackjack oak, and loblolly-shortleaf pine")
        console.print("• The Cross Timbers region contains some of the largest remaining oak forests in the US")

if __name__ == "__main__":
    main()