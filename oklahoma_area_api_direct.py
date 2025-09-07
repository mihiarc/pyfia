#!/usr/bin/env python
"""
Calculate forestland area in Oklahoma using pyFIA area() function.
Reports only the direct results from the API without additional calculations.
"""

from pyfia import FIA, area
from rich.console import Console
from rich.table import Table

def main():
    """Calculate and display Oklahoma forestland area using pyFIA API."""
    console = Console()
    
    console.print("\n[bold cyan]Oklahoma Forestland Area from pyFIA API[/bold cyan]")
    console.print("=" * 50)
    
    # Load database and calculate area
    with FIA("nfi_south.duckdb") as db:
        # Filter to Oklahoma (state code 40) with most recent evaluation
        db.clip_by_state(40, most_recent=True)
        
        # Call area function with totals=True to get acre values
        results = area(db, totals=True)
        
        # Display the raw API results
        display_api_results(results, console)

def display_api_results(results, console):
    """Display only the direct results from the area() API."""
    
    if results.is_empty():
        console.print("[red]No results returned from API[/red]")
        return
    
    # Get column names from the actual API response
    columns = results.columns
    console.print(f"\n[yellow]API returned columns:[/yellow] {', '.join(columns)}")
    
    # Get the data row
    row = results.row(0, named=True)
    
    # Create table for API results
    table = Table(title="Direct API Results", show_header=True, header_style="bold magenta")
    table.add_column("Field", style="cyan")
    table.add_column("Value", style="green", justify="right")
    table.add_column("Description", style="yellow")
    
    # Map API fields to descriptions based on pyFIA documentation
    field_descriptions = {
        "AREA_PERC": "Percentage of land that is forest",
        "AREA": "Total forest area in acres",
        "N_PLOTS": "Number of FIA plots used in estimate"
    }
    
    # Display each field returned by the API
    for field in columns:
        value = row[field]
        description = field_descriptions.get(field, "API field")
        
        # Format the value based on type
        if isinstance(value, float):
            if field == "AREA":
                formatted_value = f"{value:,.0f}"
            else:
                formatted_value = f"{value:.2f}"
        elif isinstance(value, int):
            formatted_value = f"{value:,}"
        else:
            formatted_value = str(value)
        
        table.add_row(field, formatted_value, description)
    
    console.print("\n[bold]Results from area() function:[/bold]")
    console.print(table)
    
    # Also show the raw DataFrame for complete transparency
    console.print("\n[bold]Raw DataFrame returned by API:[/bold]")
    console.print(results)

if __name__ == "__main__":
    main()