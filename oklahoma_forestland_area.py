#!/usr/bin/env python
"""
Calculate forestland area in Oklahoma using pyFIA.
"""

from pyfia import FIA, area
from rich.console import Console
from rich.table import Table

console = Console()

# Open the multi-state database
console.print("\n[bold cyan]Calculating Oklahoma Forestland Area[/bold cyan]")
console.print("=" * 50)

with FIA("nfi_south.duckdb") as db:
    # Filter to Oklahoma (state code 40) and use most recent evaluation
    console.print("\n[yellow]Filtering to Oklahoma (STATECD=40)...[/yellow]")
    db.clip_by_state(40, most_recent=True)
    
    # Calculate forestland area
    console.print("[yellow]Calculating forestland area...[/yellow]")
    
    # Basic area calculation for forest land
    results = area(db, totals=True)
    
    # Display results
    console.print("\n[bold green]Results:[/bold green]")
    
    if not results.is_empty():
        # Get the first (and only) row since we're not grouping
        row = results.row(0, named=True)
        
        # Create results table
        table = Table(title="Oklahoma Forestland Statistics")
        table.add_column("Metric", style="cyan", no_wrap=True)
        table.add_column("Value", style="green", justify="right")
        table.add_column("Unit", style="yellow")
        
        # Forest area percentage
        if "AREA_PERC" in row:
            table.add_row("Forest Coverage", f"{row['AREA_PERC']:.2f}", "%")
            
        # Total forest area in acres
        if "AREA" in row:
            area_acres = row["AREA"]
            table.add_row("Forest Area", f"{area_acres:,.0f}", "acres")
            
            # Convert to square miles
            area_sq_miles = area_acres / 640
            table.add_row("Forest Area", f"{area_sq_miles:,.1f}", "square miles")
            
        # Standard error (calculate from variance if available)
        if "AREA_SE" in row:
            se = row["AREA_SE"]
            table.add_row("Standard Error", f"±{se:,.0f}", "acres")
            
            # Calculate 95% confidence interval
            if "AREA" in row:
                area_val = row["AREA"]
                ci_lower = area_val - (1.96 * se)
                ci_upper = area_val + (1.96 * se)
                table.add_row("95% CI", f"{ci_lower:,.0f} - {ci_upper:,.0f}", "acres")
        
        # Number of plots
        if "N_PLOTS" in row:
            table.add_row("Sample Size", str(row["N_PLOTS"]), "plots")
        
        console.print(table)
        
        # Summary statistics
        console.print("\n[bold]Summary:[/bold]")
        
        if "AREA" in row:
            area_val = row["AREA"]
            
            console.print(f"• Oklahoma has [green]{area_val:,.0f}[/green] acres of forestland")
            
            # Use the AREA_PERC column directly for percentage
            if "AREA_PERC" in row:
                console.print(f"• This represents [yellow]{row['AREA_PERC']:.1f}%[/yellow] of the state's total land area")
            
            if "AREA_SE" in row:
                se = row["AREA_SE"]
                cv = (se / area_val) * 100  # Coefficient of variation
                console.print(f"• Standard error: ±{se:,.0f} acres (CV={cv:.1f}%)")
            
            if "N_PLOTS" in row:
                console.print(f"• Based on {row['N_PLOTS']} forest inventory plots")
        
        # Show the raw dataframe for debugging
        console.print("\n[dim]Raw results DataFrame:[/dim]")
        console.print(results)
    else:
        console.print("[red]No results returned![/red]")