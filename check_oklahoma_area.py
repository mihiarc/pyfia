#!/usr/bin/env python
"""
Script to calculate forestland area in Oklahoma using pyFIA.
This demonstrates using the area() function with state filtering.
"""

from pyfia import FIA, area
import polars as pl
from rich.console import Console
from rich.table import Table

console = Console()

# First, let's check what states are in the database
console.print("\n[bold cyan]Checking states in nfi_south.duckdb...[/bold cyan]")

with FIA("nfi_south.duckdb") as db:
    # Check available states using direct query through the backend
    conn = db._reader._backend._connection
    states_query = """
        SELECT 
            STATECD as state_code,
            COUNT(DISTINCT CN) as plot_count,
            COUNT(DISTINCT INVYR) as years,
            MIN(INVYR) as min_year,
            MAX(INVYR) as max_year
        FROM PLOT
        GROUP BY STATECD
        ORDER BY STATECD
    """
    
    states = conn.execute(states_query).fetchall()
    
    # State names mapping
    state_names = {
        1: "Alabama", 12: "Florida", 13: "Georgia", 
        22: "Louisiana", 28: "Mississippi", 40: "Oklahoma", 48: "Texas"
    }
    
    # Display states table
    table = Table(title="States in Database")
    table.add_column("State", style="cyan")
    table.add_column("FIPS Code", style="magenta")
    table.add_column("Plots", style="green")
    table.add_column("Years", style="yellow")
    table.add_column("Year Range", style="blue")
    
    for state_code, plots, years, min_year, max_year in states:
        name = state_names.get(state_code, f"Unknown ({state_code})")
        table.add_row(
            name, 
            str(state_code), 
            f"{plots:,}", 
            str(years),
            f"{min_year}-{max_year}"
        )
    
    console.print(table)
    
    # Check if Oklahoma (40) is in the database
    oklahoma_present = any(state[0] == 40 for state in states)
    
    if not oklahoma_present:
        console.print("\n[bold red]Oklahoma (STATECD=40) not found in database![/bold red]")
        console.print("Please ensure Oklahoma data has been added to nfi_south.duckdb")
    else:
        console.print(f"\n[bold green]✓ Oklahoma found in database[/bold green]")
        
        # Now calculate forestland area for Oklahoma
        console.print("\n[bold cyan]Calculating forestland area in Oklahoma...[/bold cyan]")
        
        # Filter to Oklahoma and most recent evaluation
        db.clip_by_state(40, most_recent=True)
        
        # Calculate area by land type
        # 'forest' land_type includes all forestland
        area_results = area(db, land_type='forest')
        
        console.print("\n[bold yellow]Forestland Area in Oklahoma:[/bold yellow]")
        
        # Display results
        if isinstance(area_results, pl.DataFrame):
            # Convert to pandas for easier display or keep as polars
            total_area = area_results.filter(pl.col("YEAR") == area_results["YEAR"].max())
            
            if len(total_area) > 0:
                row = total_area.row(0, named=True)
                
                # Create results table
                results_table = Table(title="Oklahoma Forestland Statistics")
                results_table.add_column("Metric", style="cyan")
                results_table.add_column("Value", style="green")
                results_table.add_column("Unit", style="yellow")
                
                results_table.add_row("Year", str(row.get("YEAR", "N/A")), "")
                results_table.add_row("Forest Area", f"{row.get('AREA_TOTAL', 0):,.0f}", "acres")
                results_table.add_row("Standard Error", f"{row.get('AREA_TOTAL_SE', 0):,.0f}", "acres")
                results_table.add_row("Sample Size (plots)", str(row.get('nPlots_TOTAL', 0)), "plots")
                
                # Calculate confidence interval (95%)
                area_val = row.get('AREA_TOTAL', 0)
                se_val = row.get('AREA_TOTAL_SE', 0)
                ci_lower = area_val - (1.96 * se_val)
                ci_upper = area_val + (1.96 * se_val)
                
                results_table.add_row("95% CI Lower", f"{ci_lower:,.0f}", "acres")
                results_table.add_row("95% CI Upper", f"{ci_upper:,.0f}", "acres")
                
                # Convert to square miles for context
                area_sq_miles = area_val / 640  # 640 acres per square mile
                results_table.add_row("Forest Area", f"{area_sq_miles:,.1f}", "square miles")
                
                console.print(results_table)
                
                # Calculate percentage of state that is forested
                # Oklahoma total area is approximately 44.8 million acres
                oklahoma_total = 44_825_600  # acres
                forest_pct = (area_val / oklahoma_total) * 100
                
                console.print(f"\n[bold]Summary:[/bold]")
                console.print(f"• Oklahoma has approximately [green]{area_val:,.0f}[/green] acres of forestland")
                console.print(f"• This represents about [yellow]{forest_pct:.1f}%[/yellow] of the state's total land area")
                console.print(f"• Standard error: ±{se_val:,.0f} acres")
                console.print(f"• Based on {row.get('nPlots_TOTAL', 0)} forest inventory plots")
                
            else:
                console.print("[red]No data returned for the most recent year[/red]")
                console.print("\nFull results DataFrame:")
                console.print(area_results)
        else:
            console.print("[red]Unexpected result format[/red]")
            console.print(area_results)