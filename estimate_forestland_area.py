#!/usr/bin/env python
"""
Estimate total forestland area in nfi_south.duckdb grouped by state.
"""

from pathlib import Path
import polars as pl
from pyfia import FIA, area
from pyfia.constants import StateCodes
from rich.console import Console
from rich.table import Table

console = Console()

def estimate_forestland_area_by_state():
    """Estimate forestland area by state in the southern NFI database."""
    
    # Path to the database
    db_path = Path("data/nfi_south.duckdb")
    
    if not db_path.exists():
        console.print(f"[red]Error: Database not found at {db_path}[/red]")
        return
    
    console.print(f"[cyan]Connecting to {db_path}...[/cyan]")
    
    # First, let's check what states are in the database
    import duckdb
    with duckdb.connect(str(db_path), read_only=True) as conn:
        states_query = """
        SELECT DISTINCT 
            STATECD,
            COUNT(DISTINCT CN) as plot_count
        FROM PLOT
        GROUP BY STATECD
        ORDER BY STATECD
        """
        states_in_db = conn.execute(states_query).fetchall()
        
        console.print("\n[green]States in database:[/green]")
        for state_code, plot_count in states_in_db:
            state_name = StateCodes.CODE_TO_NAME.get(state_code, f"Unknown ({state_code})")
            console.print(f"  {state_name} (Code: {state_code}): {plot_count:,} plots")
        
        # Check available EVALIDs from POP_EVAL table
        evalid_query = """
        SELECT DISTINCT 
            pe.EVALID,
            pe.STATECD,
            pe.EVAL_DESCR,
            pet.EVAL_TYP,
            COUNT(DISTINCT ppsa.PLT_CN) as plot_count
        FROM POP_EVAL pe
        JOIN POP_EVAL_TYP pet ON pe.CN = pet.EVAL_CN
        LEFT JOIN POP_PLOT_STRATUM_ASSGN ppsa ON pe.EVALID = ppsa.EVALID
        WHERE pet.EVAL_TYP = 'EXPALL'  -- Area estimation evaluations
        GROUP BY pe.EVALID, pe.STATECD, pe.EVAL_DESCR, pet.EVAL_TYP
        ORDER BY pe.STATECD, pe.EVALID DESC
        """
        
        try:
            evalids = conn.execute(evalid_query).fetchall()
            
            console.print("\n[green]Available EXPALL evaluations for area estimation:[/green]")
            current_state = None
            shown = 0
            for evalid, state_code, eval_descr, eval_typ, plot_count in evalids:
                if state_code != current_state:
                    current_state = state_code
                    state_name = StateCodes.CODE_TO_NAME.get(state_code, f"Unknown ({state_code})")
                    console.print(f"\n  {state_name}:")
                    shown = 0
                if shown < 2:  # Show top 2 EVALIDs per state
                    console.print(f"    EVALID: {evalid} - {eval_descr} (Plots: {plot_count:,})")
                    shown += 1
        except Exception as e:
            console.print(f"[yellow]Could not query EVALIDs: {e}[/yellow]")
    
    # Now estimate area using pyfia
    console.print("\n[cyan]Estimating forestland area by state...[/cyan]")
    
    try:
        with FIA(str(db_path)) as db:
            # Don't filter by EVALID - let's get all available data
            # The area function will handle proper expansion factors
            
            # Estimate forestland area grouped by state
            # We'll group by STATECD to get state-level estimates
            results = area(
                db,
                land_type="forest",  # Forestland only
                grp_by="STATECD",    # Group by state
                variance=True        # Include variance for standard error calculation
            )
            
        console.print(f"\n[green]Successfully estimated area for {len(results)} states[/green]")
        return results
        
    except Exception as e:
        console.print(f"\n[red]Error during area estimation: {e}[/red]")
        console.print("\n[yellow]Attempting alternative approach...[/yellow]")
        
        # Try a simpler approach without specific EVALID filtering
        try:
            with FIA(str(db_path)) as db:
                # Just estimate without any filtering
                results = area(
                    db,
                    land_type="forest",
                    grp_by="STATECD"
                )
                console.print(f"\n[green]Successfully estimated area using simplified approach[/green]")
                return results
        except Exception as e2:
            console.print(f"\n[red]Alternative approach also failed: {e2}[/red]")
            return None

def display_results(results):
    """Display the area estimation results in a formatted table."""
    
    if results is None or results.is_empty():
        console.print("[red]No results to display[/red]")
        return
    
    # Print column names to understand the structure
    console.print(f"\n[dim]Result columns: {results.columns}[/dim]")
    
    # Create a rich table for display
    table = Table(title="Forestland Area by State", show_header=True, header_style="bold magenta")
    table.add_column("State", style="cyan", no_wrap=True)
    table.add_column("Area (acres)", justify="right")
    
    # Check if variance columns exist
    has_variance = "AREA_VAR" in results.columns or "AREA_TOTAL_VAR" in results.columns
    if has_variance:
        table.add_column("SE (acres)", justify="right")
        table.add_column("95% CI Lower", justify="right")
        table.add_column("95% CI Upper", justify="right")
    
    table.add_column("% of Total", justify="right")
    
    # Determine the area column name
    area_col = "AREA" if "AREA" in results.columns else "AREA_TOTAL" if "AREA_TOTAL" in results.columns else None
    var_col = "AREA_VAR" if "AREA_VAR" in results.columns else "AREA_TOTAL_VAR" if "AREA_TOTAL_VAR" in results.columns else None
    
    if area_col is None:
        console.print("[red]Could not find area column in results[/red]")
        return
    
    # Calculate total area for percentage calculation
    total_area = results.select(pl.sum(area_col)).item()
    
    # Sort by state code for consistent display
    sorted_results = results.sort("STATECD")
    
    for row in sorted_results.iter_rows(named=True):
        state_code = row.get("STATECD")
        state_name = StateCodes.CODE_TO_NAME.get(state_code, f"State {state_code}")
        area = row.get(area_col, 0)
        
        # Calculate percentage of total
        pct_of_total = (area / total_area * 100) if total_area else 0
        
        if has_variance and var_col:
            variance = row.get(var_col, 0)
            se = variance ** 0.5  # SE is square root of variance
            
            # Calculate 95% confidence interval
            ci_lower = max(0, area - (1.96 * se))
            ci_upper = area + (1.96 * se)
            
            table.add_row(
                state_name,
                f"{area:,.0f}",
                f"{se:,.0f}",
                f"{ci_lower:,.0f}",
                f"{ci_upper:,.0f}",
                f"{pct_of_total:.1f}%"
            )
        else:
            table.add_row(
                state_name,
                f"{area:,.0f}",
                f"{pct_of_total:.1f}%"
            )
    
    # Add total row
    if total_area > 0:
        table.add_section()
        if has_variance and var_col:
            total_var = results.select(pl.sum(var_col)).item()
            total_se = total_var ** 0.5 if total_var else 0
            ci_lower = max(0, total_area - (1.96 * total_se))
            ci_upper = total_area + (1.96 * total_se)
            
            table.add_row(
                "[bold]TOTAL[/bold]",
                f"[bold]{total_area:,.0f}[/bold]",
                f"[bold]{total_se:,.0f}[/bold]",
                f"[bold]{ci_lower:,.0f}[/bold]",
                f"[bold]{ci_upper:,.0f}[/bold]",
                "[bold]100.0%[/bold]"
            )
        else:
            table.add_row(
                "[bold]TOTAL[/bold]",
                f"[bold]{total_area:,.0f}[/bold]",
                "[bold]100.0%[/bold]"
            )
    
    console.print("\n")
    console.print(table)
    
    # Also save to CSV for further analysis
    csv_path = Path("forestland_area_by_state.csv")
    results.write_csv(str(csv_path))
    console.print(f"\n[green]Results saved to {csv_path}[/green]")

if __name__ == "__main__":
    console.print("[bold cyan]Forestland Area Estimation by State[/bold cyan]\n")
    
    results = estimate_forestland_area_by_state()
    
    if results is not None:
        display_results(results)
        console.print("\n[green]✓ Analysis complete![/green]")
    else:
        console.print("\n[red]Analysis failed. Please check the error messages above.[/red]")