"""
Example script demonstrating detailed mortality estimation for Georgia using pyFIA.
This replicates the functionality of the mortality_with_variance_detailed_groups.sql query.
"""

from pathlib import Path
import polars as pl
from rich.console import Console
from rich.table import Table

from pyfia import FIA
from pyfia.estimation.mortality import mortality
from pyfia.filters.grouping import get_ownership_group_name

console = Console()

def get_mortality_estimates(db_path: str = "fia.duckdb"):
    """
    Calculate mortality estimates grouped by various characteristics.
    
    Args:
        db_path: Path to FIA DuckDB database
        
    Returns:
        DataFrame with mortality estimates and variance statistics
    """
    # Initialize FIA database connection and filter to Georgia
    db = FIA(db_path)
    db.clip_by_state(state=13, most_recent=True)  # 13 is Georgia's FIPS code
    
    # Define grouping variables
    group_vars = [
        "SPCD",          # Species code
        "SPGRPCD",       # Species group code
        "OWNGRPCD",      # Ownership group code
        "UNITCD",        # Unit code
        "AGENTCD",       # Mortality agent code
        "DSTRBCD1",      # Disturbance code 1
        "DSTRBCD2",      # Disturbance code 2
        "DSTRBCD3"       # Disturbance code 3
    ]
    
    # Calculate mortality estimates
    mortality_estimates = mortality(
        db=db,
        grp_by=group_vars,    # Group by our specified variables
        tree_class="all",     # Include all tree classes
        land_type="forest",   # Forest land only
        totals=True,         # Get total estimates (not just per acre)
        variance=True        # Include variance statistics
    )
    
    # Add ownership group names
    mortality_estimates = mortality_estimates.with_columns([
        pl.col("OWNGRPCD").map_elements(get_ownership_group_name, return_dtype=pl.Utf8).alias("OWNERSHIP_GROUP")
    ])
    
    # Sort by mortality estimate descending
    mortality_estimates = mortality_estimates.sort("MORTALITY_TPA", descending=True)
    
    return mortality_estimates

def display_results(results: pl.DataFrame):
    """Display mortality results in a formatted table."""
    
    console.print("\n[bold blue]Georgia Forest Mortality Analysis[/bold blue]")
    console.print("[dim]All values are annual mortality in cubic feet[/dim]\n")
    
    # Create summary table
    table = Table(title="Mortality by Ownership Group")
    table.add_column("Ownership", style="cyan")
    table.add_column("Mortality (ft³)", justify="right", style="green")
    table.add_column("Per Acre", justify="right")
    table.add_column("SE %", justify="right")
    table.add_column("Plots", justify="right")
    
    # Group by ownership
    by_owner = results.group_by("OWNERSHIP_GROUP").agg([
        pl.col("MORTALITY_TPA").sum().alias("mortality"),
        pl.col("MORTALITY_TPA_ACRE").mean().alias("per_acre"),
        pl.col("SE_PERCENT").mean().alias("se_pct"),
        pl.col("nPlots").sum().alias("plots")
    ])
    
    # Add rows to table
    for row in by_owner.iter_rows(named=True):
        table.add_row(
            row["OWNERSHIP_GROUP"],
            f"{row['mortality']:,.0f}",
            f"{row['per_acre']:,.2f}",
            f"{row['se_pct']:.2f}%",
            str(row["plots"])
        )
    
    console.print(table)

if __name__ == "__main__":
    import sys
    
    # Check if database path is provided
    db_path = sys.argv[1] if len(sys.argv) > 1 else "fia.duckdb"
    
    try:
        # Get mortality estimates
        results = get_mortality_estimates(db_path)
        
        # Display formatted results
        display_results(results)
        
        # Save detailed results to CSV
        output_path = "georgia_mortality_detailed.csv"
        results.write_csv(output_path)
        console.print(f"\nDetailed results saved to {output_path}")
        
    except Exception as e:
        console.print(f"\n[bold red]Error:[/bold red] {str(e)}")
        console.print("\n[yellow]Usage:[/yellow] python georgia_mortality_by_groups.py [path_to_fia_db]")
        console.print("\nIf no path is provided, the script will use 'fia.duckdb' in the current directory.")
        sys.exit(1)