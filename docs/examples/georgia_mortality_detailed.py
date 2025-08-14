"""
Calculate detailed mortality estimates for Georgia using pyFIA's new implementation.
This script replicates the functionality of mortality_with_variance_detailed_groups.sql.
"""

from pathlib import Path
import polars as pl
from rich.console import Console
from rich.table import Table
import numpy as np
from decimal import Decimal, getcontext

from pyfia.core.fia import FIA
from pyfia.estimation.mortality import mortality
from pyfia.estimation.config import MortalityConfig

# Set high precision for decimal calculations
getcontext().prec = 50

console = Console()

def format_number(x: str) -> str:
    """Format a number string to handle scientific notation."""
    try:
        # Convert scientific notation to decimal string
        d = Decimal(x)
        return f"{d:f}"
    except:
        return x

def calculate_detailed_mortality(db_path: str = "fia.duckdb"):
    """Calculate detailed mortality estimates for Georgia."""
    
    # Initialize FIA database and filter to Georgia
    db = FIA(db_path)
    db.clip_by_state(state=13, most_recent=True)  # 13 is Georgia's FIPS code
    
    # Configure mortality estimation
    config = MortalityConfig(
        # Grouping options
        grp_by=["SPCD", "SPGRPCD", "OWNGRPCD", "UNITCD"],
        group_by_agent=True,
        group_by_disturbance=True,
        
        # Mortality calculation options
        mortality_type="both",  # Calculate both TPA and volume
        tree_type="all",       # Include all trees (required for mortality)
        tree_class="all",      # Include all tree classes
        land_type="forest",    # Forest land only
        
        # Output options
        include_components=True,  # Include basal area and volume
        totals=True,             # Include total estimates
        variance=True,           # Include variance statistics
        
        # Variance calculation
        variance_method="ratio"  # Use ratio estimation for variance
    )
    
    # Calculate mortality
    results = mortality(db, config)
    
    # Convert numeric columns to decimal strings first to preserve precision
    numeric_cols = [
        col for col in results.columns 
        if any(x in col.lower() for x in ["mortality", "se", "var", "total"])
    ]
    
    # Convert to decimal strings with high precision
    results = results.with_columns([
        pl.col(col).map_elements(lambda x: format_number(str(x)) if x is not None else None)
        for col in numeric_cols
    ])
    
    # Cast integer columns
    integer_cols = ["SPCD", "SPGRPCD", "OWNGRPCD", "UNITCD", "AGENTCD", 
                   "DSTRBCD1", "DSTRBCD2", "DSTRBCD3", "N_PLOTS"]
    results = results.with_columns([
        pl.col(col).cast(pl.Int64, strict=False) 
        for col in integer_cols 
        if col in results.columns
    ])
    
    return results

def display_results(results: pl.DataFrame):
    """Display mortality results in formatted tables."""
    
    console.print("\n[bold blue]Georgia Forest Mortality Analysis[/bold blue]")
    console.print("[dim]All values are annual mortality[/dim]\n")
    
    # 1. Overall Summary
    console.print("[bold green]1. Overall Mortality Summary[/bold green]")
    total_mortality = format_number(str(results["MORTALITY_VOL_TOTAL"].sum()))
    avg_se = format_number(str(results["MORTALITY_VOL_SE"].mean()))
    total_plots = results["N_PLOTS"].max()
    
    summary = Table(title="Summary Statistics")
    summary.add_column("Metric", style="cyan")
    summary.add_column("Value", justify="right")
    
    summary.add_row("Total Mortality (ft³)", total_mortality)
    summary.add_row("Average SE", avg_se)
    summary.add_row("Total Plots", str(total_plots))
    
    console.print(summary)
    
    # 2. Mortality by Ownership
    console.print("\n[bold green]2. Mortality by Ownership Group[/bold green]")
    
    by_owner = results.group_by("OWNGRPCD").agg([
        pl.col("MORTALITY_VOL_TOTAL").map_elements(lambda x: Decimal(x)).sum().alias("total_mortality"),
        pl.col("MORTALITY_VOL_SE").map_elements(lambda x: Decimal(x)).mean().alias("avg_se"),
        pl.col("N_PLOTS").max().alias("plots")
    ]).sort("total_mortality", descending=True)
    
    owner_table = Table(title="Mortality by Ownership")
    owner_table.add_column("Owner Code", style="cyan")
    owner_table.add_column("Mortality (ft³)", justify="right")
    owner_table.add_column("SE", justify="right")
    owner_table.add_column("Plots", justify="right")
    
    for row in by_owner.iter_rows(named=True):
        owner_table.add_row(
            str(row["OWNGRPCD"]),
            format_number(str(row["total_mortality"])),
            format_number(str(row["avg_se"])),
            str(row["plots"])
        )
    
    console.print(owner_table)
    
    # 3. Top Species
    console.print("\n[bold green]3. Top 10 Species by Mortality[/bold green]")
    
    by_species = results.group_by(["SPCD", "SPGRPCD"]).agg([
        pl.col("MORTALITY_VOL_TOTAL").map_elements(lambda x: Decimal(x)).sum().alias("total_mortality"),
        pl.col("MORTALITY_VOL_SE").map_elements(lambda x: Decimal(x)).mean().alias("avg_se")
    ]).sort("total_mortality", descending=True).head(10)
    
    species_table = Table(title="Top Species Mortality")
    species_table.add_column("Species", style="cyan")
    species_table.add_column("Group", justify="right")
    species_table.add_column("Mortality (ft³)", justify="right")
    species_table.add_column("SE", justify="right")
    
    for row in by_species.iter_rows(named=True):
        species_table.add_row(
            str(row["SPCD"]),
            str(row["SPGRPCD"]),
            format_number(str(row["total_mortality"])),
            format_number(str(row["avg_se"]))
        )
    
    console.print(species_table)
    
    # 4. Mortality by Agent
    console.print("\n[bold green]4. Mortality by Agent[/bold green]")
    
    by_agent = results.group_by("AGENTCD").agg([
        pl.col("MORTALITY_VOL_TOTAL").map_elements(lambda x: Decimal(x)).sum().alias("total_mortality"),
        pl.col("MORTALITY_VOL_SE").map_elements(lambda x: Decimal(x)).mean().alias("avg_se")
    ]).sort("total_mortality", descending=True)
    
    agent_table = Table(title="Mortality by Agent")
    agent_table.add_column("Agent Code", style="cyan")
    agent_table.add_column("Mortality (ft³)", justify="right")
    agent_table.add_column("SE", justify="right")
    
    for row in by_agent.iter_rows(named=True):
        if row["AGENTCD"] is not None:  # Skip null agents
            agent_table.add_row(
                str(row["AGENTCD"]),
                format_number(str(row["total_mortality"])),
                format_number(str(row["avg_se"]))
            )
    
    console.print(agent_table)

if __name__ == "__main__":
    import sys
    
    # Check if database path is provided
    db_path = sys.argv[1] if len(sys.argv) > 1 else "fia.duckdb"
    
    try:
        # Calculate mortality estimates
        results = calculate_detailed_mortality(db_path)
        
        # Display formatted results
        display_results(results)
        
        # Save detailed results to CSV
        output_path = "georgia_mortality_detailed_new.csv"
        results.write_csv(output_path)
        console.print(f"\nDetailed results saved to {output_path}")
        
    except Exception as e:
        console.print(f"\n[bold red]Error:[/bold red] {str(e)}")
        console.print("\n[yellow]Usage:[/yellow] python georgia_mortality_detailed.py [path_to_fia_db]")
        console.print("\nIf no path is provided, the script will use 'fia.duckdb' in the current directory.")
        sys.exit(1)