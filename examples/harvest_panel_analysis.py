"""
Example script demonstrating harvest panel analysis using pyFIA.

This script shows how to use the panel() function to:
1. Create condition-level panels for harvest probability analysis
2. Create tree-level panels for individual tree fate tracking
3. Analyze harvest rates by ownership and species
4. Explore multi-period remeasurement chains

Requirements:
    - pyFIA installed
    - FIA database (e.g., downloaded via pyfia.download())
"""

from pathlib import Path

import polars as pl
from rich.console import Console
from rich.table import Table

from pyfia import FIA, panel

console = Console()


def analyze_condition_harvest(db: FIA) -> None:
    """Analyze harvest rates at the condition level."""
    console.print("\n[bold blue]CONDITION-LEVEL HARVEST ANALYSIS[/bold blue]")
    console.print("-" * 50)

    # Create condition panel for forest land
    cond_panel = panel(db, level="condition", land_type="forest")

    # Basic statistics
    n_conditions = len(cond_panel)
    n_harvested = cond_panel["HARVEST"].sum()
    harvest_rate = cond_panel["HARVEST"].mean()
    avg_remper = cond_panel["REMPER"].mean()

    # Calculate annualized rate
    annual_rate = 1 - (1 - harvest_rate) ** (1 / avg_remper)

    console.print(f"Total condition pairs: {n_conditions:,}")
    console.print(f"Harvested conditions: {n_harvested:,}")
    console.print(f"Period harvest rate: {harvest_rate:.1%}")
    console.print(f"Avg remeasurement period: {avg_remper:.1f} years")
    console.print(f"[green]Annualized harvest rate: {annual_rate:.2%}/year[/green]")

    # Harvest by ownership
    console.print("\n[bold]Harvest Rate by Ownership:[/bold]")
    own_names = {10: "USFS", 20: "Other Federal", 30: "State/Local", 40: "Private"}

    harvest_by_owner = (
        cond_panel.group_by("t2_OWNGRPCD")
        .agg([pl.len().alias("n_conditions"), pl.col("HARVEST").mean().alias("harvest_rate")])
        .sort("t2_OWNGRPCD")
    )

    table = Table(show_header=True, header_style="bold")
    table.add_column("Ownership")
    table.add_column("N Conditions", justify="right")
    table.add_column("Harvest Rate", justify="right")

    for row in harvest_by_owner.iter_rows(named=True):
        name = own_names.get(row["t2_OWNGRPCD"], f"Code {row['t2_OWNGRPCD']}")
        table.add_row(name, f"{row['n_conditions']:,}", f"{row['harvest_rate']:.1%}")

    console.print(table)


def analyze_tree_fates(db: FIA) -> None:
    """Analyze tree fates including cut trees."""
    console.print("\n[bold blue]TREE-LEVEL FATE ANALYSIS[/bold blue]")
    console.print("-" * 50)

    # Create tree panel (infer_cut=True by default)
    tree_panel = panel(db, level="tree", tree_type="all")

    console.print(f"Total tree pairs: {len(tree_panel):,}")

    # Tree fate distribution
    console.print("\n[bold]Tree Fate Distribution:[/bold]")
    fate_dist = (
        tree_panel.group_by("TREE_FATE")
        .agg(pl.len().alias("n"))
        .with_columns((pl.col("n") / pl.col("n").sum() * 100).alias("pct"))
        .sort("n", descending=True)
    )

    table = Table(show_header=True, header_style="bold")
    table.add_column("Fate")
    table.add_column("Count", justify="right")
    table.add_column("Percent", justify="right")

    for row in fate_dist.iter_rows(named=True):
        table.add_row(row["TREE_FATE"], f"{row['n']:,}", f"{row['pct']:.1f}%")

    console.print(table)

    # Cut trees analysis
    cut_trees = tree_panel.filter(pl.col("TREE_FATE") == "cut")
    if len(cut_trees) > 0:
        console.print(f"\n[bold]Top 10 Species Cut ({len(cut_trees):,} total):[/bold]")

        spcd_names = {
            131: "Loblolly pine",
            132: "Longleaf pine",
            110: "Shortleaf pine",
            111: "Slash pine",
            611: "Sweetgum",
            621: "Yellow-poplar",
            802: "White oak",
            833: "N. red oak",
            316: "Red maple",
            261: "E. white pine",
        }

        species_cut = (
            cut_trees.filter(pl.col("t1_SPCD").is_not_null())
            .group_by("t1_SPCD")
            .agg([pl.len().alias("n_trees"), pl.col("t1_DIA").mean().alias("avg_dia")])
            .sort("n_trees", descending=True)
            .head(10)
        )

        table = Table(show_header=True, header_style="bold")
        table.add_column("Species")
        table.add_column("Trees Cut", justify="right")
        table.add_column("Avg DBH", justify="right")

        for row in species_cut.iter_rows(named=True):
            spcd = int(row["t1_SPCD"])
            name = spcd_names.get(spcd, f"SPCD {spcd}")
            table.add_row(name, f"{row['n_trees']:,}", f"{row['avg_dia']:.1f}\"")

        console.print(table)


def analyze_remeasurement_chains(db: FIA) -> None:
    """Analyze multi-period remeasurement chains."""
    console.print("\n[bold blue]REMEASUREMENT CHAIN ANALYSIS[/bold blue]")
    console.print("-" * 50)

    cond_panel = panel(db, level="condition")

    # Count plots by number of measurement periods
    chain_lengths = (
        cond_panel.group_by("PLT_CN")
        .agg(pl.len().alias("periods"))
        .group_by("periods")
        .agg(pl.len().alias("n_plots"))
        .sort("periods")
    )

    console.print("[bold]Plots by Number of Measurement Periods:[/bold]")
    table = Table(show_header=True, header_style="bold")
    table.add_column("Periods")
    table.add_column("N Plots", justify="right")

    for row in chain_lengths.iter_rows(named=True):
        table.add_row(str(row["periods"]), f"{row['n_plots']:,}")

    console.print(table)

    # Harvest transition analysis for multi-period plots
    multi_period_plots = (
        cond_panel.group_by("PLT_CN").agg(pl.len().alias("n")).filter(pl.col("n") > 1)["PLT_CN"]
    )

    multi_period = cond_panel.filter(pl.col("PLT_CN").is_in(multi_period_plots)).sort(
        ["PLT_CN", "INVYR"]
    )

    if len(multi_period) > 0:
        console.print("\n[bold]Harvest Transitions (plots with 2+ periods):[/bold]")

        transitions = (
            multi_period.with_columns(
                [pl.col("HARVEST").shift(1).over("PLT_CN").alias("PREV_HARVEST")]
            )
            .filter(pl.col("PREV_HARVEST").is_not_null())
            .group_by(["PREV_HARVEST", "HARVEST"])
            .agg(pl.len().alias("count"))
            .sort(["PREV_HARVEST", "HARVEST"])
        )

        table = Table(show_header=True, header_style="bold")
        table.add_column("Previous Period")
        table.add_column("Current Period")
        table.add_column("Count", justify="right")

        for row in transitions.iter_rows(named=True):
            prev = "Harvested" if row["PREV_HARVEST"] == 1 else "Not harvested"
            curr = "Harvested" if row["HARVEST"] == 1 else "Not harvested"
            table.add_row(prev, curr, f"{row['count']:,}")

        console.print(table)


def main(db_path: str = "data/nc.duckdb", state_code: int = 37):
    """
    Run harvest panel analysis.

    Args:
        db_path: Path to FIA DuckDB database
        state_code: FIPS state code (default 37 = North Carolina)
    """
    console.print(f"[bold green]Harvest Panel Analysis[/bold green]")
    console.print(f"Database: {db_path}")
    console.print(f"State code: {state_code}")

    # Check if database exists
    if not Path(db_path).exists():
        console.print(f"[red]Database not found: {db_path}[/red]")
        console.print("Download data first with: pyfia.download(states='NC', dir='data/')")
        return

    with FIA(db_path) as db:
        db.clip_by_state(state_code)

        # Run analyses
        analyze_condition_harvest(db)
        analyze_tree_fates(db)
        analyze_remeasurement_chains(db)

    console.print("\n[bold green]Analysis complete![/bold green]")


if __name__ == "__main__":
    import sys

    # Allow command-line arguments for database path and state
    db_path = sys.argv[1] if len(sys.argv) > 1 else "data/nc.duckdb"
    state_code = int(sys.argv[2]) if len(sys.argv) > 2 else 37

    main(db_path, state_code)
