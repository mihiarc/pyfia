"""
Demonstration of multi-backend support in pyFIA.

This example shows how to use pyFIA with both DuckDB and SQLite backends.
"""

from pathlib import Path

import polars as pl
from rich.console import Console
from rich.table import Table

from pyfia import FIA
from pyfia.core.data_reader import FIADataReader

console = Console()


def demo_backend_autodetection(db_path: Path):
    """Demonstrate automatic backend detection."""
    console.print("\n[bold blue]Backend Auto-detection Demo[/bold blue]")
    
    # Create reader with auto-detection
    reader = FIADataReader(db_path)  # Auto-detects backend
    
    # Read some data
    console.print(f"Database: {db_path.name}")
    console.print(f"Backend: {reader._backend.__class__.__name__}")
    
    # Read a sample of tree data
    trees = reader.read_table(
        "TREE",
        columns=["CN", "SPCD", "DIA", "STATUSCD"],
        where="STATUSCD = 1",
        limit=5
    )
    
    console.print(f"Sample live trees: {len(trees)} records")
    
    # Show data types (demonstrates backend-specific handling)
    table = Table(title="Column Types")
    table.add_column("Column")
    table.add_column("Type")
    
    for col, dtype in trees.schema.items():
        table.add_row(col, str(dtype))
    
    console.print(table)


def demo_explicit_backend(db_path: Path, engine: str):
    """Demonstrate explicit backend selection."""
    console.print(f"\n[bold blue]Explicit {engine.upper()} Backend Demo[/bold blue]")
    
    # Create reader with explicit backend
    reader = FIADataReader(db_path, engine=engine)
    
    # Demonstrate batch processing for large queries
    console.print("Testing batch processing with large IN clause...")
    
    # Get some plot CNs
    plots = reader.read_table("PLOT", columns=["CN"], limit=2000)
    plot_cns = plots["CN"].to_list()
    
    # Read filtered data (will use batching automatically)
    trees = reader.read_filtered_data("TREE", "PLT_CN", plot_cns[:1500])
    
    console.print(f"Trees from {len(plot_cns[:1500])} plots: {len(trees)} records")


def demo_fia_class_with_backend(db_path: Path, engine: str = None):
    """Demonstrate FIA class with backend support."""
    console.print(f"\n[bold blue]FIA Class Backend Demo[/bold blue]")
    
    # Create FIA instance (auto-detect or explicit engine)
    fia = FIA(db_path, engine=engine)
    
    # Find evaluations
    evalids = fia.find_evalid(most_recent=True)
    console.print(f"Found {len(evalids)} most recent evaluations")
    
    if evalids:
        # Use the first evaluation
        fia.clip_by_evalid(evalids[0])
        
        # Get some data
        plots = fia.get_plots(columns=["CN", "STATECD", "PLOT"])
        console.print(f"Plots in evaluation: {len(plots)}")
        
        # Show state distribution
        state_counts = plots.group_by("STATECD").agg(
            pl.count().alias("plot_count")
        ).sort("plot_count", descending=True)
        
        table = Table(title="Plots by State")
        table.add_column("State")
        table.add_column("Plot Count")
        
        for row in state_counts.head(5).iter_rows():
            table.add_row(str(row[0]), str(row[1]))
        
        console.print(table)


def demo_performance_options(db_path: Path):
    """Demonstrate backend-specific performance options."""
    console.print("\n[bold blue]Performance Options Demo[/bold blue]")
    
    # DuckDB with memory configuration
    if db_path.suffix.lower() in [".duckdb", ".ddb"]:
        console.print("Creating DuckDB reader with performance settings...")
        reader = FIADataReader(
            db_path,
            engine="duckdb",
            memory_limit="8GB",
            threads=4
        )
        console.print("✓ DuckDB configured with 8GB memory limit and 4 threads")
    
    # SQLite with timeout configuration
    elif db_path.suffix.lower() in [".db", ".sqlite"]:
        console.print("Creating SQLite reader with timeout settings...")
        reader = FIADataReader(
            db_path,
            engine="sqlite",
            timeout=60.0
        )
        console.print("✓ SQLite configured with 60 second timeout")


def main():
    """Run the demonstration."""
    # Example database paths (adjust to your setup)
    duckdb_path = Path("data/fia_georgia.duckdb")
    sqlite_path = Path("data/fia_georgia.db")
    
    console.print("[bold green]pyFIA Multi-Backend Demonstration[/bold green]")
    
    # Check which databases are available
    available_dbs = []
    if duckdb_path.exists():
        available_dbs.append(("DuckDB", duckdb_path))
    if sqlite_path.exists():
        available_dbs.append(("SQLite", sqlite_path))
    
    if not available_dbs:
        console.print("[red]No FIA databases found. Please adjust the paths in main()[/red]")
        return
    
    # Run demos for available databases
    for db_type, db_path in available_dbs:
        console.print(f"\n[yellow]{'=' * 60}[/yellow]")
        console.print(f"[yellow]Testing with {db_type} database: {db_path}[/yellow]")
        console.print(f"[yellow]{'=' * 60}[/yellow]")
        
        # Auto-detection demo
        demo_backend_autodetection(db_path)
        
        # Explicit backend demo
        engine = "duckdb" if db_type == "DuckDB" else "sqlite"
        demo_explicit_backend(db_path, engine)
        
        # FIA class demo
        demo_fia_class_with_backend(db_path)
        
        # Performance options demo
        demo_performance_options(db_path)


if __name__ == "__main__":
    main()