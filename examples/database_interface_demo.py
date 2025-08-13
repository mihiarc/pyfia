"""
Demonstration of the flexible database interface for pyFIA.

This example shows how to use the new database interface layer that
supports both DuckDB and SQLite backends.
"""

from pathlib import Path

from rich.console import Console
from rich.table import Table

from pyfia.database import create_interface
from pyfia.database.enhanced_reader import EnhancedFIADataReader

console = Console()


def demo_basic_interface(db_path: Path):
    """Demonstrate basic database interface usage."""
    console.print("\n[bold blue]Basic Database Interface Demo[/bold blue]")

    # Create interface (auto-detects database type)
    interface = create_interface(db_path)

    with interface:
        # Check if TREE table exists
        if interface.table_exists("TREE"):
            console.print("✓ TREE table found")

            # Get table schema
            schema = interface.get_table_schema("TREE")
            console.print(f"  Columns: {len(schema)}")

            # Execute a simple query
            result = interface.execute_query(
                "SELECT COUNT(*) as total_trees FROM TREE WHERE STATUSCD = 1"
            )
            total_trees = result.data["total_trees"][0]
            console.print(f"  Live trees: {total_trees:,}")

            # Execute a parameterized query
            result = interface.execute_query(
                """
                SELECT STATECD, COUNT(*) as tree_count 
                FROM TREE 
                WHERE STATUSCD = :status AND DIA >= :min_dia
                GROUP BY STATECD
                ORDER BY tree_count DESC
                LIMIT 5
                """,
                params={"status": 1, "min_dia": 10.0},
            )

            # Display results in a table
            table = Table(title="Top 5 States by Large Tree Count (DIA ≥ 10)")
            table.add_column("State Code", style="cyan")
            table.add_column("Tree Count", style="green")

            for row in result.data.iter_rows():
                table.add_row(str(row[0]), f"{row[1]:,}")

            console.print(table)


def demo_enhanced_reader(db_path: Path):
    """Demonstrate the enhanced FIA data reader."""
    console.print("\n[bold blue]Enhanced FIA Data Reader Demo[/bold blue]")

    with EnhancedFIADataReader(db_path) as reader:
        # Get database information
        tables = reader.list_tables()
        console.print(f"Database contains {len(tables)} tables")

        # Read PLOT table with filtering
        plots = reader.read_table(
            "PLOT",
            columns=["CN", "STATECD", "COUNTYCD", "PLOT", "LAT", "LON"],
            where="STATECD = 37",  # North Carolina
            lazy=False,
        )
        console.print(f"North Carolina plots: {len(plots):,}")

        # Get table information
        tree_info = reader.get_table_info("TREE")
        console.print(f"\nTREE table info:")
        console.print(f"  Rows: {tree_info['row_count']:,}")
        console.print(f"  Columns: {tree_info['column_count']}")

        # Demonstrate batch reading with large IN clause
        if plots.height > 0:
            plot_cns = plots["CN"].head(1000).to_list()
            trees = reader.read_filtered_data(
                "TREE",
                filter_column="PLT_CN",
                filter_values=plot_cns,
                columns=["CN", "PLT_CN", "SPCD", "DIA", "HT", "STATUSCD"],
            )
            console.print(f"Trees in first 1000 NC plots: {len(trees):,}")

        # Execute custom analysis query
        result = reader.execute_custom_query(
            """
            SELECT 
                SPCD,
                COUNT(*) as tree_count,
                ROUND(AVG(DIA), 2) as avg_dia,
                ROUND(AVG(HT), 1) as avg_ht
            FROM TREE
            WHERE STATUSCD = 1 AND DIA IS NOT NULL AND HT IS NOT NULL
            GROUP BY SPCD
            ORDER BY tree_count DESC
            LIMIT 10
            """
        )

        # Display species analysis
        table = Table(title="Top 10 Species by Tree Count")
        table.add_column("Species Code", style="cyan")
        table.add_column("Count", style="green")
        table.add_column("Avg DIA", style="yellow")
        table.add_column("Avg HT", style="magenta")

        for row in result.iter_rows():
            table.add_row(
                str(row[0]),
                f"{row[1]:,}",
                f"{row[2]:.2f}",
                f"{row[3]:.1f}",
            )

        console.print(table)


def demo_cross_backend_compatibility(sqlite_path: Path, duckdb_path: Path):
    """Demonstrate that both backends provide consistent results."""
    console.print("\n[bold blue]Cross-Backend Compatibility Demo[/bold blue]")

    query = """
    SELECT 
        STATECD,
        COUNT(*) as plot_count
    FROM PLOT
    GROUP BY STATECD
    ORDER BY STATECD
    LIMIT 5
    """

    # Run same query on both backends
    results = {}

    for path, backend in [(sqlite_path, "SQLite"), (duckdb_path, "DuckDB")]:
        if path.exists():
            interface = create_interface(path)
            with interface:
                result = interface.execute_query(query)
                results[backend] = result.data

    # Compare results
    if len(results) == 2:
        sqlite_data = results["SQLite"]
        duckdb_data = results["DuckDB"]

        table = Table(title="Backend Comparison - Plot Counts by State")
        table.add_column("State Code", style="cyan")
        table.add_column("SQLite Count", style="green")
        table.add_column("DuckDB Count", style="blue")
        table.add_column("Match", style="yellow")

        for i in range(min(len(sqlite_data), len(duckdb_data))):
            sqlite_row = sqlite_data.row(i)
            duckdb_row = duckdb_data.row(i)
            match = "✓" if sqlite_row == duckdb_row else "✗"

            table.add_row(
                str(sqlite_row[0]),
                f"{sqlite_row[1]:,}",
                f"{duckdb_row[1]:,}",
                match,
            )

        console.print(table)


def main():
    """Run the demonstration."""
    console.print("[bold green]pyFIA Database Interface Demonstration[/bold green]")

    # Example database paths (adjust as needed)
    sqlite_db = Path("./FIA_data.db")
    duckdb_db = Path("./FIA_data.duckdb")

    # Find an available database
    db_path = None
    if duckdb_db.exists():
        db_path = duckdb_db
        console.print(f"Using DuckDB database: {db_path}")
    elif sqlite_db.exists():
        db_path = sqlite_db
        console.print(f"Using SQLite database: {db_path}")
    else:
        console.print(
            "[red]No database found. Please provide a path to a FIA database.[/red]"
        )
        return

    # Run demonstrations
    demo_basic_interface(db_path)
    demo_enhanced_reader(db_path)

    # If both databases exist, show cross-backend compatibility
    if sqlite_db.exists() and duckdb_db.exists():
        demo_cross_backend_compatibility(sqlite_db, duckdb_db)


if __name__ == "__main__":
    main()