#!/usr/bin/env python3
"""
Example: Mortality estimates grouped by cause of death (AGENTCD).

This example demonstrates the new AGENTCD and DSTRBCD grouping capabilities
in the mortality() function, enabling timber casualty loss analysis.

Use Case: Forest landowners need to classify mortality by cause for
federal income tax purposes:
- Casualty Loss (tax-deductible): Fire, hurricanes/wind
- Non-Casualty Loss: Insects, disease, drought
- Non-Deductible: Animal damage, vegetation competition

Usage:
    # With MotherDuck
    uv run python examples/mortality_by_cause.py --motherduck fia_va

    # With local DuckDB
    uv run python examples/mortality_by_cause.py --duckdb data/virginia.duckdb
"""

import argparse
from rich.console import Console
from rich.table import Table

console = Console()


# AGENTCD code descriptions
AGENTCD_NAMES = {
    0: "No agent recorded",
    10: "Insect",
    20: "Disease",
    30: "Fire",
    40: "Animal",
    50: "Weather",
    60: "Vegetation",
    70: "Unknown",
    80: "Silvicultural",
}

# Tax classification by AGENTCD
TAX_CLASSIFICATION = {
    30: "Casualty",      # Fire - sudden event
    50: "Casualty",      # Weather - sudden event
    10: "Non-Casualty",  # Insect - gradual
    20: "Non-Casualty",  # Disease - gradual
    40: "Non-Deductible",  # Animal
    60: "Non-Deductible",  # Vegetation
    80: "Non-Deductible",  # Silvicultural
    0: "Unknown",
    70: "Unknown",
}


def run_mortality_by_agentcd(db):
    """Run mortality estimates grouped by AGENTCD."""
    from pyfia import mortality

    console.print("\n[bold]Mortality by Cause of Death (AGENTCD)[/bold]")
    console.print("=" * 60)

    # Run mortality with AGENTCD grouping
    result = mortality(
        db,
        grp_by="AGENTCD",
        measure="volume",
        tree_type="gs",
        land_type="forest",
        variance=True,
    )

    # Display results
    table = Table(title="Annual Mortality Volume by Cause")
    table.add_column("AGENTCD", justify="right")
    table.add_column("Cause", justify="left")
    table.add_column("Tax Class", justify="left")
    table.add_column("Volume (cuft/yr)", justify="right")
    table.add_column("SE", justify="right")

    for row in result.iter_rows(named=True):
        agentcd = row.get("AGENTCD", 0) or 0
        cause = AGENTCD_NAMES.get(agentcd, f"Code {agentcd}")
        tax_class = TAX_CLASSIFICATION.get(agentcd, "Unknown")
        volume = row.get("MORT_TOTAL", 0) or 0
        se = row.get("MORT_TOTAL_SE", 0) or 0

        table.add_row(
            str(agentcd),
            cause,
            tax_class,
            f"{volume:,.0f}",
            f"{se:,.0f}",
        )

    console.print(table)

    # Summary by tax classification
    console.print("\n[bold]Summary by Tax Classification[/bold]")

    casualty_total = 0
    non_casualty_total = 0
    non_deductible_total = 0

    for row in result.iter_rows(named=True):
        agentcd = row.get("AGENTCD", 0) or 0
        volume = row.get("MORT_TOTAL", 0) or 0
        tax_class = TAX_CLASSIFICATION.get(agentcd, "Unknown")

        if tax_class == "Casualty":
            casualty_total += volume
        elif tax_class == "Non-Casualty":
            non_casualty_total += volume
        elif tax_class == "Non-Deductible":
            non_deductible_total += volume

    console.print(f"  Casualty (tax-deductible):     {casualty_total:>15,.0f} cuft/yr")
    console.print(f"  Non-Casualty (limited deduct): {non_casualty_total:>15,.0f} cuft/yr")
    console.print(f"  Non-Deductible:                {non_deductible_total:>15,.0f} cuft/yr")

    return result


def run_mortality_by_dstrbcd(db):
    """Run mortality estimates grouped by DSTRBCD1 (disturbance code)."""
    from pyfia import mortality

    console.print("\n[bold]Mortality by Disturbance Type (DSTRBCD1)[/bold]")
    console.print("=" * 60)

    # Run mortality with DSTRBCD1 grouping
    result = mortality(
        db,
        grp_by="DSTRBCD1",
        measure="volume",
        tree_type="gs",
        land_type="forest",
        variance=True,
    )

    # Display results
    table = Table(title="Annual Mortality Volume by Disturbance")
    table.add_column("DSTRBCD1", justify="right")
    table.add_column("Volume (cuft/yr)", justify="right")
    table.add_column("SE", justify="right")

    for row in result.iter_rows(named=True):
        dstrbcd = row.get("DSTRBCD1", 0) or 0
        volume = row.get("MORT_TOTAL", 0) or 0
        se = row.get("MORT_TOTAL_SE", 0) or 0

        table.add_row(
            str(dstrbcd),
            f"{volume:,.0f}",
            f"{se:,.0f}",
        )

    console.print(table)

    return result


def main():
    parser = argparse.ArgumentParser(
        description="Demonstrate mortality grouping by cause of death"
    )
    parser.add_argument(
        "--motherduck", "-m",
        help="MotherDuck database name (e.g., fia_va)",
    )
    parser.add_argument(
        "--duckdb", "-d",
        help="Path to local DuckDB file",
    )
    parser.add_argument(
        "--evalid",
        type=int,
        help="Optional EVALID to filter to",
    )

    args = parser.parse_args()

    if not args.motherduck and not args.duckdb:
        console.print("[red]Error: Must specify either --motherduck or --duckdb[/red]")
        parser.print_help()
        return

    # Connect to database
    if args.motherduck:
        from pyfia import MotherDuckFIA
        console.print(f"[cyan]Connecting to MotherDuck: {args.motherduck}[/cyan]")
        db = MotherDuckFIA(args.motherduck)
    else:
        from pyfia import FIA
        console.print(f"[cyan]Connecting to DuckDB: {args.duckdb}[/cyan]")
        db = FIA(args.duckdb)

    with db:
        if args.evalid:
            console.print(f"[cyan]Filtering to EVALID: {args.evalid}[/cyan]")
            db.clip_by_evalid(args.evalid)

        # Run mortality by AGENTCD
        run_mortality_by_agentcd(db)

        # Run mortality by DSTRBCD1
        run_mortality_by_dstrbcd(db)

    console.print("\n[green]Done![/green]")


if __name__ == "__main__":
    main()
