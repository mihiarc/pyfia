"""
pyFIA benchmark tests.

Benchmarks pyFIA estimation functions for comparison against rFIA and EVALIDator.
"""

from pathlib import Path
from typing import List, Optional

from rich.console import Console

from benchmarks.comparison.timing import BenchmarkSuite, TimingResult, benchmark_function

console = Console()


def run_pyfia_benchmarks(
    db_path: Path,
    state_fips: int,
    iterations: int = 10,
) -> List[TimingResult]:
    """
    Run comprehensive pyFIA benchmarks.

    Parameters
    ----------
    db_path : Path
        Path to DuckDB database
    state_fips : int
        State FIPS code for the database
    iterations : int
        Number of benchmark iterations

    Returns
    -------
    List[TimingResult]
        List of benchmark results
    """
    from pyfia import FIA, area, biomass, mortality, tpa, volume

    results = []

    console.print(f"[bold]Running pyFIA benchmarks on {db_path}[/bold]")
    console.print(f"  State FIPS: {state_fips}")
    console.print(f"  Iterations: {iterations}")

    # Open database using context manager
    with FIA(db_path) as db:
        db.clip_most_recent()

        # ==========================================================================
        # Benchmark 1: Basic Area Estimation
        # ==========================================================================
        console.print("\n  [cyan]1. Area estimation (total forest)[/cyan]")

        def bench_area_total():
            return area(db)

        results.append(
            benchmark_function(bench_area_total, "area_total", "pyfia", iterations=iterations)
        )

        # ==========================================================================
        # Benchmark 2: Area by Ownership
        # ==========================================================================
        console.print("  [cyan]2. Area by ownership[/cyan]")

        def bench_area_by_ownership():
            return area(db, grp_by="OWNGRPCD")

        results.append(
            benchmark_function(
                bench_area_by_ownership, "area_by_ownership", "pyfia", iterations=iterations
            )
        )

        # ==========================================================================
        # Benchmark 3: Total Volume
        # ==========================================================================
        console.print("  [cyan]3. Volume estimation (total)[/cyan]")

        def bench_volume_total():
            return volume(db)

        results.append(
            benchmark_function(bench_volume_total, "volume_total", "pyfia", iterations=iterations)
        )

        # ==========================================================================
        # Benchmark 4: Volume by Species
        # ==========================================================================
        console.print("  [cyan]4. Volume by species[/cyan]")

        def bench_volume_by_species():
            return volume(db, by_species=True)

        results.append(
            benchmark_function(
                bench_volume_by_species, "volume_by_species", "pyfia", iterations=iterations
            )
        )

        # ==========================================================================
        # Benchmark 5: Volume by Size Class
        # ==========================================================================
        console.print("  [cyan]5. Volume by size class[/cyan]")

        def bench_volume_by_sizeclass():
            return volume(db, by_size_class=True)

        results.append(
            benchmark_function(
                bench_volume_by_sizeclass, "volume_by_sizeclass", "pyfia", iterations=iterations
            )
        )

        # ==========================================================================
        # Benchmark 6: TPA Total
        # ==========================================================================
        console.print("  [cyan]6. TPA estimation (total)[/cyan]")

        def bench_tpa_total():
            return tpa(db)

        results.append(
            benchmark_function(bench_tpa_total, "tpa_total", "pyfia", iterations=iterations)
        )

        # ==========================================================================
        # Benchmark 7: TPA by Species
        # ==========================================================================
        console.print("  [cyan]7. TPA by species[/cyan]")

        def bench_tpa_by_species():
            return tpa(db, by_species=True)

        results.append(
            benchmark_function(
                bench_tpa_by_species, "tpa_by_species", "pyfia", iterations=iterations
            )
        )

        # ==========================================================================
        # Benchmark 8: Biomass Total
        # ==========================================================================
        console.print("  [cyan]8. Biomass estimation (total)[/cyan]")

        def bench_biomass_total():
            return biomass(db)

        results.append(
            benchmark_function(bench_biomass_total, "biomass_total", "pyfia", iterations=iterations)
        )

        # ==========================================================================
        # Benchmark 9: Mortality Total
        # ==========================================================================
        console.print("  [cyan]9. Mortality estimation (total)[/cyan]")

        def bench_mortality_total():
            return mortality(db)

        results.append(
            benchmark_function(
                bench_mortality_total, "mortality_total", "pyfia", iterations=iterations
            )
        )

        # ==========================================================================
        # Benchmark 10: Mortality by Species
        # ==========================================================================
        console.print("  [cyan]10. Mortality by species[/cyan]")

        def bench_mortality_by_species():
            return mortality(db, by_species=True)

        results.append(
            benchmark_function(
                bench_mortality_by_species, "mortality_by_species", "pyfia", iterations=iterations
            )
        )

        # ==========================================================================
        # Benchmark 11: Complex Query - Volume by Species, Size Class, and Ownership
        # ==========================================================================
        console.print("  [cyan]11. Complex: Volume by species + size class + ownership[/cyan]")

        def bench_volume_complex():
            return volume(db, by_species=True, by_size_class=True, grp_by="OWNGRPCD")

        results.append(
            benchmark_function(
                bench_volume_complex, "volume_complex", "pyfia", iterations=iterations
            )
        )

        # ==========================================================================
        # Benchmark 12: Timberland Only (filtered)
        # ==========================================================================
        console.print("  [cyan]12. Volume (timberland only)[/cyan]")

        def bench_volume_timberland():
            return volume(db, land_type="timber")

        results.append(
            benchmark_function(
                bench_volume_timberland, "volume_timberland", "pyfia", iterations=iterations
            )
        )

    return results


def create_pyfia_suite(
    db_path: Path,
    state_fips: int,
    iterations: int = 10,
) -> BenchmarkSuite:
    """Create a benchmark suite for pyFIA."""
    results = run_pyfia_benchmarks(db_path, state_fips, iterations)

    suite = BenchmarkSuite(
        name="pyFIA Benchmarks",
        description=f"Performance benchmarks for pyFIA on state {state_fips}",
    )
    for r in results:
        suite.add_result(r)

    return suite


if __name__ == "__main__":
    import sys

    from rich.table import Table

    # Default to Rhode Island for testing
    if len(sys.argv) > 1:
        db_path = Path(sys.argv[1])
    else:
        # Try to find a database
        default_paths = [
            Path("data/ri/ri.duckdb"),
            Path("data/de/de.duckdb"),
            Path("~/.pyfia/data/ri/ri.duckdb").expanduser(),
        ]
        db_path = None
        for p in default_paths:
            if p.exists():
                db_path = p
                break

        if db_path is None:
            console.print("[red]No database found. Please provide a path or download data.[/red]")
            console.print("Usage: python -m benchmarks.comparison.bench_pyfia <db_path>")
            console.print("\nTo download data:")
            console.print("  from pyfia import download")
            console.print("  download('RI')  # or any state")
            sys.exit(1)

    suite = create_pyfia_suite(db_path, state_fips=44, iterations=10)  # 44 = Rhode Island

    # Print results
    table = Table(title="pyFIA Benchmark Results")
    table.add_column("Benchmark", style="cyan")
    table.add_column("Mean (ms)", justify="right")
    table.add_column("Std Dev", justify="right")
    table.add_column("Min", justify="right")
    table.add_column("Cold Start", justify="right")

    for r in suite.results:
        if r.error:
            table.add_row(r.name, "[red]ERROR[/red]", "-", "-", "-")
        else:
            table.add_row(
                r.name,
                f"{r.mean_ms:.2f}",
                f"{r.std_ms:.2f}",
                f"{r.min_ms:.2f}",
                f"{r.cold_start_ms:.2f}" if r.cold_start_ms else "-",
            )

    console.print(table)
