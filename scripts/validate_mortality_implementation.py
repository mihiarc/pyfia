"""
Validation script to compare pyFIA mortality implementation with SQL query results.

This script:
1. Runs the mortality estimation using pyFIA
2. Executes the equivalent SQL query
3. Compares the results for accuracy
"""

import sys
from pathlib import Path
import polars as pl
import duckdb
from rich.console import Console
from rich.table import Table

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from pyfia import FIA
from pyfia.estimation import mortality
from pyfia.estimation.config import MortalityConfig
from pyfia.estimation.mortality import MortalityEstimator

console = Console()


def load_sql_results(sql_file: Path) -> pl.DataFrame:
    """Load expected results from SQL query output."""
    if sql_file.suffix == '.csv':
        return pl.read_csv(sql_file)
    elif sql_file.suffix == '.parquet':
        return pl.read_parquet(sql_file)
    else:
        raise ValueError(f"Unsupported file format: {sql_file.suffix}")


def run_sql_query(db_path: str, query_file: Path) -> pl.DataFrame:
    """Execute SQL query directly against database."""
    conn = duckdb.connect(db_path)
    
    with open(query_file, 'r') as f:
        query = f.read()
        
    result = conn.execute(query).pl()
    conn.close()
    
    return result


def run_pyfia_mortality(db_path: str, config: dict) -> pl.DataFrame:
    """Run mortality estimation using pyFIA."""
    db = FIA(db_path)
    
    # Apply any filters
    if 'state_codes' in config:
        db.clip_by_state(config['state_codes'])
    
    if 'evalids' in config:
        db.clip_by_evalid(config['evalids'])
        
    # Build mortality config
    mortality_config = MortalityConfig(
        mortality_type=config.get('mortality_type', 'tpa'),
        by_species=config.get('by_species', False),
        group_by_ownership=config.get('group_by_ownership', False),
        group_by_agent=config.get('group_by_agent', False),
        group_by_disturbance=config.get('group_by_disturbance', False),
        tree_domain=config.get('tree_domain'),
        area_domain=config.get('area_domain'),
        variance=config.get('variance', True),
        totals=config.get('totals', False)
    )
    
    # Run estimation
    estimator = MortalityEstimator(db, mortality_config)
    return estimator.estimate()


def compare_results(pyfia_results: pl.DataFrame, sql_results: pl.DataFrame, 
                    tolerance: float = 0.01) -> dict:
    """Compare pyFIA results with SQL results."""
    comparison = {
        'status': 'PASS',
        'differences': [],
        'summary': {}
    }
    
    # Check row counts
    pyfia_rows = len(pyfia_results)
    sql_rows = len(sql_results)
    
    if pyfia_rows != sql_rows:
        comparison['status'] = 'FAIL'
        comparison['differences'].append(
            f"Row count mismatch: pyFIA={pyfia_rows}, SQL={sql_rows}"
        )
    
    # Find common columns
    pyfia_cols = set(pyfia_results.columns)
    sql_cols = set(sql_results.columns)
    common_cols = pyfia_cols & sql_cols
    
    # Compare numeric columns
    numeric_cols = [col for col in common_cols 
                   if pyfia_results[col].dtype in [pl.Float64, pl.Float32, pl.Int64, pl.Int32]]
    
    for col in numeric_cols:
        pyfia_values = pyfia_results[col].to_numpy()
        sql_values = sql_results[col].to_numpy()
        
        # Calculate relative differences
        rel_diff = abs(pyfia_values - sql_values) / (sql_values + 1e-10)
        max_diff = rel_diff.max()
        
        if max_diff > tolerance:
            comparison['status'] = 'FAIL'
            comparison['differences'].append(
                f"Column '{col}': max relative difference = {max_diff:.4f}"
            )
            
        comparison['summary'][col] = {
            'max_diff': float(max_diff),
            'mean_diff': float(rel_diff.mean()),
            'n_differences': int((rel_diff > tolerance).sum())
        }
    
    return comparison


def display_comparison_report(comparison: dict, pyfia_results: pl.DataFrame, 
                              sql_results: pl.DataFrame):
    """Display a detailed comparison report."""
    
    # Status header
    if comparison['status'] == 'PASS':
        console.print("[green]✓ Validation PASSED[/green]")
    else:
        console.print("[red]✗ Validation FAILED[/red]")
        
    # Differences
    if comparison['differences']:
        console.print("\n[bold]Differences found:[/bold]")
        for diff in comparison['differences']:
            console.print(f"  - {diff}")
    
    # Summary table
    console.print("\n[bold]Column Comparison Summary:[/bold]")
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Column")
    table.add_column("Max Diff")
    table.add_column("Mean Diff")
    table.add_column("# Differences")
    
    for col, stats in comparison['summary'].items():
        table.add_row(
            col,
            f"{stats['max_diff']:.6f}",
            f"{stats['mean_diff']:.6f}",
            str(stats['n_differences'])
        )
    
    console.print(table)
    
    # Sample data comparison
    console.print("\n[bold]Sample Data Comparison (first 5 rows):[/bold]")
    
    # Show pyFIA results
    console.print("\n[cyan]pyFIA Results:[/cyan]")
    console.print(pyfia_results.head())
    
    # Show SQL results
    console.print("\n[cyan]SQL Results:[/cyan]")
    console.print(sql_results.head())


def main():
    """Main validation workflow."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Validate pyFIA mortality implementation against SQL results"
    )
    parser.add_argument(
        "--db", 
        required=True,
        help="Path to FIA database"
    )
    parser.add_argument(
        "--sql-file",
        help="Path to SQL query file to execute"
    )
    parser.add_argument(
        "--sql-results",
        help="Path to pre-computed SQL results (CSV or Parquet)"
    )
    parser.add_argument(
        "--state-codes",
        nargs="+",
        type=int,
        help="State codes to filter"
    )
    parser.add_argument(
        "--by-species",
        action="store_true",
        help="Group by species"
    )
    parser.add_argument(
        "--by-ownership",
        action="store_true",
        help="Group by ownership"
    )
    parser.add_argument(
        "--by-agent",
        action="store_true",
        help="Group by mortality agent"
    )
    parser.add_argument(
        "--tolerance",
        type=float,
        default=0.01,
        help="Relative tolerance for numeric comparisons (default: 0.01)"
    )
    
    args = parser.parse_args()
    
    # Build configuration
    config = {
        'mortality_type': 'tpa',
        'by_species': args.by_species,
        'group_by_ownership': args.by_ownership,
        'group_by_agent': args.by_agent,
        'variance': True,
        'totals': False
    }
    
    if args.state_codes:
        config['state_codes'] = args.state_codes
    
    console.print("[bold]Running pyFIA Mortality Validation[/bold]\n")
    
    # Run pyFIA mortality
    console.print("Running pyFIA mortality estimation...")
    pyfia_results = run_pyfia_mortality(args.db, config)
    console.print(f"  Generated {len(pyfia_results)} rows")
    
    # Get SQL results
    if args.sql_results:
        console.print(f"\nLoading SQL results from {args.sql_results}...")
        sql_results = load_sql_results(Path(args.sql_results))
    elif args.sql_file:
        console.print(f"\nExecuting SQL query from {args.sql_file}...")
        sql_results = run_sql_query(args.db, Path(args.sql_file))
    else:
        console.print("[red]Error: Must provide either --sql-file or --sql-results[/red]")
        sys.exit(1)
        
    console.print(f"  Loaded {len(sql_results)} rows")
    
    # Compare results
    console.print("\nComparing results...")
    comparison = compare_results(pyfia_results, sql_results, args.tolerance)
    
    # Display report
    display_comparison_report(comparison, pyfia_results, sql_results)
    
    # Exit with appropriate code
    sys.exit(0 if comparison['status'] == 'PASS' else 1)


if __name__ == "__main__":
    main()