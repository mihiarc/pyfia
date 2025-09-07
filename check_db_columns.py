#!/usr/bin/env python
"""Check available columns in the nfi_south.duckdb database."""

import duckdb
from rich.console import Console
from rich.table import Table

console = Console()

def main():
    db_path = "nfi_south.duckdb"
    
    console.print(f"\n[bold cyan]Checking columns in {db_path}[/bold cyan]")
    console.print("=" * 60)
    
    try:
        with duckdb.connect(db_path, read_only=True) as conn:
            # Check TREE table columns
            tree_cols = conn.execute("PRAGMA table_info('TREE')").fetchall()
            
            console.print("\n[bold yellow]TREE table columns:[/bold yellow]")
            
            # Look for volume-related columns
            vol_cols = []
            all_cols = []
            for col in tree_cols:
                col_name = col[1]  # Column name is second element
                all_cols.append(col_name)
                if 'VOL' in col_name.upper():
                    vol_cols.append(col_name)
            
            if vol_cols:
                console.print(f"[green]Found {len(vol_cols)} volume columns:[/green]")
                for col in vol_cols:
                    console.print(f"  - {col}")
            else:
                console.print("[red]No volume columns found![/red]")
                console.print("\n[yellow]Available columns (first 30):[/yellow]")
                for col in all_cols[:30]:
                    console.print(f"  - {col}")
            
            # Check if we have the required tables
            console.print("\n[bold yellow]Checking required tables:[/bold yellow]")
            required_tables = ["PLOT", "TREE", "COND", "POP_STRATUM", "POP_PLOT_STRATUM_ASSGN"]
            
            for table in required_tables:
                try:
                    count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                    console.print(f"  [green]✓[/green] {table}: {count:,} rows")
                except:
                    console.print(f"  [red]✗[/red] {table}: Not found")
            
            # Check Texas data specifically
            console.print("\n[bold yellow]Texas data summary:[/bold yellow]")
            tx_plots = conn.execute("SELECT COUNT(DISTINCT CN) FROM PLOT WHERE STATECD = 48").fetchone()[0]
            tx_trees = conn.execute("SELECT COUNT(*) FROM TREE WHERE PLT_CN IN (SELECT CN FROM PLOT WHERE STATECD = 48)").fetchone()[0]
            console.print(f"  Texas plots: {tx_plots:,}")
            console.print(f"  Texas trees: {tx_trees:,}")
            
            # Check what evaluation types we have
            console.print("\n[bold yellow]Available evaluation types:[/bold yellow]")
            try:
                eval_types = conn.execute("""
                    SELECT DISTINCT 
                        pe.EVAL_TYP,
                        COUNT(DISTINCT pp.PLT_CN) as plot_count
                    FROM POP_EVAL pe
                    JOIN POP_PLOT_STRATUM_ASSGN pp ON pe.CN = pp.EVAL_CN
                    JOIN PLOT p ON pp.PLT_CN = p.CN
                    WHERE p.STATECD = 48
                    GROUP BY pe.EVAL_TYP
                """).fetchall()
                
                for eval_typ, count in eval_types:
                    console.print(f"  {eval_typ}: {count:,} plots")
            except Exception as e:
                console.print(f"  [red]Could not check evaluation types: {e}[/red]")
                
    except Exception as e:
        console.print(f"\n[red]Error:[/red] {str(e)}")

if __name__ == "__main__":
    main()