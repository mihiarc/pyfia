#!/usr/bin/env python
"""
Direct volume calculation example for Texas forestland.

This example demonstrates a simplified approach to volume estimation
that directly queries the database and performs the calculations.
"""

import duckdb
from rich.console import Console
from rich.table import Table

console = Console()

def main():
    """Calculate volume estimates using direct database queries."""
    
    db_path = "nfi_south.duckdb"
    
    console.print("\n[bold cyan]Direct Volume Estimation - Texas Forestland[/bold cyan]")
    console.print("=" * 60)
    
    try:
        with duckdb.connect(db_path, read_only=True) as conn:
            console.print(f"[green]✓[/green] Connected to: {db_path}")
            
            # Direct SQL query based on the example you provided
            # This calculates volume per acre using FIA expansion factors
            query = """
            SELECT 
                COUNT(DISTINCT plot.cn) as n_plots,
                SUM(CAST(tree.TPA_UNADJ AS DOUBLE) * 
                    CAST(tree.VOLCFNET AS DOUBLE) * 
                    CAST(pop_stratum.EXPNS AS DOUBLE) * 
                    CAST(CASE 
                        WHEN tree.DIA IS NULL THEN pop_stratum.ADJ_FACTOR_SUBP
                        WHEN tree.DIA < 5.0 THEN pop_stratum.ADJ_FACTOR_MICR
                        WHEN tree.DIA < COALESCE(plot.MACRO_BREAKPOINT_DIA, 9999) THEN pop_stratum.ADJ_FACTOR_SUBP
                        ELSE pop_stratum.ADJ_FACTOR_MACR
                    END AS DOUBLE)
                ) as total_volume,
                SUM(CAST(pop_stratum.EXPNS AS DOUBLE) * 
                    CAST(cond.CONDPROP_UNADJ AS DOUBLE) * 
                    CASE 
                        WHEN cond.COND_STATUS_CD = 1 THEN 1.0 
                        ELSE 0.0 
                    END
                ) as forest_area
            FROM pop_stratum
            JOIN pop_plot_stratum_assgn ON (pop_plot_stratum_assgn.STRATUM_CN = pop_stratum.CN)
            JOIN plot ON (pop_plot_stratum_assgn.PLT_CN = plot.CN)
            JOIN cond ON (cond.PLT_CN = plot.CN)
            JOIN tree ON (tree.PLT_CN = cond.PLT_CN AND tree.CONDID = cond.CONDID)
            WHERE 
                tree.STATUSCD = 1  -- Live trees
                AND cond.COND_STATUS_CD = 1  -- Forestland
                AND tree.TPA_UNADJ IS NOT NULL
                AND tree.VOLCFNET IS NOT NULL
                AND plot.STATECD = 48  -- Texas
                -- Use most recent evaluation
                AND pop_stratum.EVALID IN (
                    SELECT MAX(EVALID) 
                    FROM pop_stratum ps2 
                    WHERE ps2.STATECD = 48
                )
            """
            
            console.print("\n[yellow]Executing volume calculation query...[/yellow]")
            result = conn.execute(query).fetchone()
            
            if result:
                n_plots, total_volume, forest_area = result
                
                # Calculate per-acre value
                if forest_area and forest_area > 0:
                    volume_per_acre = total_volume / forest_area
                else:
                    volume_per_acre = 0
                
                # Display results
                console.print("\n[bold]Texas Forestland Volume Estimates:[/bold]")
                table = Table(show_header=True, header_style="bold magenta")
                table.add_column("Metric", style="cyan", width=30)
                table.add_column("Value", justify="right", style="white")
                table.add_column("Units", style="dim")
                
                table.add_row(
                    "Number of Plots",
                    f"{n_plots:,}",
                    "plots"
                )
                table.add_row(
                    "Total Net Volume",
                    f"{total_volume:,.0f}",
                    "cubic feet"
                )
                table.add_row(
                    "Forest Area (weighted)",
                    f"{forest_area:,.0f}",
                    "acres"
                )
                table.add_row(
                    "Volume per Acre",
                    f"{volume_per_acre:,.2f}",
                    "cubic feet/acre"
                )
                
                console.print(table)
                
                # Get volume by species (top 5)
                console.print("\n[yellow]Calculating volume by species...[/yellow]")
                
                species_query = """
                SELECT 
                    tree.SPCD,
                    COUNT(DISTINCT plot.cn) as n_plots,
                    SUM(CAST(tree.TPA_UNADJ AS DOUBLE) * 
                        CAST(tree.VOLCFNET AS DOUBLE) * 
                        CAST(pop_stratum.EXPNS AS DOUBLE) * 
                        CAST(CASE 
                            WHEN tree.DIA IS NULL THEN pop_stratum.ADJ_FACTOR_SUBP
                            WHEN tree.DIA < 5.0 THEN pop_stratum.ADJ_FACTOR_MICR
                            WHEN tree.DIA < COALESCE(plot.MACRO_BREAKPOINT_DIA, 9999) THEN pop_stratum.ADJ_FACTOR_SUBP
                            ELSE pop_stratum.ADJ_FACTOR_MACR
                        END AS DOUBLE)
                    ) as species_volume
                FROM pop_stratum
                JOIN pop_plot_stratum_assgn ON (pop_plot_stratum_assgn.STRATUM_CN = pop_stratum.CN)
                JOIN plot ON (pop_plot_stratum_assgn.PLT_CN = plot.CN)
                JOIN cond ON (cond.PLT_CN = plot.CN)
                JOIN tree ON (tree.PLT_CN = cond.PLT_CN AND tree.CONDID = cond.CONDID)
                WHERE 
                    tree.STATUSCD = 1
                    AND cond.COND_STATUS_CD = 1
                    AND tree.TPA_UNADJ IS NOT NULL
                    AND tree.VOLCFNET IS NOT NULL
                    AND plot.STATECD = 48
                    AND pop_stratum.EVALID IN (
                        SELECT MAX(EVALID) 
                        FROM pop_stratum ps2 
                        WHERE ps2.STATECD = 48
                    )
                GROUP BY tree.SPCD
                ORDER BY species_volume DESC
                LIMIT 5
                """
                
                species_results = conn.execute(species_query).fetchall()
                
                if species_results:
                    console.print("\n[bold]Top 5 Species by Volume:[/bold]")
                    species_table = Table(show_header=True, header_style="bold magenta")
                    species_table.add_column("Species Code", style="cyan")
                    species_table.add_column("Volume (cubic ft)", justify="right")
                    species_table.add_column("% of Total", justify="right", style="yellow")
                    species_table.add_column("Plots", justify="right", style="dim")
                    
                    for spcd, plots, vol in species_results:
                        pct = (vol / total_volume * 100) if total_volume > 0 else 0
                        species_table.add_row(
                            str(int(spcd)),
                            f"{vol:,.0f}",
                            f"{pct:.1f}%",
                            f"{plots:,}"
                        )
                    
                    console.print(species_table)
                
                console.print("\n[green bold]✓ Analysis complete![/green bold]")
                console.print("\n[dim]Note: This is a simplified calculation. Production code should[/dim]")
                console.print("[dim]include proper variance estimation and post-stratification.[/dim]\n")
            else:
                console.print("[red]No data found for Texas forestland[/red]")
                
    except Exception as e:
        console.print(f"\n[red]Error:[/red] {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()