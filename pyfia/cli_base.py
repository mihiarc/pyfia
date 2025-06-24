"""
Base CLI functionality for pyFIA command-line interfaces.

This module provides shared functionality for both the direct CLI and AI CLI,
including history management, database connection patterns, dataframe display,
and common command handling.
"""

import atexit
import cmd
import os
from pathlib import Path
from typing import Optional

# Optional readline import for Windows compatibility
try:
    import readline
    HAS_READLINE = True
except ImportError:
    HAS_READLINE = False

import polars as pl
from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.prompt import Confirm
from rich.table import Table

from pyfia.cli_config import CLIConfig


class BaseCLI(cmd.Cmd):
    """Base class for pyFIA CLI interfaces with shared functionality."""
    
    def __init__(self, history_filename: str = ".pyfia_history"):
        super().__init__()
        self.console = Console()
        self.last_result: Optional[pl.DataFrame] = None
        self.history_file = Path.home() / history_filename
        self.config = CLIConfig()
        
        # Setup command history
        self._setup_history()
    
    def _setup_history(self):
        """Setup command history with readline."""
        if not HAS_READLINE:
            return
        
        if self.history_file.exists():
            try:
                readline.read_history_file(self.history_file)
            except:
                pass
        
        readline.set_history_length(1000)
        atexit.register(self._save_history)
    
    def _save_history(self):
        """Save command history."""
        if not HAS_READLINE:
            return
        
        try:
            readline.write_history_file(self.history_file)
        except:
            pass
    
    def _display_dataframe(self, df: pl.DataFrame, title: str = "", max_rows: int = 20):
        """Display a Polars DataFrame as a rich table."""
        if df.is_empty():
            self.console.print("[yellow]No data to display[/yellow]")
            return
        
        # Create table
        table = Table(title=title, show_lines=True, box=box.ROUNDED)
        
        # Add columns with appropriate styling
        for col in df.columns:
            if df[col].dtype in [pl.Float32, pl.Float64]:
                table.add_column(col, style="cyan", justify="right")
            elif df[col].dtype in [pl.Int32, pl.Int64]:
                table.add_column(col, style="green", justify="right")
            else:
                table.add_column(col, style="white")
        
        # Format and add rows
        for row in df.head(max_rows).iter_rows():
            formatted_row = []
            for i, value in enumerate(row):
                if value is None:
                    formatted_row.append("[dim]NULL[/dim]")
                elif isinstance(value, float):
                    if abs(value) < 0.01 and value != 0:
                        formatted_row.append(f"{value:.2e}")
                    else:
                        formatted_row.append(f"{value:.2f}")
                else:
                    formatted_row.append(str(value))
            table.add_row(*formatted_row)
        
        self.console.print(table)
        
        # Show truncation message if needed
        if len(df) > max_rows:
            self.console.print(f"[dim]... showing {max_rows} of {len(df)} rows[/dim]")
    
    def _create_progress_bar(self, description: str = "Processing"):
        """Create a standardized progress bar."""
        return Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=self.console,
            transient=True,
        )
    
    def _validate_database_path(self, path_str: str) -> Optional[Path]:
        """Validate and return database path."""
        if not path_str:
            self.console.print("[red]Error: Please provide a database path[/red]")
            return None
        
        db_path = Path(path_str.strip())
        if not db_path.exists():
            self.console.print(f"[red]Error: Database not found: {db_path}[/red]")
            return None
        
        return db_path
    
    def _show_connection_status(self, db_path: Path, success: bool = True):
        """Show database connection status."""
        if success:
            self.console.print(
                Panel(
                    f"✅ Connected to: [cyan]{db_path.name}[/cyan]\n"
                    f"Path: [dim]{db_path}[/dim]",
                    title="Database Connected",
                    border_style="green",
                )
            )
        else:
            self.console.print(
                Panel(
                    f"❌ Failed to connect to: [cyan]{db_path.name}[/cyan]",
                    title="Connection Failed",
                    border_style="red",
                )
            )
    
    def _auto_connect_database(self):
        """Auto-connect to default database if configured."""
        if self.config.default_database:
            self.console.print("[cyan]Auto-connecting to default database...[/cyan]")
            return self._connect_to_database(self.config.default_database)
        return False
    
    def _connect_to_database(self, db_path_str: str) -> bool:
        """Connect to database with validation and progress display.
        
        Subclasses should override this method to implement specific connection logic.
        """
        raise NotImplementedError("Subclasses must implement _connect_to_database")
    
    def do_quit(self, arg: str):
        """Exit the CLI."""
        self.console.print("[yellow]Goodbye! 👋[/yellow]")
        return True
    
    def do_exit(self, arg: str):
        """Exit the CLI."""
        return self.do_quit(arg)
    
    def do_clear(self, arg: str):
        """Clear the screen."""
        os.system('cls' if os.name == 'nt' else 'clear')
    
    def do_last(self, arg: str):
        """Show last query results.
        Usage:
            last            - Show last 20 rows
            last n=50       - Show last 50 rows
            last stats      - Show summary statistics
            last columns    - Show column info
        """
        if self.last_result is None:
            self.console.print("[yellow]No previous results to display[/yellow]")
            return
        
        try:
            if not arg:
                self._display_dataframe(self.last_result, "Last Result")
            elif arg == "stats":
                self._show_dataframe_stats(self.last_result)
            elif arg == "columns":
                self._show_dataframe_columns(self.last_result)
            elif arg.startswith("n="):
                n = int(arg.split("=")[1])
                self._display_dataframe(self.last_result, "Last Result", max_rows=n)
            else:
                self.console.print(f"[red]Unknown option: {arg}[/red]")
        
        except Exception as e:
            self.console.print(f"[red]Error: {e}[/red]")
    
    def _show_dataframe_stats(self, df: pl.DataFrame):
        """Show summary statistics for numeric columns."""
        # Get numeric columns
        numeric_cols = [
            col for col in df.columns
            if df[col].dtype in [pl.Float32, pl.Float64, pl.Int32, pl.Int64]
        ]
        
        if not numeric_cols:
            self.console.print("[yellow]No numeric columns to summarize[/yellow]")
            return
        
        # Calculate statistics
        stats_df = df.select(numeric_cols).describe()
        self._display_dataframe(stats_df, "Summary Statistics")
    
    def _show_dataframe_columns(self, df: pl.DataFrame):
        """Show column information."""
        info_data = []
        for col in df.columns:
            null_count = df[col].null_count()
            info_data.append({
                "Column": col,
                "Type": str(df[col].dtype),
                "Non-Null": len(df) - null_count,
                "Null": null_count,
            })
        
        info_df = pl.DataFrame(info_data)
        self._display_dataframe(info_df, "Column Information")
    
    def do_export(self, arg: str):
        """Export last results to file.
        Usage:
            export results.csv      - Export to CSV
            export results.xlsx     - Export to Excel
            export results.json     - Export to JSON
        """
        if self.last_result is None:
            self.console.print("[yellow]No data to export[/yellow]")
            return
        
        if not arg:
            self.console.print("[red]Please specify output filename[/red]")
            return
        
        try:
            output_path = Path(arg.strip())
            
            if output_path.suffix.lower() == ".csv":
                self.last_result.write_csv(output_path)
            elif output_path.suffix.lower() == ".xlsx":
                self.last_result.write_excel(output_path)
            elif output_path.suffix.lower() == ".json":
                self.last_result.write_json(output_path)
            else:
                self.console.print("[red]Unsupported format. Use .csv, .xlsx, or .json[/red]")
                return
            
            self.console.print(f"[green]✅ Exported {len(self.last_result)} rows to {output_path}[/green]")
        
        except Exception as e:
            self.console.print(f"[red]Export failed: {e}[/red]")
    
    def emptyline(self):
        """Handle empty line - do nothing instead of repeating last command."""
        pass
    
    def default(self, line: str):
        """Handle unknown commands."""
        self.console.print(f"[red]Unknown command: {line}[/red]")
        self.console.print("Type 'help' for available commands.")