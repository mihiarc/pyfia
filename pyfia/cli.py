#!/usr/bin/env python3
"""
Interactive CLI tool for querying FIA databases using rich.

This module provides an interactive command-line interface for exploring
and analyzing Forest Inventory and Analysis (FIA) data.
"""

import sys
import os
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple
import cmd
import readline
import atexit

from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.layout import Layout
from rich.text import Text
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.syntax import Syntax
from rich.tree import Tree
from rich import box
from rich.prompt import Prompt, Confirm
from rich.markdown import Markdown
from rich.columns import Columns
from rich.live import Live
from rich.align import Align

import polars as pl
from pyfia.core import FIA
from pyfia.cli_config import CLIConfig


class FIAShell(cmd.Cmd):
    """Interactive shell for FIA database queries."""
    
    intro = None
    prompt = "fia> "
    
    def __init__(self, db_path: Optional[str] = None):
        super().__init__()
        self.console = Console()
        self.fia: Optional[FIA] = None
        self.db_path: Optional[Path] = None
        self.last_result: Optional[pl.DataFrame] = None
        self.history_file = Path.home() / ".fia_cli_history"
        self.config = CLIConfig()
        
        # Setup command history
        self._setup_history()
        
        # Display welcome message
        self._show_welcome()
        
        # Connect to database if provided
        if db_path:
            self.do_connect(db_path)
        elif self.config.default_database:
            # Auto-connect to default database
            self.console.print(f"[cyan]Auto-connecting to default database...[/cyan]")
            self.do_connect(self.config.default_database)
    
    def _setup_history(self):
        """Setup command history with readline."""
        # Set up command history
        if self.history_file.exists():
            readline.read_history_file(self.history_file)
        
        # Set history length
        readline.set_history_length(1000)
        
        # Save history on exit
        atexit.register(self._save_history)
    
    def _save_history(self):
        """Save command history."""
        try:
            readline.write_history_file(self.history_file)
        except:
            pass
    
    def _show_welcome(self):
        """Display welcome message."""
        quick_start = []
        
        # Add shortcuts if any exist
        shortcuts = self.config.state_shortcuts
        if shortcuts:
            quick_start.append("[yellow]State Shortcuts:[/yellow]")
            for state in sorted(shortcuts.keys())[:3]:
                quick_start.append(f"  • [cyan]shortcut {state}[/cyan] - Connect to {state} database")
        
        # Add recent databases
        recent = self.config.recent_databases
        if recent:
            quick_start.append("\n[yellow]Recent Databases:[/yellow]")
            quick_start.append("  • [cyan]recent[/cyan] - Show recent databases")
            quick_start.append("  • [cyan]recent 1[/cyan] - Connect to most recent")
        
        # Build welcome message
        welcome_text = (
            "[bold green]Welcome to FIA Interactive CLI[/bold green]\n\n"
            "A powerful tool for querying and analyzing Forest Inventory and Analysis data.\n\n"
            "[yellow]Quick Commands:[/yellow]\n"
            "  • [cyan]help[/cyan] - Show available commands\n"
            "  • [cyan]connect <path>[/cyan] - Connect to FIA database\n"
            "  • [cyan]setdefault[/cyan] - Set default database\n"
            "  • [cyan]info[/cyan] - Show database information\n"
            "  • [cyan]exit[/cyan] - Exit the CLI"
        )
        
        if quick_start:
            welcome_text += "\n\n" + "\n".join(quick_start)
        
        welcome_text += "\n\nType [bold]help[/bold] for more commands."
        
        welcome = Panel(
            welcome_text,
            title="[bold blue]FIA CLI v1.0[/bold blue]",
            border_style="blue"
        )
        self.console.print(welcome)
    
    def do_connect(self, arg: str):
        """Connect to an FIA database.
        Usage: connect <database_path>
        """
        if not arg:
            self.console.print("[red]Error: Please provide a database path[/red]")
            return
        
        db_path = Path(arg.strip())
        if not db_path.exists():
            self.console.print(f"[red]Error: Database not found: {db_path}[/red]")
            return
        
        try:
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=self.console
            ) as progress:
                task = progress.add_task("Connecting to database...", total=None)
                self.fia = FIA(str(db_path))
                self.db_path = db_path
                progress.update(task, completed=True)
            
            self.console.print(f"[green]✓ Connected to: {db_path.name}[/green]")
            
            # Save to recent databases
            self.config.add_recent_database(str(db_path))
            
            # Show basic info
            self._show_db_summary()
            
        except Exception as e:
            self.console.print(f"[red]Error connecting to database: {e}[/red]")
    
    def _show_db_summary(self):
        """Show database summary information."""
        if not self.fia:
            return
        
        try:
            # Get available evaluations (returns list of EVALID values)
            evalids = self.fia.find_evalid()
            
            summary = Table(title="Database Summary", box=box.ROUNDED)
            summary.add_column("Property", style="cyan")
            summary.add_column("Value", style="yellow")
            
            summary.add_row("Database", self.db_path.name)
            summary.add_row("Available Evaluations", str(len(evalids)))
            
            if evalids:
                try:
                    # Load POP_EVAL table if needed
                    if 'POP_EVAL' not in self.fia.tables:
                        self.fia.load_table('POP_EVAL')
                    
                    pop_eval = self.fia.tables['POP_EVAL'].collect()
                    # Filter to our evalids
                    pop_eval = pop_eval.filter(pl.col('EVALID').is_in(evalids))
                    
                    # Get states
                    states = pop_eval['STATECD'].unique().to_list()
                    summary.add_row("States", ", ".join(map(str, sorted(states))))
                    
                    # Get year range
                    years = pop_eval['END_INVYR'].to_list()
                    if years:
                        summary.add_row("Year Range", f"{min(years)} - {max(years)}")
                except:
                    # Fall back to simple range
                    summary.add_row("EVALID Range", f"{min(evalids)} - {max(evalids)}")
            
            self.console.print(summary)
            
        except Exception as e:
            self.console.print(f"[yellow]Warning: Could not fetch summary: {e}[/yellow]")
    
    def do_info(self, arg: str):
        """Show database information and available evaluations."""
        if not self._check_connection():
            return
        
        try:
            # Get evaluations (returns list of EVALID values)
            evalid_list = self.fia.find_evalid()
            # Debug: check what type we got
            if hasattr(evalid_list, 'is_empty'):
                # It's a DataFrame, not a list
                self.console.print("[red]Debug: find_evalid returned DataFrame instead of list[/red]")
                if evalid_list.is_empty():
                    self.console.print("[yellow]No evaluations found in database[/yellow]")
                    return
                # Convert to list
                evalid_list = evalid_list['EVALID'].to_list()
            
            if not evalid_list:
                self.console.print("[yellow]No evaluations found in database[/yellow]")
                return
            
            # Get detailed evaluation info from POP_EVAL and POP_EVAL_TYP
            if 'POP_EVAL' not in self.fia.tables:
                self.fia.load_table('POP_EVAL')
            if 'POP_EVAL_TYP' not in self.fia.tables:
                self.fia.load_table('POP_EVAL_TYP', ['EVAL_CN', 'EVAL_TYP'])
            
            # Get evaluation details
            pop_eval = self.fia.tables['POP_EVAL'].collect()
            pop_eval_typ = self.fia.tables['POP_EVAL_TYP'].collect()
            
            # Join and filter
            evalids = pop_eval.join(
                pop_eval_typ,
                left_on='CN',
                right_on='EVAL_CN',
                how='left'
            ).filter(pl.col('EVALID').is_in(evalid_list))
            
            if evalids.height == 0:  # Use height instead of is_empty()
                self.console.print("[yellow]No evaluation details found[/yellow]")
                return
            
            # Sort by state and year
            evalids_sorted = evalids.sort(['STATECD', 'END_INVYR'], descending=[False, True])
            
            # Use the formatted evaluation table
            self._display_evaluation_table(evalids_sorted.head(20), title="Available Evaluations")
            
            if len(evalids) > 20:
                self.console.print(f"\n[italic]Showing first 20 of {len(evalids)} evaluations. "
                                 "Use 'evalid' command for full list.[/italic]")
            
        except Exception as e:
            import traceback
            self.console.print(f"[red]Error fetching info: {e}[/red]")
            self.console.print(f"[dim]{traceback.format_exc()}[/dim]")
    
    def do_evalid(self, arg: str):
        """List or search evaluations.
        Usage: 
            evalid                    - List all evaluations
            evalid <state_code>       - List evaluations for a state
            evalid recent             - Show most recent evaluations
        """
        if not self._check_connection():
            return
        
        try:
            # Get evaluations based on argument
            if arg.strip() == 'recent':
                evalid_list = self.fia.find_evalid(most_recent=True)
                title = "Most Recent Evaluations"
            elif arg.strip().isdigit():
                state_code = int(arg.strip())
                evalid_list = self.fia.find_evalid(state=state_code)
                title = f"Evaluations for State {state_code}"
            else:
                evalid_list = self.fia.find_evalid()
                title = "All Evaluations"
            
            if not evalid_list:
                self.console.print("[yellow]No evaluations found[/yellow]")
                return
            
            # Get detailed info
            if 'POP_EVAL' not in self.fia.tables:
                self.fia.load_table('POP_EVAL')
            if 'POP_EVAL_TYP' not in self.fia.tables:
                self.fia.load_table('POP_EVAL_TYP', ['EVAL_CN', 'EVAL_TYP'])
            
            pop_eval = self.fia.tables['POP_EVAL'].collect()
            pop_eval_typ = self.fia.tables['POP_EVAL_TYP'].collect()
            
            evalids = pop_eval.join(
                pop_eval_typ,
                left_on='CN',
                right_on='EVAL_CN',
                how='left'
            ).filter(pl.col('EVALID').is_in(evalid_list))
            
            # Sort by state and year for better display
            evalids = evalids.sort(['STATECD', 'END_INVYR'], descending=[False, True])
            
            # Create a nicely formatted table instead of raw dataframe
            self._display_evaluation_table(evalids, title=title)
            
        except Exception as e:
            self.console.print(f"[red]Error: {e}[/red]")
    
    def do_clip(self, arg: str):
        """Set EVALID filter for subsequent queries.
        Usage:
            clip <evalid>             - Clip to specific EVALID
            clip recent               - Clip to most recent evaluation
            clip recent <type>        - Clip to most recent of type (VOL, GRM, etc.)
        """
        if not self._check_connection():
            return
        
        try:
            args = arg.strip().split()
            
            if not args:
                self.console.print("[red]Error: Please specify EVALID or 'recent'[/red]")
                return
            
            if args[0] == 'recent':
                eval_type = args[1] if len(args) > 1 else 'VOL'
                self.fia.clip_most_recent(eval_type=eval_type)
                self.console.print(f"[green]✓ Clipped to most recent {eval_type} evaluation[/green]")
            else:
                evalid = int(args[0])
                self.fia.evalid = [evalid]
                self.console.print(f"[green]✓ Clipped to EVALID: {evalid}[/green]")
            
            # Show what we clipped to
            if self.fia.evalid:
                self.console.print(f"[cyan]Active EVALID(s): {self.fia.evalid}[/cyan]")
            
        except Exception as e:
            self.console.print(f"[red]Error: {e}[/red]")
    
    def do_tpa(self, arg: str):
        """Calculate trees per acre.
        Usage:
            tpa                       - Basic TPA calculation
            tpa bySpecies             - TPA by species
            tpa bySizeClass           - TPA by size class
            tpa grpBy=FORTYPCD        - Group by forest type
        """
        if not self._check_connection():
            return
        
        try:
            # Parse arguments
            kwargs = self._parse_kwargs(arg)
            
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=self.console
            ) as progress:
                task = progress.add_task("Calculating TPA...", total=None)
                result = self.fia.tpa(**kwargs)
                progress.update(task, completed=True)
            
            self.last_result = result
            self._display_results(result, "Trees Per Acre")
            
        except Exception as e:
            self.console.print(f"[red]Error: {e}[/red]")
    
    def do_biomass(self, arg: str):
        """Calculate biomass.
        Usage:
            biomass                   - Basic biomass calculation
            biomass bySpecies         - Biomass by species
            biomass bySizeClass       - Biomass by size class
        """
        if not self._check_connection():
            return
        
        try:
            kwargs = self._parse_kwargs(arg)
            
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=self.console
            ) as progress:
                task = progress.add_task("Calculating biomass...", total=None)
                result = self.fia.biomass(**kwargs)
                progress.update(task, completed=True)
            
            self.last_result = result
            self._display_results(result, "Biomass")
            
        except Exception as e:
            self.console.print(f"[red]Error: {e}[/red]")
    
    def do_volume(self, arg: str):
        """Calculate volume.
        Usage:
            volume                    - Basic volume calculation
            volume bySpecies          - Volume by species
            volume volType=NET        - Net volume (GROSS, NET, SOUND)
        """
        if not self._check_connection():
            return
        
        try:
            kwargs = self._parse_kwargs(arg)
            
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=self.console
            ) as progress:
                task = progress.add_task("Calculating volume...", total=None)
                result = self.fia.volume(**kwargs)
                progress.update(task, completed=True)
            
            self.last_result = result
            self._display_results(result, "Volume")
            
        except Exception as e:
            self.console.print(f"[red]Error: {e}[/red]")
    
    def do_mortality(self, arg: str):
        """Calculate mortality.
        Usage:
            mortality                 - Basic mortality calculation
            mortality bySpecies       - Mortality by species
        """
        if not self._check_connection():
            return
        
        try:
            kwargs = self._parse_kwargs(arg)
            
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=self.console
            ) as progress:
                task = progress.add_task("Calculating mortality...", total=None)
                result = self.fia.mortality(**kwargs)
                progress.update(task, completed=True)
            
            self.last_result = result
            self._display_results(result, "Mortality")
            
        except Exception as e:
            self.console.print(f"[red]Error: {e}[/red]")
    
    def do_export(self, arg: str):
        """Export last result to file.
        Usage:
            export <filename.csv>     - Export to CSV
            export <filename.parquet> - Export to Parquet
        """
        if not self.last_result:
            self.console.print("[yellow]No results to export. Run a query first.[/yellow]")
            return
        
        if not arg:
            self.console.print("[red]Error: Please specify output filename[/red]")
            return
        
        try:
            filename = Path(arg.strip())
            
            if filename.suffix.lower() == '.csv':
                self.last_result.write_csv(filename)
            elif filename.suffix.lower() == '.parquet':
                self.last_result.write_parquet(filename)
            else:
                self.console.print("[red]Error: Unsupported format. Use .csv or .parquet[/red]")
                return
            
            self.console.print(f"[green]✓ Exported to: {filename}[/green]")
            
        except Exception as e:
            self.console.print(f"[red]Error: {e}[/red]")
    
    def do_show(self, arg: str):
        """Show details of last result.
        Usage:
            show              - Show last result
            show stats        - Show statistics
            show schema       - Show column types
        """
        if not self.last_result:
            self.console.print("[yellow]No results to show. Run a query first.[/yellow]")
            return
        
        try:
            if arg.strip() == 'stats':
                self._show_stats()
            elif arg.strip() == 'schema':
                self._show_schema()
            else:
                self._display_dataframe(self.last_result, "Last Result")
        
        except Exception as e:
            self.console.print(f"[red]Error: {e}[/red]")
    
    def do_clear(self, arg: str):
        """Clear the screen."""
        os.system('clear' if os.name == 'posix' else 'cls')
    
    def do_setdefault(self, arg: str):
        """Set default database.
        Usage:
            setdefault            - Set current database as default
            setdefault <path>     - Set specific database as default
            setdefault none      - Clear default database
        """
        if arg.strip() == 'none':
            self.config.default_database = None
            self.config.save_config()
            self.console.print("[yellow]Default database cleared[/yellow]")
        elif arg.strip():
            # Set specific path as default
            path = Path(arg.strip())
            if path.exists():
                self.config.default_database = str(path)
                self.console.print(f"[green]✓ Default database set to: {path.name}[/green]")
            else:
                self.console.print(f"[red]Error: Database not found: {path}[/red]")
        elif self.db_path:
            # Set current database as default
            self.config.default_database = str(self.db_path)
            self.console.print(f"[green]✓ Default database set to: {self.db_path.name}[/green]")
        else:
            self.console.print("[red]No database connected[/red]")
    
    def do_recent(self, arg: str):
        """Show or connect to recent databases.
        Usage:
            recent            - Show recent databases
            recent <number>   - Connect to database from recent list
        """
        recent = self.config.recent_databases
        
        if not recent:
            self.console.print("[yellow]No recent databases[/yellow]")
            return
        
        if arg.strip().isdigit():
            # Connect to specific recent database
            idx = int(arg.strip()) - 1
            if 0 <= idx < len(recent):
                self.do_connect(recent[idx])
            else:
                self.console.print(f"[red]Invalid selection. Choose 1-{len(recent)}[/red]")
        else:
            # Show recent databases
            table = Table(title="Recent Databases", box=box.ROUNDED)
            table.add_column("#", style="cyan", width=3)
            table.add_column("Database", style="yellow")
            table.add_column("Path", style="dim")
            
            for i, path in enumerate(recent, 1):
                p = Path(path)
                table.add_row(str(i), p.name, str(p.parent))
            
            self.console.print(table)
            self.console.print("\n[italic]Use 'recent <number>' to connect[/italic]")
    
    def do_shortcut(self, arg: str):
        """Manage state shortcuts.
        Usage:
            shortcut                  - Show all shortcuts
            shortcut add NC <path>    - Add shortcut for NC
            shortcut NC               - Connect using NC shortcut
        """
        args = arg.strip().split(maxsplit=2)
        
        if not args:
            # Show all shortcuts
            shortcuts = self.config.state_shortcuts
            if not shortcuts:
                self.console.print("[yellow]No shortcuts defined[/yellow]")
                return
            
            table = Table(title="State Shortcuts", box=box.ROUNDED)
            table.add_column("State", style="cyan")
            table.add_column("Database", style="yellow")
            
            for state, path in sorted(shortcuts.items()):
                table.add_row(state, Path(path).name)
            
            self.console.print(table)
        
        elif args[0] == 'add' and len(args) >= 3:
            # Add shortcut
            state = args[1].upper()
            path = Path(args[2])
            if path.exists():
                self.config.add_state_shortcut(state, str(path))
                self.console.print(f"[green]✓ Shortcut '{state}' → {path.name}[/green]")
            else:
                self.console.print(f"[red]Error: Database not found: {path}[/red]")
        
        elif len(args) == 1:
            # Use shortcut
            state = args[0].upper()
            shortcuts = self.config.state_shortcuts
            if state in shortcuts:
                self.do_connect(shortcuts[state])
            else:
                self.console.print(f"[red]No shortcut for '{state}'[/red]")
    
    def do_exit(self, arg: str):
        """Exit the FIA CLI."""
        if Confirm.ask("Are you sure you want to exit?", default=True):
            self.console.print("[yellow]Goodbye![/yellow]")
            return True
    
    def do_quit(self, arg: str):
        """Exit the FIA CLI."""
        return self.do_exit(arg)
    
    def do_help(self, arg: str):
        """Show help for commands."""
        if arg:
            # Show help for specific command
            try:
                func = getattr(self, f'do_{arg}')
                if func.__doc__:
                    self.console.print(Panel(
                        func.__doc__,
                        title=f"Help: {arg}",
                        border_style="blue"
                    ))
                else:
                    self.console.print(f"[yellow]No help available for '{arg}'[/yellow]")
            except AttributeError:
                self.console.print(f"[red]Unknown command: {arg}[/red]")
        else:
            # Show all commands
            commands = Table(title="Available Commands", box=box.ROUNDED)
            commands.add_column("Command", style="cyan", width=20)
            commands.add_column("Description", style="white")
            
            # Get all commands
            for name in sorted(dir(self)):
                if name.startswith('do_') and name != 'do_EOF':
                    cmd_name = name[3:]
                    func = getattr(self, name)
                    if func.__doc__:
                        desc = func.__doc__.split('\n')[0]
                    else:
                        desc = "No description available"
                    commands.add_row(cmd_name, desc)
            
            self.console.print(commands)
            self.console.print("\n[italic]Type 'help <command>' for detailed help[/italic]")
    
    def _check_connection(self) -> bool:
        """Check if connected to database."""
        if not self.fia:
            self.console.print("[red]Not connected to database. Use 'connect <path>' first.[/red]")
            return False
        return True
    
    def _parse_kwargs(self, arg: str) -> Dict[str, Any]:
        """Parse command arguments into kwargs."""
        kwargs = {}
        
        if not arg:
            return kwargs
        
        # Handle special flags
        parts = arg.split()
        for part in parts:
            if part == 'bySpecies':
                kwargs['by_species'] = True
            elif part == 'bySizeClass':
                kwargs['by_size_class'] = True
            elif '=' in part:
                key, value = part.split('=', 1)
                # Try to convert to appropriate type
                try:
                    # Try int first
                    kwargs[key] = int(value)
                except ValueError:
                    try:
                        # Try float
                        kwargs[key] = float(value)
                    except ValueError:
                        # Keep as string
                        kwargs[key] = value
        
        return kwargs
    
    def _display_results(self, df: pl.DataFrame, title: str):
        """Display estimation results."""
        if df.is_empty():
            self.console.print("[yellow]No data returned[/yellow]")
            return
        
        # Check for TPA-specific columns and rename for consistency
        rename_map = {
            'TPA': 'estimate',
            'TPA_SE': 'se', 
            'TPA_CV': 'cv',
            'AREA_TOTAL': 'area'
        }
        
        for old_col, new_col in rename_map.items():
            if old_col in df.columns and new_col not in df.columns:
                df = df.rename({old_col: new_col})
        
        # Format numeric columns
        numeric_cols = ['estimate', 'variance', 'se', 'cv', 'total', 'area',
                       'nPlots', 'mean', 'se_mean', 'lower_bound', 'upper_bound']
        
        table = Table(title=title, box=box.ROUNDED, show_header=True)
        
        # Add columns
        for col in df.columns:
            if col in numeric_cols:
                table.add_column(col, justify="right", style="yellow")
            else:
                table.add_column(col, style="cyan")
        
        # Add rows (limit to first 20 for display)
        for row in df.head(20).iter_rows():
            formatted_row = []
            for i, value in enumerate(row):
                col_name = df.columns[i]
                if col_name in numeric_cols and value is not None:
                    if col_name == 'cv':
                        formatted_row.append(f"{value:.1%}")
                    elif col_name in ['estimate', 'total', 'area']:
                        formatted_row.append(f"{value:,.0f}")
                    else:
                        formatted_row.append(f"{value:,.2f}")
                else:
                    formatted_row.append(str(value) if value is not None else "")
            table.add_row(*formatted_row)
        
        self.console.print(table)
        
        if len(df) > 20:
            self.console.print(f"\n[italic]Showing first 20 of {len(df)} rows[/italic]")
    
    def _display_dataframe(self, df: pl.DataFrame, title: str = ""):
        """Display a generic dataframe."""
        if df.is_empty():
            self.console.print("[yellow]No data[/yellow]")
            return
        
        table = Table(title=title, box=box.ROUNDED)
        
        # Add columns
        for col in df.columns:
            table.add_column(col, style="cyan")
        
        # Add rows (limit for display)
        for row in df.head(50).iter_rows():
            table.add_row(*[str(v) if v is not None else "" for v in row])
        
        self.console.print(table)
        
        if len(df) > 50:
            self.console.print(f"\n[italic]Showing first 50 of {len(df)} rows[/italic]")
    
    def _display_evaluation_table(self, df: pl.DataFrame, title: str = ""):
        """Display evaluation data in a nicely formatted table."""
        if df.is_empty():
            self.console.print("[yellow]No evaluations found[/yellow]")
            return
        
        table = Table(title=title, box=box.ROUNDED, show_lines=False)
        
        # Add specific columns with formatting
        table.add_column("EVALID", style="bold cyan", no_wrap=True)
        table.add_column("State", style="green", width=6, justify="center")
        table.add_column("Type", style="yellow", width=8, justify="center")
        table.add_column("Years", style="magenta", width=12, justify="center")
        table.add_column("Location", style="blue", width=20)
        table.add_column("Plots", style="white", justify="right", width=8)
        table.add_column("Area (acres)", style="white", justify="right", width=15)
        
        # State code to abbreviation mapping (partial, add more as needed)
        state_abbrev = {
            37: "NC", 45: "SC", 13: "GA", 12: "FL", 1: "AL", 47: "TN",
            51: "VA", 54: "WV", 21: "KY", 5: "AR", 28: "MS", 22: "LA",
            48: "TX", 40: "OK", 20: "KS", 29: "MO", 17: "IL", 18: "IN",
            39: "OH", 26: "MI", 55: "WI", 27: "MN", 19: "IA", 31: "NE",
            46: "SD", 38: "ND", 30: "MT", 56: "WY", 8: "CO", 35: "NM",
            4: "AZ", 49: "UT", 32: "NV", 16: "ID", 53: "WA", 41: "OR",
            6: "CA", 2: "AK", 15: "HI"
        }
        
        # Process each evaluation
        for row in df.iter_rows(named=True):
            evalid = str(row.get('EVALID', ''))
            statecd = row.get('STATECD', 0)
            state = state_abbrev.get(statecd, str(statecd))
            eval_typ = row.get('EVAL_TYP', 'VOL')
            start_year = row.get('START_INVYR', '')
            end_year = row.get('END_INVYR', '')
            years = f"{start_year}-{end_year}" if start_year and end_year else ""
            
            # Get location info
            location_parts = []
            if row.get('EVAL_DESCR'):
                location_parts.append(str(row['EVAL_DESCR'])[:20])
            elif row.get('RSCD'):
                location_parts.append(f"Region {row['RSCD']}")
            location = " ".join(location_parts) or "Statewide"
            
            # Get plot count
            nplots = row.get('NPLOTS', 0)
            plots_str = f"{nplots:,}" if nplots else "N/A"
            
            # Get area if available
            area = row.get('AREA_USED', 0)
            area_str = f"{area:,.0f}" if area else "N/A"
            
            table.add_row(
                evalid,
                state,
                eval_typ,
                years,
                location,
                plots_str,
                area_str
            )
        
        self.console.print(table)
        
        # Summary statistics
        total_evals = len(df)
        states = df['STATECD'].n_unique()
        
        summary = f"\n[dim]Total: {total_evals} evaluations across {states} state(s)[/dim]"
        
        if total_evals > 50:
            summary += f"\n[italic]Showing all {total_evals} evaluations[/italic]"
        
        self.console.print(summary)
    
    def _show_stats(self):
        """Show statistics for last result."""
        if not self.last_result:
            return
        
        stats = Table(title="Result Statistics", box=box.ROUNDED)
        stats.add_column("Statistic", style="cyan")
        stats.add_column("Value", style="yellow")
        
        stats.add_row("Rows", str(len(self.last_result)))
        stats.add_row("Columns", str(len(self.last_result.columns)))
        
        # Show numeric column stats
        numeric_cols = [col for col in self.last_result.columns 
                       if self.last_result[col].dtype in [pl.Float32, pl.Float64, pl.Int32, pl.Int64]]
        
        if numeric_cols:
            stats.add_row("", "")  # Empty row
            stats.add_row("[bold]Numeric Columns[/bold]", "")
            
            for col in numeric_cols:
                series = self.last_result[col]
                if series.null_count() < len(series):
                    stats.add_row(f"  {col} (mean)", f"{series.mean():.2f}")
                    stats.add_row(f"  {col} (std)", f"{series.std():.2f}")
                    stats.add_row(f"  {col} (min)", f"{series.min():.2f}")
                    stats.add_row(f"  {col} (max)", f"{series.max():.2f}")
        
        self.console.print(stats)
    
    def _show_schema(self):
        """Show schema for last result."""
        if not self.last_result:
            return
        
        schema = Table(title="Result Schema", box=box.ROUNDED)
        schema.add_column("Column", style="cyan")
        schema.add_column("Type", style="yellow")
        schema.add_column("Null Count", style="magenta")
        
        for col in self.last_result.columns:
            schema.add_row(
                col,
                str(self.last_result[col].dtype),
                str(self.last_result[col].null_count())
            )
        
        self.console.print(schema)
    
    def emptyline(self):
        """Do nothing on empty line."""
        pass
    
    def default(self, line):
        """Handle unknown commands."""
        self.console.print(f"[red]Unknown command: {line}. Type 'help' for available commands.[/red]")
    
    def cmdloop(self, intro=None):
        """Override cmdloop to use rich formatting."""
        self.preloop()
        if intro is not None:
            self.intro = intro
        if self.intro:
            self.console.print(self.intro)
        stop = None
        while not stop:
            try:
                # Use rich to print the prompt
                self.console.print("[bold cyan]fia>[/bold cyan] ", end="")
                line = input()
                line = self.precmd(line)
                stop = self.onecmd(line)
                stop = self.postcmd(stop, line)
            except KeyboardInterrupt:
                self.console.print("^C")
            except EOFError:
                self.console.print("^D")
                break
        self.postloop()


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="FIA Interactive CLI")
    parser.add_argument("database", nargs="?", help="Path to FIA database")
    parser.add_argument("--no-history", action="store_true", help="Disable command history")
    parser.add_argument("--nc", action="store_true", help="Connect to NC database (looks for common paths)")
    
    args = parser.parse_args()
    
    # Handle NC database shortcut
    db_path = args.database
    config = CLIConfig()
    
    if args.nc and not db_path:
        # First check if NC shortcut exists
        shortcuts = config.state_shortcuts
        if 'NC' in shortcuts:
            db_path = shortcuts['NC']
        else:
            # Look for NC database in common locations
            common_paths = [
                Path.home() / "FIA_data" / "SQLite_FIADB_NC.db",
                Path.home() / "data" / "FIA" / "SQLite_FIADB_NC.db",
                Path.home() / "Downloads" / "SQLite_FIADB_NC.db",
                Path.cwd() / "SQLite_FIADB_NC.db",
                Path.cwd() / "data" / "SQLite_FIADB_NC.db",
                Path("/Users/mihiarc/data/FIA/SQLite_FIADB_NC.db"),  # Your specific path
            ]
            
            for path in common_paths:
                if path.exists():
                    db_path = str(path)
                    # Automatically save as NC shortcut
                    config.add_state_shortcut('NC', db_path)
                    break
            
            if not db_path:
                console = Console()
                console.print("[red]NC database not found in common locations.[/red]")
                console.print("Searched in:")
                for path in common_paths:
                    console.print(f"  • {path}")
                console.print("\n[yellow]Tip: Once you find your NC database, use:[/yellow]")
                console.print("  [cyan]shortcut add NC /path/to/database[/cyan]")
                sys.exit(1)
    
    # Create and run shell
    shell = FIAShell(db_path=db_path)
    
    try:
        shell.cmdloop()
    except KeyboardInterrupt:
        shell.console.print("\n[yellow]Use 'exit' to quit[/yellow]")
        shell.cmdloop()
    except Exception as e:
        shell.console.print(f"\n[red]Fatal error: {e}[/red]")
        sys.exit(1)


if __name__ == "__main__":
    main()