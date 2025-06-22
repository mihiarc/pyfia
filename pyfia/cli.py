#!/usr/bin/env python3
"""
Direct CLI for pyFIA - Programmatic access to FIA estimation methods.

This module provides a command-line interface for direct interaction with
pyFIA's statistical estimation methods without SQL or AI layers.
Uses DuckDB for efficient handling of large-scale FIA datasets.
"""

import atexit
import cmd
import os
import readline
import sys
from pathlib import Path
from typing import Any, Dict, Optional

import polars as pl
from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.prompt import Confirm, Prompt
from rich.table import Table

from pyfia.cli_config import CLIConfig
from pyfia.core import FIA


class FIADirectCLI(cmd.Cmd):
    """Direct interface to pyFIA estimation methods."""

    intro = None
    prompt = "pyfia> "
    doc_header = "Commands (type help <command> for details):"
    undoc_header = "Other commands:"

    def __init__(self, db_path: Optional[str] = None):
        super().__init__()
        self.console = Console()
        self.fia: Optional[FIA] = None
        self.db_path: Optional[Path] = None
        self.last_result: Optional[pl.DataFrame] = None
        self.history_file = Path.home() / ".pyfia_history"
        self.config = CLIConfig()

        # Setup command history
        self._setup_history()

        # Display welcome message
        self._show_welcome()

        # Connect to database if provided
        if db_path:
            self.do_connect(db_path)
        elif self.config.default_database:
            self.console.print("[cyan]Auto-connecting to default database...[/cyan]")
            self.do_connect(self.config.default_database)

    def _setup_history(self):
        """Setup command history with readline."""
        if self.history_file.exists():
            try:
                readline.read_history_file(self.history_file)
            except:
                pass

        readline.set_history_length(1000)
        atexit.register(self._save_history)

    def _save_history(self):
        """Save command history."""
        try:
            readline.write_history_file(self.history_file)
        except:
            pass

    def _show_welcome(self):
        """Display welcome message."""
        # Add status information
        status_items = []
        if self.config.default_database:
            status_items.append(f"Default: {Path(self.config.default_database).name}")

        shortcuts = self.config.state_shortcuts
        if shortcuts:
            status_items.append(f"Shortcuts: {', '.join(shortcuts.keys())}")

        # Display welcome message with proper formatting
        self.console.print()
        self.console.rule(
            "[bold green]🌲 pyFIA Direct Interface[/bold green]", style="green"
        )
        self.console.print()
        self.console.print(
            "Access Forest Inventory Analysis estimation methods directly."
        )
        self.console.print()

        # Quick Start section
        self.console.print("[bold cyan]Quick Start:[/bold cyan]")
        self.console.print("  connect <path>    - Connect to FIA database")
        self.console.print("  evalid            - Manage evaluation selection")
        self.console.print("  area              - Calculate forest area")
        self.console.print("  biomass           - Calculate tree biomass")
        self.console.print("  volume            - Calculate wood volume")
        self.console.print("  tpa               - Calculate trees per acre")
        self.console.print("  mortality         - Calculate mortality")
        self.console.print("  help              - Show all commands")
        self.console.print()

        # Examples section
        self.console.print("[bold cyan]Examples:[/bold cyan]")
        self.console.print("  area bySpecies landType=timber")
        self.console.print("  biomass component=AG bySpecies")
        self.console.print("  tpa bySizeClass treeType=live")
        self.console.print("  evalid mostRecent")
        self.console.print("  export results.csv")
        self.console.print()

        # Status line
        if status_items:
            self.console.rule(f"[dim]{' | '.join(status_items)}[/dim]", style="dim")
        else:
            self.console.rule("[dim]No database connected[/dim]", style="dim")
        self.console.print()

    def do_connect(self, arg: str):
        """Connect to an FIA DuckDB database.
        Usage: connect <database_path>

        Expects a DuckDB database file (.duckdb, .duck, or .db extension).
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
                console=self.console,
            ) as progress:
                task = progress.add_task("Connecting to database...", total=None)

                # Always use DuckDB engine
                self.fia = FIA(str(db_path), engine="duckdb")
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
            # Get available evaluations
            evalids = self.fia.find_evalid()

            summary = Table(title="Database Summary", box=box.ROUNDED)
            summary.add_column("Property", style="cyan")
            summary.add_column("Value", style="yellow")

            summary.add_row("Database", self.db_path.name)
            summary.add_row("Engine", "DuckDB")
            summary.add_row("Available Evaluations", str(len(evalids)))

            if evalids:
                try:
                    # Load POP_EVAL table if needed
                    if "POP_EVAL" not in self.fia.tables:
                        self.fia.load_table("POP_EVAL")

                    pop_eval = self.fia.tables["POP_EVAL"].collect()
                    pop_eval = pop_eval.filter(pl.col("EVALID").is_in(evalids))

                    # Get states and convert to abbreviations
                    state_codes = sorted(pop_eval["STATECD"].unique().to_list())

                    # State code to abbreviation mapping
                    code_to_abbr = {
                        1: "AL",
                        2: "AK",
                        4: "AZ",
                        5: "AR",
                        6: "CA",
                        8: "CO",
                        9: "CT",
                        10: "DE",
                        12: "FL",
                        13: "GA",
                        15: "HI",
                        16: "ID",
                        17: "IL",
                        18: "IN",
                        19: "IA",
                        20: "KS",
                        21: "KY",
                        22: "LA",
                        23: "ME",
                        24: "MD",
                        25: "MA",
                        26: "MI",
                        27: "MN",
                        28: "MS",
                        29: "MO",
                        30: "MT",
                        31: "NE",
                        32: "NV",
                        33: "NH",
                        34: "NJ",
                        35: "NM",
                        36: "NY",
                        37: "NC",
                        38: "ND",
                        39: "OH",
                        40: "OK",
                        41: "OR",
                        42: "PA",
                        44: "RI",
                        45: "SC",
                        46: "SD",
                        47: "TN",
                        48: "TX",
                        49: "UT",
                        50: "VT",
                        51: "VA",
                        53: "WA",
                        54: "WV",
                        55: "WI",
                        56: "WY",
                        60: "AS",
                        64: "FM",
                        66: "GU",
                        68: "MH",
                        69: "MP",
                        70: "PW",
                        72: "PR",
                        78: "VI",
                    }

                    state_abbrs = [
                        code_to_abbr.get(code, f"#{code}") for code in state_codes
                    ]
                    summary.add_row("States", ", ".join(state_abbrs))

                    # Get year range and ensure integers
                    years = pop_eval["END_INVYR"].to_list()
                    if years:
                        # Convert to integers to remove any decimal points
                        years = [int(y) for y in years if y is not None]
                        if years:
                            summary.add_row(
                                "Year Range", f"{min(years)} - {max(years)}"
                            )
                except:
                    summary.add_row("EVALID Range", f"{min(evalids)} - {max(evalids)}")

            # Show current EVALID if set
            if self.fia.evalid:
                summary.add_row("Current EVALID", ", ".join(map(str, self.fia.evalid)))

            self.console.print(summary)

        except Exception as e:
            self.console.print(f"[yellow]Could not load summary: {e}[/yellow]")

    def do_evalid(self, arg: str):
        """Manage EVALID selection for statistically valid estimates.
        Usage:
            evalid                  - Show available evaluations (interactive)
            evalid <state>          - Show evaluations for state
            evalid <number>         - Select specific EVALID (6 digits)
            evalid mostRecent       - Select most recent evaluation
            evalid mostRecent VOL   - Select most recent volume evaluation
            evalid clear            - Clear EVALID filter

        State can be specified as:
            - Number: evalid 37
            - Abbreviation: evalid NC or evalid nc
            - Full name: evalid "North Carolina" or evalid alabama
        """
        # Handle 'evalid help' syntax
        if arg.strip().lower() == "help":
            self.do_help("evalid")
            return

        if not self._check_connection():
            return

        try:
            if not arg:
                # Show available evaluations with interactive state selection
                self._show_evaluations()

            elif arg.lower() == "clear":
                self.fia.evalid = None
                self.console.print("[yellow]EVALID filter cleared[/yellow]")

            elif arg.lower().startswith("mostrecent"):
                parts = arg.split()
                eval_type = parts[1].upper() if len(parts) > 1 else None
                self.fia.clip_most_recent(eval_type=eval_type)
                self.console.print(
                    f"[green]✓ Selected most recent evaluation: {self.fia.evalid}[/green]"
                )

            else:
                # Try to parse as state identifier or EVALID
                state_code = self._parse_state_identifier(arg)

                if state_code is not None:
                    # It's a state identifier
                    self._show_evaluations(state_filter=state_code)
                else:
                    # Try to parse as EVALID (6-digit number)
                    try:
                        evalid = int(arg)
                        if len(str(evalid)) == 6:
                            self.fia.clip_by_evalid(evalid)
                            self.console.print(
                                f"[green]✓ Selected EVALID: {evalid}[/green]"
                            )
                        else:
                            self.console.print(
                                f"[red]Invalid EVALID: {arg} (must be 6 digits)[/red]"
                            )
                    except ValueError:
                        self.console.print(f"[red]Invalid input: {arg}[/red]")
                        self.console.print("Use 'help evalid' for usage information")

        except Exception as e:
            self.console.print(f"[red]Error: {e}[/red]")

    def _show_evaluations(self, state_filter=None):
        """Show available evaluations with details."""
        # First, get all available states if no filter provided
        if state_filter is None:
            # Load POP_EVAL to get available states
            if "POP_EVAL" not in self.fia.tables:
                self.fia.load_table("POP_EVAL")

            pop_eval_all = self.fia.tables["POP_EVAL"].collect()
            available_states = sorted(pop_eval_all["STATECD"].unique().to_list())

            # Map state codes to names (simplified for common states)
            state_names = {
                1: "AL",
                2: "AK",
                4: "AZ",
                5: "AR",
                6: "CA",
                8: "CO",
                9: "CT",
                10: "DE",
                12: "FL",
                13: "GA",
                15: "HI",
                16: "ID",
                17: "IL",
                18: "IN",
                19: "IA",
                20: "KS",
                21: "KY",
                22: "LA",
                23: "ME",
                24: "MD",
                25: "MA",
                26: "MI",
                27: "MN",
                28: "MS",
                29: "MO",
                30: "MT",
                31: "NE",
                32: "NV",
                33: "NH",
                34: "NJ",
                35: "NM",
                36: "NY",
                37: "NC",
                38: "ND",
                39: "OH",
                40: "OK",
                41: "OR",
                42: "PA",
                44: "RI",
                45: "SC",
                46: "SD",
                47: "TN",
                48: "TX",
                49: "UT",
                50: "VT",
                51: "VA",
                53: "WA",
                54: "WV",
                55: "WI",
                56: "WY",
            }

            # Show available states
            self.console.print("\n[bold cyan]Available States:[/bold cyan]")
            states_display = []
            for state_code in available_states:
                state_abbr = state_names.get(state_code, f"Code {state_code}")
                states_display.append(f"{state_code:2d} ({state_abbr})")

            # Display in columns
            for i in range(0, len(states_display), 6):
                self.console.print("  " + "  ".join(states_display[i : i + 6]))

            self.console.print("\n[bold]Options:[/bold]")
            self.console.print("  - Enter a state (e.g., 37, NC, or North Carolina)")
            self.console.print("  - Enter 'all' to see all evaluations")
            self.console.print("  - Press Enter to cancel")

            choice = Prompt.ask("\nSelect state", default="cancel")

            if choice.lower() == "cancel" or choice == "":
                return
            elif choice.lower() == "all":
                state_filter = None
            else:
                # Try to parse state identifier
                state_code = self._parse_state_identifier(choice)
                if state_code is not None:
                    if state_code in available_states:
                        state_filter = state_code
                    else:
                        self.console.print(
                            f"[red]State {choice} (code {state_code}) not found in database[/red]"
                        )
                        return
                else:
                    self.console.print(f"[red]Invalid state: {choice}[/red]")
                    self.console.print(
                        "Try a state code (37), abbreviation (NC), or name (North Carolina)"
                    )
                    return

        # Now get evaluations for the selected state(s)
        if state_filter:
            evalids = self.fia.find_evalid(state=state_filter)
        else:
            evalids = self.fia.find_evalid()

        if not evalids:
            self.console.print("[yellow]No evaluations found[/yellow]")
            return

        # Load tables for details
        if "POP_EVAL" not in self.fia.tables:
            self.fia.load_table("POP_EVAL")
        if "POP_EVAL_TYP" not in self.fia.tables:
            self.fia.load_table("POP_EVAL_TYP")

        # Get evaluation details
        pop_eval = (
            self.fia.tables["POP_EVAL"]
            .filter(pl.col("EVALID").is_in(evalids))
            .collect()
        )

        # Get eval types - need to join through CN/EVAL_CN
        eval_typ = self.fia.tables["POP_EVAL_TYP"].collect()

        # Join POP_EVAL with POP_EVAL_TYP on CN = EVAL_CN
        eval_data = pop_eval.join(
            eval_typ.select(["EVAL_CN", "EVAL_TYP"]),
            left_on="CN",
            right_on="EVAL_CN",
            how="left",
        )

        # Create table
        eval_table = Table(
            title="Available Evaluations", show_lines=True, box=box.ROUNDED
        )
        eval_table.add_column("EVALID", style="cyan", width=8)
        eval_table.add_column("State", style="green", width=6)
        eval_table.add_column("Year", style="yellow", width=6)
        eval_table.add_column("Type", style="magenta", width=10)
        eval_table.add_column("Description", style="white", overflow="fold")

        for row in eval_data.iter_rows(named=True):
            eval_table.add_row(
                str(row["EVALID"]),
                str(row.get("STATECD", "")),
                str(row.get("END_INVYR", "")),
                str(row.get("EVAL_TYP", "")),
                str(row.get("EVAL_DESCR", "")),
            )

        self.console.print(eval_table)

        if self.fia.evalid:
            self.console.print(
                f"\n[bold]Current:[/bold] {', '.join(map(str, self.fia.evalid))}"
            )

    def do_area(self, arg: str):
        """Calculate forest area estimates.
        Usage:
            area                        - Forest area (acres and % of total land)
            area <state>                - Forest area for specific state
            area byLandType             - Area by land type categories
            area bySpecies              - Area by species
            area landType=timber        - Timber land only
            area grpBy=FORTYPCD         - Group by forest type
            area treeDomain="DIA > 10"  - Custom tree filter
            area variance               - Include variance (not SE)

        State can be specified as:
            - Abbreviation: area AL or area al
            - Full name: area Alabama or area alabama
            - Code: area 1

        Notes:
            - AREA shows total acres
            - AREA_PERC shows percentage of total land area
            - Use byLandType to see forest vs non-forest breakdown

        Examples:
            area                                    - Forest area (current EVALID)
            area AL                                 - Alabama forest area
            area NC byLandType                      - North Carolina land types
            area byLandType                         - All land types breakdown
            area bySpecies landType=timber          - Timber area by species
            area grpBy=OWNCD areaDomain="COND_STATUS_CD == 1"
        """
        # Handle 'area help' syntax
        if arg.strip().lower() == "help":
            self.do_help("area")
            return

        if not self._check_connection():
            return

        try:
            # Check if first argument is a state identifier
            args = arg.strip().split(maxsplit=1)
            state_code = None
            state_name = None

            if args:
                # Try to parse first argument as state
                potential_state = args[0]
                state_code = self._parse_state_identifier(potential_state)

                if state_code is not None:
                    # Found a state, use the rest as kwargs
                    state_name = potential_state
                    arg = args[1] if len(args) > 1 else ""

                    # Set evaluation to most recent for this state
                    original_evalid = self.fia.evalid
                    evalids = self.fia.find_evalid(state=state_code, most_recent=True)
                    if evalids:
                        self.fia.clip_by_evalid(evalids)
                    else:
                        self.console.print(
                            f"[red]No evaluations found for state: {state_name}[/red]"
                        )
                        return

            kwargs = self._parse_kwargs(arg)

            # Always include totals for area command unless explicitly set to False
            if "totals" not in kwargs:
                kwargs["totals"] = True

            # Convert camelCase to snake_case for area function parameters
            kwargs = self._convert_kwargs_to_snake_case(kwargs)

            # Note: AREA_PERC shows percentage of total area meeting criteria
            # When landType="forest" (default), it shows forest as % of total land
            # The percentage is most meaningful when using byLandType or other groupings

            # Build descriptive title
            title = "Forest Area Estimates"
            if state_name:
                title = f"Forest Area Estimates - {state_name.upper()}"
            if self.fia.evalid:
                title += f" (EVALID: {', '.join(map(str, self.fia.evalid))})"

            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=self.console,
            ) as progress:
                task = progress.add_task("Calculating forest area...", total=None)
                result = self.fia.area(**kwargs)
                progress.update(task, completed=True)

            self.last_result = result
            self._display_results(result, title)

            # Restore original evalid if we changed it
            if state_code is not None and "original_evalid" in locals():
                self.fia.evalid = original_evalid

        except Exception as e:
            self.console.print(f"[red]Error: {e}[/red]")

    def do_biomass(self, arg: str):
        """Calculate tree biomass estimates.
        Usage:
            biomass                     - Total biomass
            biomass bySpecies           - Biomass by species
            biomass component=AG        - Aboveground only (AG, BG, TOTAL)
            biomass bySizeClass         - By 2-inch diameter classes
            biomass treeType=live       - Live trees only

        Examples:
            biomass component=AG bySpecies
            biomass component=TOTAL landType=timber
        """
        # Handle 'biomass help' syntax
        if arg.strip().lower() == "help":
            self.do_help("biomass")
            return

        if not self._check_connection():
            return

        try:
            kwargs = self._parse_kwargs(arg)
            kwargs = self._convert_kwargs_to_snake_case(kwargs)

            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=self.console,
            ) as progress:
                task = progress.add_task("Calculating biomass...", total=None)
                result = self.fia.biomass(**kwargs)
                progress.update(task, completed=True)

            self.last_result = result
            self._display_results(result, "Biomass Estimates")

        except Exception as e:
            self.console.print(f"[red]Error: {e}[/red]")

    def do_volume(self, arg: str):
        """Calculate wood volume estimates.
        Usage:
            volume                      - Total volume
            volume bySpecies            - Volume by species
            volume volType=NET          - Volume type (GROSS, NET, SOUND)
            volume bySizeClass          - By diameter class
            volume treeType=live        - Live trees only

        Examples:
            volume volType=NET bySpecies
            volume volType=SOUND landType=timber
        """
        # Handle 'volume help' syntax
        if arg.strip().lower() == "help":
            self.do_help("volume")
            return

        if not self._check_connection():
            return

        try:
            kwargs = self._parse_kwargs(arg)
            kwargs = self._convert_kwargs_to_snake_case(kwargs)

            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=self.console,
            ) as progress:
                task = progress.add_task("Calculating volume...", total=None)
                result = self.fia.volume(**kwargs)
                progress.update(task, completed=True)

            self.last_result = result
            self._display_results(result, "Volume Estimates")

        except Exception as e:
            self.console.print(f"[red]Error: {e}[/red]")

    def do_tpa(self, arg: str):
        """Calculate trees per acre estimates.
        Usage:
            tpa                         - Total TPA
            tpa bySpecies               - TPA by species
            tpa bySizeClass             - By 2-inch diameter classes
            tpa treeType=live           - Tree type (live, dead, gs, all)
            tpa treeDomain="DIA >= 5"   - Custom filter

        Examples:
            tpa bySpecies treeType=live
            tpa bySizeClass landType=timber
        """
        # Handle 'tpa help' syntax
        if arg.strip().lower() == "help":
            self.do_help("tpa")
            return

        if not self._check_connection():
            return

        try:
            kwargs = self._parse_kwargs(arg)
            kwargs = self._convert_kwargs_to_snake_case(kwargs)

            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=self.console,
            ) as progress:
                task = progress.add_task("Calculating trees per acre...", total=None)
                result = self.fia.tpa(**kwargs)
                progress.update(task, completed=True)

            self.last_result = result
            self._display_results(result, "Trees Per Acre Estimates")

        except Exception as e:
            self.console.print(f"[red]Error: {e}[/red]")

    def do_mortality(self, arg: str):
        """Calculate mortality estimates.
        Usage:
            mortality                   - Total mortality
            mortality bySpecies         - Mortality by species
            mortality treeType=live     - Tree type filter

        Note: Requires GRM (Growth/Removal/Mortality) evaluation type.

        Examples:
            mortality bySpecies
            mortality grpBy=AGENTCD
        """
        # Handle 'mortality help' syntax
        if arg.strip().lower() == "help":
            self.do_help("mortality")
            return

        if not self._check_connection():
            return

        try:
            kwargs = self._parse_kwargs(arg)
            kwargs = self._convert_kwargs_to_snake_case(kwargs)

            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=self.console,
            ) as progress:
                task = progress.add_task("Calculating mortality...", total=None)
                result = self.fia.mortality(**kwargs)
                progress.update(task, completed=True)

            self.last_result = result
            self._display_results(result, "Mortality Estimates")

        except Exception as e:
            self.console.print(f"[red]Error: {e}[/red]")

    def do_show(self, arg: str):
        """Show details of last result.
        Usage:
            show            - Display last result
            show stats      - Show summary statistics
            show columns    - Show column information
            show n=50       - Show more rows
        """
        if not self.last_result:
            self.console.print(
                "[yellow]No results to show. Run an estimation command first.[/yellow]"
            )
            return

        try:
            if not arg:
                self._display_dataframe(self.last_result, "Last Result")
            elif arg == "stats":
                self._show_stats()
            elif arg == "columns":
                self._show_columns()
            elif arg.startswith("n="):
                n = int(arg.split("=")[1])
                self._display_dataframe(self.last_result, "Last Result", max_rows=n)
            else:
                self.console.print(f"[red]Unknown option: {arg}[/red]")

        except Exception as e:
            self.console.print(f"[red]Error: {e}[/red]")

    def _show_stats(self):
        """Show summary statistics for numeric columns."""
        df = self.last_result

        # Get numeric columns
        numeric_cols = [
            col
            for col in df.columns
            if df[col].dtype in [pl.Float32, pl.Float64, pl.Int32, pl.Int64]
        ]

        if not numeric_cols:
            self.console.print("[yellow]No numeric columns to summarize[/yellow]")
            return

        stats_table = Table(title="Summary Statistics", show_lines=True)
        stats_table.add_column("Column", style="cyan")
        stats_table.add_column("Mean", style="green")
        stats_table.add_column("Std Dev", style="yellow")
        stats_table.add_column("Min", style="blue")
        stats_table.add_column("Max", style="magenta")

        for col in numeric_cols:
            stats_table.add_row(
                col,
                f"{df[col].mean():.2f}" if df[col].mean() is not None else "N/A",
                f"{df[col].std():.2f}" if df[col].std() is not None else "N/A",
                f"{df[col].min():.2f}" if df[col].min() is not None else "N/A",
                f"{df[col].max():.2f}" if df[col].max() is not None else "N/A",
            )

        self.console.print(stats_table)

    def _show_columns(self):
        """Show column information."""
        df = self.last_result

        col_table = Table(title="Column Information", show_lines=True)
        col_table.add_column("Column", style="cyan")
        col_table.add_column("Type", style="green")
        col_table.add_column("Non-null Count", style="yellow")

        for col in df.columns:
            col_table.add_row(col, str(df[col].dtype), str(df[col].count()))

        self.console.print(col_table)

    def do_export(self, arg: str):
        """Export last result to file.
        Usage:
            export results.csv          - Export to CSV
            export results.parquet      - Export to Parquet
            export results.xlsx         - Export to Excel (requires openpyxl)
        """
        if not self.last_result:
            self.console.print(
                "[yellow]No results to export. Run an estimation command first.[/yellow]"
            )
            return

        if not arg:
            self.console.print("[red]Error: Please specify output filename[/red]")
            return

        try:
            filename = Path(arg.strip())

            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=self.console,
            ) as progress:
                task = progress.add_task(
                    f"Exporting to {filename.suffix}...", total=None
                )

                if filename.suffix.lower() == ".csv":
                    self.last_result.write_csv(filename)
                elif filename.suffix.lower() == ".parquet":
                    self.last_result.write_parquet(filename)
                elif filename.suffix.lower() == ".xlsx":
                    self.last_result.write_excel(filename)
                else:
                    self.console.print(
                        "[red]Error: Unsupported format. Use .csv, .parquet, or .xlsx[/red]"
                    )
                    return

                progress.update(task, completed=True)

            self.console.print(
                f"[green]✓ Exported {len(self.last_result)} rows to: {filename}[/green]"
            )

        except Exception as e:
            self.console.print(f"[red]Error: {e}[/red]")

    def do_recent(self, arg: str):
        """Show or connect to recent databases.
        Usage:
            recent              - Show recent databases
            recent <number>     - Connect to database from list
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
                self.console.print(
                    f"[red]Invalid selection. Choose 1-{len(recent)}[/red]"
                )
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
            shortcut                    - Show all shortcuts
            shortcut add NC <path>      - Add shortcut for NC
            shortcut remove NC          - Remove shortcut
            shortcut NC                 - Connect using shortcut
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

        elif args[0] == "add" and len(args) == 3:
            # Add shortcut
            state, path = args[1].upper(), args[2]
            if Path(path).exists():
                self.config.add_state_shortcut(state, path)
                self.console.print(
                    f"[green]✓ Added shortcut: {state} → {Path(path).name}[/green]"
                )
            else:
                self.console.print(f"[red]Error: Database not found: {path}[/red]")

        elif args[0] == "remove" and len(args) == 2:
            # Remove shortcut
            state = args[1].upper()
            if self.config.remove_state_shortcut(state):
                self.console.print(f"[yellow]Removed shortcut: {state}[/yellow]")
            else:
                self.console.print(f"[red]Shortcut not found: {state}[/red]")

        elif len(args) == 1:
            # Use shortcut to connect
            state = args[0].upper()
            shortcuts = self.config.state_shortcuts
            if state in shortcuts:
                self.do_connect(shortcuts[state])
            else:
                self.console.print(f"[red]Shortcut not found: {state}[/red]")
                self.console.print("Use 'shortcut' to see available shortcuts")

    def do_setdefault(self, arg: str):
        """Set default database.
        Usage:
            setdefault              - Set current database as default
            setdefault <path>       - Set specific database as default
            setdefault none         - Clear default database
        """
        if arg.strip() == "none":
            self.config.default_database = None
            self.config.save_config()
            self.console.print("[yellow]Default database cleared[/yellow]")
        elif arg.strip():
            # Set specific path as default
            path = Path(arg.strip())
            if path.exists():
                self.config.default_database = str(path)
                self.console.print(
                    f"[green]✓ Default database set to: {path.name}[/green]"
                )
            else:
                self.console.print(f"[red]Error: Database not found: {path}[/red]")
        elif self.db_path:
            # Set current database as default
            self.config.default_database = str(self.db_path)
            self.console.print(
                f"[green]✓ Default database set to: {self.db_path.name}[/green]"
            )
        else:
            self.console.print("[red]No database connected[/red]")

    def do_clear(self, arg: str):
        """Clear the screen."""
        os.system("clear" if os.name == "posix" else "cls")

    def do_exit(self, arg: str):
        """Exit pyFIA CLI."""
        if Confirm.ask("Exit pyFIA?"):
            self.console.print("[green]Goodbye! 🌲[/green]")
            return True

    def do_quit(self, arg: str):
        """Exit pyFIA CLI."""
        return self.do_exit(arg)

    def default(self, line: str):
        """Handle unknown commands."""
        if line.strip() == "?":
            self.do_help("")
        else:
            self.console.print(f"[red]Unknown command: {line}[/red]")
            self.console.print("Type 'help' for available commands.")

    def do_help(self, arg: str):
        """Show help for commands."""
        if arg:
            # Show help for specific command
            try:
                func = getattr(self, "do_" + arg)
                doc = func.__doc__ or "No help available"
                # Format the docstring nicely
                lines = doc.strip().split("\n")
                formatted_lines = [line.strip() for line in lines]
                self.console.print(
                    Panel(
                        "\n".join(formatted_lines),
                        title=f"Help: {arg}",
                        border_style="blue",
                    )
                )
            except AttributeError:
                self.console.print(f"[red]Unknown command: {arg}[/red]")
        else:
            # Show list of available commands with brief descriptions
            self.console.print("\n[bold cyan]Available Commands:[/bold cyan]\n")

            # Database commands
            self.console.print("[bold yellow]Database Commands:[/bold yellow]")
            self.console.print("  connect <path>     - Connect to FIA DuckDB database")
            self.console.print("  evalid             - Manage EVALID selection")
            self.console.print(
                "  recent             - Show/connect to recent databases"
            )
            self.console.print("  shortcut           - Manage state shortcuts")
            self.console.print()

            # Estimation commands
            self.console.print("[bold yellow]Estimation Commands:[/bold yellow]")
            self.console.print("  area               - Calculate forest area")
            self.console.print("  biomass            - Calculate tree biomass")
            self.console.print("  volume             - Calculate wood volume")
            self.console.print("  tpa                - Calculate trees per acre")
            self.console.print("  mortality          - Calculate mortality")
            self.console.print()

            # Data commands
            self.console.print("[bold yellow]Data Commands:[/bold yellow]")
            self.console.print("  show               - Display last results")
            self.console.print("  export <file>      - Export to CSV/Parquet/Excel")
            self.console.print("  clear              - Clear screen")
            self.console.print()

            # Settings
            self.console.print("[bold yellow]Settings:[/bold yellow]")
            self.console.print("  setdefault         - Set default database")
            self.console.print()

            # Other
            self.console.print("[bold yellow]Other:[/bold yellow]")
            self.console.print("  help <command>     - Show detailed help for command")
            self.console.print("  exit/quit          - Exit pyFIA")
            self.console.print()

    # Utility methods
    def _check_connection(self) -> bool:
        """Check if database is connected."""
        if not self.fia:
            self.console.print("[red]No database connected. Use 'connect <path>'[/red]")
            return False
        return True

    def _parse_state_identifier(self, identifier: str) -> Optional[int]:
        """Parse state identifier (code, abbreviation, or name) to state code.

        Args:
            identifier: State identifier (e.g., "37", "NC", "North Carolina")

        Returns:
            State code if found, None otherwise
        """
        # State mappings
        state_abbr_to_code = {
            "AL": 1,
            "AK": 2,
            "AZ": 4,
            "AR": 5,
            "CA": 6,
            "CO": 8,
            "CT": 9,
            "DE": 10,
            "FL": 12,
            "GA": 13,
            "HI": 15,
            "ID": 16,
            "IL": 17,
            "IN": 18,
            "IA": 19,
            "KS": 20,
            "KY": 21,
            "LA": 22,
            "ME": 23,
            "MD": 24,
            "MA": 25,
            "MI": 26,
            "MN": 27,
            "MS": 28,
            "MO": 29,
            "MT": 30,
            "NE": 31,
            "NV": 32,
            "NH": 33,
            "NJ": 34,
            "NM": 35,
            "NY": 36,
            "NC": 37,
            "ND": 38,
            "OH": 39,
            "OK": 40,
            "OR": 41,
            "PA": 42,
            "RI": 44,
            "SC": 45,
            "SD": 46,
            "TN": 47,
            "TX": 48,
            "UT": 49,
            "VT": 50,
            "VA": 51,
            "WA": 53,
            "WV": 54,
            "WI": 55,
            "WY": 56,
            "PR": 72,
            "VI": 78,
        }

        state_name_to_code = {
            "alabama": 1,
            "alaska": 2,
            "arizona": 4,
            "arkansas": 5,
            "california": 6,
            "colorado": 8,
            "connecticut": 9,
            "delaware": 10,
            "florida": 12,
            "georgia": 13,
            "hawaii": 15,
            "idaho": 16,
            "illinois": 17,
            "indiana": 18,
            "iowa": 19,
            "kansas": 20,
            "kentucky": 21,
            "louisiana": 22,
            "maine": 23,
            "maryland": 24,
            "massachusetts": 25,
            "michigan": 26,
            "minnesota": 27,
            "mississippi": 28,
            "missouri": 29,
            "montana": 30,
            "nebraska": 31,
            "nevada": 32,
            "new hampshire": 33,
            "new jersey": 34,
            "new mexico": 35,
            "new york": 36,
            "north carolina": 37,
            "north dakota": 38,
            "ohio": 39,
            "oklahoma": 40,
            "oregon": 41,
            "pennsylvania": 42,
            "rhode island": 44,
            "south carolina": 45,
            "south dakota": 46,
            "tennessee": 47,
            "texas": 48,
            "utah": 49,
            "vermont": 50,
            "virginia": 51,
            "washington": 53,
            "west virginia": 54,
            "wisconsin": 55,
            "wyoming": 56,
            "puerto rico": 72,
            "virgin islands": 78,
        }

        # Remove quotes if present
        identifier = identifier.strip().strip('"').strip("'")

        # Try numeric code first
        try:
            code = int(identifier)
            if 1 <= code <= 78:  # Valid state code range
                return code
        except ValueError:
            pass

        # Try abbreviation (case insensitive)
        upper_id = identifier.upper()
        if upper_id in state_abbr_to_code:
            return state_abbr_to_code[upper_id]

        # Try full name (case insensitive)
        lower_id = identifier.lower()
        if lower_id in state_name_to_code:
            return state_name_to_code[lower_id]

        # Not found
        return None

    def _parse_kwargs(self, arg: str) -> Dict[str, Any]:
        """Parse command arguments into kwargs."""
        kwargs = {}
        if not arg:
            return kwargs

        # Handle boolean flags
        for flag in [
            "bySpecies",
            "bySizeClass",
            "byLandType",
            "mostRecent",
            "totals",
            "variance",
        ]:
            if flag in arg:
                kwargs[flag] = True
                arg = arg.replace(flag, "")

        # Parse key=value pairs
        import shlex

        try:
            parts = shlex.split(arg)
        except ValueError:
            parts = arg.split()

        for part in parts:
            if "=" in part:
                key, value = part.split("=", 1)
                # Handle quoted strings
                if value.startswith('"') and value.endswith('"'):
                    value = value[1:-1]
                elif value.startswith("'") and value.endswith("'"):
                    value = value[1:-1]
                else:
                    # Try to convert to appropriate type
                    try:
                        value = int(value)
                    except ValueError:
                        try:
                            value = float(value)
                        except ValueError:
                            pass  # Keep as string
                kwargs[key] = value

        return kwargs

    def _convert_kwargs_to_snake_case(self, kwargs: Dict[str, Any]) -> Dict[str, Any]:
        """Convert camelCase kwargs to snake_case for FIA functions."""
        conversions = {
            "byLandType": "by_land_type",
            "bySpecies": "by_species",
            "bySizeClass": "by_size_class",
            "landType": "land_type",
            "treeDomain": "tree_domain",
            "areaDomain": "area_domain",
            "treeType": "tree_type",
            "volType": "vol_type",
            "mostRecent": "most_recent",
        }

        for camel, snake in conversions.items():
            if camel in kwargs:
                kwargs[snake] = kwargs.pop(camel)

        return kwargs

    def _display_dataframe(self, df: pl.DataFrame, title: str = "", max_rows: int = 20):
        """Display a Polars DataFrame as a rich table."""
        if df.is_empty():
            self.console.print("[yellow]No data to display[/yellow]")
            return

        # Create table
        table = Table(title=title, show_lines=True, box=box.ROUNDED)

        # Add columns
        for col in df.columns:
            # Style numeric columns differently
            if df[col].dtype in [pl.Float32, pl.Float64]:
                table.add_column(col, style="cyan", justify="right")
            elif df[col].dtype in [pl.Int32, pl.Int64]:
                table.add_column(col, style="green", justify="right")
            else:
                table.add_column(col, style="white")

        # Format and add rows
        for row in df.head(max_rows).iter_rows():
            formatted_row = []
            for i, val in enumerate(row):
                if val is None:
                    formatted_row.append("NULL")
                elif isinstance(val, float):
                    # Format floats with appropriate precision
                    if abs(val) < 0.01 and val != 0:
                        formatted_row.append(f"{val:.2e}")
                    else:
                        formatted_row.append(f"{val:.2f}")
                else:
                    formatted_row.append(str(val))
            table.add_row(*formatted_row)

        self.console.print(table)

        if len(df) > max_rows:
            self.console.print(
                f"[dim]Showing {max_rows} of {len(df)} rows. Use 'export' to save all data.[/dim]"
            )

    def _display_results(self, result: pl.DataFrame, title: str):
        """Display estimation results with proper formatting."""
        if result.is_empty():
            self.console.print("[yellow]No results found[/yellow]")
            return

        # Check for standard estimation columns
        any(col in result.columns for col in ["ESTIMATE", "TOTAL", "TPA", "BIOMASS_AG"])
        any(col in result.columns for col in ["SE", "SE_PERCENT", "VAR"])

        self._display_dataframe(result, title)

        # Show column explanations if needed
        explanations = []

        # Area-specific explanations
        if "AREA" in result.columns:
            explanations.append("AREA = Total acres")
        if "AREA_PERC" in result.columns:
            if "LAND_TYPE" in result.columns:
                explanations.append("AREA_PERC = Percentage of total land area")
            else:
                explanations.append(
                    "AREA_PERC = Forest/criteria area as % of total land"
                )

        # Standard error explanations
        if "SE" in result.columns:
            explanations.append("SE = Standard Error")
        if "SE_PERCENT" in result.columns:
            explanations.append("SE% = Standard Error as % of estimate")
        if "AREA_SE" in result.columns:
            explanations.append("AREA_SE = Standard Error of area estimate")
        if "AREA_PERC_SE" in result.columns:
            explanations.append("AREA_PERC_SE = Standard Error of percentage")

        # Other columns
        if "N_PLOTS" in result.columns:
            explanations.append("N_PLOTS = Number of plots used")
        if "nPlots" in result.columns:
            explanations.append("nPlots = Number of plots used")

        if explanations:
            self.console.print(f"\n[dim]{', '.join(explanations)}[/dim]")

        # Add interpretation help for area results
        if "AREA_PERC" in result.columns and "LAND_TYPE" in result.columns:
            self.console.print(
                "\n[dim]Note: Percentages show each land type as a portion of total evaluated land area[/dim]"
            )


def main():
    """Main entry point for pyFIA direct CLI."""
    import argparse

    parser = argparse.ArgumentParser(
        description="pyFIA Direct CLI - Programmatic access to FIA estimation methods"
    )
    parser.add_argument(
        "database", nargs="?", help="Path to FIA database (optional if default is set)"
    )

    args = parser.parse_args()

    try:
        cli = FIADirectCLI(args.database)
        cli.cmdloop()
    except KeyboardInterrupt:
        Console().print("\n[yellow]Interrupted. Use 'exit' to quit properly.[/yellow]")
    except Exception as e:
        Console().print(f"[red]Fatal error: {e}[/red]")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
