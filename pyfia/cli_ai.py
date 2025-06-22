#!/usr/bin/env python3
"""
AI-Enhanced CLI for pyFIA - Natural language queries and SQL generation.

This module provides an AI-powered command-line interface for:
- Natural language to SQL translation
- Direct SQL query execution
- Integration with AI agents (basic, enhanced, or Cognee)
- Rich formatting for results and AI responses
"""

import atexit
import cmd
import os
import sys
from pathlib import Path

# Optional readline import for Windows compatibility
try:
    import readline
    HAS_READLINE = True
except ImportError:
    HAS_READLINE = False
from typing import Optional

# Load environment variables
try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

import polars as pl
from rich import box
from rich.console import Console
from rich.markdown import Markdown
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.prompt import Confirm
from rich.syntax import Syntax
from rich.table import Table

from pyfia.cli_config import CLIConfig
from pyfia.duckdb_query_interface import DuckDBQueryInterface


class FIAAICli(cmd.Cmd):
    """AI-enhanced CLI for natural language FIA queries."""

    intro = None
    prompt = "fia-ai> "

    def __init__(self, db_path: Optional[str] = None, agent_type: str = "basic"):
        super().__init__()
        self.console = Console()
        self.db_path: Optional[Path] = None
        self.query_interface: Optional[DuckDBQueryInterface] = None
        self.last_result: Optional[pl.DataFrame] = None
        self.query_history = []
        self.history_file = Path.home() / ".fia_ai_history"
        self.config = CLIConfig()
        self.agent_type = agent_type
        self.agent = None

        # Check API key
        self.api_key = os.environ.get("OPENAI_API_KEY")
        if not self.api_key:
            self.console.print(
                "[yellow]Warning: OPENAI_API_KEY not set. Some features will be limited.[/yellow]"
            )

        # Setup command history
        self._setup_history()

        # Display welcome
        self._show_welcome()

        # Auto-connect if provided
        if db_path:
            self.do_connect(db_path)
        elif self.config.default_database:
            self.console.print("[cyan]Auto-connecting to default database...[/cyan]")
            self.do_connect(self.config.default_database)

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

    def _show_welcome(self):
        """Display welcome message."""
        welcome_text = """
# 🤖 pyFIA AI Assistant

Natural language interface for Forest Inventory Analysis data.

## Features:
• **Natural Language Queries**: Ask questions in plain English
• **SQL Generation**: AI translates questions to SQL
• **Direct SQL**: Execute SQL queries with syntax highlighting
• **Schema Awareness**: AI understands FIA database structure
• **Export Results**: Save query results to CSV/Parquet

## Quick Start:
• Just type your question: "How many live oak trees are in North Carolina?"
• Use `sql:` prefix for direct SQL queries
• Type `help` for all commands

## Examples:
• "Show me the total forest area by state"
• "What's the average DBH of pine trees?"
• "Find plots with high biomass in evaluation 372301"
• "sql: SELECT COUNT(*) FROM TREE WHERE STATUSCD = 1"
"""

        # Status
        status_items = []
        if self.api_key:
            status_items.append("AI: ✓ Ready")
        else:
            status_items.append("AI: ✗ No API key")

        status_items.append(f"Agent: {self.agent_type}")

        if self.db_path:
            status_items.append(f"DB: {self.db_path.name}")

        self.console.print(
            Panel(
                Markdown(welcome_text),
                title="pyFIA AI Assistant",
                subtitle=f"[dim]{' | '.join(status_items)}[/dim]",
                border_style="blue",
            )
        )

    def do_connect(self, arg: str):
        """Connect to FIA database.
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
                console=self.console,
            ) as progress:
                task = progress.add_task("Connecting to database...", total=None)

                # Initialize query interface
                self.query_interface = DuckDBQueryInterface(str(db_path))
                self.db_path = db_path

                # Initialize AI agent if API key available
                if self.api_key:
                    self._initialize_agent()

                progress.update(task, completed=True)

            self.console.print(f"[green]✓ Connected to: {db_path.name}[/green]")

            # Save to recent
            self.config.add_recent_database(str(db_path))

            # Show database info
            self._show_db_info()

        except Exception as e:
            self.console.print(f"[red]Error: {e}[/red]")

    def _initialize_agent(self):
        """Initialize the appropriate AI agent."""
        try:
            if self.agent_type == "cognee":
                from pyfia.cognee_fia_agent import CogneeFIAAgent

                self.agent = CogneeFIAAgent(self.db_path)
                self.console.print("[green]✓ Cognee AI agent initialized[/green]")

            elif self.agent_type == "enhanced":
                from pyfia.ai_agent_enhanced import FIAAgentConfig, FIAAgentEnhanced

                config = FIAAgentConfig(db_path=self.db_path)
                self.agent = FIAAgentEnhanced(config)
                self.console.print("[green]✓ Enhanced AI agent initialized[/green]")

            else:  # basic
                from pyfia.ai_agent import FIAAgent, FIAAgentConfig

                config = FIAAgentConfig(db_path=self.db_path, api_key=self.api_key)
                self.agent = FIAAgent(self.db_path, config)
                self.console.print("[green]✓ Basic AI agent initialized[/green]")

        except Exception as e:
            self.console.print(f"[yellow]Could not initialize AI agent: {e}[/yellow]")
            self.console.print("[yellow]SQL queries will still work[/yellow]")
            self.agent = None

    def _show_db_info(self):
        """Show database information."""
        if not self.query_interface:
            return

        try:
            # Get schema info
            schema = self.query_interface.get_database_schema()

            # Get EVALID info
            evalid_info = self.query_interface.get_evalid_info()

            # Create summary table
            info_table = Table(title="Database Overview", box=box.ROUNDED)
            info_table.add_column("Property", style="cyan")
            info_table.add_column("Value", style="yellow")

            info_table.add_row("Database", self.db_path.name)
            info_table.add_row("Total Tables", str(len(schema)))
            info_table.add_row("Total Evaluations", str(len(evalid_info)))

            # Get largest tables
            table_sizes = [(name, info.row_count) for name, info in schema.items()]
            table_sizes.sort(key=lambda x: x[1], reverse=True)
            top_tables = ", ".join(
                [f"{name} ({count:,})" for name, count in table_sizes[:3]]
            )
            info_table.add_row("Largest Tables", top_tables)

            self.console.print(info_table)

        except Exception as e:
            self.console.print(f"[yellow]Could not load database info: {e}[/yellow]")

    def default(self, line: str):
        """Handle natural language queries and SQL."""
        line = line.strip()
        if not line:
            return

        # Check for SQL prefix
        if line.lower().startswith("sql:") or line.upper().startswith("SQL "):
            sql_query = line[4:].strip()
            self._execute_sql(sql_query)
        else:
            # Natural language query
            self._execute_natural_language(line)

    def _execute_natural_language(self, query: str):
        """Execute natural language query using AI."""
        if not self._check_connection():
            return

        if not self.api_key:
            self.console.print(
                "[yellow]Natural language queries require an OpenAI API key.[/yellow]"
            )
            self.console.print("Set: export OPENAI_API_KEY='your-key'")
            self.console.print(
                "\n[dim]You can still use 'sql:' prefix for direct SQL queries[/dim]"
            )
            return

        if not self.agent:
            self.console.print(
                "[yellow]AI agent not initialized. Trying to initialize now...[/yellow]"
            )
            self._initialize_agent()
            if not self.agent:
                return

        try:
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=self.console,
            ) as progress:
                task = progress.add_task("Processing query...", total=None)

                # Different handling for different agent types
                if self.agent_type == "cognee":
                    response = self.agent.query_sync(query)
                else:
                    result = self.agent.query(query)
                    if isinstance(result, pl.DataFrame):
                        self.last_result = result
                        response = (
                            f"Query executed successfully. Found {len(result)} results."
                        )
                    else:
                        response = str(result)

                progress.update(task, completed=True)

            # Display response
            self.console.print("\n[bold green]Response:[/bold green]")

            # Check if response contains SQL
            if "```sql" in response:
                # Extract and highlight SQL
                parts = response.split("```sql")
                self.console.print(parts[0])

                for i in range(1, len(parts)):
                    sql_part = parts[i].split("```")[0]
                    remaining = parts[i].split("```")[1] if "```" in parts[i] else ""

                    syntax = Syntax(
                        sql_part.strip(), "sql", theme="monokai", line_numbers=False
                    )
                    self.console.print(syntax)

                    if remaining:
                        self.console.print(remaining)
            else:
                self.console.print(Panel(response, border_style="green"))

            # If we have results, display them
            if self.last_result is not None and not self.last_result.is_empty():
                self.console.print(
                    f"\n[green]Query returned {len(self.last_result)} rows[/green]"
                )
                self._display_dataframe(self.last_result, max_rows=10)

            # Add to history
            self.query_history.append(
                {
                    "type": "natural",
                    "question": query,
                    "response": response[:200] + "..."
                    if len(response) > 200
                    else response,
                }
            )

        except Exception as e:
            self.console.print(f"[red]Error: {e}[/red]")

    def _execute_sql(self, query: str):
        """Execute SQL query directly."""
        if not self._check_connection():
            return

        try:
            # Show SQL with syntax highlighting
            self.console.print("\n[bold]SQL Query:[/bold]")
            syntax = Syntax(query, "sql", theme="monokai", line_numbers=False)
            self.console.print(syntax)

            # Execute
            with self.console.status("[bold green]Executing query..."):
                result = self.query_interface.execute_query(query)

            if result.is_empty():
                self.console.print("\n[yellow]No results found.[/yellow]")
            else:
                self.console.print(
                    f"\n[green]Query returned {len(result)} rows[/green]"
                )
                self.last_result = result
                self._display_dataframe(result)

                # Add to history
                self.query_history.append(
                    {"type": "sql", "query": query, "rows": len(result)}
                )

        except Exception as e:
            self.console.print(f"\n[red]Query error: {e}[/red]")

    def do_schema(self, arg: str):
        """Show database schema.
        Usage:
            schema              - Show all tables
            schema TREE         - Show specific table details
            schema tree sample  - Show table with sample data
        """
        if not self._check_connection():
            return

        args = arg.strip().split()
        table_name = args[0].upper() if args else None
        show_sample = len(args) > 1 and args[1].lower() == "sample"

        try:
            if table_name:
                self._show_table_details(table_name, show_sample)
            else:
                self._show_all_tables()

        except Exception as e:
            self.console.print(f"[red]Error: {e}[/red]")

    def _show_table_details(self, table_name: str, show_sample: bool = False):
        """Show details for a specific table."""
        info = self.query_interface._get_table_info(table_name)

        self.console.print(f"\n[bold cyan]Table: {info.name}[/bold cyan]")
        if info.description:
            self.console.print(f"[italic]{info.description}[/italic]")
        self.console.print(f"Rows: {info.row_count:,}\n")

        # Columns
        col_table = Table(title="Columns", show_lines=True, box=box.ROUNDED)
        col_table.add_column("Name", style="cyan")
        col_table.add_column("Type", style="green")
        col_table.add_column("Nullable", style="yellow")

        for col in info.columns[:30]:
            col_table.add_row(
                col["name"], col["type"], "Yes" if col.get("nullable", True) else "No"
            )

        if len(info.columns) > 30:
            col_table.add_row(
                "...", f"[dim]+{len(info.columns) - 30} more[/dim]", "..."
            )

        self.console.print(col_table)

        # Sample data
        if show_sample and info.sample_data is not None:
            self.console.print("\n[bold]Sample Data:[/bold]")
            self._display_dataframe(info.sample_data, max_rows=5)

    def _show_all_tables(self):
        """Show all database tables."""
        schema = self.query_interface.get_database_schema()

        # Group tables
        categories = {
            "Core": ["PLOT", "TREE", "COND", "SUBPLOT"],
            "Population": ["POP_EVAL", "POP_STRATUM", "POP_PLOT_STRATUM_ASSGN"],
            "Reference": ["REF_SPECIES", "REF_FOREST_TYPE"],
            "Other": [],
        }

        # Categorize
        categorized = set()
        for category in categories:
            if category != "Other":
                for table in categories[category]:
                    if table in schema:
                        categorized.add(table)

        categories["Other"] = [t for t in schema if t not in categorized]

        # Display
        for category, tables in categories.items():
            if not tables:
                continue

            cat_table = Table(
                title=f"{category} Tables", show_lines=True, box=box.SIMPLE
            )
            cat_table.add_column("Table", style="cyan")
            cat_table.add_column("Rows", style="green", justify="right")

            for table_name in sorted(tables):
                if table_name in schema:
                    info = schema[table_name]
                    cat_table.add_row(table_name, f"{info.row_count:,}")

            if cat_table.row_count > 0:
                self.console.print(cat_table)
                self.console.print()

    def do_evalid(self, arg: str):
        """Show available evaluations.
        Usage: evalid [search_term]
        """
        if not self._check_connection():
            return

        try:
            evalid_info = self.query_interface.get_evalid_info()

            # Filter if search term provided
            if arg:
                search = arg.lower()
                evalid_info = {
                    k: v
                    for k, v in evalid_info.items()
                    if search in str(k).lower()
                    or search in v.state_name.lower()
                    or search in v.eval_typ.lower()
                }

            if not evalid_info:
                self.console.print("[yellow]No evaluations found[/yellow]")
                return

            # Create table
            eval_table = Table(title="Available Evaluations", show_lines=True)
            eval_table.add_column("EVALID", style="cyan")
            eval_table.add_column("State", style="green")
            eval_table.add_column("Year", style="yellow")
            eval_table.add_column("Type", style="magenta")
            eval_table.add_column("Plots", style="blue", justify="right")

            for evalid, info in sorted(evalid_info.items())[:50]:
                eval_table.add_row(
                    str(evalid),
                    info.state_name,
                    str(info.end_year),
                    info.eval_typ,
                    f"{info.plot_count:,}",
                )

            self.console.print(eval_table)

            if len(evalid_info) > 50:
                self.console.print(
                    f"[dim]Showing 50 of {len(evalid_info)} evaluations[/dim]"
                )

        except Exception as e:
            self.console.print(f"[red]Error: {e}[/red]")

    def do_history(self, arg: str):
        """Show query history.
        Usage:
            history         - Show recent queries
            history clear   - Clear history
        """
        if arg.strip() == "clear":
            self.query_history.clear()
            self.console.print("[yellow]Query history cleared[/yellow]")
            return

        if not self.query_history:
            self.console.print("[yellow]No queries in history[/yellow]")
            return

        hist_table = Table(title="Query History", show_lines=True)
        hist_table.add_column("#", style="cyan", width=4)
        hist_table.add_column("Type", style="green")
        hist_table.add_column("Query/Question", style="white")
        hist_table.add_column("Result", style="yellow")

        for i, item in enumerate(self.query_history[-20:], 1):
            if item["type"] == "sql":
                hist_table.add_row(
                    str(i),
                    "SQL",
                    item["query"][:60] + "..."
                    if len(item["query"]) > 60
                    else item["query"],
                    f"{item['rows']} rows",
                )
            else:
                hist_table.add_row(
                    str(i),
                    "Natural",
                    item["question"][:60] + "..."
                    if len(item["question"]) > 60
                    else item["question"],
                    "Answered",
                )

        self.console.print(hist_table)

    def do_export(self, arg: str):
        """Export last result to file.
        Usage:
            export results.csv      - Export to CSV
            export results.parquet  - Export to Parquet
        """
        if not self.last_result:
            self.console.print(
                "[yellow]No results to export. Run a query first.[/yellow]"
            )
            return

        if not arg:
            self.console.print("[red]Error: Please specify output filename[/red]")
            return

        try:
            filename = Path(arg.strip())

            if filename.suffix.lower() == ".csv":
                self.last_result.write_csv(filename)
            elif filename.suffix.lower() == ".parquet":
                self.last_result.write_parquet(filename)
            else:
                self.console.print(
                    "[red]Error: Unsupported format. Use .csv or .parquet[/red]"
                )
                return

            self.console.print(
                f"[green]✓ Exported {len(self.last_result)} rows to: {filename}[/green]"
            )

        except Exception as e:
            self.console.print(f"[red]Error: {e}[/red]")

    def do_show(self, arg: str):
        """Show last query results.
        Usage:
            show        - Show last results
            show 50     - Show more rows
        """
        if not self.last_result:
            self.console.print(
                "[yellow]No results to show. Run a query first.[/yellow]"
            )
            return

        try:
            max_rows = int(arg) if arg.isdigit() else 20
            self._display_dataframe(self.last_result, "Last Query Results", max_rows)
        except Exception as e:
            self.console.print(f"[red]Error: {e}[/red]")

    def do_agent(self, arg: str):
        """Switch AI agent type.
        Usage:
            agent               - Show current agent
            agent basic         - Use basic agent
            agent enhanced      - Use enhanced agent (with RAG)
            agent cognee        - Use Cognee agent (with memory)
        """
        if not arg:
            self.console.print(f"[cyan]Current agent: {self.agent_type}[/cyan]")
            return

        agent_type = arg.strip().lower()
        if agent_type not in ["basic", "enhanced", "cognee"]:
            self.console.print(
                "[red]Invalid agent type. Choose: basic, enhanced, or cognee[/red]"
            )
            return

        self.agent_type = agent_type
        self.agent = None

        if self.db_path and self.api_key:
            self._initialize_agent()
        else:
            self.console.print(f"[yellow]Agent type set to: {agent_type}[/yellow]")
            self.console.print(
                "[yellow]Connect to a database to initialize the agent[/yellow]"
            )

    def do_help(self, arg: str):
        """Show help information."""
        if arg:
            try:
                func = getattr(self, "do_" + arg)
                self.console.print(
                    Panel(
                        func.__doc__ or "No help available",
                        title=f"Help: {arg}",
                        border_style="blue",
                    )
                )
            except AttributeError:
                self.console.print(f"[red]Unknown command: {arg}[/red]")
        else:
            help_text = """
## Natural Language Queries
Just type your question naturally:
• "How many live trees are there by species?"
• "What's the total forest area in North Carolina?"
• "Show me plots with high biomass"

## SQL Queries
Use `sql:` prefix for direct SQL:
• `sql: SELECT COUNT(*) FROM TREE WHERE STATUSCD = 1`

## Commands
• **connect** <path> - Connect to database
• **schema** [table] - View database schema
• **evalid** [search] - Show evaluations
• **history** - View query history
• **export** <file> - Export results
• **show** [n] - Show last results
• **agent** [type] - Switch AI agent
• **clear** - Clear screen
• **exit** - Exit the CLI

## Tips
• Natural language queries require OPENAI_API_KEY
• Use tab completion for commands
• Query results are automatically formatted
"""
            self.console.print(
                Panel(
                    Markdown(help_text),
                    title="pyFIA AI Assistant Help",
                    border_style="blue",
                )
            )

    def do_clear(self, arg: str):
        """Clear the screen."""
        os.system("clear" if os.name == "posix" else "cls")

    def do_exit(self, arg: str):
        """Exit the AI CLI."""
        if Confirm.ask("Exit pyFIA AI Assistant?"):
            self.console.print("[green]Goodbye! 🌲[/green]")
            return True

    def do_quit(self, arg: str):
        """Exit the AI CLI."""
        return self.do_exit(arg)

    # Utility methods
    def _check_connection(self) -> bool:
        """Check if database is connected."""
        if not self.query_interface:
            self.console.print("[red]No database connected. Use 'connect <path>'[/red]")
            return False
        return True

    def _display_dataframe(self, df: pl.DataFrame, title: str = "", max_rows: int = 20):
        """Display a Polars DataFrame as a rich table."""
        if df.is_empty():
            self.console.print("[yellow]No data to display[/yellow]")
            return

        # Create table
        table = Table(title=title, show_lines=True, box=box.ROUNDED)

        # Add columns
        for col in df.columns:
            if df[col].dtype in [pl.Float32, pl.Float64]:
                table.add_column(col, style="cyan", justify="right")
            elif df[col].dtype in [pl.Int32, pl.Int64]:
                table.add_column(col, style="green", justify="right")
            else:
                table.add_column(col, style="white")

        # Add rows
        for row in df.head(max_rows).iter_rows():
            formatted_row = []
            for val in row:
                if val is None:
                    formatted_row.append("NULL")
                elif isinstance(val, float):
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
                f"[dim]Showing {max_rows} of {len(df)} rows. Use 'export' to save all.[/dim]"
            )


def main():
    """Main entry point for pyFIA AI CLI."""
    import argparse

    parser = argparse.ArgumentParser(
        description="pyFIA AI Assistant - Natural language interface for FIA data"
    )
    parser.add_argument("database", nargs="?", help="Path to FIA database")
    parser.add_argument(
        "--agent",
        choices=["basic", "enhanced", "cognee"],
        default="basic",
        help="AI agent type to use",
    )
    parser.add_argument(
        "--api-key", help="OpenAI API key (or set OPENAI_API_KEY env var)"
    )

    args = parser.parse_args()

    # Set API key if provided
    if args.api_key:
        os.environ["OPENAI_API_KEY"] = args.api_key

    try:
        cli = FIAAICli(args.database, args.agent)
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
