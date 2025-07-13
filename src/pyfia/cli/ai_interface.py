#!/usr/bin/env python3
"""
AI-Enhanced CLI for pyFIA - Natural language queries and SQL generation.

This module provides an AI-powered command-line interface for:
- Natural language to SQL translation
- Direct SQL query execution
- Integration with AI agents (basic, enhanced, or Cognee)
- Rich formatting for results and AI responses
"""

import os
import sys
from pathlib import Path
from typing import Optional

# Load environment variables
try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

import polars as pl
from rich import box
from rich.markdown import Markdown
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.prompt import Confirm
from rich.syntax import Syntax
from rich.table import Table

from ..ai.domain_knowledge import fia_knowledge
from ..database.query_interface import DuckDBQueryInterface
from .base import BaseCLI


class FIAAICli(BaseCLI):
    """AI-enhanced CLI for natural language FIA queries."""

    intro = None
    prompt = "fia-ai> "

    def __init__(self, db_path: Optional[str] = None):
        """Initialize the AI CLI."""
        super().__init__(history_filename=".fia_ai_history")
        self.db_path = Path(db_path) if db_path else None
        self.query_interface: Optional[DuckDBQueryInterface] = None
        self.query_history = []
        self.agent = None
        self.api_key = os.environ.get("OPENAI_API_KEY")

        # Check API key
        if not self.api_key:
            self.console.print(
                "[yellow]Warning: OPENAI_API_KEY not set. Some features will be limited.[/yellow]"
            )

        # Show welcome and try to connect if path provided
        self._show_welcome()
        if self.db_path:
            self._connect_to_database(str(self.db_path))
        else:
            self._auto_connect_database()

    def _show_welcome(self):
        """Display welcome message."""
        welcome_text = """
# 🌲 pyFIA AI Assistant

**Natural language interface for Forest Inventory Analysis (FIA) data**

## 🚀 Quick Start

Just type your question naturally:
```
How many live oak trees are in North Carolina?
Calculate biomass by species
Show forest area trends over time
```

## 📊 Key Features

**Natural Language** → Ask questions in plain English  
**Smart SQL** → AI generates optimized FIA queries  
**Domain Knowledge** → Understands forestry terms (TPA, basal area, DBH)  
**Direct SQL** → Use `sql:` prefix for manual queries  
**Export Data** → Save results as CSV or Parquet  

## 💡 Example Queries

**Analysis:** "What's the mortality rate for pine species?"  
**Concepts:** "What is EVALID and why is it important?"  
**pyFIA:** "How do I calculate biomass using pyFIA?"  
**Direct:** `sql: SELECT * FROM TREE WHERE DIA > 20 LIMIT 10`

## 🎯 Pro Tips

• Be specific: "live trees" instead of just "trees"
• Use FIA terms: TPA, biomass, volume, mortality
• Filter by EVALID for valid statistical estimates
• Type `concepts` to explore FIA terminology
• Type `help` for all available commands
"""

        # Status
        status_items = []
        if self.api_key:
            status_items.append("AI: ✓ Ready")
        else:
            status_items.append("AI: ✗ No API key")

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

    def _connect_to_database(self, db_path_str: str) -> bool:
        """Connect to database with validation and progress display."""
        db_path = self._validate_database_path(db_path_str)
        if not db_path:
            return False

        try:
            with self._create_progress_bar("Connecting to database...") as progress:
                task = progress.add_task("Connecting...", total=None)

                # Initialize query interface
                self.query_interface = DuckDBQueryInterface(str(db_path))
                self.db_path = db_path

                # Initialize AI agent if API key available
                if self.api_key:
                    self._initialize_agent()

                progress.update(task, completed=True)

            self._show_connection_status(db_path, success=True)

            # Save to recent
            self.config.add_recent_database(str(db_path))

            # Show database info
            self._show_db_info()
            return True

        except Exception as e:
            self.console.print(f"[red]Error: {e}[/red]")
            self._show_connection_status(db_path, success=False)
            return False

    def _initialize_agent(self):
        """Initialize the appropriate AI agent."""
        try:
            from ..ai.agent import FIAAgent

            # Create checkpoint directory in user's home
            checkpoint_dir = Path.home() / ".pyfia" / "checkpoints"
            checkpoint_dir.mkdir(parents=True, exist_ok=True)

            self.agent = FIAAgent(
                db_path=self.db_path,
                api_key=self.api_key,
                verbose=True,
                checkpoint_dir=str(checkpoint_dir),
            )
            self.console.print("[green]✓ AI agent initialized[/green]")
            self.console.print("[dim]Using LangGraph with memory persistence[/dim]")

        except ImportError as e:
            self.console.print(f"[red]Error importing agent: {e}[/red]")
            self.console.print("[yellow]Install with: pip install pyfia[langchain][/yellow]")
        except Exception as e:
            self.console.print(f"[red]Error initializing agent: {e}[/red]")
            if "OpenAI API key" in str(e):
                self.console.print(
                    "[yellow]Set OPENAI_API_KEY environment variable or use --api-key[/yellow]"
                )

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

    # Use inherited do_last from BaseCLI instead of do_show

    def do_concepts(self, arg: str):
        """Show FIA concepts and terminology.
        Usage:
            concepts            - List all FIA concepts
            concepts biomass    - Explain a specific concept
            concepts search oak - Find concepts in a phrase
        """
        args = arg.strip().split(maxsplit=1)

        if not args:
            # List all concepts
            concepts_table = Table(title="FIA Domain Concepts", show_lines=True)
            concepts_table.add_column("Concept", style="cyan")
            concepts_table.add_column("Category", style="green")
            concepts_table.add_column("Description", style="white")

            for name, concept in list(fia_knowledge.concepts.items())[:20]:
                concepts_table.add_row(
                    name.replace('_', ' ').title(),
                    concept.category.replace('_', ' ').title(),
                    concept.description[:60] + "..." if len(concept.description) > 60 else concept.description
                )

            self.console.print(concepts_table)
            self.console.print(f"\n[dim]Showing 20 of {len(fia_knowledge.concepts)} concepts[/dim]")

        elif args[0] == "search" and len(args) > 1:
            # Search for concepts in a phrase
            phrase = args[1]
            concepts = fia_knowledge.extract_concepts(phrase)

            if concepts:
                self.console.print(f"\n[bold]Concepts found in '{phrase}':[/bold]")
                for concept in concepts:
                    self.console.print(f"\n[cyan]{concept.name.replace('_', ' ').title()}[/cyan]")
                    self.console.print(f"  {concept.description}")
                    self.console.print(f"  [dim]Synonyms: {', '.join(concept.synonyms[:3])}[/dim]")
            else:
                self.console.print(f"[yellow]No FIA concepts found in '{phrase}'[/yellow]")

        else:
            # Explain specific concept
            concept_name = args[0]
            concept = fia_knowledge.get_concept(concept_name)

            if not concept:
                # Try to find partial match
                concepts = fia_knowledge.extract_concepts(concept_name)
                if concepts:
                    concept = concepts[0]

            if concept:
                help_text = fia_knowledge.format_concept_help(concept.name)
                self.console.print(Panel(
                    Markdown(help_text),
                    title=f"FIA Concept: {concept.name.replace('_', ' ').title()}",
                    border_style="cyan"
                ))
            else:
                self.console.print(f"[yellow]Concept '{concept_name}' not found[/yellow]")
                self.console.print("Try: concepts (to list all)")

    def do_verbose(self, arg: str):
        """Toggle verbose mode for debugging.
        Usage: verbose [on|off]
        """
        if not arg:
            if hasattr(self, 'agent') and hasattr(self.agent, 'config'):
                current = self.agent.config.verbose
                self.console.print(f"[cyan]Verbose mode is: {'ON' if current else 'OFF'}[/cyan]")
            else:
                self.console.print("[yellow]No agent initialized[/yellow]")
            return

        if arg.lower() in ['on', 'true', '1']:
            if hasattr(self, 'agent') and hasattr(self.agent, 'config'):
                self.agent.config.verbose = True
                self.console.print("[green]Verbose mode enabled[/green]")
            else:
                self.console.print("[yellow]No agent to configure[/yellow]")
        elif arg.lower() in ['off', 'false', '0']:
            if hasattr(self, 'agent') and hasattr(self.agent, 'config'):
                self.agent.config.verbose = False
                self.console.print("[yellow]Verbose mode disabled[/yellow]")
            else:
                self.console.print("[yellow]No agent to configure[/yellow]")
        else:
            self.console.print("[red]Usage: verbose [on|off][/red]")

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
• **concepts** [term] - Explore FIA terminology
• **history** - View query history
• **export** <file> - Export results
• **last** [n] - Show last results
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

    def do_connect(self, arg: str):
        """Connect to FIA database.
        Usage: connect <database_path>
        """
        self._connect_to_database(arg.strip())


def main():
    """Main entry point for pyFIA AI CLI."""
    import argparse

    parser = argparse.ArgumentParser(
        description="pyFIA AI Assistant - Natural language interface for FIA data"
    )
    parser.add_argument("database", nargs="?", help="Path to FIA database")

    args = parser.parse_args()

    try:
        cli = FIAAICli(args.database)
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
