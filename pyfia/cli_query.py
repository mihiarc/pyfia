"""
Rich CLI interface for natural language FIA database queries using LangChain.

This module provides an interactive command-line interface for querying
FIA databases using natural language, powered by LangChain and DuckDB.
"""

import os
import sys
from pathlib import Path
from typing import Optional, Dict, Any
import click
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.prompt import Prompt, Confirm
from rich.syntax import Syntax
from rich.markdown import Markdown
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.live import Live
from rich.layout import Layout
from rich.text import Text
import polars as pl

from .duckdb_query_interface import DuckDBQueryInterface


console = Console()


class FIAQueryCLI:
    """Interactive CLI for FIA database queries."""
    
    def __init__(self, db_path: str):
        """Initialize the CLI with database connection."""
        self.db_path = db_path
        self.interface = DuckDBQueryInterface(db_path)
        self.history = []
        self.langchain_available = self._check_langchain()
        
        if self.langchain_available:
            self._setup_langchain()
    
    def _check_langchain(self) -> bool:
        """Check if LangChain is available."""
        try:
            import langchain
            from langchain_openai import ChatOpenAI
            return True
        except ImportError:
            return False
    
    def _setup_langchain(self):
        """Setup LangChain components if available."""
        try:
            from langchain_openai import ChatOpenAI
            from langchain.agents import create_sql_agent
            from langchain.agents.agent_toolkits import SQLDatabaseToolkit
            from langchain.sql_database import SQLDatabase
            from langchain.agents.agent_types import AgentType
            
            # Check for OpenAI API key
            if not os.environ.get("OPENAI_API_KEY"):
                console.print("[yellow]Warning: OPENAI_API_KEY not set. Natural language queries will be disabled.[/yellow]")
                self.langchain_available = False
                return
            
            # Create SQLDatabase object for LangChain
            self.sql_db = SQLDatabase.from_uri(f"duckdb:///{self.db_path}")
            
            # Create LLM
            self.llm = ChatOpenAI(temperature=0, model="gpt-4")
            
            # Create SQL agent
            toolkit = SQLDatabaseToolkit(db=self.sql_db, llm=self.llm)
            self.agent = create_sql_agent(
                llm=self.llm,
                toolkit=toolkit,
                verbose=False,
                agent_type=AgentType.ZERO_SHOT_REACT_DESCRIPTION,
                handle_parsing_errors=True,
                max_iterations=10,
                early_stopping_method="generate"
            )
            
            console.print("[green]✓ LangChain agent initialized successfully[/green]")
            
        except Exception as e:
            console.print(f"[red]Failed to setup LangChain: {e}[/red]")
            self.langchain_available = False
    
    def display_welcome(self):
        """Display welcome message."""
        welcome_text = """
# FIA Database Query Interface

Welcome to the Forest Inventory and Analysis (FIA) database query tool.
This interface allows you to query the FIA database using:

1. **Natural Language** (requires OpenAI API key)
2. **SQL Queries**
3. **Schema Exploration**

Type 'help' for available commands or 'exit' to quit.
        """
        
        console.print(Panel(Markdown(welcome_text), title="pyFIA Query CLI", border_style="green"))
        
        # Show database info
        with console.status("[bold green]Loading database information..."):
            evalid_info = self.interface.get_evalid_info()
            total_evals = len(evalid_info)
            
        info_table = Table(title="Database Overview", show_header=False)
        info_table.add_column("Property", style="cyan")
        info_table.add_column("Value", style="white")
        
        info_table.add_row("Database Path", str(self.db_path))
        info_table.add_row("Total Evaluations", str(total_evals))
        info_table.add_row("LangChain Status", 
                          "[green]Available[/green]" if self.langchain_available else "[yellow]Not Available[/yellow]")
        
        console.print(info_table)
        console.print()
    
    def display_schema(self, table_name: Optional[str] = None):
        """Display database schema information."""
        with console.status("[bold green]Loading schema information..."):
            if table_name:
                # Show specific table
                try:
                    info = self.interface._get_table_info(table_name.upper())
                    
                    # Table header
                    console.print(f"\n[bold cyan]Table: {info.name}[/bold cyan]")
                    if info.description:
                        console.print(f"[italic]{info.description}[/italic]")
                    console.print(f"Rows: {info.row_count:,}\n")
                    
                    # Columns table
                    col_table = Table(title="Columns", show_lines=True)
                    col_table.add_column("Name", style="cyan")
                    col_table.add_column("Type", style="green")
                    col_table.add_column("Nullable", style="yellow")
                    
                    for col in info.columns:
                        col_table.add_row(
                            col['name'],
                            col['type'],
                            "Yes" if col.get('nullable', True) else "No"
                        )
                    
                    console.print(col_table)
                    
                    # Sample data
                    if info.sample_data is not None and not info.sample_data.is_empty():
                        console.print("\n[bold]Sample Data:[/bold]")
                        self._display_dataframe(info.sample_data)
                    
                except Exception as e:
                    console.print(f"[red]Error: {e}[/red]")
            else:
                # Show all tables
                schema = self.interface.get_database_schema()
                
                table_list = Table(title="Database Tables", show_lines=True)
                table_list.add_column("Table Name", style="cyan")
                table_list.add_column("Description", style="white")
                table_list.add_column("Row Count", style="green", justify="right")
                
                for name, info in sorted(schema.items()):
                    table_list.add_row(
                        name,
                        info.description or "-",
                        f"{info.row_count:,}"
                    )
                
                console.print(table_list)
    
    def _display_dataframe(self, df: pl.DataFrame, max_rows: int = 10):
        """Display a Polars DataFrame as a rich table."""
        # Create table
        table = Table(show_lines=True)
        
        # Add columns
        for col in df.columns:
            table.add_column(col, style="cyan")
        
        # Add rows (limit to max_rows)
        for row in df.head(max_rows).iter_rows():
            table.add_row(*[str(val) if val is not None else "NULL" for val in row])
        
        console.print(table)
        
        if len(df) > max_rows:
            console.print(f"[dim]... showing {max_rows} of {len(df)} rows[/dim]")
    
    def execute_sql_query(self, query: str):
        """Execute a SQL query and display results."""
        try:
            with console.status("[bold green]Executing query..."):
                result = self.interface.execute_query(query)
            
            if result.is_empty():
                console.print("[yellow]No results found.[/yellow]")
            else:
                console.print(f"\n[green]Query returned {len(result)} rows[/green]")
                self._display_dataframe(result)
                
                # Add to history
                self.history.append({
                    "type": "sql",
                    "query": query,
                    "rows": len(result)
                })
        
        except Exception as e:
            console.print(f"[red]Query error: {e}[/red]")
    
    def execute_natural_language_query(self, question: str):
        """Execute a natural language query using LangChain."""
        if not self.langchain_available:
            console.print("[red]Natural language queries require LangChain and OpenAI API key.[/red]")
            console.print("Please install: pip install langchain langchain-openai")
            console.print("And set: export OPENAI_API_KEY='your-key'")
            return
        
        try:
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=console,
            ) as progress:
                task = progress.add_task("Thinking...", total=None)
                
                # Get agent response
                response = self.agent.run(question)
                progress.update(task, completed=True)
            
            # Display response
            console.print("\n[bold green]Answer:[/bold green]")
            console.print(Panel(str(response), border_style="green"))
            
            # Add to history
            self.history.append({
                "type": "natural",
                "question": question,
                "response": response
            })
            
        except Exception as e:
            console.print(f"[red]Error processing query: {e}[/red]")
    
    def show_help(self):
        """Display help information."""
        help_text = """
## Available Commands:

### Queries:
- **Natural language query**: Just type your question (e.g., "How many plots are in North Carolina?")
- **SQL query**: Start with 'sql:' (e.g., "sql: SELECT COUNT(*) FROM PLOT WHERE STATECD = 37")

### Commands:
- **schema**: Show all tables in the database
- **schema [table]**: Show details for a specific table
- **history**: Show query history
- **clear**: Clear the screen
- **help**: Show this help message
- **exit/quit**: Exit the application

### Examples:
- "What is the total forest area in evaluation 372301?"
- "Show me the species distribution in North Carolina"
- "sql: SELECT SPCD, COUNT(*) as count FROM TREE GROUP BY SPCD ORDER BY count DESC LIMIT 10"
- "schema TREE"
        """
        
        console.print(Panel(Markdown(help_text), title="Help", border_style="blue"))
    
    def show_history(self):
        """Display query history."""
        if not self.history:
            console.print("[yellow]No queries in history.[/yellow]")
            return
        
        history_table = Table(title="Query History", show_lines=True)
        history_table.add_column("#", style="cyan", width=4)
        history_table.add_column("Type", style="green")
        history_table.add_column("Query/Question", style="white")
        history_table.add_column("Result", style="yellow")
        
        for i, item in enumerate(self.history[-10:], 1):  # Show last 10
            if item["type"] == "sql":
                history_table.add_row(
                    str(i),
                    "SQL",
                    item["query"][:50] + "..." if len(item["query"]) > 50 else item["query"],
                    f"{item['rows']} rows"
                )
            else:
                history_table.add_row(
                    str(i),
                    "Natural",
                    item["question"][:50] + "..." if len(item["question"]) > 50 else item["question"],
                    "Answered"
                )
        
        console.print(history_table)
    
    def run(self):
        """Run the interactive CLI."""
        self.display_welcome()
        
        while True:
            try:
                # Get user input
                user_input = Prompt.ask("\n[bold cyan]FIA Query[/bold cyan]").strip()
                
                if not user_input:
                    continue
                
                # Handle commands
                if user_input.lower() in ['exit', 'quit']:
                    if Confirm.ask("Are you sure you want to exit?"):
                        console.print("[green]Goodbye![/green]")
                        break
                
                elif user_input.lower() == 'help':
                    self.show_help()
                
                elif user_input.lower() == 'clear':
                    console.clear()
                    self.display_welcome()
                
                elif user_input.lower() == 'history':
                    self.show_history()
                
                elif user_input.lower() == 'schema':
                    self.display_schema()
                
                elif user_input.lower().startswith('schema '):
                    table_name = user_input[7:].strip()
                    self.display_schema(table_name)
                
                elif user_input.lower().startswith('sql:'):
                    # SQL query
                    query = user_input[4:].strip()
                    self.execute_sql_query(query)
                
                else:
                    # Natural language query
                    self.execute_natural_language_query(user_input)
                
            except KeyboardInterrupt:
                console.print("\n[yellow]Use 'exit' to quit.[/yellow]")
            except Exception as e:
                console.print(f"[red]Unexpected error: {e}[/red]")


@click.command()
@click.argument('database', type=click.Path(exists=True))
@click.option('--api-key', envvar='OPENAI_API_KEY', help='OpenAI API key for natural language queries')
def main(database: str, api_key: Optional[str]):
    """
    Interactive FIA database query interface.
    
    DATABASE: Path to the FIA DuckDB database file
    """
    if api_key:
        os.environ['OPENAI_API_KEY'] = api_key
    
    try:
        cli = FIAQueryCLI(database)
        cli.run()
    except Exception as e:
        console.print(f"[red]Failed to start CLI: {e}[/red]")
        sys.exit(1)


if __name__ == "__main__":
    main()