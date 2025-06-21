"""
Modern CLI interface for the FIA AI Agent.

This module provides a beautiful command-line interface for interacting with
the FIA AI Agent using Rich for formatting and interactive features.
"""

import os
import sys
from pathlib import Path
from typing import Optional
import argparse

# Load environment variables
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    # python-dotenv not available, skip loading
    pass

from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt, Confirm
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.syntax import Syntax
from rich.markdown import Markdown
from rich.text import Text
from rich.layout import Layout
from rich.live import Live
import polars as pl

from .ai_agent import FIAAgent, FIAAgentConfig, create_fia_agent


class FIAAgentCLI:
    """
    Modern CLI interface for the FIA AI Agent.
    
    Provides an interactive command-line interface with:
    - Rich formatting and colors
    - Progress indicators
    - Syntax highlighting for SQL
    - Table formatting for results
    - Error handling and user feedback
    """
    
    def __init__(self, db_path: str, agent_config: Optional[FIAAgentConfig] = None):
        """
        Initialize the CLI interface.
        
        Args:
            db_path: Path to FIA DuckDB database
            agent_config: Optional agent configuration
        """
        self.console = Console()
        self.db_path = Path(db_path)
        
        if not self.db_path.exists():
            self.console.print(f"[red]Error: Database file not found: {db_path}[/red]")
            sys.exit(1)
        
        # Initialize agent with progress indicator
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=self.console
        ) as progress:
            task = progress.add_task("Initializing FIA AI Agent...", total=None)
            
            try:
                self.agent = FIAAgent(
                    db_path=db_path,
                    config=agent_config or FIAAgentConfig()
                )
                progress.update(task, description="✓ FIA AI Agent ready!")
            except Exception as e:
                self.console.print(f"[red]Failed to initialize agent: {str(e)}[/red]")
                sys.exit(1)
    
    def show_welcome(self):
        """Display welcome screen with database information."""
        # Create welcome panel
        welcome_text = """
🌲 **Forest Inventory Analysis AI Agent** 🌲

Ask natural language questions about forest inventory data!

**Examples:**
• "How many live trees are there by species?"
• "What's the average tree diameter in oak forests?"
• "Show me forest area by state"
• "Find plots with high biomass"

**Commands:**
• `help` - Show available commands
• `schema` - View database schema
• `evalids` - List available evaluations
• `examples` - Show query examples
• `quit` or `exit` - Exit the application
        """
        
        self.console.print(Panel(
            Markdown(welcome_text),
            title="🌲 FIA AI Agent",
            border_style="green",
            padding=(1, 2)
        ))
        
        # Show database info
        try:
            # Get basic database statistics
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=self.console
            ) as progress:
                task = progress.add_task("Loading database information...", total=None)
                
                # Get schema info
                schema_info = self.agent.query_interface.get_database_schema()
                evalids = self.agent.get_available_evaluations()
                
                progress.update(task, description="✓ Database information loaded!")
            
            # Create info table
            info_table = Table(title="Database Overview", show_header=True, header_style="bold magenta")
            info_table.add_column("Metric", style="cyan", no_wrap=True)
            info_table.add_column("Value", style="white")
            
            info_table.add_row("Database Path", str(self.db_path))
            info_table.add_row("Total Tables", str(len(schema_info)))
            info_table.add_row("Available Evaluations", str(len(evalids)))
            
            # Show top tables by row count
            table_sizes = [(name, info.row_count) for name, info in schema_info.items()]
            table_sizes.sort(key=lambda x: x[1], reverse=True)
            top_tables = ", ".join([f"{name} ({count:,})" for name, count in table_sizes[:3]])
            info_table.add_row("Largest Tables", top_tables)
            
            self.console.print(info_table)
            self.console.print()
            
        except Exception as e:
            self.console.print(f"[yellow]Warning: Could not load database info: {str(e)}[/yellow]")
    
    def show_help(self):
        """Display help information."""
        help_text = """
## Available Commands

**Query Commands:**
- Ask any natural language question about forest data
- Examples: "How many trees?", "Show species distribution", "Forest area by type"

**Information Commands:**
- `schema` - View database table structure
- `evalids [state_code]` - List available evaluation IDs
- `examples` - Show common query patterns
- `species <name>` - Find species codes by name

**Utility Commands:**
- `help` - Show this help message
- `clear` - Clear the screen
- `quit` or `exit` - Exit the application

**Tips:**
- Be specific about what forest metrics you want
- Mention time periods if relevant (e.g., "recent data")
- Include geographic scope if needed (e.g., "in California")
- Ask about statistical methods if you need population estimates
        """
        
        self.console.print(Panel(
            Markdown(help_text),
            title="Help - FIA AI Agent",
            border_style="blue"
        ))
    
    def show_schema(self):
        """Display database schema information."""
        try:
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=self.console
            ) as progress:
                task = progress.add_task("Loading database schema...", total=None)
                schema_info = self.agent.query_interface.get_database_schema()
                progress.update(task, description="✓ Schema loaded!")
            
            # Create schema table
            schema_table = Table(title="FIA Database Schema", show_header=True, header_style="bold magenta")
            schema_table.add_column("Table", style="cyan", no_wrap=True)
            schema_table.add_column("Rows", justify="right", style="green")
            schema_table.add_column("Columns", justify="right", style="blue")
            schema_table.add_column("Description", style="white")
            
            # Sort tables by row count
            sorted_tables = sorted(
                schema_info.items(),
                key=lambda x: x[1].row_count,
                reverse=True
            )
            
            for table_name, info in sorted_tables:
                schema_table.add_row(
                    table_name,
                    f"{info.row_count:,}",
                    str(len(info.columns)),
                    info.description or "No description"
                )
            
            self.console.print(schema_table)
            
        except Exception as e:
            self.console.print(f"[red]Error loading schema: {str(e)}[/red]")
    
    def show_evalids(self, state_code: Optional[int] = None):
        """Display available evaluation IDs."""
        try:
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=self.console
            ) as progress:
                task = progress.add_task("Loading evaluation information...", total=None)
                evalids = self.agent.get_available_evaluations(state_code)
                progress.update(task, description="✓ Evaluations loaded!")
            
            if len(evalids) == 0:
                self.console.print("[yellow]No evaluations found.[/yellow]")
                return
            
            # Create evalids table
            evalid_table = Table(
                title=f"Available Evaluations{f' (State {state_code})' if state_code else ''}",
                show_header=True,
                header_style="bold magenta"
            )
            evalid_table.add_column("EVALID", style="cyan", no_wrap=True)
            evalid_table.add_column("State", justify="center", style="green")
            evalid_table.add_column("End Year", justify="center", style="blue")
            evalid_table.add_column("Description", style="white")
            
            # Sort by end year (most recent first)
            sorted_evalids = evalids.sort('END_INVYR', descending=True).head(20)
            
            for row in sorted_evalids.iter_rows(named=True):
                evalid_table.add_row(
                    str(row['EVALID']),
                    str(row['STATECD']),
                    str(row['END_INVYR']),
                    row.get('EVAL_DESCR', 'No description')[:50]
                )
            
            self.console.print(evalid_table)
            
            if len(evalids) > 20:
                self.console.print(f"[dim]Showing 20 of {len(evalids)} evaluations. Use state filter to narrow results.[/dim]")
            
        except Exception as e:
            self.console.print(f"[red]Error loading evaluations: {str(e)}[/red]")
    
    def show_examples(self):
        """Display query examples."""
        examples_text = """
## Common FIA Query Examples

### Basic Tree Queries
- "How many live trees are in the database?"
- "What are the top 10 most common tree species?"
- "Show me trees with diameter greater than 20 inches"
- "Find all oak species in the database"

### Forest Area Analysis
- "What's the total forest area by state?"
- "Show forest area by ownership type"
- "How much area is in different forest types?"

### Volume and Biomass
- "What's the total volume by species?"
- "Show aboveground biomass for hardwood species"
- "Calculate volume per acre by forest type"

### Condition and Plot Analysis  
- "How many plots are there by state?"
- "Show average stand age by forest type"
- "Find plots measured in the last 5 years"

### Species-Specific Queries
- "Show diameter distribution for white oak"
- "Find plots with high pine density"
- "What's the average height of Douglas fir trees?"

### Statistical Queries (require EVALID)
- "Estimate trees per acre by species for California"
- "Calculate forest area for the most recent evaluation"
- "Show population estimates for oak volume"
        """
        
        self.console.print(Panel(
            Markdown(examples_text),
            title="Query Examples",
            border_style="cyan"
        ))
    
    def find_species(self, species_name: str):
        """Find species codes by name."""
        try:
            # Use the agent's species finding tool
            for tool in self.agent.tools:
                if tool.name == "find_species_codes":
                    result = tool.func(species_name)
                    self.console.print(Panel(
                        result,
                        title=f"Species Search: '{species_name}'",
                        border_style="green"
                    ))
                    break
        except Exception as e:
            self.console.print(f"[red]Error finding species: {str(e)}[/red]")
    
    def process_query(self, user_input: str):
        """Process a natural language query."""
        try:
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=self.console
            ) as progress:
                task = progress.add_task("Processing your query...", total=None)
                
                # Execute query through agent
                response = self.agent.query(user_input)
                
                progress.update(task, description="✓ Query completed!")
            
            # Display response with markdown formatting
            self.console.print(Panel(
                Markdown(response),
                title="Query Results",
                border_style="green"
            ))
            
        except Exception as e:
            self.console.print(f"[red]Error processing query: {str(e)}[/red]")
            self.console.print("[yellow]Try rephrasing your question or use 'help' for guidance.[/yellow]")
    
    def run_interactive(self):
        """Run the interactive CLI session."""
        self.show_welcome()
        
        while True:
            try:
                # Get user input with rich prompt
                user_input = Prompt.ask(
                    "\n[bold cyan]FIA AI[/bold cyan]",
                    default=""
                ).strip()
                
                if not user_input:
                    continue
                
                # Handle commands
                if user_input.lower() in ['quit', 'exit', 'q']:
                    self.console.print("[green]Thank you for using FIA AI Agent! 🌲[/green]")
                    break
                
                elif user_input.lower() == 'help':
                    self.show_help()
                
                elif user_input.lower() == 'schema':
                    self.show_schema()
                
                elif user_input.lower().startswith('evalids'):
                    parts = user_input.split()
                    state_code = None
                    if len(parts) > 1:
                        try:
                            state_code = int(parts[1])
                        except ValueError:
                            self.console.print("[red]Invalid state code. Use numeric state code.[/red]")
                            continue
                    self.show_evalids(state_code)
                
                elif user_input.lower() == 'examples':
                    self.show_examples()
                
                elif user_input.lower().startswith('species '):
                    species_name = user_input[8:].strip()
                    if species_name:
                        self.find_species(species_name)
                    else:
                        self.console.print("[red]Please provide a species name. Example: 'species oak'[/red]")
                
                elif user_input.lower() == 'clear':
                    os.system('clear' if os.name == 'posix' else 'cls')
                    self.show_welcome()
                
                else:
                    # Process as natural language query
                    self.process_query(user_input)
                
            except KeyboardInterrupt:
                self.console.print("\n[yellow]Use 'quit' to exit gracefully.[/yellow]")
            except EOFError:
                self.console.print("\n[green]Goodbye! 🌲[/green]")
                break


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="FIA AI Agent - Natural language interface for forest inventory data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  fia-ai /path/to/fia_database.duckdb
  fia-ai --model gpt-4o-mini /data/fia.duckdb
  fia-ai --help
        """
    )
    
    parser.add_argument(
        'database',
        help='Path to FIA DuckDB database file'
    )
    
    parser.add_argument(
        '--model',
        default='gpt-4o',
        help='OpenAI model to use (default: gpt-4o)'
    )
    
    parser.add_argument(
        '--temperature',
        type=float,
        default=0.1,
        help='Model temperature (default: 0.1)'
    )
    
    parser.add_argument(
        '--limit',
        type=int,
        default=100,
        help='Result limit for queries (default: 100)'
    )
    
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose output'
    )
    
    args = parser.parse_args()
    
    # Check for OpenAI API key
    if not os.environ.get('OPENAI_API_KEY'):
        console = Console()
        console.print("[red]Error: OPENAI_API_KEY environment variable not set.[/red]")
        console.print("[yellow]Please set your OpenAI API key:[/yellow]")
        console.print("export OPENAI_API_KEY='your-api-key-here'")
        sys.exit(1)
    
    # Create agent configuration
    config = FIAAgentConfig(
        model_name=args.model,
        temperature=args.temperature,
        result_limit=args.limit,
        verbose=args.verbose
    )
    
    try:
        # Initialize and run CLI
        cli = FIAAgentCLI(args.database, config)
        cli.run_interactive()
        
    except Exception as e:
        console = Console()
        console.print(f"[red]Failed to start FIA AI Agent: {str(e)}[/red]")
        if args.verbose:
            console.print_exception()
        sys.exit(1)


if __name__ == "__main__":
    main() 