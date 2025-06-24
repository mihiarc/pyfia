#!/usr/bin/env python3
"""
Run both mypy and ty type checkers for comprehensive type checking.

This script runs both type checkers and provides a combined report.
Once ty reaches stable release, we can migrate fully.
"""

import subprocess
import sys
from pathlib import Path
from typing import List, Tuple

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

console = Console()


def run_type_checker(command: List[str], name: str) -> Tuple[int, str]:
    """Run a type checker and return exit code and output."""
    console.print(f"\n[bold blue]Running {name}...[/bold blue]")
    
    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            cwd=Path(__file__).parent.parent
        )
        return result.returncode, result.stdout + result.stderr
    except FileNotFoundError:
        return 1, f"{name} not found. Please install with: uv pip install -e .[dev]"


def parse_mypy_output(output: str) -> Tuple[int, int, int]:
    """Parse mypy output for error, warning, and note counts."""
    errors = output.count("error:")
    warnings = output.count("warning:")
    notes = output.count("note:")
    return errors, warnings, notes


def parse_ty_output(output: str) -> Tuple[int, int, int]:
    """Parse ty output for error, warning, and note counts."""
    # Ty output format may differ, adjust as needed
    errors = output.count("error:")
    warnings = output.count("warning:") 
    notes = output.count("note:")
    return errors, warnings, notes


def main():
    """Run both type checkers and display results."""
    console.print(Panel.fit(
        "[bold]PyFIA Type Checking Suite[/bold]\n"
        "Running both mypy and ty for comprehensive type analysis",
        border_style="green"
    ))
    
    # Run mypy
    mypy_code, mypy_output = run_type_checker(
        ["uv", "run", "mypy", "pyfia/", "--show-error-codes"],
        "mypy"
    )
    
    # Run ty
    ty_code, ty_output = run_type_checker(
        ["uv", "run", "ty", "check", "pyfia/"],
        "ty (alpha)"
    )
    
    # Parse results
    mypy_errors, mypy_warnings, mypy_notes = parse_mypy_output(mypy_output)
    ty_errors, ty_warnings, ty_notes = parse_ty_output(ty_output)
    
    # Create summary table
    table = Table(title="Type Checking Summary")
    table.add_column("Checker", style="cyan")
    table.add_column("Status", style="bold")
    table.add_column("Errors", style="red")
    table.add_column("Warnings", style="yellow")
    table.add_column("Notes", style="blue")
    
    mypy_status = "[green]PASSED[/green]" if mypy_code == 0 else "[red]FAILED[/red]"
    ty_status = "[green]PASSED[/green]" if ty_code == 0 else "[red]FAILED[/red]"
    
    table.add_row("mypy", mypy_status, str(mypy_errors), str(mypy_warnings), str(mypy_notes))
    table.add_row("ty", ty_status, str(ty_errors), str(ty_warnings), str(ty_notes))
    
    console.print("\n")
    console.print(table)
    
    # Show detailed output if there are errors
    if mypy_code != 0:
        console.print("\n[bold red]mypy output:[/bold red]")
        console.print(mypy_output)
    
    if ty_code != 0:
        console.print("\n[bold red]ty output:[/bold red]")
        console.print(ty_output)
    
    # Exit with failure if either checker failed
    if mypy_code != 0 or ty_code != 0:
        console.print("\n[bold red]Type checking failed![/bold red]")
        sys.exit(1)
    else:
        console.print("\n[bold green]All type checks passed![/bold green]")
        sys.exit(0)


if __name__ == "__main__":
    main()