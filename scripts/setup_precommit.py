#!/usr/bin/env python3
"""
Set up pre-commit hooks for pyFIA development.

This script installs and configures pre-commit hooks for code quality.
"""

import subprocess
import sys
from pathlib import Path

from rich.console import Console
from rich.panel import Panel

console = Console()


def run_command(cmd: list[str], description: str) -> bool:
    """Run a command and return success status."""
    console.print(f"[blue]{description}...[/blue]")
    try:
        subprocess.run(cmd, check=True, capture_output=True, text=True)
        console.print(f"[green]✓[/green] {description}")
        return True
    except subprocess.CalledProcessError as e:
        console.print(f"[red]✗[/red] {description} failed")
        if e.stderr:
            console.print(f"[red]Error:[/red] {e.stderr}")
        return False


def main():
    """Set up pre-commit hooks."""
    console.print(Panel.fit(
        "[bold]PyFIA Pre-commit Setup[/bold]\n"
        "Installing and configuring code quality hooks",
        border_style="green"
    ))
    
    # Change to project root
    project_root = Path(__file__).parent.parent
    
    # Install pre-commit if needed
    if not run_command(
        ["uv", "pip", "install", "pre-commit"],
        "Installing pre-commit"
    ):
        sys.exit(1)
    
    # Install the git hooks
    if not run_command(
        ["uv", "run", "pre-commit", "install"],
        "Installing git hooks"
    ):
        sys.exit(1)
    
    # Install commit-msg hook for conventional commits
    if not run_command(
        ["uv", "run", "pre-commit", "install", "--hook-type", "commit-msg"],
        "Installing commit-msg hooks"
    ):
        console.print("[yellow]Warning:[/yellow] commit-msg hook installation failed")
    
    # Run all hooks on all files to check current status
    console.print("\n[blue]Running all hooks on existing files...[/blue]")
    result = subprocess.run(
        ["uv", "run", "pre-commit", "run", "--all-files"],
        capture_output=True,
        text=True
    )
    
    if result.returncode == 0:
        console.print("[green]✓[/green] All pre-commit hooks passed!")
    else:
        console.print("[yellow]⚠[/yellow] Some hooks failed (this is normal for initial setup)")
        console.print("Run [cyan]uv run pre-commit run --all-files[/cyan] to see details")
    
    # Create secrets baseline if it doesn't exist
    secrets_baseline = project_root / ".secrets.baseline"
    if not secrets_baseline.exists():
        console.print("\n[blue]Creating secrets baseline...[/blue]")
        subprocess.run(
            ["uv", "run", "detect-secrets", "scan", "--baseline", ".secrets.baseline"],
            cwd=project_root,
            capture_output=True
        )
        console.print("[green]✓[/green] Secrets baseline created")
    
    console.print(Panel.fit(
        "[bold green]Setup complete![/bold green]\n\n"
        "Pre-commit will now run automatically on git commit.\n"
        "To run manually: [cyan]uv run pre-commit run --all-files[/cyan]\n"
        "To update hooks: [cyan]uv run pre-commit autoupdate[/cyan]",
        border_style="green"
    ))


if __name__ == "__main__":
    main()