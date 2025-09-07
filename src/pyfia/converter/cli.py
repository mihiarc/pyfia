"""
Command-line interface for FIA SQLite to DuckDB converter.

This module provides a rich CLI for converting FIA databases with progress
tracking, validation, and comprehensive reporting.
"""

import sys
from pathlib import Path
from typing import Optional

import click
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from .models import CompressionLevel, ConverterConfig, ValidationLevel
from .sqlite_to_duckdb import FIAConverter

console = Console()


@click.group()
@click.version_option()
def cli():
    """
    FIA SQLite to DuckDB Converter

    Convert FIA DataMart SQLite databases to optimized DuckDB format
    for improved analytical performance.
    """
    pass


@cli.command()
@click.option(
    '--source', '-s',
    type=click.Path(exists=True, path_type=Path),
    required=True,
    help='Path to source SQLite database'
)
@click.option(
    '--target', '-t',
    type=click.Path(path_type=Path),
    default='fia.duckdb',
    help='Target DuckDB database path'
)
@click.option(
    '--state-code', '-c',
    type=int,
    help='FIPS state code (auto-detected if not provided)'
)
@click.option(
    '--batch-size', '-b',
    type=int,
    default=100_000,
    help='Records per batch for processing'
)
@click.option(
    '--parallel-workers', '-p',
    type=int,
    default=4,
    help='Number of parallel worker threads'
)
@click.option(
    '--memory-limit', '-m',
    type=str,
    default='4GB',
    help='DuckDB memory limit'
)
@click.option(
    '--compression',
    type=click.Choice(['none', 'low', 'medium', 'high', 'adaptive']),
    default='medium',
    help='Compression level for target database'
)
@click.option(
    '--validation',
    type=click.Choice(['none', 'basic', 'standard', 'comprehensive']),
    default='standard',
    help='Level of data validation to perform'
)
@click.option(
    '--no-indexes',
    is_flag=True,
    help='Skip index creation for faster conversion'
)
@click.option(
    '--no-progress',
    is_flag=True,
    help='Disable progress bars'
)
@click.option(
    '--temp-dir',
    type=click.Path(path_type=Path),
    help='Temporary directory for conversion operations'
)
@click.option(
    '--append',
    is_flag=True,
    help='Append data to existing tables without removing existing data'
)
@click.option(
    '--dedupe',
    is_flag=True,
    help='Remove duplicate records when appending (requires --append)'
)
@click.option(
    '--dedupe-keys',
    type=str,
    help='Comma-separated column names for deduplication (e.g., "CN,PLT_CN")'
)
@click.option(
    '--log-level',
    type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']),
    default='CRITICAL',
    help='Logging level'
)
def convert(
    source: Path,
    target: Path,
    state_code: Optional[int],
    batch_size: int,
    parallel_workers: int,
    memory_limit: str,
    compression: str,
    validation: str,
    no_indexes: bool,
    no_progress: bool,
    temp_dir: Optional[Path],
    append: bool,
    dedupe: bool,
    dedupe_keys: Optional[str],
    log_level: str
):
    """Convert a single state SQLite database to DuckDB format."""

    console.print(Panel.fit(
        f"[bold blue]FIA SQLite to DuckDB Converter[/bold blue]\n"
        f"Converting: {source}\n"
        f"Target: {target}",
        title="Conversion Started"
    ))

    # Validate dedupe options
    if dedupe and not append:
        console.print("[red]Error: --dedupe requires --append flag[/red]")
        sys.exit(1)
    
    # Parse dedupe keys if provided
    dedupe_keys_list = None
    if dedupe_keys:
        dedupe_keys_list = [k.strip() for k in dedupe_keys.split(",")]
        if dedupe and not dedupe_keys_list:
            console.print("[yellow]Warning: --dedupe flag set but no dedupe keys provided[/yellow]")
    
    try:
        # Create converter configuration
        config = ConverterConfig(
            source_dir=source.parent,
            target_path=target,
            temp_dir=temp_dir,
            batch_size=batch_size,
            parallel_workers=parallel_workers,
            memory_limit=memory_limit,
            compression_level=CompressionLevel(compression),
            validation_level=ValidationLevel(validation),
            create_indexes=not no_indexes,
            show_progress=not no_progress,
            append_mode=append,
            dedupe_on_append=dedupe,
            dedupe_keys=dedupe_keys_list,
            log_level=log_level
        )

        # Initialize converter
        converter = FIAConverter(config)

        # Auto-detect state code if not provided
        if state_code is None:
            state_code = converter._extract_state_code_from_path(source)
            if state_code is None:
                state_code = converter._get_state_code_from_db(source)

            if state_code:
                console.print(f"[green]Auto-detected state code: {state_code}[/green]")
            else:
                console.print("[yellow]Warning: Could not auto-detect state code[/yellow]")
                state_code = 0  # Use 0 as fallback

        # Perform conversion
        result = converter.convert_state(source, state_code, target)

        # Display results
        _display_conversion_result(result)

        # Exit with appropriate code
        sys.exit(0 if result.is_successful() else 1)

    except Exception as e:
        console.print(f"[red]Conversion failed: {e}[/red]")
        sys.exit(1)


@cli.command()
@click.option(
    '--source-dir', '-s',
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    required=True,
    help='Directory containing SQLite databases'
)
@click.option(
    '--target', '-t',
    type=click.Path(path_type=Path),
    default='merged_fia.duckdb',
    help='Target DuckDB database path'
)
@click.option(
    '--states',
    type=str,
    help='Comma-separated list of state codes to include'
)
@click.option(
    '--pattern',
    type=str,
    default='*.db',
    help='File pattern to match SQLite databases'
)
@click.option(
    '--batch-size', '-b',
    type=int,
    default=100_000,
    help='Records per batch for processing'
)
@click.option(
    '--parallel-workers', '-p',
    type=int,
    default=4,
    help='Number of parallel worker threads'
)
@click.option(
    '--memory-limit', '-m',
    type=str,
    default='8GB',
    help='DuckDB memory limit for multi-state merge'
)
@click.option(
    '--compression',
    type=click.Choice(['none', 'low', 'medium', 'high', 'adaptive']),
    default='medium',
    help='Compression level for target database'
)
@click.option(
    '--validation',
    type=click.Choice(['none', 'basic', 'standard', 'comprehensive']),
    default='standard',
    help='Level of data validation to perform'
)
@click.option(
    '--no-progress',
    is_flag=True,
    help='Disable progress bars'
)
def merge(
    source_dir: Path,
    target: Path,
    states: Optional[str],
    pattern: str,
    batch_size: int,
    parallel_workers: int,
    memory_limit: str,
    compression: str,
    validation: str,
    no_progress: bool
):
    """Merge multiple state SQLite databases into a single DuckDB."""

    # Find source files
    source_files = list(source_dir.glob(pattern))

    if not source_files:
        console.print(f"[red]No files found matching pattern '{pattern}' in {source_dir}[/red]")
        sys.exit(1)

    # Filter by states if specified
    if states:
        state_codes = [int(s.strip()) for s in states.split(',')]
        console.print(f"[blue]Filtering to states: {state_codes}[/blue]")
    else:
        state_codes = []

    console.print(Panel.fit(
        f"[bold blue]FIA Multi-State Merger[/bold blue]\n"
        f"Sources: {len(source_files)} files in {source_dir}\n"
        f"Target: {target}",
        title="Merge Started"
    ))

    try:
        # Create converter configuration
        config = ConverterConfig(
            source_dir=source_dir,
            target_path=target,
            batch_size=batch_size,
            parallel_workers=parallel_workers,
            memory_limit=memory_limit,
            compression_level=CompressionLevel(compression),
            validation_level=ValidationLevel(validation),
            include_states=state_codes if state_codes else None,
            show_progress=not no_progress
        )

        # Initialize converter
        converter = FIAConverter(config)

        # Perform merge
        result = converter.merge_states(source_files, target)

        # Display results
        _display_conversion_result(result)

        # Exit with appropriate code
        sys.exit(0 if result.is_successful() else 1)

    except Exception as e:
        console.print(f"[red]Merge failed: {e}[/red]")
        sys.exit(1)


@cli.command()
@click.option(
    '--database', '-d',
    type=click.Path(exists=True, path_type=Path),
    required=True,
    help='DuckDB database to validate'
)
@click.option(
    '--validation',
    type=click.Choice(['basic', 'standard', 'comprehensive']),
    default='standard',
    help='Level of validation to perform'
)
@click.option(
    '--source',
    type=click.Path(exists=True, path_type=Path),
    help='Source SQLite database for comparison'
)
@click.option(
    '--output',
    type=click.Path(path_type=Path),
    help='Save validation report to file'
)
def validate(
    database: Path,
    validation: str,
    source: Optional[Path],
    output: Optional[Path]
):
    """Validate a converted FIA database."""

    console.print(Panel.fit(
        f"[bold blue]FIA Database Validator[/bold blue]\n"
        f"Database: {database}\n"
        f"Level: {validation}",
        title="Validation Started"
    ))

    try:
        from .validation import DataValidator

        validator = DataValidator()
        result = validator.validate_database(
            database,
            ValidationLevel(validation),
            source
        )

        # Display validation results
        _display_validation_result(result)

        # Save report if requested
        if output:
            _save_validation_report(result, output)
            console.print(f"[green]Validation report saved to {output}[/green]")

        # Exit with appropriate code
        sys.exit(0 if result.is_valid else 1)

    except Exception as e:
        console.print(f"[red]Validation failed: {e}[/red]")
        sys.exit(1)


@cli.command()
@click.option(
    '--database', '-d',
    type=click.Path(exists=True, path_type=Path),
    required=True,
    help='DuckDB database to analyze'
)
@click.option(
    '--table',
    type=str,
    help='Specific table to analyze (analyze all if not specified)'
)
def info(database: Path, table: Optional[str]):
    """Display information about a converted FIA database."""

    try:
        import duckdb

        console.print(Panel.fit(
            f"[bold blue]FIA Database Information[/bold blue]\n"
            f"Database: {database}",
            title="Database Analysis"
        ))

        with duckdb.connect(str(database), read_only=True) as conn:
            # Get basic database info
            db_size = database.stat().st_size
            console.print(f"\n[bold]Database Size:[/bold] {_format_bytes(db_size)}")

            # Get table information
            if table:
                _display_table_info(conn, table)
            else:
                _display_database_overview(conn)

    except Exception as e:
        console.print(f"[red]Failed to analyze database: {e}[/red]")
        sys.exit(1)


def _display_conversion_result(result) -> None:
    """Display conversion results in a formatted table."""

    # Status panel
    status_color = "green" if result.is_successful() else "red"
    status_text = "✅ SUCCESS" if result.is_successful() else "❌ FAILED"

    console.print(f"\n[{status_color}]{status_text}[/{status_color}]")

    # Summary table
    table = Table(title="Conversion Summary")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="white")

    table.add_row("Status", result.status.value.title())
    table.add_row("Source Files", str(len(result.source_paths)))
    table.add_row("Target Path", str(result.target_path))

    if result.stats.duration_seconds:
        table.add_row("Duration", f"{result.stats.duration_seconds:.1f} seconds")

    table.add_row("Records Processed", f"{result.stats.source_records_processed:,}")
    table.add_row("Records Written", f"{result.stats.target_records_written:,}")
    table.add_row("Tables Created", str(result.stats.target_tables_created))
    table.add_row("Indexes Created", str(result.stats.target_indexes_created))

    if result.stats.compression_ratio:
        table.add_row("Compression Ratio", f"{result.stats.compression_ratio:.2f}x")

    if result.stats.throughput_records_per_second:
        table.add_row("Throughput", f"{result.stats.throughput_records_per_second:,.0f} records/sec")

    console.print(table)

    # Validation results
    if result.validation:
        console.print("\n[bold]Validation Results:[/bold]")
        if result.validation.is_valid:
            console.print("[green]✅ All validations passed[/green]")
        else:
            console.print(f"[red]❌ {len(result.validation.errors)} validation errors[/red]")
            for error in result.validation.errors[:5]:  # Show first 5
                console.print(f"  • {error.message}")

    # Errors
    if result.error_message:
        console.print(f"\n[red]Error: {result.error_message}[/red]")


def _display_validation_result(result) -> None:
    """Display validation results."""

    # Status
    if result.is_valid:
        console.print("\n[green]✅ VALIDATION PASSED[/green]")
    else:
        console.print("\n[red]❌ VALIDATION FAILED[/red]")

    # Summary
    table = Table(title="Validation Summary")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="white")

    table.add_row("Duration", f"{result.validation_duration_seconds:.2f} seconds")
    table.add_row("Tables Validated", str(result.tables_validated))
    table.add_row("Records Validated", f"{result.records_validated:,}")
    table.add_row("Errors", str(len(result.errors)))
    table.add_row("Warnings", str(len(result.warnings)))

    console.print(table)

    # Errors and warnings
    if result.errors:
        console.print(f"\n[red]Errors ({len(result.errors)}):[/red]")
        for error in result.errors[:10]:  # Show first 10
            console.print(f"  • [{error.table_name or 'Unknown'}] {error.message}")

    if result.warnings:
        console.print(f"\n[yellow]Warnings ({len(result.warnings)}):[/yellow]")
        for warning in result.warnings[:5]:  # Show first 5
            console.print(f"  • [{warning.table_name or 'Unknown'}] {warning.message}")


def _display_database_overview(conn) -> None:
    """Display overview of database tables."""

    # Get table list with row counts
    tables_query = """
    SELECT
        table_name,
        estimated_size as row_count
    FROM duckdb_tables()
    WHERE schema_name = 'main'
    AND table_name NOT LIKE '__pyfia_%'
    ORDER BY estimated_size DESC
    """

    try:
        tables_result = conn.execute(tables_query).fetchall()

        table = Table(title="Database Tables")
        table.add_column("Table", style="cyan")
        table.add_column("Rows", style="white", justify="right")

        total_rows = 0
        for table_name, row_count in tables_result:
            table.add_row(table_name, f"{row_count:,}")
            total_rows += row_count

        table.add_row("[bold]TOTAL[/bold]", f"[bold]{total_rows:,}[/bold]")
        console.print(table)

    except Exception as e:
        console.print(f"[yellow]Could not get table overview: {e}[/yellow]")


def _display_table_info(conn, table_name: str) -> None:
    """Display detailed information about a specific table."""

    try:
        # Get table schema
        schema_result = conn.execute(f"DESCRIBE {table_name}").fetchall()

        # Get row count
        count_result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        row_count = count_result[0] if count_result else 0

        console.print(f"\n[bold]Table: {table_name}[/bold]")
        console.print(f"Rows: {row_count:,}")

        # Schema table
        schema_table = Table(title="Schema")
        schema_table.add_column("Column", style="cyan")
        schema_table.add_column("Type", style="white")
        schema_table.add_column("Nullable", style="white")

        for col_name, col_type, nullable, *_ in schema_result:
            schema_table.add_row(
                col_name,
                col_type,
                "Yes" if nullable == "YES" else "No"
            )

        console.print(schema_table)

    except Exception as e:
        console.print(f"[red]Failed to get table info: {e}[/red]")


def _save_validation_report(result, output_path: Path) -> None:
    """Save validation report to file."""

    report_lines = [
        "FIA Database Validation Report",
        "=" * 40,
        f"Validation Time: {result.validation_time}",
        f"Duration: {result.validation_duration_seconds:.2f} seconds",
        f"Status: {'PASSED' if result.is_valid else 'FAILED'}",
        f"Tables Validated: {result.tables_validated}",
        f"Records Validated: {result.records_validated:,}",
        f"Errors: {len(result.errors)}",
        f"Warnings: {len(result.warnings)}",
        ""
    ]

    if result.errors:
        report_lines.append("ERRORS:")
        report_lines.append("-" * 20)
        for error in result.errors:
            report_lines.append(f"[{error.table_name or 'Unknown'}] {error.message}")
        report_lines.append("")

    if result.warnings:
        report_lines.append("WARNINGS:")
        report_lines.append("-" * 20)
        for warning in result.warnings:
            report_lines.append(f"[{warning.table_name or 'Unknown'}] {warning.message}")

    output_path.write_text("\n".join(report_lines))


def _format_bytes(bytes_value: int) -> str:
    """Format bytes as human-readable string."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_value < 1024.0:
            return f"{bytes_value:.1f} {unit}"
        bytes_value /= 1024.0
    return f"{bytes_value:.1f} PB"


if __name__ == '__main__':
    cli()
