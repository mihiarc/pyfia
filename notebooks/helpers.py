"""
Shared utilities for pyFIA tutorial notebooks.

Provides helper functions for data management, display formatting,
and visualization across all tutorial notebooks.
"""

from pathlib import Path
from typing import Optional, Union

import matplotlib.pyplot as plt
import polars as pl
from rich.console import Console
from rich.table import Table


def ensure_ri_data(data_dir: Optional[Path] = None) -> Path:
    """
    Ensure Rhode Island FIA data is downloaded and return the database path.

    Rhode Island is used as the primary tutorial dataset because it's the
    smallest state, making downloads fast (~1-2 minutes) while still
    providing representative FIA data.

    Parameters
    ----------
    data_dir : Path, optional
        Directory to store data. Defaults to notebooks/data/

    Returns
    -------
    Path
        Path to the Rhode Island DuckDB database file.

    Example
    -------
    >>> db_path = ensure_ri_data()
    >>> print(db_path)
    PosixPath('data/ri/ri.duckdb')
    """
    from pyfia import download

    if data_dir is None:
        data_dir = Path(__file__).parent / "data"

    data_dir = Path(data_dir)
    db_path = data_dir / "ri" / "ri.duckdb"

    if db_path.exists():
        print(f"Rhode Island data already available at: {db_path}")
        return db_path

    print("Downloading Rhode Island FIA data (this may take 1-2 minutes)...")
    db_path = download("RI", dir=data_dir)
    print(f"Download complete! Database saved to: {db_path}")
    return db_path


def display_estimate(
    df: pl.DataFrame,
    title: str = "",
    max_rows: int = 20,
    precision: int = 2,
) -> None:
    """
    Format and display an estimation result using Rich tables.

    Provides nicely formatted output for pyFIA estimation results,
    with appropriate number formatting and column alignment.

    Parameters
    ----------
    df : pl.DataFrame
        DataFrame containing estimation results.
    title : str, optional
        Title to display above the table.
    max_rows : int, optional
        Maximum rows to display. Defaults to 20.
    precision : int, optional
        Decimal places for floating point numbers. Defaults to 2.

    Example
    -------
    >>> result = area(db)
    >>> display_estimate(result, title="Forest Area by Type")
    """
    console = Console()

    if title:
        console.print(f"\n[bold blue]{title}[/bold blue]")

    # Create Rich table
    table = Table(show_header=True, header_style="bold cyan")

    # Add columns
    for col in df.columns:
        table.add_column(col, justify="right" if df[col].dtype in [pl.Float64, pl.Int64, pl.Int32] else "left")

    # Add rows (limited)
    display_df = df.head(max_rows)
    for row in display_df.iter_rows():
        formatted_row = []
        for val, col in zip(row, df.columns):
            if val is None:
                formatted_row.append("-")
            elif isinstance(val, float):
                formatted_row.append(f"{val:,.{precision}f}")
            elif isinstance(val, int):
                formatted_row.append(f"{val:,}")
            else:
                formatted_row.append(str(val))
        table.add_row(*formatted_row)

    console.print(table)

    if len(df) > max_rows:
        console.print(f"[dim]... showing {max_rows} of {len(df)} rows[/dim]")


def plot_by_category(
    df: pl.DataFrame,
    category_col: str,
    value_col: str,
    error_col: Optional[str] = None,
    top_n: int = 10,
    title: str = "",
    xlabel: str = "",
    ylabel: str = "",
    figsize: tuple = (10, 6),
    color: str = "#2E7D32",
    horizontal: bool = True,
) -> plt.Figure:
    """
    Create a bar chart for categorical estimation results.

    Produces publication-quality bar charts with optional error bars,
    suitable for visualizing pyFIA estimation results.

    Parameters
    ----------
    df : pl.DataFrame
        DataFrame containing the data.
    category_col : str
        Column name for categories (x-axis or y-axis).
    value_col : str
        Column name for values (bar heights).
    error_col : str, optional
        Column name for error values (for error bars).
    top_n : int, optional
        Number of top categories to display. Defaults to 10.
    title : str, optional
        Chart title.
    xlabel : str, optional
        X-axis label.
    ylabel : str, optional
        Y-axis label.
    figsize : tuple, optional
        Figure size in inches. Defaults to (10, 6).
    color : str, optional
        Bar color. Defaults to forest green.
    horizontal : bool, optional
        If True, create horizontal bar chart. Defaults to True.

    Returns
    -------
    matplotlib.figure.Figure
        The created figure object.

    Example
    -------
    >>> result = volume(db, by_species=True)
    >>> result = join_species_names(result, "SPCD")
    >>> fig = plot_by_category(
    ...     result,
    ...     category_col="SPCD_NAME",
    ...     value_col="VOLCFNET_ACRE",
    ...     error_col="VOLCFNET_ACRE_SE",
    ...     title="Volume by Species"
    ... )
    >>> plt.show()
    """
    # Sort and limit to top_n
    sorted_df = df.sort(value_col, descending=True).head(top_n)

    # Convert to lists for matplotlib
    categories = sorted_df[category_col].to_list()
    values = sorted_df[value_col].to_list()
    errors = sorted_df[error_col].to_list() if error_col and error_col in df.columns else None

    # Create figure
    fig, ax = plt.subplots(figsize=figsize)

    if horizontal:
        # Reverse for top-to-bottom display
        categories = categories[::-1]
        values = values[::-1]
        if errors:
            errors = errors[::-1]

        bars = ax.barh(categories, values, color=color, edgecolor='white', linewidth=0.5)
        if errors:
            ax.errorbar(values, categories, xerr=errors, fmt='none', color='black', capsize=3)
        ax.set_xlabel(xlabel or value_col)
        ax.set_ylabel(ylabel or category_col)
    else:
        bars = ax.bar(categories, values, color=color, edgecolor='white', linewidth=0.5)
        if errors:
            ax.errorbar(categories, values, yerr=errors, fmt='none', color='black', capsize=3)
        ax.set_xlabel(xlabel or category_col)
        ax.set_ylabel(ylabel or value_col)
        plt.xticks(rotation=45, ha='right')

    if title:
        ax.set_title(title, fontsize=14, fontweight='bold')

    # Clean up
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    plt.tight_layout()

    return fig


def plot_time_series(
    df: pl.DataFrame,
    year_col: str = "YEAR",
    value_col: str = "",
    error_col: Optional[str] = None,
    title: str = "",
    xlabel: str = "Year",
    ylabel: str = "",
    figsize: tuple = (10, 6),
    color: str = "#2E7D32",
) -> plt.Figure:
    """
    Create a time series plot for temporal estimation results.

    Parameters
    ----------
    df : pl.DataFrame
        DataFrame containing time series data.
    year_col : str, optional
        Column name for years. Defaults to "YEAR".
    value_col : str
        Column name for values.
    error_col : str, optional
        Column name for error values (shaded confidence band).
    title : str, optional
        Chart title.
    xlabel : str, optional
        X-axis label. Defaults to "Year".
    ylabel : str, optional
        Y-axis label.
    figsize : tuple, optional
        Figure size in inches.
    color : str, optional
        Line color. Defaults to forest green.

    Returns
    -------
    matplotlib.figure.Figure
        The created figure object.
    """
    sorted_df = df.sort(year_col)
    years = sorted_df[year_col].to_list()
    values = sorted_df[value_col].to_list()

    fig, ax = plt.subplots(figsize=figsize)

    ax.plot(years, values, color=color, linewidth=2, marker='o', markersize=6)

    if error_col and error_col in df.columns:
        errors = sorted_df[error_col].to_list()
        lower = [v - e for v, e in zip(values, errors)]
        upper = [v + e for v, e in zip(values, errors)]
        ax.fill_between(years, lower, upper, color=color, alpha=0.2)

    if title:
        ax.set_title(title, fontsize=14, fontweight='bold')
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel or value_col)

    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    plt.tight_layout()

    return fig


def format_large_number(value: Union[int, float], precision: int = 1) -> str:
    """
    Format large numbers with appropriate suffixes (K, M, B).

    Parameters
    ----------
    value : int or float
        Number to format.
    precision : int, optional
        Decimal places. Defaults to 1.

    Returns
    -------
    str
        Formatted string with suffix.

    Example
    -------
    >>> format_large_number(1234567)
    '1.2M'
    >>> format_large_number(5432)
    '5.4K'
    """
    if abs(value) >= 1e9:
        return f"{value/1e9:.{precision}f}B"
    elif abs(value) >= 1e6:
        return f"{value/1e6:.{precision}f}M"
    elif abs(value) >= 1e3:
        return f"{value/1e3:.{precision}f}K"
    else:
        return f"{value:.{precision}f}"


def summarize_estimate(df: pl.DataFrame, estimate_col: str, se_col: str) -> dict:
    """
    Create a summary dictionary from an estimation result.

    Parameters
    ----------
    df : pl.DataFrame
        DataFrame with estimation results.
    estimate_col : str
        Column name for the estimate.
    se_col : str
        Column name for the standard error.

    Returns
    -------
    dict
        Summary statistics including total, mean, min, max, and CV.
    """
    return {
        "total": df[estimate_col].sum(),
        "mean": df[estimate_col].mean(),
        "min": df[estimate_col].min(),
        "max": df[estimate_col].max(),
        "mean_cv": (df[se_col].mean() / df[estimate_col].mean() * 100) if df[estimate_col].mean() else None,
        "n_groups": len(df),
    }
