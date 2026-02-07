"""
Validation utilities for comparing panel harvest estimates against removals.

Both `panel()` and `removals()` now use TREE_GRM_COMPONENT for tree fate
classification, ensuring consistent methodology. This module provides tools
to cross-validate that both functions produce equivalent estimates.

Tree fate classification from GRM:
- CUT1/CUT2: Trees removed by harvest → 'cut' fate
- DIVERSION1/DIVERSION2: Trees removed by land use change → 'diversion' fate
- MORTALITY1/MORTALITY2: Trees that died naturally → 'mortality' fate
- SURVIVOR: Trees alive at both measurements → 'survivor' fate
- INGROWTH: New trees crossing 5" DBH threshold → 'ingrowth' fate

Since both functions use the same GRM source, they should produce very
similar estimates (ratio ~1.0). Significant discrepancies indicate bugs.

References
----------
Bechtold & Patterson (2005), Chapter 4: Change Estimation
FIA Database User Guide, TREE_GRM_COMPONENT documentation
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import polars as pl
from rich.console import Console
from rich.table import Table

from ...core import FIA
from .panel import panel
from .removals import removals

console = Console()


@dataclass
class ComparisonResult:
    """Result of comparing panel to removals estimates."""

    group_values: dict | None  # Group column values (None if no grouping)
    panel_cut_trees: int  # Raw count of cut/diversion trees from panel
    panel_annualized: float  # Panel estimate (TPA * trees / REMPER)
    removals_estimate: float  # Annualized estimate from removals()
    avg_remper: float  # Average remeasurement period from panel
    ratio: float  # panel_annualized / removals_estimate
    abs_diff: float  # Absolute difference
    pct_diff: float  # Percentage difference
    status: str  # "MATCH", "PANEL_HIGHER", "PANEL_LOWER"

    @property
    def is_valid(self) -> bool:
        """
        Check if comparison indicates valid harvest detection.

        Since both panel and removals use GRM components, they should
        produce very similar estimates. The ratio should be close to 1.0.
        """
        # Both use GRM, so ratio should be close to 1.0
        # Allow 20% tolerance for minor computation differences
        return 0.8 <= self.ratio <= 1.2


def compare_panel_to_removals(
    db: str | FIA,
    grp_by: str | list[str] | None = None,
    land_type: str = "forest",
    tree_type: str = "gs",
    measure: Literal["tpa", "volume"] = "tpa",
    min_invyr: int = 2000,
    verbose: bool = True,
    expand: bool = False,
) -> pl.DataFrame:
    """
    Compare panel-identified removals to removals() estimates.

    Both functions now use GRM tables for tree fate classification:
    - panel(level="tree"): Returns trees with TREE_FATE from GRM COMPONENT
    - removals(): Returns annualized estimates from TREE_GRM_COMPONENT

    Since both use the same underlying data, they should produce very
    similar estimates (ratio ~1.0).

    Parameters
    ----------
    db : str | FIA
        Database connection or path to FIA database
    grp_by : str | list[str] | None
        Grouping columns (e.g., "STATECD", ["STATECD", "COUNTYCD"])
    land_type : str
        "forest" or "timber" - must match between both functions
    tree_type : str
        "gs" for growing stock (>= 5" DBH), "all" for all trees
    measure : Literal["tpa", "volume"]
        What to compare: "tpa" (trees per acre) or "volume" (cubic feet)
    min_invyr : int
        Minimum inventory year for panel data (default 2000, post-annual methodology)
    verbose : bool
        Print detailed comparison table using rich
    expand : bool
        If True, use proper per-acre expansion (ADJ_FACTOR × EXPNS) from panel().
        This should produce results closely matching removals() (ratio ~1.0).
        If False (default), use raw TPA_UNADJ values without expansion.

    Returns
    -------
    pl.DataFrame
        Comparison results with columns:
        - Group columns (if specified)
        - PANEL_CUT_TREES: Raw count from panel (cut + diversion)
        - PANEL_ANNUALIZED: Panel estimate per acre per year
        - REMOVALS_ESTIMATE: Annualized estimate from removals()
        - AVG_REMPER: Average remeasurement period
        - RATIO: PANEL_ANNUALIZED / REMOVALS_ESTIMATE
        - ABS_DIFF: Absolute difference
        - PCT_DIFF: Percentage difference
        - STATUS: "MATCH" / "PANEL_HIGHER" / "PANEL_LOWER"

    Notes
    -----
    Since both panel and removals use GRM components:
    - Ratio should be close to 1.0 (0.8-1.2 is acceptable)
    - Significant deviations indicate implementation bugs
    - Minor differences may occur due to aggregation approach
    """
    # Normalize group_by to list
    group_cols = []
    if grp_by:
        group_cols = [grp_by] if isinstance(grp_by, str) else list(grp_by)

    # === Load panel data ===
    # Panel now uses GRM tables directly, matching the tree_type/land_type
    # to the correct GRM column suffixes
    # Get path from FIA instance if needed
    db_path = db.db_path if isinstance(db, FIA) else db

    with FIA(db_path) as fia_db:
        # === Method depends on expand flag ===
        if expand:
            # Use proper per-acre expansion from panel()
            # This uses ADJ_FACTOR × EXPNS for stratified estimation
            panel_expanded = panel(
                fia_db,
                level="tree",
                harvest_only=True,  # Only cut/diversion trees
                land_type=land_type,
                tree_type=tree_type,
                min_invyr=min_invyr,
                expand=True,  # Enable proper expansion
                measure=measure,
                grp_by=group_cols if group_cols else None,
            )

            # Also get raw tree counts for comparison table
            panel_raw = panel(
                fia_db,
                level="tree",
                harvest_only=True,
                land_type=land_type,
                tree_type=tree_type,
                min_invyr=min_invyr,
                expand=False,
            )

            # Build aggregation with expanded per-acre estimate
            if "PANEL_ACRE" in panel_expanded.columns:
                panel_estimate = panel_expanded["PANEL_ACRE"][0]
            elif "PER_ACRE" in panel_expanded.columns:
                panel_estimate = panel_expanded["PER_ACRE"][0]
            else:
                # Try to find the per-acre column
                per_acre_cols = [
                    c for c in panel_expanded.columns if "ACRE" in c.upper()
                ]
                if per_acre_cols:
                    panel_estimate = panel_expanded[per_acre_cols[0]][0]
                else:
                    raise ValueError(
                        f"Could not find per-acre column in expanded panel. "
                        f"Available: {panel_expanded.columns}"
                    )

            panel_cut_trees = panel_raw.height
            avg_remper = (
                panel_raw["REMPER"].mean() if "REMPER" in panel_raw.columns else 5.0
            )

            panel_agg = pl.DataFrame(
                {
                    "PANEL_CUT_TREES": [panel_cut_trees],
                    "PANEL_ANNUALIZED": [panel_estimate],
                    "AVG_REMPER": [avg_remper],
                }
            )

        else:
            # Original method: raw TPA_UNADJ without proper expansion
            # Get panel with GRM-based tree fate classification
            panel_data = panel(
                fia_db,
                level="tree",
                harvest_only=False,  # Get all trees to filter ourselves
                land_type=land_type,
                tree_type=tree_type,  # GRM handles this correctly now
                min_invyr=min_invyr,
            )

            # Filter to removal trees (cut + diversion = total removals)
            # This matches what removals() counts from TREE_GRM_COMPONENT
            cut_trees = panel_data.filter(
                pl.col("TREE_FATE").is_in(["cut", "diversion"])
            )

            # === Aggregate panel results to per-acre basis ===
            # Panel data has TPA_UNADJ (trees per acre unadjusted) for each tree
            # TPA_UNADJ is already a per-acre expansion factor from the GRM tables
            tpa_col = "TPA_UNADJ"  # From GRM COMPONENT
            has_tpa = tpa_col in cut_trees.columns

            # Get total number of unique plots in the GRM sample
            # This should be all plots with GRM data (same as removals() uses)
            n_plots = panel_data["PLT_CN"].n_unique()

            # For a proper comparison with removals:
            # - removals() uses stratified estimation to get total TPA per acre per year
            # - Panel has raw TPA_UNADJ values that sum to total expansion
            # - To compare: panel_total = sum(TPA_UNADJ) / REMPER
            #               panel_per_acre = panel_total / n_plots
            # This gives average TPA per sample plot per year
            # Since each FIA plot represents ~1 acre, this should approximate per-acre rate

            if measure == "tpa":
                if has_tpa:
                    # Calculate sum of TPA_UNADJ and average REMPER
                    if group_cols:
                        available_group_cols = [
                            c for c in group_cols if c in cut_trees.columns
                        ]
                        if not available_group_cols:
                            raise ValueError(
                                f"None of the grouping columns {group_cols} found in panel data. "
                                f"Available columns: {cut_trees.columns[:20]}..."
                            )
                        panel_agg = cut_trees.group_by(available_group_cols).agg(
                            [
                                pl.len().alias("PANEL_CUT_TREES"),
                                pl.col(tpa_col).sum().alias("PANEL_TPA_SUM"),
                                pl.col("REMPER").mean().alias("AVG_REMPER"),
                            ]
                        )
                    else:
                        panel_agg = cut_trees.select(
                            [
                                pl.len().alias("PANEL_CUT_TREES"),
                                pl.col(tpa_col).sum().alias("PANEL_TPA_SUM"),
                                pl.col("REMPER").mean().alias("AVG_REMPER"),
                            ]
                        )
                    # Per-acre per-year estimate:
                    # sum(TPA_UNADJ) gives total expansion
                    # Divide by REMPER for annualization
                    # Divide by n_plots for per-plot (≈per-acre) average
                    panel_agg = panel_agg.with_columns(
                        [
                            (
                                pl.col("PANEL_TPA_SUM") / pl.col("AVG_REMPER") / n_plots
                            ).alias("PANEL_ANNUALIZED")
                        ]
                    )
                else:
                    # Fallback to raw counts if no TPA column
                    if group_cols:
                        available_group_cols = [
                            c for c in group_cols if c in cut_trees.columns
                        ]
                        panel_agg = cut_trees.group_by(available_group_cols).agg(
                            [
                                pl.len().alias("PANEL_CUT_TREES"),
                                pl.col("REMPER").mean().alias("AVG_REMPER"),
                            ]
                        )
                    else:
                        panel_agg = cut_trees.select(
                            [
                                pl.len().alias("PANEL_CUT_TREES"),
                                pl.col("REMPER").mean().alias("AVG_REMPER"),
                            ]
                        )
                    panel_agg = panel_agg.with_columns(
                        [
                            (pl.col("PANEL_CUT_TREES") / pl.col("AVG_REMPER")).alias(
                                "PANEL_ANNUALIZED"
                            )
                        ]
                    )
            else:  # volume
                # GRM-based panel uses t2_VOLCFNET (from TREE_GRM_MIDPT)
                if "t2_VOLCFNET" in cut_trees.columns:
                    vol_col = "t2_VOLCFNET"
                elif "t1_VOLCFNET" in cut_trees.columns:
                    vol_col = "t1_VOLCFNET"
                elif "VOLCFNET" in cut_trees.columns:
                    vol_col = "VOLCFNET"
                else:
                    raise ValueError(
                        "Volume column not found in panel data. "
                        "Expected t2_VOLCFNET, t1_VOLCFNET, or VOLCFNET. "
                        "Make sure 'columns' parameter includes VOLCFNET."
                    )

                if has_tpa:
                    # Proper volume per acre: TPA * volume
                    if group_cols:
                        available_group_cols = [
                            c for c in group_cols if c in cut_trees.columns
                        ]
                        panel_agg = cut_trees.group_by(available_group_cols).agg(
                            [
                                pl.len().alias("PANEL_CUT_TREES"),
                                (pl.col(tpa_col) * pl.col(vol_col))
                                .sum()
                                .alias("PANEL_VOL_SUM"),
                                pl.col("REMPER").mean().alias("AVG_REMPER"),
                            ]
                        )
                    else:
                        panel_agg = cut_trees.select(
                            [
                                pl.len().alias("PANEL_CUT_TREES"),
                                (pl.col(tpa_col) * pl.col(vol_col))
                                .sum()
                                .alias("PANEL_VOL_SUM"),
                                pl.col("REMPER").mean().alias("AVG_REMPER"),
                            ]
                        )
                    panel_agg = panel_agg.with_columns(
                        [
                            (pl.col("PANEL_VOL_SUM") / pl.col("AVG_REMPER")).alias(
                                "PANEL_ANNUALIZED"
                            )
                        ]
                    )
                else:
                    # Fallback without TPA
                    if group_cols:
                        available_group_cols = [
                            c for c in group_cols if c in cut_trees.columns
                        ]
                        panel_agg = cut_trees.group_by(available_group_cols).agg(
                            [
                                pl.len().alias("PANEL_CUT_TREES"),
                                pl.col(vol_col).sum().alias("PANEL_VOL_SUM"),
                                pl.col("REMPER").mean().alias("AVG_REMPER"),
                            ]
                        )
                    else:
                        panel_agg = cut_trees.select(
                            [
                                pl.len().alias("PANEL_CUT_TREES"),
                                pl.col(vol_col).sum().alias("PANEL_VOL_SUM"),
                                pl.col("REMPER").mean().alias("AVG_REMPER"),
                            ]
                        )
                    panel_agg = panel_agg.with_columns(
                        [
                            (pl.col("PANEL_VOL_SUM") / pl.col("AVG_REMPER")).alias(
                                "PANEL_ANNUALIZED"
                            )
                        ]
                    )

        # === Load removals data (per-acre estimates) ===
        removals_data = removals(
            fia_db,
            grp_by=grp_by,
            land_type=land_type,
            tree_type=tree_type,
            measure=measure,
            totals=True,  # Need totals to get expanded estimates
        )

        # Extract the PER_ACRE removals estimate column (comparable to panel TPA sum)
        removals_col = None
        for col in removals_data.columns:
            if "REMOVALS" in col.upper() and "SE" not in col.upper():
                if "PER_ACRE" in col.upper():
                    removals_col = col
                    break

        if removals_col is None:
            # Try TOTAL if no PER_ACRE (we'll note this mismatch)
            for col in removals_data.columns:
                if (
                    "REMOVALS" in col.upper()
                    and "SE" not in col.upper()
                    and "TOTAL" in col.upper()
                ):
                    removals_col = col
                    break

        if removals_col is None:
            # Fall back to any removals column
            for col in removals_data.columns:
                if "REMV" in col.upper() or "REMOVALS" in col.upper():
                    if "SE" not in col.upper():
                        removals_col = col
                        break

        if removals_col is None:
            raise ValueError(
                f"Could not find removals estimate column. "
                f"Available columns: {removals_data.columns}"
            )

        removals_data = removals_data.rename({removals_col: "REMOVALS_ESTIMATE"})

    # === Join and compare ===
    if group_cols:
        available_group_cols = [c for c in group_cols if c in panel_agg.columns]
        # Find matching columns in removals
        removals_group_cols = [c for c in group_cols if c in removals_data.columns]

        if available_group_cols and removals_group_cols:
            # Join on common group columns
            common_cols = list(set(available_group_cols) & set(removals_group_cols))
            if common_cols:
                comparison = panel_agg.join(
                    removals_data.select(common_cols + ["REMOVALS_ESTIMATE"]),
                    on=common_cols,
                    how="outer",
                )
            else:
                # No common columns, create cross comparison
                comparison = panel_agg.with_columns(
                    [
                        pl.lit(removals_data["REMOVALS_ESTIMATE"][0]).alias(
                            "REMOVALS_ESTIMATE"
                        )
                    ]
                )
        else:
            comparison = panel_agg.with_columns(
                [
                    pl.lit(removals_data["REMOVALS_ESTIMATE"][0]).alias(
                        "REMOVALS_ESTIMATE"
                    )
                ]
            )
    else:
        # No grouping - simple comparison
        comparison = panel_agg.with_columns(
            [pl.lit(removals_data["REMOVALS_ESTIMATE"][0]).alias("REMOVALS_ESTIMATE")]
        )

    # Fill nulls
    comparison = comparison.with_columns(
        [
            pl.col("PANEL_CUT_TREES").fill_null(0),
            pl.col("PANEL_ANNUALIZED").fill_null(0.0),
            pl.col("REMOVALS_ESTIMATE").fill_null(0.0),
            pl.col("AVG_REMPER").fill_null(5.0),  # Default remper
        ]
    )

    # Calculate comparison metrics
    comparison = comparison.with_columns(
        [
            # Ratio (handle division by zero)
            pl.when(pl.col("REMOVALS_ESTIMATE") > 0)
            .then(pl.col("PANEL_ANNUALIZED") / pl.col("REMOVALS_ESTIMATE"))
            .when(pl.col("PANEL_ANNUALIZED") > 0)
            .then(pl.lit(float("inf")))
            .otherwise(pl.lit(1.0))
            .alias("RATIO"),
            # Absolute difference
            (pl.col("PANEL_ANNUALIZED") - pl.col("REMOVALS_ESTIMATE")).alias(
                "ABS_DIFF"
            ),
        ]
    )

    comparison = comparison.with_columns(
        [
            # Percentage difference
            pl.when(pl.col("REMOVALS_ESTIMATE") > 0)
            .then((pl.col("ABS_DIFF") / pl.col("REMOVALS_ESTIMATE") * 100).abs())
            .otherwise(pl.lit(100.0))
            .alias("PCT_DIFF"),
            # Status classification
            pl.when(pl.col("RATIO").is_between(0.8, 1.2))
            .then(pl.lit("MATCH"))
            .when(pl.col("RATIO") > 1.2)
            .then(pl.lit("PANEL_HIGHER"))
            .when(pl.col("RATIO") < 0.8)
            .then(pl.lit("PANEL_LOWER"))
            .otherwise(pl.lit("UNKNOWN"))
            .alias("STATUS"),
        ]
    )

    # Print results if verbose
    if verbose:
        _print_comparison_table(comparison, measure, group_cols)

    return comparison


def _print_comparison_table(
    comparison: pl.DataFrame,
    measure: str,
    group_cols: list[str],
) -> None:
    """Print a rich table of comparison results."""
    table = Table(
        title=f"Panel vs Removals Comparison ({measure.upper()})",
        show_header=True,
        header_style="bold cyan",
    )

    # Add columns
    for col in group_cols:
        if col in comparison.columns:
            table.add_column(col, style="dim")

    table.add_column("Panel Cut", justify="right")
    table.add_column("Panel Ann.", justify="right", style="green")
    table.add_column("Removals", justify="right", style="blue")
    table.add_column("Ratio", justify="right")
    table.add_column("Status", justify="center")

    # Add rows
    for row in comparison.iter_rows(named=True):
        row_values = []

        for col in group_cols:
            if col in comparison.columns:
                row_values.append(str(row.get(col, "")))

        row_values.append(f"{row['PANEL_CUT_TREES']:,}")
        row_values.append(f"{row['PANEL_ANNUALIZED']:,.1f}")
        row_values.append(f"{row['REMOVALS_ESTIMATE']:,.1f}")
        row_values.append(f"{row['RATIO']:.2f}")

        # Color-code status
        status = row["STATUS"]
        if status == "MATCH":
            status_str = "[green]MATCH[/green]"
        elif status == "PANEL_HIGHER":
            status_str = "[yellow]PANEL_HIGHER[/yellow]"
        elif status == "PANEL_LOWER":
            status_str = "[red]PANEL_LOWER[/red]"
        else:
            status_str = status

        row_values.append(status_str)

        table.add_row(*row_values)

    console.print(table)

    # Summary interpretation
    console.print()
    if comparison["STATUS"].to_list().count("PANEL_LOWER") > 0:
        console.print(
            "[yellow]Warning:[/yellow] Panel found fewer trees than removals. "
            "Both use GRM components - check implementation for bugs."
        )
    if comparison["STATUS"].to_list().count("PANEL_HIGHER") > 0:
        console.print(
            "[yellow]Note:[/yellow] Panel found more trees than removals. "
            "Both use GRM components - check aggregation differences."
        )


def diagnose_panel_removals_diff(
    db: str | FIA,
    land_type: str = "forest",
    tree_type: str = "gs",
    min_invyr: int = 2000,
    sample_size: int = 100,
    verbose: bool = True,
) -> pl.DataFrame:
    """
    Diagnose tree fate distribution from panel.

    This function helps understand the tree fate breakdown from GRM-based
    panel classification.

    Parameters
    ----------
    db : str | FIA
        Database connection
    land_type : str
        Land type filter
    tree_type : str
        Tree type filter
    min_invyr : int
        Minimum inventory year
    sample_size : int
        Number of trees to sample for detailed diagnosis
    verbose : bool
        Print diagnostic information

    Returns
    -------
    pl.DataFrame
        Tree fate counts showing distribution of fates
    """
    # Get path from FIA instance if needed
    db_path = db.db_path if isinstance(db, FIA) else db

    with FIA(db_path) as fia_db:
        # Get panel data with tree fate from GRM
        panel_data = panel(
            fia_db,
            level="tree",
            harvest_only=False,
            land_type=land_type,
            tree_type=tree_type,
            min_invyr=min_invyr,
        )

        # Count by tree fate
        fate_counts = panel_data.group_by("TREE_FATE").agg(
            [
                pl.count().alias("COUNT"),
            ]
        )

        if verbose:
            console.print(
                "\n[bold]Tree Fate Distribution (from GRM-based panel):[/bold]"
            )
            table = Table(show_header=True, header_style="bold cyan")
            table.add_column("Tree Fate")
            table.add_column("Count", justify="right")
            table.add_column("Percent", justify="right")

            total = fate_counts["COUNT"].sum()
            for row in fate_counts.sort("COUNT", descending=True).iter_rows(named=True):
                pct = row["COUNT"] / total * 100 if total > 0 else 0
                table.add_row(row["TREE_FATE"], f"{row['COUNT']:,}", f"{pct:.1f}%")

            console.print(table)

        # Analyze removal trees (cut + diversion)
        removal_trees = panel_data.filter(
            pl.col("TREE_FATE").is_in(["cut", "diversion"])
        )

        if verbose:
            console.print("\n[bold]Removal Trees Analysis:[/bold]")
            console.print(f"Total removal trees: {len(removal_trees):,}")

            # Break down by cut vs diversion
            cut_count = panel_data.filter(pl.col("TREE_FATE") == "cut").height
            div_count = panel_data.filter(pl.col("TREE_FATE") == "diversion").height

            console.print(f"  - Cut (harvest): {cut_count:,}")
            console.print(f"  - Diversion (land use change): {div_count:,}")

            # Show GRM component distribution if available
            if "COMPONENT" in removal_trees.columns:
                console.print("\n[bold]GRM Component Breakdown:[/bold]")
                comp_counts = (
                    removal_trees.group_by("COMPONENT")
                    .len()
                    .sort("len", descending=True)
                )
                for row in comp_counts.iter_rows(named=True):
                    console.print(f"  - {row['COMPONENT']}: {row['len']:,}")

        # Sample trees for detailed inspection
        if sample_size > 0 and len(removal_trees) > 0:
            sample = removal_trees.head(sample_size)

            if verbose:
                console.print(
                    f"\n[bold]Sample of {min(sample_size, len(removal_trees))} removal trees:[/bold]"
                )

                sample_cols = ["PLT_CN", "TRE_CN", "TREE_FATE"]
                if "COMPONENT" in sample.columns:
                    sample_cols.append("COMPONENT")
                if "DIA_BEGIN" in sample.columns:
                    sample_cols.append("DIA_BEGIN")
                if "TPA_UNADJ" in sample.columns:
                    sample_cols.append("TPA_UNADJ")

                console.print(sample.select(sample_cols))

        return fate_counts


def validate_panel_harvest(
    db: str | FIA,
    tolerance_ratio: float = 0.8,
    verbose: bool = True,
    expand: bool = False,
) -> bool:
    """
    Validate that panel harvest detection is consistent with removals.

    This is a simple pass/fail validation suitable for automated testing.

    Parameters
    ----------
    db : str | FIA
        Database connection
    tolerance_ratio : float
        Minimum acceptable ratio of panel_annualized / removals_estimate
        Default 0.8 allows panel to find up to 20% fewer trees than removals
    verbose : bool
        Print validation details
    expand : bool
        If True, use proper per-acre expansion from panel().
        This should produce a ratio close to 1.0.
        If False (default), use raw TPA_UNADJ values.

    Returns
    -------
    bool
        True if validation passes, False otherwise
    """
    comparison = compare_panel_to_removals(
        db,
        measure="tpa",
        tree_type="gs",
        verbose=verbose,
        expand=expand,
    )

    # Check if ratio is acceptable
    ratio = comparison["RATIO"][0]

    if ratio >= tolerance_ratio:
        if verbose:
            console.print(
                f"\n[green]VALIDATION PASSED[/green]: "
                f"Ratio {ratio:.2f} >= {tolerance_ratio:.2f}"
            )
        return True
    else:
        if verbose:
            console.print(
                f"\n[red]VALIDATION FAILED[/red]: "
                f"Ratio {ratio:.2f} < {tolerance_ratio:.2f}"
            )
        return False
