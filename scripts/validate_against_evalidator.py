#!/usr/bin/env python
"""
Validate pyFIA estimates against official USFS EVALIDator values.

This script compares pyFIA estimation results with EVALIDator API responses
to verify statistical accuracy.
"""

from pathlib import Path
from typing import Tuple
import polars as pl
from rich.console import Console
from rich.table import Table
from rich import box

from pyfia import FIA, area, volume, biomass, tpa
from pyfia.evalidator.client import EVALIDatorClient
from pyfia.evalidator.estimate_types import EstimateType
from pyfia.evalidator.validation import compare_estimates

console = Console()


def extract_estimate_and_se(result: pl.DataFrame, estimate_type: str) -> Tuple[float, float]:
    """
    Extract the estimate value and standard error from a pyFIA result DataFrame.

    Different estimators use different column naming conventions:
    - area(): AREA, AREA_SE
    - volume(): VOLCFNET_TOTAL, VOLCFNET_TOTAL_SE
    - biomass(): BIO_TOTAL, BIO_TOTAL_SE
    - tpa(): TPA_TOTAL, TPA_TOTAL_SE (when totals=True)
    """
    cols = result.columns

    # Find the main estimate column and corresponding SE column
    est_col = None
    se_col = None

    if estimate_type == "area":
        est_col = "AREA" if "AREA" in cols else None
        se_col = "AREA_SE" if "AREA_SE" in cols else None
    elif estimate_type == "volume":
        # Look for VOLCFNET_TOTAL or similar volume total columns
        for col in cols:
            if col.endswith("_TOTAL") and "VOL" in col.upper() and "SE" not in col:
                est_col = col
                se_col = f"{col}_SE" if f"{col}_SE" in cols else None
                break
    elif estimate_type == "biomass":
        # Look for BIO_TOTAL or BIOMASS_TOTAL
        for col in cols:
            if col.endswith("_TOTAL") and "BIO" in col.upper() and "SE" not in col:
                est_col = col
                se_col = f"{col}_SE" if f"{col}_SE" in cols else None
                break
    elif estimate_type == "tpa":
        # Look for TPA_TOTAL
        for col in cols:
            if col.endswith("_TOTAL") and "TPA" in col.upper() and "SE" not in col:
                est_col = col
                se_col = f"{col}_SE" if f"{col}_SE" in cols else None
                break

    # Fallback: look for columns ending in _TOTAL
    if est_col is None:
        for col in cols:
            col_upper = col.upper()
            if col_upper.endswith("_TOTAL") and "EXPNS" not in col_upper and "SE" not in col_upper:
                est_col = col
                # Look for corresponding SE column
                se_col = f"{col}_SE" if f"{col}_SE" in cols else None
                break

    # Final fallback for SE: look for _TOTAL_SE columns (not _ACRE_SE)
    if se_col is None:
        for col in cols:
            col_upper = col.upper()
            if "_TOTAL_SE" in col_upper:
                se_col = col
                break

    if est_col is None:
        raise ValueError(f"Could not find estimate column in result: {cols}")

    estimate = float(result[est_col][0])
    se = float(result[se_col][0]) if se_col else 0.0

    return estimate, se


def validate_state(
    db_path: str,
    state_code: int,
    state_name: str,
    year: int,
    client: EVALIDatorClient
) -> list[dict]:
    """Run validation tests for a single state."""
    results = []

    console.print(f"\n[bold blue]{'='*60}[/]")
    console.print(f"[bold blue]Validating {state_name} (State {state_code}, Year {year})[/]")
    console.print(f"[bold blue]{'='*60}[/]")

    with FIA(db_path) as db:
        # Test 1: Forest Area
        # Note: EVALIDator uses EXPCURR evaluation type for forest area (snum=2),
        # not EXPALL. EXPCURR is designed for "current area" (forestland) estimation
        # with different expansion factors than EXPALL.
        console.print("\n[yellow]Testing Forest Area...[/]")
        try:
            db.clip_by_state(state_code)
            db.clip_most_recent(eval_type="CURR")

            pyfia_result = area(db, land_type="forest")
            pyfia_value, pyfia_se = extract_estimate_and_se(pyfia_result, "area")

            # Get EVALIDator estimate
            ev_result = client.get_forest_area(state_code=state_code, year=year)

            validation = compare_estimates(pyfia_value, pyfia_se, ev_result)

            results.append({
                "state": state_name,
                "estimate_type": "Forest Area",
                "units": "acres",
                "pyfia": pyfia_value,
                "pyfia_se": pyfia_se,
                "evalidator": ev_result.estimate,
                "evalidator_se": ev_result.sampling_error,
                "pct_diff": validation.pct_diff,
                "passed": validation.passed,
                "message": validation.message
            })

            console.print(f"  pyFIA:       {pyfia_value:>20,.0f} acres (SE: {pyfia_se:,.0f})")
            console.print(f"  EVALIDator:  {ev_result.estimate:>20,.0f} acres (SE: {ev_result.sampling_error:,.0f})")
            console.print(f"  Difference:  {validation.pct_diff:>20.2f}%")
            status = "[green]PASS[/]" if validation.passed else "[red]FAIL[/]"
            console.print(f"  Status:      {status} - {validation.message}")

        except Exception as e:
            console.print(f"  [red]Error: {e}[/]")
            results.append({
                "state": state_name,
                "estimate_type": "Forest Area",
                "units": "acres",
                "error": str(e)
            })

    # Test 2: Timberland Area
    # Note: EVALIDator uses EXPCURR for timberland area (snum=3) as well
    with FIA(db_path) as db:
        console.print("\n[yellow]Testing Timberland Area...[/]")
        try:
            db.clip_by_state(state_code)
            db.clip_most_recent(eval_type="CURR")

            pyfia_result = area(db, land_type="timber")
            pyfia_value, pyfia_se = extract_estimate_and_se(pyfia_result, "area")

            ev_result = client.get_forest_area(state_code=state_code, year=year, land_type="timber")

            validation = compare_estimates(pyfia_value, pyfia_se, ev_result)

            results.append({
                "state": state_name,
                "estimate_type": "Timberland Area",
                "units": "acres",
                "pyfia": pyfia_value,
                "pyfia_se": pyfia_se,
                "evalidator": ev_result.estimate,
                "evalidator_se": ev_result.sampling_error,
                "pct_diff": validation.pct_diff,
                "passed": validation.passed,
                "message": validation.message
            })

            console.print(f"  pyFIA:       {pyfia_value:>20,.0f} acres (SE: {pyfia_se:,.0f})")
            console.print(f"  EVALIDator:  {ev_result.estimate:>20,.0f} acres (SE: {ev_result.sampling_error:,.0f})")
            console.print(f"  Difference:  {validation.pct_diff:>20.2f}%")
            status = "[green]PASS[/]" if validation.passed else "[red]FAIL[/]"
            console.print(f"  Status:      {status} - {validation.message}")

        except Exception as e:
            console.print(f"  [red]Error: {e}[/]")
            results.append({
                "state": state_name,
                "estimate_type": "Timberland Area",
                "units": "acres",
                "error": str(e)
            })

    # Test 3: Volume (Net Growing-Stock)
    # Note: EVALIDator snum=15 returns net volume of growing-stock trees (TREECLCD=2)
    with FIA(db_path) as db:
        console.print("\n[yellow]Testing Net Volume (Growing-Stock)...[/]")
        try:
            db.clip_by_state(state_code)
            db.clip_most_recent(eval_type="VOL")

            # Growing-stock trees = TREECLCD == 2
            pyfia_result = volume(db, land_type="forest", vol_type="net", tree_domain="TREECLCD == 2")
            pyfia_value, pyfia_se = extract_estimate_and_se(pyfia_result, "volume")

            ev_result = client.get_volume(state_code=state_code, year=year, vol_type="net")

            validation = compare_estimates(pyfia_value, pyfia_se, ev_result, tolerance_pct=10.0)

            results.append({
                "state": state_name,
                "estimate_type": "GS Net Volume",
                "units": "cu ft",
                "pyfia": pyfia_value,
                "pyfia_se": pyfia_se,
                "evalidator": ev_result.estimate,
                "evalidator_se": ev_result.sampling_error,
                "pct_diff": validation.pct_diff,
                "passed": validation.passed,
                "message": validation.message
            })

            console.print(f"  pyFIA:       {pyfia_value:>20,.0f} cu ft (SE: {pyfia_se:,.0f})")
            console.print(f"  EVALIDator:  {ev_result.estimate:>20,.0f} cu ft (SE: {ev_result.sampling_error:,.0f})")
            console.print(f"  Difference:  {validation.pct_diff:>20.2f}%")
            status = "[green]PASS[/]" if validation.passed else "[red]FAIL[/]"
            console.print(f"  Status:      {status} - {validation.message}")

        except Exception as e:
            console.print(f"  [red]Error: {e}[/]")
            results.append({
                "state": state_name,
                "estimate_type": "GS Net Volume",
                "units": "cu ft",
                "error": str(e)
            })

    # Test 4: Biomass (Aboveground)
    with FIA(db_path) as db:
        console.print("\n[yellow]Testing Aboveground Biomass...[/]")
        try:
            db.clip_by_state(state_code)
            db.clip_most_recent(eval_type="VOL")

            pyfia_result = biomass(db, land_type="forest", component="ag")
            pyfia_value, pyfia_se = extract_estimate_and_se(pyfia_result, "biomass")

            ev_result = client.get_biomass(state_code=state_code, year=year, component="ag")

            validation = compare_estimates(pyfia_value, pyfia_se, ev_result, tolerance_pct=10.0)

            results.append({
                "state": state_name,
                "estimate_type": "AG Biomass",
                "units": "tons",
                "pyfia": pyfia_value,
                "pyfia_se": pyfia_se,
                "evalidator": ev_result.estimate,
                "evalidator_se": ev_result.sampling_error,
                "pct_diff": validation.pct_diff,
                "passed": validation.passed,
                "message": validation.message
            })

            console.print(f"  pyFIA:       {pyfia_value:>20,.0f} tons (SE: {pyfia_se:,.0f})")
            console.print(f"  EVALIDator:  {ev_result.estimate:>20,.0f} tons (SE: {ev_result.sampling_error:,.0f})")
            console.print(f"  Difference:  {validation.pct_diff:>20.2f}%")
            status = "[green]PASS[/]" if validation.passed else "[red]FAIL[/]"
            console.print(f"  Status:      {status} - {validation.message}")

        except Exception as e:
            console.print(f"  [red]Error: {e}[/]")
            results.append({
                "state": state_name,
                "estimate_type": "AG Biomass",
                "units": "tons",
                "error": str(e)
            })

    # Test 5: Tree Count (Growing-stock trees >=5" DBH)
    # Note: EVALIDator snum=5 returns growing-stock trees (TREECLCD=2) only
    with FIA(db_path) as db:
        console.print("\n[yellow]Testing Growing-Stock Tree Count (>=5\" DBH)...[/]")
        try:
            db.clip_by_state(state_code)
            db.clip_most_recent(eval_type="VOL")

            # Growing-stock trees = TREECLCD == 2 (not rough cull or short-log cull)
            pyfia_result = tpa(db, land_type="forest", tree_domain="DIA >= 5.0 AND TREECLCD == 2", totals=True)
            pyfia_value, pyfia_se = extract_estimate_and_se(pyfia_result, "tpa")

            ev_result = client.get_tree_count(state_code=state_code, year=year, min_diameter=5.0)

            validation = compare_estimates(pyfia_value, pyfia_se, ev_result, tolerance_pct=10.0)

            results.append({
                "state": state_name,
                "estimate_type": "GS Tree Count (5\"+)",
                "units": "trees",
                "pyfia": pyfia_value,
                "pyfia_se": pyfia_se,
                "evalidator": ev_result.estimate,
                "evalidator_se": ev_result.sampling_error,
                "pct_diff": validation.pct_diff,
                "passed": validation.passed,
                "message": validation.message
            })

            console.print(f"  pyFIA:       {pyfia_value:>20,.0f} trees (SE: {pyfia_se:,.0f})")
            console.print(f"  EVALIDator:  {ev_result.estimate:>20,.0f} trees (SE: {ev_result.sampling_error:,.0f})")
            console.print(f"  Difference:  {validation.pct_diff:>20.2f}%")
            status = "[green]PASS[/]" if validation.passed else "[red]FAIL[/]"
            console.print(f"  Status:      {status} - {validation.message}")

        except Exception as e:
            console.print(f"  [red]Error: {e}[/]")
            results.append({
                "state": state_name,
                "estimate_type": "GS Tree Count (5\"+)",
                "units": "trees",
                "error": str(e)
            })

    return results


def print_summary(all_results: list[dict]):
    """Print a summary table of all validation results."""
    console.print("\n\n")
    console.print("[bold cyan]" + "="*80 + "[/]")
    console.print("[bold cyan]VALIDATION SUMMARY[/]")
    console.print("[bold cyan]" + "="*80 + "[/]")

    table = Table(box=box.ROUNDED, show_header=True, header_style="bold magenta")
    table.add_column("State", style="cyan")
    table.add_column("Estimate", style="white")
    table.add_column("pyFIA", justify="right")
    table.add_column("EVALIDator", justify="right")
    table.add_column("Diff %", justify="right")
    table.add_column("Status", justify="center")

    passed = 0
    failed = 0
    errors = 0

    for r in all_results:
        if "error" in r:
            table.add_row(
                r["state"],
                r["estimate_type"],
                "-",
                "-",
                "-",
                "[yellow]ERROR[/]"
            )
            errors += 1
        else:
            status = "[green]PASS[/]" if r["passed"] else "[red]FAIL[/]"
            if r["passed"]:
                passed += 1
            else:
                failed += 1

            # Format numbers based on magnitude
            if r["pyfia"] > 1e9:
                pyfia_str = f"{r['pyfia']/1e9:.2f}B"
                ev_str = f"{r['evalidator']/1e9:.2f}B"
            elif r["pyfia"] > 1e6:
                pyfia_str = f"{r['pyfia']/1e6:.2f}M"
                ev_str = f"{r['evalidator']/1e6:.2f}M"
            else:
                pyfia_str = f"{r['pyfia']:,.0f}"
                ev_str = f"{r['evalidator']:,.0f}"

            table.add_row(
                r["state"],
                r["estimate_type"],
                pyfia_str,
                ev_str,
                f"{r['pct_diff']:.2f}%",
                status
            )

    console.print(table)

    console.print(f"\n[bold]Results: [green]{passed} passed[/], [red]{failed} failed[/], [yellow]{errors} errors[/][/]")

    if failed > 0:
        console.print("\n[bold red]Failed validations may indicate:[/]")
        console.print("  - Different EVALID/year selection")
        console.print("  - Different tree/area domain filters")
        console.print("  - Methodology differences in expansion factors")
        console.print("  - Unit conversion issues")


def main():
    console.print("[bold green]pyFIA vs EVALIDator Validation Test[/]")
    console.print("Comparing pyFIA estimates against official USFS EVALIDator values\n")

    client = EVALIDatorClient(timeout=60)
    all_results = []

    # Test Rhode Island
    ri_results = validate_state(
        db_path=str(Path(__file__).parent.parent / "data" / "ri" / "ri" / "ri.duckdb"),
        state_code=44,
        state_name="Rhode Island",
        year=2024,  # Use 2024 to match most recent EVALID in database
        client=client
    )
    all_results.extend(ri_results)

    # Print summary
    print_summary(all_results)


if __name__ == "__main__":
    main()
