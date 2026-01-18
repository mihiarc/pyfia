"""
EVALIDator API benchmark tests.

Benchmarks the USDA Forest Service EVALIDator API for comparison against pyFIA.
"""

import gc
import time
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

import requests
from rich.console import Console

from benchmarks.comparison.timing import TimingResult

console = Console()

# EVALIDator API base URL
EVALIDATOR_API_URL = "https://apps.fs.usda.gov/fiadb-api/fullreport"

# State FIPS codes for EVALIDator wc parameter
STATE_EVALIDS = {
    "RI": "44",  # Rhode Island
    "DE": "10",  # Delaware
    "CT": "09",  # Connecticut
    "NC": "37",  # North Carolina
    "GA": "13",  # Georgia
}


def get_most_recent_evalid(state_fips: str) -> Optional[str]:
    """
    Get the most recent EVALID for a state.

    The wc parameter is state FIPS + 4-digit inventory year.
    """
    # Query parameters endpoint to find valid EVALIDs
    url = "https://apps.fs.usda.gov/fiadb-api/fullreport/parameters/wc"

    try:
        response = requests.get(url, timeout=30)
        if response.status_code == 200:
            data = response.json()
            # Find EVALIDs for this state
            state_evalids = [
                item["value"]
                for item in data
                if item["value"].startswith(state_fips)
            ]
            if state_evalids:
                # Return most recent (highest year)
                return sorted(state_evalids, reverse=True)[0]
    except Exception as e:
        console.print(f"[yellow]Warning: Could not fetch EVALID: {e}[/yellow]")

    # Fallback: assume 2023 inventory
    return f"{state_fips}2023"


def build_evalidator_request(
    evalid: str,
    snum: str,
    rselected: str = "None",
    cselected: str = "None",
    output_format: str = "NJSON",
) -> Dict[str, str]:
    """
    Build EVALIDator API request parameters.

    Parameters
    ----------
    evalid : str
        Evaluation ID (state FIPS + 4-digit year)
    snum : str
        Attribute number for the estimate
    rselected : str
        Row grouping
    cselected : str
        Column grouping
    output_format : str
        Output format (NJSON for flat JSON)

    Returns
    -------
    Dict[str, str]
        Request parameters
    """
    return {
        "wc": evalid,
        "snum": snum,
        "rselected": rselected,
        "cselected": cselected,
        "outputFormat": output_format,
    }


# Common estimate attribute numbers (snum) for EVALIDator
# These are numeric codes, not text descriptions
# Reference: https://apps.fs.usda.gov/fiadb-api/fullreport/parameters/snum
ESTIMATE_ATTRIBUTES = {
    "area_forest": "2",  # Area of forest land, in acres
    "volume_net_cf": "6",  # Net volume of live trees (5+ in), cubic feet
    "biomass_ag": "16",  # Aboveground dry weight of live trees, dry short tons
    "tpa": "3",  # Number of live trees (1+ inch d.b.h./d.r.c.)
    "mortality_tpa": "111",  # Average annual mortality (5+ in), number of trees
}

# Row grouping options - use actual parameter names
ROW_GROUPINGS = {
    "none": "None",
    "species": "Species",
    "species_group": "Species group",
    "ownership": "Ownership group",
    "size_class": "Stand-size class",
    "diameter_class": "Diameter class (2-inch)",
}


def call_evalidator_api(
    params: Dict[str, str],
    timeout: float = 60.0,
) -> tuple[float, Optional[Dict[str, Any]], Optional[str]]:
    """
    Call EVALIDator API and return timing + result.

    Returns
    -------
    tuple
        (elapsed_ms, result_dict, error_message)
    """
    try:
        start = time.perf_counter()
        response = requests.get(EVALIDATOR_API_URL, params=params, timeout=timeout)
        elapsed = (time.perf_counter() - start) * 1000

        if response.status_code == 200:
            return elapsed, response.json(), None
        else:
            return elapsed, None, f"HTTP {response.status_code}: {response.text[:200]}"

    except requests.exceptions.Timeout:
        return 0.0, None, "Request timed out"
    except Exception as e:
        return 0.0, None, str(e)


def benchmark_evalidator(
    evalid: str,
    snum: str,
    rselected: str,
    name: str,
    iterations: int = 5,
) -> TimingResult:
    """
    Benchmark a single EVALIDator API call.

    Note: Fewer iterations than local tools due to network latency
    and API rate limiting considerations.
    """
    times = []
    cold_start = None
    error = None

    params = build_evalidator_request(evalid, snum, rselected)

    # Cold start
    gc.collect()
    elapsed, result, err = call_evalidator_api(params)
    if err:
        return TimingResult(
            name=name, tool="evalidator", iterations=0, times_ms=[], error=err
        )
    cold_start = elapsed

    # Timed iterations (including first as warmup)
    for _ in range(iterations):
        gc.collect()
        elapsed, result, err = call_evalidator_api(params)
        if err:
            error = err
            break
        times.append(elapsed)

    return TimingResult(
        name=name,
        tool="evalidator",
        iterations=iterations,
        times_ms=times,
        cold_start_ms=cold_start,
        error=error,
    )


def run_evalidator_benchmarks(
    state: str,
    iterations: int = 5,
) -> List[TimingResult]:
    """
    Run EVALIDator API benchmarks.

    Parameters
    ----------
    state : str
        State abbreviation (e.g., 'RI', 'DE')
    iterations : int
        Number of benchmark iterations (lower than local tools due to network)

    Returns
    -------
    List[TimingResult]
        List of benchmark results
    """
    results = []

    state_fips = STATE_EVALIDS.get(state.upper())
    if not state_fips:
        console.print(f"[red]Unknown state: {state}[/red]")
        return results

    console.print(f"[bold]Running EVALIDator API benchmarks for {state}[/bold]")

    # Get most recent EVALID
    evalid = get_most_recent_evalid(state_fips)
    console.print(f"  Using EVALID: {evalid}")
    console.print(f"  Iterations: {iterations}")
    console.print(f"  Note: Network latency included in measurements")

    # ==========================================================================
    # Benchmark 1: Area Total
    # ==========================================================================
    console.print("\n  [cyan]1. Area estimation (total forest)[/cyan]")
    results.append(
        benchmark_evalidator(
            evalid=evalid,
            snum=ESTIMATE_ATTRIBUTES["area_forest"],  # snum=2
            rselected=ROW_GROUPINGS["none"],
            name="area_total",
            iterations=iterations,
        )
    )

    # ==========================================================================
    # Benchmark 2: Area by Ownership
    # ==========================================================================
    console.print("  [cyan]2. Area by ownership[/cyan]")
    results.append(
        benchmark_evalidator(
            evalid=evalid,
            snum=ESTIMATE_ATTRIBUTES["area_forest"],
            rselected=ROW_GROUPINGS["ownership"],
            name="area_by_ownership",
            iterations=iterations,
        )
    )

    # ==========================================================================
    # Benchmark 3: Volume Total
    # ==========================================================================
    console.print("  [cyan]3. Volume estimation (total)[/cyan]")
    results.append(
        benchmark_evalidator(
            evalid=evalid,
            snum=ESTIMATE_ATTRIBUTES["volume_net_cf"],  # snum=6
            rselected=ROW_GROUPINGS["none"],
            name="volume_total",
            iterations=iterations,
        )
    )

    # ==========================================================================
    # Benchmark 4: Volume by Species
    # ==========================================================================
    console.print("  [cyan]4. Volume by species[/cyan]")
    results.append(
        benchmark_evalidator(
            evalid=evalid,
            snum=ESTIMATE_ATTRIBUTES["volume_net_cf"],
            rselected=ROW_GROUPINGS["species"],
            name="volume_by_species",
            iterations=iterations,
        )
    )

    # ==========================================================================
    # Benchmark 5: Volume by Size Class
    # ==========================================================================
    console.print("  [cyan]5. Volume by size class[/cyan]")
    results.append(
        benchmark_evalidator(
            evalid=evalid,
            snum=ESTIMATE_ATTRIBUTES["volume_net_cf"],
            rselected=ROW_GROUPINGS["diameter_class"],
            name="volume_by_sizeclass",
            iterations=iterations,
        )
    )

    # ==========================================================================
    # Benchmark 6: TPA Total
    # ==========================================================================
    console.print("  [cyan]6. TPA estimation (total)[/cyan]")
    results.append(
        benchmark_evalidator(
            evalid=evalid,
            snum=ESTIMATE_ATTRIBUTES["tpa"],  # snum=3
            rselected=ROW_GROUPINGS["none"],
            name="tpa_total",
            iterations=iterations,
        )
    )

    # ==========================================================================
    # Benchmark 7: TPA by Species
    # ==========================================================================
    console.print("  [cyan]7. TPA by species[/cyan]")
    results.append(
        benchmark_evalidator(
            evalid=evalid,
            snum=ESTIMATE_ATTRIBUTES["tpa"],
            rselected=ROW_GROUPINGS["species"],
            name="tpa_by_species",
            iterations=iterations,
        )
    )

    # ==========================================================================
    # Benchmark 8: Biomass Total
    # ==========================================================================
    console.print("  [cyan]8. Biomass estimation (total)[/cyan]")
    results.append(
        benchmark_evalidator(
            evalid=evalid,
            snum=ESTIMATE_ATTRIBUTES["biomass_ag"],  # snum=16
            rselected=ROW_GROUPINGS["none"],
            name="biomass_total",
            iterations=iterations,
        )
    )

    # ==========================================================================
    # Benchmark 9: Mortality Total
    # ==========================================================================
    console.print("  [cyan]9. Mortality estimation (total)[/cyan]")
    results.append(
        benchmark_evalidator(
            evalid=evalid,
            snum=ESTIMATE_ATTRIBUTES["mortality_tpa"],  # snum=111
            rselected=ROW_GROUPINGS["none"],
            name="mortality_total",
            iterations=iterations,
        )
    )

    # ==========================================================================
    # Benchmark 10: Mortality by Species
    # ==========================================================================
    console.print("  [cyan]10. Mortality by species[/cyan]")
    results.append(
        benchmark_evalidator(
            evalid=evalid,
            snum=ESTIMATE_ATTRIBUTES["mortality_tpa"],
            rselected=ROW_GROUPINGS["species"],
            name="mortality_by_species",
            iterations=iterations,
        )
    )

    # ==========================================================================
    # Benchmark 11: Complex Query - Volume by Species (with ownership as column)
    # Note: EVALIDator has different multi-dimension support
    # ==========================================================================
    console.print("  [cyan]11. Complex: Volume by species + ownership[/cyan]")
    params = build_evalidator_request(
        evalid=evalid,
        snum=ESTIMATE_ATTRIBUTES["volume_net_cf"],
        rselected=ROW_GROUPINGS["species"],
        cselected=ROW_GROUPINGS["ownership"],
    )
    # Custom timing for complex query
    times = []
    cold_start = None
    error = None

    gc.collect()
    elapsed, result, err = call_evalidator_api(params)
    if err:
        results.append(
            TimingResult(
                name="volume_complex",
                tool="evalidator",
                iterations=0,
                times_ms=[],
                error=err,
            )
        )
    else:
        cold_start = elapsed
        for _ in range(iterations):
            gc.collect()
            elapsed, result, err = call_evalidator_api(params)
            if err:
                error = err
                break
            times.append(elapsed)

        results.append(
            TimingResult(
                name="volume_complex",
                tool="evalidator",
                iterations=iterations,
                times_ms=times,
                cold_start_ms=cold_start,
                error=error,
            )
        )

    # ==========================================================================
    # Benchmark 12: Timberland Only
    # Note: EVALIDator uses different filtering approach (strFilter parameter)
    # ==========================================================================
    console.print("  [cyan]12. Volume (timberland - via filter)[/cyan]")
    # Timberland filter in EVALIDator
    params = {
        "wc": evalid,
        "snum": ESTIMATE_ATTRIBUTES["volume_net_cf"],
        "rselected": ROW_GROUPINGS["none"],
        "cselected": ROW_GROUPINGS["none"],
        "strFilter": "RESERVCD = 0 AND SITECLCD in (1,2,3,4,5,6)",  # Timberland filter
        "outputFormat": "NJSON",
    }

    times = []
    cold_start = None
    error = None

    gc.collect()
    elapsed, result, err = call_evalidator_api(params)
    if err:
        results.append(
            TimingResult(
                name="volume_timberland",
                tool="evalidator",
                iterations=0,
                times_ms=[],
                error=err,
            )
        )
    else:
        cold_start = elapsed
        for _ in range(iterations):
            gc.collect()
            elapsed, result, err = call_evalidator_api(params)
            if err:
                error = err
                break
            times.append(elapsed)

        results.append(
            TimingResult(
                name="volume_timberland",
                tool="evalidator",
                iterations=iterations,
                times_ms=times,
                cold_start_ms=cold_start,
                error=error,
            )
        )

    return results


def check_evalidator_available() -> bool:
    """Check if EVALIDator API is reachable."""
    try:
        response = requests.get(
            "https://apps.fs.usda.gov/fiadb-api/fullreport/parameters/wc",
            timeout=10,
        )
        return response.status_code == 200
    except Exception:
        return False


if __name__ == "__main__":
    import sys

    from rich.table import Table

    state = sys.argv[1] if len(sys.argv) > 1 else "RI"

    if not check_evalidator_available():
        console.print("[red]EVALIDator API not reachable.[/red]")
        console.print("Please check your internet connection.")
        sys.exit(1)

    results = run_evalidator_benchmarks(state, iterations=5)

    # Print results
    table = Table(title="EVALIDator API Benchmark Results")
    table.add_column("Benchmark", style="cyan")
    table.add_column("Mean (ms)", justify="right")
    table.add_column("Std Dev", justify="right")
    table.add_column("Min", justify="right")
    table.add_column("Cold Start", justify="right")

    for r in results:
        if r.error:
            table.add_row(r.name, f"[red]{r.error[:30]}[/red]", "-", "-", "-")
        else:
            table.add_row(
                r.name,
                f"{r.mean_ms:.2f}",
                f"{r.std_ms:.2f}",
                f"{r.min_ms:.2f}",
                f"{r.cold_start_ms:.2f}" if r.cold_start_ms else "-",
            )

    console.print(table)

    # Note about network latency
    console.print("\n[yellow]Note: EVALIDator times include network latency.[/yellow]")
    console.print("For fair comparison, consider that pyFIA runs locally.")
