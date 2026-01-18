"""
rFIA benchmark tests.

Benchmarks rFIA (R package) for comparison against pyFIA.
Uses subprocess to run R scripts with timing.
"""

import json
import subprocess
import tempfile
import time
from pathlib import Path
from typing import List, Optional

from rich.console import Console

from benchmarks.comparison.timing import TimingResult

console = Console()

# R script template for rFIA benchmarks
RFIA_BENCHMARK_SCRIPT = '''
# rFIA Benchmark Script
# This script runs equivalent benchmarks to pyFIA for fair comparison

suppressPackageStartupMessages({{
  library(rFIA)
  library(jsonlite)
}})

# Configuration
state <- "{state}"
iterations <- {iterations}
data_dir <- "{data_dir}"

# Results storage
results <- list()

# Helper function for timing
benchmark_function <- function(name, func, iterations) {{
  times <- numeric(iterations)

  # Cold start (first run)
  gc()
  cold_start <- system.time({{ result <- func() }})["elapsed"] * 1000

  # Warm-up (already done with cold start)

  # Timed iterations
  for (i in 1:iterations) {{
    gc()
    elapsed <- system.time({{ result <- func() }})["elapsed"] * 1000
    times[i] <- elapsed
  }}

  list(
    name = name,
    tool = "rfia",
    iterations = iterations,
    times_ms = times,
    cold_start_ms = cold_start,
    error = NULL
  )
}}

# Load FIA data
cat("Loading FIA data for", state, "...\\n")
load_start <- Sys.time()

# Download or load cached data
if (dir.exists(data_dir) && length(list.files(data_dir, pattern = "\\\\.csv$")) > 0) {{
  # Load from existing CSVs
  fia_data <- readFIA(data_dir, common = TRUE, nCores = 1)
}} else {{
  # Download fresh
  fia_data <- getFIA(states = state, dir = data_dir, common = TRUE, nCores = 1)
}}

load_time <- as.numeric(difftime(Sys.time(), load_start, units = "secs")) * 1000
cat("Data loaded in", round(load_time), "ms\\n")

# Clip to most recent inventory
fia_data <- clipFIA(fia_data)

# =============================================================================
# Benchmark 1: Area Total
# =============================================================================
cat("Running benchmark: area_total\\n")
tryCatch({{
  results$area_total <- benchmark_function("area_total", function() {{
    area(fia_data, landType = "forest")
  }}, iterations)
}}, error = function(e) {{
  results$area_total <- list(name = "area_total", tool = "rfia", error = as.character(e))
}})

# =============================================================================
# Benchmark 2: Area by Ownership
# =============================================================================
cat("Running benchmark: area_by_ownership\\n")
tryCatch({{
  results$area_by_ownership <- benchmark_function("area_by_ownership", function() {{
    area(fia_data, grpBy = OWNGRPCD, landType = "forest")
  }}, iterations)
}}, error = function(e) {{
  results$area_by_ownership <- list(name = "area_by_ownership", tool = "rfia", error = as.character(e))
}})

# =============================================================================
# Benchmark 3: Volume Total
# =============================================================================
cat("Running benchmark: volume_total\\n")
tryCatch({{
  results$volume_total <- benchmark_function("volume_total", function() {{
    volume(fia_data, landType = "forest", treeType = "live", volType = "NET")
  }}, iterations)
}}, error = function(e) {{
  results$volume_total <- list(name = "volume_total", tool = "rfia", error = as.character(e))
}})

# =============================================================================
# Benchmark 4: Volume by Species
# =============================================================================
cat("Running benchmark: volume_by_species\\n")
tryCatch({{
  results$volume_by_species <- benchmark_function("volume_by_species", function() {{
    volume(fia_data, bySpecies = TRUE, landType = "forest", treeType = "live")
  }}, iterations)
}}, error = function(e) {{
  results$volume_by_species <- list(name = "volume_by_species", tool = "rfia", error = as.character(e))
}})

# =============================================================================
# Benchmark 5: Volume by Size Class
# =============================================================================
cat("Running benchmark: volume_by_sizeclass\\n")
tryCatch({{
  results$volume_by_sizeclass <- benchmark_function("volume_by_sizeclass", function() {{
    volume(fia_data, bySizeClass = TRUE, landType = "forest", treeType = "live")
  }}, iterations)
}}, error = function(e) {{
  results$volume_by_sizeclass <- list(name = "volume_by_sizeclass", tool = "rfia", error = as.character(e))
}})

# =============================================================================
# Benchmark 6: TPA Total
# =============================================================================
cat("Running benchmark: tpa_total\\n")
tryCatch({{
  results$tpa_total <- benchmark_function("tpa_total", function() {{
    tpa(fia_data, landType = "forest", treeType = "live")
  }}, iterations)
}}, error = function(e) {{
  results$tpa_total <- list(name = "tpa_total", tool = "rfia", error = as.character(e))
}})

# =============================================================================
# Benchmark 7: TPA by Species
# =============================================================================
cat("Running benchmark: tpa_by_species\\n")
tryCatch({{
  results$tpa_by_species <- benchmark_function("tpa_by_species", function() {{
    tpa(fia_data, bySpecies = TRUE, landType = "forest", treeType = "live")
  }}, iterations)
}}, error = function(e) {{
  results$tpa_by_species <- list(name = "tpa_by_species", tool = "rfia", error = as.character(e))
}})

# =============================================================================
# Benchmark 8: Biomass Total
# =============================================================================
cat("Running benchmark: biomass_total\\n")
tryCatch({{
  results$biomass_total <- benchmark_function("biomass_total", function() {{
    biomass(fia_data, landType = "forest", treeType = "live")
  }}, iterations)
}}, error = function(e) {{
  results$biomass_total <- list(name = "biomass_total", tool = "rfia", error = as.character(e))
}})

# =============================================================================
# Benchmark 9: Mortality Total (using growMort)
# =============================================================================
cat("Running benchmark: mortality_total\\n")
tryCatch({{
  results$mortality_total <- benchmark_function("mortality_total", function() {{
    growMort(fia_data, landType = "forest")
  }}, iterations)
}}, error = function(e) {{
  results$mortality_total <- list(name = "mortality_total", tool = "rfia", error = as.character(e))
}})

# =============================================================================
# Benchmark 10: Mortality by Species
# =============================================================================
cat("Running benchmark: mortality_by_species\\n")
tryCatch({{
  results$mortality_by_species <- benchmark_function("mortality_by_species", function() {{
    growMort(fia_data, bySpecies = TRUE, landType = "forest")
  }}, iterations)
}}, error = function(e) {{
  results$mortality_by_species <- list(name = "mortality_by_species", tool = "rfia", error = as.character(e))
}})

# =============================================================================
# Benchmark 11: Complex Query - Volume with multiple groupings
# =============================================================================
cat("Running benchmark: volume_complex\\n")
tryCatch({{
  results$volume_complex <- benchmark_function("volume_complex", function() {{
    volume(fia_data, bySpecies = TRUE, bySizeClass = TRUE, grpBy = OWNGRPCD, landType = "forest")
  }}, iterations)
}}, error = function(e) {{
  results$volume_complex <- list(name = "volume_complex", tool = "rfia", error = as.character(e))
}})

# =============================================================================
# Benchmark 12: Timberland Only
# =============================================================================
cat("Running benchmark: volume_timberland\\n")
tryCatch({{
  results$volume_timberland <- benchmark_function("volume_timberland", function() {{
    volume(fia_data, landType = "timber", treeType = "live")
  }}, iterations)
}}, error = function(e) {{
  results$volume_timberland <- list(name = "volume_timberland", tool = "rfia", error = as.character(e))
}})

# Output results as JSON
cat("\\n=== RESULTS_JSON_START ===\\n")
cat(toJSON(results, auto_unbox = TRUE, pretty = FALSE))
cat("\\n=== RESULTS_JSON_END ===\\n")
'''


def run_rfia_benchmarks(
    state: str,
    data_dir: Optional[Path] = None,
    iterations: int = 10,
    timeout: int = 600,
) -> List[TimingResult]:
    """
    Run rFIA benchmarks via R subprocess.

    Parameters
    ----------
    state : str
        State abbreviation (e.g., 'RI', 'DE')
    data_dir : Path, optional
        Directory containing FIA CSV files. If None, rFIA will download.
    iterations : int
        Number of benchmark iterations
    timeout : int
        Timeout in seconds for R script execution

    Returns
    -------
    List[TimingResult]
        List of benchmark results
    """
    results = []

    console.print(f"[bold]Running rFIA benchmarks for {state}[/bold]")
    console.print(f"  Iterations: {iterations}")
    console.print(f"  Timeout: {timeout}s")

    # Check if R and rFIA are available
    try:
        r_check = subprocess.run(
            ["Rscript", "-e", "library(rFIA); cat('OK')"],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if "OK" not in r_check.stdout:
            console.print("[red]rFIA package not installed in R[/red]")
            console.print("Install with: install.packages('rFIA')")
            return results
    except FileNotFoundError:
        console.print("[red]R/Rscript not found. Please install R.[/red]")
        return results
    except subprocess.TimeoutExpired:
        console.print("[red]R check timed out[/red]")
        return results

    # Create data directory if needed
    if data_dir is None:
        data_dir = Path(tempfile.mkdtemp()) / state.lower()
    data_dir = Path(data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)

    # Generate R script
    script_content = RFIA_BENCHMARK_SCRIPT.format(
        state=state, iterations=iterations, data_dir=str(data_dir)
    )

    # Write script to temp file
    with tempfile.NamedTemporaryFile(mode="w", suffix=".R", delete=False) as f:
        f.write(script_content)
        script_path = f.name

    try:
        console.print(f"  Running R script...")
        start_time = time.perf_counter()

        # Run R script
        process = subprocess.run(
            ["Rscript", script_path],
            capture_output=True,
            text=True,
            timeout=timeout,
        )

        total_time = time.perf_counter() - start_time
        console.print(f"  Total R execution time: {total_time:.1f}s")

        if process.returncode != 0:
            console.print(f"[red]R script failed:[/red]")
            console.print(process.stderr)
            return results

        # Parse JSON results from output
        output = process.stdout
        if "=== RESULTS_JSON_START ===" in output:
            json_start = output.index("=== RESULTS_JSON_START ===") + len(
                "=== RESULTS_JSON_START ==="
            )
            json_end = output.index("=== RESULTS_JSON_END ===")
            json_str = output[json_start:json_end].strip()

            try:
                r_results = json.loads(json_str)

                for name, data in r_results.items():
                    if data.get("error"):
                        results.append(
                            TimingResult(
                                name=name,
                                tool="rfia",
                                iterations=0,
                                times_ms=[],
                                error=data["error"],
                            )
                        )
                    else:
                        results.append(
                            TimingResult(
                                name=data["name"],
                                tool="rfia",
                                iterations=data["iterations"],
                                times_ms=data["times_ms"],
                                cold_start_ms=data.get("cold_start_ms"),
                            )
                        )

            except json.JSONDecodeError as e:
                console.print(f"[red]Failed to parse R results: {e}[/red]")

    except subprocess.TimeoutExpired:
        console.print(f"[red]R script timed out after {timeout}s[/red]")
    except Exception as e:
        console.print(f"[red]Error running R script: {e}[/red]")
    finally:
        # Clean up temp script
        Path(script_path).unlink(missing_ok=True)

    return results


def check_rfia_available() -> bool:
    """Check if rFIA is available."""
    try:
        result = subprocess.run(
            ["Rscript", "-e", "library(rFIA); cat('OK')"],
            capture_output=True,
            text=True,
            timeout=30,
        )
        return "OK" in result.stdout
    except Exception:
        return False


if __name__ == "__main__":
    import sys

    from rich.table import Table

    state = sys.argv[1] if len(sys.argv) > 1 else "RI"

    if not check_rfia_available():
        console.print("[red]rFIA not available. Please install R and rFIA package.[/red]")
        console.print("  1. Install R: https://cran.r-project.org/")
        console.print("  2. Install rFIA: install.packages('rFIA')")
        sys.exit(1)

    results = run_rfia_benchmarks(state, iterations=5)

    # Print results
    table = Table(title="rFIA Benchmark Results")
    table.add_column("Benchmark", style="cyan")
    table.add_column("Mean (ms)", justify="right")
    table.add_column("Std Dev", justify="right")
    table.add_column("Min", justify="right")
    table.add_column("Cold Start", justify="right")

    for r in results:
        if r.error:
            table.add_row(r.name, "[red]ERROR[/red]", "-", "-", "-")
        else:
            table.add_row(
                r.name,
                f"{r.mean_ms:.2f}",
                f"{r.std_ms:.2f}",
                f"{r.min_ms:.2f}",
                f"{r.cold_start_ms:.2f}" if r.cold_start_ms else "-",
            )

    console.print(table)
