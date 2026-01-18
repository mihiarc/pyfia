# Benchmark Methodology

This document describes the methodology used for comparing pyFIA performance against rFIA and EVALIDator API.

## Overview

We benchmark three tools for FIA data analysis:

1. **pyFIA** - Python library using DuckDB + Polars (local computation)
2. **rFIA** - R package (local computation with R)
3. **EVALIDator API** - USDA Forest Service web API (remote computation)

## Benchmark Design

### Equivalent Operations

Each benchmark measures the time to complete equivalent operations across all three tools:

| Benchmark | pyFIA | rFIA | EVALIDator |
|-----------|-------|------|------------|
| Area (total) | `area(db)` | `area(db, landType="forest")` | `snum="Area of forest land"` |
| Area by ownership | `area(db, by_ownership=True)` | `area(db, grpBy=OWNGRPCD)` | `rselected="Ownership group"` |
| Volume (total) | `volume(db)` | `volume(db, landType="forest")` | `snum="Net volume of live trees"` |
| Volume by species | `volume(db, by_species=True)` | `volume(db, bySpecies=TRUE)` | `rselected="Species"` |
| Volume by size class | `volume(db, by_size_class=True)` | `volume(db, bySizeClass=TRUE)` | `rselected="Diameter class"` |
| TPA (total) | `tpa(db)` | `tpa(db, landType="forest")` | `snum="Number of live trees"` |
| TPA by species | `tpa(db, by_species=True)` | `tpa(db, bySpecies=TRUE)` | `rselected="Species"` |
| Biomass (total) | `biomass(db)` | `biomass(db, landType="forest")` | `snum="Aboveground dry weight"` |
| Mortality (total) | `mortality(db)` | `growMort(db, landType="forest")` | `snum="Annual mortality"` |
| Mortality by species | `mortality(db, by_species=True)` | `growMort(db, bySpecies=TRUE)` | `rselected="Species"` |
| Complex (multi-group) | `volume(db, by_species=True, by_size_class=True, by_ownership=True)` | `volume(db, bySpecies=TRUE, bySizeClass=TRUE, grpBy=OWNGRPCD)` | `rselected="Species", cselected="Ownership group"` |
| Timberland filter | `volume(db, land_type="timber")` | `volume(db, landType="timber")` | `strFilter="RESERVCD=0..."` |

### Timing Methodology

For each benchmark:

1. **Cold Start**: First execution after data loading (includes any JIT compilation, caching)
2. **Warm-up**: 2 additional iterations (not timed)
3. **Timed Iterations**: 10 iterations with garbage collection between runs
4. **Statistics**: Mean, standard deviation, min, max, median

```python
# Timing approach
for iteration in range(iterations):
    gc.collect()  # Clean memory state
    start = time.perf_counter()
    result = function()
    elapsed = time.perf_counter() - start
    times.append(elapsed)
```

### Considerations

#### pyFIA
- Uses DuckDB for data storage and Polars for computation
- Data is loaded once at start, queries operate on in-memory database
- Benefits from columnar storage and lazy evaluation

#### rFIA
- Uses R's data frames (typically with dplyr backend)
- Data loaded via `readFIA()` or `getFIA()`
- May use parallel processing with `nCores` parameter (set to 1 for fair comparison)

#### EVALIDator API
- Remote computation via HTTPS API
- Times include network latency (RTT)
- Server-side processing time is opaque
- Fewer iterations due to rate limiting concerns

## Fair Comparison Notes

### What We Compare

1. **End-to-end query time**: From function call to result
2. **Equivalent statistical operations**: Same filters, groupings, metrics
3. **Same data source**: Same state's FIA data for all tools

### What We Don't Compare

1. **Data loading time**: Each tool has different data formats
   - pyFIA: DuckDB (pre-converted)
   - rFIA: CSV files (loaded into R environment)
   - EVALIDator: Pre-loaded on server

2. **Network latency for EVALIDator**: This is a real-world consideration but not a fair algorithm comparison

3. **Parallel processing**: All local tools run single-threaded

### Limitations

1. **EVALIDator limitations**:
   - Network latency dominates timing
   - Server load varies
   - Not all queries are directly equivalent

2. **rFIA limitations**:
   - R subprocess overhead
   - Memory management differs from Python

3. **Benchmark variance**:
   - System load affects results
   - Multiple runs recommended

## Running Benchmarks

### Prerequisites

```bash
# pyFIA (required)
pip install pyfia

# rFIA (optional, requires R)
# Install R from https://cran.r-project.org/
# Then: install.packages("rFIA")

# EVALIDator (optional, requires internet)
# No installation needed
```

### Basic Usage

```bash
# Run all benchmarks for Rhode Island
python -m benchmarks.comparison.run_comparison --state RI --download

# Run only pyFIA benchmarks
python -m benchmarks.comparison.run_comparison --state RI --tools pyfia

# Export results
python -m benchmarks.comparison.run_comparison --state RI --export results.csv
```

### Interpreting Results

Results include:

- **Mean (ms)**: Average execution time across iterations
- **Std Dev**: Standard deviation (lower = more consistent)
- **Min/Max**: Best and worst case times
- **Cold Start**: First execution time (may be higher due to initialization)
- **Speedup**: Ratio of alternative tool time to pyFIA time

Example output:

```
Speedup Summary (pyFIA vs alternatives):
  pyFIA vs rfia: 15.3x faster (mean)
  pyFIA vs evalidator: 47.2x faster (mean)
```

## Reproducibility

For reproducible results:

1. Use the same FIA database version
2. Run on a quiet system (minimal background load)
3. Run multiple times and report averages
4. Document hardware specifications
5. Document software versions

### Recommended Citation

When citing these benchmarks in publications:

```
Performance benchmarks conducted using pyFIA v{version} benchmark suite
comparing against rFIA v1.1.2 and EVALIDator API (accessed {date}).
Benchmarks run on {hardware} using {state} FIA data with {n} iterations.
```

## References

- Stanke, H., et al. (2020). rFIA: An R package for estimation of forest attributes with the US Forest Inventory and Analysis database. Environmental Modelling & Software, 127, 104664.
- USDA Forest Service. EVALIDator API Documentation. https://apps.fs.usda.gov/fiadb-api/
- Bechtold, W.A. & Patterson, P.L. (2005). The Enhanced Forest Inventory and Analysis Program - National Sampling Design and Estimation Procedures. Gen. Tech. Rep. SRS-80.
