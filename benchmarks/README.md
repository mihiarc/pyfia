# pyFIA Performance Benchmarks

This directory contains reproducible benchmarks comparing pyFIA against rFIA and EVALIDator API.

## Quick Start

```bash
# Install pyFIA with dev dependencies
uv pip install -e ".[dev]"

# Run full benchmark suite (downloads data automatically)
uv run python -m benchmarks.comparison.run_comparison \
    --state RI \
    --download \
    --tools pyfia,rfia,evalidator \
    --iterations 10 \
    --export benchmarks/results.csv
```

## Results Summary

Benchmarks conducted on Rhode Island FIA data (22,707 tree records, 130 plots).

### pyFIA vs rFIA

| Operation | pyFIA (ms) | rFIA (ms) | Speedup |
|-----------|------------|-----------|---------|
| Area (total) | 3.3 ± 0.3 | 89.2 ± 2.7 | **27.3×** |
| Area (by ownership) | 4.4 ± 0.3 | 106.4 ± 17.7 | **24.3×** |
| Volume (total) | 5.3 ± 0.6 | 89.6 ± 1.7 | **17.0×** |
| Volume (by species) | 8.2 ± 0.6 | 144.0 ± 19.7 | **17.6×** |
| Volume (by diameter class) | 4.8 ± 0.6 | 104.8 ± 2.3 | **22.0×** |
| TPA (total) | 5.2 ± 0.4 | 88.6 ± 1.8 | **16.9×** |
| TPA (by species) | 62.1 ± 27.5 | 131.6 ± 6.4 | **2.1×** |
| Biomass (total) | 6.0 ± 2.0 | 115.4 ± 1.5 | **19.3×** |
| Mortality (total) | 5.0 ± 0.5 | 246.4 ± 20.1 | **48.9×** |
| Mortality (by species) | 19.2 ± 1.8 | 332.6 ± 51.8 | **17.3×** |
| Volume (complex grouping) | 7.8 ± 0.7 | 175.6 ± 0.9 | **22.6×** |
| Volume (timberland only) | 5.6 ± 0.5 | 92.2 ± 4.3 | **16.4×** |

**Mean speedup: 21.0× (median: 18.5×, range: 2.1×–48.9×)**

### pyFIA vs EVALIDator API

| Operation | pyFIA (ms) | EVALIDator (ms) | Speedup |
|-----------|------------|-----------------|---------|
| Area (total) | 3.3 ± 0.3 | 906.2 ± 225.7 | **277.0×** |
| Area (by ownership) | 4.4 ± 0.3 | 660.4 ± 28.7 | **151.0×** |
| Volume (total) | 5.3 ± 0.6 | 662.0 ± 89.3 | **125.5×** |
| Volume (by species) | 8.2 ± 0.6 | 916.2 ± 194.9 | **112.2×** |
| Volume (by diameter class) | 4.8 ± 0.6 | 624.5 ± 62.3 | **131.1×** |
| TPA (total) | 5.2 ± 0.4 | 894.9 ± 125.8 | **170.6×** |
| Biomass (total) | 6.0 ± 2.0 | 630.1 ± 31.2 | **105.5×** |
| Mortality (total) | 5.0 ± 0.5 | 792.4 ± 251.4 | **157.3×** |
| Volume (complex grouping) | 7.8 ± 0.7 | 1025.2 ± 32.3 | **132.1×** |
| Volume (timberland only) | 5.6 ± 0.5 | 903.8 ± 149.4 | **160.7×** |

**Mean speedup: 152.3× (median: 141.6×, range: 105.5×–277.0×)**

*Note: EVALIDator times include network latency (~500-1000ms RTT).*

## Reproducing the Benchmarks

### Prerequisites

```bash
# Python environment
uv venv && source .venv/bin/activate
uv pip install -e ".[dev]"

# For rFIA benchmarks (optional)
# Install R: https://cran.r-project.org/
Rscript -e "install.packages('rFIA')"
```

### Running Benchmarks

```bash
# pyFIA only (fastest)
uv run python -m benchmarks.comparison.run_comparison \
    --state RI --download --tools pyfia

# pyFIA + EVALIDator (requires internet)
uv run python -m benchmarks.comparison.run_comparison \
    --state RI --download --tools pyfia,evalidator

# Full comparison (requires R + rFIA)
uv run python -m benchmarks.comparison.run_comparison \
    --state RI --download --tools pyfia,rfia,evalidator

# Larger state (North Carolina)
uv run python -m benchmarks.comparison.run_comparison \
    --state NC --download --iterations 10 --export results_nc.csv
```

### Command Line Options

| Option | Description |
|--------|-------------|
| `--state` | State abbreviation (RI, DE, NC, GA, etc.) |
| `--db` | Path to existing DuckDB database |
| `--download` | Download FIA data if not present |
| `--tools` | Comma-separated: pyfia,rfia,evalidator |
| `--iterations` | Number of timed iterations (default: 10) |
| `--export` | Export results to CSV or JSON |

### Supported States

| State | FIPS | Size | Notes |
|-------|------|------|-------|
| RI | 44 | Small | ~23K trees, fast benchmarks |
| DE | 10 | Small | ~15K trees |
| CT | 09 | Small | ~30K trees |
| NC | 37 | Medium | ~250K trees |
| GA | 13 | Large | ~400K trees |

## Benchmark Methodology

### Operations Tested

1. **Area estimation**: Total forest area, area by ownership group
2. **Volume estimation**: Net cubic foot volume (total, by species, by diameter class)
3. **Trees per acre (TPA)**: Tree density (total, by species)
4. **Biomass estimation**: Aboveground dry weight
5. **Mortality estimation**: Annual mortality using GRM methodology
6. **Complex queries**: Multi-dimensional grouping (species × size × ownership)
7. **Filtered queries**: Timberland-only estimates

### Timing Protocol

1. **Cold start**: First execution after data loading
2. **Warm-up**: 2 iterations (not timed)
3. **Timed iterations**: N iterations with GC between runs
4. **Statistics**: Mean, std dev, min, max, median

### Fair Comparison Notes

- **rFIA**: Single-threaded (`nCores=1`) for fair comparison
- **EVALIDator**: Times include network latency (real-world conditions)
- **Data source**: All tools use equivalent FIA DataMart data
- **Statistical methods**: All implement Bechtold & Patterson (2005)

## File Structure

```
benchmarks/
├── README.md                 # This file
├── METHODOLOGY.md            # Detailed methodology documentation
├── MANUSCRIPT_BENCHMARKS.md  # Draft text for publications
├── results_full_ri.csv       # Rhode Island benchmark results
├── benchmark_two_stage_aggregation.py  # Internal aggregation benchmarks
└── comparison/
    ├── __init__.py
    ├── timing.py             # Timing utilities
    ├── bench_pyfia.py        # pyFIA benchmarks
    ├── bench_rfia.py         # rFIA benchmarks (R subprocess)
    ├── bench_evalidator.py   # EVALIDator API benchmarks
    └── run_comparison.py     # Main comparison script
```

## Hardware Configuration

Benchmarks in this README were run on:

- **CPU**: Apple M-series
- **RAM**: 16GB
- **OS**: macOS Darwin 25.2.0
- **Python**: 3.12
- **pyFIA**: 1.1.0
- **rFIA**: 1.1.2
- **DuckDB**: 0.9+
- **Polars**: 1.0+

## Citation

If you use these benchmarks in your research, please cite:

```bibtex
@software{pyfia,
  title = {pyFIA: High-Performance Python Library for Forest Inventory Analysis},
  author = {Mihiar, Chris},
  year = {2025},
  url = {https://github.com/mihiarc/pyfia}
}
```

## References

- Bechtold, W.A. & Patterson, P.L. (2005). The Enhanced Forest Inventory and Analysis Program. Gen. Tech. Rep. SRS-80.
- Stanke, H. et al. (2020). rFIA: An R package for estimation of forest attributes. Environmental Modelling & Software, 127, 104664.
- USDA Forest Service. EVALIDator API. https://apps.fs.usda.gov/fiadb-api/
