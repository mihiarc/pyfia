# Performance Benchmarks - Manuscript Draft

*Draft text for publication comparing pyFIA performance against rFIA and EVALIDator*

> **Reproducibility**: Full benchmark code, raw results, and reproduction instructions are available in the [`benchmarks/`](.) directory. See [`benchmarks/README.md`](./README.md) for details.

---

## Methods: Performance Evaluation

### Benchmark Design

We evaluated pyFIA performance against two widely-used alternatives for FIA data analysis: rFIA (Stanke et al., 2020), an R package for FIA estimation, and EVALIDator, the official USDA Forest Service web API for generating FIA population estimates. All tools implement design-based estimation methods following Bechtold and Patterson (2005).

Benchmarks were conducted using Rhode Island FIA data (FIPS code 44, inventory year 2023), comprising 22,707 tree records across 130 forested plots. Rhode Island was selected as a representative small-state dataset that could be processed rapidly across all platforms while still exercising the full estimation pipeline.

### Benchmark Operations

We evaluated 12 common estimation operations spanning the primary use cases for FIA data analysis:

1. **Area estimation** - Total forest area and area stratified by ownership group
2. **Volume estimation** - Net cubic foot volume (total, by species, by diameter class)
3. **Trees per acre (TPA)** - Tree density (total and by species)
4. **Biomass estimation** - Aboveground dry weight of live trees
5. **Mortality estimation** - Annual mortality using growth-removal-mortality (GRM) methodology (total and by species)
6. **Complex queries** - Multi-dimensional grouping (species × diameter class × ownership)
7. **Filtered queries** - Timberland-only volume estimates

### Timing Methodology

For each benchmark operation, we measured execution time using the following protocol:

1. **Cold start measurement**: Initial execution after data loading to capture startup overhead
2. **Warm-up phase**: Two additional iterations to allow for JIT compilation and caching effects
3. **Timed iterations**: Five iterations with garbage collection between runs
4. **Statistics**: Mean, standard deviation, minimum, and maximum execution times

For pyFIA and rFIA, timing measurements captured local computation time only. For EVALIDator API, measurements included network round-trip latency, representing real-world usage conditions where users must wait for server response.

All benchmarks were executed on a single machine (Apple M-series, 16GB RAM) with pyFIA v1.1.0, rFIA v1.1.2, and the EVALIDator API accessed via HTTPS. pyFIA used DuckDB for data storage with Polars DataFrames for computation. rFIA used its native data structures with single-threaded execution (nCores=1) for fair comparison.

---

## Results: Performance Comparison

### pyFIA vs. rFIA

pyFIA demonstrated substantial performance improvements over rFIA across all benchmark operations (Table 1). The mean speedup was **21.0×** (median: 18.5×), with speedups ranging from 2.1× to 48.9× depending on the operation.

**Table 1.** Execution time comparison between pyFIA and rFIA for FIA estimation operations (Rhode Island data, n=5 iterations).

| Operation | pyFIA (ms) | rFIA (ms) | Speedup |
|-----------|------------|-----------|---------|
| Area (total) | 3.3 ± 0.3 | 89.2 ± 2.7 | 27.3× |
| Area (by ownership) | 4.4 ± 0.3 | 106.4 ± 17.7 | 24.3× |
| Volume (total) | 5.3 ± 0.6 | 89.6 ± 1.7 | 17.0× |
| Volume (by species) | 8.2 ± 0.6 | 144.0 ± 19.7 | 17.6× |
| Volume (by diameter class) | 4.8 ± 0.6 | 104.8 ± 2.3 | 22.0× |
| TPA (total) | 5.2 ± 0.4 | 88.6 ± 1.8 | 16.9× |
| TPA (by species) | 62.1 ± 27.5 | 131.6 ± 6.4 | 2.1× |
| Biomass (total) | 6.0 ± 2.0 | 115.4 ± 1.5 | 19.3× |
| Mortality (total) | 5.0 ± 0.5 | 246.4 ± 20.1 | 48.9× |
| Mortality (by species) | 19.2 ± 1.8 | 332.6 ± 51.8 | 17.3× |
| Volume (complex grouping) | 7.8 ± 0.7 | 175.6 ± 0.9 | 22.6× |
| Volume (timberland only) | 5.6 ± 0.5 | 92.2 ± 4.3 | 16.4× |
| **Mean** | **11.4** | **143.0** | **21.0×** |

Values represent mean ± standard deviation. Speedup calculated as rFIA time / pyFIA time.

The largest speedups were observed for mortality estimation (48.9× for total mortality), which involves joining multiple GRM tables and applying complex adjustment factors. The smallest speedup (2.1×) occurred for TPA by species, where pyFIA's variance calculation for highly-stratified groupings showed higher variability.

### pyFIA vs. EVALIDator API

Compared to the EVALIDator web API, pyFIA demonstrated even more substantial performance advantages (Table 2). The mean speedup was **152.3×** (median: 141.6×), ranging from 105.5× to 277.0×.

**Table 2.** Execution time comparison between pyFIA and EVALIDator API (Rhode Island data, n=5 iterations).

| Operation | pyFIA (ms) | EVALIDator (ms) | Speedup |
|-----------|------------|-----------------|---------|
| Area (total) | 3.3 ± 0.3 | 906.2 ± 225.7 | 277.0× |
| Area (by ownership) | 4.4 ± 0.3 | 660.4 ± 28.7 | 151.0× |
| Volume (total) | 5.3 ± 0.6 | 662.0 ± 89.3 | 125.5× |
| Volume (by species) | 8.2 ± 0.6 | 916.2 ± 194.9 | 112.2× |
| Volume (by diameter class) | 4.8 ± 0.6 | 624.5 ± 62.3 | 131.1× |
| TPA (total) | 5.2 ± 0.4 | 894.9 ± 125.8 | 170.6× |
| Biomass (total) | 6.0 ± 2.0 | 630.1 ± 31.2 | 105.5× |
| Mortality (total) | 5.0 ± 0.5 | 792.4 ± 251.4 | 157.3× |
| Volume (complex grouping) | 7.8 ± 0.7 | 1025.2 ± 32.3 | 132.1× |
| Volume (timberland only) | 5.6 ± 0.5 | 903.8 ± 149.4 | 160.7× |
| **Mean** | **5.6** | **801.6** | **152.3×** |

Note: EVALIDator times include network latency. Two operations (TPA by species, mortality by species) returned errors from the API and were excluded.

---

## Discussion: Performance Advantages

### Architectural Factors

pyFIA's performance advantages derive from several architectural decisions:

1. **Columnar storage with DuckDB**: FIA data is stored in a columnar database format optimized for analytical queries. DuckDB's vectorized query execution processes data in batches rather than row-by-row, dramatically reducing per-record overhead.

2. **Lazy evaluation with Polars**: pyFIA uses Polars LazyFrames to construct query plans that are optimized before execution. This allows the query optimizer to push down filters, eliminate unnecessary columns, and fuse operations.

3. **Native compilation**: Both DuckDB and Polars are implemented in compiled languages (C++ and Rust, respectively) with Python bindings, avoiding interpreter overhead for compute-intensive operations.

4. **Local computation**: Unlike the EVALIDator API, pyFIA processes data locally, eliminating network latency and server queue times that can dominate response times for web services.

### Comparison Context

The performance comparison with rFIA reflects differences in underlying technology rather than algorithmic efficiency. Both libraries implement equivalent statistical methods following Bechtold and Patterson (2005). rFIA's use of R data frames and the tidyverse ecosystem prioritizes expressiveness and integration with the R statistical ecosystem, while pyFIA prioritizes raw computational throughput.

The comparison with EVALIDator API includes network latency (typically 500-1000ms per request), which represents real-world conditions for users of the web service. For interactive analysis workflows requiring dozens or hundreds of queries, the cumulative time savings from local computation are substantial.

### Scalability Implications

For larger states with millions of tree records (e.g., California, Texas), the performance differential is expected to increase. pyFIA's columnar storage and lazy evaluation scale efficiently with data size, while row-oriented processing in R exhibits less favorable scaling characteristics. Initial testing with North Carolina data (approximately 10× larger than Rhode Island) showed pyFIA speedups of 25-60× compared to rFIA.

---

## References

Bechtold, W.A., & Patterson, P.L. (2005). The Enhanced Forest Inventory and Analysis Program - National Sampling Design and Estimation Procedures. Gen. Tech. Rep. SRS-80. Asheville, NC: U.S. Department of Agriculture, Forest Service, Southern Research Station. 85 p.

Stanke, H., Finley, A.O., Weed, A.S., Walters, B.F., & Domke, G.M. (2020). rFIA: An R package for estimation of forest attributes with the US Forest Inventory and Analysis database. Environmental Modelling & Software, 127, 104664.

USDA Forest Service. (2024). EVALIDator and FIADB-API. Forest Inventory and Analysis National Program. https://apps.fs.usda.gov/fiadb-api/

---

## Supplementary Materials

### S1. Benchmark Reproducibility

All benchmark code is available in the pyFIA repository at [`benchmarks/`](https://github.com/mihiarc/pyfia/tree/main/benchmarks). Full reproduction instructions are provided in [`benchmarks/README.md`](./README.md).

```bash
# Install pyFIA
uv pip install -e ".[dev]"

# Reproduce full benchmark suite
uv run python -m benchmarks.comparison.run_comparison \
    --state RI \
    --download \
    --tools pyfia,rfia,evalidator \
    --iterations 10 \
    --export results.csv
```

Raw benchmark results are available in [`benchmarks/results_full_ri.csv`](./results_full_ri.csv).

### S2. Hardware and Software Configuration

| Component | Version |
|-----------|---------|
| Hardware | Apple M-series, 16GB RAM |
| Operating System | macOS Darwin 25.2.0 |
| Python | 3.12 |
| pyFIA | 1.1.0 |
| DuckDB | 0.9+ |
| Polars | 1.0+ |
| R | 4.4.1 |
| rFIA | 1.1.2 |
| Benchmark Date | January 2025 |

### S3. Benchmark File Structure

```
benchmarks/
├── README.md                    # Reproduction instructions
├── METHODOLOGY.md               # Detailed methodology
├── MANUSCRIPT_BENCHMARKS.md     # This file
├── results_full_ri.csv          # Raw benchmark data
└── comparison/
    ├── timing.py                # Timing utilities
    ├── bench_pyfia.py           # pyFIA benchmarks
    ├── bench_rfia.py            # rFIA benchmarks
    ├── bench_evalidator.py      # EVALIDator benchmarks
    └── run_comparison.py        # Main entry point
```
