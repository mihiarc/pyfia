# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This repository contains both R and Python implementations for analyzing USDA Forest Inventory and Analysis (FIA) data:
- **rFIA**: R package providing user-friendly access to FIA Database
- **pyFIA**: Python implementation using Polars for high performance

## Development Commands

### R Package (rFIA)

```bash
# Build and check
R CMD build .
R CMD check rFIA_*.tar.gz

# Run tests
devtools::test()              # In R console
Rscript -e "testthat::test_dir('tests/testthat')"

# Install locally
devtools::install()           # In R console
```

### Python Package (pyFIA)

```bash
# Setup development environment (using uv)
cd pyFIA
uv venv
source .venv/bin/activate     # Unix/Mac
uv pip install -e .[dev]

# Run tests
uv run pytest
uv run pytest --cov=pyfia    # With coverage

# Code quality
uv run black pyfia/ tests/   # Format code
uv run flake8 pyfia/         # Lint
uv run mypy pyfia/           # Type check
```

### Benchmarking

```bash
# Compare R and Python implementations
./run_benchmarks.sh

# Individual benchmarks
Rscript benchmark_rfia_sqlite.R
cd pyFIA && uv run python benchmark_pyfia_optimized_v2.py
```

## Architecture Overview

### R Package Structure
- `R/`: Core estimation functions (tpa.R, biomass.R, carbon.R, etc.)
- `src/`: C++ implementations for performance-critical operations
- `man/`: Documentation generated from roxygen2 comments
- `tests/testthat/`: Test suite using testthat framework
- `vignettes/`: User guides and tutorials

### Python Package Structure  
- `pyfia/core.py`: Main FIA class and common functionality
- `pyfia/estimation_common.py`: Shared estimation procedures
- `pyfia/optimized_sqlite_reader.py`: High-performance SQLite data loading
- `pyfia/*_equations.py`: Biomass and volume equation implementations
- Individual estimation modules matching R functions

### Key Design Patterns

1. **Design-based estimation**: Both implementations follow Bechtold & Patterson (2005) procedures
2. **Lazy evaluation**: Python uses Polars lazy frames for memory efficiency
3. **Spatial support**: Integration with sf (R) and geopandas (Python)
4. **Consistent API**: Python mirrors R function names and parameters where possible

## Development Guidelines

- **rFIA estimates are the ground truth**: All pyFIA estimates must match rFIA results
- Use polars where possible in Python implementation
- Maintain consistency between R and Python APIs
- All estimation functions should support:
  - Temporal queries (by year)
  - Spatial queries (by polygon)
  - Domain specifications (treeDomain, areaDomain)
  - Grouping variables (grpBy, bySpecies, bySizeClass)
- Include comprehensive tests for new functionality
- Document all public functions with examples
- When debugging pyFIA, always compare against rFIA output using the comparison scripts

## FIA Data Organization - Critical for Correct Implementation

### Evaluation System (EVALID)
FIA data is organized by **evaluations** - statistically valid groups of plots for population estimates. Each evaluation has:
- **EVALID**: 6-digit code (2-state, 2-year, 2-type) e.g., `372301` = NC 2023 Volume
- **Evaluation Types**: VOL (volume), GRM (growth/removal/mortality), CHNG (change), DWM, INVASIVE
- **Temporal Span**: START_INVYR to END_INVYR (typically 5-8 years)

### Key Tables for Evaluation-Based Analysis
- **POP_EVAL**: Core evaluation definitions with EVALID, temporal boundaries
- **POP_EVAL_TYP**: Links evaluations to types (VOL, GRM, etc.)
- **POP_PLOT_STRATUM_ASSGN**: Links plots to evaluations via strata
- **POP_STRATUM**: Stratification and expansion factors

### Critical Implementation Note
**DO NOT filter by year alone!** Must filter by EVALID to ensure:
- Statistically valid plot groupings
- Proper expansion factors
- Correct temporal boundaries
- Matches rFIA methodology

Example: NC 2023 evaluation (`372301`) includes plots from 2016-2023, using ~3,500 plots.
Filtering by year 2023 alone would incorrectly use all plots from all evaluations.

## Lessons Learned from EVALID Implementation

### 1. FIA Database Complexity
- FIA data cannot be naively filtered by year or state - must use the evaluation system
- The relationship between plots and evaluations is many-to-many through POP_PLOT_STRATUM_ASSGN
- A single plot can belong to multiple evaluations (e.g., VOL and GRM)
- Mixing data from different evaluations produces invalid estimates

### 2. Implementation Best Practices
- **Always use clipFIA()** before running estimators to ensure proper EVALID filtering
- When implementing new estimators, use `prepare_data()` method to get EVALID-filtered data
- Test against rFIA with identical EVALID to ensure matching results
- Use `findEVALID()` / `find_evalid()` to discover available evaluations

### 3. Common Pitfalls to Avoid
- **Never filter PLOT table by INVYR alone** - this mixes incompatible evaluations
- **Don't assume most recent year = most recent evaluation** - evaluations span multiple years
- **Don't mix evaluation types** - VOL, GRM, and other types have different plot sets
- **Remember plot counts differ by evaluation** - not all plots are measured every year

### 4. Debugging Tips
- If pyFIA results differ from rFIA, first check if same EVALID is being used
- Plot count differences usually indicate EVALID filtering issues
- Large estimate discrepancies (>10%) often mean mixing evaluation data
- Use `mostRecent=TRUE` to automatically select the latest complete evaluation

### 5. Data Type Considerations
- CN (Control Number) fields are VARCHAR(34) in SQLite, not integers
- These large identifiers must be handled as strings in both R and Python
- When joining tables, ensure CN fields have consistent types (string)
- Polars schema inference may need overrides for CN columns