# PyFIA

**High-performance Python library for USDA Forest Inventory and Analysis (FIA) data.**

[![PyPI version](https://badge.fury.io/py/pyfia.svg)](https://badge.fury.io/py/pyfia)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

PyFIA provides statistically valid estimation methods following [Bechtold & Patterson (2005)](https://www.fs.usda.gov/research/treesearch/20121) methodology, enabling researchers and analysts to work with FIA data efficiently.

## Features

- **Statistical Rigor**: Design-based estimation with proper variance calculations
- **High Performance**: Built on [Polars](https://pola.rs/) and [DuckDB](https://duckdb.org/) for fast data processing
- **EVALIDator Compatible**: Results validated against official USFS estimates
- **Simple API**: Intuitive functions like `volume()`, `mortality()`, and `tpa()`
- **Lazy Evaluation**: Memory-efficient processing for large datasets

## Quick Example

```python
import pyfia

# Connect to FIA database
db = pyfia.FIA("georgia.duckdb")

# Filter to Georgia's most recent inventory
db.clip_by_state("GA")

# Calculate total volume by species
result = pyfia.volume(db, grp_by="SPCD")
print(result)
```

## Installation

```bash
pip install pyfia
```

## Documentation Overview

<div class="grid cards" markdown>

-   :material-rocket-launch: **[Getting Started](getting-started.md)**

    Installation, setup, and your first analysis

-   :material-book-open-variant: **[User Guide](guides/index.md)**

    Domain filtering, variance estimation, and workflows

-   :material-api: **[API Reference](api/index.md)**

    Complete function and class documentation

-   :material-file-code: **[Examples](examples.md)**

    Real-world analysis examples

</div>

## Estimation Functions

| Function | Description |
|----------|-------------|
| [`area()`](api/area.md) | Estimate forest area by land type and categories |
| [`volume()`](api/volume.md) | Estimate standing tree volume |
| [`tpa()`](api/tpa.md) | Calculate trees per acre and basal area |
| [`biomass()`](api/biomass.md) | Estimate tree biomass and carbon |
| [`mortality()`](api/mortality.md) | Calculate annual mortality rates |
| [`growth()`](api/growth.md) | Estimate annual growth |
| [`removals()`](api/removals.md) | Estimate timber removals |

## Why PyFIA?

FIA data analysis traditionally requires complex SQL queries and careful attention to statistical methodology. PyFIA handles:

1. **Proper stratified estimation** with expansion factors
2. **Variance calculation** following ratio-of-means methodology
3. **EVALID filtering** for consistent evaluation groups
4. **Domain-specific filtering** (forest land, timberland, growing stock)

## License

PyFIA is released under the MIT License.
