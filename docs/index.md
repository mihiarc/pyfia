---
title: PyFIA - Python API for Forest Inventory Analysis
description: High-performance Python library for USDA FIA data analysis. Built on DuckDB and Polars for 10-100x faster analysis than EVALIDator with statistically valid estimates.
---

# PyFIA

**High-performance Python library for USDA Forest Inventory and Analysis (FIA) data.**

!!! tip "Part of the FIAtools Ecosystem"
    PyFIA is one of four integrated Python tools for forest inventory analysis. Visit **[fiatools.org](https://fiatools.org)** to explore the complete ecosystem and see how the tools work together.

[![PyPI version](https://badge.fury.io/py/pyfia.svg)](https://badge.fury.io/py/pyfia)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

PyFIA provides statistically valid estimation methods following [Bechtold & Patterson (2005)](https://www.fs.usda.gov/research/treesearch/20121) methodology, enabling researchers and analysts to work with FIA data efficiently.

## Features

- **Direct Data Download**: Download FIA data directly from the DataMart with `download("GA")`
- **Statistical Rigor**: Design-based estimation with proper variance calculations
- **High Performance**: Built on [Polars](https://pola.rs/) and [DuckDB](https://duckdb.org/) for fast data processing
- **EVALIDator Compatible**: Results validated against official USFS estimates
- **Simple API**: Intuitive functions like `volume()`, `mortality()`, and `tpa()`
- **Lazy Evaluation**: Memory-efficient processing for large datasets

## Quick Example

```python
from pyfia import download, FIA, area

# Download Georgia FIA data directly
db_path = download("GA")

# Connect and analyze
with FIA(db_path) as db:
    db.clip_most_recent()
    result = area(db, land_type="forest")
    print(result)
```

## Installation

```bash
pip install pyfia
```

## Getting Started

### Download FIA Data

```python
from pyfia import download

# Download a single state
db_path = download("GA")

# Download multiple states (merged into single database)
db_path = download(["GA", "FL", "SC"])

# Download to specific directory
db_path = download("GA", dir="./data")
```

### Run Analysis

```python
from pyfia import FIA, volume

with FIA(db_path) as db:
    db.clip_by_state("GA")
    result = volume(db, grp_by="SPCD")
    print(result)
```

## Documentation Overview

<div class="grid cards" markdown>

-   :material-download: **[Downloading Data](guides/downloading.md)**

    Download FIA data directly from the DataMart

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
| [`area_change()`](api/area_change.md) | Estimate annual changes in forest area |
| [`volume()`](api/volume.md) | Estimate standing tree volume |
| [`tpa()`](api/tpa.md) | Calculate trees per acre and basal area |
| [`biomass()`](api/biomass.md) | Estimate tree biomass and carbon |
| [`mortality()`](api/mortality.md) | Calculate annual mortality rates |
| [`growth()`](api/growth.md) | Estimate annual growth |
| [`removals()`](api/removals.md) | Estimate timber removals |

## Why PyFIA?

FIA data analysis traditionally requires complex SQL queries and careful attention to statistical methodology. PyFIA handles:

1. **Direct data access** with automatic downloads from FIA DataMart
2. **Proper stratified estimation** with expansion factors
3. **Variance calculation** following ratio-of-means methodology
4. **EVALID filtering** for consistent evaluation groups
5. **Domain-specific filtering** (forest land, timberland, growing stock)

## Comparison with rFIA

PyFIA brings the ease of rFIA's `getFIA()` to Python:

| Feature | rFIA (R) | PyFIA (Python) |
|---------|----------|----------------|
| Download data | `getFIA(states='GA')` | `download("GA")` |
| Multiple states | `getFIA(states=c('GA','FL'))` | `download(["GA", "FL"])` |
| Estimate area | `area(fiaData)` | `area(db)` |
| Estimate volume | `biomass(fiaData)` | `volume(db)` |

## The FIAtools Ecosystem

| Tool | Purpose | Key Features |
|------|---------|--------------|
| [**pyFIA**](https://fiatools.org) | Survey & plot data | DuckDB backend, 10-100x faster than EVALIDator |
| [**gridFIA**](https://fiatools.org) | Spatial raster analysis | 327 species at 30m resolution, Zarr storage |
| [**pyFVS**](https://fiatools.org) | Growth simulation | Chapman-Richards curves, yield projections |
| [**askFIA**](https://fiatools.org) | AI interface | Natural language queries for forest data |

[:material-arrow-right: Explore the full ecosystem at fiatools.org](https://fiatools.org){ .md-button .md-button--primary }

## License

PyFIA is released under the MIT License.

---

<div align="center">
<strong><a href="https://fiatools.org">fiatools.org</a></strong> · Python Ecosystem for Forest Inventory Analysis
</div>
