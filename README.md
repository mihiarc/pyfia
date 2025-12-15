# pyFIA

[![Documentation](https://img.shields.io/badge/docs-GitHub%20Pages-blue)](https://mihiarc.github.io/pyfia/)
[![Deploy Documentation](https://github.com/mihiarc/pyfia/actions/workflows/deploy-docs.yml/badge.svg)](https://github.com/mihiarc/pyfia/actions/workflows/deploy-docs.yml)

A high-performance Python library for analyzing USDA Forest Inventory and Analysis (FIA) data using modern data science tools.

## Overview

pyFIA provides a programmatic API for working with Forest Inventory and Analysis (FIA) data. It leverages modern Python data science tools like Polars and DuckDB for efficient processing of large-scale national forest inventory datasets with statistically valid estimation methods.

## Features

### Core Estimation Functions
- ✅ **Trees per acre** (`tpa()`) - Live and dead tree abundance
- ✅ **Biomass** (`biomass()`) - Above/belowground biomass and carbon
- ✅ **Volume** (`volume()`) - Merchantable volume (cubic feet)
- ✅ **Forest area** (`area()`) - Forest land area by category
- ✅ **Mortality** (`mortality()`) - Annual mortality rates
- ✅ **Growth** (`growth()`) - Net growth estimation

### Statistical Methods
- **Design-based estimation** following Bechtold & Patterson (2005)
- **Post-stratified estimation** with proper variance calculation
- **Temporally indifferent (TI) estimation** matching EVALIDator default
- **EVALID-based filtering** for statistically valid estimates
- **Ratio-of-means estimators** for per-acre values

### Performance Features
- **DuckDB backend** for efficient large-scale data processing
- **Polars DataFrames** for fast in-memory operations
- **Lazy evaluation** for memory-efficient workflows
- **Parallel processing** support

## Installation

```bash
# Basic installation
pip install pyfia

# With spatial analysis support  
pip install pyfia[spatial]

# For development
pip install -e .[dev]
```

## Quick Start

```python
from pyfia import FIA, biomass, tpa, volume, area

# Load FIA data and filter to a state
with FIA("path/to/FIA_database.duckdb") as db:
    # Filter to state (required before estimation)
    db.clip_by_state(37)  # North Carolina
    db.clip_most_recent(eval_type="EXPVOL")

    # Get trees per acre (live trees on forestland)
    tpa_results = tpa(db, tree_domain="STATUSCD == 1")

    # Get biomass estimates
    biomass_results = biomass(db, land_type="forest")

    # Get forest area
    area_results = area(db, land_type="forest")

    # Get volume estimates
    volume_results = volume(db, land_type="forest")
```

## Domain Filtering and Grouping

pyFIA supports flexible domain filtering and grouping:

```python
# Tree-level filtering (snake_case parameters)
tpa_live = tpa(db, tree_domain="STATUSCD == 1")

# Group by species
biomass_by_species = biomass(db, by_species=True)

# Area domain filtering
area_timberland = area(db, land_type="timber")

# Group by custom column
volume_by_owner = volume(db, grp_by="OWNGRPCD")
```

## Data Organization

pyFIA follows FIA's evaluation-based data structure:
- **EVALID**: 6-digit codes identifying statistically valid plot groupings
- **Evaluation types**: EXPALL (area), EXPVOL (volume), EXPMORT (mortality), EXPGROW (growth)
- **EVALID management**: Use `db.clip_most_recent(eval_type="EXPVOL")` for latest evaluations

## Advanced Usage

```python
# Context manager for automatic connection handling
with FIA("path/to/FIA_database.duckdb") as db:
    # Filter to state and most recent evaluation
    db.clip_by_state(37)  # North Carolina
    db.clip_most_recent(eval_type="EXPVOL")

    # Biomass by species
    results = biomass(db, by_species=True)

    # Multiple estimations with same connection
    tpa_results = tpa(db, tree_domain="STATUSCD == 1")
    volume_results = volume(db, tree_domain="DIA >= 10.0")
    area_results = area(db, land_type="timber")
```

## Documentation

Full documentation available at [https://mihiarc.github.io/pyfia/](https://mihiarc.github.io/pyfia/)

## Performance

pyFIA achieves excellent performance through modern database technologies:
- **10-100x faster** for large-scale queries using DuckDB columnar storage
- **2-5x faster** for in-memory operations using Polars DataFrames
- **Statistically valid** estimates following FIA methodology

## Citation

If you use pyFIA in your research, please cite:

```bibtex
@software{pyfia2024,
  title = {pyFIA: A Python Library for Forest Inventory Analysis},
  author = {Mihiar, Chris},
  year = {2024},
  url = {https://github.com/mihiarc/pyfia}
}
```

## License

MIT License - see LICENSE file for details.

## Acknowledgments

- Uses USDA Forest Service FIA data
- Statistical methods from Bechtold & Patterson (2005):
  - Bechtold, W.A.; Patterson, P.L., eds. 2005. *The Enhanced Forest Inventory and
    Analysis Program - National Sampling Design and Estimation Procedures*.
    Gen. Tech. Rep. SRS-80. Asheville, NC: U.S. Department of Agriculture,
    Forest Service, Southern Research Station. 85 p. https://doi.org/10.2737/SRS-GTR-80
  - Key equations: Chapter 4 (pp. 53-77) - see Eq. 4.1 (domain indicator), Eq. 4.2
    (adjustment factor), Eq. 4.8 (tree attributes), Section 4.2 (variance estimation)
- Inspired by various FIA analysis tools and methodologies in the forestry community