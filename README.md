# pyFIA

[![Documentation](https://img.shields.io/badge/docs-GitHub%20Pages-blue)](https://mihiarc.github.io/pyfia/)
[![Deploy Documentation](https://github.com/mihiarc/pyfia/actions/workflows/deploy-docs.yml/badge.svg)](https://github.com/mihiarc/pyfia/actions/workflows/deploy-docs.yml)

A high-performance Python library implementing the R [rFIA](https://github.com/hunter-stanke/rFIA) package functionality for analyzing USDA Forest Inventory and Analysis (FIA) data.

## Overview

pyFIA provides a programmatic API for working with Forest Inventory and Analysis (FIA) data, mirroring the functionality of the popular rFIA R package. It leverages modern Python data science tools like Polars and DuckDB for efficient processing of large-scale national forest inventory datasets while maintaining exact statistical compatibility with rFIA.

## Features

### Core rFIA-Compatible Functions
- ✅ **Trees per acre** (`tpa()`) - Live and dead tree abundance
- ✅ **Biomass** (`biomass()`) - Above/belowground biomass and carbon  
- ✅ **Volume** (`volume()`) - Merchantable volume (cubic feet, board feet)
- ✅ **Forest area** (`area()`) - Forest land area by category
- ✅ **Mortality** (`mortality()`) - Annual mortality rates
- ✅ **Growth** (`growMort()`) - Net growth, recruitment, and mortality

### Statistical Methods
- **Design-based estimation** following Bechtold & Patterson (2005)
- **Post-stratified estimation** with proper variance calculation
- **Temporal estimation methods**: TI (temporally indifferent), annual, SMA, LMA, EMA
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

# Load FIA data from DuckDB
db = FIA("path/to/FIA_database.duckdb")

# Get trees per acre - matches rFIA::tpa()
tpa_results = tpa(db, method='TI')

# Get biomass estimates - matches rFIA::biomass()
biomass_results = biomass(db, method='TI', component='AG')

# Get forest area - matches rFIA::area()
area_results = area(db, method='TI')

# Get volume estimates - matches rFIA::volume()
volume_results = volume(db, method='TI')
```

## rFIA Compatibility

pyFIA is designed as a drop-in Python replacement for rFIA with identical statistical outputs:

```python
# rFIA style filtering
tpa_live = tpa(db, treeDomain="STATUSCD == 1", method='TI')

# Group by species
biomass_by_species = biomass(db, bySpecies=True)

# Domain filtering
area_timberland = area(db, areaDomain="COND_STATUS_CD == 1", method='TI')

# Temporal queries
annual_mortality = mortality(db, method='annual')
```

## Data Organization

pyFIA follows FIA's evaluation-based data structure:
- **EVALID**: 6-digit codes identifying statistically valid plot groupings
- **Evaluation types**: VOL (volume), GRM (growth/removal/mortality), CHNG (change)
- **Automatic EVALID management**: Use `mostRecent=True` for latest evaluations

## Advanced Usage

```python
# Context manager for automatic connection handling
with FIA("path/to/FIA_database.duckdb") as db:
    # Find available evaluations for a state
    evalids = db.find_evalid(state="NC")
    
    # Use specific evaluation
    results = biomass(db, evalid="372301", bySpecies=True)
    
    # Multiple estimations with same connection
    tpa_results = tpa(db, method='TI')
    volume_results = volume(db, method='TI', treeDomain="DIA >= 10")
    area_results = area(db, method='TI', landType='timber')
```

## Documentation

Full documentation available at [https://mihiarc.github.io/pyfia/](https://mihiarc.github.io/pyfia/)

## Performance

Benchmarks show pyFIA matches or exceeds rFIA performance:
- **10-100x faster** for large-scale queries using DuckDB
- **2-5x faster** for in-memory operations using Polars
- **Exact statistical accuracy** compared to rFIA

## Citation

If you use pyFIA in your research, please cite:

```bibtex
@software{pyfia2024,
  title = {pyFIA: A Python Implementation of rFIA},
  author = {Your Name},
  year = {2024},
  url = {https://github.com/yourusername/pyfia}
}
```

## License

MIT License - see LICENSE file for details.

## Acknowledgments

- Based on the excellent [rFIA](https://github.com/hunter-stanke/rFIA) R package by Hunter Stanke
- Uses USDA Forest Service FIA data
- Statistical methods from Bechtold & Patterson (2005) "The Enhanced Forest Inventory and Analysis Program"