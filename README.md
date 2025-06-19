# pyFIA

A Python implementation of the R rFIA package for Forest Inventory Analysis.

## Overview

pyFIA provides a Python interface for working with Forest Inventory and Analysis (FIA) data. It's designed to be a performant, user-friendly alternative to the R rFIA package, leveraging modern Python data science tools.

## Features

- ✅ **Fast data loading** with optimized SQLite reader
- ✅ **EVALID-based filtering** for statistically valid estimates
- ✅ **Core estimators validated against rFIA**:
  - Trees per acre (TPA)
  - Biomass and carbon
  - Volume (VOLCFNET, VOLBFNET)
  - Forest area
- ✅ **Temporal estimation methods**: TI, annual, SMA, LMA, EMA
- ✅ **Polars-based** for efficient data processing

## Installation

```bash
pip install pyfia
```

## Quick Start

```python
from pyfia import read_fia_sqlite_optimized, biomass, tpa, volume, area

# Load FIA data
db = read_fia_sqlite_optimized("path/to/FIA_database.db")

# Get trees per acre
tpa_results = tpa(db, method='TI')

# Get biomass estimates
biomass_results = biomass(db, method='TI')

# Get forest area
area_results = area(db, method='TI')
```

## Validation Results

pyFIA has been validated against rFIA ground truth values:

| Estimator | Accuracy vs rFIA | Status |
|-----------|------------------|---------|
| Forest Area | 0.1% difference | ✅ EXCELLENT |
| Biomass | 3.7% difference | ✅ EXCELLENT |
| Volume | 7.6% difference | ✅ GOOD |
| Trees per Acre | 12.8% difference | ✅ ACCEPTABLE |

See [VALIDATION_RESULTS_SUMMARY.md](VALIDATION_RESULTS_SUMMARY.md) for detailed validation results.

## Requirements

- Python 3.8+
- polars>=0.20.0
- numpy>=1.20.0
- pandas (optional, for compatibility)
- geopandas (optional, for spatial features)

## Development

```bash
# Clone the repository
git clone https://github.com/mihiarc/pyfia.git
cd pyfia

# Install in development mode
pip install -e .

# Run tests
pytest
```

## License

MIT License

## Acknowledgments

This is a Python port of the excellent [rFIA package](https://github.com/doserjef/rFIA) by Hunter Stanke and Andrew Finley. The algorithms and methodology follow the original R implementation.

## Citation

If you use pyFIA in your research, please cite both this package and the original rFIA package:

```bibtex
@article{rfia2020,
  title={rFIA: An R package for estimation of forest attributes with the US Forest Inventory and Analysis database},
  author={Stanke, Hunter and Finley, Andrew O and Weed, Aaron S and Walters, Brian F and Domke, Grant M},
  journal={Environmental Modelling & Software},
  volume={127},
  pages={104664},
  year={2020},
  publisher={Elsevier}
}
```