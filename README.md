<div align="center">
  <h1>pyFIA</h1>

  <p><strong>The Python API for forest inventory data</strong></p>

  <p>
    <a href="https://pypi.org/project/pyfia/"><img src="https://img.shields.io/pypi/v/pyfia?color=006D6D&label=PyPI" alt="PyPI"></a>
    <a href="https://pypi.org/project/pyfia/"><img src="https://img.shields.io/pypi/dm/pyfia?color=006D6D&label=Downloads" alt="PyPI Downloads"></a>
    <a href="https://mihiarc.github.io/pyfia/"><img src="https://img.shields.io/badge/docs-GitHub%20Pages-006D6D" alt="Documentation"></a>
    <a href="https://opensource.org/licenses/MIT"><img src="https://img.shields.io/badge/License-MIT-006D6D" alt="License: MIT"></a>
    <a href="https://www.python.org/downloads/"><img src="https://img.shields.io/badge/python-3.11+-006D6D" alt="Python 3.11+"></a>
  </p>
</div>

---

A high-performance Python library for analyzing USDA Forest Inventory and Analysis (FIA) data. Built on DuckDB and Polars for speed, with statistical methods that match EVALIDator exactly.

## Why pyFIA?

| Feature | pyFIA | EVALIDator |
|---------|-------|------------|
| Speed | **10-100x faster** | Baseline |
| Interface | Python API | Web UI |
| Reproducibility | Code-based | Manual |
| Custom analysis | Unlimited | Limited options |
| Statistical validity | ✓ Exact match | ✓ Reference |

## Quick Start

```bash
pip install pyfia
```

```python
from pyfia import FIA, biomass, tpa, volume, area

with FIA("path/to/FIA_database.duckdb") as db:
    db.clip_by_state(37)  # North Carolina
    db.clip_most_recent(eval_type="EXPVOL")

    # Core estimates
    trees = tpa(db, tree_domain="STATUSCD == 1")
    carbon = biomass(db, by_species=True)
    timber = volume(db, land_type="timber")
    forest = area(db, land_type="forest")
```

## Core Functions

| Function | Description | Example |
|----------|-------------|---------|
| `tpa()` | Trees per acre | `tpa(db, tree_domain="DIA >= 5.0")` |
| `biomass()` | Above/belowground biomass | `biomass(db, by_species=True)` |
| `volume()` | Merchantable volume (ft³) | `volume(db, land_type="timber")` |
| `area()` | Forest land area | `area(db, grp_by="FORTYPCD")` |
| `site_index()` | Site productivity index | `site_index(db, grp_by="COUNTYCD")` |
| `mortality()` | Annual mortality rates | `mortality(db)` |
| `growth()` | Net growth estimation | `growth(db)` |

## Statistical Methods

pyFIA implements design-based estimation following [Bechtold & Patterson (2005)](https://www.srs.fs.usda.gov/pubs/gtr/gtr_srs080/gtr_srs080.pdf):

- **Post-stratified estimation** with proper variance calculation
- **Ratio-of-means estimators** for per-acre values
- **EVALID-based filtering** for statistically valid estimates
- **Temporal methods**: TI, annual, SMA, LMA, EMA

## Installation

```bash
# Recommended: use uv for fast, reliable installs
uv pip install pyfia

# Or with pip
pip install pyfia

# With spatial support (polygon clipping, spatial joins)
uv pip install pyfia[spatial]
```

> **Tip:** Always use a virtual environment (`uv venv` or `python -m venv .venv`).

For development setup, see the [Getting Started guide](https://mihiarc.github.io/pyfia/getting-started/).

## Documentation

Full documentation: [mihiarc.github.io/pyfia](https://mihiarc.github.io/pyfia/)

## Citation

```bibtex
@software{pyfia2025,
  title = {pyFIA: A Python Library for Forest Inventory Applications},
  author = {Mihiar, Christopher},
  year = {2025},
  url = {https://github.com/mihiarc/pyfia}
}
```

---

## Affiliation

Developed in collaboration with USDA Forest Service Research & Development. pyFIA provides programmatic access to Forest Inventory and Analysis (FIA) data but is not part of the official FIA Program.

---

<div align="center">
  <sub>Built by <a href="https://github.com/mihiarc">Chris Mihiar</a></sub>
</div>
