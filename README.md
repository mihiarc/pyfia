<div align="center">
  <a href="https://fiatools.org"><img src="https://fiatools.org/logos/pyfia_logo.png" alt="pyFIA" width="400"></a>

  <p><strong>The Python API for forest inventory data</strong></p>

  <p>
    <a href="https://fiatools.org"><img src="https://img.shields.io/badge/FIAtools-Ecosystem-2E7D32" alt="FIAtools Ecosystem"></a>
    <a href="https://pypi.org/project/pyfia/"><img src="https://img.shields.io/pypi/v/pyfia?color=006D6D&label=PyPI" alt="PyPI"></a>
    <a href="https://pypi.org/project/pyfia/"><img src="https://img.shields.io/pypi/dm/pyfia?color=006D6D&label=Downloads" alt="PyPI Downloads"></a>
    <a href="https://mihiarc.github.io/pyfia/"><img src="https://img.shields.io/badge/docs-GitHub%20Pages-006D6D" alt="Documentation"></a>
    <a href="https://opensource.org/licenses/MIT"><img src="https://img.shields.io/badge/License-MIT-006D6D" alt="License: MIT"></a>
    <a href="https://www.python.org/downloads/"><img src="https://img.shields.io/badge/python-3.11+-006D6D" alt="Python 3.11+"></a>
  </p>

  <p>
    <strong>Part of the <a href="https://fiatools.org">FIAtools Python Ecosystem</a></strong><br>
    <a href="https://fiatools.org/tools/pyfia/">pyFIA</a> ·
    <a href="https://fiatools.org/tools/gridfia/">gridFIA</a> ·
    <a href="https://fiatools.org/tools/pyfvs/">pyFVS</a> ·
    <a href="https://fiatools.org/tools/askfia/">askFIA</a>
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
| `mortality()` | Annual mortality rates | `mortality(db)` |
| `growth()` | Net growth estimation | `growth(db)` |

## Statistical Methods

pyFIA implements design-based estimation following [Bechtold & Patterson (2005)](https://www.srs.fs.usda.gov/pubs/gtr/gtr_srs080/gtr_srs080.pdf):

- **Post-stratified estimation** with proper variance calculation
- **Ratio-of-means estimators** for per-acre values
- **EVALID-based filtering** for statistically valid estimates
- **Temporal methods**: TI, annual, SMA, LMA, EMA

## Installation Options

```bash
# Basic
pip install pyfia

# With spatial support
pip install pyfia[spatial]

# Development
git clone https://github.com/mihiarc/pyfia.git
cd pyfia && pip install -e .[dev]
```

## Documentation

📖 **Full docs:** [mihiarc.github.io/pyfia](https://mihiarc.github.io/pyfia/)

## Integration with FIAtools

pyFIA works seamlessly with other tools in the ecosystem:

```python
# Use pyFIA data with gridFIA for spatial analysis
from pyfia import FIA
from gridfia import GridFIA

with FIA("database.duckdb") as db:
    species_list = db.get_species_codes()

api = GridFIA()
api.download_species(state="NC", species_codes=species_list)
```

## The FIAtools Ecosystem

PyFIA is part of the [FIAtools Python ecosystem](https://fiatools.org) - a unified suite of open-source tools for forest inventory analysis:

| Tool | Purpose | Key Features |
|------|---------|--------------|
| [**pyFIA**](https://fiatools.org) | Survey & plot data | DuckDB backend, 10-100x faster than EVALIDator |
| [**gridFIA**](https://fiatools.org) | Spatial raster analysis | 327 species at 30m resolution, Zarr storage |
| [**pyFVS**](https://fiatools.org) | Growth simulation | Chapman-Richards curves, yield projections |
| [**askFIA**](https://fiatools.org) | AI interface | Natural language queries for forest data |

**[Explore the full ecosystem at fiatools.org](https://fiatools.org)**

## Citation

```bibtex
@software{pyfia2025,
  title = {pyFIA: A Python Library for Forest Inventory Analysis},
  author = {Mihiar, Christopher},
  year = {2025},
  url = {https://fiatools.org}
}
```

---

<div align="center">
  <a href="https://fiatools.org"><strong>fiatools.org</strong></a> · Python Ecosystem for Forest Inventory Analysis<br>
  <sub>Built by <a href="https://github.com/mihiarc">Chris Mihiar</a> · USDA Forest Service Southern Research Station</sub>
</div>
