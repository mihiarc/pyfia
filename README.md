<div align="center">
  <img src="https://fiatools.org/logos/pyfia-logo.svg" alt="pyFIA" width="140">

  <h1>pyFIA</h1>

  <p><strong>The Python API for forest inventory data</strong></p>

  <p>
    <a href="https://mihiarc.github.io/pyfia/"><img src="https://img.shields.io/badge/docs-GitHub%20Pages-2D5016" alt="Documentation"></a>
    <a href="https://github.com/mihiarc/pyfia/actions/workflows/deploy-docs.yml"><img src="https://github.com/mihiarc/pyfia/actions/workflows/deploy-docs.yml/badge.svg" alt="Deploy Documentation"></a>
    <a href="https://opensource.org/licenses/MIT"><img src="https://img.shields.io/badge/License-MIT-2D5016" alt="License: MIT"></a>
    <a href="https://www.python.org/downloads/"><img src="https://img.shields.io/badge/python-3.9+-2D5016" alt="Python 3.9+"></a>
  </p>

  <p>
    <sub>Part of the <a href="https://fiatools.org"><strong>FIAtools</strong></a> ecosystem:
    <a href="https://github.com/mihiarc/pyfia">pyFIA</a> ·
    <a href="https://github.com/mihiarc/gridfia">gridFIA</a> ·
    <a href="https://github.com/mihiarc/pyfvs">pyFVS</a> ·
    <a href="https://github.com/mihiarc/askfia">askFIA</a></sub>
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

## Citation

```bibtex
@software{pyfia2024,
  title = {pyFIA: A Python Library for Forest Inventory Analysis},
  author = {Mihiar, Christopher},
  year = {2024},
  url = {https://github.com/mihiarc/pyfia}
}
```

## License

MIT License — see [LICENSE](LICENSE) for details.

---

<div align="center">
  <sub>Built with 🌲 by <a href="https://github.com/mihiarc">Chris Mihiar</a> · USDA Forest Service Southern Research Station</sub>
</div>
