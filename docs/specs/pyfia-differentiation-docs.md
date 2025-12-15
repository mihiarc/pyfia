# pyFIA Documentation: Differentiation & Validation

## Why pyFIA?

pyFIA is a high-performance Python library for **statistically valid estimation** of forest attributes from USDA Forest Inventory and Analysis (FIA) data. It implements the design-based estimation methodology established by Bechtold & Patterson (2005) and validated against official USDA Forest Service EVALIDator estimates.

### The Problem pyFIA Solves

FIA data analysis traditionally requires:

1. **Complex SQL queries** across multiple relational tables
2. **Proper handling of expansion factors** and stratified sampling weights
3. **Variance estimation** following ratio-of-means methodology
4. **EVALID filtering** to ensure statistically valid population estimates

Existing tools either require proprietary software (EVALIDator web interface), work only in R (rFIA), or don't implement proper statistical methodology. pyFIA brings rigorous FIA estimation to the Python ecosystem.

---

## Statistical Rigor: What Sets pyFIA Apart

### Design-Based Estimation

pyFIA implements the **post-stratified estimation procedures** described in:

> Bechtold, W.A.; Patterson, P.L., eds. 2005. *The Enhanced Forest Inventory and Analysis Program—National Sampling Design and Estimation Procedures*. Gen. Tech. Rep. SRS-80. Asheville, NC: U.S. Department of Agriculture, Forest Service, Southern Research Station. 85 p.

This includes:

- **Stratified random sampling estimators** with proper expansion factors
- **Ratio-of-means estimators** for per-acre values (TPA, volume/acre, biomass/acre)
- **Variance estimation** accounting for the two-phase sampling design
- **Post-stratification** using forest type, stand size, and ownership groups

### EVALID-Based Filtering

FIA data is organized into **evaluation groups (EVALIDs)**—6-digit codes that define statistically valid combinations of plots, strata, and time periods. pyFIA properly filters data by EVALID and evaluation type:

- `EXPALL` — Area estimation
- `EXPVOL` — Volume and biomass estimation  
- `EXPMORT` — Mortality estimation
- `EXPGROW` — Growth estimation

```python
with FIA(db_path) as db:
    db.clip_by_state(37)  # North Carolina
    db.clip_most_recent(eval_type="EXPVOL")  # Most recent volume evaluation
    result = volume(db)
```

### Variance and Sampling Error

Every pyFIA estimate includes proper **sampling error** calculations:

| Output Column | Description |
|---------------|-------------|
| `ESTIMATE` | Point estimate (e.g., total volume in cubic feet) |
| `SE` | Standard error of the estimate |
| `SE_PCT` | Relative standard error as percentage |
| `N_PLOTS` | Number of plots contributing to estimate |

---

## Validation Against EVALIDator

### Automated Test Suite

pyFIA includes an automated validation suite that compares estimates against the official USDA Forest Service EVALIDator API. Tests enforce **exact matching** (threshold = 0.00) for:

- Forest area by land type
- Volume by species and diameter class
- Trees per acre (live/dead)
- Biomass and carbon estimates
- Mortality rates
- Net growth

### Example Validation

```
Test: NC Forest Area (EXPALL)
  EVALIDator: 18,587,234 acres
  pyFIA:      18,587,234 acres
  Difference: 0.00%
  Status:     ✓ PASS

Test: NC Softwood Volume (EXPVOL)  
  EVALIDator: 24,891,456,789 cubic feet
  pyFIA:      24,891,456,789 cubic feet
  Difference: 0.00%
  Status:     ✓ PASS
```

### Running Validation Tests

```bash
# Run full validation suite
pytest tests/validation/ -v

# Run against specific state
pytest tests/validation/ -v -k "north_carolina"
```

---

## Comparison with Other Tools

### pyFIA vs. EVALIDator (Web Interface)

| Feature | EVALIDator | pyFIA |
|---------|------------|-------|
| Statistical validity | ✅ Official | ✅ Validated against EVALIDator |
| Reproducibility | ❌ Manual web clicks | ✅ Scriptable, version-controlled |
| Custom analysis | ❌ Limited options | ✅ Full flexibility |
| Batch processing | ❌ One query at a time | ✅ Programmatic loops |
| Integration | ❌ Standalone | ✅ Python ecosystem |

### pyFIA vs. rFIA (R Package)

| Feature | rFIA | pyFIA |
|---------|------|-------|
| Language | R | Python |
| Statistical methods | ✅ Bechtold & Patterson | ✅ Bechtold & Patterson |
| Validation | ✅ Against EVALIDator | ✅ Against EVALIDator |
| Performance | dplyr (good) | DuckDB + Polars (faster) |
| Data format | CSV files in memory | DuckDB columnar storage |
| Peer-reviewed paper | ✅ Stanke et al. 2020 | 🔄 In preparation |

**rFIA citation:** Stanke, H., Finley, A.O., Weed, A.S., Walters, B.F., & Domke, G.M. (2020). rFIA: An R package for estimation of forest attributes with the US Forest Inventory and Analysis database. *Environmental Modelling & Software*, 127, 104664.

### pyFIA vs. Wei et al. PyFIA (2025)

A separate Python tool also named "PyFIA" was published in *Carbon Balance and Management* (Wei et al., 2025). The two packages serve **different purposes**:

| Feature | Wei et al. PyFIA | pyFIA (this package) |
|---------|------------------|----------------------|
| **Primary purpose** | Visualization & carbon bookkeeping | Statistical estimation |
| **Design-based estimation** | ❌ Not implemented | ✅ Full implementation |
| **Variance/sampling error** | ❌ Not calculated | ✅ Proper SE for all estimates |
| **EVALIDator validation** | ❌ None | ✅ Exact matching tests |
| **EVALID filtering** | ❌ Not implemented | ✅ Full support |
| **Expansion factors** | ❌ Unclear handling | ✅ Proper methodology |
| **Climate integration** | ✅ ClimateNA linkage | ❌ Not current focus |
| **Bookkeeping model** | ✅ Biomass dynamics | ❌ Not current focus |
| **Spatial visualization** | ✅ Mapping tools | ❌ Not current focus |
| **Installation** | Scripts (not packaged) | ✅ `pip install pyfia` |
| **Performance** | Standard pandas | ✅ DuckDB + Polars |

**When to use Wei et al. PyFIA:**
- Exploratory visualization of FIA plot data
- Climate-forest relationship analysis
- Carbon bookkeeping and flux modeling

**When to use pyFIA (this package):**
- Official statistics with proper sampling errors
- Reproducible research requiring validated estimates
- Integration with EVALIDator-based workflows
- High-performance processing of large datasets
- Any analysis that will be compared to official FIA estimates

---

## Estimation Functions

pyFIA provides validated estimation functions that match EVALIDator outputs:

| Function | Description | Evaluation Type |
|----------|-------------|-----------------|
| `area()` | Forest land area by category | EXPALL |
| `volume()` | Standing tree volume (cubic feet) | EXPVOL |
| `tpa()` | Trees per acre and basal area | EXPVOL |
| `biomass()` | Above/belowground biomass and carbon | EXPVOL |
| `mortality()` | Annual mortality rates | EXPMORT |
| `growth()` | Net annual growth | EXPGROW |
| `removals()` | Timber removals | EXPREMV |

### Temporal Estimation Approach

pyFIA uses the **Temporally Indifferent (TI)** method for combining annual panels, which is the standard FIA approach and matches EVALIDator's default behavior. This method pools all plots from annual panels within an evaluation period without differential weighting, producing statistically valid estimates that match official USDA Forest Service outputs.

The TI approach treats all plots within an evaluation group (EVALID) equally regardless of their measurement year, which is appropriate for:
- Current status estimates (e.g., "What is the current forest area?")
- Cross-sectional comparisons across regions
- Matching official FIA publications and reports

---

## Performance

pyFIA achieves high performance through modern data infrastructure:

### DuckDB Backend

- **Columnar storage** optimized for analytical queries
- **Vectorized execution** for aggregations
- **Lazy evaluation** for memory efficiency
- **10-100x faster** than row-based approaches for large queries

### Polars DataFrames

- **Zero-copy** data sharing with DuckDB
- **Parallel execution** on multi-core systems
- **2-5x faster** than pandas for typical operations

---

## Quick Start

### Installation

```bash
pip install pyfia
```

### Download Data

```python
from pyfia import download

# Download single state
db_path = download("NC")

# Download multiple states
db_path = download(["NC", "SC", "GA"])
```

### Basic Usage

```python
from pyfia import FIA, area, volume, tpa, biomass

with FIA(db_path) as db:
    # Filter to state and most recent evaluation
    db.clip_by_state("NC")
    db.clip_most_recent(eval_type="EXPVOL")
    
    # Forest area
    area_result = area(db, land_type="forest")
    
    # Volume by species
    volume_result = volume(db, by_species=True)
    
    # Trees per acre (live trees only)
    tpa_result = tpa(db, tree_domain="STATUSCD == 1")
    
    # Biomass estimates
    biomass_result = biomass(db)
```

---

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

---

## References

Bechtold, W.A.; Patterson, P.L., eds. 2005. The Enhanced Forest Inventory and Analysis Program—National Sampling Design and Estimation Procedures. Gen. Tech. Rep. SRS-80. Asheville, NC: U.S. Department of Agriculture, Forest Service, Southern Research Station. 85 p.

Stanke, H., Finley, A.O., Weed, A.S., Walters, B.F., & Domke, G.M. (2020). rFIA: An R package for estimation of forest attributes with the US Forest Inventory and Analysis database. *Environmental Modelling & Software*, 127, 104664.

Wei, X., Hayes, D., McHale, G. et al. (2025). PyFIA: analyzing and visualizing forest attributes using the United States Forest Inventory and Analysis database. *Carbon Balance and Management*. https://doi.org/10.1186/s13021-025-00364-7
