---
title: 'pyFIA: A Python Library for Statistically Rigorous Forest Inventory Analysis'
tags:
  - Python
  - forestry
  - forest inventory
  - FIA
  - statistical estimation
  - design-based inference
authors:
  - name: Chris Mihiar
    orcid: 0000-0000-0000-0000
    affiliation: 1
affiliations:
  - name: Independent Researcher
    index: 1
date: 25 December 2025
bibliography: paper.bib
---

# Summary

pyFIA is a high-performance Python library for analyzing data from the USDA Forest Service's Forest Inventory and Analysis (FIA) program. The FIA program maintains the largest continuous forest inventory in the world, collecting standardized measurements from over 300,000 permanent sample plots across all U.S. forests. pyFIA implements statistically rigorous design-based estimation methods following Bechtold & Patterson [-@bechtold2005], enabling researchers to produce population-level estimates of forest attributes—including biomass, volume, tree density, mortality, and growth—with properly calculated standard errors. The library leverages modern data science tools (Polars and DuckDB) for efficient processing of large-scale inventory datasets while maintaining exact statistical compatibility with the official USFS EVALIDator tool.

# Statement of Need

Forest inventory data underpin critical decisions in forest management, carbon accounting, climate policy, and ecological research. The FIA database is freely available, but extracting statistically valid estimates requires specialized knowledge of the complex sampling design, including post-stratification, expansion factors, and temporally-indifferent estimation procedures. Analysts without this expertise risk producing biased estimates or incorrect uncertainty quantification.

Existing tools for FIA analysis include EVALIDator (a web-based USFS tool), FIESTA [@frescino2023] (an R package), and rFIA [@stanke2020] (another R package). However, these options present barriers for Python-based research workflows increasingly common in data science, machine learning, and large-scale ecological modeling. A separate Python package also named PyFIA [@wei2025] focuses on visualization and bookkeeping for carbon analysis but does not implement the rigorous statistical estimation methodology required for design-based inference.

pyFIA fills this gap by providing:

1. **Design-based estimation** implementing the exact methodology from Bechtold & Patterson [-@bechtold2005], including ratio-of-means estimators and stratified variance calculations
2. **EVALIDator-validated results** with estimates matching the official USFS tool within expected sampling error
3. **Modern Python integration** using Polars DataFrames and DuckDB for efficient large-scale analysis
4. **Flexible domain filtering** supporting SQL-like syntax for custom tree and area domains
5. **Spatial analysis capabilities** via DuckDB's spatial extension for polygon-based filtering

The library targets forest ecologists, carbon scientists, natural resource managers, and data scientists requiring statistically defensible forest estimates within Python workflows.

# Core Functionality

pyFIA provides a simple, consistent API for forest attribute estimation:

```python
from pyfia import FIA, biomass, tpa, volume, area

with FIA("fia_database.duckdb") as db:
    db.clip_by_state(37)  # North Carolina
    db.clip_most_recent(eval_type="EXPVOL")

    # Estimate live tree biomass on forestland
    result = biomass(db, tree_type="live", land_type="forest")

    # Estimate by species with custom domain
    by_species = tpa(db, tree_domain="DIA >= 10.0", by_species=True)
```

Each estimation function returns a Polars DataFrame containing population estimates, standard errors, and sample sizes. The library handles the complex multi-stage calculations internally: merging tree, condition, and plot data; applying adjustment factors for the nested plot design; computing stratum-level statistics; and calculating design-based variance using the ratio-of-means estimator.

## Estimation Functions

| Function | Description |
|----------|-------------|
| `area()` | Forest land area by category |
| `biomass()` | Above/belowground biomass and carbon |
| `volume()` | Merchantable volume (cubic feet) |
| `tpa()` | Trees per acre and basal area |
| `mortality()` | Annual mortality rates |
| `growth()` | Net annual growth |
| `removals()` | Annual harvest removals |
| `area_change()` | Forest land transitions |

## Statistical Methodology

pyFIA implements temporally-indifferent (TI) estimation, the default method used by EVALIDator, which pools plots across multiple measurement years to increase sample size. The stratified ratio-of-means estimator follows Equation 4.8 of Bechtold & Patterson [-@bechtold2005]:

$$\hat{R} = \frac{\sum_h W_h \bar{y}_h}{\sum_h W_h \bar{x}_h}$$

where $W_h$ represents stratum weights, $\bar{y}_h$ is the stratum mean of the attribute of interest, and $\bar{x}_h$ is the stratum mean of the domain area. Variance estimation accounts for the covariance between numerator and denominator in ratio estimation.

# Validation

pyFIA includes an EVALIDator client for automated validation against official USFS estimates. Integration tests confirm that pyFIA estimates match EVALIDator within expected sampling variation (typically <3% relative difference) across multiple states and evaluation types.

# Acknowledgements

This work relies on data collected by the USDA Forest Service Forest Inventory and Analysis program. The statistical methodology follows Bechtold & Patterson [-@bechtold2005]. Development was assisted by Claude (Anthropic), an AI assistant, for code implementation and documentation; the author reviewed and validated all code and takes full responsibility for the software's correctness.

# References
