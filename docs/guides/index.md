# User Guide

This guide covers PyFIA's core concepts and workflows for FIA data analysis.

## Overview

PyFIA implements design-based estimation following the methodology described in Bechtold & Patterson (2005). This ensures statistically valid estimates that match official USFS results.

## Key Concepts

### EVALID (Evaluation Identifier)

FIA data is organized by evaluation groups. Each EVALID represents a complete inventory cycle for a state or region. PyFIA automatically selects the most recent evaluation when you filter by state.

```python
from pyfia import download, FIA

db_path = download("GA")  # Download Georgia data
db = FIA(db_path)
db.clip_by_state("GA")    # Auto-selects most recent EVALID
```

### Domain Filtering

Domains define the population of interest:

- **Land Type**: Forest land, timberland, or all sampled land
- **Tree Type**: Growing stock, all live, or sawtimber
- **Custom Domains**: SQL-like conditions for specialized analysis

### Expansion Factors

FIA uses a stratified sampling design. Each plot/tree has an expansion factor indicating how many similar plots/trees it represents. PyFIA handles these automatically.

## Guides

- **[Downloading Data](downloading.md)**: Download FIA data directly from the DataMart
- **[Domain Filtering](filtering.md)**: Control which plots, conditions, and trees are included
- **[Variance Estimation](../variance_calculation_guide.md)**: Understanding uncertainty in estimates
- **[Lazy Evaluation](../lazy_evaluation_guide.md)**: Memory-efficient workflows for large datasets
