# Mortality Estimation Module

This module implements enhanced mortality estimation for pyFIA following FIA statistical procedures.

## Overview

The mortality module provides comprehensive tree mortality estimation with support for:
- Multiple grouping variables (species, ownership, mortality agents, disturbance codes)
- Proper stratified variance calculation
- Both trees per acre (TPA) and volume/basal area mortality metrics
- Integration with the FIA database structure

## Components

### MortalityCalculator
The main calculator class that inherits from `BaseEstimator` and implements:
- Plot-level mortality aggregation
- Stratum-level expansion
- Population-level estimation with proper variance

### MortalityEstimatorConfig
Extended configuration class that adds mortality-specific parameters:
- `group_by_species_group`: Group by SPGRPCD
- `group_by_ownership`: Group by OWNGRPCD
- `group_by_agent`: Group by mortality agent (AGENTCD)
- `group_by_disturbance`: Group by disturbance codes (DSTRBCD1-3)
- `include_components`: Include BA and volume mortality

### MortalityVarianceCalculator
Implements stratified sampling variance calculation:
- Stratum-level variance components
- Population-level variance aggregation
- Ratio variance for complex estimates

### MortalityGroupHandler
Manages grouping operations:
- Validates grouping variables
- Adds reference table lookups (species names, owner names, etc.)
- Filters to statistically significant groups

## Usage

```python
from pyfia import FIA, mortality

# Basic mortality estimation
with FIA("path/to/fia.duckdb") as db:
    # Simple mortality estimate
    results = mortality(db)
    
    # Mortality by species and ownership
    results = mortality(
        db,
        by_species=True,
        by_ownership=True,
        variance=True
    )
    
    # Detailed mortality with all components
    results = mortality(
        db,
        by_species=True,
        by_agent=True,
        by_disturbance=True,
        include_components=True,
        totals=True
    )
```

## Grouping Variables

The module supports grouping by:
- **SPCD**: Individual species
- **SPGRPCD**: Species groups
- **OWNGRPCD**: Ownership groups
- **AGENTCD**: Mortality agents (disease, insects, fire, etc.)
- **DSTRBCD1-3**: Disturbance codes
- **UNITCD**: FIA estimation units
- **FORTYPCD**: Forest type codes
- Any custom columns via `grp_by` parameter

## Output Structure

Results include:
- **MORTALITY_TPA**: Trees per acre mortality
- **MORTALITY_BA**: Basal area mortality (optional)
- **MORTALITY_VOL**: Volume mortality (optional)
- **Standard errors** or **variances**
- **N_PLOTS**: Number of plots in estimate
- **Grouping columns** with reference names where available

## Statistical Methodology

The module follows FIA's design-based estimation procedures:
1. Uses TREE_GRM_COMPONENT table for mortality data
2. Applies proper stratification via POP_STRATUM
3. Calculates variance using stratified sampling formulas
4. Supports ratio-of-means estimation

## Integration with SQL Queries

The module is designed to eventually support SQL-based queries for performance while maintaining the same statistical rigor. The current implementation uses Polars for in-memory processing but can be extended to use DuckDB SQL queries directly.

## Future Enhancements

- Direct SQL query generation for large-scale processing
- Temporal analysis (annual, SMA, LMA, EMA methods)
- Growth-removal-mortality component breakdown
- Custom mortality agent groupings
- Spatial analysis capabilities