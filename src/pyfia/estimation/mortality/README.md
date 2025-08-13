# Enhanced Mortality Estimation Module

This module implements comprehensive mortality estimation functionality for pyFIA, supporting detailed grouping variables and variance calculations as specified in the mortality estimation plan.

## Overview

The enhanced mortality estimation module provides:
- Support for all FIA grouping variables (SPCD, SPGRPCD, OWNGRPCD, UNITCD, AGENTCD, DSTRBCD1-3)
- Complete variance calculations at plot, stratum, and population levels
- Flexible query building for both DuckDB and SQLite backends
- Full integration with the existing pyFIA estimation framework

## Components

### 1. **MortalityEstimator** (`estimator.py`)
The main estimation class that orchestrates the mortality calculation process.

```python
from pyfia import FIA
from pyfia.estimation.config import MortalityConfig
from pyfia.estimation.mortality import MortalityEstimator

# Configure estimation
config = MortalityConfig(
    mortality_type="tpa",
    by_species=True,
    group_by_ownership=True,
    group_by_agent=True,
    variance=True
)

# Run estimation
estimator = MortalityEstimator(db, config)
results = estimator.estimate()
```

### 2. **MortalityCalculator** (`calculator.py`)
Handles the core statistical calculations for mortality at plot, stratum, and population levels.

Key features:
- Plot-level expansion using TREE_GRM_COMPONENT table
- Stratum-level aggregation with proper weighting
- Population-level estimation with expansion factors

### 3. **MortalityQueryBuilder** (`query_builder.py`)
Generates optimized SQL queries for mortality data extraction.

Features:
- Database-specific query optimization (DuckDB vs SQLite)
- Flexible grouping variable support
- CTE-based query structure for performance
- Reference table joins for descriptive names

### 4. **MortalityVarianceCalculator** (`variance.py`)
Implements proper variance calculations following FIA statistical methodology.

Supports:
- Stratum-level variance components
- Population-level variance aggregation
- Ratio variance for per-acre estimates
- Multiple variance calculation methods (standard, ratio, hybrid)

### 5. **MortalityGroupHandler** (`group_handler.py`)
Manages grouping operations and reference table lookups.

Features:
- Validates grouping variables
- Adds descriptive names from reference tables
- Filters statistically significant groups
- Handles NULL values appropriately

## Configuration

The module uses the `MortalityConfig` class from `pyfia.estimation.config`:

```python
config = MortalityConfig(
    # Mortality type
    mortality_type="tpa",  # "tpa", "volume", or "both"
    tree_class="all",      # "all", "timber", "growing_stock"
    
    # Grouping options
    by_species=True,
    by_size_class=False,
    group_by_species_group=False,
    group_by_ownership=True,
    group_by_agent=True,
    group_by_disturbance=False,
    
    # Domain filters
    tree_domain="DIA >= 10.0",
    area_domain=None,
    land_type="forest",
    
    # Output options
    variance=True,
    totals=False,
    variance_method="ratio",  # "standard", "ratio", "hybrid"
    
    # Component options
    include_components=False,
    include_natural=True,
    include_harvest=True
)
```

## Usage Examples

### Basic Mortality Estimation
```python
from pyfia import FIA, mortality

db = FIA("path/to/fia.duckdb")

# Simple mortality by species
results = mortality(db, by_species=True, mortality_type="tpa")
```

### Advanced Grouping
```python
# Multiple grouping variables
results = mortality(
    db,
    grp_by=["SPCD", "OWNGRPCD", "AGENTCD"],
    mortality_type="tpa",
    variance=True,
    totals=True
)
```

### Custom Configuration
```python
from pyfia.estimation.config import MortalityConfig
from pyfia.estimation.mortality import MortalityEstimator

config = MortalityConfig(
    mortality_type="both",  # TPA and volume
    by_species=True,
    group_by_ownership=True,
    tree_domain="DIA >= 5.0 AND DIA < 20.0",
    variance_method="ratio",
    include_components=True
)

estimator = MortalityEstimator(db, config)
results = estimator.estimate()
```

## Database Interface Integration

The module integrates with the new database interface layer:

```python
from pyfia.database import create_interface

# Auto-detect database type
with create_interface("path/to/fia.duckdb") as db_interface:
    db = FIA("path/to/fia.duckdb")
    db._interface = db_interface
    
    results = mortality(db, by_species=True)
```

## Testing

Comprehensive test coverage is provided:

- `test_mortality_enhanced.py` - Integration tests for all functionality
- `test_mortality_query_builder.py` - Unit tests for query generation
- `test_mortality_variance.py` - Variance calculation tests

Run tests with:
```bash
uv run pytest tests/test_mortality*.py -v
```

## Validation

Use the validation script to compare with SQL results:

```bash
uv run python scripts/validate_mortality_implementation.py \
    --db path/to/fia.duckdb \
    --sql-results docs/queries/mortality/expected_results.csv \
    --by-species \
    --by-ownership \
    --tolerance 0.01
```

## Performance Considerations

1. **Query Optimization**: The query builder uses proper join order (smallest to largest tables)
2. **Lazy Evaluation**: Uses Polars LazyFrames for memory efficiency
3. **Batch Processing**: Handles large EVALID lists in batches
4. **Index Usage**: Leverages database indexes on EVALID, PLT_CN, and TRE_CN

## Statistical Methodology

The module implements design-based estimation following:
- Bechtold & Patterson (2005) FIA sampling methodology
- Post-stratified estimation with proper variance calculation
- Ratio-of-means estimators for per-acre values

## Future Enhancements

1. Caching for repeated calculations
2. Parallel processing for large datasets
3. Additional variance calculation methods
4. Support for custom mortality metrics
5. Integration with spatial analysis tools