# PyFIA Examples

This directory contains complete example scripts demonstrating how to use pyFIA for various forest analysis tasks.

## Available Examples

### Volume Estimation

- **`estimate_california_volume_by_diameter.py`**: Comprehensive analysis of California's merchantable timber volume by diameter class, species, and ownership. Demonstrates volume estimation with grouping and statistical reporting.

### Area Estimation  

- **`estimate_minnesota_forest_area.py`**: Forest area analysis for Minnesota, including forest type distributions and land use classifications.

- **`estimate_oregon_forest_area.py`**: Oregon forest area analysis with focus on forest types and ownership patterns.

## Running Examples

All examples can be run directly with Python and accept an optional database path:

```bash
# Use default database path (fia.duckdb)
uv run python estimate_california_volume_by_diameter.py

# Specify custom database path
uv run python estimate_california_volume_by_diameter.py /path/to/your/fia.duckdb
```

## Requirements

Examples use additional formatting and progress libraries:

- **rich**: Terminal formatting and progress bars
- **polars**: Data manipulation
- **pyfia**: Core estimation functions

All dependencies are included in pyFIA's development installation.

## Features Demonstrated

### Statistical Estimation
- Volume, area, and tree count estimation
- Domain filtering (treeDomain, areaDomain) 
- Statistical grouping (bySpecies, by_size_class)
- Variance calculation and standard errors

### Data Analysis Patterns
- State-level filtering with `clip_by_state()`
- Most recent evaluation selection
- Custom grouping with `grp_by` parameter
- Rich terminal output with tables and progress bars

### Database Operations
- Efficient DuckDB queries
- Context manager usage
- Lazy evaluation for memory efficiency

## Customization

Examples serve as templates for your own analysis. Common modifications:

- Change state codes for different regions
- Modify domain filters for specific tree/area criteria  
- Add custom grouping variables
- Adjust output formatting
- Include additional estimation parameters

## Related Documentation

- [Estimation Functions](../queries/README.md): SQL examples and methodologies
- [CLAUDE.md](../../CLAUDE.md): Development setup and patterns
- [Architecture](../ARCHITECTURE_DIAGRAMS.md): System design overview