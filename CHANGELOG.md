# Changelog

All notable changes to pyFIA will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.2.0] - 2025-01-18

### Added
- **`panel()` function** - Create t1/t2 remeasurement panels from FIA data:
  - Condition-level panels for harvest and area change analysis
  - Tree-level panels with GRM-based tree fate classification (survivor, mortality, cut, diversion, ingrowth)
  - Expansion factor support for per-acre estimates via `expand=True`
  - Configurable filters: `min_remper`, `max_remper`, `min_invyr`, `harvest_only`
- **`by_size_class` parameter for GRM estimators** - Group mortality, growth, and removals by diameter class:
  - `size_class_type="market"`: Pre-merchantable, Pulpwood, Chip-n-Saw, Sawtimber (species-aware)
  - `size_class_type="standard"`: FIA numeric ranges (1.0-4.9, 5.0-9.9, etc.)
  - `size_class_type="descriptive"`: Saplings, Small, Medium, Large
- **Pre-merchantable tree support** - Trees <5" DBH now supported in mortality estimation (fixes #67):
  - Use `tree_type="live"` to include all live trees
  - Properly categorized as "Pre-merchantable" in market size classes
  - TPA recommended for small trees (FIA doesn't calculate volume for <5" DBH)
- **AGENTCD grouping in mortality()** - Group mortality estimates by cause of death (tree-level)
- **DSTRBCD grouping in mortality()** - Group mortality estimates by disturbance code (condition-level)
- Example script `examples/mortality_by_cause.py` for timber casualty loss analysis

### Fixed
- **GRM totals ~60x too high when EVALID not set** - Trees were counted multiple times across all annual evaluations. Now auto-filters to most recent GRM evaluation with warning.
- **`area_domain` filter not applied in `area()` function** - Domain filters were being ignored
- **SQL injection vulnerabilities** - Fixed high-severity security issues in query construction
- **UNITCD not included when using AGENTCD grouping** - Missing grouping column in mortality output
- **Table caching bug** - Tables now reload when new columns are needed
- **Null rows in area estimation grouped output** - Fixed null rows appearing in results
- **COND table caching after other estimators** - Fixed stale cache issues

### Changed
- Refactored estimators to use centralized column resolution and data loading modules
- Improved error handling with custom exception hierarchy
- Enhanced type hints and validation across estimation module

## [1.1.0b1] - 2025-12-23

### Added
- **MotherDuck backend** - Cloud-based FIA data access via MotherDuck serverless warehouse
- **Spatial filtering** - New methods for geographic subsetting:
  - `clip_by_polygon()` - Filter plots to polygon boundary
  - `intersect_polygons()` - Join polygon attributes to plots for grouping
- **`area_change()` estimator** - Forest land transition analysis between inventory cycles
- `get_table_schema()` method for MotherDuckReaderWrapper
- `include_trees` parameter to `prepare_estimation_data()`
- Area change estimates to EVALIDator client
- Comprehensive spatial filtering guide

### Changed
- Optimized memory usage in biomass estimator with SQL-level filtering
- Optimized `area.py` apply_filters for cloud backends
- Standardized error handling with custom exception hierarchy
- Replaced magic numbers with EVALIDYearParsing constants

### Fixed
- MACRO_BREAKPOINT_DIA type comparison by casting to Float64
- Estimation import paths to use re-exported functions
- Spatial filters now properly flow polygon attributes through estimators

### Testing
- Added area_change validation tests against EVALIDator
- Added comprehensive unit tests for area_change estimator
- Added comprehensive unit tests for exceptions, parser, and constants

## [1.0.0b1] - 2025-12-16

### Added
- Complete estimation API with 8 core estimators:
  - `area()` - Forest land area estimation
  - `volume()` - Standing timber volume (net/gross/sound/sawlog)
  - `biomass()` - Above/below ground biomass
  - `tpa()` - Trees per acre and basal area
  - `mortality()` - Annual mortality volume
  - `growth()` - Annual net growth volume
  - `removals()` - Annual removals volume
  - `carbon_pools()` - Carbon stock by pool (AG/BG/total)
- EVALIDator validation framework with automated API testing
- Comprehensive variance estimation following Bechtold & Patterson (2005)
- FIA DataMart integration for direct data downloads
- Reference table utilities for species, forest types, and state names
- Filtering system for land types, tree types, and domain expressions
- Grouping capabilities for stratified analysis
- DuckDB backend with Polars LazyFrame for high performance

### Validated
- All core estimators validated against USFS EVALIDator API
- Point estimates match exactly (within floating point tolerance)
- Standard errors within acceptable tolerance:
  - Area estimates: 3%
  - Tree-based estimates (volume, biomass, tpa): 15%
  - GRM estimates (growth, mortality, removals): 30%

### Technical
- Implements stratified ratio-of-means variance formula (Eq. 4.1, 4.2, 4.8)
- Two-stage aggregation for proper stratum weighting
- Supports EVALID-based plot filtering for consistent estimation units

## [0.3.0] - 2025-12-15

### Added
- GRMBaseEstimator for growth/mortality/removals (Phase 2 estimates)
- Type hints across estimation module
- Property-based tests with Hypothesis
- Plot count tracking in validation tests
- SE validation with configurable tolerances

### Changed
- Replaced placeholder variance calculations with proper error handling
- Improved documentation with specific Bechtold & Patterson (2005) citations
- Enhanced validation test framework with plot count comparisons

### Fixed
- Variance calculations now properly handle ratio-of-means estimation
- GRM tests now correctly unpack 3 values from extract_grm_estimate

## [0.2.0] - 2025-XX-XX

### Added
- Initial estimation functionality
- Core database connectivity with DuckDB
- Basic filtering capabilities
- EVALIDator client for validation

## [0.1.0] - 2025-XX-XX

### Added
- Initial project structure
- FIA database abstraction layer
- Basic data reading capabilities

[Unreleased]: https://github.com/mihiarc/pyfia/compare/v1.2.0...HEAD
[1.2.0]: https://github.com/mihiarc/pyfia/compare/v1.1.0b1...v1.2.0
[1.1.0b1]: https://github.com/mihiarc/pyfia/compare/v1.0.0b1...v1.1.0b1
[1.0.0b1]: https://github.com/mihiarc/pyfia/compare/v0.3.0...v1.0.0b1
[0.3.0]: https://github.com/mihiarc/pyfia/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/mihiarc/pyfia/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/mihiarc/pyfia/releases/tag/v0.1.0
