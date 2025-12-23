# Changelog

All notable changes to pyFIA will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/mihiarc/pyfia/compare/v1.1.0b1...HEAD
[1.1.0b1]: https://github.com/mihiarc/pyfia/compare/v1.0.0b1...v1.1.0b1
[1.0.0b1]: https://github.com/mihiarc/pyfia/compare/v0.3.0...v1.0.0b1
[0.3.0]: https://github.com/mihiarc/pyfia/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/mihiarc/pyfia/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/mihiarc/pyfia/releases/tag/v0.1.0
