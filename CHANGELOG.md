# Changelog

All notable changes to pyFIA will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **`live_tree()` function** — NSVB live tree carbon estimation:
  - Recomputes above-ground biomass from scratch using the NSVB framework (Westfall et al. 2023, GTR-WO-104)
  - Species-specific S10a carbon fractions (0.40–0.55) replace the flat 0.47 multiplier used by `biomass()`
  - 3-level coefficient lookup precedence: Bailey DIVISION, species-level, Jenkins fallback
  - Cull adjustment using Harmon et al. (2011) DECAYCD=3 density proportions
  - `pool='ag'|'bg'|'total'` — AG via NSVB, BG bridges to FIADB `TREE.CARBON_BG`
  - Validated against FIADB `TREE.CARBON_AG` on Georgia EVALID 132401 (130,952 trees): median per-tree relative error 0.085%
- **`standing_dead()` function** — NSVB standing dead carbon estimation:
  - Same NSVB biomass pipeline as `live_tree()`, plus decay-class reductions from `REF_TREE_DECAY_PROP`
  - `DENSITY_PROP` x wood, `BARK_LOSS_PROP` x bark, `BRANCH_LOSS_PROP` x branch by hardwood/softwood x DECAYCD
  - S10b dead-tree carbon fractions by hardwood/softwood x DECAYCD
  - No `TREE.CULL` adjustment for dead trees (per FIADB Appendix K)
  - `pool='ag'|'bg'|'total'` — same pool semantics as `live_tree()`
  - Broken-top corrections: crown-proportion adjustment (Appendix K) + paraboloid volume-ratio for trees with `ACTUALHT < HT`
  - Vendored Table S11 (`REF_TREE_STND_DEAD_CR_PROP`) for mean intact crown ratios by ecoregion province
  - Validated against FIADB on Georgia EVALID 132401 (6,870 trees): median 10.9% per-tree relative error
- **`pyfia.carbon` subpackage** — NSVB equation library, coefficient loaders, carbon fractions:
  - `pyfia.carbon.nsvb.equations` — Models 1, 2, 4, 5, harmonization, vectorized pipelines
  - `pyfia.carbon.nsvb.coefficients` — S1a–S8b coefficient tables, Bailey DIVISION lookup
  - `pyfia.carbon.nsvb.carbon_fractions` — S10a (live), S10b (dead), `REF_TREE_DECAY_PROP` loaders
  - Vendored coefficient CSVs from GTR-WO-104 supplementary archive
- **NSVB validation gate** — `tests/validation/test_live_tree_nsvb.py` and `test_standing_dead_nsvb.py`:
  - Per-tree parity tests against FIADB `CARBON_AG` on real Georgia inventory data
  - Layered diagnostics: carbon rel-error, biomass ratio, FIADB-implied carbon fraction
  - Ratchet thresholds that detect regressions and reward improvements
  - EVALID-scoped to the current annual evaluation (avoids legacy CRM data contamination)

### Changed
- **NGHGI report-reproduction scripts moved to the `pyfcaf` package** (`scripts/nghgi/` removed). pyfia refocuses on database querying and statistical estimation; pyfcaf consumes pyfia's carbon estimators to reproduce the EPA Chapter 6 / Annex 3.13 tables. The new entry points live at `python -m pyfcaf.nghgi.{stage_a,stage_b,multi_year,dead_wood_diagnostic}`.
- **Carbon module docstrings**: softened "aligned with EPA NGHGI LULUCF X pool" wording to point at the operational FIADB `CARBON_*` columns; dropped trailing USEPA Annex 3.13 citations. Operational methodology citations (Westfall 2023, Domke 2013/2016/2017, Smith & Heath, Birdsey, Bechtold & Patterson) preserved.

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
