# Changelog

All notable changes to pyFIA will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.4.0] - 2026-06-26

First release of the NSVB carbon subsystem. Large additive release — all
public APIs from 1.3.0 remain backward compatible.

### Added
- **`pyfia.carbon` subpackage** — NSVB equation library, coefficient loaders, carbon fractions:
  - `pyfia.carbon.nsvb.equations` — Models 1, 2, 4, 5, harmonization, vectorized pipelines
  - `pyfia.carbon.nsvb.coefficients` — S1a–S8b coefficient tables, Bailey DIVISION lookup
  - `pyfia.carbon.nsvb.carbon_fractions` — S10a (live), S10b (dead), `REF_TREE_DECAY_PROP` loaders
  - Vendored coefficient CSVs from GTR-WO-104 supplementary archive
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
- **Condition-level carbon pool estimators** — `downed_dead()`, `litter()`, `soil_organic()`, `understory()` read FIADB `COND.CARBON_*` densities and expand with the two-stage estimator.
- **`total_ecosystem()` function** — sums all 6 IPCC carbon pools into a total-ecosystem estimate.
- **`stock_change()` function** — condition-level carbon stock-change accounting between remeasurement periods (per-pool or total), with REMPER annualization and dual-NULL filtering.
- **`tree_metrics()` estimator** — TPA-weighted descriptive statistics for derived per-condition tree metrics (#73).
- **Public `query()` method** on `FIA` for raw SQL execution (#72).
- **PLT_CN / CONDID grouping** — support plot- and condition-level grouping columns for plot-condition estimates (#71).
- `PLOTGEOM` added to `COMMON_TABLES` (enables Bailey DIVISION lookup for NSVB).
- **NSVB validation gates** — `tests/validation/test_live_tree_nsvb.py` and `test_standing_dead_nsvb.py`:
  - Per-tree parity tests against FIADB `CARBON_AG` on real Georgia inventory data
  - Layered diagnostics: carbon rel-error, biomass ratio, FIADB-implied carbon fraction
  - Ratchet thresholds that detect regressions and reward improvements
  - EVALID-scoped to the current annual evaluation (avoids legacy CRM data contamination)

### Changed
- **NGHGI report-reproduction scripts moved to the `pyfcaf` package** (`scripts/nghgi/` removed). pyfia refocuses on database querying and statistical estimation; pyfcaf consumes pyfia's carbon estimators to reproduce the EPA Chapter 6 / Annex 3.13 tables. The new entry points live at `python -m pyfcaf.nghgi.{stage_a,stage_b,multi_year,dead_wood_diagnostic}`.
- **Carbon module docstrings**: softened "aligned with EPA NGHGI LULUCF X pool" wording to point at the operational FIADB `CARBON_*` columns; dropped trailing USEPA Annex 3.13 citations. Operational methodology citations (Westfall 2023, Domke 2013/2016/2017, Smith & Heath, Birdsey, Bechtold & Patterson) preserved.
- **`__version__` is now read from package metadata** via `importlib.metadata` (#92) — `pyproject.toml` is the single source of truth, so the version string can no longer drift from the published package.

### Fixed
- **EVALID year parsing for single-digit state FIPS codes** (#78, #79, #80) — `_extract_evaluation_year()` now uses `END_INVYR` from `POP_EVAL` with an EVALID tiebreaker.
- **EVALID filtering not applied to all PLT_CN tables** (#76, #77) — tables such as `TREE_GRM_COMPONENT` were not filtered by EVALID; column-detection filtering now applies to direct and spatial filter paths.
- **Area variance underestimation for rare categories** when using `grp_by` (#68).
- **`sanitize_sql_path()` failure on Windows** when calling `download()` (#74).
- **Woodland-species biomass collapse** in the `live_tree()` NSVB path; **woodland-species zeroing** in `standing_dead()`; both share a guard via the carbon estimator base class.
- DECAYCD empty-string filter leak in the standing-dead path.
- **Non-atomic file writes** in the downloader (#88) — downloads now write to a temporary file and atomically replace the destination (with cleanup on interrupt), and cache metadata is written the same way; an interrupted download/write no longer leaves a truncated file. `get_cached()` now verifies file size on every hit and supports opt-in MD5 verification (the stored checksum was previously never checked).
- **Silently cached broken databases on reference-table download failure** (#86) — `download()` now verifies the built database contains the required reference tables (`REF_SPECIES`, `REF_FOREST_TYPE`, `REF_STATE`) before caching. If any are missing/empty the incomplete database is discarded and a clear `DownloadError` is raised telling the user to retry, instead of caching a database that breaks `by_species` / name-join operations.

### Security
- **`clip_by_state()` / `clip_by_evalid()` now coerce arguments to `int`** (#87) — enforces the documented `int | list[int]` contract and rejects non-numeric input (e.g. `"37 OR 1=1"`) before it reaches the SQL `IN (...)` clause, closing a string-interpolation seam.

## [1.3.0] - 2026-02-07

### Added
- **Ratio-of-means variance for per-acre estimates** (#70).
- `Makefile` with test and validation convenience commands.
- Unit tests for previously untested estimators.

### Changed
- Modernized type annotations; explicit GRM error handling; stricter domain validation.

### Fixed
- All mypy type errors across 22 source files.
- Panel validation now sets the EVALID filter before calling `removals()`.
- Out-of-memory crash from conftest marker hooks; slow tests filtered by default.
- Miscellaneous technical-debt cleanup (#69).

## [1.2.1] – [1.2.3] - 2026-01-28 – 2026-01-29

Rapid maintenance releases. (Entries reconstructed from git history — these
versions predate git tagging, so per-patch attribution is approximate.)

### Added
- **`site_index()` estimator** — area-weighted mean site index.
- Jupyter tutorial notebooks for learning pyFIA.

### Changed
- Refactored variance calculation to eliminate duplication across estimators.
- Standardized EVALID handling across estimators.
- Consolidated input-validation logic into shared utilities.
- Removed fiatools branding/references.

### Fixed
- `growth()` now handles a missing `BEGINEND` table in DuckDB.
- Reference-table helper functions accept `Path` objects.
- Misleading ERROR log for missing tables (e.g. `BEGINEND`).
- Numerous notebook column-name, API, and Colab-compatibility fixes.

## [1.2.0] - 2026-01-18

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

[Unreleased]: https://github.com/mihiarc/pyfia/compare/v1.4.0...HEAD
[1.4.0]: https://github.com/mihiarc/pyfia/compare/v1.3.0...v1.4.0
[1.3.0]: https://github.com/mihiarc/pyfia/compare/v1.2.3...v1.3.0
[1.2.1]: https://github.com/mihiarc/pyfia/compare/v1.2.0...v1.2.3
[1.2.0]: https://github.com/mihiarc/pyfia/compare/v1.1.0b1...v1.2.0
[1.1.0b1]: https://github.com/mihiarc/pyfia/compare/v1.0.0b1...v1.1.0b1
[1.0.0b1]: https://github.com/mihiarc/pyfia/compare/v0.3.0...v1.0.0b1
[0.3.0]: https://github.com/mihiarc/pyfia/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/mihiarc/pyfia/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/mihiarc/pyfia/releases/tag/v0.1.0
