# Changelog

All notable changes to pyFIA will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### NSVB carbon subsystem (targeted for 1.5.0)

Feature-complete on `main` but **held out of 1.4.0** pending domain verification
of condition-level pool NULL handling (#90) and resolution of the two
live-tree-stock implementations (#89).

#### Added
- **`pyfia.carbon` subpackage** — NSVB equation library, coefficient loaders, and carbon fractions (Models 1/2/4/5; S1a–S8b coefficient tables; Bailey DIVISION lookup; S10a/S10b carbon fractions; vendored coefficient CSVs from GTR-WO-104 Supp1).
- **`live_tree()`** — NSVB live-tree carbon (AG recomputed from biomass; species-specific carbon fractions; Bailey DIVISION → species → Jenkins precedence). Validated vs FIADB `CARBON_AG` on Georgia (130,952 trees): median per-tree error 0.085%.
- **`standing_dead()`** — NSVB standing-dead carbon with decay-class reductions and broken-top corrections.
- **Condition-level carbon pools** — `downed_dead()`, `litter()`, `soil_organic()`, `understory()` from FIADB `COND.CARBON_*` densities.
- **`total_ecosystem()`** — sum of all six IPCC carbon pools.
- **`stock_change()`** — condition-level carbon stock change between remeasurement periods (per-pool or total), REMPER-annualized.
- `PLOTGEOM` added to `COMMON_TABLES` (Bailey DIVISION lookup for NSVB).
- **NSVB validation gates** — per-tree parity tests against FIADB `CARBON_AG` on real Georgia inventory data.

#### Changed
- **NGHGI report-reproduction scripts moved to the `pyfcaf` package** (`scripts/nghgi/` removed); pyfcaf consumes pyfia's carbon estimators.
- **Carbon module docstrings** softened to point at the operational FIADB `CARBON_*` columns.

#### Fixed
- **Woodland-species biomass collapse** in `live_tree()`; **woodland-species zeroing** in `standing_dead()` (shared guard in the carbon estimator base class).
- DECAYCD empty-string filter leak in the standing-dead path.

## [1.4.1] - 2026-06-30

Bug-fix release for the Growth-Removal-Mortality (GRM) / condition-column paths,
reported while building disturbance × treatment-stratified carbon stock-change
against per-state FIADB DuckDBs. All public APIs from 1.4.0 remain backward
compatible; no estimation math, variance formulas, or EVALIDator-validated
numbers change for previously-working queries.

### Fixed
- **`clip_most_recent(eval_type="GRM")` always raised `NoEVALIDError`** (#102) — `find_evalid()` built a non-existent `EXPGRM` evaluation type. `eval_type` is now resolved through an explicit token → `EVAL_TYP` map: `"GRM"` is a working alias for the shared growth/removal/mortality family EVALID, raw `EXP*` codes pass through, and unknown tokens raise a clear error listing the valid options.
- **`area_domain` could not filter on COND columns that `grp_by` accepts** (#103) — e.g. `area_domain="DSTRBCD1 > 0"` raised `ColumnNotFoundError` for `growth()`/`mortality()`/`removals()`. Columns referenced in `grp_by`, `area_domain`, and `tree_domain` are now resolved against the real table schemas and threaded into the COND/TREE/PLOT loads uniformly across all four estimators.
- **`mortality()` / `removals()` crashed on `grp_by` columns outside a fixed allowlist** (#104) — e.g. `TRTCD1` was dropped before the variance step (a `ColumnNotFoundError`) where `growth()`/`biomass()` succeeded. `aggregate_cond_to_plot()` now carries every loaded condition column to plot level, so any grouping column survives.
- **`biomass(grp_by=...)` crashed on states where a grouping key is null for every group** (#105) — e.g. `DSTRBCD1` on Oregon raised a polars `SchemaError` (`i64` vs `null`) when the per-group variance frame inferred a `Null` dtype. Variance join keys are now dtype-aligned before joining; no joined value changes.
- **GRM estimators crashed on state databases that store numeric columns as `VARCHAR`** (#106) — e.g. `growth()` on AZ/NM/WY (`division with 'String'`) and `removals()` on MT/NV (`cannot compare string with numeric`). GRM arithmetic columns (`SUBP_TPA*_UNADJ_*`, `REMPER`, `DIA*`, `DRYBIO_*`, `MACRO_BREAKPOINT_DIA`, …) are cast to their declared numeric types at load.

## [1.4.0] - 2026-06-26

Adds the `tree_metrics()` estimator and a public `query()` method, plus EVALID,
variance, and downloader fixes and release hardening. All public APIs from
1.3.0 remain backward compatible.

### Added
- **`tree_metrics()` estimator** — TPA-weighted descriptive statistics for derived per-condition tree metrics (#73).
- **Public `query()` method** on `FIA` for raw SQL execution (#72).
- **PLT_CN / CONDID grouping** — support plot- and condition-level grouping columns for plot-condition estimates (#71).

### Changed
- **`__version__` is now read from package metadata** via `importlib.metadata` (#92) — `pyproject.toml` is the single source of truth, so the version string can no longer drift from the published package.

### Fixed
- **EVALID year parsing for single-digit state FIPS codes** (#78, #79, #80) — `_extract_evaluation_year()` now uses `END_INVYR` from `POP_EVAL` with an EVALID tiebreaker.
- **EVALID filtering not applied to all PLT_CN tables** (#76, #77) — tables such as `TREE_GRM_COMPONENT` were not filtered by EVALID; column-detection filtering now applies to direct and spatial filter paths.
- **Area variance underestimation for rare categories** when using `grp_by` (#68).
- **`sanitize_sql_path()` failure on Windows** when calling `download()` (#74).
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

[Unreleased]: https://github.com/mihiarc/pyfia/compare/v1.4.1...HEAD
[1.4.1]: https://github.com/mihiarc/pyfia/compare/v1.4.0...v1.4.1
[1.4.0]: https://github.com/mihiarc/pyfia/compare/v1.3.0...v1.4.0
[1.3.0]: https://github.com/mihiarc/pyfia/compare/v1.2.3...v1.3.0
[1.2.1]: https://github.com/mihiarc/pyfia/compare/v1.2.0...v1.2.3
[1.2.0]: https://github.com/mihiarc/pyfia/compare/v1.1.0b1...v1.2.0
[1.1.0b1]: https://github.com/mihiarc/pyfia/compare/v1.0.0b1...v1.1.0b1
[1.0.0b1]: https://github.com/mihiarc/pyfia/compare/v0.3.0...v1.0.0b1
[0.3.0]: https://github.com/mihiarc/pyfia/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/mihiarc/pyfia/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/mihiarc/pyfia/releases/tag/v0.1.0
