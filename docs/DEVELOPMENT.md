# Development Guide

> Technical reference for pyFIA development. For business context and product strategy, see [CLAUDE.md](../CLAUDE.md).

## Setup

```bash
# Install with uv in development mode
uv venv
source .venv/bin/activate  # or .venv\Scripts\activate on Windows
uv pip install -e .[dev]

# Setup pre-commit hooks
pre-commit install
```

## Essential Commands

### Testing
```bash
uv run pytest                              # Run all tests
uv run pytest tests/test_area.py           # Run specific test file
uv run pytest --cov=pyfia --cov-report=html # With coverage
uv run pytest tests/test_property_based.py -v # Property-based tests
```

### Code Quality
```bash
uv run ruff format              # Format code
uv run ruff check --fix         # Lint code
uv run mypy src/pyfia/          # Type checking
uv run pre-commit run --all-files # All hooks
```

### Documentation
```bash
uv run mkdocs serve   # Build locally
uv run mkdocs build   # Build for deployment
```

## Architecture

### Module Structure
```
pyfia/
├── core/               # Database and reader functionality
│   ├── fia.py         # Main FIA database class
│   ├── data_reader.py # Efficient data loading
│   └── backends/      # DuckDB, SQLite, MotherDuck backends
├── carbon/             # NSVB carbon estimation
│   ├── live_tree.py   # Live tree carbon (NSVB AG + FIADB BG bridge)
│   ├── standing_dead.py # Standing dead carbon (NSVB + decay reductions)
│   ├── understory.py  # Understory vegetation carbon (condition-level)
│   ├── downed_dead.py # Downed dead wood carbon (condition-level)
│   ├── litter.py      # Litter carbon (condition-level)
│   ├── soil_organic.py # Soil organic carbon (condition-level)
│   ├── total_ecosystem.py # Sum of all 6 pools
│   ├── stock_change.py # Carbon stock change between inventory periods
│   ├── data/          # Non-NSVB coefficient tables (Birdsey/Smith & Heath)
│   └── nsvb/          # NSVB equation library, coefficients, carbon fractions
├── estimation/         # Statistical estimation
│   ├── base.py        # BaseEstimator with Template Method pattern
│   ├── grm.py         # GRM data loading and adjustment
│   ├── grm_base.py    # GRM base estimator (mortality, growth, removals)
│   ├── variance.py    # Shared variance calculations
│   └── estimators/    # Individual estimators
│       ├── area.py
│       ├── area_change.py
│       ├── biomass.py
│       ├── carbon_pools.py
│       ├── growth.py
│       ├── mortality.py
│       ├── panel.py
│       ├── removals.py
│       ├── site_index.py
│       ├── tpa.py
│       ├── tree_metrics.py
│       └── volume.py
├── filtering/          # Domain filtering and indicators
│   ├── core/parser.py # Centralized domain expression parser
│   ├── tree/filters.py
│   ├── area/filters.py
│   └── indicators/    # Land type classification
├── evalidator/         # EVALIDator API client for validation
├── downloader/         # FIA data download from DataMart
├── validation.py       # Input validation utilities
├── utils/              # Reference table helpers
└── constants/          # FIA constants and standard values
```

### Core Components

**FIA Database Class (`pyfia.core.fia.FIA`)**
- Main entry point for database connections
- Supports DuckDB and SQLite backends
- Key methods: `clip_by_evalid()`, `clip_by_state()`, `clip_most_recent()`

**Estimation Functions**
- Simple API: `area()`, `biomass()`, `volume()`, `tpa()`, `live_tree()`, `standing_dead()`, `understory()`, `downed_dead()`, `litter()`, `soil_organic()`, `total_ecosystem()`, `stock_change()`, `mortality()`, `growth()`, `removals()`, `area_change()`, `site_index()`, `tree_metrics()`
- All support domain filtering, grouping, variance calculations
- BaseEstimator uses Template Method for consistent workflow

**Data Reader (`pyfia.core.data_reader.FIADataReader`)**
- Efficient data loading with WHERE clause support
- Backend-specific optimizations

## Dependencies

| Package | Purpose |
|---------|---------|
| **Polars** | Primary dataframe library |
| **DuckDB** | Database engine |
| **Pydantic v2** | Settings management |
| **Rich** | Terminal output |
| **ConnectorX** | Fast database connectivity |

## Code Patterns

### Do
- Use Polars LazyFrame for memory efficiency
- Use Pydantic v2 for settings only (not data)
- Follow FIA naming conventions in public APIs
- Prefer functions over classes
- Use context managers for connections

### Don't
- Create Strategy, Factory, Builder patterns without clear need
- Add abstraction layers for hypothetical flexibility
- Create deep directory nesting (max 3 levels)
- Use complex inheritance hierarchies

## FIA Quick Reference

> Full details in [fia_technical_context.md](./fia_technical_context.md)

### EVALID System
Codes in SSYYTT format for statistically valid plot groupings.
Note: state codes with single-digit FIPS (e.g., AL=1) produce 5-digit EVALIDs.

```python
with FIA("data/nfi_south.duckdb") as db:
    db.clip_by_state(37)                   # North Carolina
    db.clip_most_recent(eval_type="VOL")   # Most recent volume evaluation
    results = volume(db)
```

### Evaluation Types
- `EXPALL`: Area estimation → `area()`
- `EXPVOL`: Volume/biomass → `volume()`, `biomass()`, `tpa()`
- `EXPMORT`/`EXPGROW`: Mortality and growth

### Critical Rules
1. Never mix EVALIDs
2. Match eval_type to estimation function
3. Always filter before estimation

### Domain Filtering
```python
volume(db, tree_domain="STATUSCD == 1")      # Live trees
area(db, area_domain="SLOPE < 30")           # Low slope areas
tpa(db, tree_domain="DIA >= 10.0")           # Large trees
```

### Variance Calculation
pyFIA implements the stratified domain total variance formula from Bechtold & Patterson (2005):

```
V(Ŷ) = Σ_h w_h² × s²_yh × n_h
```

Key implementation details:
- **All plots included**: Include plots with zero values in variance calculation
- **Per-acre SE**: Calculated as `SE_total / total_area`
- **Single-plot strata**: Excluded (variance undefined with n=1)

The `calculate_domain_total_variance()` function in `variance.py` implements this formula and matches EVALIDator output within 1-3%.

## Testing Patterns

- **Use real FIA data** when possible (georgia.duckdb, nfi_south.duckdb)
- Mock databases must include complete table structures (including GRM tables)
- Property-based tests for statistical accuracy
- Validate against EVALIDator results

## Documentation Standards

All public API functions use NumPy-style docstrings. The `mortality()` function is the reference implementation.

### Required Sections
1. Summary line
2. Extended summary
3. Parameters (with types and valid values)
4. Returns (with column descriptions)
5. See Also
6. Notes
7. Examples

## Refactoring Guidelines

### When to Simplify
- Deep nesting (>3 levels)
- Unnecessary patterns without clear benefit
- Pass-through layers
- Complex configs for simple parameters

### How to Simplify
- Replace class hierarchies with functions
- Use direct parameters instead of config objects
- Flatten directory structures
- Remove abstraction layers that don't add value

## Performance Notes

- DuckDB provides 10-100x faster queries than SQLite
- 5-6x compression ratio
- Polars LazyFrame enables memory-efficient streaming
