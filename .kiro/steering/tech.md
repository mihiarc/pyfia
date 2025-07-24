# PyFIA Technology Stack

## Build System & Package Management
- **Build System**: `setuptools` with `pyproject.toml` configuration
- **Package Manager**: `uv` for fast dependency resolution and virtual environment management
- **Python Versions**: 3.11, 3.12, 3.13 (minimum 3.11)

## Core Dependencies
- **Data Processing**: Polars (>=1.31.0) for fast DataFrames, DuckDB (>=0.9.0) for SQL queries
- **Database Connectivity**: ConnectorX (>=0.3.1) for efficient data loading
- **Data Formats**: PyArrow (>=14.0.0) for columnar data, NumPy (>=2.3.0) for numerical operations
- **Configuration**: Pydantic (>=2.11.0) for data validation and settings management

## Optional Dependencies
- **Spatial Analysis**: `geopandas`, `shapely` (install with `pip install pyfia[spatial]`)
- **Pandas Compatibility**: `pandas` (install with `pip install pyfia[pandas]`)

## Development Tools
- **Testing**: pytest with hypothesis for property-based testing, pytest-cov for coverage
- **Code Quality**: ruff for linting/formatting, mypy for type checking, ty for modern type analysis
- **Security**: bandit for security scanning, detect-secrets for credential detection
- **Documentation**: MkDocs with Material theme, git-revision-date plugin
- **Pre-commit**: Comprehensive hooks for code quality enforcement

## Common Commands

### Development Setup
```bash
# Install in development mode with all dependencies
pip install -e .[dev]

# Or using uv (recommended)
uv pip install -e .[dev]

# Setup pre-commit hooks
uv run pyfia-setup-precommit
```

### Testing
```bash
# Run all tests
uv run pytest

# Run with coverage
uv run pytest --cov=pyfia --cov-report=html

# Run property-based tests with more examples
uv run pytest tests/test_property_based.py --hypothesis-profile=ci

# Run specific test file
uv run pytest tests/test_biomass_comprehensive.py -v
```

### Code Quality
```bash
# Format code
uv run ruff format pyfia/

# Lint and auto-fix
uv run ruff check --fix pyfia/

# Type checking (both mypy and ty)
uv run pyfia-typecheck

# Run all pre-commit hooks
uv run pre-commit run --all-files
```

### Documentation
```bash
# Serve docs locally
uv run mkdocs serve

# Build documentation
uv run mkdocs build

# Deploy to GitHub Pages
./deploy_docs.sh
```

## Configuration Files
- **pyproject.toml**: Main project configuration (dependencies, tools, metadata)
- **mkdocs.yml**: Documentation site configuration
- **.pre-commit-config.yaml**: Pre-commit hooks configuration
- **.bandit**: Security scanning exclusions
- **.secrets.baseline**: Known false positive secrets for detect-secrets

## Performance Considerations
- **DuckDB**: Used for large-scale data queries (10-100x faster than pandas)
- **Polars**: Used for in-memory operations (2-5x faster than pandas)
- **Lazy Evaluation**: Polars lazy frames for memory-efficient workflows
- **Parallel Processing**: Built-in support for concurrent operations