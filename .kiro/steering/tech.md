# Technology Stack & Development

## Core Technologies
- **Python 3.11+** - Minimum supported version
- **Polars** - Primary data processing framework for high performance
- **DuckDB** - Columnar database for large-scale analytics
- **NumPy** - Numerical computing foundation
- **Pydantic** - Data validation and settings management
- **Rich** - Terminal formatting and CLI enhancement

## Optional Dependencies
- **LangChain/LangGraph** - AI agent functionality
- **GeoPandas/Shapely** - Spatial data processing
- **Pandas** - Legacy compatibility support

## Build System
- **setuptools** - Package building
- **uv** - Recommended package manager for development
- **pyproject.toml** - Modern Python packaging configuration

## Code Quality Tools
- **Ruff** - Fast Python linter and formatter (replaces black, flake8, isort)
- **MyPy** - Static type checking with gradual adoption
- **Pre-commit** - Git hooks for code quality
- **Pytest** - Testing framework with coverage support
- **Hypothesis** - Property-based testing

## Common Commands

### Development Setup
```bash
# Clone and setup with uv (recommended)
uv venv
source .venv/bin/activate
uv pip install -e .[dev]

# Alternative with pip
pip install -e .[dev]
```

### Testing
```bash
# Run all tests
uv run pytest
# or
pytest

# Run with coverage
pytest --cov=pyfia
```

### Code Quality
```bash
# Format code
uv run ruff format src/ tests/
ruff format src/ tests/

# Lint and fix
uv run ruff check src/ tests/ --fix
ruff check src/ tests/ --fix

# Type checking
uv run mypy src/pyfia/
mypy src/pyfia/
```

### Pre-commit
```bash
# Install hooks
pre-commit install

# Run on all files
pre-commit run --all-files
```

### Documentation
```bash
# Serve docs locally
mkdocs serve

# Deploy docs
./deploy_docs.sh
```

## CLI Tools
- **pyfia** - Direct CLI interface
- **pyfia-ai** - AI agent CLI
- **pyfia-typecheck** - Type checking script
- **pyfia-setup-precommit** - Pre-commit setup utility