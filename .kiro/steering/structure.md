# PyFIA Project Structure

## Root Directory Layout
```
pyfia/
├── src/pyfia/           # Main package source code
├── tests/               # Test suite
├── docs/                # Documentation source
├── scripts/             # Development and utility scripts
├── overrides/           # MkDocs template overrides
├── .github/             # GitHub Actions workflows
├── .kiro/               # Kiro steering rules
└── pyproject.toml       # Project configuration
```

## Source Code Organization (`src/pyfia/`)

### Core Modules (`src/pyfia/core/`)
- **`fia.py`**: Main FIA class - primary entry point for database interactions
- **`data_reader.py`**: FIADataReader class for data loading and caching
- **`config.py`**: Configuration management and default settings
- **`settings.py`**: Pydantic settings models for type-safe configuration

### Estimation Functions (`src/pyfia/estimation/`)
- **`area.py`**: Forest area estimation (`area()` function)
- **`biomass.py`**: Biomass and carbon estimation (`biomass()` function)
- **`volume.py`**: Volume estimation (`volume()` function)
- **`tpa.py`**: Trees per acre estimation (`tpa()` function)
- **`mortality.py`**: Mortality estimation (`mortality()` function)
- **`growth.py`**: Growth estimation (`growth()` function)
- **`tree.py`**: Tree count estimation (`tree_count()` function)
- **`utils.py`**: Shared estimation utilities and helper functions

### Data Processing (`src/pyfia/filters/`)
- **`domain.py`**: Domain filtering logic (treeDomain, areaDomain)
- **`grouping.py`**: Data grouping and aggregation functions
- **`joins.py`**: Common table joins and data merging
- **`evalid.py`**: EVALID management and validation
- **`adjustment.py`**: Adjustment factor calculations
- **`classification.py`**: Data classification and categorization

### Database Layer (`src/pyfia/database/`)
- **`query_interface.py`**: Database query abstraction layer
- **`schema_mapper.py`**: FIA database schema mapping and validation
- **`memory_docs/`**: In-memory documentation for database schemas

### Supporting Modules
- **`models/`**: Pydantic data models for type safety
- **`constants/`**: FIA-specific constants and lookup tables
- **`locations/`**: Geographic location parsing and resolution

## Test Organization (`tests/`)

### Test Categories
- **`test_*_comprehensive.py`**: Comprehensive integration tests for major functions
- **`test_properties_*.py`**: Property-based tests using Hypothesis
- **`test_*_integration.py`**: Integration tests with real data scenarios
- **`conftest.py`**: Shared pytest fixtures and test utilities

### Test Data Strategy
- **Sample Database**: SQLite database with realistic FIA data structure
- **Mock Objects**: For unit testing without database dependencies
- **Property Testing**: Hypothesis-based tests for statistical properties
- **Integration Testing**: End-to-end tests with sample datasets

## Documentation Structure (`docs/`)

### Main Documentation
- **`README.md`**: Project overview and getting started guide
- **`ARCHITECTURE_DIAGRAMS.md`**: System architecture documentation
- **Development Guides**: Pre-commit, property testing, Pydantic v2 guides

### Query Library (`docs/queries/`)
- **Organized by Function**: `basic_tree/`, `biomass_carbon/`, `forest_area/`, etc.
- **Working Examples**: Real-world query examples with explanations
- **Methodology**: Statistical methods and evaluation approaches

## Development Scripts (`scripts/`)
- **`setup_precommit.py`**: Pre-commit hook installation script
- **`typecheck.py`**: Combined mypy and ty type checking script

## Configuration Philosophy

### Single Source of Truth
- **`pyproject.toml`**: All tool configurations in one place
- **Type Safety**: Pydantic models for runtime validation
- **Environment-Aware**: Settings that adapt to development vs production

### Code Organization Principles
- **Functional Separation**: Clear separation between estimation, filtering, and data access
- **rFIA Compatibility**: Function signatures and behavior match R rFIA package
- **Performance Isolation**: Database operations separated from in-memory processing
- **Statistical Accuracy**: Estimation logic isolated for easy validation against rFIA

## Import Patterns

### Public API (from `__init__.py`)
```python
from pyfia import FIA, area, biomass, volume, tpa, mortality, growth
```

### Internal Imports
- Use relative imports within package: `from .core import FIA`
- Import utilities explicitly: `from pyfia.estimation.utils import calculate_estimates`
- Keep database layer separate: `from pyfia.database import QueryInterface`

## File Naming Conventions
- **Snake Case**: All Python files use snake_case
- **Descriptive Names**: File names clearly indicate functionality
- **Test Matching**: Test files match source files with `test_` prefix
- **Documentation**: Markdown files use UPPER_CASE for major docs