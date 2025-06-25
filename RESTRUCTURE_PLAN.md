# pyFIA Restructuring Plan - 2025 Python Package Best Practices

## Overview

Reorganize pyFIA from flat layout to modern **src/ layout** with logical module grouping following 2025 Python packaging standards.

## New Project Structure

```
pyfia/
├── pyproject.toml              # Modern build configuration
├── README.md                   # Main documentation
├── LICENSE                     # License file
├── CHANGELOG.md                # Version history
├── .gitignore                  # Git ignore patterns
│
├── src/                        # SOURCE LAYOUT (2025 best practice)
│   └── pyfia/                  # Main package
│       ├── __init__.py         # Package exports
│       ├── py.typed            # Type checking marker
│       │
│       ├── core/               # Core functionality
│       │   ├── __init__.py
│       │   ├── fia.py          # Main FIA class (renamed from core.py)
│       │   ├── data_reader.py  # Database interface
│       │   ├── config.py       # Configuration management
│       │   └── settings.py     # Pydantic settings
│       │
│       ├── database/           # Database-related modules
│       │   ├── __init__.py
│       │   ├── query_interface.py  # DuckDB interface
│       │   ├── schema_mapper.py    # Schema utilities
│       │   └── memory_docs/        # FIA documentation
│       │       ├── __init__.py
│       │       └── *.md
│       │
│       ├── estimation/         # Statistical estimation methods
│       │   ├── __init__.py
│       │   ├── base.py         # Base estimation classes
│       │   ├── utils.py        # Estimation utilities
│       │   ├── area.py         # Forest area estimation
│       │   ├── biomass.py      # Biomass calculations
│       │   ├── volume.py       # Volume estimation
│       │   ├── tpa.py          # Trees per acre
│       │   ├── mortality.py    # Mortality analysis
│       │   ├── growth.py       # Growth estimation
│       │   └── tree.py         # Tree counting
│       │
│       ├── filters/            # Data filtering and processing
│       │   ├── __init__.py
│       │   ├── domain.py       # Domain filtering
│       │   ├── grouping.py     # Data grouping utilities
│       │   └── joins.py        # Common joins (renamed from common_joins.py)
│       │
│       ├── ai/                 # AI and ML components
│       │   ├── __init__.py
│       │   ├── agent.py        # Main AI agent
│       │   ├── domain_knowledge.py  # FIA expertise
│       │   └── prompts/        # Prompt templates
│       │       ├── __init__.py
│       │       └── *.txt
│       │
│       ├── cli/                # Command-line interfaces
│       │   ├── __init__.py
│       │   ├── base.py         # Base CLI functionality
│       │   ├── direct.py       # Direct API CLI (renamed from cli.py)
│       │   ├── ai_interface.py # AI-enhanced CLI (renamed from cli_ai.py)
│       │   ├── config.py       # CLI configuration
│       │   └── utils.py        # CLI utilities
│       │
│       ├── models/             # Data models and validation
│       │   ├── __init__.py
│       │   ├── estimation.py   # Estimation result models
│       │   ├── database.py     # Database models
│       │   └── validation.py   # Data validation
│       │
│       └── constants/          # Constants and enums
│           ├── __init__.py
│           ├── fia_codes.py    # FIA-specific codes
│           ├── math.py         # Mathematical constants
│           └── defaults.py     # Default values
│
├── tests/                      # Test suite (outside src/)
│   ├── __init__.py
│   ├── conftest.py             # Test configuration
│   ├── test_core/              # Core functionality tests
│   ├── test_estimation/        # Estimation tests
│   ├── test_cli/               # CLI tests
│   ├── test_ai/                # AI component tests
│   └── integration/            # Integration tests
│
├── docs/                       # Documentation (outside src/)
│   ├── README.md
│   ├── architecture_diagram.md
│   ├── queries/                # Query examples
│   └── *.md
│
├── scripts/                    # Development scripts
│   ├── __init__.py
│   ├── setup_precommit.py
│   └── typecheck.py
│
└── examples/                   # Usage examples
    ├── notebooks/              # Jupyter notebooks
    ├── basic_usage.py
    └── advanced_analysis.py
```

## Key Improvements

### 1. **src/ Layout Benefits**
- **Import safety**: Prevents accidental imports from development directory
- **Testing reliability**: Tests run against installed package, not source files
- **Distribution clarity**: Clear separation between package code and development files
- **CI/CD friendly**: Better behavior in automated environments

### 2. **Logical Module Organization**

#### Core (`src/pyfia/core/`)
- Main FIA class and fundamental functionality
- Database interfaces and configuration
- Settings management

#### Estimation (`src/pyfia/estimation/`)
- All statistical estimation methods grouped together
- Base classes for common patterns
- Shared utilities for estimation procedures

#### Database (`src/pyfia/database/`)
- Query interfaces and schema utilities
- FIA documentation and memory docs
- Database-specific functionality

#### AI (`src/pyfia/ai/`)
- AI agents and domain knowledge
- Separated from core functionality
- Optional dependency handling

#### CLI (`src/pyfia/cli/`)
- All command-line interfaces
- Shared CLI utilities and configuration
- Clear separation between direct and AI interfaces

### 3. **Clean Package Exports**

#### Main `__init__.py`
```python
"""
pyFIA - Python implementation of rFIA for Forest Inventory Analysis
"""

__version__ = "0.2.0"
__author__ = "Chris Mihiar"

# Core exports
from pyfia.core import FIA, get_fia
from pyfia.core.data_reader import FIADataReader
from pyfia.core.settings import PyFIASettings, settings

# Estimation functions
from pyfia.estimation import (
    area, biomass, volume, tpa, mortality, growth, tree_count
)

# Configuration
from pyfia.core.config import config, get_default_db_path, get_default_engine

__all__ = [
    # Core classes
    "FIA", "get_fia", "FIADataReader",
    # Settings
    "PyFIASettings", "settings",
    # Estimation functions  
    "area", "biomass", "volume", "tpa", "mortality", "growth", "tree_count",
    # Configuration
    "config", "get_default_db_path", "get_default_engine",
]
```

### 4. **Updated pyproject.toml**

```toml
[build-system]
requires = ["setuptools>=68.0", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "pyfia"
version = "0.2.0"
description = "Python implementation of rFIA for Forest Inventory Analysis"
readme = "README.md"
license = {text = "MIT"}
authors = [{name = "Chris Mihiar", email = "your.email@example.com"}]
maintainers = [{name = "Chris Mihiar", email = "your.email@example.com"}]
classifiers = [
    "Development Status :: 4 - Beta",
    "Intended Audience :: Science/Research",
    "License :: OSI Approved :: MIT License",
    "Programming Language :: Python :: 3",
    "Programming Language :: Python :: 3.11",
    "Programming Language :: Python :: 3.12",
    "Topic :: Scientific/Engineering",
]
requires-python = ">=3.11"
dependencies = [
    "polars>=1.31.0",
    "duckdb>=0.9.0",
    "numpy>=2.3.0",
    "pydantic>=2.5.0",
    "pydantic-settings>=2.1.0",
    "rich>=13.0.0",
]

[project.optional-dependencies]
ai = [
    "langchain>=0.1.0",
    "langchain-openai>=0.1.0",
    "langgraph>=0.1.0",
    "openai>=1.12.0",
]
spatial = [
    "geopandas>=0.14.0",
]
dev = [
    "pytest>=7.4.0",
    "pytest-cov>=4.1.0",
    "ruff>=0.1.0",
    "mypy>=1.7.0",
    "pre-commit>=3.6.0",
]
all = ["pyfia[ai,spatial,dev]"]

[project.scripts]
pyfia = "pyfia.cli.direct:main"
pyfia-ai = "pyfia.cli.ai_interface:main"

[project.urls]
Homepage = "https://github.com/mihiarc/pyfia"
Documentation = "https://pyfia.readthedocs.io"
Repository = "https://github.com/mihiarc/pyfia"
Issues = "https://github.com/mihiarc/pyfia/issues"

[tool.setuptools]
package-dir = {"" = "src"}

[tool.setuptools.packages.find]
where = ["src"]
include = ["pyfia*"]

[tool.setuptools.package-data]
"pyfia.database.memory_docs" = ["*.md"]
"pyfia.ai.prompts" = ["*.txt"]
```

## Migration Steps

### Phase 1: Create New Structure
1. Create `src/pyfia/` directory structure
2. Move and rename files according to plan
3. Update import statements throughout codebase
4. Update `__init__.py` files with proper exports

### Phase 2: Update Configuration
1. Update `pyproject.toml` for src layout
2. Configure setuptools package discovery
3. Update entry points for CLI tools
4. Add optional dependencies structure

### Phase 3: Fix Imports and Tests
1. Update all internal imports to use new structure
2. Move tests outside src/ directory
3. Update test imports and configurations
4. Ensure editable installation works: `pip install -e .`

### Phase 4: Update Documentation
1. Update import examples in documentation
2. Update CLI installation instructions
3. Update development setup instructions
4. Add migration guide for existing users

## Benefits of This Structure

### **Developer Experience**
- **Clear responsibility separation**: Each module has a defined purpose
- **Easier navigation**: Logical grouping makes finding code intuitive
- **Better IDE support**: Modern IDEs work better with src/ layout
- **Type checking**: Cleaner type checking with proper package structure

### **Maintainability**
- **Modular design**: Components can be developed/tested independently
- **Dependency management**: Optional dependencies properly separated
- **Future-proof**: Structure supports growth and new features
- **Standards compliance**: Follows 2025 Python packaging best practices

### **Testing and Quality**
- **Reliable testing**: Tests run against installed package
- **CI/CD friendly**: Better behavior in automated environments
- **Import safety**: Prevents development-time import issues
- **Distribution validation**: Package building catches import errors

### **User Experience**
- **Clean imports**: `from pyfia.estimation import area`
- **Optional features**: Users can install only needed components
- **CLI tools**: Proper entry points for command-line usage
- **Documentation**: Clear API structure for users

## Next Steps

1. **Review and approve** this structure plan
2. **Create migration branch** to implement changes
3. **Update dependencies** to use modern versions
4. **Test thoroughly** to ensure all functionality works
5. **Update documentation** to reflect new structure
6. **Communicate changes** to users with migration guide

This restructuring positions pyFIA as a modern, professional Python package that follows 2025 best practices while maintaining all existing functionality. 