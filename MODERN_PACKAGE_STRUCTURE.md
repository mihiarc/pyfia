# pyFIA Modern Package Structure - 2025 Best Practices

## Current vs. Modern Structure

### Current (Flat Layout)
```
pyfia/
├── pyproject.toml
├── README.md
├── pyfia/                    # ❌ All modules directly exposed
│   ├── __init__.py
│   ├── core.py
│   ├── area.py
│   ├── biomass.py
│   ├── cli.py
│   ├── cli_ai.py
│   ├── ai_agent.py
│   └── ... (25+ modules)
└── tests/
```

### Modern (src/ Layout)
```
pyfia/
├── pyproject.toml              # ✅ Configured for src/ layout
├── README.md
├── LICENSE
├── CHANGELOG.md
│
├── src/                        # ✅ SOURCE ISOLATION
│   └── pyfia/                  # Main package
│       ├── __init__.py         # Clean public API
│       ├── py.typed            # Type checking marker
│       │
│       ├── core/               # ✅ LOGICAL GROUPING
│       │   ├── __init__.py
│       │   ├── fia.py          # Main FIA class
│       │   ├── data_reader.py  # Database interface
│       │   ├── config.py       # Configuration
│       │   └── settings.py     # Pydantic settings
│       │
│       ├── estimation/         # ✅ ALL ESTIMATION TOGETHER
│       │   ├── __init__.py
│       │   ├── base.py         # Base estimation classes
│       │   ├── utils.py        # Shared utilities
│       │   ├── area.py
│       │   ├── biomass.py
│       │   ├── volume.py
│       │   ├── tpa.py
│       │   ├── mortality.py
│       │   ├── growth.py
│       │   └── tree.py
│       │
│       ├── database/           # ✅ DB-SPECIFIC FUNCTIONALITY
│       │   ├── __init__.py
│       │   ├── query_interface.py
│       │   ├── schema_mapper.py
│       │   └── memory_docs/
│       │
│       ├── ai/                 # ✅ AI COMPONENTS ISOLATED
│       │   ├── __init__.py
│       │   ├── agent.py
│       │   ├── domain_knowledge.py
│       │   └── prompts/
│       │
│       ├── cli/                # ✅ ALL CLI TOGETHER
│       │   ├── __init__.py
│       │   ├── base.py
│       │   ├── direct.py       # Main CLI
│       │   ├── ai_interface.py # AI CLI
│       │   ├── config.py
│       │   └── utils.py
│       │
│       ├── filters/            # ✅ DATA PROCESSING
│       │   ├── __init__.py
│       │   ├── domain.py
│       │   ├── grouping.py
│       │   └── joins.py
│       │
│       ├── models/             # ✅ PYDANTIC MODELS
│       │   ├── __init__.py
│       │   ├── estimation.py
│       │   ├── database.py
│       │   └── validation.py
│       │
│       └── constants/          # ✅ CONSTANTS GROUPED
│           ├── __init__.py
│           ├── fia_codes.py
│           ├── math.py
│           └── defaults.py
│
├── tests/                      # ✅ OUTSIDE src/
│   ├── __init__.py
│   ├── conftest.py
│   ├── test_core/
│   ├── test_estimation/
│   ├── test_cli/
│   └── integration/
│
├── docs/                       # Documentation
├── scripts/                    # Development scripts
└── examples/                   # Usage examples
```

## Key Benefits of This Structure

### 1. **Import Safety**
```python
# ❌ Current: Can accidentally import from development files
from pyfia.area import area  # Might import from wrong location

# ✅ Modern: Only imports from installed package
from pyfia.estimation import area  # Always correct
```

### 2. **Clear Responsibility Separation**
- `core/`: Fundamental FIA functionality
- `estimation/`: All statistical methods together
- `database/`: Query interfaces and schema utilities
- `ai/`: AI components (optional dependency)
- `cli/`: Command-line interfaces
- `filters/`: Data filtering and processing

### 3. **Better Testing**
```bash
# ❌ Current: Tests might use development files
pytest  # Unpredictable behavior

# ✅ Modern: Tests always use installed package
pip install -e .  # Install in editable mode
pytest            # Tests against installed package
```

### 4. **Professional API**
```python
# ✅ Clean, intuitive imports
from pyfia.core import FIA
from pyfia.estimation import area, biomass, volume
from pyfia.ai import FIAAgent  # Optional
```

## Modern pyproject.toml Configuration

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

# ✅ Optional dependencies for modular installation
[project.optional-dependencies]
ai = [
    "langchain>=0.1.0",
    "langchain-openai>=0.1.0",
    "langgraph>=0.1.0",
    "openai>=1.12.0",
]
spatial = ["geopandas>=0.14.0"]
dev = [
    "pytest>=7.4.0",
    "pytest-cov>=4.1.0",
    "ruff>=0.1.0",
    "mypy>=1.7.0",
    "pre-commit>=3.6.0",
]
all = ["pyfia[ai,spatial,dev]"]

# ✅ Proper CLI entry points
[project.scripts]
pyfia = "pyfia.cli.direct:main"
pyfia-ai = "pyfia.cli.ai_interface:main"

# ✅ src/ layout configuration
[tool.setuptools]
package-dir = {"" = "src"}

[tool.setuptools.packages.find]
where = ["src"]
include = ["pyfia*"]

[tool.setuptools.package-data]
"pyfia.database.memory_docs" = ["*.md"]
"pyfia.ai.prompts" = ["*.txt"]
```

## Migration Strategy

### Phase 1: Create Structure
1. Create `src/pyfia/` directory
2. Create module subdirectories (`core/`, `estimation/`, etc.)
3. Move files to appropriate locations
4. Update `__init__.py` files

### Phase 2: Update Configuration
1. Update `pyproject.toml` for src/ layout
2. Configure package discovery
3. Update entry points

### Phase 3: Fix Imports
1. Update all internal imports
2. Test editable installation: `pip install -e .`
3. Update tests to use new structure

### Phase 4: Validate
1. Run all tests
2. Test CLI tools work correctly
3. Verify package builds correctly

## Why This Matters for Your Learning

Understanding modern Python packaging is crucial because:

1. **Industry Standard**: All new Python projects use src/ layout
2. **Tool Support**: Modern tools (Poetry, PDM, Hatch) default to src/
3. **CI/CD**: Better behavior in automated environments
4. **Distribution**: Proper packaging for PyPI publication
5. **Collaboration**: Makes your code more professional and maintainable

## Next Steps

Would you like me to:
1. **Create example files** showing the new structure?
2. **Help with the migration process** step by step?
3. **Research specific tools** for automating the migration?
4. **Show you how to validate** the new structure works correctly?

This restructuring will position pyFIA as a modern, professional Python package following 2025 best practices while maintaining all existing functionality. 