# pyFIA

[![Documentation](https://img.shields.io/badge/docs-GitHub%20Pages-blue)](https://mihiarc.github.io/pyfia/)
[![Deploy Documentation](https://github.com/mihiarc/pyfia/actions/workflows/deploy-docs.yml/badge.svg)](https://github.com/mihiarc/pyfia/actions/workflows/deploy-docs.yml)

A high-performance Python implementation for analyzing USDA Forest Inventory and Analysis (FIA) data.

## Overview

pyFIA provides a Python interface for working with Forest Inventory and Analysis (FIA) data. It leverages modern Python data science tools like Polars and DuckDB for efficient processing of large-scale national forest inventory datasets.

## Features

### Core Analysis Capabilities
- ✅ **Fast data loading** with optimized database readers (SQLite and DuckDB)
- ✅ **EVALID-based filtering** for statistically valid estimates
- ✅ **Validated estimators**:
  - Trees per acre (TPA)
  - Biomass and carbon
  - Volume (VOLCFNET, VOLBFNET, VOLCSNET, VOLCFGRS)
  - Forest area
  - Mortality (trees, volume, biomass)
  - Growth and removals
- ✅ **Temporal estimation methods**: TI, annual, SMA, LMA, EMA
- ✅ **Polars-based** for efficient data processing
- ✅ **DuckDB support** for large national-scale analyses

### 🤖 AI Agent (NEW!)
- **Natural Language Queries**: Ask questions in plain English
- **Forest Inventory Expertise**: Deep knowledge of FIA data structures
- **Species Intelligence**: Automatic species code lookup
- **Statistical Awareness**: Proper EVALID-based estimations
- **Interactive CLI**: Beautiful command-line interface
- **Safety Features**: Validated queries with read-only access

## Installation

```bash
# Basic installation
pip install pyfia

# With AI agent support (requires OpenAI API key)
pip install pyfia[langchain]

# With all optional dependencies
pip install pyfia[all]
```

### AI Agent Setup

To use the AI agent features, you'll need an OpenAI API key:

```bash
export OPENAI_API_KEY="your-api-key-here"
```

## Quick Start

```python
from pyfia import FIA, biomass, tpa, volume, area

# Load FIA data from DuckDB
db = FIA("path/to/FIA_database.duckdb", engine="duckdb")

# Get trees per acre
tpa_results = tpa(db, method='TI')

# Get biomass estimates
biomass_results = biomass(db, method='TI')

# Get forest area
area_results = area(db, method='TI')

# Get volume estimates
volume_results = volume(db, method='TI')
```

## 🤖 AI Agent for Natural Language Queries

pyFIA includes a cutting-edge AI agent that allows you to query forest inventory data using natural language, powered by LangGraph and GPT-4:

### Python API

```python
from pyfia.ai.agent import FIAAgent

# Create AI agent (requires OpenAI API key)
agent = FIAAgent("path/to/FIA_database.duckdb")

# Ask questions in natural language
response = agent.query("How many live oak trees are in North Carolina?")
print(response)

# Complex analysis queries
response = agent.query("What's the average diameter of pine trees by forest type?")
print(response)

# Statistical queries with proper EVALID handling
response = agent.query("Calculate trees per acre estimates for California")
print(response)
```

### Interactive CLI

Use the beautiful interactive command-line interface:

```bash
# Start the AI agent CLI
pyfia-ai path/to/database.duckdb

# Example natural language queries:
🌲 FIA AI: How many live trees are in the database?
🌲 FIA AI: What are the top 10 most common species?
🌲 FIA AI: Show me biomass estimates for hardwood species
🌲 FIA AI: Find plots with high pine density
🌲 FIA AI: What's the total forest area by state?
```

### AI Agent Features

- **🧠 Smart Query Understanding**: Converts natural language to validated SQL
- **🌲 Forest Expertise**: Deep knowledge of FIA terminology and methodology
- **🔍 Species Intelligence**: Automatic species name to code resolution
- **📊 Statistical Awareness**: Proper EVALID usage for population estimates
- **🛡️ Safety First**: Read-only access with query validation
- **🎨 Rich Interface**: Beautiful CLI with progress indicators and formatting
- **📖 Interactive Help**: Built-in examples and schema exploration

See the [AI Agent documentation](docs/ai_agent/README.md) for comprehensive documentation.

## Validation Results

pyFIA has been thoroughly validated and achieves excellent accuracy:

| Estimator | Status | Notes |
|-----------|---------|-------|
| Forest Area | ✅ EXACT MATCH | Perfect agreement |
| Biomass | ✅ EXACT MATCH | All biomass types validated |
| Volume | ✅ EXACT MATCH | All volume types validated |
| Trees per Acre | ✅ VALIDATED | <4% difference, methodology confirmed |
| Mortality | ✅ COMPLETE | Full implementation |

## Requirements

- Python 3.11+
- polars>=1.31.0
- numpy>=2.3.0
- duckdb>=0.9.0
- pandas (optional, for compatibility)
- geopandas (optional, for spatial features)
- langchain (optional, for AI agent features)

## Development

```bash
# Clone the repository
git clone https://github.com/mihiarc/pyfia.git
cd pyfia

# Install with uv (recommended)
uv venv
source .venv/bin/activate
uv pip install -e .[dev]

# Run tests
uv run pytest

# Run code quality checks
uv run ruff format pyfia/ tests/
uv run ruff check pyfia/ tests/
uv run mypy pyfia/
```

## Database Support

pyFIA supports multiple database backends:

- **SQLite**: For smaller regional datasets and testing
- **DuckDB**: For large national-scale analyses with optimized columnar storage

## License

MIT License

## Citation

If you use pyFIA in your research, please cite:

```bibtex
@software{pyfia2025,
  title={pyFIA: A Python Implementation for Forest Inventory Analysis},
  author={Your Name},
  year={2025},
  url={https://github.com/mihiarc/pyfia}
}
```