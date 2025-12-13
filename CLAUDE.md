# CLAUDE.md

> This file provides guidance to Claude Code when working with this repository.

## Project Overview

**pyFIA** is a high-performance Python library for analyzing USDA Forest Inventory and Analysis (FIA) data. It provides statistically valid estimation methods following Bechtold & Patterson (2005) methodology.

## Design Philosophy

### Simplicity First
- **No over-engineering**: Avoid unnecessary patterns (Strategy, Factory, Builder)
- **Direct functions**: `volume(db)` not `VolumeEstimatorFactory.create().estimate()`
- **YAGNI**: Don't build for hypothetical future needs
- **Flat structure**: Maximum 3 levels of directory nesting

### Statistical Rigor
- Design-based estimation following Bechtold & Patterson (2005)
- Results must match EVALIDator (official USFS tool)
- Always include uncertainty estimates (SE, confidence intervals)
- Never compromise accuracy for convenience

### User Trust
- Show your work: Transparent methodology
- Validate against official sources
- Clear error messages when queries can't be answered
- Honest about limitations

## Documentation Map

| Document | Purpose |
|----------|---------|
| [README.md](./README.md) | Quick start for users |
| [docs/DEVELOPMENT.md](./docs/DEVELOPMENT.md) | Technical setup, architecture |
| [docs/fia_technical_context.md](./docs/fia_technical_context.md) | FIA methodology reference |
| [~/business/](../business/) | Business strategy and market analysis (outside repo) |

## Development Quick Reference

```bash
# Setup
uv venv && source .venv/bin/activate && uv pip install -e .[dev]

# Test
uv run pytest

# Quality
uv run ruff format && uv run ruff check --fix && uv run mypy src/pyfia/
```

## Core Principles for Contributors

1. **User value first**: Every feature should reduce friction for end users
2. **Statistical validity**: Never ship estimates that could mislead
3. **Simplicity**: When in doubt, choose the simpler approach
4. **Real data testing**: Always test with actual FIA databases
5. **Documentation**: If it's not documented, it doesn't exist

## Important Notes

- **No backward compatibility debt**: Refactor freely, don't maintain old APIs
- **Performance matters**: Choose fast implementations over elegant abstractions
- **YAML schemas are source of truth**: FIA table definitions live in YAML
- **`mortality()` is the documentation gold standard**: Match its docstring quality

## Project Structure

```
pyfia/
├── src/pyfia/           # Library source code
│   ├── core/            # Database and settings
│   ├── estimation/      # Statistical estimation
│   ├── filtering/       # Domain filtering
│   └── downloader/      # FIA data download from DataMart
├── tests/               # Test suite
├── docs/                # Technical documentation
├── ../business/         # Business docs (outside repo)
├── examples/            # Example scripts
└── data/                # Test databases
```
