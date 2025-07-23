# Project Structure & Architecture

## Source Code Organization (`src/pyfia/`)

### Core Modules
- **`core/`** - Database connections, configuration, and main FIA class
  - `fia.py` - Main FIA database class with EVALID filtering
  - `data_reader.py` - Database readers for SQLite/DuckDB
  - `config.py` - Configuration management
  - `settings.py` - Pydantic settings models

### Estimation Functions
- **`estimation/`** - Statistical estimators following FIA methodology
  - `area.py` - Forest area estimation
  - `biomass.py` - Biomass and carbon estimation
  - `volume.py` - Volume estimation (VOLCFNET, VOLBFNET, etc.)
  - `tpa.py` - Trees per acre estimation
  - `mortality.py` - Mortality estimation
  - `growth.py` - Growth and removals
  - `utils.py` - Shared estimation utilities

### Data Processing
- **`filters/`** - Data filtering and domain logic
  - `domain.py` - Domain-specific filters
  - `evalid.py` - EVALID-based filtering
  - `classification.py` - Tree/plot classification
  - `grouping.py` - Data grouping utilities
  - `joins.py` - Common table joins

### Database Layer
- **`database/`** - Database interface and schema mapping
  - `query_interface.py` - SQL query execution
  - `schema_mapper.py` - Database schema mapping
  - `memory_docs/` - FIA database documentation

### AI Agent
- **`ai/`** - Natural language query processing
  - `agent.py` - Main LangGraph-based agent
  - `domain_knowledge.py` - Forest inventory expertise
  - `result_formatter.py` - Query result formatting

### CLI Interfaces
- **`cli/`** - Command-line interfaces
  - `base.py` - Shared CLI functionality
  - `direct.py` - Direct SQL CLI
  - `ai_interface.py` - AI agent CLI
  - `config.py` - CLI configuration

### Supporting Modules
- **`models/`** - Pydantic data models
- **`constants/`** - FIA constants and lookup tables
- **`locations/`** - Geographic location parsing

## Testing Structure (`tests/`)
- Property-based testing with Hypothesis
- Comprehensive integration tests
- Validation against known FIA results
- Fixtures in `conftest.py` for consistent test data

## Documentation (`docs/`)
- **`ai_agent/`** - AI agent documentation
- **`queries/`** - Example queries organized by topic
- MkDocs-based documentation with Material theme

## Configuration Files
- **`pyproject.toml`** - Modern Python packaging and tool configuration
- **`.pre-commit-config.yaml`** - Code quality hooks
- **`mkdocs.yml`** - Documentation configuration

## Architecture Patterns
- **Lazy evaluation** - Polars LazyFrames for efficient data processing
- **EVALID-first** - All estimations use proper EVALID filtering
- **Functional API** - Estimation functions take FIA instance and return results
- **Type safety** - Gradual MyPy adoption with Pydantic models
- **CLI consistency** - Shared base classes for all CLI interfaces