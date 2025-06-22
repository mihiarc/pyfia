# pyFIA Architecture Diagram

## System Overview

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                                   USER INTERFACES                                 │
├─────────────────────────────────┬───────────────────────────────────────────────┤
│         DIRECT ACCESS           │               AI-POWERED ACCESS               │
├─────────────────────────────────┼───────────────────────────────────────────────┤
│         cli.py                  │               cli_ai.py                       │
│  - Direct pyFIA API calls       │  - Natural language queries                   │
│  - area(), biomass(), volume()  │  - SQL generation & execution                 │
│  - No SQL or AI needed          │  - Multiple agent types (basic/enhanced/cognee)│
│  - Rich formatting              │  - Schema exploration                         │
│  - EVALID management            │  - Query history & export                     │
├─────────────────────────────────┼───────────────────────────────────────────────┤
│         OTHER INTERFACES        │                                               │
├─────────────────────────────────┼───────────────────────────────────────────────┤
│  - Jupyter notebooks            │                                               │
│  - Python scripts               │                                               │
│  - Direct module imports        │                                               │
└─────────────────────────────────┴───────────────────────────────────────────────┘
                     │                                   │
                     ▼                                   ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                                AI/ML LAYER                                        │
├─────────────────────┬────────────────────────┬──────────────────────────────────┤
│   ai_agent.py       │  ai_agent_enhanced.py  │     cognee_fia_agent.py          │
│ - Basic LLM queries │  - Vector store RAG    │  - Cognee memory integration     │
│ - SQL generation    │  - Enhanced context    │  - Knowledge persistence         │
│ - 850 lines         │  - 1200 lines          │  - 350 lines (cleanest)         │
└─────────────────────┴────────────────────────┴──────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                            ESTIMATION LAYER                                       │
├──────────────┬──────────────┬──────────────┬──────────────┬───────────────────┤
│   area.py    │  biomass.py  │  volume.py   │    tpa.py    │   mortality.py    │
│ - Land area  │ - Tree bio   │ - Wood vol   │ - Trees/acre │ - Tree death      │
│ - 450 lines  │ - 400 lines  │ - 380 lines  │ - 500 lines  │ - 376 lines       │
├──────────────┴──────────────┴──────────────┴──────────────┼───────────────────┤
│                    growth.py / growth_direct.py             │ estimation_utils  │
│                    - Tree growth calculations               │ - Common funcs    │
│                    - 350 + 280 lines                        │ - 371 lines       │
└─────────────────────────────────────────────────────────────┴───────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                               CORE LAYER                                          │
├────────────────────────────┬──────────────────────────┬─────────────────────────┤
│         core.py            │    data_reader.py        │ duckdb_query_interface  │
│  - FIA class (main API)    │ - Database abstraction   │ - Query execution       │
│  - EVALID filtering        │ - SQLite/DuckDB support  │ - Schema inspection     │
│  - Table management        │ - Optimized reading      │ - Result formatting     │
│  - 580 lines               │ - 650 lines              │ - 420 lines             │
└────────────────────────────┴──────────────────────────┴─────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                            DATABASE LAYER                                         │
├─────────────────────────────────┬───────────────────────────────────────────────┤
│         SQLite Support          │              DuckDB Support                    │
│    - Legacy compatibility       │         - High performance                    │
│    - Smaller datasets           │         - Large scale analysis               │
│    - Batch processing           │         - Better SQL support                 │
└─────────────────────────────────┴───────────────────────────────────────────────┘
```

## Data Flow Diagram

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                           TWO DISTINCT PATHS                                  │
└──────────────────────────────────────────────────────────────────────────────┘

Path 1: Direct Programmatic Access
─────────────────────────────────
User Input
    │
    ▼
┌─────────────┐
│   cli.py    │
└─────────────┘
    │
    ▼
┌─────────────────────────────────┐
│      FIA Core (core.py)         │
│  - fia.area(bySpecies=True)    │
│  - fia.biomass(component='AG')  │
│  - fia.volume(volType='NET')    │
└─────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────┐
│   Estimation Modules            │
│  - area.py, biomass.py, etc.   │
│  - Direct statistical methods   │
└─────────────────────────────────┘

Path 2: AI-Enhanced Query Access
────────────────────────────────
User Query
    │
    ▼
┌─────────────┐     ┌─────────────┐
│  cli_ai.py  │────▶│  AI Agent   │
└─────────────┘     └─────────────┘
    │                      │
    │                      ▼
    │              ┌─────────────┐
    │              │ SQL Query   │
    │              │ Generation  │
    │              └─────────────┘
    │                      │
    ▼                      ▼
┌─────────────────────────────────┐
│  DuckDB Query Interface         │
│  - Direct SQL execution         │
│  - Schema inspection            │
└─────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────┐
│   Data Reader (data_reader.py)  │
│  - Database connection          │
│  - Optimized queries            │
└─────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────┐
│    Estimation Module            │
│  - Domain filtering             │
│  - Statistical calculations     │
└─────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────┐
│      Results & Formatting       │
│  - Tables, plots, exports       │
└─────────────────────────────────┘
```

## Code Duplication Heat Map

```
High Duplication (RED):
┌────────────────────────────────────────┐
│ Tree/Area Filtering (5x duplication)  │
│ - _apply_tree_filters()                │
│ - _apply_area_filters()                │
│ Files: area, biomass, volume, tpa,    │
│        mortality                       │
└────────────────────────────────────────┘
    │
    ▼
┌────────────────────────────────────────┐
│ Grouping Logic (4x duplication)       │
│ - Size class creation                  │
│ - Species grouping                     │
│ Files: biomass, volume, tpa, growth   │
└────────────────────────────────────────┘
    │
    ▼
┌────────────────────────────────────────┐
│ Stratification (4x duplication)       │
│ - Plot-stratum-EU calculations         │
│ - Adjustment factors                   │
│ Files: area, biomass, volume, tpa     │
└────────────────────────────────────────┘

Medium Duplication (YELLOW):
┌────────────────────────────────────────┐
│ CLI Connection Handling (3x)           │
│ Database Schema Display (3x)           │
│ Progress Indicators (3x)               │
└────────────────────────────────────────┘

Low/No Duplication (GREEN):
┌────────────────────────────────────────┐
│ Core FIA logic (unique)                │
│ Cognee integration (unique)            │
│ Statistical formulas (mostly unique)   │
└────────────────────────────────────────┘
```

## Dependency Graph - Two-CLI Architecture

```
Direct Path Dependencies:
┌─────────────┬─────────────┬─────────────┐
│   polars    │   duckdb    │    rich     │
│  (dataframes)│ (database)  │    (CLI)    │
└──────┬──────┴──────┬──────┴──────┬──────┘
       │             │             │
       ▼             ▼             ▼
    Estimation    data_reader    cli.py
    modules       core.py        (direct)

AI Path Dependencies:
┌─────────────┬─────────────┬─────────────┬─────────────┬─────────────┐
│   polars    │   duckdb    │    rich     │  langchain  │   cognee    │
│  (dataframes)│ (database)  │    (CLI)    │    (AI)     │  (memory)   │
└──────┬──────┴──────┬──────┴──────┬──────┴──────┬──────┴──────┬──────┘
       │             │             │             │             │
       ▼             ▼             ▼             ▼             ▼
  Query results  duckdb_query   cli_ai.py   AI agents    cognee_fia
                 _interface                  ai_*.py      _agent.py

Shared Core:
┌─────────────────────┐
│   estimation_utils  │ ◀── Used by all estimation modules
└─────────────────────┘
           │
┌─────────────────────┐
│      core.py        │ ◀── Used by cli.py and estimation modules
└─────────────────────┘
           │
┌─────────────────────┐
│   data_reader.py    │ ◀── Used by core.py
└─────────────────────┘

Note: cli.py has NO dependency on AI/LangChain/SQL generation
      cli_ai.py has NO dependency on core.py or estimation modules
```

## Refactoring Opportunities

### 1. Create Base Classes
```python
# estimation_base.py
class EstimationBase:
    def apply_filters()
    def calculate_plot_level()
    def calculate_stratum_level()
    def calculate_population_level()

# Each module inherits
class AreaEstimation(EstimationBase):
    def specific_calculations()
```

### 2. Consolidate AI Agents
```python
# Single flexible agent
class FIAAgent:
    def __init__(self, memory_backend='basic'):
        # Support basic, enhanced, cognee
```

### 3. Two-CLI Architecture (IMPLEMENTED)
```python
# cli.py - Direct programmatic access
class FIADirectCLI:
    def do_area()      # Calls fia.area()
    def do_biomass()   # Calls fia.biomass()
    def do_volume()    # Calls fia.volume()
    # No SQL, No AI

# cli_ai.py - AI-enhanced queries  
class FIAAICli:
    def _execute_natural_language()  # AI → SQL
    def _execute_sql()              # Direct SQL
    def do_agent()                  # Switch agents
    # Full AI/SQL capabilities
```

### 4. Extract Common Patterns
- Move all filter functions to estimation_utils
- Create GroupingMixin for size/species grouping
- Create StratificationMixin for common calculations

## Module Size Analysis

```
Largest Modules:
1. ai_agent_enhanced.py    - 1200 lines (could be 600)
2. cli.py                 - 886 lines  (direct API access - well organized)
3. ai_agent.py            - 850 lines  (could be 400)
4. cli_ai.py              - 765 lines  (AI-enhanced CLI - clean design)
5. data_reader.py         - 650 lines  (well organized)
6. core.py                - 580 lines  (well organized)
7. tpa.py                 - 500 lines  (could be 300)

Total Lines: ~10,200 (after removing deprecated files)
Potential After Refactor: ~8,000-8,500 (20% reduction)
```

## New Two-CLI Design Benefits

1. **Clear Separation of Concerns**
   - `cli.py`: Pure pyFIA API access, no external dependencies
   - `cli_ai.py`: All AI/NL/SQL functionality isolated

2. **User Choice**
   - Power users: Use `cli.py` for fast, direct statistical analysis
   - Exploratory users: Use `cli_ai.py` for natural language queries

3. **Reduced Complexity**
   - No mixed modes or confusing command prefixes
   - Each CLI optimized for its specific use case
   - Easier to maintain and test

4. **Performance**
   - Direct CLI avoids AI overhead entirely
   - AI CLI can use more sophisticated agents without impacting direct users