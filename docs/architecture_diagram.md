# pyFIA Architecture

## System Overview

```mermaid
graph TB
    %% Define styles
    classDef userInterface fill:#4a90e2,stroke:#fff,stroke-width:2px,color:#fff
    classDef aiLayer fill:#9b59b6,stroke:#fff,stroke-width:2px,color:#fff
    classDef estimationLayer fill:#2ecc71,stroke:#fff,stroke-width:2px,color:#fff
    classDef coreLayer fill:#e74c3c,stroke:#fff,stroke-width:2px,color:#fff
    classDef databaseLayer fill:#34495e,stroke:#fff,stroke-width:2px,color:#fff

    %% User Interfaces
    subgraph UI["User Interfaces"]
        DirectCLI["cli/direct.py<br/>Direct pyFIA API calls<br/>- area(), biomass(), volume()<br/>- No SQL or AI needed<br/>- Rich formatting<br/>1210 lines"]:::userInterface
        AICLI["cli/ai_interface.py<br/>Natural language queries<br/>- SQL generation & execution<br/>- Multiple agent types<br/>- Query history & export<br/>773 lines"]:::userInterface
        Other["Other Interfaces<br/>- Jupyter notebooks<br/>- Python scripts<br/>- Direct imports"]:::userInterface
    end

    %% AI/ML Layer
    subgraph AI["AI/ML Layer"]
        BasicAgent["ai/agent.py<br/>Modern FIA Agent<br/>SQL generation<br/>1163 lines"]:::aiLayer
        ResultFormatter["ai/result_formatter.py<br/>Result formatting<br/>Table generation<br/>1205 lines"]:::aiLayer
        DomainKnowledge["ai/domain_knowledge.py<br/>FIA domain expertise<br/>547 lines"]:::aiLayer
    end

    %% Estimation Layer
    subgraph Estimation["Estimation Layer"]
        Area["estimation/area.py<br/>Land area<br/>552 lines"]:::estimationLayer
        AreaWorkflow["estimation/area_workflow.py<br/>Advanced area workflows<br/>869 lines"]:::estimationLayer
        Volume["estimation/volume.py<br/>Wood volume<br/>392 lines"]:::estimationLayer
        TPA["estimation/tpa.py<br/>Trees/acre<br/>510 lines"]:::estimationLayer
        Mortality["estimation/mortality.py<br/>Tree death<br/>~380 lines"]:::estimationLayer
        Utils["estimation/utils.py<br/>Common functions<br/>~350 lines"]:::estimationLayer
    end

    %% Core Layer
    subgraph Core["Core Layer"]
        FIACore["core/fia.py<br/>FIA class (main API)<br/>EVALID filtering<br/>Table management<br/>415 lines"]:::coreLayer
        DataReader["core/data_reader.py<br/>Database abstraction<br/>SQLite/DuckDB support<br/>~300 lines"]:::coreLayer
        QueryInterface["database/query_interface.py<br/>Query execution<br/>Schema inspection<br/>~250 lines"]:::coreLayer
    end

    %% Database Layer
    subgraph Database["Database Layer"]
        SQLite["SQLite Support<br/>- Legacy compatibility<br/>- Smaller datasets<br/>- Batch processing"]:::databaseLayer
        DuckDB["DuckDB Support<br/>- High performance<br/>- Large scale analysis<br/>- Better SQL support"]:::databaseLayer
    end

    %% Connections
    DirectCLI --> FIACore
    Other --> FIACore
    AICLI --> BasicAgent
    AICLI --> ResultFormatter
    AICLI --> DomainKnowledge

    BasicAgent --> QueryInterface
    BasicAgent --> DomainKnowledge
    ResultFormatter --> BasicAgent

    FIACore --> Area
    FIACore --> AreaWorkflow
    FIACore --> Volume
    FIACore --> TPA
    FIACore --> Mortality

    Area --> Utils
    AreaWorkflow --> Area
    Volume --> Utils
    TPA --> Utils
    Mortality --> Utils

    FIACore --> DataReader
    QueryInterface --> DataReader
    DataReader --> SQLite
    DataReader --> DuckDB
```

## Data Flow - Two Distinct Paths

### Path 1: Direct Programmatic Access

```mermaid
graph TD
    User[User Input] --> CLI[cli.py]
    CLI --> FIA[FIA Core<br/>- fia.area&#40;bySpecies=True&#41;<br/>- fia.biomass&#40;component='AG'&#41;<br/>- fia.volume&#40;volType='NET'&#41;]
    FIA --> Est[Estimation Modules<br/>- area.py, biomass.py, etc.<br/>- Direct statistical methods]
    Est --> DB[Database Layer]
    DB --> Results[Results & Formatting]

    style User fill:#f9f,stroke:#333,stroke-width:2px
    style Results fill:#9f9,stroke:#333,stroke-width:2px
```

### Path 2: AI-Enhanced Query Access

```mermaid
graph TD
    Query[User Query] --> CLIAI[cli_ai.py]
    CLIAI --> Agent[AI Agent]
    Agent --> SQL[SQL Query Generation]
    SQL --> QI[DuckDB Query Interface<br/>- Direct SQL execution<br/>- Schema inspection]
    QI --> DR[Data Reader<br/>- Database connection<br/>- Optimized queries]
    DR --> EstMod[Estimation Module<br/>- Domain filtering<br/>- Statistical calculations]
    EstMod --> Format[Results & Formatting<br/>- Tables, plots, exports]

    style Query fill:#f9f,stroke:#333,stroke-width:2px
    style Format fill:#9f9,stroke:#333,stroke-width:2px
```

## Code Duplication Analysis

```mermaid
graph LR
    subgraph "High Duplication (5x)"
        TF[Tree/Area Filtering<br/>_apply_tree_filters&#40;&#41;<br/>_apply_area_filters&#40;&#41;]
        style TF fill:#ff6b6b,stroke:#333,stroke-width:2px
    end

    subgraph "Medium Duplication (4x)"
        GL[Grouping Logic<br/>Size class creation<br/>Species grouping]
        ST[Stratification<br/>Plot-stratum-EU calculations<br/>Adjustment factors]
        style GL fill:#ffd93d,stroke:#333,stroke-width:2px
        style ST fill:#ffd93d,stroke:#333,stroke-width:2px
    end

    subgraph "Low Duplication (3x)"
        CH[CLI Connection Handling]
        DS[Database Schema Display]
        PI[Progress Indicators]
        style CH fill:#95e1d3,stroke:#333,stroke-width:2px
        style DS fill:#95e1d3,stroke:#333,stroke-width:2px
        style PI fill:#95e1d3,stroke:#333,stroke-width:2px
    end

    subgraph "Unique Code"
        CF[Core FIA logic]
        CI[Cognee integration]
        SF[Statistical formulas]
        style CF fill:#6bcf7f,stroke:#333,stroke-width:2px
        style CI fill:#6bcf7f,stroke:#333,stroke-width:2px
        style SF fill:#6bcf7f,stroke:#333,stroke-width:2px
    end
```

## Dependency Graph

```mermaid
graph TB
    %% External Dependencies
    subgraph External["External Dependencies"]
        Polars[Polars<br/>DataFrames]
        DuckDB[DuckDB<br/>Database]
        Rich[Rich<br/>CLI Formatting]
        LangChain[LangChain<br/>AI Framework]
        Cognee[Cognee<br/>Memory]
    end

    %% Direct Path
    subgraph DirectPath["Direct Path Dependencies"]
        Polars --> EstModules[Estimation<br/>Modules]
        DuckDB --> DataReaderDirect[data_reader.py]
        Rich --> CLIDirect[cli.py<br/>&#40;direct&#41;]
        DataReaderDirect --> CorePy[core.py]
        EstModules --> CorePy
        CorePy --> CLIDirect
    end

    %% AI Path
    subgraph AIPath["AI Path Dependencies"]
        DuckDB --> QueryInt[duckdb_query<br/>_interface.py]
        Rich --> CLIAI[cli_ai.py]
        LangChain --> AIAgents[AI agents<br/>ai_*.py]
        Cognee --> CogneeAgent[cognee_fia<br/>_agent.py]
        QueryInt --> CLIAI
        AIAgents --> CLIAI
        CogneeAgent --> CLIAI
    end

    %% Shared Core
    subgraph SharedCore["Shared Core"]
        EstUtils[estimation_utils.py<br/>Used by all estimation modules]
        CoreShared[core.py<br/>Used by cli.py and estimation modules]
        DataReaderShared[data_reader.py<br/>Used by core.py]
    end

    EstModules --> EstUtils
    CorePy --> CoreShared
    CoreShared --> DataReaderShared

    style Polars fill:#4a90e2,stroke:#fff,stroke-width:2px,color:#fff
    style DuckDB fill:#4a90e2,stroke:#fff,stroke-width:2px,color:#fff
    style Rich fill:#4a90e2,stroke:#fff,stroke-width:2px,color:#fff
    style LangChain fill:#4a90e2,stroke:#fff,stroke-width:2px,color:#fff
    style Cognee fill:#4a90e2,stroke:#fff,stroke-width:2px,color:#fff
```

## Module Size Analysis

```mermaid
pie title Module Size Distribution (Lines of Code)
    "cli/direct.py" : 1210
    "ai/result_formatter.py" : 1205
    "ai/agent.py" : 1163
    "estimation/area_workflow.py" : 869
    "cli/ai_interface.py" : 773
    "filters/domain.py" : 681
    "estimation/area.py" : 552
    "ai/domain_knowledge.py" : 547
    "estimation/tpa.py" : 510
    "core/fia.py" : 415
    "estimation/volume.py" : 392
    "Other modules" : 2883
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

### 3. Two-CLI Architecture Benefits

```mermaid
graph LR
    subgraph Benefits
        A[Clear Separation<br/>of Concerns]
        B[User Choice<br/>Direct vs AI]
        C[Reduced<br/>Complexity]
        D[Better<br/>Performance]
    end

    A --> E[cli.py: Pure pyFIA API<br/>cli_ai.py: AI/NL/SQL]
    B --> F[Power users: Direct CLI<br/>Exploratory: AI CLI]
    C --> G[No mixed modes<br/>Optimized for use case]
    D --> H[Direct avoids AI overhead<br/>AI can be sophisticated]

    style A fill:#2ecc71,stroke:#fff,stroke-width:2px,color:#fff
    style B fill:#3498db,stroke:#fff,stroke-width:2px,color:#fff
    style C fill:#e74c3c,stroke:#fff,stroke-width:2px,color:#fff
    style D fill:#f39c12,stroke:#fff,stroke-width:2px,color:#fff
```

### 4. Extract Common Patterns

- Move all filter functions to `estimation_utils`
- Create `GroupingMixin` for size/species grouping
- Create `StratificationMixin` for common calculations

## Summary

The pyFIA architecture implements a clean separation between:

1. **Direct API Access** - Fast, programmatic access to FIA statistical methods
2. **AI-Enhanced Access** - Natural language queries with SQL generation

This dual-path architecture allows users to choose the appropriate interface for their needs while maintaining code organization and performance.

**Total Lines**: ~10,200 (after removing deprecated files)
**Potential After Refactor**: ~8,000-8,500 (20% reduction)