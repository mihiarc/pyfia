# pyFIA Architecture

## What is pyFIA?

pyFIA is a Python library for analyzing USDA Forest Inventory and Analysis (FIA) data. It provides:
- **Statistical estimation functions** for forest metrics (area, volume, biomass, etc.)
- **Two usage paths**: Direct Python API or Natural Language AI interface
- **High performance** using DuckDB and Polars
- **Proper FIA methodology** with EVALID-based statistical validity

## Two Ways to Use pyFIA

```mermaid
graph TB
    subgraph "User Entry Points"
        U1[Python Scripts/<br/>Notebooks]
        U2[Command Line]
    end
    
    subgraph "Usage Paths"
        Direct[Direct API Path<br/>Statistical Functions]
        AI[AI Agent Path<br/>Natural Language]
    end
    
    U1 --> Direct
    U2 --> Direct
    U2 --> AI
    
    Direct --> R1[Statistical Results<br/>DataFrames]
    AI --> R2[Formatted Results<br/>Tables & Explanations]
    
    style Direct fill:#2ecc71
    style AI fill:#9b59b6
```

### Path 1: Direct API (Green Path)
- Import pyFIA functions directly
- Call estimation functions with parameters
- Get back Polars DataFrames with results
- Full control over analysis

### Path 2: AI Agent (Purple Path)
- Ask questions in natural language
- Agent converts to appropriate queries
- Get formatted, explained results
- Interactive exploration

## Core Architecture

```mermaid
graph TB
    %% Entry Points
    subgraph "Entry Layer"
        PY[Python API<br/>import pyfia]
        CLI1[pyfia CLI<br/>Direct Functions]
        CLI2[pyfia-ai CLI<br/>Natural Language]
    end
    
    %% Core Components
    subgraph "Core Layer"
        FIA[FIA Class<br/>Database Connection<br/>EVALID Management]
        DR[Data Reader<br/>DuckDB Interface]
    end
    
    %% Processing
    subgraph "Processing Layer"
        EST[Estimation Functions<br/>area, volume, biomass<br/>tpa, mortality, growth]
        FILT[Filters<br/>Domain, EVALID<br/>Grouping, Joins]
        UTILS[Utilities<br/>Statistical Calculations<br/>Stratification]
    end
    
    %% AI Components
    subgraph "AI Layer"
        AGENT[FIA Agent<br/>Query Understanding]
        TOOLS[Agent Tools<br/>SQL, Schema, Species]
        FORMAT[Result Formatter<br/>Rich Output]
    end
    
    %% Data
    subgraph "Data Layer"
        DB[(DuckDB<br/>FIA Database)]
    end
    
    %% Direct Path
    PY --> FIA
    CLI1 --> FIA
    FIA --> EST
    EST --> FILT
    EST --> UTILS
    FIA --> DR
    DR --> DB
    
    %% AI Path  
    CLI2 --> AGENT
    AGENT --> TOOLS
    TOOLS --> DR
    AGENT --> FORMAT
    
    style FIA fill:#e74c3c
    style EST fill:#2ecc71
    style AGENT fill:#9b59b6
    style DB fill:#34495e
```

## Data Flow

### Direct API Flow
```mermaid
sequenceDiagram
    participant User
    participant pyFIA
    participant FIA Class
    participant Estimator
    participant Database
    
    User->>pyFIA: area(db, evalid=372301)
    pyFIA->>FIA Class: Get filtered data
    FIA Class->>Database: Query with EVALID
    Database-->>FIA Class: Plot/Condition data
    FIA Class-->>pyFIA: Filtered DataFrames
    pyFIA->>Estimator: Calculate estimates
    Estimator-->>pyFIA: Results with SE
    pyFIA-->>User: DataFrame with estimates
```

### AI Agent Flow
```mermaid
sequenceDiagram
    participant User
    participant Agent
    participant Tools
    participant Database
    participant Formatter
    
    User->>Agent: "How many oak trees in NC?"
    Agent->>Agent: Understand query
    Agent->>Tools: find_species_codes("oak")
    Tools-->>Agent: Oak species codes
    Agent->>Tools: execute_query(SQL)
    Tools->>Database: Run query
    Database-->>Tools: Raw results
    Tools-->>Agent: Query results
    Agent->>Formatter: Format with context
    Formatter-->>Agent: Rich formatted output
    Agent-->>User: Explained results
```

## Key Components

### Core Components

| Component | Purpose | Key Functions |
|-----------|---------|---------------|
| **FIA Class** | Main interface to database | `clipFIA()`, `readFIA()`, `findEvalid()` |
| **Data Reader** | Database abstraction | Handles DuckDB connections and queries |
| **Settings** | Configuration management | Database paths, default options |

### Estimation Functions

| Function | Calculates | Key Features |
|----------|------------|--------------|
| `area()` | Forest land area | By forest type, ownership, size class |
| `biomass()` | Tree biomass | Above/below ground, carbon content |
| `volume()` | Wood volume | Net/gross, merch/sound, board feet |
| `tpa()` | Trees per acre | By species, size, status |
| `mortality()` | Annual mortality | Trees, volume, biomass |
| `growth()` | Annual growth | Net growth accounting for mortality |

### Filter System

| Filter Type | Purpose | Example |
|-------------|---------|---------|
| **EVALID** | Statistical validity | Only use data from one evaluation |
| **Domain** | Tree/area filtering | "DIA >= 5", "OWNGRPCD == 10" |
| **Grouping** | Result aggregation | By species, size class, ownership |
| **Classification** | Tree categorization | Live/dead, growing stock |

### AI Components

| Component | Purpose | Key Features |
|-----------|---------|--------------|
| **Agent** | Natural language processing | LangGraph ReAct pattern |
| **Tools** | Agent capabilities | SQL execution, schema lookup |
| **Formatter** | Result presentation | Rich tables, statistics, explanations |
| **Domain Knowledge** | FIA expertise | Species codes, terminology |

## Design Principles

### 1. Statistical Validity First
- **EVALID-based filtering** ensures proper population estimates
- All estimators follow FIA statistical methodology
- Standard errors and confidence intervals included

### 2. Performance Optimized
- **DuckDB** for fast analytical queries
- **Polars** for efficient data manipulation
- Lazy evaluation where possible

### 3. Two Clear Paths
- **Direct API** for programmatic control
- **AI Agent** for exploration and learning
- No mixing of concerns between paths

### 4. Modular Design
- Estimation functions are independent
- Filters can be composed
- Easy to add new estimators

### 5. User Friendly
- Consistent function signatures
- Clear parameter names
- Rich documentation and examples

## File Organization

```
src/pyfia/
├── core/           # Database connection, EVALID management
├── estimation/     # Statistical estimation functions
├── filters/        # Data filtering and processing
├── ai/            # AI agent components
├── cli/           # Command-line interfaces
├── database/      # Database utilities and schema
├── models/        # Data models (Pydantic)
└── locations/     # Geographic parsing utilities
```

## Key Concepts

### EVALID System
The heart of FIA's statistical design:
- Groups plots into valid populations
- Ensures proper expansion factors
- Links to specific time periods
- Required for all population estimates

### Stratification
FIA uses post-stratified estimation:
1. Plots assigned to strata
2. Strata have expansion factors
3. Estimates calculated by stratum
4. Combined for population totals

### Dual Interface Design
- **Direct path**: Maximum control, pure functions
- **AI path**: Natural language, guided exploration
- Clean separation prevents complexity

This architecture provides a solid foundation for forest inventory analysis while remaining accessible to both programmers and domain experts.