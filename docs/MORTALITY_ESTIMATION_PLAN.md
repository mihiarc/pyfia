# Mortality Estimation Enhancement Plan

## Overview
This document outlines the implementation plan for enhancing pyFIA's mortality estimation capabilities to support detailed grouping and variance calculations, matching the functionality of our SQL-based mortality query.

## Current State
- Basic mortality estimation exists in `src/pyfia/estimation/mortality.py`
- Missing support for detailed grouping variables
- Variance calculations are incomplete
- Database interface is in transition

## Requirements
The enhanced mortality estimation should support:

1. **Grouping Variables**
   - Species (SPCD)
   - Species Group (SPGRPCD)
   - Ownership Group (OWNGRPCD)
   - Unit Code (UNITCD)
   - Mortality Agent (AGENTCD)
   - Disturbance Codes (DSTRBCD1, DSTRBCD2, DSTRBCD3)

2. **Statistical Calculations**
   - Mortality estimates (TPA and volume)
   - Standard errors
   - Variance components
   - Plot counts

3. **Data Filtering**
   - Tree class filtering
   - Land type filtering
   - Domain-specific filtering
   - Evaluation period selection

## Implementation Plan

### 1. Database Interface (`src/pyfia/database/`)
```python
class QueryInterface:
    """Base class for database interactions."""
    def execute_query(self, query: str) -> pl.DataFrame:
        """Execute a query and return results as a Polars DataFrame."""
        pass

class DuckDBInterface(QueryInterface):
    """DuckDB-specific implementation."""
    def __init__(self, db_path: str):
        self.conn = duckdb.connect(db_path)
    
    def execute_query(self, query: str) -> pl.DataFrame:
        return self.conn.execute(query).pl()

class SQLiteInterface(QueryInterface):
    """SQLite-specific implementation."""
    def __init__(self, db_path: str):
        self.conn = sqlite3.connect(db_path)
    
    def execute_query(self, query: str) -> pl.DataFrame:
        return pl.read_sql(query, self.conn)
```

### 2. Mortality Calculator (`src/pyfia/estimation/mortality/`)

#### a. Base Calculator
```python
class MortalityCalculator:
    """Base class for mortality calculations."""
    
    def __init__(self, db: FIA, config: EstimatorConfig):
        self.db = db
        self.config = config
        self.query_interface = self._get_interface()
    
    def calculate(self) -> pl.DataFrame:
        """Main calculation method."""
        plot_data = self._get_plot_data()
        stratum_data = self._calculate_stratum_estimates(plot_data)
        return self._calculate_population_estimates(stratum_data)
```

#### b. Variance Calculator
```python
class VarianceCalculator:
    """Handles variance calculations for mortality estimates."""
    
    def calculate_stratum_variance(self, data: pl.DataFrame) -> pl.DataFrame:
        """Calculate variance at stratum level."""
        pass
    
    def calculate_population_variance(self, data: pl.DataFrame) -> pl.DataFrame:
        """Calculate population-level variance."""
        pass
```

#### c. Group Handler
```python
class GroupHandler:
    """Manages grouping operations for mortality estimates."""
    
    def apply_grouping(self, data: pl.DataFrame, groups: List[str]) -> pl.DataFrame:
        """Apply grouping with proper aggregations."""
        pass
    
    def validate_groups(self, groups: List[str]) -> None:
        """Validate grouping variables."""
        pass
```

### 3. Query Builder (`src/pyfia/estimation/mortality/query_builder.py`)
```python
class MortalityQueryBuilder:
    """Builds SQL queries for mortality estimation."""
    
    def build_plot_query(self, groups: List[str]) -> str:
        """Build query for plot-level estimates."""
        pass
    
    def build_stratum_query(self, groups: List[str]) -> str:
        """Build query for stratum-level estimates."""
        pass
    
    def build_population_query(self, groups: List[str]) -> str:
        """Build query for population-level estimates."""
        pass
```

### 4. Configuration (`src/pyfia/estimation/config.py`)
```python
@dataclass
class MortalityConfig:
    """Configuration for mortality estimation."""
    group_vars: List[str]
    tree_class: str = "all"
    land_type: str = "forest"
    tree_domain: Optional[str] = None
    area_domain: Optional[str] = None
    include_variance: bool = True
    include_totals: bool = True
```

## Implementation Steps

1. **Database Interface (2 days)**
   - Implement QueryInterface base class
   - Add DuckDB and SQLite implementations
   - Add connection management and error handling

2. **Core Mortality Calculator (3 days)**
   - Implement base calculator structure
   - Add plot-level calculations
   - Add stratum-level calculations
   - Add population-level calculations

3. **Variance Calculations (2 days)**
   - Implement stratum variance calculations
   - Add population variance calculations
   - Add standard error calculations

4. **Group Handling (2 days)**
   - Implement group validation
   - Add group-specific aggregations
   - Handle missing values and edge cases

5. **Query Building (2 days)**
   - Implement query builder
   - Add support for all grouping variables
   - Optimize query performance

6. **Testing and Validation (3 days)**
   - Add unit tests
   - Add integration tests
   - Validate against SQL results
   - Performance testing

## Usage Example

```python
from pyfia import FIA
from pyfia.estimation.mortality import MortalityEstimator
from pyfia.estimation.config import MortalityConfig

# Initialize FIA database
db = FIA("fia.duckdb")

# Configure mortality estimation
config = MortalityConfig(
    group_vars=["SPCD", "OWNGRPCD", "AGENTCD"],
    tree_class="all",
    land_type="forest",
    include_variance=True
)

# Create estimator
estimator = MortalityEstimator(db, config)

# Calculate mortality
results = estimator.estimate()
```

## Validation Plan

1. **Unit Testing**
   - Test each component in isolation
   - Test edge cases and error conditions
   - Test grouping combinations

2. **Integration Testing**
   - Test full estimation workflow
   - Compare with SQL query results
   - Validate variance calculations

3. **Performance Testing**
   - Test with large datasets
   - Measure memory usage
   - Profile query execution

## Dependencies
- polars >= 0.19.0
- duckdb >= 0.9.0
- pydantic >= 2.0.0
- numpy >= 1.24.0

## Timeline
- Total Development Time: 14 days
- Testing and Validation: 3 days
- Documentation: 2 days
- Total Project Time: 19 days

## Future Enhancements
1. Support for additional grouping variables
2. Advanced filtering options
3. Custom variance calculation methods
4. Parallel processing for large datasets
5. Cache management for intermediate results
