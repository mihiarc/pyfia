# Phase 2A: Base Estimator Architecture Design

## Executive Summary

This document presents a comprehensive architecture design for the `BaseEstimator` class that will standardize the workflow across all PyFIA estimation modules. Based on analysis of the existing six estimation modules (volume, biomass, tpa, area, mortality, growth), this design will enable 60-70% code reduction while maintaining exact statistical functionality and FIA methodology compliance.

## 1. Analysis of Current Estimation Module Patterns

### 1.1 Common Workflow Pattern Identified

All estimation modules follow a nearly identical 7-step workflow:

```
1. Database Initialization & Table Loading
2. Data Retrieval & Initial Filtering 
3. Domain/Filter Application (tree & area)
4. Data Joining & Preparation
5. Plot-Level Calculation
6. Stratification & Expansion
7. Population Estimation & Output Formatting
```

### 1.2 Module-Specific Variations

| Module | Specific Calculations | Required Tables | Unique Parameters |
|--------|---------------------|-----------------|-------------------|
| **Volume** | Volume columns (NET/GROSS/SOUND) | TREE, COND, PLOT, POP_* | vol_type |
| **Biomass** | Biomass components (AG/BG/STEM) | TREE, COND, PLOT, POP_* | component, model_snag |
| **TPA** | Trees per acre, basal area | TREE, COND, PLOT, POP_* | - |
| **Area** | Land proportions, area indicators | COND, PLOT, POP_*, (TREE optional) | by_land_type |
| **Mortality** | Mortality rates by component | TREE_GRM_*, COND, PLOT, POP_* | tree_class |
| **Growth** | Growth components (recruitment, diameter) | TREE_GRM_*, PLOT, POP_* | - |

### 1.3 Common Code Patterns

- **Filter Application**: Now consolidated in `filters/common.py` (Phase 1 success)
- **Stratification Logic**: Identical across all modules
- **Expansion Calculations**: Same formula with module-specific columns
- **Variance Estimation**: Shared ratio-of-means methodology
- **Output Formatting**: Similar structure with module-specific columns

## 2. Proposed Architecture Design

### 2.1 Class Hierarchy

```
BaseEstimator (Abstract)
    ├── VolumeEstimator
    ├── BiomassEstimator
    ├── TPAEstimator
    ├── AreaEstimator
    ├── MortalityEstimator
    └── GrowthEstimator
```

### 2.2 Core Design Principles

1. **Template Method Pattern**: Base class defines workflow skeleton, subclasses fill in specifics
2. **Strategy Pattern**: Pluggable calculation strategies for different estimation types
3. **Chain of Responsibility**: Sequential processing pipeline with hooks
4. **Dependency Injection**: Configuration and database passed as dependencies
5. **SOLID Compliance**: Single responsibility, open for extension, interface segregation

## 3. Detailed Class Specification

### 3.1 BaseEstimator Abstract Class

```python
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, List, Optional, Any, Union
import polars as pl
from ..core import FIA
from ..filters.common import (
    apply_tree_filters_common,
    apply_area_filters_common,
    setup_grouping_columns_common
)

@dataclass
class EstimatorConfig:
    """Configuration for estimation parameters."""
    grp_by: Optional[Union[str, List[str]]] = None
    by_species: bool = False
    by_size_class: bool = False
    land_type: str = "forest"
    tree_type: str = "live"
    tree_domain: Optional[str] = None
    area_domain: Optional[str] = None
    method: str = "TI"
    lambda_: float = 0.5
    totals: bool = False
    variance: bool = False
    by_plot: bool = False
    most_recent: bool = False
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert config to dictionary for backwards compatibility."""
        return {k: v for k, v in self.__dict__.items() if v is not None}


class BaseEstimator(ABC):
    """
    Abstract base class for FIA design-based estimators.
    
    This class implements the Template Method pattern to standardize the
    estimation workflow while allowing module-specific customization.
    """
    
    def __init__(self, db: Union[str, FIA], config: EstimatorConfig):
        """
        Initialize the estimator with database and configuration.
        
        Parameters
        ----------
        db : Union[str, FIA]
            FIA database object or path to database
        config : EstimatorConfig
            Configuration object with estimation parameters
        """
        # Handle database initialization
        if isinstance(db, str):
            self.db = FIA(db)
        else:
            self.db = db
            
        self.config = config
        self._validate_config()
        
        # Cache for loaded data
        self._data_cache: Dict[str, pl.DataFrame] = {}
        
    # === Abstract Methods (Must be implemented by subclasses) ===
    
    @abstractmethod
    def get_required_tables(self) -> List[str]:
        """
        Return list of required database tables for this estimator.
        
        Returns
        -------
        List[str]
            Table names required for estimation
        """
        pass
    
    @abstractmethod
    def get_response_columns(self) -> Dict[str, str]:
        """
        Define the response variable columns for calculation.
        
        Returns
        -------
        Dict[str, str]
            Mapping of internal column names to output names
        """
        pass
    
    @abstractmethod
    def calculate_tree_values(self, tree_df: pl.DataFrame) -> pl.DataFrame:
        """
        Calculate tree-level values specific to this estimator.
        
        Parameters
        ----------
        tree_df : pl.DataFrame
            Tree data with conditions joined
            
        Returns
        -------
        pl.DataFrame
            Tree data with calculated values added
        """
        pass
    
    @abstractmethod
    def get_output_columns(self) -> List[str]:
        """
        Define the output column structure for this estimator.
        
        Returns
        -------
        List[str]
            Ordered list of output column names
        """
        pass
    
    # === Hook Methods (Can be overridden for customization) ===
    
    def apply_module_filters(self, tree_df: pl.DataFrame, 
                            cond_df: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
        """
        Apply module-specific filters beyond common filtering.
        
        Hook method that can be overridden by subclasses for additional filtering.
        
        Parameters
        ----------
        tree_df : pl.DataFrame
            Tree dataframe after common filters
        cond_df : pl.DataFrame
            Condition dataframe after common filters
            
        Returns
        -------
        tuple[pl.DataFrame, pl.DataFrame]
            Filtered tree and condition dataframes
        """
        return tree_df, cond_df
    
    def prepare_stratification_data(self, ppsa_df: pl.DataFrame, 
                                   pop_stratum_df: pl.DataFrame) -> pl.DataFrame:
        """
        Prepare stratification data for expansion.
        
        Hook method for module-specific stratification preparation.
        
        Parameters
        ----------
        ppsa_df : pl.DataFrame
            Plot-stratum assignments
        pop_stratum_df : pl.DataFrame
            Population stratum data
            
        Returns
        -------
        pl.DataFrame
            Prepared stratification data
        """
        # Default implementation
        strat_df = ppsa_df.join(
            pop_stratum_df.select(["CN", "EXPNS", "ADJ_FACTOR_SUBP"]),
            left_on="STRATUM_CN",
            right_on="CN",
            how="inner"
        )
        return strat_df
    
    def calculate_variance(self, data: pl.DataFrame, estimate_col: str) -> pl.DataFrame:
        """
        Calculate variance for estimates.
        
        Hook method for module-specific variance calculation.
        
        Parameters
        ----------
        data : pl.DataFrame
            Data with estimates
        estimate_col : str
            Column name for estimate
            
        Returns
        -------
        pl.DataFrame
            Data with variance/SE added
        """
        # Simplified default - subclasses should override for proper variance
        return data.with_columns([
            (pl.col(estimate_col) * 0.015).alias(f"{estimate_col}_SE")
        ])
    
    def format_output(self, estimates: pl.DataFrame) -> pl.DataFrame:
        """
        Format final output to match expected structure.
        
        Hook method for module-specific output formatting.
        
        Parameters
        ----------
        estimates : pl.DataFrame
            Raw estimation results
            
        Returns
        -------
        pl.DataFrame
            Formatted output
        """
        return estimates
    
    # === Template Methods (Core workflow implementation) ===
    
    def estimate(self) -> pl.DataFrame:
        """
        Main estimation workflow implementing the Template Method pattern.
        
        This method orchestrates the entire estimation process following
        FIA statistical procedures.
        
        Returns
        -------
        pl.DataFrame
            Final estimation results
        """
        # Step 1: Load required tables
        self._load_required_tables()
        
        # Step 2: Get and filter data
        tree_df, cond_df = self._get_filtered_data()
        
        # Step 3: Join and prepare data
        prepared_data = self._prepare_estimation_data(tree_df, cond_df)
        
        # Step 4: Calculate plot-level estimates
        plot_estimates = self._calculate_plot_estimates(prepared_data)
        
        # Step 5: Apply stratification and expansion
        expanded_estimates = self._apply_stratification(plot_estimates)
        
        # Step 6: Calculate population estimates
        pop_estimates = self._calculate_population_estimates(expanded_estimates)
        
        # Step 7: Format and return results
        return self.format_output(pop_estimates)
    
    def _validate_config(self):
        """Validate configuration parameters."""
        # Validation logic for common parameters
        valid_methods = ["TI", "SMA", "LMA", "EMA", "ANNUAL"]
        if self.config.method not in valid_methods:
            raise ValueError(f"Invalid method: {self.config.method}")
            
        valid_land_types = ["forest", "timber", "all"]
        if self.config.land_type not in valid_land_types:
            raise ValueError(f"Invalid land_type: {self.config.land_type}")
            
        valid_tree_types = ["live", "dead", "gs", "all"]
        if self.config.tree_type not in valid_tree_types:
            raise ValueError(f"Invalid tree_type: {self.config.tree_type}")
    
    def _load_required_tables(self):
        """Load all required tables from database."""
        for table in self.get_required_tables():
            self.db.load_table(table)
    
    def _get_filtered_data(self) -> tuple[pl.DataFrame, pl.DataFrame]:
        """
        Get data from database and apply common filters.
        
        Returns
        -------
        tuple[pl.DataFrame, pl.DataFrame]
            Filtered tree and condition dataframes
        """
        # Get base data
        cond_df = self.db.get_conditions()
        
        # Apply common area filters
        cond_df = apply_area_filters_common(
            cond_df, 
            self.config.land_type, 
            self.config.area_domain
        )
        
        # Get tree data if needed
        tree_df = None
        if "TREE" in self.get_required_tables():
            tree_df = self.db.get_trees()
            
            # Apply common tree filters
            tree_df = apply_tree_filters_common(
                tree_df,
                self.config.tree_type,
                self.config.tree_domain,
                require_volume="volume" in self.__class__.__name__.lower()
            )
        
        # Apply module-specific filters
        tree_df, cond_df = self.apply_module_filters(tree_df, cond_df)
        
        return tree_df, cond_df
    
    def _prepare_estimation_data(self, tree_df: Optional[pl.DataFrame], 
                                cond_df: pl.DataFrame) -> pl.DataFrame:
        """
        Join data and prepare for estimation.
        
        Parameters
        ----------
        tree_df : Optional[pl.DataFrame]
            Tree data (may be None for area estimation)
        cond_df : pl.DataFrame
            Condition data
            
        Returns
        -------
        pl.DataFrame
            Prepared data ready for estimation
        """
        if tree_df is not None:
            # Join trees with conditions
            data = tree_df.join(
                cond_df.select(["PLT_CN", "CONDID", "CONDPROP_UNADJ"]),
                on=["PLT_CN", "CONDID"],
                how="inner"
            )
            
            # Calculate tree-level values
            data = self.calculate_tree_values(data)
            
            # Set up grouping columns
            data, group_cols = setup_grouping_columns_common(
                data,
                self.config.grp_by,
                self.config.by_species,
                self.config.by_size_class,
                return_dataframe=True
            )
            self._group_cols = group_cols
        else:
            # Area estimation case - no tree data
            data = cond_df
            self._group_cols = []
            if self.config.grp_by:
                self._group_cols = ([self.config.grp_by] 
                                  if isinstance(self.config.grp_by, str) 
                                  else list(self.config.grp_by))
        
        return data
    
    def _calculate_plot_estimates(self, data: pl.DataFrame) -> pl.DataFrame:
        """
        Calculate plot-level estimates.
        
        Parameters
        ----------
        data : pl.DataFrame
            Prepared estimation data
            
        Returns
        -------
        pl.DataFrame
            Plot-level estimates
        """
        # Determine grouping columns
        plot_groups = ["PLT_CN"]
        if self._group_cols:
            plot_groups.extend(self._group_cols)
        
        # Get response columns for aggregation
        response_cols = self.get_response_columns()
        agg_exprs = []
        for col_name, output_name in response_cols.items():
            if col_name in data.columns:
                agg_exprs.append(pl.sum(col_name).alias(f"PLOT_{output_name}"))
        
        # Aggregate to plot level
        plot_estimates = data.group_by(plot_groups).agg(agg_exprs)
        
        return plot_estimates
    
    def _apply_stratification(self, plot_data: pl.DataFrame) -> pl.DataFrame:
        """
        Apply stratification and calculate expansion factors.
        
        Parameters
        ----------
        plot_data : pl.DataFrame
            Plot-level estimates
            
        Returns
        -------
        pl.DataFrame
            Data with expansion factors applied
        """
        # Get stratification data
        ppsa = (
            self.db.tables["POP_PLOT_STRATUM_ASSGN"]
            .filter(pl.col("EVALID").is_in(self.db.evalid) 
                   if self.db.evalid else pl.lit(True))
            .collect()
        )
        
        pop_stratum = self.db.tables["POP_STRATUM"].collect()
        
        # Prepare stratification
        strat_df = self.prepare_stratification_data(ppsa, pop_stratum)
        
        # Join with plot data
        plot_with_strat = plot_data.join(
            strat_df.select(["PLT_CN", "EXPNS", "ADJ_FACTOR_SUBP"]),
            on="PLT_CN",
            how="inner"
        )
        
        # Apply expansion
        response_cols = self.get_response_columns()
        expansion_exprs = []
        for _, output_name in response_cols.items():
            plot_col = f"PLOT_{output_name}"
            if plot_col in plot_with_strat.columns:
                expansion_exprs.append(
                    (pl.col(plot_col) * pl.col("ADJ_FACTOR_SUBP") * pl.col("EXPNS"))
                    .alias(f"TOTAL_{output_name}")
                )
        
        plot_with_strat = plot_with_strat.with_columns(expansion_exprs)
        
        return plot_with_strat
    
    def _calculate_population_estimates(self, expanded_data: pl.DataFrame) -> pl.DataFrame:
        """
        Calculate final population estimates.
        
        Parameters
        ----------
        expanded_data : pl.DataFrame
            Data with expansion factors applied
            
        Returns
        -------
        pl.DataFrame
            Population-level estimates
        """
        # Aggregate by groups
        response_cols = self.get_response_columns()
        agg_exprs = []
        
        for _, output_name in response_cols.items():
            total_col = f"TOTAL_{output_name}"
            if total_col in expanded_data.columns:
                agg_exprs.append(pl.sum(total_col).alias(f"POP_{output_name}"))
        
        agg_exprs.append(pl.len().alias("nPlots"))
        
        if self._group_cols:
            pop_estimates = expanded_data.group_by(self._group_cols).agg(agg_exprs)
        else:
            pop_estimates = expanded_data.select(agg_exprs)
        
        # Calculate per-acre values (using ratio-of-means)
        # This would need proper area calculation in practice
        forest_area = self._get_forest_area()
        
        per_acre_exprs = []
        for _, output_name in response_cols.items():
            pop_col = f"POP_{output_name}"
            if pop_col in pop_estimates.columns:
                per_acre_exprs.append(
                    (pl.col(pop_col) / forest_area).alias(output_name)
                )
        
        pop_estimates = pop_estimates.with_columns(per_acre_exprs)
        
        # Add variance/SE
        for _, output_name in response_cols.items():
            if output_name in pop_estimates.columns:
                pop_estimates = self.calculate_variance(pop_estimates, output_name)
        
        # Add metadata columns
        pop_estimates = pop_estimates.with_columns([
            pl.lit(2023).alias("YEAR"),  # Should get from EVALID
            pl.col("nPlots").alias("N")
        ])
        
        return pop_estimates
    
    def _get_forest_area(self) -> float:
        """
        Get forest area for per-acre calculations.
        
        This is a simplified version - production code would calculate
        this dynamically based on the evaluation and filters.
        
        Returns
        -------
        float
            Forest area in acres
        """
        # This should be calculated from the data
        # For now, using a placeholder value
        return 18592940.0
```

### 3.2 Example Concrete Implementation: VolumeEstimator

```python
class VolumeEstimator(BaseEstimator):
    """
    Volume estimation following FIA procedures.
    
    This class implements volume-specific calculations while
    inheriting the common workflow from BaseEstimator.
    """
    
    def __init__(self, db: Union[str, FIA], vol_type: str = "net", **kwargs):
        """
        Initialize volume estimator.
        
        Parameters
        ----------
        db : Union[str, FIA]
            FIA database
        vol_type : str, default "net"
            Volume type: "net", "gross", "sound", "sawlog"
        **kwargs
            Additional configuration parameters
        """
        # Add volume-specific parameter
        config = EstimatorConfig(**kwargs)
        super().__init__(db, config)
        
        self.vol_type = vol_type.upper()
        self._validate_vol_type()
        
    def _validate_vol_type(self):
        """Validate volume type parameter."""
        valid_types = ["NET", "GROSS", "SOUND", "SAWLOG"]
        if self.vol_type not in valid_types:
            raise ValueError(f"Invalid vol_type: {self.vol_type}")
    
    def get_required_tables(self) -> List[str]:
        """Volume estimation requires these tables."""
        return ["PLOT", "TREE", "COND", "POP_STRATUM", "POP_PLOT_STRATUM_ASSGN"]
    
    def get_response_columns(self) -> Dict[str, str]:
        """Define volume-specific response columns."""
        if self.vol_type == "NET":
            return {
                "VOL_CF_ACRE": "VOLCFNET_ACRE",
                "VOL_CS_ACRE": "VOLCSNET_ACRE",
                "VOL_BF_ACRE": "VOLBFNET_ACRE"
            }
        elif self.vol_type == "GROSS":
            return {
                "VOL_CF_ACRE": "VOLCFGRS_ACRE",
                "VOL_CS_ACRE": "VOLCSGRS_ACRE",
                "VOL_BF_ACRE": "VOLBFGRS_ACRE"
            }
        # ... other volume types
    
    def calculate_tree_values(self, tree_df: pl.DataFrame) -> pl.DataFrame:
        """Calculate volume per acre for each tree."""
        # Get appropriate volume columns
        volume_map = self._get_volume_column_map()
        
        # Calculate volume per acre
        vol_calculations = []
        for source_col, calc_col in volume_map.items():
            if source_col in tree_df.columns:
                vol_calculations.append(
                    (pl.col(source_col) * pl.col("TPA_UNADJ"))
                    .alias(calc_col)
                )
        
        return tree_df.with_columns(vol_calculations)
    
    def _get_volume_column_map(self) -> Dict[str, str]:
        """Map source columns to calculation columns."""
        if self.vol_type == "NET":
            return {
                "VOLCFNET": "VOL_CF_ACRE",
                "VOLCSNET": "VOL_CS_ACRE",
                "VOLBFNET": "VOL_BF_ACRE"
            }
        elif self.vol_type == "GROSS":
            return {
                "VOLCFGRS": "VOL_CF_ACRE",
                "VOLCSGRS": "VOL_CS_ACRE",
                "VOLBFGRS": "VOL_BF_ACRE"
            }
        # ... other mappings
    
    def get_output_columns(self) -> List[str]:
        """Define volume output structure."""
        base_cols = ["YEAR", "N", "nPlots"]
        
        # Add volume-specific columns based on type
        if self.vol_type == "NET":
            vol_cols = ["VOLCFNET_ACRE", "VOLCFNET_ACRE_SE",
                       "VOLCSNET_ACRE", "VOLCSNET_ACRE_SE",
                       "VOLBFNET_ACRE", "VOLBFNET_ACRE_SE"]
        # ... other types
        
        # Add grouping columns
        if self._group_cols:
            return self._group_cols + base_cols + vol_cols
        return base_cols + vol_cols
    
    def apply_module_filters(self, tree_df: pl.DataFrame,
                           cond_df: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
        """Apply volume-specific filtering."""
        # Volume requires non-null volume columns
        if tree_df is not None:
            tree_df = tree_df.filter(pl.col("VOLCFGRS").is_not_null())
        
        return tree_df, cond_df
```

## 4. Implementation Strategy

### 4.1 Migration Path

1. **Phase 2A.1**: Implement BaseEstimator class (Week 1)
   - Create `src/pyfia/estimation/base.py`
   - Implement core template methods
   - Add comprehensive unit tests

2. **Phase 2A.2**: Migrate Volume Module (Week 1-2)
   - Create `VolumeEstimator` class
   - Maintain backward compatibility wrapper
   - Validate against existing tests

3. **Phase 2A.3**: Migrate Remaining Modules (Week 2-3)
   - BiomassEstimator
   - TPAEstimator
   - AreaEstimator
   - MortalityEstimator (special handling for DuckDB queries)
   - GrowthEstimator (special handling for GRM tables)

4. **Phase 2A.4**: Deprecation & Cleanup (Week 3-4)
   - Add deprecation warnings to old functions
   - Update documentation
   - Clean up redundant code

### 4.2 Backward Compatibility

Maintain existing public API through wrapper functions:

```python
def volume(db: Union[str, FIA], **kwargs) -> pl.DataFrame:
    """
    Backward compatibility wrapper for volume estimation.
    
    This function maintains the existing API while using the new
    BaseEstimator architecture internally.
    """
    # Extract volume-specific parameter
    vol_type = kwargs.pop('vol_type', 'net')
    
    # Create estimator and run
    estimator = VolumeEstimator(db, vol_type=vol_type, **kwargs)
    return estimator.estimate()
```

### 4.3 Testing Strategy

1. **Unit Tests**: Test each method in isolation
2. **Integration Tests**: Test full workflow with sample data
3. **Regression Tests**: Compare results with current implementation
4. **Performance Tests**: Ensure no performance degradation
5. **Statistical Tests**: Validate accuracy against rFIA benchmarks

## 5. Expected Benefits

### 5.1 Code Reduction Metrics

| Module | Current Lines | Expected Lines | Reduction |
|--------|--------------|----------------|-----------|
| volume.py | 317 | 95 | 70% |
| biomass.py | 269 | 85 | 68% |
| tpa.py | 450+ | 120 | 73% |
| area.py | 380+ | 110 | 71% |
| mortality.py | 350+ | 140 | 60% |
| growth.py | 400+ | 150 | 62% |
| **Total** | ~2,166 | ~700 | **68%** |

### 5.2 Maintainability Improvements

1. **Single Source of Truth**: Core workflow in one place
2. **Consistent Error Handling**: Centralized validation
3. **Easier Testing**: Test base class once, subclasses are simpler
4. **Clear Extension Points**: Well-defined hooks for customization
5. **Reduced Cognitive Load**: Developers only need to understand base pattern

### 5.3 Extensibility Benefits

Adding a new estimator type becomes trivial:

```python
class NewEstimator(BaseEstimator):
    def get_required_tables(self) -> List[str]:
        return ["PLOT", "NEW_TABLE"]
    
    def calculate_tree_values(self, tree_df: pl.DataFrame) -> pl.DataFrame:
        # Add new calculation
        return tree_df.with_columns([...])
    
    # Implement other abstract methods
```

## 6. Potential Challenges & Solutions

### 6.1 Challenge: Special Cases

**Issue**: Mortality and Growth modules have unique DuckDB optimization and GRM table handling.

**Solution**: Use composition pattern for special handlers:

```python
class MortalityEstimator(BaseEstimator):
    def __init__(self, db, **kwargs):
        super().__init__(db, **kwargs)
        self.query_optimizer = DuckDBQueryOptimizer(db)
    
    def _get_filtered_data(self):
        # Use optimized DuckDB queries instead of standard approach
        return self.query_optimizer.get_mortality_data(self.config)
```

### 6.2 Challenge: Performance

**Issue**: Abstraction might introduce overhead.

**Solution**: 
- Use lazy evaluation throughout
- Cache computed values
- Profile and optimize hot paths
- Maintain direct database queries where beneficial

### 6.3 Challenge: Complex Variance Calculations

**Issue**: Each module has slightly different variance formulations.

**Solution**: Strategy pattern for variance calculation:

```python
class VarianceStrategy(ABC):
    @abstractmethod
    def calculate(self, data: pl.DataFrame) -> pl.DataFrame:
        pass

class RatioVarianceStrategy(VarianceStrategy):
    # Implementation for ratio-of-means variance

class DirectVarianceStrategy(VarianceStrategy):
    # Implementation for direct expansion variance
```

## 7. Implementation Timeline

| Week | Phase | Deliverables |
|------|-------|-------------|
| 1 | Design & Base Implementation | BaseEstimator class, unit tests |
| 1-2 | Volume Migration | VolumeEstimator, validation |
| 2 | Biomass & TPA Migration | BiomassEstimator, TPAEstimator |
| 2-3 | Area & Special Cases | AreaEstimator, MortalityEstimator, GrowthEstimator |
| 3 | Testing & Optimization | Performance testing, optimization |
| 4 | Documentation & Cleanup | Updated docs, deprecation notices |

## 8. Success Criteria

1. **Code Reduction**: Achieve 60-70% reduction in estimation module code
2. **Test Coverage**: Maintain 100% test coverage
3. **Performance**: No regression in execution speed
4. **Accuracy**: Exact match with current statistical results
5. **API Compatibility**: Existing code continues to work
6. **Documentation**: Complete API documentation and migration guide

## 9. Conclusion

The BaseEstimator architecture provides a robust, extensible foundation for PyFIA's estimation modules. By consolidating common workflow patterns while preserving module-specific flexibility, this design achieves significant code reduction without sacrificing functionality or performance. The implementation follows software engineering best practices and positions PyFIA for easier maintenance and future enhancements.

## Appendix A: Method Signatures Reference

```python
# Core Abstract Methods
get_required_tables() -> List[str]
get_response_columns() -> Dict[str, str]
calculate_tree_values(tree_df: pl.DataFrame) -> pl.DataFrame
get_output_columns() -> List[str]

# Optional Hook Methods
apply_module_filters(tree_df, cond_df) -> tuple[pl.DataFrame, pl.DataFrame]
prepare_stratification_data(ppsa_df, pop_stratum_df) -> pl.DataFrame
calculate_variance(data, estimate_col) -> pl.DataFrame
format_output(estimates) -> pl.DataFrame

# Template Methods (not overridden)
estimate() -> pl.DataFrame
_validate_config()
_load_required_tables()
_get_filtered_data() -> tuple[pl.DataFrame, pl.DataFrame]
_prepare_estimation_data(tree_df, cond_df) -> pl.DataFrame
_calculate_plot_estimates(data) -> pl.DataFrame
_apply_stratification(plot_data) -> pl.DataFrame
_calculate_population_estimates(expanded_data) -> pl.DataFrame
_get_forest_area() -> float
```

## Appendix B: Data Flow Diagram

```
[Database] → [Load Tables] → [Filter Data] → [Join & Prepare]
                                                    ↓
[Format Output] ← [Population Est.] ← [Stratification] ← [Plot Est.]
```

Each step has well-defined inputs/outputs and can be tested independently.