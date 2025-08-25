"""
Type-safe data contracts for the complete FIA estimation workflow.

This module defines all data contracts used in the pyFIA pipeline framework,
providing type safety and validation for data flowing between pipeline steps.
The contracts ensure that each step receives and produces data in the expected
format, enabling early error detection and robust pipeline composition.

Contract Types:
- RawTables: Initial database table data
- FilteredData: Data after domain filtering
- JoinedData: Joined tables ready for value calculation  
- ValuedData: Data with calculated tree/plot values
- PlotEstimates: Plot-level aggregated estimates
- StratifiedEstimates: Estimates with expansion factors applied
- PopulationEstimates: Final population-level estimates
- FormattedOutput: Output ready for return to user

Each contract includes:
- Schema validation for required/optional columns
- Metadata about processing applied
- Type safety with Pydantic v2
- Integration with lazy evaluation framework
"""

import time
from typing import Any, Dict, List, Optional, Set, Union

import polars as pl
from pydantic import BaseModel, Field, ConfigDict

from ..lazy_evaluation import LazyFrameWrapper


class DataContract(BaseModel):
    """
    Base class for type-safe data contracts between pipeline steps.
    
    Data contracts define the structure and validation rules for data
    flowing between pipeline steps, ensuring type safety and data integrity.
    All contracts inherit from this base class and implement schema validation.
    """
    
    model_config = ConfigDict(
        validate_assignment=True,
        extra="forbid",
        arbitrary_types_allowed=True
    )
    
    # Metadata
    contract_version: str = Field(default="1.0", description="Contract version for compatibility tracking")
    created_at: float = Field(default_factory=time.time, description="Creation timestamp for debugging")
    step_id: Optional[str] = Field(default=None, description="ID of step that created this contract")
    processing_metadata: Dict[str, Any] = Field(default_factory=dict, description="Metadata about processing applied")
    
    def validate_schema(self, frame: Union[pl.DataFrame, pl.LazyFrame, LazyFrameWrapper]) -> None:
        """
        Validate that a frame matches the expected schema.
        
        This method checks that all required columns are present and validates
        the schema against the contract requirements. It works with Polars
        DataFrames, LazyFrames, and the pyFIA LazyFrameWrapper.
        
        Parameters
        ----------
        frame : Union[pl.DataFrame, pl.LazyFrame, LazyFrameWrapper]
            Frame to validate against this contract
            
        Raises
        ------
        DataContractViolation
            If the frame doesn't match the expected schema
        """
        from .core import DataContractViolation  # Avoid circular import
        
        # Extract the underlying frame for validation
        if isinstance(frame, LazyFrameWrapper):
            actual_frame = frame.frame
        else:
            actual_frame = frame
            
        # Get column names depending on frame type
        if isinstance(actual_frame, pl.LazyFrame):
            actual_cols = set(actual_frame.collect_schema().names())
        else:
            actual_cols = set(actual_frame.columns)
        
        # Validate required columns
        required_cols = self.get_required_columns()
        missing_cols = required_cols - actual_cols
        
        if missing_cols:
            raise DataContractViolation(
                f"Missing required columns in {self.__class__.__name__}: {missing_cols}. "
                f"Available columns: {actual_cols}",
                step_id=self.step_id
            )
        
        # Validate column types if schema validation is enabled
        if hasattr(self, "_validate_column_types") and self._validate_column_types:
            self._validate_column_datatypes(actual_frame)
    
    def get_required_columns(self) -> Set[str]:
        """
        Get the set of required columns for this contract.
        
        Subclasses should override this method to specify their required
        columns. The base implementation returns an empty set.
        
        Returns
        -------
        Set[str]
            Set of required column names
        """
        return set()
    
    def get_optional_columns(self) -> Set[str]:
        """
        Get the set of optional columns for this contract.
        
        Optional columns may be present but are not required for the
        contract to be valid. Subclasses can override this method.
        
        Returns
        -------
        Set[str]
            Set of optional column names
        """
        return set()
    
    def get_all_expected_columns(self) -> Set[str]:
        """
        Get all columns (required + optional) expected by this contract.
        
        Returns
        -------
        Set[str]
            Complete set of expected column names
        """
        return self.get_required_columns() | self.get_optional_columns()
    
    def _validate_column_datatypes(self, frame: Union[pl.DataFrame, pl.LazyFrame]) -> None:
        """
        Validate column data types (override in subclasses if needed).
        
        Parameters
        ----------
        frame : Union[pl.DataFrame, pl.LazyFrame]
            Frame to validate datatypes for
        """
        # Default implementation does no type checking
        # Subclasses can override for specific type validation
        pass
    
    def add_processing_metadata(self, key: str, value: Any) -> None:
        """
        Add metadata about processing applied to this data.
        
        Parameters
        ----------
        key : str
            Metadata key
        value : Any
            Metadata value
        """
        self.processing_metadata[key] = value
    
    def get_processing_metadata(self, key: str, default: Any = None) -> Any:
        """
        Get processing metadata.
        
        Parameters
        ----------
        key : str
            Metadata key
        default : Any
            Default value if key not found
            
        Returns
        -------
        Any
            Metadata value or default
        """
        return self.processing_metadata.get(key, default)


class RawTablesContract(DataContract):
    """
    Contract for raw table data loaded from the FIA database.
    
    This represents the initial state of data loaded from database tables
    before any filtering or processing has been applied. Tables are stored
    as LazyFrameWrapper objects to enable lazy evaluation.
    """
    
    tables: Dict[str, LazyFrameWrapper] = Field(description="Loaded tables by name (e.g., 'TREE', 'COND', 'PLOT')")
    evalid: Optional[Union[int, List[int]]] = Field(default=None, description="EVALID filter applied during loading")
    state_filter: Optional[Union[int, List[int]]] = Field(default=None, description="State code filter applied")
    
    def get_required_columns(self) -> Set[str]:
        """Raw tables don't have universal required columns."""
        return set()
    
    def get_table(self, table_name: str) -> LazyFrameWrapper:
        """
        Get a specific table from the loaded tables.
        
        Parameters
        ----------
        table_name : str
            Name of table to retrieve
            
        Returns
        -------
        LazyFrameWrapper
            The requested table
            
        Raises
        ------
        KeyError
            If table not found
        """
        if table_name not in self.tables:
            available = list(self.tables.keys())
            raise KeyError(f"Table '{table_name}' not found. Available tables: {available}")
        return self.tables[table_name]
    
    def has_table(self, table_name: str) -> bool:
        """Check if a table is available."""
        return table_name in self.tables


class FilteredDataContract(DataContract):
    """
    Contract for data after domain filtering has been applied.
    
    This represents the state of data after tree domain, area domain,
    and plot domain filters have been applied but before tables are
    joined together. Each data type is stored separately.
    """
    
    tree_data: Optional[LazyFrameWrapper] = Field(default=None, description="Filtered tree data (TREE table)")
    condition_data: LazyFrameWrapper = Field(description="Filtered condition data (COND table)")
    plot_data: Optional[LazyFrameWrapper] = Field(default=None, description="Filtered plot data (PLOT table)")
    seedling_data: Optional[LazyFrameWrapper] = Field(default=None, description="Filtered seedling data (SEEDLING table)")
    
    # Filter metadata for auditing and debugging
    tree_domain: Optional[str] = Field(default=None, description="Tree domain filter expression applied")
    area_domain: Optional[str] = Field(default=None, description="Area domain filter expression applied")
    plot_domain: Optional[str] = Field(default=None, description="Plot domain filter expression applied")
    
    # Record counts for validation
    records_filtered: Dict[str, Dict[str, int]] = Field(
        default_factory=dict, 
        description="Record counts: {table: {before: count, after: count}}"
    )
    
    def get_required_columns(self) -> Set[str]:
        """Condition data must have plot linking columns for joins."""
        return {"PLT_CN", "CONDID"}
    
    def get_optional_columns(self) -> Set[str]:
        """Optional columns that may be present after filtering."""
        return {"SUBP", "TREE", "STATUSCD", "SPCD", "DIA", "HT"}
    
    def add_filter_metadata(self, table_name: str, before_count: int, after_count: int) -> None:
        """Add record count metadata for a filtered table."""
        self.records_filtered[table_name] = {"before": before_count, "after": after_count}
    
    def get_filter_efficiency(self, table_name: str) -> Optional[float]:
        """Get filter efficiency (fraction of records retained) for a table."""
        if table_name not in self.records_filtered:
            return None
        
        counts = self.records_filtered[table_name]
        if counts["before"] == 0:
            return 0.0
        
        return counts["after"] / counts["before"]


class JoinedDataContract(DataContract):
    """
    Contract for data after tables have been joined together.
    
    This represents the complete dataset ready for value calculation,
    with all necessary tables joined and grouped appropriately.
    All required FIA identifiers and linking columns are present.
    """
    
    data: LazyFrameWrapper = Field(description="Complete joined estimation dataset")
    join_strategy: str = Field(default="standard", description="Join strategy used (standard, optimized, etc.)")
    group_columns: List[str] = Field(default_factory=list, description="Columns used for grouping analysis")
    
    # Join metadata
    join_performance: Dict[str, Any] = Field(default_factory=dict, description="Performance metrics for joins")
    tables_joined: List[str] = Field(default_factory=list, description="Names of tables that were joined")
    
    def get_required_columns(self) -> Set[str]:
        """Joined data must have core FIA identifiers for further processing."""
        return {"PLT_CN", "CONDID"}
    
    def get_optional_columns(self) -> Set[str]:
        """Common columns that may be present after joining."""
        return {
            "STATECD", "UNITCD", "COUNTYCD", "PLOT", "SUBP", "TREE",
            "SPCD", "STATUSCD", "DIA", "HT", "ACTUALHT",
            "CONDPROP_UNADJ", "COND_STATUS_CD", "OWNCD", "FORTYPCD"
        }


class ValuedDataContract(DataContract):
    """
    Contract for data with calculated tree/condition values.
    
    This represents data after value calculations (volume, biomass, TPA, etc.)
    have been applied at the tree or condition level. Values are ready for
    aggregation to plot level.
    """
    
    data: LazyFrameWrapper = Field(description="Data with calculated values added")
    value_columns: List[str] = Field(description="Names of calculated value columns")
    value_type: str = Field(description="Type of values calculated (volume, biomass, tpa, area, etc.)")
    group_columns: List[str] = Field(default_factory=list, description="Columns used for grouping")
    
    # Calculation metadata
    calculation_method: str = Field(default="standard", description="Calculation method used")
    equations_used: List[str] = Field(default_factory=list, description="Equations or models applied")
    
    def get_required_columns(self) -> Set[str]:
        """Must have plot identifier and at least one value column."""
        base_cols = {"PLT_CN"}
        if self.value_columns:
            base_cols.update(self.value_columns)
        return base_cols
    
    def get_optional_columns(self) -> Set[str]:
        """Common columns present with calculated values."""
        return {"CONDID", "SUBP", "TREE", "SPCD", "STATUSCD", "DIA", "HT"}
    
    def validate_value_columns(self) -> None:
        """Validate that all declared value columns are numeric types."""
        if hasattr(self.data, "frame"):
            # This is a more thorough validation that could be implemented
            # For now, we rely on the general schema validation
            pass


class PlotEstimatesContract(DataContract):
    """
    Contract for plot-level aggregated estimates.
    
    This represents estimates that have been aggregated from tree/condition
    level to plot level, but before stratification and expansion to population
    level. Each plot has estimates for the requested attributes.
    """
    
    data: LazyFrameWrapper = Field(description="Plot-level estimates dataset")
    estimate_columns: List[str] = Field(description="Names of estimate columns")
    estimate_type: str = Field(description="Type of estimates (volume, biomass, tpa, area, etc.)")
    group_columns: List[str] = Field(default_factory=list, description="Grouping columns used")
    
    # Aggregation metadata
    aggregation_method: str = Field(default="sum", description="Method used for aggregation (sum, mean, etc.)")
    plots_processed: int = Field(default=0, description="Number of plots processed")
    
    def get_required_columns(self) -> Set[str]:
        """Must have plot identifier and estimate columns."""
        base_cols = {"PLT_CN"}
        base_cols.update(self.estimate_columns)
        return base_cols
    
    def get_optional_columns(self) -> Set[str]:
        """Optional columns that may be present with plot estimates."""
        return {
            "STATECD", "UNITCD", "COUNTYCD", "PLOT", 
            "INVYR", "MEASYEAR", "CONDID"
        } | set(self.group_columns)


class StratifiedEstimatesContract(DataContract):
    """
    Contract for estimates with stratification and expansion factors applied.
    
    This represents plot-level estimates that have been assigned to strata
    and have expansion factors applied, ready for population-level calculation.
    """
    
    data: LazyFrameWrapper = Field(description="Stratified estimates with expansion factors")
    expansion_columns: List[str] = Field(description="Names of expansion factor columns")
    stratum_columns: List[str] = Field(description="Names of stratum identifier columns")
    estimate_columns: List[str] = Field(description="Names of estimate columns")
    
    # Stratification metadata
    stratification_method: str = Field(default="post", description="Stratification method (post, pre)")
    strata_count: int = Field(default=0, description="Number of strata identified")
    
    def get_required_columns(self) -> Set[str]:
        """Must have plot ID, stratum info, and expansion factors."""
        base_cols = {"PLT_CN"}
        base_cols.update(self.expansion_columns)
        base_cols.update(self.stratum_columns)
        base_cols.update(self.estimate_columns)
        return base_cols
    
    def get_optional_columns(self) -> Set[str]:
        """Optional columns for stratified estimates."""
        return {"STATECD", "INVYR", "ESTN_UNIT_CD", "STRATUM_CD", "P1POINTCNT", "P1PNTCNT_EU"}


class PopulationEstimatesContract(DataContract):
    """
    Contract for final population-level estimates with variance.
    
    This represents the final statistical estimates at the population level,
    including point estimates, variance estimates, and confidence intervals.
    This is the statistical output before formatting for user consumption.
    """
    
    data: LazyFrameWrapper = Field(description="Population estimates with statistics")
    estimate_columns: List[str] = Field(description="Names of population estimate columns")
    variance_columns: List[str] = Field(default_factory=list, description="Names of variance/SE columns")
    total_columns: List[str] = Field(default_factory=list, description="Names of total estimate columns")
    
    # Statistical metadata
    estimation_method: str = Field(default="post_stratified", description="Statistical estimation method")
    confidence_level: float = Field(default=0.68, description="Confidence level for intervals")
    degrees_of_freedom: Optional[int] = Field(default=None, description="Degrees of freedom for t-distribution")
    
    def get_required_columns(self) -> Set[str]:
        """Must have estimate columns at minimum."""
        return set(self.estimate_columns)
    
    def get_optional_columns(self) -> Set[str]:
        """Optional statistical columns."""
        return set(self.variance_columns) | set(self.total_columns) | {
            "NPLOTS_SAMPLED", "NPLOTS_TOTAL", "SAMPLING_ERROR", 
            "CONF_INT_LOWER", "CONF_INT_UPPER"
        }


class FormattedOutputContract(DataContract):
    """
    Contract for final formatted output ready for user consumption.
    
    This represents the final output that will be returned to the user,
    with appropriate formatting, column naming, and metadata included.
    This is a concrete DataFrame rather than lazy evaluation.
    """
    
    data: pl.DataFrame = Field(description="Final formatted output as concrete DataFrame")
    output_metadata: Dict[str, Any] = Field(default_factory=dict, description="Metadata about the analysis")
    
    # Format metadata
    format_version: str = Field(default="1.0", description="Output format version")
    column_descriptions: Dict[str, str] = Field(default_factory=dict, description="Descriptions of output columns")
    
    def get_required_columns(self) -> Set[str]:
        """Final output has no universal column requirements."""
        return set()
    
    def get_optional_columns(self) -> Set[str]:
        """All columns are optional in final output."""
        return set(self.data.columns) if hasattr(self.data, "columns") else set()
    
    def add_column_description(self, column: str, description: str) -> None:
        """Add a description for an output column."""
        self.column_descriptions[column] = description
    
    def get_column_description(self, column: str) -> Optional[str]:
        """Get description for a column."""
        return self.column_descriptions.get(column)
    
    def validate_final_output(self) -> List[str]:
        """
        Validate the final output for common issues.
        
        Returns
        -------
        List[str]
            List of validation warnings (empty if no issues)
        """
        warnings = []
        
        if self.data.is_empty():
            warnings.append("Output dataset is empty")
        
        # Check for null values in key columns
        null_counts = self.data.null_count()
        for col, null_count in zip(self.data.columns, null_counts.row(0)):
            if null_count > 0:
                warnings.append(f"Column '{col}' has {null_count} null values")
        
        return warnings


# Export all contract types
__all__ = [
    "DataContract",
    "RawTablesContract", 
    "FilteredDataContract",
    "JoinedDataContract",
    "ValuedDataContract",
    "PlotEstimatesContract",
    "StratifiedEstimatesContract",
    "PopulationEstimatesContract",
    "FormattedOutputContract",
]