"""
Consolidated configuration system for pyFIA estimation modules.

This module provides a streamlined configuration system that eliminates redundancy
and reduces configuration classes from 19 to 8 through composition and dynamic validation.
No backward compatibility maintained - clean slate design.
"""

from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Union
import warnings

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from pydantic_settings import BaseSettings


# === Enums for Configuration Options ===

class EstimationMethod(str, Enum):
    """FIA estimation methods."""
    TI = "TI"  # Temporally Indifferent
    SMA = "SMA"  # Simple Moving Average
    LMA = "LMA"  # Linear Moving Average
    EMA = "EMA"  # Exponential Moving Average
    ANNUAL = "ANNUAL"  # Annual estimates


class LandType(str, Enum):
    """Land type filters for FIA estimation."""
    FOREST = "forest"
    TIMBER = "timber"
    ALL = "all"


class TreeType(str, Enum):
    """Tree type filters for FIA estimation."""
    LIVE = "live"
    DEAD = "dead"
    GS = "gs"  # Growing Stock
    ALL = "all"


class VarianceMethod(str, Enum):
    """Variance calculation methods."""
    STANDARD = "standard"
    RATIO = "ratio"
    HYBRID = "hybrid"


class LazyEvaluationMode(str, Enum):
    """Lazy evaluation modes."""
    DISABLED = "disabled"  # Always eager
    ENABLED = "enabled"  # Always lazy
    AUTO = "auto"  # Automatic based on data size
    ADAPTIVE = "adaptive"  # Adaptive based on query complexity


# === FIA Column Validation (shared) ===

VALID_FIA_COLUMNS = {
    # Core identifiers
    "STATECD", "UNITCD", "COUNTYCD", "PLOT", "SUBP", "CONDID",
    # Species and tree attributes
    "SPCD", "SPGRPCD", "STATUSCD", "DIA", "HT", "ACTUALHT",
    # Ownership and land use
    "OWNCD", "OWNGRPCD", "LANDCLCD", "RESERVCD", "SITECLCD",
    # Forest type and stand attributes
    "FORTYPCD", "STDSZCD", "FLDSZCD", "PHYSCLCD",
    # Disturbance and mortality
    "AGENTCD", "DSTRBCD1", "DSTRBCD2", "DSTRBCD3",
    "SEVERITY1", "SEVERITY2", "SEVERITY3",
    "TREATCD1", "TREATCD2", "TREATCD3",
    # Temporal
    "INVYR", "MEASYEAR", "MEASMON", "MEASDAY",
    # Geographic
    "ECOSUBCD", "CONGCD", "MACROPLCD",
    # Growth accounting
    "RECONCILECD", "PREVDIA", "P2A_GRM_FLG",
    # Custom groupings that may be added
    "SIZE_CLASS", "BA_CLASS", "VOL_CLASS"
}


# === 1. SYSTEM CONFIGURATION ===

class PyFIASettings(BaseSettings):
    """
    Global pyFIA system settings.
    
    Consolidates system-wide settings from PyFIASettings and legacy Config class.
    Environment variables can override these settings using PYFIA_ prefix.
    """
    
    model_config = ConfigDict(
        env_prefix="PYFIA_",
        validate_assignment=True,
        extra="forbid"
    )
    
    # Database settings
    database_path: Path = Field(
        default=Path("fia.duckdb"),
        description="Path to FIA database"
    )
    database_engine: Literal["sqlite", "duckdb"] = Field(
        default="duckdb",
        description="Database engine type"
    )
    
    # Directory settings
    cache_dir: Path = Field(
        default_factory=lambda: Path.home() / ".pyfia" / "cache",
        description="Directory for cache storage"
    )
    log_dir: Path = Field(
        default_factory=lambda: Path.home() / ".pyfia" / "logs",
        description="Directory for log files"
    )
    
    # Global behavior
    type_check_on_load: bool = Field(
        default=False,
        description="Enable type checking when loading data"
    )
    cli_page_size: int = Field(
        default=20,
        ge=5,
        le=100,
        description="Number of rows per page in CLI output"
    )
    cli_max_width: int = Field(
        default=120,
        ge=80,
        le=300,
        description="Maximum width for CLI output"
    )
    
    @model_validator(mode="after")
    def create_directories(self) -> "PyFIASettings":
        """Create necessary directories if they don't exist."""
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        return self


# === 2. PERFORMANCE CONFIGURATION ===

class PerformanceConfig(BaseModel):
    """
    Unified performance and resource management configuration.
    
    Consolidates performance settings from LazyEvaluationConfig, ConverterConfig,
    and PyFIASettings into a single comprehensive configuration.
    """
    
    model_config = ConfigDict(
        validate_assignment=True,
        extra="forbid"
    )
    
    # Threading and parallelization
    max_threads: int = Field(
        default=4,
        ge=1,
        le=32,
        description="Maximum number of threads for parallel operations"
    )
    parallel_workers: int = Field(
        default=4,
        ge=1,
        le=16,
        description="Number of parallel workers for processing"
    )
    
    # Memory management
    memory_limit_mb: Optional[int] = Field(
        default=None,
        ge=100,
        description="Memory limit in MB (None = no limit)"
    )
    chunk_size: int = Field(
        default=50_000,
        ge=1000,
        description="Records per chunk for streaming operations"
    )
    batch_size: int = Field(
        default=100_000,
        ge=1000,
        description="Records per batch for batch operations"
    )
    
    # Lazy evaluation settings
    lazy_mode: LazyEvaluationMode = Field(
        default=LazyEvaluationMode.AUTO,
        description="Lazy evaluation mode"
    )
    lazy_threshold_rows: int = Field(
        default=10_000,
        ge=0,
        description="Row count threshold for automatic lazy evaluation"
    )
    collection_strategy: Literal["sequential", "parallel", "streaming", "adaptive"] = Field(
        default="adaptive",
        description="Strategy for collecting lazy frames"
    )
    
    # Query optimization flags
    enable_predicate_pushdown: bool = Field(
        default=True,
        description="Enable predicate pushdown optimization"
    )
    enable_projection_pushdown: bool = Field(
        default=True,
        description="Enable projection pushdown optimization"
    )
    enable_slice_pushdown: bool = Field(
        default=True,
        description="Enable slice pushdown optimization"
    )
    
    @model_validator(mode="after")
    def validate_memory_settings(self) -> "PerformanceConfig":
        """Validate memory-related settings are consistent."""
        if self.memory_limit_mb:
            # Estimate memory usage per chunk (rough heuristic: 1KB per row)
            estimated_chunk_mb = self.chunk_size / 1000
            estimated_batch_mb = self.batch_size / 1000
            
            if estimated_chunk_mb > self.memory_limit_mb / 2:
                warnings.warn(
                    f"Chunk size ({self.chunk_size}) may exceed memory limit "
                    f"({self.memory_limit_mb}MB). Consider reducing chunk_size.",
                    UserWarning
                )
            
            if estimated_batch_mb > self.memory_limit_mb:
                warnings.warn(
                    f"Batch size ({self.batch_size}) may exceed memory limit "
                    f"({self.memory_limit_mb}MB). Consider reducing batch_size.",
                    UserWarning
                )
        
        return self


# === 3. CACHE CONFIGURATION ===

class CacheConfig(BaseModel):
    """
    Unified caching configuration.
    
    Consolidates cache settings from CacheConfig, LazyEvaluationConfig,
    and PyFIASettings into a single comprehensive configuration.
    """
    
    model_config = ConfigDict(
        validate_assignment=True,
        extra="forbid"
    )
    
    # Memory cache settings
    memory_cache_enabled: bool = Field(
        default=True,
        description="Enable in-memory caching"
    )
    memory_cache_size_mb: int = Field(
        default=512,
        ge=0,
        description="Maximum memory cache size in MB"
    )
    memory_cache_ttl_seconds: int = Field(
        default=300,
        ge=0,
        description="Memory cache time-to-live in seconds (0 = no expiry)"
    )
    
    # Disk cache settings
    disk_cache_enabled: bool = Field(
        default=False,
        description="Enable disk-based caching"
    )
    disk_cache_size_gb: float = Field(
        default=1.0,
        ge=0.0,
        description="Maximum disk cache size in GB"
    )
    disk_cache_ttl_days: int = Field(
        default=7,
        ge=0,
        description="Disk cache time-to-live in days (0 = no expiry)"
    )
    
    # Expression cache (for lazy evaluation)
    expression_cache_enabled: bool = Field(
        default=True,
        description="Cache intermediate expression results"
    )
    
    @model_validator(mode="after")
    def validate_cache_settings(self) -> "CacheConfig":
        """Validate cache settings are reasonable."""
        if not self.memory_cache_enabled and not self.disk_cache_enabled:
            warnings.warn(
                "Both memory and disk caching are disabled. This may impact performance.",
                UserWarning
            )
        
        if self.memory_cache_size_mb > 2048:
            warnings.warn(
                f"Large memory cache size ({self.memory_cache_size_mb}MB) may impact system memory.",
                UserWarning
            )
        
        return self


# === 4. LOGGING CONFIGURATION ===

class LoggingConfig(BaseModel):
    """
    Unified logging and progress display configuration.
    
    Consolidates settings from ProgressConfig, ConverterConfig logging,
    and PyFIASettings logging into a single configuration.
    """
    
    model_config = ConfigDict(
        validate_assignment=True,
        extra="forbid"
    )
    
    # Logging settings
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = Field(
        default="INFO",
        description="Logging level"
    )
    log_to_file: bool = Field(
        default=False,
        description="Write logs to file"
    )
    log_format: str = Field(
        default="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        description="Log message format"
    )
    
    # Progress display settings
    show_progress: bool = Field(
        default=True,
        description="Show progress bars"
    )
    show_details: bool = Field(
        default=False,
        description="Show detailed progress information"
    )
    show_memory: bool = Field(
        default=False,
        description="Show memory usage in progress"
    )
    show_summary: bool = Field(
        default=True,
        description="Show operation summary after completion"
    )
    update_frequency: float = Field(
        default=0.1,
        ge=0.01,
        le=1.0,
        description="Progress update frequency in seconds"
    )


# === 5. MODULE PARAMETERS ===

class ModuleParameters(BaseModel):
    """
    Dynamic module-specific parameters with validation.
    
    Replaces all module-specific config classes (VolumeSpecificConfig,
    BiomassSpecificConfig, etc.) with a single flexible system.
    """
    
    model_config = ConfigDict(
        validate_assignment=True,
        extra="allow"  # Allow arbitrary parameters for flexibility
    )
    
    # Store parameters as a dictionary for maximum flexibility
    parameters: Dict[str, Any] = Field(
        default_factory=dict,
        description="Module-specific parameters"
    )
    
    def validate_for_module(self, module_name: str) -> None:
        """
        Validate parameters based on module requirements.
        
        Parameters
        ----------
        module_name : str
            Name of the module to validate for
            
        Raises
        ------
        ValueError
            If parameters are invalid for the module
        """
        if module_name == "volume":
            # Validate volume-specific parameters
            if "volume_equation" in self.parameters:
                valid_equations = ["default", "regional", "custom"]
                if self.parameters["volume_equation"] not in valid_equations:
                    raise ValueError(
                        f"Invalid volume_equation: {self.parameters['volume_equation']}. "
                        f"Must be one of {valid_equations}"
                    )
            
            if "merchantable_top_diameter" in self.parameters:
                if self.parameters["merchantable_top_diameter"] < 0:
                    raise ValueError("merchantable_top_diameter must be >= 0")
        
        elif module_name == "biomass":
            # Validate biomass-specific parameters
            if "component" in self.parameters:
                valid_components = ["aboveground", "belowground", "total", "merchantable"]
                if self.parameters["component"] not in valid_components:
                    raise ValueError(
                        f"Invalid component: {self.parameters['component']}. "
                        f"Must be one of {valid_components}"
                    )
            
            if "carbon_fraction" in self.parameters:
                cf = self.parameters["carbon_fraction"]
                if not 0.0 <= cf <= 1.0:
                    raise ValueError("carbon_fraction must be between 0.0 and 1.0")
        
        elif module_name == "growth":
            # Validate growth-specific parameters
            if "growth_type" in self.parameters:
                valid_types = ["net", "gross", "components"]
                if self.parameters["growth_type"] not in valid_types:
                    raise ValueError(
                        f"Invalid growth_type: {self.parameters['growth_type']}. "
                        f"Must be one of {valid_types}"
                    )
        
        elif module_name == "mortality":
            # Validate mortality-specific parameters
            if "mortality_type" in self.parameters:
                valid_types = ["tpa", "volume", "both"]
                if self.parameters["mortality_type"] not in valid_types:
                    raise ValueError(
                        f"Invalid mortality_type: {self.parameters['mortality_type']}. "
                        f"Must be one of {valid_types}"
                    )
            
            if "tree_class" in self.parameters:
                valid_classes = ["all", "timber", "growing_stock"]
                if self.parameters["tree_class"] not in valid_classes:
                    raise ValueError(
                        f"Invalid tree_class: {self.parameters['tree_class']}. "
                        f"Must be one of {valid_classes}"
                    )
        
        elif module_name == "area":
            # Validate area-specific parameters
            if "area_basis" in self.parameters:
                valid_basis = ["condition", "forest", "land"]
                if self.parameters["area_basis"] not in valid_basis:
                    raise ValueError(
                        f"Invalid area_basis: {self.parameters['area_basis']}. "
                        f"Must be one of {valid_basis}"
                    )
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get a parameter value with optional default."""
        return self.parameters.get(key, default)
    
    def set(self, key: str, value: Any) -> None:
        """Set a parameter value."""
        self.parameters[key] = value


# === 6. BASE ESTIMATOR CONFIGURATION ===

class EstimatorConfig(BaseModel):
    """
    Base configuration for all FIA estimators.
    
    Streamlined version that uses composition for performance, caching, and logging
    instead of inheritance or duplication.
    """
    
    model_config = ConfigDict(
        validate_assignment=True,
        extra="forbid",
        str_strip_whitespace=True
    )
    
    # Core FIA parameters
    grp_by: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="Column(s) to group estimates by"
    )
    by_species: bool = Field(
        default=False,
        description="Group by species code (SPCD)"
    )
    by_size_class: bool = Field(
        default=False,
        description="Group by diameter size classes"
    )
    
    # Domain filters
    land_type: LandType = Field(
        default=LandType.FOREST,
        description="Land type filter"
    )
    tree_type: TreeType = Field(
        default=TreeType.LIVE,
        description="Tree type filter"
    )
    tree_domain: Optional[str] = Field(
        default=None,
        description="SQL-like expression for tree filtering"
    )
    area_domain: Optional[str] = Field(
        default=None,
        description="SQL-like expression for area filtering"
    )
    plot_domain: Optional[str] = Field(
        default=None,
        description="SQL-like expression for plot filtering"
    )
    
    # Estimation parameters
    method: EstimationMethod = Field(
        default=EstimationMethod.TI,
        description="Estimation method"
    )
    lambda_: float = Field(
        default=0.5,
        ge=0.0,
        le=1.0,
        description="Temporal weighting for EMA"
    )
    
    # Output options
    totals: bool = Field(
        default=False,
        description="Include total estimates"
    )
    variance: bool = Field(
        default=False,
        description="Return variance instead of SE"
    )
    by_plot: bool = Field(
        default=False,
        description="Return plot-level estimates"
    )
    most_recent: bool = Field(
        default=False,
        description="Use only most recent evaluation"
    )
    variance_method: VarianceMethod = Field(
        default=VarianceMethod.RATIO,
        description="Variance calculation method"
    )
    
    # Composed configurations
    performance: PerformanceConfig = Field(
        default_factory=PerformanceConfig,
        description="Performance configuration"
    )
    caching: CacheConfig = Field(
        default_factory=CacheConfig,
        description="Cache configuration"
    )
    logging: LoggingConfig = Field(
        default_factory=LoggingConfig,
        description="Logging configuration"
    )
    
    @field_validator("grp_by")
    @classmethod
    def validate_grp_by(cls, v: Optional[Union[str, List[str]]]) -> Optional[Union[str, List[str]]]:
        """Validate grouping columns against known FIA columns."""
        if v is None:
            return v
        
        columns = [v] if isinstance(v, str) else v
        invalid_cols = [col for col in columns if col not in VALID_FIA_COLUMNS]
        
        if invalid_cols:
            warnings.warn(
                f"Unknown grouping columns: {invalid_cols}. May be valid derived columns.",
                UserWarning
            )
        
        return v
    
    @field_validator("tree_domain", "area_domain", "plot_domain")
    @classmethod
    def validate_domain_expression(cls, v: Optional[str]) -> Optional[str]:
        """Validate domain expressions for SQL injection."""
        if v is None:
            return v
        
        # Clean whitespace
        v = " ".join(v.split())
        
        # Check for dangerous SQL patterns
        dangerous = ["DROP", "DELETE", "INSERT", "UPDATE", "ALTER", "CREATE", "EXEC", "--", "/*"]
        v_upper = v.upper()
        for pattern in dangerous:
            if pattern in v_upper:
                raise ValueError(f"Domain expression contains forbidden keyword: {pattern}")
        
        return v
    
    def get_grouping_columns(self) -> List[str]:
        """Get all grouping columns based on configuration."""
        columns = []
        
        if self.grp_by:
            if isinstance(self.grp_by, str):
                columns.append(self.grp_by)
            else:
                columns.extend(self.grp_by)
        
        if self.by_species:
            columns.append("SPCD")
        
        # Remove duplicates while preserving order
        seen = set()
        return [x for x in columns if not (x in seen or seen.add(x))]


# === 7. MODULE ESTIMATOR CONFIGURATION ===

class ModuleEstimatorConfig(EstimatorConfig):
    """
    Configuration for any FIA estimation module.
    
    Replaces all module-specific config classes (VolumeConfig, BiomassConfig, etc.)
    with a single unified configuration that uses dynamic parameters.
    """
    
    # Module identification
    module_name: str = Field(
        description="Name of the estimation module"
    )
    
    # Module-specific parameters
    module_parameters: ModuleParameters = Field(
        default_factory=ModuleParameters,
        description="Module-specific parameters"
    )
    
    @field_validator("module_name")
    @classmethod
    def validate_module_name(cls, v: str) -> str:
        """Validate module name is recognized."""
        valid_modules = {"area", "volume", "biomass", "growth", "mortality", "tpa"}
        if v not in valid_modules:
            raise ValueError(f"Unknown module: {v}. Valid modules: {valid_modules}")
        return v
    
    @model_validator(mode="after")
    def validate_module_combination(self) -> "ModuleEstimatorConfig":
        """Validate module-specific parameter combinations."""
        # Validate parameters for the specific module
        self.module_parameters.validate_for_module(self.module_name)
        
        # Module-specific validation of base parameters
        if self.module_name == "mortality":
            if self.tree_type == TreeType.LIVE:
                raise ValueError(
                    "Mortality estimation requires tree_type='dead' or 'all', not 'live'"
                )
        
        elif self.module_name == "growth":
            if self.method not in [EstimationMethod.TI, EstimationMethod.ANNUAL]:
                warnings.warn(
                    f"Growth estimation typically uses TI or ANNUAL methods, not {self.method}",
                    UserWarning
                )
        
        elif self.module_name == "area":
            if self.tree_domain:
                warnings.warn(
                    "Area estimation with tree_domain may not produce expected results. "
                    "Consider using area_domain instead.",
                    UserWarning
                )
        
        return self
    
    def get_module_param(self, key: str, default: Any = None) -> Any:
        """Get a module-specific parameter value."""
        return self.module_parameters.get(key, default)
    
    def set_module_param(self, key: str, value: Any) -> None:
        """Set a module-specific parameter value."""
        self.module_parameters.set(key, value)
        # Re-validate after setting
        self.module_parameters.validate_for_module(self.module_name)


# === 8. CONVERTER CONFIGURATION ===

class ConverterConfig(BaseSettings):
    """
    Streamlined configuration for database conversion operations.
    
    Uses composition for performance and logging settings instead of duplication.
    """
    
    model_config = ConfigDict(
        env_prefix="PYFIA_CONVERTER_",
        validate_assignment=True,
        extra="forbid"
    )
    
    # File paths
    source_dir: Path = Field(
        default=Path("."),
        description="Directory containing source SQLite files"
    )
    target_path: Path = Field(
        default=Path("fia.duckdb"),
        description="Path to target DuckDB database"
    )
    temp_dir: Optional[Path] = Field(
        default=None,
        description="Directory for temporary files"
    )
    
    # Conversion settings
    compression_level: Literal["none", "low", "medium", "high"] = Field(
        default="medium",
        description="DuckDB compression level"
    )
    validation_level: Literal["none", "basic", "standard", "comprehensive"] = Field(
        default="standard",
        description="Data validation level"
    )
    create_indexes: bool = Field(
        default=True,
        description="Create database indexes"
    )
    optimize_storage: bool = Field(
        default=True,
        description="Optimize storage layout"
    )
    
    # Append mode settings
    append_mode: bool = Field(
        default=False,
        description="Append to existing database"
    )
    dedupe_on_append: bool = Field(
        default=False,
        description="Remove duplicates when appending"
    )
    dedupe_keys: List[str] = Field(
        default_factory=lambda: ["CN"],
        description="Columns to use for deduplication"
    )
    
    # State filtering
    include_states: Optional[List[int]] = Field(
        default=None,
        description="State FIPS codes to include"
    )
    exclude_states: Optional[List[int]] = Field(
        default=None,
        description="State FIPS codes to exclude"
    )
    
    # Table filtering
    include_tables: Optional[List[str]] = Field(
        default=None,
        description="Tables to include"
    )
    exclude_tables: Optional[List[str]] = Field(
        default=None,
        description="Tables to exclude"
    )
    
    # Composed configurations
    performance: PerformanceConfig = Field(
        default_factory=PerformanceConfig,
        description="Performance configuration"
    )
    logging: LoggingConfig = Field(
        default_factory=LoggingConfig,
        description="Logging configuration"
    )
    
    @model_validator(mode="after")
    def validate_settings(self) -> "ConverterConfig":
        """Validate converter settings."""
        if self.include_states and self.exclude_states:
            raise ValueError("Cannot specify both include_states and exclude_states")
        
        if self.include_tables and self.exclude_tables:
            raise ValueError("Cannot specify both include_tables and exclude_tables")
        
        if self.dedupe_on_append and not self.append_mode:
            warnings.warn(
                "dedupe_on_append=True has no effect when append_mode=False",
                UserWarning
            )
        
        return self


# === Configuration Factory ===

class ConfigFactory:
    """
    Factory for creating appropriate configuration objects.
    
    Simplified factory that creates ModuleEstimatorConfig for all modules
    with appropriate default parameters.
    """
    
    # Default module parameters for each module type
    MODULE_DEFAULTS = {
        "volume": {
            "volume_equation": "default",
            "include_sound": True,
            "include_rotten": False,
            "merchantable_top_diameter": 4.0,
            "stump_height": 1.0
        },
        "biomass": {
            "component": "aboveground",
            "include_foliage": True,
            "include_saplings": False,
            "carbon_fraction": 0.5,
            "units": "tons"
        },
        "growth": {
            "growth_type": "net",
            "include_ingrowth": True,
            "include_mortality": True,
            "include_removals": True,
            "annual_only": False
        },
        "mortality": {
            "mortality_type": "tpa",
            "tree_class": "all",
            "include_components": False,
            "include_natural": True,
            "include_harvest": True
        },
        "area": {
            "area_basis": "condition",
            "include_nonforest": False,
            "include_water": False,
            "ownership_groups": None
        },
        "tpa": {}  # TPA has no specific parameters
    }
    
    @classmethod
    def create_config(
        cls,
        module: str,
        **kwargs
    ) -> ModuleEstimatorConfig:
        """
        Create appropriate configuration for a module.
        
        Parameters
        ----------
        module : str
            Module name (e.g., "volume", "biomass", "mortality")
        **kwargs
            Configuration parameters
            
        Returns
        -------
        ModuleEstimatorConfig
            Configuration for the module with defaults applied
        """
        # Extract module-specific parameters from kwargs
        module_params = kwargs.pop("module_parameters", {})
        
        # Apply module defaults
        if module in cls.MODULE_DEFAULTS:
            defaults = cls.MODULE_DEFAULTS[module].copy()
            defaults.update(module_params)
            module_params = defaults
        
        # Create module parameters object
        params = ModuleParameters(parameters=module_params)
        
        # Create and return config
        return ModuleEstimatorConfig(
            module_name=module,
            module_parameters=params,
            **kwargs
        )


# === Export Public API ===

__all__ = [
    # Settings
    "PyFIASettings",
    
    # Component configurations  
    "PerformanceConfig",
    "CacheConfig",
    "LoggingConfig",
    
    # Estimator configurations
    "EstimatorConfig",
    "ModuleEstimatorConfig",
    "ModuleParameters",
    
    # Converter configuration
    "ConverterConfig",
    
    # Factory
    "ConfigFactory",
    
    # Enums
    "EstimationMethod",
    "LandType",
    "TreeType",
    "VarianceMethod",
    "LazyEvaluationMode",
    
    # Constants
    "VALID_FIA_COLUMNS",
]