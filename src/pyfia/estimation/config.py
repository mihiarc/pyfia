"""
Configuration system for pyFIA estimation modules.

This module provides a modern configuration system that:
1. Uses Pydantic v2 for validation and type safety
2. Supports module-specific parameters without class proliferation
3. Includes lazy evaluation settings for performance optimization
4. Provides comprehensive validation for FIA-specific parameters
"""

from enum import Enum
from typing import Any, Dict, Generic, List, Literal, Optional, TypeVar, Union
import warnings

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


# === FIA Column Validation ===

VALID_FIA_GROUPING_COLUMNS = {
    # Core identifiers
    "STATECD", "UNITCD", "COUNTYCD", "PLOT", "SUBP", "CONDID",
    
    # Species and tree attributes
    "SPCD", "SPGRPCD", "STATUSCD", "DIA", "HT", "ACTUALHT",
    
    # Ownership and land use
    "OWNCD", "OWNGRPCD", "LANDCLCD", "RESERVCD", "SITECLCD",
    
    # Forest type and stand attributes
    "FORTYPCD", "STDSZCD", "FLDSZCD", "PHYSCLCD", "DSTRBCD1", "DSTRBCD2", "DSTRBCD3",
    
    # Disturbance and mortality
    "AGENTCD", "SEVERITY1", "SEVERITY2", "SEVERITY3", "TREATCD1", "TREATCD2", "TREATCD3",
    
    # Temporal
    "INVYR", "MEASYEAR", "MEASMON", "MEASDAY",
    
    # Geographic
    "ECOSUBCD", "CONGCD", "MACROPLCD",
    
    # Growth accounting
    "RECONCILECD", "PREVDIA", "P2A_GRM_FLG",
    
    # Custom groupings that may be added
    "SIZE_CLASS", "BA_CLASS", "VOL_CLASS"
}


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


# === Lazy Evaluation Configuration ===

class LazyEvaluationConfig(BaseModel):
    """
    Configuration for lazy evaluation behavior.
    
    This configuration controls how and when lazy evaluation is used
    in estimators, including thresholds, strategies, and performance tuning.
    """
    
    model_config = ConfigDict(
        validate_assignment=True,
        extra="forbid"
    )
    
    # Core lazy settings
    mode: LazyEvaluationMode = Field(
        default=LazyEvaluationMode.AUTO,
        description="Lazy evaluation mode"
    )
    
    threshold_rows: int = Field(
        default=10_000,
        ge=0,
        description="Row count threshold for automatic lazy evaluation"
    )
    
    # Collection strategies
    collection_strategy: Literal["sequential", "parallel", "streaming", "adaptive"] = Field(
        default="adaptive",
        description="Strategy for collecting lazy frames"
    )
    
    max_parallel_collections: int = Field(
        default=4,
        ge=1,
        le=16,
        description="Maximum parallel collections when using parallel strategy"
    )
    
    # Memory management
    memory_limit_mb: Optional[int] = Field(
        default=None,
        ge=100,
        description="Memory limit in MB for lazy operations"
    )
    
    chunk_size: int = Field(
        default=50_000,
        ge=1000,
        description="Chunk size for streaming operations"
    )
    
    # Optimization settings
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
    
    # Caching
    enable_expression_caching: bool = Field(
        default=True,
        description="Cache intermediate expressions"
    )
    
    cache_ttl_seconds: int = Field(
        default=300,
        ge=0,
        description="Cache time-to-live in seconds (0 = no expiry)"
    )
    
    @model_validator(mode="after")
    def validate_memory_settings(self) -> "LazyEvaluationConfig":
        """Validate memory-related settings."""
        if self.memory_limit_mb:
            # Check if chunk size might be too large (rough heuristic)
            # Assume each row is about 1KB on average
            estimated_chunk_mb = self.chunk_size / 1000
            if estimated_chunk_mb > self.memory_limit_mb / 2:
                warnings.warn(
                    f"Chunk size ({self.chunk_size}) may be too large for "
                    f"memory limit ({self.memory_limit_mb}MB). Consider reducing chunk_size.",
                    UserWarning
                )
        return self


# === Module-Specific Configuration Classes ===

class VolumeSpecificConfig(BaseModel):
    """Volume estimation specific configuration."""
    
    model_config = ConfigDict(validate_assignment=True, extra="forbid")
    
    volume_equation: Literal["default", "regional", "custom"] = Field(
        default="default",
        description="Volume equation to use"
    )
    
    include_sound: bool = Field(
        default=True,
        description="Include sound volume"
    )
    
    include_rotten: bool = Field(
        default=False,
        description="Include rotten/missing volume"
    )
    
    merchantable_top_diameter: float = Field(
        default=4.0,
        ge=0.0,
        description="Merchantable top diameter in inches"
    )
    
    stump_height: float = Field(
        default=1.0,
        ge=0.0,
        description="Stump height in feet"
    )


class BiomassSpecificConfig(BaseModel):
    """Biomass estimation specific configuration."""
    
    model_config = ConfigDict(validate_assignment=True, extra="forbid")
    
    component: Literal["aboveground", "belowground", "total", "merchantable"] = Field(
        default="aboveground",
        description="Biomass component to estimate"
    )
    
    include_foliage: bool = Field(
        default=True,
        description="Include foliage in biomass estimates"
    )
    
    include_saplings: bool = Field(
        default=False,
        description="Include saplings in biomass estimates"
    )
    
    carbon_fraction: float = Field(
        default=0.5,
        ge=0.0,
        le=1.0,
        description="Carbon fraction of biomass"
    )
    
    units: Literal["tons", "kg", "lbs"] = Field(
        default="tons",
        description="Output units for biomass"
    )


class GrowthSpecificConfig(BaseModel):
    """Growth estimation specific configuration."""
    
    model_config = ConfigDict(validate_assignment=True, extra="forbid")
    
    growth_type: Literal["net", "gross", "components"] = Field(
        default="net",
        description="Type of growth to calculate"
    )
    
    include_ingrowth: bool = Field(
        default=True,
        description="Include ingrowth in calculations"
    )
    
    include_mortality: bool = Field(
        default=True,
        description="Include mortality in net growth"
    )
    
    include_removals: bool = Field(
        default=True,
        description="Include removals in net growth"
    )
    
    annual_only: bool = Field(
        default=False,
        description="Calculate annual growth only"
    )


class AreaSpecificConfig(BaseModel):
    """Area estimation specific configuration."""
    
    model_config = ConfigDict(validate_assignment=True, extra="forbid")
    
    area_basis: Literal["condition", "forest", "land"] = Field(
        default="condition",
        description="Basis for area estimation"
    )
    
    include_nonforest: bool = Field(
        default=False,
        description="Include non-forest land in estimates"
    )
    
    include_water: bool = Field(
        default=False,
        description="Include water areas"
    )
    
    ownership_groups: Optional[List[int]] = Field(
        default=None,
        description="Ownership group codes to include"
    )


# Type variable for module-specific configs
TModuleConfig = TypeVar("TModuleConfig", bound=BaseModel)


# === Base Configuration ===

class EstimatorConfig(BaseModel):
    """
    Configuration for FIA estimation parameters using Pydantic v2.
    
    This configuration class provides validation and type safety for all
    common estimation parameters across pyFIA modules.
    """
    
    model_config = ConfigDict(
        validate_assignment=True,
        extra="forbid",
        str_strip_whitespace=True,
        use_enum_values=True
    )
    
    # Grouping parameters
    grp_by: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="Column(s) to group estimates by. Must be valid FIA columns."
    )
    by_species: bool = Field(
        default=False,
        description="Whether to group by species code (SPCD)"
    )
    by_size_class: bool = Field(
        default=False,
        description="Whether to group by diameter size classes"
    )
    
    # Domain filters
    land_type: Union[str, LandType] = Field(
        default="forest",
        description="Land type filter for estimation"
    )
    tree_type: Union[str, TreeType] = Field(
        default="live",
        description="Tree type filter: live, dead, growing stock (gs), or all"
    )
    tree_domain: Optional[str] = Field(
        default=None,
        description="SQL-like expression for tree-level filtering"
    )
    area_domain: Optional[str] = Field(
        default=None,
        description="SQL-like expression for area/condition-level filtering"
    )
    plot_domain: Optional[str] = Field(
        default=None,
        description="SQL-like expression for plot-level filtering"
    )
    
    # Estimation method parameters
    method: Union[str, EstimationMethod] = Field(
        default="TI",
        description="Estimation method: Temporally Indifferent (TI) or moving averages"
    )
    lambda_: float = Field(
        default=0.5,
        ge=0.0,
        le=1.0,
        description="Temporal weighting parameter for exponential moving average (EMA)"
    )
    
    # Output options
    totals: bool = Field(
        default=False,
        description="Whether to include total estimates in addition to per-acre values"
    )
    variance: bool = Field(
        default=False,
        description="Whether to return variance instead of standard error"
    )
    by_plot: bool = Field(
        default=False,
        description="Whether to return plot-level estimates"
    )
    most_recent: bool = Field(
        default=False,
        description="Whether to use only the most recent evaluation"
    )
    
    # Variance calculation
    variance_method: Union[str, VarianceMethod] = Field(
        default="ratio",
        description="Variance calculation method"
    )
    
    # Lazy evaluation configuration
    lazy_config: Optional[LazyEvaluationConfig] = Field(
        default_factory=LazyEvaluationConfig,
        description="Lazy evaluation configuration"
    )
    
    # Additional parameters
    extra_params: Dict[str, Any] = Field(
        default_factory=dict,
        description="Additional module-specific parameters"
    )
    
    @field_validator("grp_by")
    @classmethod
    def validate_grp_by(cls, v: Optional[Union[str, List[str]]]) -> Optional[Union[str, List[str]]]:
        """Validate grouping columns against known FIA columns."""
        if v is None:
            return v
            
        columns = [v] if isinstance(v, str) else v
        invalid_cols = [col for col in columns if col not in VALID_FIA_GROUPING_COLUMNS]
        
        if invalid_cols:
            # Don't fail for unknown columns, just warn - they might be custom derived columns
            warnings.warn(
                f"Unknown grouping columns: {invalid_cols}. These may be valid derived columns.",
                UserWarning
            )
            
        return v
    
    @field_validator("tree_domain", "area_domain")
    @classmethod
    def validate_domain_expression(cls, v: Optional[str]) -> Optional[str]:
        """Basic validation of domain expressions."""
        if v is None:
            return v
            
        # Remove extra whitespace
        v = " ".join(v.split())
        
        # Check for basic SQL injection patterns
        dangerous_patterns = ["DROP", "DELETE", "INSERT", "UPDATE", "ALTER", "CREATE", "EXEC", "EXECUTE", "--", "/*", "*/"]
        v_upper = v.upper()
        for pattern in dangerous_patterns:
            if pattern in v_upper:
                raise ValueError(f"Domain expression contains forbidden keyword: {pattern}")
                
        return v
    
    @field_validator("land_type", "tree_type", "method", "variance_method", mode="before")
    @classmethod
    def convert_to_enum(cls, v: Union[str, Enum], info) -> Union[str, Enum]:
        """Convert string values to enums if needed."""
        if isinstance(v, str):
            field_name = info.field_name
            if field_name == "land_type" and v in ["forest", "timber", "all"]:
                return v
            elif field_name == "tree_type" and v in ["live", "dead", "gs", "all"]:
                return v
            elif field_name == "method" and v in ["TI", "SMA", "LMA", "EMA", "ANNUAL"]:
                return v
            elif field_name == "variance_method" and v in ["standard", "ratio", "hybrid"]:
                return v
        return v
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        data = self.model_dump(exclude_none=True)
        
        # Flatten lazy_config if present
        if "lazy_config" in data and data["lazy_config"]:
            lazy_data = data.pop("lazy_config")
            for key, value in lazy_data.items():
                data[f"lazy_{key}"] = value
        
        # Merge extra_params
        if "extra_params" in data:
            extra = data.pop("extra_params")
            data.update(extra)
            
        return data
    
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


# === Mortality Configuration ===

class MortalityConfig(EstimatorConfig):
    """
    Configuration specific to mortality estimation.
    
    Extends the base configuration with mortality-specific parameters
    and validation rules.
    """
    
    # Mortality-specific grouping options
    group_by_species_group: bool = Field(
        default=False,
        description="Group by species group code (SPGRPCD)"
    )
    group_by_ownership: bool = Field(
        default=False,
        description="Group by ownership group code (OWNGRPCD)"
    )
    group_by_agent: bool = Field(
        default=False,
        description="Group by mortality agent code (AGENTCD)"
    )
    group_by_disturbance: bool = Field(
        default=False,
        description="Group by disturbance codes (DSTRBCD1, DSTRBCD2, DSTRBCD3)"
    )
    
    # Mortality type and calculation options
    mortality_type: Literal["tpa", "volume", "both"] = Field(
        default="tpa",
        description="Type of mortality to calculate: trees per acre, volume, or both"
    )
    tree_class: Literal["all", "timber", "growing_stock"] = Field(
        default="all",
        description="Tree classification for mortality calculation"
    )
    
    # Component breakdown options
    include_components: bool = Field(
        default=False,
        description="Include component breakdowns (BA, VOL) in addition to TPA"
    )
    include_natural: bool = Field(
        default=True,
        description="Include natural mortality in calculations"
    )
    include_harvest: bool = Field(
        default=True,
        description="Include harvest mortality in calculations"
    )
    
    @model_validator(mode="after")
    def validate_mortality_options(self) -> "MortalityConfig":
        """Validate mortality-specific option combinations."""
        # If calculating volume mortality, ensure we're not filtering to live trees only
        if self.mortality_type in ["volume", "both"] and self.tree_type == "live":
            raise ValueError(
                "Cannot calculate volume mortality with tree_type='live'. "
                "Mortality requires dead trees. Use tree_type='dead' or 'all'."
            )
            
        # Validate tree_class and land_type combination
        if self.tree_class == "timber" and self.land_type not in ["timber", "all"]:
            raise ValueError(
                f"tree_class='timber' requires land_type='timber' or 'all', "
                f"not '{self.land_type}'"
            )
            
        return self
    
    def get_grouping_columns(self) -> List[str]:
        """Get all grouping columns including mortality-specific ones."""
        columns = super().get_grouping_columns()
        
        if self.group_by_species_group:
            columns.append("SPGRPCD")
            
        if self.group_by_ownership:
            columns.append("OWNGRPCD")
            
        if self.group_by_agent:
            columns.append("AGENTCD")
            
        if self.group_by_disturbance:
            columns.extend(["DSTRBCD1", "DSTRBCD2", "DSTRBCD3"])
            
        # Add size class if requested
        if self.by_size_class and "SIZE_CLASS" not in columns:
            columns.append("SIZE_CLASS")
            
        # Remove duplicates while preserving order
        seen = set()
        return [x for x in columns if not (x in seen or seen.add(x))]
    
    def get_output_columns(self) -> List[str]:
        """Get expected output columns based on configuration."""
        columns = []
        
        # Base mortality columns
        if self.mortality_type in ["tpa", "both"]:
            columns.append("MORTALITY_TPA")
            if self.variance:
                columns.append("MORTALITY_TPA_VAR")
            else:
                columns.append("MORTALITY_TPA_SE")
            if self.totals:
                columns.append("MORTALITY_TPA_TOTAL")
                
        if self.mortality_type in ["volume", "both"]:
            columns.append("MORTALITY_VOL")
            if self.variance:
                columns.append("MORTALITY_VOL_VAR")
            else:
                columns.append("MORTALITY_VOL_SE")
            if self.totals:
                columns.append("MORTALITY_VOL_TOTAL")
                
        # Component columns if requested
        if self.include_components:
            columns.extend([
                "MORTALITY_BA",
                "MORTALITY_BA_VAR" if self.variance else "MORTALITY_BA_SE"
            ])
            if self.totals:
                columns.append("MORTALITY_BA_TOTAL")
                
        return columns
    


# === Module-Specific Configuration with Generic Support ===

class ModularEstimatorConfig(EstimatorConfig, Generic[TModuleConfig]):
    """
    Configuration for pyFIA estimators with module-specific parameters.
    
    This configuration class provides:
    1. Common parameters for all estimators (inherited from EstimatorConfig)
    2. Module-specific parameters through generics
    3. Lazy evaluation settings
    4. Comprehensive validation for FIA parameters
    """
    
    # Module-specific configuration
    module_config: Optional[TModuleConfig] = Field(
        default=None,
        description="Module-specific configuration"
    )
    
    def with_module_config(self, module_config: TModuleConfig) -> "ModularEstimatorConfig[TModuleConfig]":
        """
        Create a new config with module-specific configuration.
        
        Parameters
        ----------
        module_config : TModuleConfig
            Module-specific configuration
        
        Returns
        -------
        ModularEstimatorConfig[TModuleConfig]
            New configuration with module config
        """
        data = self.model_dump()
        data["module_config"] = module_config
        return ModularEstimatorConfig[type(module_config)](**data)
    
    
    def validate_for_module(self, module_name: str) -> None:
        """
        Validate configuration for specific module requirements.
        
        Parameters
        ----------
        module_name : str
            Name of the module (e.g., "volume", "biomass", "mortality")
        
        Raises
        ------
        ValueError
            If configuration is invalid for the module
        """
        # Module-specific validation rules
        if module_name == "mortality":
            if self.tree_type == "live" or (isinstance(self.tree_type, TreeType) and self.tree_type == TreeType.LIVE):
                raise ValueError(
                    "Mortality estimation requires tree_type='dead' or 'all', not 'live'"
                )
        
        elif module_name == "growth":
            method_val = self.method if isinstance(self.method, str) else self.method.value
            if method_val not in ["TI", "ANNUAL"]:
                raise ValueError(
                    f"Growth estimation typically uses TI or ANNUAL methods, not {method_val}"
                )
        
        elif module_name == "area":
            if self.tree_domain:
                warnings.warn(
                    "Area estimation with tree_domain may not produce expected results. "
                    "Consider using area_domain instead.",
                    UserWarning
                )


# === Specialized Configurations ===

class VolumeConfig(ModularEstimatorConfig[VolumeSpecificConfig]):
    """Unified configuration for volume estimation."""
    
    module_config: VolumeSpecificConfig = Field(
        default_factory=VolumeSpecificConfig,
        description="Volume-specific configuration"
    )


class BiomassConfig(ModularEstimatorConfig[BiomassSpecificConfig]):
    """Unified configuration for biomass estimation."""
    
    module_config: BiomassSpecificConfig = Field(
        default_factory=BiomassSpecificConfig,
        description="Biomass-specific configuration"
    )


class GrowthConfig(ModularEstimatorConfig[GrowthSpecificConfig]):
    """Unified configuration for growth estimation."""
    
    module_config: GrowthSpecificConfig = Field(
        default_factory=GrowthSpecificConfig,
        description="Growth-specific configuration"
    )


class AreaConfig(ModularEstimatorConfig[AreaSpecificConfig]):
    """Unified configuration for area estimation."""
    
    module_config: AreaSpecificConfig = Field(
        default_factory=AreaSpecificConfig,
        description="Area-specific configuration"
    )


# MortalityConfig already has all the specific parameters it needs


# === Configuration Factory ===

class ConfigFactory:
    """
    Factory for creating appropriate configuration objects.
    
    This factory helps create the right configuration type based on
    the module and input parameters.
    """
    
    @staticmethod
    def create_config(
        module: str,
        **kwargs
    ) -> EstimatorConfig:
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
        EstimatorConfig
            Appropriate configuration for the module
        """
        # Return specialized config based on module
        if module == "volume":
            return VolumeConfig(**kwargs)
        elif module == "biomass":
            return BiomassConfig(**kwargs)
        elif module == "growth":
            return GrowthConfig(**kwargs)
        elif module == "area":
            return AreaConfig(**kwargs)
        elif module == "mortality":
            # Extract mortality-specific fields from extra_params if present
            extra = kwargs.get("extra_params", {})
            mortality_fields = [
                "group_by_species_group", "group_by_ownership", "group_by_agent",
                "group_by_disturbance", "mortality_type", "tree_class",
                "include_components", "include_natural", "include_harvest"
            ]
            for field in mortality_fields:
                if field in extra:
                    kwargs[field] = extra.pop(field)
            return MortalityConfig(**kwargs)
        else:
            return EstimatorConfig(**kwargs)


# === Export public API ===

__all__ = [
    # Main configuration
    "EstimatorConfig",
    "MortalityConfig",
    
    # Module-specific configurations
    "ModularEstimatorConfig",
    "VolumeConfig",
    "BiomassConfig",
    "GrowthConfig",
    "AreaConfig",
    
    # Module-specific parameter classes
    "VolumeSpecificConfig",
    "BiomassSpecificConfig",
    "GrowthSpecificConfig",
    "AreaSpecificConfig",
    
    # Lazy evaluation
    "LazyEvaluationConfig",
    "LazyEvaluationMode",
    
    # Enums
    "EstimationMethod",
    "LandType",
    "TreeType",
    "VarianceMethod",
    
    # Factory
    "ConfigFactory",
    
    # Constants
    "VALID_FIA_GROUPING_COLUMNS",
]