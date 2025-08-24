"""
Unified configuration system for pyFIA Phase 3.

This module provides a unified configuration system that:
1. Maintains full backward compatibility with EstimatorConfig (dataclass)
2. Integrates with EstimatorConfigV2 and MortalityConfig (Pydantic)
3. Supports module-specific parameters without class proliferation
4. Includes lazy evaluation settings from Phase 2
5. Provides validation for FIA-specific parameters

The unified system uses Pydantic v2 for validation while maintaining
compatibility with legacy dataclass-based configurations.
"""

from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any, Dict, Generic, List, Literal, Optional, TypeVar, Union

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

# Import existing configuration classes for compatibility
from .base import EstimatorConfig
from .config import EstimatorConfigV2, MortalityConfig, VALID_FIA_GROUPING_COLUMNS


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
                import warnings
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


# === Unified Configuration Class ===

class UnifiedEstimatorConfig(BaseModel, Generic[TModuleConfig]):
    """
    Unified configuration for all pyFIA estimators.
    
    This configuration class provides:
    1. Common parameters for all estimators
    2. Module-specific parameters through generics
    3. Lazy evaluation settings
    4. Full backward compatibility with legacy configs
    5. Comprehensive validation for FIA parameters
    
    Examples
    --------
    Basic usage with common parameters:
    >>> config = UnifiedEstimatorConfig(
    ...     by_species=True,
    ...     land_type="forest",
    ...     method="TI"
    ... )
    
    With module-specific parameters:
    >>> config = UnifiedEstimatorConfig[VolumeSpecificConfig](
    ...     by_species=True,
    ...     module_config=VolumeSpecificConfig(
    ...         volume_equation="regional",
    ...         include_rotten=True
    ...     )
    ... )
    
    Converting from legacy config:
    >>> legacy = EstimatorConfig(by_species=True, land_type="forest")
    >>> unified = UnifiedEstimatorConfig.from_legacy(legacy)
    """
    
    model_config = ConfigDict(
        validate_assignment=True,
        extra="forbid",
        str_strip_whitespace=True,
        use_enum_values=True
    )
    
    # === Common Grouping Parameters ===
    
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
    
    # === Domain Filters ===
    
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
    
    # === Estimation Method Parameters ===
    
    method: EstimationMethod = Field(
        default=EstimationMethod.TI,
        description="Estimation method"
    )
    
    lambda_: float = Field(
        default=0.5,
        ge=0.0,
        le=1.0,
        description="Temporal weighting parameter for EMA"
    )
    
    # === Output Options ===
    
    totals: bool = Field(
        default=False,
        description="Include total estimates"
    )
    
    variance: bool = Field(
        default=False,
        description="Return variance instead of standard error"
    )
    
    by_plot: bool = Field(
        default=False,
        description="Return plot-level estimates"
    )
    
    most_recent: bool = Field(
        default=False,
        description="Use only most recent evaluation"
    )
    
    # === Variance Calculation ===
    
    variance_method: VarianceMethod = Field(
        default=VarianceMethod.RATIO,
        description="Variance calculation method"
    )
    
    # === Module-Specific Configuration ===
    
    module_config: Optional[TModuleConfig] = Field(
        default=None,
        description="Module-specific configuration"
    )
    
    # === Lazy Evaluation Configuration ===
    
    lazy_config: LazyEvaluationConfig = Field(
        default_factory=LazyEvaluationConfig,
        description="Lazy evaluation configuration"
    )
    
    # === Additional Parameters ===
    
    extra_params: Dict[str, Any] = Field(
        default_factory=dict,
        description="Additional parameters for backward compatibility"
    )
    
    # === Validators ===
    
    @field_validator("grp_by")
    @classmethod
    def validate_grp_by(cls, v: Optional[Union[str, List[str]]]) -> Optional[Union[str, List[str]]]:
        """Validate grouping columns against known FIA columns."""
        if v is None:
            return v
        
        columns = [v] if isinstance(v, str) else v
        
        # Check for invalid columns but don't fail (might be derived columns)
        invalid_cols = [col for col in columns if col not in VALID_FIA_GROUPING_COLUMNS]
        if invalid_cols:
            import warnings
            warnings.warn(
                f"Unknown grouping columns: {invalid_cols}. "
                "These may be valid derived columns.",
                UserWarning
            )
        
        return v
    
    @field_validator("tree_domain", "area_domain")
    @classmethod
    def validate_domain_expression(cls, v: Optional[str]) -> Optional[str]:
        """Validate and sanitize domain expressions."""
        if v is None:
            return v
        
        # Remove extra whitespace
        v = " ".join(v.split())
        
        # Check for SQL injection patterns
        dangerous_patterns = [
            "DROP", "DELETE", "INSERT", "UPDATE", "ALTER", 
            "CREATE", "EXEC", "EXECUTE", "--", "/*", "*/"
        ]
        v_upper = v.upper()
        for pattern in dangerous_patterns:
            if pattern in v_upper:
                raise ValueError(f"Domain expression contains forbidden keyword: {pattern}")
        
        return v
    
    @model_validator(mode="after")
    def validate_configuration_consistency(self) -> "UnifiedEstimatorConfig":
        """Validate configuration consistency across parameters."""
        # Validate tree_type and mortality calculations
        if hasattr(self.module_config, "mortality_type"):
            if self.module_config.mortality_type in ["volume", "both"] and self.tree_type == TreeType.LIVE:
                raise ValueError(
                    "Cannot calculate mortality with tree_type='live'. "
                    "Use tree_type='dead' or 'all'."
                )
        
        # Validate method and lambda combination
        if self.method == EstimationMethod.EMA and not (0 < self.lambda_ < 1):
            raise ValueError(
                f"EMA method requires lambda between 0 and 1, got {self.lambda_}"
            )
        
        # Validate lazy evaluation settings
        if self.lazy_config.mode == LazyEvaluationMode.DISABLED and self.by_plot:
            import warnings
            warnings.warn(
                "Plot-level estimates with lazy evaluation disabled may use "
                "significant memory for large datasets.",
                UserWarning
            )
        
        return self
    
    # === Conversion Methods ===
    
    @classmethod
    def from_legacy(cls, legacy_config: EstimatorConfig) -> "UnifiedEstimatorConfig":
        """
        Create unified config from legacy EstimatorConfig.
        
        Parameters
        ----------
        legacy_config : EstimatorConfig
            Legacy dataclass configuration
        
        Returns
        -------
        UnifiedEstimatorConfig
            Unified configuration with same settings
        """
        # Extract base parameters (string values will be converted to enums by Pydantic)
        params = {
            "grp_by": legacy_config.grp_by,
            "by_species": legacy_config.by_species,
            "by_size_class": legacy_config.by_size_class,
            "land_type": legacy_config.land_type,  # Pydantic will convert to enum
            "tree_type": legacy_config.tree_type,  # Pydantic will convert to enum
            "tree_domain": legacy_config.tree_domain,
            "area_domain": legacy_config.area_domain,
            "method": legacy_config.method,  # Pydantic will convert to enum
            "lambda_": legacy_config.lambda_,
            "totals": legacy_config.totals,
            "variance": legacy_config.variance,
            "by_plot": legacy_config.by_plot,
            "most_recent": legacy_config.most_recent,
        }
        
        # Add extra params
        if legacy_config.extra_params:
            params["extra_params"] = legacy_config.extra_params.copy()
        
        # Check for lazy evaluation settings in extra_params
        lazy_params = {}
        if "lazy_enabled" in legacy_config.extra_params:
            lazy_params["mode"] = (
                LazyEvaluationMode.ENABLED 
                if legacy_config.extra_params["lazy_enabled"] 
                else LazyEvaluationMode.DISABLED
            )
        if "lazy_threshold_rows" in legacy_config.extra_params:
            lazy_params["threshold_rows"] = legacy_config.extra_params["lazy_threshold_rows"]
        
        if lazy_params:
            params["lazy_config"] = LazyEvaluationConfig(**lazy_params)
        
        return cls(**params)
    
    @classmethod
    def from_v2(cls, v2_config: EstimatorConfigV2) -> "UnifiedEstimatorConfig":
        """
        Create unified config from EstimatorConfigV2.
        
        Parameters
        ----------
        v2_config : EstimatorConfigV2
            Pydantic v2 configuration
        
        Returns
        -------
        UnifiedEstimatorConfig
            Unified configuration with same settings
        """
        # Convert to dict and create unified config
        params = v2_config.model_dump(exclude={"extra_params"})
        
        # Handle MortalityConfig special case - move extra fields to extra_params
        if isinstance(v2_config, MortalityConfig):
            mortality_fields = [
                "group_by_species_group", "group_by_ownership", "group_by_agent",
                "group_by_disturbance", "mortality_type", "tree_class",
                "include_components", "include_natural", "include_harvest"
            ]
            extra_params = {}
            for field in mortality_fields:
                if field in params:
                    extra_params[field] = params.pop(field)
            
            if v2_config.extra_params:
                extra_params.update(v2_config.extra_params)
            
            params["extra_params"] = extra_params
        elif v2_config.extra_params:
            params["extra_params"] = v2_config.extra_params.copy()
        
        return cls(**params)
    
    def to_legacy(self) -> EstimatorConfig:
        """
        Convert to legacy EstimatorConfig for backward compatibility.
        
        Returns
        -------
        EstimatorConfig
            Legacy dataclass configuration
        """
        # Extract base parameters
        params = {
            "grp_by": self.grp_by,
            "by_species": self.by_species,
            "by_size_class": self.by_size_class,
            "land_type": self.land_type.value if isinstance(self.land_type, Enum) else self.land_type,
            "tree_type": self.tree_type.value if isinstance(self.tree_type, Enum) else self.tree_type,
            "tree_domain": self.tree_domain,
            "area_domain": self.area_domain,
            "method": self.method.value if isinstance(self.method, Enum) else self.method,
            "lambda_": self.lambda_,
            "totals": self.totals,
            "variance": self.variance,
            "by_plot": self.by_plot,
            "most_recent": self.most_recent,
        }
        
        # Add extra params including lazy settings
        extra_params = self.extra_params.copy()
        
        # Add lazy evaluation settings to extra_params
        extra_params["lazy_enabled"] = self.lazy_config.mode != LazyEvaluationMode.DISABLED
        extra_params["lazy_threshold_rows"] = self.lazy_config.threshold_rows
        
        # Add module-specific params if present
        if self.module_config:
            module_dict = self.module_config.model_dump()
            extra_params.update(module_dict)
        
        return EstimatorConfig(**params, extra_params=extra_params)
    
    def to_v2(self) -> EstimatorConfigV2:
        """
        Convert to EstimatorConfigV2 for compatibility.
        
        Returns
        -------
        EstimatorConfigV2
            Pydantic v2 configuration
        """
        params = {
            "grp_by": self.grp_by,
            "by_species": self.by_species,
            "by_size_class": self.by_size_class,
            "land_type": self.land_type.value if isinstance(self.land_type, Enum) else self.land_type,
            "tree_type": self.tree_type.value if isinstance(self.tree_type, Enum) else self.tree_type,
            "tree_domain": self.tree_domain,
            "area_domain": self.area_domain,
            "method": self.method.value if isinstance(self.method, Enum) else self.method,
            "lambda_": self.lambda_,
            "totals": self.totals,
            "variance": self.variance,
            "by_plot": self.by_plot,
            "most_recent": self.most_recent,
            "extra_params": self.extra_params.copy()
        }
        
        return EstimatorConfigV2(**params)
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert to dictionary for general use.
        
        Returns
        -------
        Dict[str, Any]
            Configuration as dictionary
        """
        data = self.model_dump(exclude_none=True)
        
        # Flatten module config if present
        if "module_config" in data and data["module_config"]:
            module_data = data.pop("module_config")
            data.update(module_data)
        
        # Flatten lazy config to top level for simplicity
        if "lazy_config" in data:
            lazy_data = data.pop("lazy_config")
            for key, value in lazy_data.items():
                data[f"lazy_{key}"] = value
        
        # Merge extra params
        if "extra_params" in data:
            extra = data.pop("extra_params")
            data.update(extra)
        
        return data
    
    def get_grouping_columns(self) -> List[str]:
        """
        Get all grouping columns based on configuration.
        
        Returns
        -------
        List[str]
            List of column names for grouping
        """
        columns = []
        
        if self.grp_by:
            if isinstance(self.grp_by, str):
                columns.append(self.grp_by)
            else:
                columns.extend(self.grp_by)
        
        if self.by_species:
            columns.append("SPCD")
        
        if self.by_size_class and "SIZE_CLASS" not in columns:
            columns.append("SIZE_CLASS")
        
        # Add module-specific grouping columns
        if self.module_config and hasattr(self.module_config, "get_grouping_columns"):
            columns.extend(self.module_config.get_grouping_columns())
        
        # Remove duplicates while preserving order
        seen = set()
        return [x for x in columns if not (x in seen or seen.add(x))]
    
    def with_module_config(self, module_config: TModuleConfig) -> "UnifiedEstimatorConfig[TModuleConfig]":
        """
        Create a new config with module-specific configuration.
        
        Parameters
        ----------
        module_config : TModuleConfig
            Module-specific configuration
        
        Returns
        -------
        UnifiedEstimatorConfig[TModuleConfig]
            New configuration with module config
        """
        data = self.model_dump()
        data["module_config"] = module_config
        return UnifiedEstimatorConfig[type(module_config)](**data)
    
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
            if self.tree_type == TreeType.LIVE:
                raise ValueError(
                    "Mortality estimation requires tree_type='dead' or 'all', "
                    "not 'live'"
                )
        
        elif module_name == "growth":
            if self.method not in [EstimationMethod.TI, EstimationMethod.ANNUAL]:
                raise ValueError(
                    f"Growth estimation typically uses TI or ANNUAL methods, "
                    f"not {self.method}"
                )
        
        elif module_name == "area":
            if self.tree_domain:
                import warnings
                warnings.warn(
                    "Area estimation with tree_domain may not produce expected results. "
                    "Consider using area_domain instead.",
                    UserWarning
                )


# === Specialized Unified Configs for Common Modules ===

class UnifiedVolumeConfig(UnifiedEstimatorConfig[VolumeSpecificConfig]):
    """Unified configuration for volume estimation."""
    
    module_config: VolumeSpecificConfig = Field(
        default_factory=VolumeSpecificConfig,
        description="Volume-specific configuration"
    )


class UnifiedBiomassConfig(UnifiedEstimatorConfig[BiomassSpecificConfig]):
    """Unified configuration for biomass estimation."""
    
    module_config: BiomassSpecificConfig = Field(
        default_factory=BiomassSpecificConfig,
        description="Biomass-specific configuration"
    )


class UnifiedGrowthConfig(UnifiedEstimatorConfig[GrowthSpecificConfig]):
    """Unified configuration for growth estimation."""
    
    module_config: GrowthSpecificConfig = Field(
        default_factory=GrowthSpecificConfig,
        description="Growth-specific configuration"
    )


class UnifiedAreaConfig(UnifiedEstimatorConfig[AreaSpecificConfig]):
    """Unified configuration for area estimation."""
    
    module_config: AreaSpecificConfig = Field(
        default_factory=AreaSpecificConfig,
        description="Area-specific configuration"
    )


class UnifiedMortalityConfig(UnifiedEstimatorConfig):
    """
    Unified configuration for mortality estimation.
    
    This maintains compatibility with the existing MortalityConfig
    while providing the unified interface.
    """
    
    # Mortality-specific parameters (from MortalityConfig)
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
        description="Group by disturbance codes"
    )
    
    mortality_type: Literal["tpa", "volume", "both"] = Field(
        default="tpa",
        description="Type of mortality to calculate"
    )
    
    tree_class: Literal["all", "timber", "growing_stock"] = Field(
        default="all",
        description="Tree classification for mortality"
    )
    
    include_components: bool = Field(
        default=False,
        description="Include component breakdowns"
    )
    
    include_natural: bool = Field(
        default=True,
        description="Include natural mortality"
    )
    
    include_harvest: bool = Field(
        default=True,
        description="Include harvest mortality"
    )
    
    @classmethod
    def from_mortality_config(cls, mortality_config: MortalityConfig) -> "UnifiedMortalityConfig":
        """Create from existing MortalityConfig."""
        data = mortality_config.model_dump()
        return cls(**data)
    
    def to_mortality_config(self) -> MortalityConfig:
        """Convert to MortalityConfig for compatibility."""
        params = self.model_dump(
            exclude={"module_config", "lazy_config"}
        )
        return MortalityConfig(**params)
    
    def get_grouping_columns(self) -> List[str]:
        """Get all grouping columns including mortality-specific."""
        columns = super().get_grouping_columns()
        
        if self.group_by_species_group:
            columns.append("SPGRPCD")
        if self.group_by_ownership:
            columns.append("OWNGRPCD")
        if self.group_by_agent:
            columns.append("AGENTCD")
        if self.group_by_disturbance:
            columns.extend(["DSTRBCD1", "DSTRBCD2", "DSTRBCD3"])
        
        # Remove duplicates
        seen = set()
        return [x for x in columns if not (x in seen or seen.add(x))]


# === Configuration Factory ===

class ConfigFactory:
    """
    Factory for creating appropriate configuration objects.
    
    This factory helps create the right configuration type based on
    the module and input parameters, ensuring backward compatibility.
    """
    
    @staticmethod
    def create_config(
        module: str,
        legacy_config: Optional[EstimatorConfig] = None,
        **kwargs
    ) -> UnifiedEstimatorConfig:
        """
        Create appropriate configuration for a module.
        
        Parameters
        ----------
        module : str
            Module name (e.g., "volume", "biomass", "mortality")
        legacy_config : Optional[EstimatorConfig]
            Legacy configuration to convert
        **kwargs
            Additional configuration parameters
        
        Returns
        -------
        UnifiedEstimatorConfig
            Appropriate unified configuration
        """
        # If legacy config provided, convert it first
        if legacy_config:
            base_config = UnifiedEstimatorConfig.from_legacy(legacy_config)
            # Update with any additional kwargs
            if kwargs:
                data = base_config.model_dump()
                data.update(kwargs)
                base_config = UnifiedEstimatorConfig(**data)
        else:
            base_config = UnifiedEstimatorConfig(**kwargs)
        
        # Return specialized config based on module
        if module == "volume":
            data = base_config.model_dump(exclude={"module_config"})
            return UnifiedVolumeConfig(**data)
        elif module == "biomass":
            data = base_config.model_dump(exclude={"module_config"})
            return UnifiedBiomassConfig(**data)
        elif module == "growth":
            data = base_config.model_dump(exclude={"module_config"})
            return UnifiedGrowthConfig(**data)
        elif module == "area":
            data = base_config.model_dump(exclude={"module_config"})
            return UnifiedAreaConfig(**data)
        elif module == "mortality":
            data = base_config.model_dump(exclude={"module_config"})
            # Extract mortality-specific fields from extra_params
            extra = data.get("extra_params", {})
            mortality_fields = [
                "group_by_species_group", "group_by_ownership", "group_by_agent",
                "group_by_disturbance", "mortality_type", "tree_class",
                "include_components", "include_natural", "include_harvest"
            ]
            for field in mortality_fields:
                if field in extra:
                    data[field] = extra.pop(field)
            return UnifiedMortalityConfig(**data)
        else:
            return base_config
    
    @staticmethod
    def is_unified_config(config: Any) -> bool:
        """Check if a configuration is a unified config."""
        return isinstance(config, UnifiedEstimatorConfig)
    
    @staticmethod
    def ensure_unified(config: Any, module: Optional[str] = None) -> UnifiedEstimatorConfig:
        """
        Ensure configuration is unified, converting if necessary.
        
        Parameters
        ----------
        config : Any
            Configuration object (unified, legacy, or v2)
        module : Optional[str]
            Module name for specialized configs
        
        Returns
        -------
        UnifiedEstimatorConfig
            Unified configuration
        """
        if isinstance(config, UnifiedEstimatorConfig):
            return config
        elif isinstance(config, EstimatorConfig):
            unified = UnifiedEstimatorConfig.from_legacy(config)
        elif isinstance(config, MortalityConfig):
            # Handle MortalityConfig specially
            return UnifiedMortalityConfig.from_mortality_config(config)
        elif isinstance(config, EstimatorConfigV2):
            unified = UnifiedEstimatorConfig.from_v2(config)
        elif isinstance(config, dict):
            unified = UnifiedEstimatorConfig(**config)
        else:
            raise TypeError(f"Unknown configuration type: {type(config)}")
        
        # Convert to specialized config if module specified
        if module:
            return ConfigFactory.create_config(module, **unified.model_dump())
        
        return unified


# === Export public API ===

__all__ = [
    # Main unified config
    "UnifiedEstimatorConfig",
    
    # Module-specific configs
    "UnifiedVolumeConfig",
    "UnifiedBiomassConfig", 
    "UnifiedGrowthConfig",
    "UnifiedAreaConfig",
    "UnifiedMortalityConfig",
    
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
]