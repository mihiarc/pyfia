"""
Configuration classes for pyFIA estimation modules.

This module provides Pydantic v2-based configuration classes for all estimation
modules, with proper validation and type safety. The base EstimatorConfig provides
common parameters, while specialized configs extend it for module-specific needs.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


# Define valid FIA columns for grouping validation
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


class EstimatorConfigV2(BaseModel):
    """
    Enhanced configuration for FIA estimation parameters using Pydantic v2.
    
    This configuration class provides validation and type safety for all
    common estimation parameters across pyFIA modules.
    """
    
    model_config = ConfigDict(
        validate_assignment=True,
        extra="forbid",
        str_strip_whitespace=True
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
    land_type: Literal["forest", "timber", "all"] = Field(
        default="forest",
        description="Land type filter for estimation"
    )
    tree_type: Literal["live", "dead", "gs", "all"] = Field(
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
    
    # Estimation method parameters
    method: Literal["TI", "SMA", "LMA", "EMA", "ANNUAL"] = Field(
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
            # In production, this could log a warning
            pass
            
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
        dangerous_patterns = ["DROP", "DELETE", "INSERT", "UPDATE", "ALTER", "CREATE"]
        v_upper = v.upper()
        for pattern in dangerous_patterns:
            if pattern in v_upper:
                raise ValueError(f"Domain expression contains forbidden keyword: {pattern}")
                
        return v
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for backwards compatibility with dataclass configs."""
        data = self.model_dump(exclude_none=True)
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


class MortalityConfig(EstimatorConfigV2):
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
    
    # Variance calculation options
    variance_method: Literal["standard", "ratio", "hybrid"] = Field(
        default="ratio",
        description="Variance calculation method for mortality estimates"
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
    
    def to_estimator_config(self):
        """
        Convert to legacy EstimatorConfig for backwards compatibility.
        
        This allows the new Pydantic config to work with existing code
        that expects the dataclass-based EstimatorConfig.
        """
        # Import here to avoid circular imports
        from pyfia.estimation.base import EstimatorConfig
        
        # Extract base parameters
        base_params = {
            "grp_by": self.grp_by,
            "by_species": self.by_species,
            "by_size_class": self.by_size_class,
            "land_type": self.land_type,
            "tree_type": self.tree_type,
            "tree_domain": self.tree_domain,
            "area_domain": self.area_domain,
            "method": self.method,
            "lambda_": self.lambda_,
            "totals": self.totals,
            "variance": self.variance,
            "by_plot": self.by_plot,
            "most_recent": self.most_recent,
        }
        
        # Add mortality-specific params to extra_params
        extra_params = self.extra_params.copy()
        extra_params.update({
            "group_by_species_group": self.group_by_species_group,
            "group_by_ownership": self.group_by_ownership,
            "group_by_agent": self.group_by_agent,
            "group_by_disturbance": self.group_by_disturbance,
            "mortality_type": self.mortality_type,
            "tree_class": self.tree_class,
            "include_components": self.include_components,
            "include_natural": self.include_natural,
            "include_harvest": self.include_harvest,
            "variance_method": self.variance_method,
        })
        
        return EstimatorConfig(**base_params, extra_params=extra_params)


# Note: The original EstimatorConfig is kept in base.py for backwards compatibility
# This module provides the new Pydantic-based configurations