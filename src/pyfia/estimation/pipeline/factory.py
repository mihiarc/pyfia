"""
Pipeline factory for creating estimation pipelines.

This module provides a centralized factory for creating pipelines with
auto-detection, configuration-based creation, and validation.
"""

from typing import Dict, List, Optional, Any, Type, Union
from dataclasses import dataclass
from enum import Enum
import json
from pathlib import Path

import polars as pl

from ...core import FIA
from ..config import EstimatorConfig
from .core import EstimationPipeline, ExecutionMode
from .builders import (
    PipelineBuilder,
    VolumeEstimationBuilder,
    BiomassEstimationBuilder,
    TPAEstimationBuilder,
    AreaEstimationBuilder,
    GrowthEstimationBuilder,
    MortalityEstimationBuilder,
)
from .templates import PipelineTemplate, TemplateRegistry, get_template
from .quick_start import (
    create_volume_pipeline,
    create_biomass_pipeline,
    create_tpa_pipeline,
    create_area_pipeline,
    create_growth_pipeline,
    create_mortality_pipeline,
)


class EstimationType(Enum):
    """Enumeration of estimation types."""
    
    VOLUME = "volume"
    BIOMASS = "biomass"
    TPA = "tpa"
    AREA = "area"
    GROWTH = "growth"
    MORTALITY = "mortality"


@dataclass
class PipelineConfig:
    """
    Configuration object for pipeline creation.
    
    Encapsulates all parameters needed to create a pipeline.
    """
    
    estimation_type: EstimationType
    by_species: bool = False
    by_size_class: bool = False
    grp_by: Optional[List[str]] = None
    tree_domain: Optional[str] = None
    area_domain: Optional[str] = None
    plot_domain: Optional[str] = None
    totals: bool = False
    variance: bool = True
    temporal_method: str = "TI"
    execution_mode: ExecutionMode = ExecutionMode.SEQUENTIAL
    enable_caching: bool = True
    module_config: Optional[Dict[str, Any]] = None
    custom_steps: Optional[List[Dict[str, Any]]] = None
    skip_steps: Optional[List[str]] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        result = {
            "estimation_type": self.estimation_type.value,
            "by_species": self.by_species,
            "by_size_class": self.by_size_class,
            "grp_by": self.grp_by,
            "tree_domain": self.tree_domain,
            "area_domain": self.area_domain,
            "plot_domain": self.plot_domain,
            "totals": self.totals,
            "variance": self.variance,
            "temporal_method": self.temporal_method,
            "execution_mode": self.execution_mode.value,
            "enable_caching": self.enable_caching,
        }
        
        if self.module_config:
            result["module_config"] = self.module_config
        
        if self.custom_steps:
            result["custom_steps"] = self.custom_steps
        
        if self.skip_steps:
            result["skip_steps"] = self.skip_steps
        
        return result
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PipelineConfig":
        """Create configuration from dictionary."""
        # Convert string to enum
        if isinstance(data.get("estimation_type"), str):
            data["estimation_type"] = EstimationType(data["estimation_type"])
        
        if isinstance(data.get("execution_mode"), str):
            data["execution_mode"] = ExecutionMode[data["execution_mode"].upper()]
        
        return cls(**data)
    
    def to_json(self, path: Optional[Path] = None) -> str:
        """
        Export configuration to JSON.
        
        Parameters
        ----------
        path : Optional[Path]
            Path to save JSON file (if None, returns string)
            
        Returns
        -------
        str
            JSON string representation
        """
        json_str = json.dumps(self.to_dict(), indent=2, default=str)
        
        if path:
            with open(path, "w") as f:
                f.write(json_str)
        
        return json_str
    
    @classmethod
    def from_json(cls, json_str: Union[str, Path]) -> "PipelineConfig":
        """
        Load configuration from JSON.
        
        Parameters
        ----------
        json_str : Union[str, Path]
            JSON string or path to JSON file
            
        Returns
        -------
        PipelineConfig
            Configuration object
        """
        if isinstance(json_str, Path) or (isinstance(json_str, str) and json_str.endswith(".json")):
            with open(json_str, "r") as f:
                data = json.load(f)
        else:
            data = json.loads(json_str)
        
        return cls.from_dict(data)


class EstimationPipelineFactory:
    """
    Central factory for creating estimation pipelines.
    
    Provides multiple methods for pipeline creation including
    auto-detection, configuration-based, and template-based.
    """
    
    # Builder mappings
    _builders: Dict[EstimationType, Type[PipelineBuilder]] = {
        EstimationType.VOLUME: VolumeEstimationBuilder,
        EstimationType.BIOMASS: BiomassEstimationBuilder,
        EstimationType.TPA: TPAEstimationBuilder,
        EstimationType.AREA: AreaEstimationBuilder,
        EstimationType.GROWTH: GrowthEstimationBuilder,
        EstimationType.MORTALITY: MortalityEstimationBuilder,
    }
    
    # Quick start function mappings
    _quick_start_funcs = {
        EstimationType.VOLUME: create_volume_pipeline,
        EstimationType.BIOMASS: create_biomass_pipeline,
        EstimationType.TPA: create_tpa_pipeline,
        EstimationType.AREA: create_area_pipeline,
        EstimationType.GROWTH: create_growth_pipeline,
        EstimationType.MORTALITY: create_mortality_pipeline,
    }
    
    @classmethod
    def create_pipeline(
        cls,
        estimation_type: Union[str, EstimationType],
        **kwargs
    ) -> EstimationPipeline:
        """
        Create a pipeline for the specified estimation type.
        
        Parameters
        ----------
        estimation_type : Union[str, EstimationType]
            Type of estimation
        **kwargs
            Pipeline configuration parameters
            
        Returns
        -------
        EstimationPipeline
            Configured pipeline
            
        Examples
        --------
        >>> # Create volume pipeline
        >>> pipeline = EstimationPipelineFactory.create_pipeline(
        ...     "volume",
        ...     by_species=True,
        ...     tree_domain="DIA >= 10.0"
        ... )
        """
        if isinstance(estimation_type, str):
            estimation_type = EstimationType(estimation_type.lower())
        
        if estimation_type not in cls._quick_start_funcs:
            raise ValueError(f"Unknown estimation type: {estimation_type}")
        
        func = cls._quick_start_funcs[estimation_type]
        return func(**kwargs)
    
    @classmethod
    def create_from_config(
        cls,
        config: Union[PipelineConfig, Dict[str, Any], str, Path]
    ) -> EstimationPipeline:
        """
        Create a pipeline from configuration.
        
        Parameters
        ----------
        config : Union[PipelineConfig, Dict[str, Any], str, Path]
            Pipeline configuration (object, dict, JSON string, or file path)
            
        Returns
        -------
        EstimationPipeline
            Configured pipeline
            
        Examples
        --------
        >>> # From config object
        >>> config = PipelineConfig(
        ...     estimation_type=EstimationType.VOLUME,
        ...     by_species=True
        ... )
        >>> pipeline = EstimationPipelineFactory.create_from_config(config)
        
        >>> # From dictionary
        >>> config_dict = {
        ...     "estimation_type": "biomass",
        ...     "component": "aboveground",
        ...     "by_species": True
        ... }
        >>> pipeline = EstimationPipelineFactory.create_from_config(config_dict)
        
        >>> # From JSON file
        >>> pipeline = EstimationPipelineFactory.create_from_config("config.json")
        """
        # Convert to PipelineConfig if needed
        if isinstance(config, dict):
            config = PipelineConfig.from_dict(config)
        elif isinstance(config, (str, Path)):
            config = PipelineConfig.from_json(config)
        
        # Extract parameters
        params = config.to_dict()
        estimation_type = params.pop("estimation_type")
        
        # Create pipeline
        return cls.create_pipeline(estimation_type, **params)
    
    @classmethod
    def create_from_template(
        cls,
        template_name: str,
        **kwargs
    ) -> EstimationPipeline:
        """
        Create a pipeline from a template.
        
        Parameters
        ----------
        template_name : str
            Name of the template
        **kwargs
            Override parameters
            
        Returns
        -------
        EstimationPipeline
            Configured pipeline
            
        Examples
        --------
        >>> # Use basic template
        >>> pipeline = EstimationPipelineFactory.create_from_template(
        ...     "basic_volume"
        ... )
        
        >>> # Use template with overrides
        >>> pipeline = EstimationPipelineFactory.create_from_template(
        ...     "volume_by_species",
        ...     tree_domain="DIA >= 15.0"
        ... )
        """
        template = get_template(template_name)
        return template.create_pipeline(**kwargs)
    
    @classmethod
    def auto_detect_pipeline(
        cls,
        parameters: Dict[str, Any]
    ) -> EstimationPipeline:
        """
        Auto-detect the appropriate pipeline type from parameters.
        
        This method analyzes the provided parameters to determine
        the most appropriate estimation type and configuration.
        
        Parameters
        ----------
        parameters : Dict[str, Any]
            Analysis parameters
            
        Returns
        -------
        EstimationPipeline
            Automatically configured pipeline
            
        Examples
        --------
        >>> # Auto-detect from parameters
        >>> params = {
        ...     "by_species": True,
        ...     "component": "aboveground"  # Indicates biomass
        ... }
        >>> pipeline = EstimationPipelineFactory.auto_detect_pipeline(params)
        """
        # Detection logic based on parameter signatures
        
        # Check for explicit type indicators
        if "mortality_type" in parameters:
            estimation_type = EstimationType.MORTALITY
        elif "growth_type" in parameters:
            estimation_type = EstimationType.GROWTH
        elif "component" in parameters:
            estimation_type = EstimationType.BIOMASS
        elif "land_type" in parameters:
            estimation_type = EstimationType.AREA
        elif "volume_equations" in parameters:
            estimation_type = EstimationType.VOLUME
        elif "tree_domain" in parameters and not "area_domain" in parameters:
            # Tree-level analysis, default to TPA
            estimation_type = EstimationType.TPA
        else:
            # Default to volume as most common
            estimation_type = EstimationType.VOLUME
        
        return cls.create_pipeline(estimation_type, **parameters)
    
    @classmethod
    def create_builder(
        cls,
        estimation_type: Union[str, EstimationType],
        **kwargs
    ) -> PipelineBuilder:
        """
        Create a pipeline builder for custom pipeline construction.
        
        Parameters
        ----------
        estimation_type : Union[str, EstimationType]
            Type of estimation
        **kwargs
            Builder configuration
            
        Returns
        -------
        PipelineBuilder
            Configured builder
            
        Examples
        --------
        >>> # Create custom builder
        >>> builder = EstimationPipelineFactory.create_builder("volume")
        >>> builder.skip_step("calculate_variance")
        >>> pipeline = builder.build(by_species=True)
        """
        if isinstance(estimation_type, str):
            estimation_type = EstimationType(estimation_type.lower())
        
        if estimation_type not in cls._builders:
            raise ValueError(f"Unknown estimation type: {estimation_type}")
        
        builder_class = cls._builders[estimation_type]
        return builder_class(**kwargs)
    
    @classmethod
    def validate_pipeline_config(
        cls,
        config: Union[PipelineConfig, Dict[str, Any]]
    ) -> List[str]:
        """
        Validate a pipeline configuration.
        
        Parameters
        ----------
        config : Union[PipelineConfig, Dict[str, Any]]
            Configuration to validate
            
        Returns
        -------
        List[str]
            List of validation issues (empty if valid)
            
        Examples
        --------
        >>> config = {"estimation_type": "volume", "by_species": True}
        >>> issues = EstimationPipelineFactory.validate_pipeline_config(config)
        >>> if not issues:
        ...     print("Configuration is valid")
        """
        issues = []
        
        # Convert to dict if needed
        if isinstance(config, PipelineConfig):
            config = config.to_dict()
        
        # Check required fields
        if "estimation_type" not in config:
            issues.append("Missing required field: estimation_type")
        else:
            # Validate estimation type
            try:
                EstimationType(config["estimation_type"])
            except ValueError:
                issues.append(f"Invalid estimation type: {config['estimation_type']}")
        
        # Check for conflicting parameters
        if config.get("estimation_type") == "area":
            if "tree_domain" in config:
                issues.append("Area estimation does not use tree_domain")
        
        # Validate domain filters
        for domain_key in ["tree_domain", "area_domain", "plot_domain"]:
            if domain_key in config and config[domain_key]:
                # Basic SQL syntax validation
                domain = config[domain_key]
                if not isinstance(domain, str):
                    issues.append(f"{domain_key} must be a string")
                elif ";" in domain:
                    issues.append(f"{domain_key} cannot contain semicolons")
        
        # Validate grouping parameters
        if "grp_by" in config and config["grp_by"]:
            if not isinstance(config["grp_by"], list):
                issues.append("grp_by must be a list")
            elif not all(isinstance(col, str) for col in config["grp_by"]):
                issues.append("All grp_by columns must be strings")
        
        # Validate temporal method
        if "temporal_method" in config:
            valid_methods = ["TI", "annual", "SMA", "LMA", "EMA"]
            if config["temporal_method"] not in valid_methods:
                issues.append(f"Invalid temporal_method: {config['temporal_method']}")
        
        # Validate execution mode
        if "execution_mode" in config:
            try:
                if isinstance(config["execution_mode"], str):
                    ExecutionMode[config["execution_mode"].upper()]
            except (KeyError, AttributeError):
                issues.append(f"Invalid execution_mode: {config['execution_mode']}")
        
        # Module-specific validation
        est_type = config.get("estimation_type")
        
        if est_type == "biomass" and "module_config" in config:
            module_config = config["module_config"]
            if "component" in module_config:
                valid_components = ["aboveground", "belowground", "total"]
                if module_config["component"] not in valid_components:
                    issues.append(f"Invalid biomass component: {module_config['component']}")
        
        if est_type == "mortality" and "mortality_type" in config:
            valid_types = ["volume", "biomass", "tpa"]
            if config["mortality_type"] not in valid_types:
                issues.append(f"Invalid mortality_type: {config['mortality_type']}")
        
        return issues
    
    @classmethod
    def get_available_builders(cls) -> List[str]:
        """
        Get list of available pipeline builders.
        
        Returns
        -------
        List[str]
            Available estimation types
        """
        return [e.value for e in EstimationType]
    
    @classmethod
    def get_builder_info(
        cls,
        estimation_type: Union[str, EstimationType]
    ) -> Dict[str, Any]:
        """
        Get information about a pipeline builder.
        
        Parameters
        ----------
        estimation_type : Union[str, EstimationType]
            Type of estimation
            
        Returns
        -------
        Dict[str, Any]
            Builder information
        """
        if isinstance(estimation_type, str):
            estimation_type = EstimationType(estimation_type.lower())
        
        builder_class = cls._builders.get(estimation_type)
        
        if not builder_class:
            raise ValueError(f"Unknown estimation type: {estimation_type}")
        
        return {
            "name": estimation_type.value,
            "builder_class": builder_class.__name__,
            "description": builder_class.__doc__.strip() if builder_class.__doc__ else "",
            "supports_species_grouping": estimation_type != EstimationType.AREA,
            "supports_size_class_grouping": estimation_type != EstimationType.AREA,
            "requires_tree_data": estimation_type != EstimationType.AREA,
        }


class PipelineOptimizer:
    """
    Optimizer for pipeline configurations.
    
    Analyzes pipeline configurations and suggests optimizations
    for better performance.
    """
    
    @staticmethod
    def optimize_config(
        config: Union[PipelineConfig, Dict[str, Any]]
    ) -> PipelineConfig:
        """
        Optimize a pipeline configuration for performance.
        
        Parameters
        ----------
        config : Union[PipelineConfig, Dict[str, Any]]
            Configuration to optimize
            
        Returns
        -------
        PipelineConfig
            Optimized configuration
        """
        # Convert to PipelineConfig if needed
        if isinstance(config, dict):
            config = PipelineConfig.from_dict(config)
        
        # Apply optimizations
        
        # 1. Enable caching by default
        config.enable_caching = True
        
        # 2. Use parallel execution for complex groupings
        if config.grp_by and len(config.grp_by) > 1:
            config.execution_mode = ExecutionMode.PARALLEL
        elif config.by_species and config.by_size_class:
            config.execution_mode = ExecutionMode.PARALLEL
        
        # 3. Skip unnecessary steps
        if not config.skip_steps:
            config.skip_steps = []
        
        # Skip variance if not needed
        if not config.variance and "calculate_variance" not in config.skip_steps:
            config.skip_steps.append("calculate_variance")
        
        # Skip totals if not needed
        if not config.totals and "calculate_totals" not in config.skip_steps:
            config.skip_steps.append("calculate_totals")
        
        return config
    
    @staticmethod
    def suggest_optimizations(
        config: Union[PipelineConfig, Dict[str, Any]]
    ) -> List[str]:
        """
        Suggest optimizations for a pipeline configuration.
        
        Parameters
        ----------
        config : Union[PipelineConfig, Dict[str, Any]]
            Configuration to analyze
            
        Returns
        -------
        List[str]
            Optimization suggestions
        """
        suggestions = []
        
        # Convert to dict if needed
        if isinstance(config, PipelineConfig):
            config = config.to_dict()
        
        # Check execution mode
        if config.get("execution_mode") == "SEQUENTIAL":
            if config.get("grp_by") and len(config["grp_by"]) > 1:
                suggestions.append(
                    "Consider using PARALLEL execution mode for multiple grouping columns"
                )
            if config.get("by_species") and config.get("by_size_class"):
                suggestions.append(
                    "Consider using PARALLEL execution mode for species + size class grouping"
                )
        
        # Check caching
        if not config.get("enable_caching", True):
            suggestions.append("Enable caching for better performance with repeated operations")
        
        # Check variance calculation
        if config.get("variance") and config.get("estimation_type") in ["area"]:
            if not config.get("grp_by"):
                suggestions.append(
                    "Area estimation without grouping may not need variance calculation"
                )
        
        # Check domain filters
        if config.get("tree_domain"):
            domain = config["tree_domain"]
            if "OR" in domain.upper():
                suggestions.append(
                    "Consider splitting OR conditions into separate pipelines for better optimization"
                )
        
        # Check temporal settings
        if config.get("temporal_method") != "TI":
            if not config.get("grp_by") or "INVYR" not in config.get("grp_by", []):
                suggestions.append(
                    f"Temporal method '{config['temporal_method']}' typically requires grouping by INVYR"
                )
        
        return suggestions


# === Convenience Functions ===

def create_pipeline(estimation_type: str, **kwargs) -> EstimationPipeline:
    """Convenience function to create a pipeline."""
    return EstimationPipelineFactory.create_pipeline(estimation_type, **kwargs)

def create_from_config(config: Union[PipelineConfig, Dict, str, Path]) -> EstimationPipeline:
    """Convenience function to create from configuration."""
    return EstimationPipelineFactory.create_from_config(config)

def create_from_template(template_name: str, **kwargs) -> EstimationPipeline:
    """Convenience function to create from template."""
    return EstimationPipelineFactory.create_from_template(template_name, **kwargs)

def auto_detect_pipeline(parameters: Dict[str, Any]) -> EstimationPipeline:
    """Convenience function for auto-detection."""
    return EstimationPipelineFactory.auto_detect_pipeline(parameters)

def validate_config(config: Union[PipelineConfig, Dict[str, Any]]) -> List[str]:
    """Convenience function for validation."""
    return EstimationPipelineFactory.validate_pipeline_config(config)

def optimize_config(config: Union[PipelineConfig, Dict[str, Any]]) -> PipelineConfig:
    """Convenience function for optimization."""
    return PipelineOptimizer.optimize_config(config)