"""
Pipeline templates for common FIA estimation workflows.

This module provides pre-configured pipeline templates that implement
standard FIA estimation patterns. Templates can be used directly or
customized for specific use cases.
"""

from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field
from enum import Enum

from .core import EstimationPipeline, ExecutionMode
from .builders import (
    VolumeEstimationBuilder,
    BiomassEstimationBuilder,
    TPAEstimationBuilder,
    AreaEstimationBuilder,
    GrowthEstimationBuilder,
    MortalityEstimationBuilder,
)


class TemplateCategory(Enum):
    """Categories of pipeline templates."""
    
    BASIC = "basic"
    SPECIES = "species"
    OWNERSHIP = "ownership"
    TEMPORAL = "temporal"
    CUSTOM_DOMAIN = "custom_domain"
    ADVANCED = "advanced"


@dataclass
class PipelineTemplate:
    """
    A pre-configured pipeline template.
    
    Encapsulates a complete pipeline configuration that can be
    instantiated with minimal parameters.
    """
    
    name: str
    description: str
    category: TemplateCategory
    estimation_type: str
    builder_class: type
    default_config: Dict[str, Any] = field(default_factory=dict)
    required_params: List[str] = field(default_factory=list)
    optional_params: List[str] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)
    
    def create_pipeline(self, **kwargs) -> EstimationPipeline:
        """
        Create a pipeline instance from this template.
        
        Parameters
        ----------
        **kwargs
            Pipeline configuration parameters
            
        Returns
        -------
        EstimationPipeline
            Configured pipeline instance
        """
        # Check required parameters
        missing = [p for p in self.required_params if p not in kwargs]
        if missing:
            raise ValueError(f"Missing required parameters: {missing}")
        
        # Merge with default config
        config = {**self.default_config, **kwargs}
        
        # Create builder and pipeline
        builder = self.builder_class()
        return builder.build(**config)
    
    def validate_params(self, **kwargs) -> bool:
        """
        Validate parameters against template requirements.
        
        Parameters
        ----------
        **kwargs
            Parameters to validate
            
        Returns
        -------
        bool
            Whether parameters are valid
        """
        return all(p in kwargs for p in self.required_params)


class TemplateRegistry:
    """
    Registry of available pipeline templates.
    
    Provides a centralized catalog of templates with search
    and filtering capabilities.
    """
    
    def __init__(self):
        """Initialize template registry."""
        self._templates: Dict[str, PipelineTemplate] = {}
        self._register_standard_templates()
    
    def register(self, template: PipelineTemplate) -> None:
        """
        Register a new template.
        
        Parameters
        ----------
        template : PipelineTemplate
            Template to register
        """
        if template.name in self._templates:
            raise ValueError(f"Template '{template.name}' already registered")
        self._templates[template.name] = template
    
    def get(self, name: str) -> PipelineTemplate:
        """
        Get a template by name.
        
        Parameters
        ----------
        name : str
            Template name
            
        Returns
        -------
        PipelineTemplate
            Template instance
        """
        if name not in self._templates:
            raise ValueError(f"Template '{name}' not found")
        return self._templates[name]
    
    def list_templates(
        self,
        category: Optional[TemplateCategory] = None,
        estimation_type: Optional[str] = None,
        tags: Optional[List[str]] = None
    ) -> List[PipelineTemplate]:
        """
        List available templates with optional filtering.
        
        Parameters
        ----------
        category : Optional[TemplateCategory]
            Filter by category
        estimation_type : Optional[str]
            Filter by estimation type
        tags : Optional[List[str]]
            Filter by tags (any match)
            
        Returns
        -------
        List[PipelineTemplate]
            Matching templates
        """
        templates = list(self._templates.values())
        
        if category:
            templates = [t for t in templates if t.category == category]
        
        if estimation_type:
            templates = [t for t in templates if t.estimation_type == estimation_type]
        
        if tags:
            templates = [
                t for t in templates
                if any(tag in t.tags for tag in tags)
            ]
        
        return templates
    
    def get_template_info(self, name: str) -> Dict[str, Any]:
        """
        Get detailed information about a template.
        
        Parameters
        ----------
        name : str
            Template name
            
        Returns
        -------
        Dict[str, Any]
            Template information
        """
        template = self.get(name)
        return {
            "name": template.name,
            "description": template.description,
            "category": template.category.value,
            "estimation_type": template.estimation_type,
            "required_params": template.required_params,
            "optional_params": template.optional_params,
            "default_config": template.default_config,
            "tags": template.tags,
        }
    
    def _register_standard_templates(self) -> None:
        """Register standard FIA estimation templates."""
        
        # === Basic Templates ===
        
        self.register(PipelineTemplate(
            name="basic_volume",
            description="Basic volume estimation without grouping",
            category=TemplateCategory.BASIC,
            estimation_type="volume",
            builder_class=VolumeEstimationBuilder,
            default_config={
                "variance": True,
                "totals": False,
            },
            optional_params=["tree_domain", "area_domain", "plot_domain"],
            tags=["volume", "basic", "simple"],
        ))
        
        self.register(PipelineTemplate(
            name="basic_biomass",
            description="Basic biomass estimation without grouping",
            category=TemplateCategory.BASIC,
            estimation_type="biomass",
            builder_class=BiomassEstimationBuilder,
            default_config={
                "variance": True,
                "totals": False,
                "module_config": {"component": "aboveground"},
            },
            optional_params=["tree_domain", "area_domain", "plot_domain"],
            tags=["biomass", "basic", "simple"],
        ))
        
        self.register(PipelineTemplate(
            name="basic_tpa",
            description="Basic trees per acre estimation",
            category=TemplateCategory.BASIC,
            estimation_type="tpa",
            builder_class=TPAEstimationBuilder,
            default_config={
                "variance": True,
                "totals": False,
            },
            optional_params=["tree_domain", "area_domain", "plot_domain"],
            tags=["tpa", "basic", "simple"],
        ))
        
        self.register(PipelineTemplate(
            name="basic_area",
            description="Basic area estimation",
            category=TemplateCategory.BASIC,
            estimation_type="area",
            builder_class=AreaEstimationBuilder,
            default_config={
                "variance": True,
                "land_type": "forest",
            },
            optional_params=["area_domain", "plot_domain"],
            tags=["area", "basic", "simple"],
        ))
        
        # === Species Templates ===
        
        self.register(PipelineTemplate(
            name="volume_by_species",
            description="Volume estimation grouped by species",
            category=TemplateCategory.SPECIES,
            estimation_type="volume",
            builder_class=VolumeEstimationBuilder,
            default_config={
                "by_species": True,
                "variance": True,
                "totals": True,
            },
            optional_params=["tree_domain", "area_domain", "plot_domain"],
            tags=["volume", "species", "grouping"],
        ))
        
        self.register(PipelineTemplate(
            name="biomass_by_species",
            description="Biomass estimation grouped by species",
            category=TemplateCategory.SPECIES,
            estimation_type="biomass",
            builder_class=BiomassEstimationBuilder,
            default_config={
                "by_species": True,
                "variance": True,
                "totals": True,
                "module_config": {"component": "aboveground"},
            },
            optional_params=["tree_domain", "area_domain", "plot_domain"],
            tags=["biomass", "species", "grouping"],
        ))
        
        self.register(PipelineTemplate(
            name="tpa_by_species",
            description="Trees per acre grouped by species",
            category=TemplateCategory.SPECIES,
            estimation_type="tpa",
            builder_class=TPAEstimationBuilder,
            default_config={
                "by_species": True,
                "variance": True,
                "totals": True,
            },
            optional_params=["tree_domain", "area_domain", "plot_domain"],
            tags=["tpa", "species", "grouping"],
        ))
        
        self.register(PipelineTemplate(
            name="mortality_by_species",
            description="Mortality estimation grouped by species",
            category=TemplateCategory.SPECIES,
            estimation_type="mortality",
            builder_class=MortalityEstimationBuilder,
            default_config={
                "by_species": True,
                "variance": True,
                "mortality_type": "volume",
            },
            optional_params=["tree_domain", "area_domain", "plot_domain"],
            tags=["mortality", "species", "grouping"],
        ))
        
        # === Ownership Templates ===
        
        self.register(PipelineTemplate(
            name="volume_by_ownership",
            description="Volume estimation grouped by ownership class",
            category=TemplateCategory.OWNERSHIP,
            estimation_type="volume",
            builder_class=VolumeEstimationBuilder,
            default_config={
                "grp_by": ["OWNGRPCD"],
                "variance": True,
                "totals": True,
            },
            optional_params=["tree_domain", "area_domain", "plot_domain"],
            tags=["volume", "ownership", "grouping"],
        ))
        
        self.register(PipelineTemplate(
            name="area_by_ownership",
            description="Area estimation grouped by ownership class",
            category=TemplateCategory.OWNERSHIP,
            estimation_type="area",
            builder_class=AreaEstimationBuilder,
            default_config={
                "grp_by": ["OWNGRPCD"],
                "variance": True,
                "land_type": "forest",
            },
            optional_params=["area_domain", "plot_domain"],
            tags=["area", "ownership", "grouping"],
        ))
        
        # === Temporal Templates ===
        
        self.register(PipelineTemplate(
            name="annual_volume_trend",
            description="Annual volume trends over time",
            category=TemplateCategory.TEMPORAL,
            estimation_type="volume",
            builder_class=VolumeEstimationBuilder,
            default_config={
                "temporal_method": "annual",
                "grp_by": ["INVYR"],
                "variance": True,
            },
            optional_params=["tree_domain", "area_domain", "plot_domain"],
            tags=["volume", "temporal", "trend", "annual"],
        ))
        
        self.register(PipelineTemplate(
            name="growth_trend",
            description="Growth estimation over time periods",
            category=TemplateCategory.TEMPORAL,
            estimation_type="growth",
            builder_class=GrowthEstimationBuilder,
            default_config={
                "temporal_method": "annual",
                "variance": True,
            },
            optional_params=["tree_domain", "area_domain", "plot_domain"],
            tags=["growth", "temporal", "trend"],
        ))
        
        self.register(PipelineTemplate(
            name="mortality_trend",
            description="Mortality trends over time",
            category=TemplateCategory.TEMPORAL,
            estimation_type="mortality",
            builder_class=MortalityEstimationBuilder,
            default_config={
                "temporal_method": "annual",
                "grp_by": ["INVYR"],
                "variance": True,
                "mortality_type": "volume",
            },
            optional_params=["tree_domain", "area_domain", "plot_domain"],
            tags=["mortality", "temporal", "trend"],
        ))
        
        # === Custom Domain Templates ===
        
        self.register(PipelineTemplate(
            name="large_tree_volume",
            description="Volume of large trees (DBH >= 20 inches)",
            category=TemplateCategory.CUSTOM_DOMAIN,
            estimation_type="volume",
            builder_class=VolumeEstimationBuilder,
            default_config={
                "tree_domain": "DIA >= 20.0",
                "variance": True,
                "by_species": True,
            },
            optional_params=["area_domain", "plot_domain"],
            tags=["volume", "large_trees", "custom"],
        ))
        
        self.register(PipelineTemplate(
            name="hardwood_biomass",
            description="Biomass of hardwood species",
            category=TemplateCategory.CUSTOM_DOMAIN,
            estimation_type="biomass",
            builder_class=BiomassEstimationBuilder,
            default_config={
                "tree_domain": "SPGRPCD == 2",  # Hardwood species group
                "variance": True,
                "by_species": True,
                "module_config": {"component": "aboveground"},
            },
            optional_params=["area_domain", "plot_domain"],
            tags=["biomass", "hardwood", "custom"],
        ))
        
        self.register(PipelineTemplate(
            name="pine_mortality",
            description="Mortality in pine species",
            category=TemplateCategory.CUSTOM_DOMAIN,
            estimation_type="mortality",
            builder_class=MortalityEstimationBuilder,
            default_config={
                "tree_domain": "SPCD >= 100 AND SPCD < 200",  # Pine species codes
                "variance": True,
                "mortality_type": "volume",
            },
            optional_params=["area_domain", "plot_domain"],
            tags=["mortality", "pine", "custom"],
        ))
        
        # === Advanced Templates ===
        
        self.register(PipelineTemplate(
            name="comprehensive_forest_inventory",
            description="Complete forest inventory with all metrics",
            category=TemplateCategory.ADVANCED,
            estimation_type="volume",  # Primary estimation type
            builder_class=VolumeEstimationBuilder,
            default_config={
                "by_species": True,
                "by_size_class": True,
                "grp_by": ["OWNGRPCD", "FORTYPCD"],
                "variance": True,
                "totals": True,
            },
            optional_params=["tree_domain", "area_domain", "plot_domain"],
            tags=["comprehensive", "inventory", "advanced"],
        ))
        
        self.register(PipelineTemplate(
            name="carbon_assessment",
            description="Carbon stock assessment using biomass",
            category=TemplateCategory.ADVANCED,
            estimation_type="biomass",
            builder_class=BiomassEstimationBuilder,
            default_config={
                "module_config": {"component": "total"},  # Total biomass for carbon
                "by_species": True,
                "grp_by": ["STDORGCD"],  # Stand origin
                "variance": True,
                "totals": True,
            },
            optional_params=["tree_domain", "area_domain", "plot_domain"],
            tags=["carbon", "biomass", "advanced"],
        ))
        
        self.register(PipelineTemplate(
            name="disturbance_impact",
            description="Assessment of disturbance impacts on mortality",
            category=TemplateCategory.ADVANCED,
            estimation_type="mortality",
            builder_class=MortalityEstimationBuilder,
            default_config={
                "grp_by": ["DSTRBCD1", "DSTRBYR1"],  # Disturbance code and year
                "variance": True,
                "mortality_type": "biomass",
                "temporal_method": "annual",
            },
            optional_params=["tree_domain", "area_domain", "plot_domain"],
            tags=["disturbance", "mortality", "advanced"],
        ))


class TemplateCustomizer:
    """
    Utility for customizing pipeline templates.
    
    Provides methods to modify templates for specific use cases
    while maintaining their core functionality.
    """
    
    @staticmethod
    def add_custom_domain(
        template: PipelineTemplate,
        tree_domain: Optional[str] = None,
        area_domain: Optional[str] = None,
        plot_domain: Optional[str] = None
    ) -> PipelineTemplate:
        """
        Add custom domain filters to a template.
        
        Parameters
        ----------
        template : PipelineTemplate
            Template to customize
        tree_domain : Optional[str]
            Tree-level domain filter
        area_domain : Optional[str]
            Area-level domain filter
        plot_domain : Optional[str]
            Plot-level domain filter
            
        Returns
        -------
        PipelineTemplate
            Customized template
        """
        import copy
        custom = copy.deepcopy(template)
        
        if tree_domain:
            custom.default_config["tree_domain"] = tree_domain
        if area_domain:
            custom.default_config["area_domain"] = area_domain
        if plot_domain:
            custom.default_config["plot_domain"] = plot_domain
        
        custom.name = f"{template.name}_custom"
        custom.description = f"{template.description} (customized)"
        
        return custom
    
    @staticmethod
    def add_grouping(
        template: PipelineTemplate,
        grp_by: Optional[List[str]] = None,
        by_species: bool = False,
        by_size_class: bool = False
    ) -> PipelineTemplate:
        """
        Add grouping options to a template.
        
        Parameters
        ----------
        template : PipelineTemplate
            Template to customize
        grp_by : Optional[List[str]]
            Columns to group by
        by_species : bool
            Whether to group by species
        by_size_class : bool
            Whether to group by size class
            
        Returns
        -------
        PipelineTemplate
            Customized template
        """
        import copy
        custom = copy.deepcopy(template)
        
        if grp_by:
            custom.default_config["grp_by"] = grp_by
        if by_species:
            custom.default_config["by_species"] = True
        if by_size_class:
            custom.default_config["by_size_class"] = True
        
        custom.name = f"{template.name}_grouped"
        custom.description = f"{template.description} (with grouping)"
        
        return custom
    
    @staticmethod
    def set_execution_mode(
        template: PipelineTemplate,
        mode: ExecutionMode,
        enable_caching: bool = True
    ) -> PipelineTemplate:
        """
        Set the execution mode for a template.
        
        Parameters
        ----------
        template : PipelineTemplate
            Template to customize
        mode : ExecutionMode
            Execution mode to use
        enable_caching : bool
            Whether to enable caching
            
        Returns
        -------
        PipelineTemplate
            Customized template
        """
        import copy
        custom = copy.deepcopy(template)
        
        custom.default_config["execution_mode"] = mode
        custom.default_config["enable_caching"] = enable_caching
        
        return custom


# === Template Selection Helpers ===

def select_template(
    estimation_type: str,
    complexity: str = "basic",
    grouping: Optional[str] = None
) -> PipelineTemplate:
    """
    Select an appropriate template based on requirements.
    
    Parameters
    ----------
    estimation_type : str
        Type of estimation (volume, biomass, etc.)
    complexity : str
        Complexity level (basic, intermediate, advanced)
    grouping : Optional[str]
        Grouping type (species, ownership, temporal)
        
    Returns
    -------
    PipelineTemplate
        Selected template
    """
    registry = TemplateRegistry()
    
    # Build template name
    if complexity == "basic" and not grouping:
        name = f"basic_{estimation_type}"
    elif grouping == "species":
        name = f"{estimation_type}_by_species"
    elif grouping == "ownership":
        name = f"{estimation_type}_by_ownership"
    elif grouping == "temporal":
        if estimation_type == "volume":
            name = "annual_volume_trend"
        elif estimation_type == "growth":
            name = "growth_trend"
        elif estimation_type == "mortality":
            name = "mortality_trend"
        else:
            name = f"basic_{estimation_type}"
    elif complexity == "advanced":
        if estimation_type == "volume":
            name = "comprehensive_forest_inventory"
        elif estimation_type == "biomass":
            name = "carbon_assessment"
        elif estimation_type == "mortality":
            name = "disturbance_impact"
        else:
            name = f"basic_{estimation_type}"
    else:
        name = f"basic_{estimation_type}"
    
    try:
        return registry.get(name)
    except ValueError:
        # Fallback to basic template
        return registry.get(f"basic_{estimation_type}")


def get_recommended_templates(
    use_case: str
) -> List[PipelineTemplate]:
    """
    Get recommended templates for a specific use case.
    
    Parameters
    ----------
    use_case : str
        Use case description
        
    Returns
    -------
    List[PipelineTemplate]
        Recommended templates
    """
    registry = TemplateRegistry()
    
    use_case_lower = use_case.lower()
    
    if "carbon" in use_case_lower or "climate" in use_case_lower:
        return registry.list_templates(tags=["carbon", "biomass"])
    elif "inventory" in use_case_lower:
        return registry.list_templates(tags=["comprehensive", "inventory"])
    elif "mortality" in use_case_lower or "dead" in use_case_lower:
        return registry.list_templates(estimation_type="mortality")
    elif "growth" in use_case_lower:
        return registry.list_templates(estimation_type="growth")
    elif "species" in use_case_lower:
        return registry.list_templates(category=TemplateCategory.SPECIES)
    elif "trend" in use_case_lower or "temporal" in use_case_lower:
        return registry.list_templates(category=TemplateCategory.TEMPORAL)
    else:
        return registry.list_templates(category=TemplateCategory.BASIC)


# Create global registry instance
_global_registry = TemplateRegistry()

def get_template(name: str) -> PipelineTemplate:
    """Get a template from the global registry."""
    return _global_registry.get(name)

def list_templates(**kwargs) -> List[PipelineTemplate]:
    """List templates from the global registry."""
    return _global_registry.list_templates(**kwargs)

def register_template(template: PipelineTemplate) -> None:
    """Register a template in the global registry."""
    _global_registry.register(template)