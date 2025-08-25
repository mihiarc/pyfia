"""
Pipeline builders for common FIA estimation workflows.

This module provides high-level builders that create complete pipelines
for standard FIA estimation types, integrating all the necessary steps
with proper configuration and optimization.
"""

from typing import Dict, List, Optional, Any, Type, Callable
from abc import ABC, abstractmethod

from ...core import FIA
from ..config import EstimatorConfig, VolumeConfig, BiomassConfig, GrowthConfig, AreaConfig, MortalityConfig
from .core import EstimationPipeline, ExecutionMode, PipelineStep
from .steps import (
    LoadRequiredTablesStep,
    ApplyDomainFiltersStep,
    ApplyModuleFiltersStep,
    PrepareEstimationDataStep,
)
from .steps_calculations import (
    CalculateTreeVolumesStep,
    CalculateBiomassStep,
    CalculateTPAStep,
    CalculateAreaStep,
    CalculateGrowthStep,
    CalculateMortalityStep,
    AggregateByPlotStep,
    ApplyStratificationStep,
    CalculatePopulationEstimatesStep,
    ApplyVarianceCalculationStep,
    FormatOutputStep,
    ValidateOutputStep,
)


# === Abstract Pipeline Builder ===

class PipelineBuilder(ABC):
    """
    Abstract base class for pipeline builders.
    
    Provides the framework for building estimation pipelines
    with consistent patterns and proper integration of components.
    """
    
    def __init__(
        self,
        estimation_type: str,
        config_class: Type[EstimatorConfig] = EstimatorConfig,
        execution_mode: ExecutionMode = ExecutionMode.SEQUENTIAL,
        enable_caching: bool = True,
        debug: bool = False
    ):
        """
        Initialize pipeline builder.
        
        Parameters
        ----------
        estimation_type : str
            Type of estimation (volume, biomass, etc.)
        config_class : Type[EstimatorConfig]
            Configuration class to use
        execution_mode : ExecutionMode
            Pipeline execution mode
        enable_caching : bool
            Whether to enable caching
        debug : bool
            Whether to enable debug mode
        """
        self.estimation_type = estimation_type
        self.config_class = config_class
        self.execution_mode = execution_mode
        self.enable_caching = enable_caching
        self.debug = debug
        
        # Pipeline customization options
        self.custom_steps: List[PipelineStep] = []
        self.step_overrides: Dict[str, PipelineStep] = {}
        self.skip_steps: List[str] = []
        
    @abstractmethod
    def get_value_calculation_step(self, config: EstimatorConfig) -> PipelineStep:
        """
        Get the value calculation step for this estimation type.
        
        Parameters
        ----------
        config : EstimatorConfig
            Estimation configuration
            
        Returns
        -------
        PipelineStep
            Value calculation step
        """
        pass
    
    def add_custom_step(self, step: PipelineStep, position: Optional[int] = None) -> "PipelineBuilder":
        """
        Add a custom step to the pipeline.
        
        Parameters
        ----------
        step : PipelineStep
            Custom step to add
        position : Optional[int]
            Position to insert step (None = append)
            
        Returns
        -------
        PipelineBuilder
            Self for method chaining
        """
        if position is None:
            self.custom_steps.append(step)
        else:
            self.custom_steps.insert(position, step)
        return self
    
    def override_step(self, step_name: str, step: PipelineStep) -> "PipelineBuilder":
        """
        Override a default step with a custom implementation.
        
        Parameters
        ----------
        step_name : str
            Name of step to override
        step : PipelineStep
            Replacement step
            
        Returns
        -------
        PipelineBuilder
            Self for method chaining
        """
        self.step_overrides[step_name] = step
        return self
    
    def skip_step(self, step_name: str) -> "PipelineBuilder":
        """
        Skip a default step.
        
        Parameters
        ----------
        step_name : str
            Name of step to skip
            
        Returns
        -------
        PipelineBuilder
            Self for method chaining
        """
        self.skip_steps.append(step_name)
        return self
    
    def build(self, **config_kwargs) -> EstimationPipeline:
        """
        Build the complete estimation pipeline.
        
        Parameters
        ----------
        **config_kwargs
            Configuration parameters
            
        Returns
        -------
        EstimationPipeline
            Configured pipeline ready for execution
        """
        # Create configuration
        config = self.config_class(**config_kwargs)
        
        # Initialize pipeline
        pipeline = EstimationPipeline(
            pipeline_id=f"{self.estimation_type}_pipeline",
            description=f"FIA {self.estimation_type.title()} Estimation Pipeline",
            execution_mode=self.execution_mode,
            enable_caching=self.enable_caching,
            debug=self.debug
        )
        
        # Add standard steps
        self._add_standard_steps(pipeline, config)
        
        # Add custom steps
        for step in self.custom_steps:
            pipeline.add_step(step)
        
        return pipeline
    
    def _add_standard_steps(self, pipeline: EstimationPipeline, config: EstimatorConfig) -> None:
        """
        Add the standard steps for this estimation type.
        
        Parameters
        ----------
        pipeline : EstimationPipeline
            Pipeline to add steps to
        config : EstimatorConfig
            Estimation configuration
        """
        # Step 1: Load required tables
        if "load_tables" not in self.skip_steps:
            step = self.step_overrides.get(
                "load_tables",
                LoadRequiredTablesStep(
                    estimation_type=self.estimation_type,
                    step_id="load_tables",
                    description=f"Load tables required for {self.estimation_type} estimation"
                )
            )
            pipeline.add_step(step)
        
        # Step 2: Apply domain filters
        if "apply_domain_filters" not in self.skip_steps:
            step = self.step_overrides.get(
                "apply_domain_filters",
                ApplyDomainFiltersStep(
                    step_id="apply_domain_filters",
                    description="Apply tree, area, and plot domain filters"
                )
            )
            pipeline.add_step(step)
        
        # Step 3: Apply module-specific filters
        if "apply_module_filters" not in self.skip_steps:
            step = self.step_overrides.get(
                "apply_module_filters",
                ApplyModuleFiltersStep(
                    estimation_type=self.estimation_type,
                    step_id="apply_module_filters",
                    description=f"Apply {self.estimation_type}-specific filters"
                )
            )
            pipeline.add_step(step)
        
        # Step 4: Prepare estimation data (join and setup grouping)
        if "prepare_data" not in self.skip_steps:
            step = self.step_overrides.get(
                "prepare_data",
                PrepareEstimationDataStep(
                    estimation_type=self.estimation_type,
                    step_id="prepare_data",
                    description="Join data and setup grouping columns"
                )
            )
            pipeline.add_step(step)
        
        # Step 5: Calculate values (estimation-specific)
        if "calculate_values" not in self.skip_steps:
            step = self.step_overrides.get(
                "calculate_values",
                self.get_value_calculation_step(config)
            )
            pipeline.add_step(step)
        
        # Step 6: Aggregate to plot level
        if "aggregate_plots" not in self.skip_steps and self.estimation_type != "area":
            step = self.step_overrides.get(
                "aggregate_plots",
                AggregateByPlotStep(
                    step_id="aggregate_plots",
                    description="Aggregate tree values to plot level"
                )
            )
            pipeline.add_step(step)
        
        # Step 7: Apply stratification
        if "apply_stratification" not in self.skip_steps:
            step = self.step_overrides.get(
                "apply_stratification",
                ApplyStratificationStep(
                    step_id="apply_stratification",
                    description="Apply stratification and expansion factors"
                )
            )
            pipeline.add_step(step)
        
        # Step 8: Calculate population estimates
        if "calculate_population" not in self.skip_steps:
            step = self.step_overrides.get(
                "calculate_population",
                CalculatePopulationEstimatesStep(
                    step_id="calculate_population",
                    description="Calculate population estimates with variance"
                )
            )
            pipeline.add_step(step)
        
        # Step 9: Apply proper variance calculations
        if "calculate_variance" not in self.skip_steps and config.variance:
            step = self.step_overrides.get(
                "calculate_variance",
                ApplyVarianceCalculationStep(
                    variance_method=config.variance_method,
                    step_id="calculate_variance",
                    description="Apply proper FIA variance calculations"
                )
            )
            pipeline.add_step(step)
        
        # Step 10: Format output
        if "format_output" not in self.skip_steps:
            step = self.step_overrides.get(
                "format_output",
                FormatOutputStep(
                    step_id="format_output",
                    description="Format final output for return"
                )
            )
            pipeline.add_step(step)
        
        # Step 11: Validate output
        if "validate_output" not in self.skip_steps:
            step = self.step_overrides.get(
                "validate_output",
                ValidateOutputStep(
                    step_id="validate_output",
                    description="Validate final output meets requirements"
                )
            )
            pipeline.add_step(step)


# === Specific Estimation Builders ===

class VolumeEstimationBuilder(PipelineBuilder):
    """
    Pipeline builder for volume estimation.
    
    Creates pipelines for calculating cubic foot volume estimates
    using FIA volume equations and proper statistical methods.
    """
    
    def __init__(self, **kwargs):
        """Initialize volume estimation builder."""
        super().__init__(
            estimation_type="volume",
            config_class=VolumeConfig,
            **kwargs
        )
    
    def get_value_calculation_step(self, config: EstimatorConfig) -> PipelineStep:
        """Get volume calculation step."""
        return CalculateTreeVolumesStep(
            step_id="calculate_values",
            description="Calculate tree-level cubic foot volumes"
        )


class BiomassEstimationBuilder(PipelineBuilder):
    """
    Pipeline builder for biomass estimation.
    
    Creates pipelines for calculating biomass estimates using
    FIA biomass equations and component options.
    """
    
    def __init__(self, **kwargs):
        """Initialize biomass estimation builder."""
        super().__init__(
            estimation_type="biomass",
            config_class=BiomassConfig,
            **kwargs
        )
    
    def get_value_calculation_step(self, config: EstimatorConfig) -> PipelineStep:
        """Get biomass calculation step."""
        component = "aboveground"
        if hasattr(config, "module_config") and config.module_config:
            component = getattr(config.module_config, "component", "aboveground")
        
        return CalculateBiomassStep(
            component=component,
            step_id="calculate_values",
            description=f"Calculate tree-level {component} biomass"
        )


class TPAEstimationBuilder(PipelineBuilder):
    """
    Pipeline builder for trees per acre (TPA) estimation.
    
    Creates pipelines for calculating trees per acre estimates
    with proper expansion factors.
    """
    
    def __init__(self, **kwargs):
        """Initialize TPA estimation builder."""
        super().__init__(
            estimation_type="tpa",
            config_class=EstimatorConfig,
            **kwargs
        )
    
    def get_value_calculation_step(self, config: EstimatorConfig) -> PipelineStep:
        """Get TPA calculation step."""
        return CalculateTPAStep(
            step_id="calculate_values",
            description="Calculate trees per acre using expansion factors"
        )


class AreaEstimationBuilder(PipelineBuilder):
    """
    Pipeline builder for area estimation.
    
    Creates pipelines for calculating area estimates based on
    condition-level data and land type classifications.
    """
    
    def __init__(self, **kwargs):
        """Initialize area estimation builder."""
        super().__init__(
            estimation_type="area",
            config_class=AreaConfig,
            **kwargs
        )
    
    def get_value_calculation_step(self, config: EstimatorConfig) -> PipelineStep:
        """Get area calculation step."""
        return CalculateAreaStep(
            step_id="calculate_values",
            description="Calculate area using condition proportions"
        )
    
    def _add_standard_steps(self, pipeline: EstimationPipeline, config: EstimatorConfig) -> None:
        """Add steps specific to area estimation (no tree aggregation)."""
        super()._add_standard_steps(pipeline, config)
        
        # Area estimation doesn't need plot aggregation since it works on conditions
        # The step is already skipped in the parent class for area estimation


class GrowthEstimationBuilder(PipelineBuilder):
    """
    Pipeline builder for growth estimation.
    
    Creates pipelines for calculating net and gross growth estimates
    using diameter increment and growth accounting methods.
    """
    
    def __init__(self, **kwargs):
        """Initialize growth estimation builder."""
        super().__init__(
            estimation_type="growth",
            config_class=GrowthConfig,
            **kwargs
        )
    
    def get_value_calculation_step(self, config: EstimatorConfig) -> PipelineStep:
        """Get growth calculation step."""
        return CalculateGrowthStep(
            step_id="calculate_values",
            description="Calculate tree growth using diameter increment"
        )


class MortalityEstimationBuilder(PipelineBuilder):
    """
    Pipeline builder for mortality estimation.
    
    Creates pipelines for calculating mortality estimates by
    cause and tree characteristics.
    """
    
    def __init__(self, **kwargs):
        """Initialize mortality estimation builder.""" 
        super().__init__(
            estimation_type="mortality",
            config_class=MortalityConfig,
            **kwargs
        )
    
    def get_value_calculation_step(self, config: EstimatorConfig) -> PipelineStep:
        """Get mortality calculation step."""
        mortality_type = "volume"
        if hasattr(config, "mortality_type"):
            mortality_type = getattr(config, "mortality_type", "volume")
        
        return CalculateMortalityStep(
            mortality_type=mortality_type,
            step_id="calculate_values",
            description=f"Calculate {mortality_type} mortality estimates"
        )


# === Step Factories ===

class StepFactory:
    """
    Factory for creating pipeline steps.
    
    Provides a centralized way to create and configure pipeline steps
    with consistent settings and optimizations.
    """
    
    @staticmethod
    def create_data_loading_step(
        estimation_type: str,
        tables: Optional[List[str]] = None,
        apply_evalid_filter: bool = True
    ) -> PipelineStep:
        """
        Create data loading step for estimation type.
        
        Parameters
        ----------
        estimation_type : str
            Type of estimation
        tables : Optional[List[str]]
            Specific tables to load (if None, uses defaults for estimation type)
        apply_evalid_filter : bool
            Whether to apply EVALID filtering
            
        Returns
        -------
        PipelineStep
            Configured data loading step
        """
        if tables:
            from .steps import LoadTablesStep
            return LoadTablesStep(
                tables=tables,
                apply_evalid_filter=apply_evalid_filter,
                step_id="load_custom_tables",
                description=f"Load custom tables: {', '.join(tables)}"
            )
        else:
            return LoadRequiredTablesStep(
                estimation_type=estimation_type,
                apply_evalid_filter=apply_evalid_filter,
                step_id="load_required_tables",
                description=f"Load tables required for {estimation_type} estimation"
            )
    
    @staticmethod
    def create_value_calculation_step(
        estimation_type: str,
        config: EstimatorConfig
    ) -> PipelineStep:
        """
        Create value calculation step for estimation type.
        
        Parameters
        ----------
        estimation_type : str
            Type of estimation
        config : EstimatorConfig
            Estimation configuration
            
        Returns
        -------
        PipelineStep
            Configured value calculation step
        """
        if estimation_type == "volume":
            return CalculateTreeVolumesStep(
                step_id="calculate_tree_volumes",
                description="Calculate tree-level volumes"
            )
        elif estimation_type == "biomass":
            component = "aboveground"
            if hasattr(config, "module_config") and config.module_config:
                component = getattr(config.module_config, "component", "aboveground")
            return CalculateBiomassStep(
                component=component,
                step_id="calculate_tree_biomass",
                description=f"Calculate tree-level {component} biomass"
            )
        elif estimation_type == "tpa":
            return CalculateTPAStep(
                step_id="calculate_tpa",
                description="Calculate trees per acre"
            )
        elif estimation_type == "area":
            return CalculateAreaStep(
                step_id="calculate_area",
                description="Calculate area estimates"
            )
        elif estimation_type == "growth":
            return CalculateGrowthStep(
                step_id="calculate_growth",
                description="Calculate growth estimates"
            )
        elif estimation_type == "mortality":
            mortality_type = getattr(config, "mortality_type", "volume")
            return CalculateMortalityStep(
                mortality_type=mortality_type,
                step_id="calculate_mortality",
                description="Calculate mortality estimates"
            )
        else:
            raise ValueError(f"Unknown estimation type: {estimation_type}")
    
    @staticmethod
    def create_conditional_step(
        condition: Callable[[EstimatorConfig], bool],
        step: PipelineStep,
        fallback_step: Optional[PipelineStep] = None
    ) -> "ConditionalStep":
        """
        Create a conditional step that executes based on configuration.
        
        Parameters
        ----------
        condition : Callable[[EstimatorConfig], bool]
            Condition function that takes config and returns bool
        step : PipelineStep
            Step to execute if condition is True
        fallback_step : Optional[PipelineStep]
            Step to execute if condition is False
            
        Returns
        -------
        ConditionalStep
            Conditional step wrapper
        """
        from .extensions import ConditionalStep
        return ConditionalStep(condition, step, fallback_step)


class ConditionalStepFactory:
    """
    Factory for creating conditional pipeline steps.
    
    Provides common conditional step patterns for FIA estimation workflows.
    """
    
    @staticmethod
    def create_totals_step(totals_step: PipelineStep) -> "ConditionalStep":
        """
        Create step that only executes if totals are requested.
        
        Parameters
        ----------
        totals_step : PipelineStep
            Step to execute when totals are requested
            
        Returns
        -------
        ConditionalStep
            Conditional step for totals calculation
        """
        from .extensions import ConditionalStep
        
        def should_calculate_totals(config: EstimatorConfig) -> bool:
            return getattr(config, "totals", False)
        
        return ConditionalStep(
            condition=should_calculate_totals,
            step=totals_step,
            step_id="conditional_totals",
            description="Calculate totals if requested"
        )
    
    @staticmethod  
    def create_variance_step(variance_step: PipelineStep) -> "ConditionalStep":
        """
        Create step that only executes if variance is requested.
        
        Parameters
        ----------
        variance_step : PipelineStep
            Step to execute when variance is requested
            
        Returns
        -------
        ConditionalStep
            Conditional step for variance calculation
        """
        from .extensions import ConditionalStep
        
        def should_calculate_variance(config: EstimatorConfig) -> bool:
            return getattr(config, "variance", False)
        
        return ConditionalStep(
            condition=should_calculate_variance,
            step=variance_step,
            step_id="conditional_variance", 
            description="Calculate variance if requested"
        )
    
    @staticmethod
    def create_grouping_step(grouping_step: PipelineStep) -> "ConditionalStep":
        """
        Create step that only executes if grouping is requested.
        
        Parameters
        ----------
        grouping_step : PipelineStep
            Step to execute when grouping is requested
            
        Returns
        -------
        ConditionalStep
            Conditional step for grouping
        """
        from .extensions import ConditionalStep
        
        def should_apply_grouping(config: EstimatorConfig) -> bool:
            return bool(config.grp_by or config.by_species or config.by_size_class)
        
        return ConditionalStep(
            condition=should_apply_grouping,
            step=grouping_step,
            step_id="conditional_grouping",
            description="Apply grouping if requested"
        )


# === Pipeline Templates ===

def create_volume_pipeline(**config_kwargs) -> EstimationPipeline:
    """
    Create a standard volume estimation pipeline.
    
    Parameters
    ----------
    **config_kwargs
        Configuration parameters for volume estimation
        
    Returns
    -------
    EstimationPipeline
        Configured volume estimation pipeline
    """
    return VolumeEstimationBuilder().build(**config_kwargs)


def create_biomass_pipeline(**config_kwargs) -> EstimationPipeline:
    """
    Create a standard biomass estimation pipeline.
    
    Parameters
    ----------
    **config_kwargs
        Configuration parameters for biomass estimation
        
    Returns
    -------
    EstimationPipeline
        Configured biomass estimation pipeline
    """
    return BiomassEstimationBuilder().build(**config_kwargs)


def create_tpa_pipeline(**config_kwargs) -> EstimationPipeline:
    """
    Create a standard TPA estimation pipeline.
    
    Parameters
    ----------
    **config_kwargs
        Configuration parameters for TPA estimation
        
    Returns
    -------
    EstimationPipeline
        Configured TPA estimation pipeline
    """
    return TPAEstimationBuilder().build(**config_kwargs)


def create_area_pipeline(**config_kwargs) -> EstimationPipeline:
    """
    Create a standard area estimation pipeline.
    
    Parameters
    ----------
    **config_kwargs
        Configuration parameters for area estimation
        
    Returns
    -------
    EstimationPipeline
        Configured area estimation pipeline
    """
    return AreaEstimationBuilder().build(**config_kwargs)


def create_growth_pipeline(**config_kwargs) -> EstimationPipeline:
    """
    Create a standard growth estimation pipeline.
    
    Parameters
    ----------
    **config_kwargs
        Configuration parameters for growth estimation
        
    Returns
    -------
    EstimationPipeline
        Configured growth estimation pipeline
    """
    return GrowthEstimationBuilder().build(**config_kwargs)


def create_mortality_pipeline(**config_kwargs) -> EstimationPipeline:
    """
    Create a standard mortality estimation pipeline.
    
    Parameters
    ----------
    **config_kwargs
        Configuration parameters for mortality estimation
        
    Returns
    -------
    EstimationPipeline
        Configured mortality estimation pipeline
    """
    return MortalityEstimationBuilder().build(**config_kwargs)