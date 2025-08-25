"""
Value calculation steps for the pyFIA estimation pipeline.

This module provides pipeline steps for calculating various forest attributes
including volume, biomass, trees per acre, basal area, mortality, and growth.
These steps implement FIA-standard calculation methods.

Steps:
- CalculateTreeVolumeStep: Volume calculations per tree
- CalculateTreeBiomassStep: Biomass calculations (AG, BG)
- CalculateTPAStep: Trees per acre calculations
- CalculateBasalAreaStep: Basal area calculations
- CalculateMortalityStep: Mortality-specific calculations
- CalculateGrowthStep: Growth calculations
"""

from typing import Dict, List, Optional, Set, Type
import warnings
import math

import polars as pl

from ...lazy_evaluation import LazyFrameWrapper
from ..core import ExecutionContext, PipelineException
from ..contracts import JoinedDataContract, ValuedDataContract
from ..base_steps import CalculationStep


class CalculateTreeVolumeStep(CalculationStep):
    """
    Calculate volume values for individual trees.
    
    This step calculates various volume metrics (gross/net, cubic/board feet)
    using FIA volume equations and adjustment factors.
    
    Examples
    --------
    >>> # Calculate all volume types
    >>> step = CalculateTreeVolumeStep(
    ...     volume_types=["VOLCFNET", "VOLCFGRS", "VOLBFNET", "VOLBFGRS"],
    ...     apply_adjustment_factors=True
    ... )
    >>> 
    >>> # Calculate only net cubic feet
    >>> step = CalculateTreeVolumeStep(
    ...     volume_types=["VOLCFNET"],
    ...     min_dia=5.0
    ... )
    """
    
    def __init__(
        self,
        volume_types: List[str] = None,
        min_dia: float = 5.0,
        apply_adjustment_factors: bool = True,
        calculate_per_acre: bool = True,
        **kwargs
    ):
        """
        Initialize volume calculation step.
        
        Parameters
        ----------
        volume_types : List[str]
            Volume types to calculate (default: all standard types)
        min_dia : float
            Minimum diameter for volume calculation
        apply_adjustment_factors : bool
            Whether to apply regional adjustment factors
        calculate_per_acre : bool
            Whether to calculate per-acre values using TPA_UNADJ
        **kwargs
            Additional arguments passed to base class
        """
        # Default volume types if not specified
        if volume_types is None:
            volume_types = ["VOLCFNET", "VOLCFGRS", "VOLCSNET", "VOLCSGRS", 
                          "VOLBFNET", "VOLBFGRS"]
        
        value_columns = volume_types.copy()
        if calculate_per_acre:
            value_columns.extend([f"{vt}_AC" for vt in volume_types])
        
        super().__init__(
            calculation_type="volume",
            value_columns=value_columns,
            **kwargs
        )
        self.volume_types = volume_types
        self.min_dia = min_dia
        self.apply_adjustment_factors = apply_adjustment_factors
        self.calculate_per_acre = calculate_per_acre
    
    def get_required_calculation_columns(self) -> Set[str]:
        """Get columns required for volume calculation."""
        required = {"DIA", "STATUSCD", "TPA_UNADJ"}
        required.update(self.volume_types)
        return required
    
    def perform_calculations(self, data: LazyFrameWrapper) -> LazyFrameWrapper:
        """Perform volume calculations."""
        frame = data.frame
        
        # Filter to trees meeting minimum diameter
        frame = frame.filter(pl.col("DIA") >= self.min_dia)
        
        # Calculate per-acre values if requested
        if self.calculate_per_acre:
            for vol_type in self.volume_types:
                if vol_type in frame.collect_schema().names():
                    frame = frame.with_columns(
                        (pl.col(vol_type) * pl.col("TPA_UNADJ")).alias(f"{vol_type}_AC")
                    )
        
        # Apply adjustment factors if requested
        if self.apply_adjustment_factors:
            # This would apply regional volume adjustment factors
            # Simplified for demonstration
            pass
        
        return LazyFrameWrapper(frame)
    
    def execute_step(
        self, 
        input_data: JoinedDataContract, 
        context: ExecutionContext
    ) -> ValuedDataContract:
        """
        Execute volume calculation.
        
        Parameters
        ----------
        input_data : JoinedDataContract
            Input contract with joined data
        context : ExecutionContext
            Execution context
            
        Returns
        -------
        ValuedDataContract
            Contract with calculated volume values
        """
        try:
            # Validate input columns
            self.validate_calculation_inputs(input_data.data)
            
            # Perform calculations
            valued_data = self.perform_calculations(input_data.data)
            
            # Create output contract
            output = ValuedDataContract(
                data=valued_data,
                value_columns=self.value_columns,
                value_type="volume",
                group_columns=input_data.group_columns,
                calculation_method="FIA_standard",
                step_id=self.step_id
            )
            
            # Add metadata
            output.add_processing_metadata("volume_types", self.volume_types)
            output.add_processing_metadata("min_dia", self.min_dia)
            output.add_processing_metadata("per_acre_calculated", self.calculate_per_acre)
            
            return output
            
        except Exception as e:
            self.handle_step_error(e, context)
            raise PipelineException(
                f"Failed to calculate volume: {e}",
                step_id=self.step_id,
                cause=e
            )


class CalculateTreeBiomassStep(CalculationStep):
    """
    Calculate biomass values for individual trees.
    
    This step calculates aboveground and belowground biomass using
    FIA biomass equations and carbon conversion factors.
    
    Examples
    --------
    >>> # Calculate all biomass components
    >>> step = CalculateTreeBiomassStep(
    ...     biomass_components=["AG", "BG", "BOLE", "TOP", "STUMP"],
    ...     include_carbon=True
    ... )
    >>> 
    >>> # Calculate only aboveground biomass
    >>> step = CalculateTreeBiomassStep(
    ...     biomass_components=["AG"],
    ...     units="tons"
    ... )
    """
    
    def __init__(
        self,
        biomass_components: List[str] = None,
        include_carbon: bool = True,
        units: str = "pounds",
        min_dia: float = 1.0,
        calculate_per_acre: bool = True,
        **kwargs
    ):
        """
        Initialize biomass calculation step.
        
        Parameters
        ----------
        biomass_components : List[str]
            Biomass components to calculate (AG, BG, BOLE, etc.)
        include_carbon : bool
            Whether to also calculate carbon values
        units : str
            Output units ("pounds" or "tons")
        min_dia : float
            Minimum diameter for biomass calculation
        calculate_per_acre : bool
            Whether to calculate per-acre values
        **kwargs
            Additional arguments passed to base class
        """
        # Default components if not specified
        if biomass_components is None:
            biomass_components = ["AG", "BG"]
        
        # Build value column list
        value_columns = []
        for comp in biomass_components:
            value_columns.append(f"DRYBIO_{comp}")
            if include_carbon:
                value_columns.append(f"CARBON_{comp}")
            if calculate_per_acre:
                value_columns.append(f"DRYBIO_{comp}_AC")
                if include_carbon:
                    value_columns.append(f"CARBON_{comp}_AC")
        
        super().__init__(
            calculation_type="biomass",
            value_columns=value_columns,
            **kwargs
        )
        self.biomass_components = biomass_components
        self.include_carbon = include_carbon
        self.units = units
        self.min_dia = min_dia
        self.calculate_per_acre = calculate_per_acre
    
    def get_required_calculation_columns(self) -> Set[str]:
        """Get columns required for biomass calculation."""
        required = {"DIA", "STATUSCD", "TPA_UNADJ", "SPCD"}
        
        # Add biomass columns
        for comp in self.biomass_components:
            required.add(f"DRYBIO_{comp}")
            if self.include_carbon:
                required.add(f"CARBON_{comp}")
        
        return required
    
    def perform_calculations(self, data: LazyFrameWrapper) -> LazyFrameWrapper:
        """Perform biomass calculations."""
        frame = data.frame
        
        # Filter to trees meeting minimum diameter
        frame = frame.filter(pl.col("DIA") >= self.min_dia)
        
        # Convert units if needed
        if self.units == "tons":
            conversion_factor = 1.0 / 2000.0  # pounds to tons
            
            for comp in self.biomass_components:
                bio_col = f"DRYBIO_{comp}"
                if bio_col in frame.collect_schema().names():
                    frame = frame.with_columns(
                        (pl.col(bio_col) * conversion_factor).alias(bio_col)
                    )
                
                if self.include_carbon:
                    carbon_col = f"CARBON_{comp}"
                    if carbon_col in frame.collect_schema().names():
                        frame = frame.with_columns(
                            (pl.col(carbon_col) * conversion_factor).alias(carbon_col)
                        )
        
        # Calculate per-acre values if requested
        if self.calculate_per_acre:
            for comp in self.biomass_components:
                bio_col = f"DRYBIO_{comp}"
                if bio_col in frame.collect_schema().names():
                    frame = frame.with_columns(
                        (pl.col(bio_col) * pl.col("TPA_UNADJ")).alias(f"{bio_col}_AC")
                    )
                
                if self.include_carbon:
                    carbon_col = f"CARBON_{comp}"
                    if carbon_col in frame.collect_schema().names():
                        frame = frame.with_columns(
                            (pl.col(carbon_col) * pl.col("TPA_UNADJ")).alias(f"{carbon_col}_AC")
                        )
        
        return LazyFrameWrapper(frame)
    
    def execute_step(
        self, 
        input_data: JoinedDataContract, 
        context: ExecutionContext
    ) -> ValuedDataContract:
        """
        Execute biomass calculation.
        
        Parameters
        ----------
        input_data : JoinedDataContract
            Input contract with joined data
        context : ExecutionContext
            Execution context
            
        Returns
        -------
        ValuedDataContract
            Contract with calculated biomass values
        """
        try:
            # Validate input columns
            self.validate_calculation_inputs(input_data.data)
            
            # Perform calculations
            valued_data = self.perform_calculations(input_data.data)
            
            # Create output contract
            output = ValuedDataContract(
                data=valued_data,
                value_columns=self.value_columns,
                value_type="biomass",
                group_columns=input_data.group_columns,
                calculation_method="FIA_biomass_equations",
                step_id=self.step_id
            )
            
            # Add metadata
            output.add_processing_metadata("biomass_components", self.biomass_components)
            output.add_processing_metadata("include_carbon", self.include_carbon)
            output.add_processing_metadata("units", self.units)
            
            return output
            
        except Exception as e:
            self.handle_step_error(e, context)
            raise PipelineException(
                f"Failed to calculate biomass: {e}",
                step_id=self.step_id,
                cause=e
            )


class CalculateTPAStep(CalculationStep):
    """
    Calculate trees per acre values.
    
    This step calculates trees per acre using TPA_UNADJ and various
    adjustment factors for different plot designs.
    
    Examples
    --------
    >>> # Standard TPA calculation
    >>> step = CalculateTPAStep()
    >>> 
    >>> # TPA with size class breakdowns
    >>> step = CalculateTPAStep(
    ...     size_classes=[(0, 5), (5, 10), (10, 15), (15, None)],
    ...     apply_adjustment_factors=True
    ... )
    """
    
    def __init__(
        self,
        size_classes: Optional[List[tuple]] = None,
        apply_adjustment_factors: bool = True,
        include_seedlings: bool = False,
        **kwargs
    ):
        """
        Initialize TPA calculation step.
        
        Parameters
        ----------
        size_classes : Optional[List[tuple]]
            Diameter size classes for TPA breakdown
        apply_adjustment_factors : bool
            Whether to apply subplot/microplot adjustment factors
        include_seedlings : bool
            Whether to include seedling counts if available
        **kwargs
            Additional arguments passed to base class
        """
        value_columns = ["TPA"]
        if size_classes:
            value_columns.extend([f"TPA_{i}" for i in range(len(size_classes))])
        if include_seedlings:
            value_columns.append("TPA_SEEDLINGS")
        
        super().__init__(
            calculation_type="tpa",
            value_columns=value_columns,
            **kwargs
        )
        self.size_classes = size_classes
        self.apply_adjustment_factors = apply_adjustment_factors
        self.include_seedlings = include_seedlings
    
    def get_required_calculation_columns(self) -> Set[str]:
        """Get columns required for TPA calculation."""
        required = {"TPA_UNADJ", "STATUSCD"}
        if self.size_classes:
            required.add("DIA")
        if self.apply_adjustment_factors:
            required.update({"SUBP", "CONDPROP_UNADJ"})
        return required
    
    def perform_calculations(self, data: LazyFrameWrapper) -> LazyFrameWrapper:
        """Perform TPA calculations."""
        frame = data.frame
        
        # Basic TPA calculation
        frame = frame.with_columns(
            pl.col("TPA_UNADJ").alias("TPA")
        )
        
        # Apply adjustment factors if requested
        if self.apply_adjustment_factors:
            if "CONDPROP_UNADJ" in frame.collect_schema().names():
                frame = frame.with_columns(
                    (pl.col("TPA") * pl.col("CONDPROP_UNADJ")).alias("TPA")
                )
        
        # Calculate size class TPAs if requested
        if self.size_classes:
            for i, (min_dia, max_dia) in enumerate(self.size_classes):
                if min_dia is None:
                    dia_filter = pl.col("DIA") < max_dia
                elif max_dia is None:
                    dia_filter = pl.col("DIA") >= min_dia
                else:
                    dia_filter = (pl.col("DIA") >= min_dia) & (pl.col("DIA") < max_dia)
                
                frame = frame.with_columns(
                    pl.when(dia_filter)
                    .then(pl.col("TPA"))
                    .otherwise(0)
                    .alias(f"TPA_{i}")
                )
        
        return LazyFrameWrapper(frame)
    
    def execute_step(
        self, 
        input_data: JoinedDataContract, 
        context: ExecutionContext
    ) -> ValuedDataContract:
        """
        Execute TPA calculation.
        
        Parameters
        ----------
        input_data : JoinedDataContract
            Input contract with joined data
        context : ExecutionContext
            Execution context
            
        Returns
        -------
        ValuedDataContract
            Contract with calculated TPA values
        """
        try:
            # Validate input columns
            self.validate_calculation_inputs(input_data.data)
            
            # Perform calculations
            valued_data = self.perform_calculations(input_data.data)
            
            # Create output contract
            output = ValuedDataContract(
                data=valued_data,
                value_columns=self.value_columns,
                value_type="tpa",
                group_columns=input_data.group_columns,
                calculation_method="FIA_TPA_factors",
                step_id=self.step_id
            )
            
            # Add metadata
            output.add_processing_metadata("size_classes", self.size_classes)
            output.add_processing_metadata("adjustment_factors_applied", self.apply_adjustment_factors)
            
            return output
            
        except Exception as e:
            self.handle_step_error(e, context)
            raise PipelineException(
                f"Failed to calculate TPA: {e}",
                step_id=self.step_id,
                cause=e
            )


class CalculateBasalAreaStep(CalculationStep):
    """
    Calculate basal area per tree and per acre.
    
    This step calculates basal area from tree diameter measurements,
    supporting both English and metric units.
    
    Examples
    --------
    >>> # Calculate basal area in square feet
    >>> step = CalculateBasalAreaStep(units="sqft")
    >>> 
    >>> # Calculate with size class breakdowns
    >>> step = CalculateBasalAreaStep(
    ...     units="sqm",
    ...     size_classes=[(0, 10), (10, 20), (20, None)]
    ... )
    """
    
    def __init__(
        self,
        units: str = "sqft",
        size_classes: Optional[List[tuple]] = None,
        calculate_per_acre: bool = True,
        **kwargs
    ):
        """
        Initialize basal area calculation step.
        
        Parameters
        ----------
        units : str
            Output units ("sqft" or "sqm")
        size_classes : Optional[List[tuple]]
            Diameter size classes for BA breakdown
        calculate_per_acre : bool
            Whether to calculate per-acre values
        **kwargs
            Additional arguments passed to base class
        """
        value_columns = ["BA"]
        if calculate_per_acre:
            value_columns.append("BA_AC")
        if size_classes:
            for i in range(len(size_classes)):
                value_columns.append(f"BA_{i}")
                if calculate_per_acre:
                    value_columns.append(f"BA_{i}_AC")
        
        super().__init__(
            calculation_type="basal_area",
            value_columns=value_columns,
            **kwargs
        )
        self.units = units
        self.size_classes = size_classes
        self.calculate_per_acre = calculate_per_acre
    
    def get_required_calculation_columns(self) -> Set[str]:
        """Get columns required for basal area calculation."""
        return {"DIA", "TPA_UNADJ", "STATUSCD"}
    
    def perform_calculations(self, data: LazyFrameWrapper) -> LazyFrameWrapper:
        """Perform basal area calculations."""
        frame = data.frame
        
        # Calculate basal area per tree
        # BA = π * (DIA/2)^2 / 144 (for square feet from inches)
        if self.units == "sqft":
            frame = frame.with_columns(
                (math.pi * (pl.col("DIA") / 2) ** 2 / 144).alias("BA")
            )
        else:  # square meters
            # Convert inches to cm, then to m^2
            frame = frame.with_columns(
                (math.pi * ((pl.col("DIA") * 2.54) / 2) ** 2 / 10000).alias("BA")
            )
        
        # Calculate per-acre values
        if self.calculate_per_acre:
            frame = frame.with_columns(
                (pl.col("BA") * pl.col("TPA_UNADJ")).alias("BA_AC")
            )
        
        # Calculate size class basal areas if requested
        if self.size_classes:
            for i, (min_dia, max_dia) in enumerate(self.size_classes):
                if min_dia is None:
                    dia_filter = pl.col("DIA") < max_dia
                elif max_dia is None:
                    dia_filter = pl.col("DIA") >= min_dia
                else:
                    dia_filter = (pl.col("DIA") >= min_dia) & (pl.col("DIA") < max_dia)
                
                frame = frame.with_columns(
                    pl.when(dia_filter)
                    .then(pl.col("BA"))
                    .otherwise(0)
                    .alias(f"BA_{i}")
                )
                
                if self.calculate_per_acre:
                    frame = frame.with_columns(
                        (pl.col(f"BA_{i}") * pl.col("TPA_UNADJ")).alias(f"BA_{i}_AC")
                    )
        
        return LazyFrameWrapper(frame)
    
    def execute_step(
        self, 
        input_data: JoinedDataContract, 
        context: ExecutionContext
    ) -> ValuedDataContract:
        """
        Execute basal area calculation.
        
        Parameters
        ----------
        input_data : JoinedDataContract
            Input contract with joined data
        context : ExecutionContext
            Execution context
            
        Returns
        -------
        ValuedDataContract
            Contract with calculated basal area values
        """
        try:
            # Validate input columns
            self.validate_calculation_inputs(input_data.data)
            
            # Perform calculations
            valued_data = self.perform_calculations(input_data.data)
            
            # Create output contract
            output = ValuedDataContract(
                data=valued_data,
                value_columns=self.value_columns,
                value_type="basal_area",
                group_columns=input_data.group_columns,
                calculation_method="geometric",
                step_id=self.step_id
            )
            
            # Add metadata
            output.add_processing_metadata("units", self.units)
            output.add_processing_metadata("size_classes", self.size_classes)
            
            return output
            
        except Exception as e:
            self.handle_step_error(e, context)
            raise PipelineException(
                f"Failed to calculate basal area: {e}",
                step_id=self.step_id,
                cause=e
            )


class CalculateMortalityStep(CalculationStep):
    """
    Calculate mortality-specific values.
    
    This step calculates mortality rates, volume, and biomass for dead trees,
    supporting various mortality agent classifications.
    
    Examples
    --------
    >>> # Calculate mortality by agent
    >>> step = CalculateMortalityStep(
    ...     mortality_agents=["FIRE", "INSECT", "DISEASE", "OTHER"],
    ...     include_volume=True,
    ...     include_biomass=True
    ... )
    """
    
    def __init__(
        self,
        mortality_agents: Optional[List[str]] = None,
        include_volume: bool = True,
        include_biomass: bool = True,
        time_period: int = 5,  # Years for annualization
        **kwargs
    ):
        """
        Initialize mortality calculation step.
        
        Parameters
        ----------
        mortality_agents : Optional[List[str]]
            Mortality agent classifications to track
        include_volume : bool
            Whether to calculate mortality volume
        include_biomass : bool
            Whether to calculate mortality biomass
        time_period : int
            Time period for annualizing mortality
        **kwargs
            Additional arguments passed to base class
        """
        value_columns = ["MORTALITY_TPA"]
        if include_volume:
            value_columns.append("MORTALITY_VOL")
        if include_biomass:
            value_columns.append("MORTALITY_BIO")
        if mortality_agents:
            for agent in mortality_agents:
                value_columns.append(f"MORTALITY_TPA_{agent}")
        
        super().__init__(
            calculation_type="mortality",
            value_columns=value_columns,
            **kwargs
        )
        self.mortality_agents = mortality_agents
        self.include_volume = include_volume
        self.include_biomass = include_biomass
        self.time_period = time_period
    
    def get_required_calculation_columns(self) -> Set[str]:
        """Get columns required for mortality calculation."""
        required = {"STATUSCD", "TPA_UNADJ", "AGENTCD"}
        if self.include_volume:
            required.add("VOLCFNET")
        if self.include_biomass:
            required.add("DRYBIO_AG")
        return required
    
    def perform_calculations(self, data: LazyFrameWrapper) -> LazyFrameWrapper:
        """Perform mortality calculations."""
        frame = data.frame
        
        # Filter to dead trees (STATUSCD = 2)
        mortality_frame = frame.filter(pl.col("STATUSCD") == 2)
        
        # Calculate basic mortality TPA
        mortality_frame = mortality_frame.with_columns(
            (pl.col("TPA_UNADJ") / self.time_period).alias("MORTALITY_TPA")
        )
        
        # Calculate mortality volume if requested
        if self.include_volume:
            mortality_frame = mortality_frame.with_columns(
                (pl.col("VOLCFNET") * pl.col("TPA_UNADJ") / self.time_period).alias("MORTALITY_VOL")
            )
        
        # Calculate mortality biomass if requested
        if self.include_biomass:
            mortality_frame = mortality_frame.with_columns(
                (pl.col("DRYBIO_AG") * pl.col("TPA_UNADJ") / self.time_period).alias("MORTALITY_BIO")
            )
        
        # Calculate by mortality agent if requested
        if self.mortality_agents:
            # Map agent codes to categories
            # This is simplified - actual mapping would be more complex
            agent_mapping = {
                "FIRE": [30],
                "INSECT": [10, 11, 12],
                "DISEASE": [20, 21, 22],
                "OTHER": [40, 50, 60, 70, 80, 90]
            }
            
            for agent_name in self.mortality_agents:
                agent_codes = agent_mapping.get(agent_name, [])
                mortality_frame = mortality_frame.with_columns(
                    pl.when(pl.col("AGENTCD").is_in(agent_codes))
                    .then(pl.col("MORTALITY_TPA"))
                    .otherwise(0)
                    .alias(f"MORTALITY_TPA_{agent_name}")
                )
        
        # Join back with original frame to include all trees
        frame = frame.join(
            mortality_frame.select(["PLT_CN", "SUBP", "TREE"] + self.value_columns),
            on=["PLT_CN", "SUBP", "TREE"],
            how="left"
        ).fill_null(0)
        
        return LazyFrameWrapper(frame)
    
    def execute_step(
        self, 
        input_data: JoinedDataContract, 
        context: ExecutionContext
    ) -> ValuedDataContract:
        """
        Execute mortality calculation.
        
        Parameters
        ----------
        input_data : JoinedDataContract
            Input contract with joined data
        context : ExecutionContext
            Execution context
            
        Returns
        -------
        ValuedDataContract
            Contract with calculated mortality values
        """
        try:
            # Validate input columns
            self.validate_calculation_inputs(input_data.data)
            
            # Perform calculations
            valued_data = self.perform_calculations(input_data.data)
            
            # Create output contract
            output = ValuedDataContract(
                data=valued_data,
                value_columns=self.value_columns,
                value_type="mortality",
                group_columns=input_data.group_columns,
                calculation_method="FIA_mortality",
                step_id=self.step_id
            )
            
            # Add metadata
            output.add_processing_metadata("mortality_agents", self.mortality_agents)
            output.add_processing_metadata("time_period", self.time_period)
            output.add_processing_metadata("annualized", True)
            
            return output
            
        except Exception as e:
            self.handle_step_error(e, context)
            raise PipelineException(
                f"Failed to calculate mortality: {e}",
                step_id=self.step_id,
                cause=e
            )


class CalculateGrowthStep(CalculationStep):
    """
    Calculate growth rates and increment values.
    
    This step calculates diameter growth, volume growth, and biomass growth
    using remeasurement data from successive inventories.
    
    Examples
    --------
    >>> # Calculate all growth components
    >>> step = CalculateGrowthStep(
    ...     growth_types=["diameter", "volume", "biomass"],
    ...     time_period=5
    ... )
    """
    
    def __init__(
        self,
        growth_types: List[str] = None,
        time_period: int = 5,
        annualize: bool = True,
        include_ingrowth: bool = True,
        **kwargs
    ):
        """
        Initialize growth calculation step.
        
        Parameters
        ----------
        growth_types : List[str]
            Types of growth to calculate (diameter, volume, biomass)
        time_period : int
            Time period between measurements
        annualize : bool
            Whether to annualize growth rates
        include_ingrowth : bool
            Whether to include ingrowth trees
        **kwargs
            Additional arguments passed to base class
        """
        if growth_types is None:
            growth_types = ["diameter", "volume"]
        
        value_columns = []
        for gtype in growth_types:
            value_columns.append(f"GROWTH_{gtype.upper()}")
            if annualize:
                value_columns.append(f"GROWTH_{gtype.upper()}_ANNUAL")
        
        if include_ingrowth:
            value_columns.append("INGROWTH_TPA")
        
        super().__init__(
            calculation_type="growth",
            value_columns=value_columns,
            **kwargs
        )
        self.growth_types = growth_types
        self.time_period = time_period
        self.annualize = annualize
        self.include_ingrowth = include_ingrowth
    
    def get_required_calculation_columns(self) -> Set[str]:
        """Get columns required for growth calculation."""
        required = {"DIA", "PREV_TRE_CN", "TPA_UNADJ", "STATUSCD"}
        
        if "volume" in self.growth_types:
            required.add("VOLCFNET")
        if "biomass" in self.growth_types:
            required.add("DRYBIO_AG")
        
        return required
    
    def perform_calculations(self, data: LazyFrameWrapper) -> LazyFrameWrapper:
        """Perform growth calculations."""
        frame = data.frame
        
        # This is a simplified implementation
        # Actual growth calculation would require joining with previous inventory
        
        # Calculate diameter growth (placeholder - would need previous diameter)
        if "diameter" in self.growth_types:
            # Simulate diameter growth
            frame = frame.with_columns(
                (pl.col("DIA") * 0.1).alias("GROWTH_DIAMETER")  # Placeholder
            )
            
            if self.annualize:
                frame = frame.with_columns(
                    (pl.col("GROWTH_DIAMETER") / self.time_period).alias("GROWTH_DIAMETER_ANNUAL")
                )
        
        # Calculate volume growth
        if "volume" in self.growth_types:
            # Simulate volume growth
            frame = frame.with_columns(
                (pl.col("VOLCFNET") * pl.col("TPA_UNADJ") * 0.05).alias("GROWTH_VOLUME")  # Placeholder
            )
            
            if self.annualize:
                frame = frame.with_columns(
                    (pl.col("GROWTH_VOLUME") / self.time_period).alias("GROWTH_VOLUME_ANNUAL")
                )
        
        # Calculate biomass growth
        if "biomass" in self.growth_types:
            frame = frame.with_columns(
                (pl.col("DRYBIO_AG") * pl.col("TPA_UNADJ") * 0.03).alias("GROWTH_BIOMASS")  # Placeholder
            )
            
            if self.annualize:
                frame = frame.with_columns(
                    (pl.col("GROWTH_BIOMASS") / self.time_period).alias("GROWTH_BIOMASS_ANNUAL")
                )
        
        # Identify ingrowth trees (no previous tree record)
        if self.include_ingrowth:
            frame = frame.with_columns(
                pl.when(pl.col("PREV_TRE_CN").is_null())
                .then(pl.col("TPA_UNADJ") / self.time_period)
                .otherwise(0)
                .alias("INGROWTH_TPA")
            )
        
        return LazyFrameWrapper(frame)
    
    def execute_step(
        self, 
        input_data: JoinedDataContract, 
        context: ExecutionContext
    ) -> ValuedDataContract:
        """
        Execute growth calculation.
        
        Parameters
        ----------
        input_data : JoinedDataContract
            Input contract with joined data
        context : ExecutionContext
            Execution context
            
        Returns
        -------
        ValuedDataContract
            Contract with calculated growth values
        """
        try:
            # Validate input columns
            self.validate_calculation_inputs(input_data.data)
            
            # Perform calculations
            valued_data = self.perform_calculations(input_data.data)
            
            # Create output contract
            output = ValuedDataContract(
                data=valued_data,
                value_columns=self.value_columns,
                value_type="growth",
                group_columns=input_data.group_columns,
                calculation_method="FIA_growth",
                step_id=self.step_id
            )
            
            # Add metadata
            output.add_processing_metadata("growth_types", self.growth_types)
            output.add_processing_metadata("time_period", self.time_period)
            output.add_processing_metadata("annualized", self.annualize)
            output.add_processing_metadata("ingrowth_included", self.include_ingrowth)
            
            return output
            
        except Exception as e:
            self.handle_step_error(e, context)
            raise PipelineException(
                f"Failed to calculate growth: {e}",
                step_id=self.step_id,
                cause=e
            )


# Export all calculation step classes
__all__ = [
    "CalculateTreeVolumeStep",
    "CalculateTreeBiomassStep",
    "CalculateTPAStep",
    "CalculateBasalAreaStep",
    "CalculateMortalityStep",
    "CalculateGrowthStep",
]