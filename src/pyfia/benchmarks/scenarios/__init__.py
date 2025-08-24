"""
Realistic benchmark scenarios for pyFIA lazy evaluation testing.

This package contains specific benchmark scenarios that represent
common FIA analysis use cases, designed to test the performance
improvements of lazy evaluation under realistic conditions.
"""

from .single_state_volume import SingleStateVolumeScenario
from .multi_state_area import MultiStateAreaScenario
from .species_biomass import SpeciesBiomassScenario
from .complex_grouping import ComplexGroupingScenario

__all__ = [
    "SingleStateVolumeScenario",
    "MultiStateAreaScenario", 
    "SpeciesBiomassScenario",
    "ComplexGroupingScenario"
]