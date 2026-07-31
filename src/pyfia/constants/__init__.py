"""
Constants and enumerations for pyFIA.

This package provides FIA-specific constants organized by domain:

- plot_design: Plot design parameters, diameter breakpoints, size classes
- status_codes: Tree/land status codes, ownership, evaluation types
- states: State FIPS code mappings
- tables: FIA database table names
- columns: FIA column name constants
- defaults: Default values, validation ranges, error messages
"""

from __future__ import annotations

from .columns import (
    CondColumns,
    OutputColumns,
    PlotColumns,
    StratColumns,
    TreeColumns,
)
from .species_extra import (
    NZ_SPECIES_EXTRAS,
    SpeciesExtra,
)
from .species_extra import (
    by_alias as species_by_alias,
)
from .species_extra import (
    by_name as species_by_name,
)
from .species_extra import (
    lookup as species_lookup,
)
from .species_macros import (
    MACRON_ALIASES,
    macron_to_canonical,
)

__all__ = [
    "CondColumns",
    "MACRON_ALIASES",
    "NZ_SPECIES_EXTRAS",
    "OutputColumns",
    "PlotColumns",
    "SpeciesExtra",
    "StratColumns",
    "TreeColumns",
    "macron_to_canonical",
    "species_by_alias",
    "species_by_name",
    "species_lookup",
]
