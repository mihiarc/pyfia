"""
Constants and enumerations for pyFIA.

This package provides FIA-specific constants organized by domain:

- plot_design: Plot design parameters, diameter breakpoints, size classes
- status_codes: Tree/land status codes, ownership, evaluation types
- species: Species groupings taken from REF_SPECIES
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
    RefColumns,
    StratColumns,
    TreeColumns,
)

__all__ = [
    "TreeColumns",
    "CondColumns",
    "PlotColumns",
    "RefColumns",
    "StratColumns",
    "OutputColumns",
]
