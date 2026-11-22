"""
NSVB (Westfall et al. 2023, GTR-WO-104) equation library and coefficient loaders.

This subpackage implements the National Scale Volume and Biomass framework
from scratch, validated against the worked examples in the GTR-WO-104 source
PDF. The public data path is the vectorized polars pipeline
(``compute_nsvb_biomass`` / ``compute_nsvb_dead_biomass``); the independent
scalar reference implementation used to cross-check it lives in the test suite
(``tests/nsvb_oracle.py``), not in the shipped library (issue #127).

Models 1-5 are implemented (the forms used by the live-tree and standing-dead
biomass pipelines). Model 6 (volume-ratio) never appears in the biomass
component tables and is not implemented.
"""

from pyfia.carbon.nsvb.carbon_fractions import (
    DEFAULT_LIVE_CARBON_FRACTION,
    get_carbon_fraction_dead,
    get_carbon_fraction_live,
    load_carbon_fractions_dead,
    load_carbon_fractions_dead_df,
    load_carbon_fractions_live,
    load_carbon_fractions_live_df,
    load_dead_decay_proportions_df,
)
from pyfia.carbon.nsvb.coefficients import (
    CoefficientTables,
    VectorizedLookupTables,
    build_division_lookup,
    build_jenkins_lookup,
    build_species_level_lookup,
    get_vectorized_lookup_tables,
    load_nsvb_coefficients,
)
from pyfia.carbon.nsvb.equations import (
    compute_nsvb_biomass,
    compute_nsvb_dead_biomass,
    nsvb_biomass_expr,
)

__all__ = [
    "CoefficientTables",
    "DEFAULT_LIVE_CARBON_FRACTION",
    "VectorizedLookupTables",
    "build_division_lookup",
    "build_jenkins_lookup",
    "build_species_level_lookup",
    "compute_nsvb_biomass",
    "compute_nsvb_dead_biomass",
    "get_carbon_fraction_dead",
    "get_carbon_fraction_live",
    "get_vectorized_lookup_tables",
    "load_carbon_fractions_dead",
    "load_carbon_fractions_dead_df",
    "load_carbon_fractions_live",
    "load_carbon_fractions_live_df",
    "load_dead_decay_proportions_df",
    "load_nsvb_coefficients",
    "nsvb_biomass_expr",
]
