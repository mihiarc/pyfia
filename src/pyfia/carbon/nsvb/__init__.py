"""
NSVB (Westfall et al. 2023, GTR-WO-104) equation library and coefficient loaders.

This subpackage implements the National Scale Volume and Biomass framework
from scratch in pure Python, validated against the worked examples in the
GTR-WO-104 source PDF. The reverse-engineering approach is intentional — see
``pyfia_carbon_tech_spec.md`` section 1.2 for the rationale.

Phase 1 implements Models 1, 2, 4, and 5 (the model forms used by the live
tree biomass pipeline). Models 3 and 6 are deferred to a later phase.
"""

from pyfia.carbon.nsvb.carbon_fractions import (
    DEFAULT_LIVE_CARBON_FRACTION,
    get_carbon_fraction_dead,
    get_carbon_fraction_live,
    load_carbon_fractions_dead,
    load_carbon_fractions_live,
    load_carbon_fractions_live_df,
)
from pyfia.carbon.nsvb.coefficients import (
    CoefficientTables,
    VectorizedLookupTables,
    build_jenkins_lookup,
    build_species_level_lookup,
    get_vectorized_lookup_tables,
    load_nsvb_coefficients,
    lookup_coefficients,
)
from pyfia.carbon.nsvb.equations import (
    Coefficients,
    TreeBiomassResult,
    compute_nsvb_biomass,
    harmonize_components,
    model_1,
    model_2,
    model_4,
    model_5_jenkins,
    nsvb_biomass_expr,
    predict_tree_biomass,
)

__all__ = [
    "Coefficients",
    "CoefficientTables",
    "DEFAULT_LIVE_CARBON_FRACTION",
    "TreeBiomassResult",
    "VectorizedLookupTables",
    "build_jenkins_lookup",
    "build_species_level_lookup",
    "compute_nsvb_biomass",
    "get_carbon_fraction_dead",
    "get_carbon_fraction_live",
    "get_vectorized_lookup_tables",
    "harmonize_components",
    "load_carbon_fractions_dead",
    "load_carbon_fractions_live",
    "load_carbon_fractions_live_df",
    "load_nsvb_coefficients",
    "lookup_coefficients",
    "model_1",
    "model_2",
    "model_4",
    "model_5_jenkins",
    "nsvb_biomass_expr",
    "predict_tree_biomass",
]
