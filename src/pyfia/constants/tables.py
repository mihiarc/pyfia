"""
FIA table names.

Contains standard FIA database table names used throughout pyFIA.
"""

from __future__ import annotations


class TableNames:
    """Standard FIA table names."""

    # Core tables
    PLOT = "PLOT"
    TREE = "TREE"
    COND = "COND"
    SUBPLOT = "SUBPLOT"

    # Population estimation tables
    POP_EVAL = "POP_EVAL"
    POP_EVAL_TYP = "POP_EVAL_TYP"
    POP_STRATUM = "POP_STRATUM"
    POP_PLOT_STRATUM_ASSGN = "POP_PLOT_STRATUM_ASSGN"
    POP_ESTN_UNIT = "POP_ESTN_UNIT"

    # GRM tables for growth/mortality
    TREE_GRM_BEGIN = "TREE_GRM_BEGIN"
    TREE_GRM_MIDPT = "TREE_GRM_MIDPT"
    TREE_GRM_COMPONENT = "TREE_GRM_COMPONENT"

    # Reference tables
    REF_SPECIES = "REF_SPECIES"
    REF_SPECIES_GROUP = "REF_SPECIES_GROUP"
    REF_FOREST_TYPE = "REF_FOREST_TYPE"
    REF_FOREST_TYPE_GROUP = "REF_FOREST_TYPE_GROUP"
    REF_OWNGRPCD = "REF_OWNGRPCD"
    REF_UNIT = "REF_UNIT"
    REF_STATE = "REF_STATE"
    REF_FIADB_VERSION = "REF_FIADB_VERSION"
    COUNTY = "COUNTY"
    SURVEY = "SURVEY"
