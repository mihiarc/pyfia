"""
FIA table names.

Contains standard FIA database table names used throughout pyFIA.
"""


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
    REF_FOREST_TYPE = "REF_FOREST_TYPE"
    REF_STATE = "REF_STATE"
