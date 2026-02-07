"""FIA column name constants.

Centralizes column name strings to prevent typos and enable IDE autocompletion.
These constants match the official FIA database column names.
"""

from __future__ import annotations


class TreeColumns:
    """TREE table column names."""

    CN = "CN"
    PLT_CN = "PLT_CN"
    CONDID = "CONDID"
    STATUSCD = "STATUSCD"
    SPCD = "SPCD"
    DIA = "DIA"
    TPA_UNADJ = "TPA_UNADJ"
    TREECLCD = "TREECLCD"
    HT = "HT"
    ACTUALHT = "ACTUALHT"
    CR = "CR"
    CCLCD = "CCLCD"
    SPGRPCD = "SPGRPCD"
    DECAYCD = "DECAYCD"
    # Volume columns
    VOLCFNET = "VOLCFNET"
    VOLCFGRS = "VOLCFGRS"
    VOLCFSND = "VOLCFSND"
    VOLBFNET = "VOLBFNET"
    VOLCSNET = "VOLCSNET"
    # Biomass columns
    DRYBIO_AG = "DRYBIO_AG"
    DRYBIO_BG = "DRYBIO_BG"
    CARBON_AG = "CARBON_AG"
    CARBON_BG = "CARBON_BG"


class CondColumns:
    """COND table column names."""

    PLT_CN = "PLT_CN"
    CONDID = "CONDID"
    COND_STATUS_CD = "COND_STATUS_CD"
    CONDPROP_UNADJ = "CONDPROP_UNADJ"
    OWNGRPCD = "OWNGRPCD"
    FORTYPCD = "FORTYPCD"
    SITECLCD = "SITECLCD"
    RESERVCD = "RESERVCD"
    STDSZCD = "STDSZCD"
    STDAGE = "STDAGE"
    STDORGCD = "STDORGCD"
    PROP_BASIS = "PROP_BASIS"


class PlotColumns:
    """PLOT table column names."""

    CN = "CN"
    STATECD = "STATECD"
    INVYR = "INVYR"
    EVALID = "EVALID"
    LAT = "LAT"
    LON = "LON"
    MACRO_BREAKPOINT_DIA = "MACRO_BREAKPOINT_DIA"


class StratColumns:
    """Stratification and population table column names."""

    STRATUM_CN = "STRATUM_CN"
    EXPNS = "EXPNS"
    ADJ_FACTOR_MICR = "ADJ_FACTOR_MICR"
    ADJ_FACTOR_SUBP = "ADJ_FACTOR_SUBP"
    ADJ_FACTOR_MACR = "ADJ_FACTOR_MACR"
    ADJ_FACTOR = "ADJ_FACTOR"
    EVALID = "EVALID"


class OutputColumns:
    """Common output column names for estimation results."""

    N_PLOTS = "N_PLOTS"
    N_STRATA = "N_STRATA"
    TOTAL_AREA = "TOTAL_AREA"
    SE_SUFFIX = "_SE"
    TOTAL_SUFFIX = "_TOTAL"
