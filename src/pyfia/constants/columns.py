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
    STATECD = "STATECD"
    INVYR = "INVYR"
    MICRPROP_UNADJ = "MICRPROP_UNADJ"
    SUBPPROP_UNADJ = "SUBPPROP_UNADJ"
    MACRPROP_UNADJ = "MACRPROP_UNADJ"
    BALIVE = "BALIVE"


class PlotColumns:
    """PLOT table column names."""

    CN = "CN"
    STATECD = "STATECD"
    INVYR = "INVYR"
    EVALID = "EVALID"
    LAT = "LAT"
    LON = "LON"
    MACRO_BREAKPOINT_DIA = "MACRO_BREAKPOINT_DIA"


class RefColumns:
    """Reference-table (REF_*, COUNTY) column names."""

    VALUE = "VALUE"
    MEANING = "MEANING"
    NAME = "NAME"
    # REF_FOREST_TYPE
    TYPGRPCD = "TYPGRPCD"
    ALLOWED_IN_FIELD = "ALLOWED_IN_FIELD"
    # REF_SPECIES and REF_SPECIES_GROUP
    SPCD = "SPCD"
    COMMON_NAME = "COMMON_NAME"
    SCIENTIFIC_NAME = "SCIENTIFIC_NAME"
    GENUS = "GENUS"
    SPECIES = "SPECIES"
    SFTWD_HRDWD = "SFTWD_HRDWD"
    WOODLAND = "WOODLAND"
    MAJOR_SPGRPCD = "MAJOR_SPGRPCD"
    E_SPGRPCD = "E_SPGRPCD"
    W_SPGRPCD = "W_SPGRPCD"
    SPGRPCD = "SPGRPCD"
    WOOD_SPGR_GREENVOL_DRYWT = "WOOD_SPGR_GREENVOL_DRYWT"
    MC_PCT_GREEN_WOOD = "MC_PCT_GREEN_WOOD"
    BARK_SPGR_GREENVOL_DRYWT = "BARK_SPGR_GREENVOL_DRYWT"
    MC_PCT_GREEN_BARK = "MC_PCT_GREEN_BARK"
    BARK_VOL_PCT = "BARK_VOL_PCT"
    DRYWT_TO_GREENWT_CONVERSION = "DRYWT_TO_GREENWT_CONVERSION"
    # REF_OWNGRPCD
    OWNGRPCD = "OWNGRPCD"
    # REF_UNIT and COUNTY
    STATECD = "STATECD"
    UNITCD = "UNITCD"
    COUNTYCD = "COUNTYCD"
    COUNTYNM = "COUNTYNM"
    # SURVEY
    STATEAB = "STATEAB"
    STATENM = "STATENM"
    # REF_FIADB_VERSION
    VERSION = "VERSION"


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
