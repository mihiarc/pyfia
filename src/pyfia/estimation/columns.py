"""Column resolution helpers for FIA estimation.

This module centralizes column selection logic used across estimators.
It eliminates duplicated code in volume.py, biomass.py, and tpa.py
by providing shared column lists and resolution functions.
"""

from __future__ import annotations

# Base tree columns always needed for estimation
BASE_TREE_COLUMNS = [
    "CN",
    "PLT_CN",
    "CONDID",
    "STATUSCD",
    "SPCD",
    "DIA",
    "TPA_UNADJ",
    "TREECLCD",
]

# Additional columns commonly needed by specific estimators
VOLUME_COLUMNS = ["VOLCFNET", "VOLCFGRS", "VOLCFSND", "VOLBFNET", "VOLBFGRS"]
BIOMASS_COLUMNS = ["DRYBIO_AG", "DRYBIO_BG", "CARBON_AG", "CARBON_BG"]

# Columns that can be used for grouping from TREE table
TREE_GROUPING_COLUMNS = [
    "HT",
    "ACTUALHT",
    "CR",
    "CCLCD",
    "SPGRPCD",
    "SPCD",
    "TREECLCD",
    "DECAYCD",
    "AGENTCD",  # Mortality agent code (cause of death)
]

# Base condition columns
BASE_COND_COLUMNS = [
    "PLT_CN",
    "CONDID",
    "COND_STATUS_CD",
    "CONDPROP_UNADJ",
]

# Timber land columns (for land_type="timber" filtering)
TIMBER_LAND_COLUMNS = ["SITECLCD", "RESERVCD"]

# Condition grouping columns
COND_GROUPING_COLUMNS = [
    "OWNGRPCD",
    "FORTYPCD",
    "STDSZCD",
    "STDAGE",
    "STDORGCD",
    "SITECLCD",
    "RESERVCD",
    "PROP_BASIS",
    "DSTRBCD1",  # Primary disturbance code
    "DSTRBCD2",  # Secondary disturbance code
    "DSTRBCD3",  # Tertiary disturbance code
]

# Plot-level grouping columns
PLOT_GROUPING_COLUMNS = [
    "STATECD",
    "COUNTYCD",
    "UNITCD",  # FIA survey unit code
    "INVYR",
    "CYCLE",
    "SUBCYCLE",
]


def get_tree_columns(
    estimator_cols: list[str],
    grp_by: str | list[str] | None = None,
    base_cols: list[str] | None = None,
) -> list[str]:
    """
    Resolve tree columns for estimation.

    Combines base columns, estimator-specific columns, and grouping columns
    into a single deduplicated list.

    Parameters
    ----------
    estimator_cols : list[str]
        Estimator-specific columns (e.g., VOLCFNET for volume estimation,
        DRYBIO_AG for biomass estimation)
    grp_by : str or list[str], optional
        User-specified grouping columns. Only columns that exist in
        TREE_GROUPING_COLUMNS will be added.
    base_cols : list[str], optional
        Override default base columns. If not provided, uses BASE_TREE_COLUMNS.

    Returns
    -------
    list[str]
        Complete list of tree columns to select, deduplicated.

    Examples
    --------
    >>> get_tree_columns(["VOLCFNET"])
    ['CN', 'PLT_CN', 'CONDID', 'STATUSCD', 'SPCD', 'DIA', 'TPA_UNADJ', 'TREECLCD', 'VOLCFNET']

    >>> get_tree_columns(["DRYBIO_AG"], grp_by="SPCD")
    ['CN', 'PLT_CN', 'CONDID', 'STATUSCD', 'SPCD', 'DIA', 'TPA_UNADJ', 'TREECLCD', 'DRYBIO_AG']

    >>> get_tree_columns(["VOLCFNET"], grp_by=["SPCD", "HT"])
    ['CN', 'PLT_CN', 'CONDID', 'STATUSCD', 'SPCD', 'DIA', 'TPA_UNADJ', 'TREECLCD', 'VOLCFNET', 'HT']
    """
    cols = list(base_cols or BASE_TREE_COLUMNS)

    # Add estimator-specific columns
    for col in estimator_cols:
        if col not in cols:
            cols.append(col)

    # Add grouping columns if specified
    if grp_by:
        if isinstance(grp_by, str):
            grp_by = [grp_by]
        for col in grp_by:
            if col not in cols and col in TREE_GROUPING_COLUMNS:
                cols.append(col)

    return cols


def get_cond_columns(
    land_type: str = "forest",
    grp_by: str | list[str] | None = None,
    base_cols: list[str] | None = None,
    include_prop_basis: bool = False,
) -> list[str]:
    """
    Resolve condition columns for estimation.

    Combines base columns, land type-specific columns, and grouping columns
    into a single deduplicated list.

    Parameters
    ----------
    land_type : str, default "forest"
        Land type filter. Options:
        - "forest": Include all forest land
        - "timber": Include timberland (adds SITECLCD, RESERVCD)
        - "all": All conditions
    grp_by : str or list[str], optional
        User-specified grouping columns. Only columns that exist in
        COND_GROUPING_COLUMNS will be added.
    base_cols : list[str], optional
        Override default base columns. If not provided, uses BASE_COND_COLUMNS.
    include_prop_basis : bool, default False
        Whether to include PROP_BASIS column for area adjustment calculations.

    Returns
    -------
    list[str]
        Complete list of condition columns to select, deduplicated.

    Examples
    --------
    >>> get_cond_columns()
    ['PLT_CN', 'CONDID', 'COND_STATUS_CD', 'CONDPROP_UNADJ']

    >>> get_cond_columns(land_type="timber")
    ['PLT_CN', 'CONDID', 'COND_STATUS_CD', 'CONDPROP_UNADJ', 'SITECLCD', 'RESERVCD']

    >>> get_cond_columns(grp_by="OWNGRPCD")
    ['PLT_CN', 'CONDID', 'COND_STATUS_CD', 'CONDPROP_UNADJ', 'OWNGRPCD']
    """
    cols = list(base_cols or BASE_COND_COLUMNS)

    # Add PROP_BASIS if requested (needed for area adjustment calculations)
    if include_prop_basis and "PROP_BASIS" not in cols:
        cols.append("PROP_BASIS")

    # Add timber land columns if needed
    if land_type == "timber":
        for col in TIMBER_LAND_COLUMNS:
            if col not in cols:
                cols.append(col)

    # Add grouping columns if specified
    if grp_by:
        if isinstance(grp_by, str):
            grp_by = [grp_by]
        for col in grp_by:
            if col not in cols and col in COND_GROUPING_COLUMNS:
                cols.append(col)

    return cols
