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


def collect_referenced_columns(
    grp_by: str | list[str] | None,
    area_domain: str | None = None,
    tree_domain: str | None = None,
) -> list[str]:
    """Collect every column a user referenced via grp_by and domain filters.

    Returns a de-duplicated, order-preserving list of column names drawn from
    ``grp_by`` plus any columns referenced in ``area_domain`` / ``tree_domain``.
    Callers route these to the table that actually holds each column (via
    :func:`columns_in_table`) so that grouping and domain filtering resolve
    consistently across estimators (issues #103 and #104).

    Parameters
    ----------
    grp_by : str or list[str], optional
        User-specified grouping column(s).
    area_domain : str, optional
        SQL-like area/condition filter expression.
    tree_domain : str, optional
        SQL-like tree filter expression.

    Returns
    -------
    list[str]
        Unique column names referenced by the user, in first-seen order.
    """
    # Local import avoids a module-load cycle (filtering imports estimation).
    from ..filtering.parser import DomainExpressionParser

    cols: list[str] = []
    if grp_by:
        cols.extend([grp_by] if isinstance(grp_by, str) else list(grp_by))
    for expr in (area_domain, tree_domain):
        if expr:
            cols.extend(DomainExpressionParser.extract_columns(expr))

    seen: set[str] = set()
    out: list[str] = []
    for col in cols:
        if col and col not in seen:
            seen.add(col)
            out.append(col)
    return out


def columns_in_table(db, table_name: str, candidates: list[str]) -> list[str]:
    """Return the subset of ``candidates`` that exist in ``table_name``'s schema.

    Used to schema-route user-referenced columns into the correct table load.
    Falls back to returning all candidates when the schema cannot be read
    (e.g. a mock database in unit tests), preserving prior best-effort behavior.

    Parameters
    ----------
    db : FIA
        Database connection exposing ``_reader.get_table_schema``.
    table_name : str
        Name of the table whose schema to check (e.g. "COND", "TREE", "PLOT").
    candidates : list[str]
        Column names to test for membership.

    Returns
    -------
    list[str]
        Candidates present in the table schema (order preserved), or all
        candidates if the schema is unavailable.
    """
    try:
        schema = db._reader.get_table_schema(table_name)
        if not isinstance(schema, dict):
            return list(candidates)
        names = set(schema.keys())
    except (AttributeError, TypeError, KeyError):
        return list(candidates)
    return [c for c in candidates if c in names]


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
