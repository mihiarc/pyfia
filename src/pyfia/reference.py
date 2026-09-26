"""
FIADB reference-table lookups.

Each lookup returns one row per code, with meanings taken verbatim from the
database's REF tables (or the COUNTY and SURVEY tables), so labels match the FIADB
release the database holds. ``join_reference`` attaches a lookup to a frame of
codes and raises :class:`~pyfia.core.exceptions.UnknownCodeError` when the
frame holds a code the reference table doesn't define, instead of silently
leaving a null label.

Lookups read the whole reference table and ignore any EVALID the database is
clipped to.

References
----------
USDA Forest Service. *The Forest Inventory and Analysis Database: Database
Description and User Guide* (chapters 1-3, revision 12.2024), sections on
SURVEY, COUNTY, REF_FOREST_TYPE, REF_FOREST_TYPE_GROUP, REF_SPECIES,
REF_SPECIES_GROUP, REF_OWNGRPCD and REF_UNIT.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence

import polars as pl

from .constants.columns import CondColumns, PlotColumns, RefColumns, TreeColumns
from .constants.tables import TableNames
from .core.exceptions import MissingColumnError, UnknownCodeError
from .core.fia import FIA
from .estimation.utils import ensure_fia_instance

R = RefColumns

#: Columns ``species()`` returns when ``columns`` is None.
SPECIES_COLUMNS: tuple[str, ...] = (
    R.SPCD,
    R.COMMON_NAME,
    R.SCIENTIFIC_NAME,
    R.GENUS,
    R.SPECIES,
    R.SFTWD_HRDWD,
    R.WOODLAND,
    R.MAJOR_SPGRPCD,
    R.E_SPGRPCD,
    "E_SPGRPCD_NAME",
    R.W_SPGRPCD,
    "W_SPGRPCD_NAME",
    R.WOOD_SPGR_GREENVOL_DRYWT,
    R.MC_PCT_GREEN_WOOD,
    R.BARK_SPGR_GREENVOL_DRYWT,
    R.MC_PCT_GREEN_BARK,
    R.BARK_VOL_PCT,
    R.DRYWT_TO_GREENWT_CONVERSION,
)


def _read(db: str | FIA, table: str, columns: list[str]) -> pl.DataFrame:
    """Read reference-table columns, without any EVALID filter."""
    fia, owns_db = ensure_fia_instance(db)
    try:
        return fia._reader.read_table(table, columns=columns, lazy=False)
    finally:
        if owns_db and hasattr(fia, "close"):
            fia.close()


def forest_type_groups(db: str | FIA) -> pl.DataFrame:
    """
    Forest type groups from REF_FOREST_TYPE_GROUP.

    Parameters
    ----------
    db : str | FIA
        Database connection or path to FIA database.

    Returns
    -------
    pl.DataFrame
        One row per group, sorted by code:

        - TYPGRPCD: Forest type group code
        - TYPGRPCD_NAME: Group name, verbatim from ``MEANING``

    See Also
    --------
    forest_types : Forest types with their groups
    join_reference : Attach a lookup to a frame of codes
    """
    return (
        _read(db, TableNames.REF_FOREST_TYPE_GROUP, [R.VALUE, R.MEANING])
        .select(
            pl.col(R.VALUE).cast(pl.Int64).alias(R.TYPGRPCD),
            pl.col(R.MEANING).alias("TYPGRPCD_NAME"),
        )
        .sort(R.TYPGRPCD)
    )


def forest_types(db: str | FIA) -> pl.DataFrame:
    """
    Forest types and their groups from REF_FOREST_TYPE.

    Parameters
    ----------
    db : str | FIA
        Database connection or path to FIA database.

    Returns
    -------
    pl.DataFrame
        One row per forest type, sorted by code:

        - FORTYPCD: Forest type code (``COND.FORTYPCD``)
        - FORTYPCD_NAME: Forest type name, verbatim from ``MEANING``
        - TYPGRPCD: Forest type group code
        - TYPGRPCD_NAME: Group name from REF_FOREST_TYPE_GROUP
        - ALLOWED_IN_FIELD: "Y" if field crews may record the type; group
          codes (100, 120, ...) are "N"

    See Also
    --------
    forest_type_groups : The groups alone
    join_reference : Attach a lookup to a frame of codes

    Examples
    --------
    >>> ft = forest_types(db)
    >>> ft.filter(pl.col("TYPGRPCD") == 160)  # loblolly / shortleaf pine group
    """
    types = _read(
        db,
        TableNames.REF_FOREST_TYPE,
        [R.VALUE, R.MEANING, R.TYPGRPCD, R.ALLOWED_IN_FIELD],
    ).select(
        pl.col(R.VALUE).cast(pl.Int64).alias(CondColumns.FORTYPCD),
        pl.col(R.MEANING).alias("FORTYPCD_NAME"),
        pl.col(R.TYPGRPCD).cast(pl.Int64),
        pl.col(R.ALLOWED_IN_FIELD),
    )
    return (
        types.join(forest_type_groups(db), on=R.TYPGRPCD, how="left")
        .select(
            CondColumns.FORTYPCD,
            "FORTYPCD_NAME",
            R.TYPGRPCD,
            "TYPGRPCD_NAME",
            R.ALLOWED_IN_FIELD,
        )
        .sort(CondColumns.FORTYPCD)
    )


def species_groups(db: str | FIA) -> pl.DataFrame:
    """
    Species groups from REF_SPECIES_GROUP.

    Group codes are unique across regions, so this one lookup labels
    ``TREE.SPGRPCD`` in eastern and western states alike.

    Parameters
    ----------
    db : str | FIA
        Database connection or path to FIA database.

    Returns
    -------
    pl.DataFrame
        One row per group, sorted by code:

        - SPGRPCD: Species group code
        - SPGRPCD_NAME: Group name, verbatim from ``NAME``

    See Also
    --------
    species : Species, with their eastern and western group codes
    """
    return (
        _read(db, TableNames.REF_SPECIES_GROUP, [R.SPGRPCD, R.NAME])
        .select(
            pl.col(R.SPGRPCD).cast(pl.Int64),
            pl.col(R.NAME).alias("SPGRPCD_NAME"),
        )
        .sort(R.SPGRPCD)
    )


def species(db: str | FIA, columns: Sequence[str] | None = None) -> pl.DataFrame:
    """
    Species attributes from REF_SPECIES.

    Parameters
    ----------
    db : str | FIA
        Database connection or path to FIA database.
    columns : sequence of str, optional
        Columns to return besides SPCD, which always comes first. Any
        REF_SPECIES column may be named, as well as ``E_SPGRPCD_NAME`` and
        ``W_SPGRPCD_NAME``. Defaults to :data:`SPECIES_COLUMNS`.

    Returns
    -------
    pl.DataFrame
        One row per species, sorted by SPCD. The default columns are:

        - SPCD, COMMON_NAME, SCIENTIFIC_NAME, GENUS, SPECIES
        - SFTWD_HRDWD: "S" softwood or "H" hardwood
        - WOODLAND: "Y" for woodland species (diameter at root collar)
        - MAJOR_SPGRPCD: 1 pine, 2 other softwood, 3 soft hardwood,
          4 hard hardwood
        - E_SPGRPCD, E_SPGRPCD_NAME: Species group used in eastern states
        - W_SPGRPCD, W_SPGRPCD_NAME: Species group used in western states
        - WOOD_SPGR_GREENVOL_DRYWT, MC_PCT_GREEN_WOOD,
          BARK_SPGR_GREENVOL_DRYWT, MC_PCT_GREEN_BARK, BARK_VOL_PCT,
          DRYWT_TO_GREENWT_CONVERSION: Wood and bark specific gravity,
          moisture content and bark volume, for weight conversions

    Raises
    ------
    MissingColumnError
        If a requested column is not in REF_SPECIES.

    See Also
    --------
    species_groups : Group names alone, for ``TREE.SPGRPCD``
    join_reference : Attach a lookup to a frame of codes

    Notes
    -----
    REF_SPECIES has no single ``SPGRPCD``: a tree's ``TREE.SPGRPCD`` is its
    species' ``E_SPGRPCD`` in eastern states and ``W_SPGRPCD`` in western
    ones, which is why both are returned.

    Examples
    --------
    >>> species(db, columns=["COMMON_NAME", "WOOD_SPGR_GREENVOL_DRYWT"])
    """
    wanted = [c for c in (columns or SPECIES_COLUMNS) if c != R.SPCD]
    group_names = {"E_SPGRPCD_NAME": R.E_SPGRPCD, "W_SPGRPCD_NAME": R.W_SPGRPCD}

    fia, owns_db = ensure_fia_instance(db)
    try:
        available = set(fia._reader.get_table_schema(TableNames.REF_SPECIES))
        missing = [c for c in wanted if c not in available and c not in group_names]
        if missing:
            raise MissingColumnError(missing, TableNames.REF_SPECIES)

        read_cols = [R.SPCD] + [c for c in wanted if c in available]
        read_cols += [
            code
            for name, code in group_names.items()
            if name in wanted and code not in read_cols
        ]
        result = fia._reader.read_table(
            TableNames.REF_SPECIES, columns=read_cols, lazy=False
        ).with_columns(pl.col(R.SPCD).cast(pl.Int64))

        needed_groups = [name for name in group_names if name in wanted]
        if needed_groups:
            groups = species_groups(fia)
            for name in needed_groups:
                code = group_names[name]
                result = result.with_columns(pl.col(code).cast(pl.Int64)).join(
                    groups.rename({R.SPGRPCD: code, "SPGRPCD_NAME": name}),
                    on=code,
                    how="left",
                )
    finally:
        if owns_db and hasattr(fia, "close"):
            fia.close()

    return result.select([R.SPCD, *wanted]).sort(R.SPCD)


def owner_groups(db: str | FIA) -> pl.DataFrame:
    """
    Ownership groups from REF_OWNGRPCD.

    Parameters
    ----------
    db : str | FIA
        Database connection or path to FIA database.

    Returns
    -------
    pl.DataFrame
        One row per group, sorted by code:

        - OWNGRPCD: Owner group code (``COND.OWNGRPCD``)
        - OWNGRPCD_NAME: Group name, verbatim from ``MEANING``

    See Also
    --------
    join_reference : Attach a lookup to a frame of codes
    """
    return (
        _read(db, TableNames.REF_OWNGRPCD, [R.OWNGRPCD, R.MEANING])
        .select(
            pl.col(R.OWNGRPCD).cast(pl.Int64),
            pl.col(R.MEANING).alias("OWNGRPCD_NAME"),
        )
        .sort(R.OWNGRPCD)
    )


def survey_units(db: str | FIA) -> pl.DataFrame:
    """
    FIA survey units from REF_UNIT.

    Parameters
    ----------
    db : str | FIA
        Database connection or path to FIA database.

    Returns
    -------
    pl.DataFrame
        One row per (state, unit), sorted:

        - STATECD: State FIPS code
        - UNITCD: Survey unit code, unique within a state
        - UNIT_NAME: Unit name, verbatim from ``MEANING``

    See Also
    --------
    counties : Counties with their survey unit
    join_reference : Attach a lookup to a frame of codes
    """
    return (
        _read(db, TableNames.REF_UNIT, [R.STATECD, R.VALUE, R.MEANING])
        .select(
            pl.col(R.STATECD).cast(pl.Int64),
            pl.col(R.VALUE).cast(pl.Int64).alias(R.UNITCD),
            pl.col(R.MEANING).alias("UNIT_NAME"),
        )
        .sort(R.STATECD, R.UNITCD)
    )


def counties(db: str | FIA) -> pl.DataFrame:
    """
    Counties from the COUNTY table.

    Parameters
    ----------
    db : str | FIA
        Database connection or path to FIA database.

    Returns
    -------
    pl.DataFrame
        One row per (state, county), sorted:

        - STATECD: State FIPS code
        - COUNTYCD: County FIPS code, unique within a state
        - COUNTYNM: County name
        - UNITCD: The survey unit the county belongs to

    See Also
    --------
    survey_units : Survey unit names
    join_reference : Attach a lookup to a frame of codes
    """
    return (
        _read(db, TableNames.COUNTY, [R.STATECD, R.COUNTYCD, R.COUNTYNM, R.UNITCD])
        .select(
            pl.col(R.STATECD).cast(pl.Int64),
            pl.col(R.COUNTYCD).cast(pl.Int64),
            pl.col(R.COUNTYNM),
            pl.col(R.UNITCD).cast(pl.Int64),
        )
        .sort(R.STATECD, R.COUNTYCD)
    )


def states(db: str | FIA) -> pl.DataFrame:
    """
    States and territories in the database, from the SURVEY table.

    FIADB distributions don't all include REF_STATE, but every one has
    SURVEY, which names the states (and territories such as Puerto Rico and
    Guam) whose inventories the database holds.

    Parameters
    ----------
    db : str | FIA
        Database connection or path to FIA database.

    Returns
    -------
    pl.DataFrame
        One row per state in the database, sorted by code:

        - STATECD: State FIPS code
        - STATE_NAME: Name, from ``SURVEY.STATENM``
        - STATE_ABBR: Postal abbreviation, from ``SURVEY.STATEAB``

    See Also
    --------
    join_reference : Attach a lookup to a frame of codes
    """
    return (
        _read(db, TableNames.SURVEY, [R.STATECD, R.STATENM, R.STATEAB])
        .select(
            pl.col(R.STATECD).cast(pl.Int64),
            pl.col(R.STATENM).alias("STATE_NAME"),
            pl.col(R.STATEAB).alias("STATE_ABBR"),
        )
        .unique(R.STATECD, keep="first", maintain_order=True)
        .sort(R.STATECD)
    )


#: kind -> (lookup, key columns, table the codes come from)
_KINDS: dict[str, tuple[Callable[..., pl.DataFrame], tuple[str, ...], str]] = {
    "forest_type": (forest_types, (CondColumns.FORTYPCD,), TableNames.REF_FOREST_TYPE),
    "forest_type_group": (
        forest_type_groups,
        (R.TYPGRPCD,),
        TableNames.REF_FOREST_TYPE_GROUP,
    ),
    "species": (species, (TreeColumns.SPCD,), TableNames.REF_SPECIES),
    "species_group": (
        species_groups,
        (TreeColumns.SPGRPCD,),
        TableNames.REF_SPECIES_GROUP,
    ),
    "owner_group": (owner_groups, (CondColumns.OWNGRPCD,), TableNames.REF_OWNGRPCD),
    "survey_unit": (survey_units, (R.STATECD, R.UNITCD), TableNames.REF_UNIT),
    "county": (counties, (R.STATECD, R.COUNTYCD), TableNames.COUNTY),
    "state": (states, (PlotColumns.STATECD,), TableNames.SURVEY),
}

#: The reference kinds ``join_reference`` accepts.
REFERENCE_KINDS: tuple[str, ...] = tuple(_KINDS)


def join_reference(
    df: pl.DataFrame,
    db: str | FIA,
    kind: str,
    on: str | Sequence[str] | None = None,
    columns: Sequence[str] | None = None,
    prefix: str = "",
) -> pl.DataFrame:
    """
    Attach reference-table labels to a frame of FIA codes.

    Left-joins a lookup onto ``df`` by its code column(s), keeping every row
    of ``df`` in order. A null code gets null labels; a non-null code the
    reference table doesn't define raises, so a stale or mismatched lookup
    can't pass unnoticed.

    Parameters
    ----------
    df : pl.DataFrame
        Data holding the codes, such as estimator output grouped by
        ``FORTYPCD``.
    db : str | FIA
        Database connection or path to FIA database.
    kind : {'forest_type', 'forest_type_group', 'species', 'species_group', \
'owner_group', 'survey_unit', 'county', 'state'}
        Which lookup to join:

        - 'forest_type': :func:`forest_types` on FORTYPCD
        - 'forest_type_group': :func:`forest_type_groups` on TYPGRPCD
        - 'species': :func:`species` on SPCD
        - 'species_group': :func:`species_groups` on SPGRPCD
        - 'owner_group': :func:`owner_groups` on OWNGRPCD
        - 'survey_unit': :func:`survey_units` on (STATECD, UNITCD)
        - 'county': :func:`counties` on (STATECD, COUNTYCD)
        - 'state': :func:`states` on STATECD
    on : str or sequence of str, optional
        Column(s) of ``df`` that hold the codes, in the order of the lookup's
        key columns. Defaults to the key names, e.g. ``"FORTYPCD"``. Use it for
        prefixed columns such as ``"t1_FORTYPCD"``.
    columns : sequence of str, optional
        Lookup columns to add. Defaults to all of the lookup's non-key
        columns.
    prefix : str, default ""
        Prefix for the added columns, e.g. ``"t1_"`` to label time-1 codes
        next to time-2 ones.

    Returns
    -------
    pl.DataFrame
        ``df`` with the requested lookup columns added, rows in their
        original order. The code columns keep their original dtype.

    Raises
    ------
    UnknownCodeError
        If ``df`` holds a non-null code the reference table doesn't define.
        The error lists the codes.
    MissingColumnError
        If an ``on`` column is not in ``df``, or a requested lookup column
        doesn't exist.
    ValueError
        If ``kind`` is unknown, ``on`` has the wrong number of columns, or an
        added column would overwrite one already in ``df``.

    See Also
    --------
    forest_types, species, owner_groups, survey_units, counties : The lookups

    Examples
    --------
    Label a forest-type grouping:

    >>> result = area(db, grp_by="FORTYPCD")
    >>> result = join_reference(result, db, "forest_type")

    Label time-1 and time-2 owner groups side by side:

    >>> df = join_reference(df, db, "owner_group", on="t1_OWNGRPCD", prefix="t1_")
    >>> df = join_reference(df, db, "owner_group", on="t2_OWNGRPCD", prefix="t2_")

    Counties need the state too:

    >>> join_reference(result, db, "county")  # joins on STATECD and COUNTYCD
    """
    if kind not in _KINDS:
        raise ValueError(f"kind must be one of {REFERENCE_KINDS}, got {kind!r}")
    lookup_fn, keys, table = _KINDS[kind]

    on_cols = list(keys) if on is None else [on] if isinstance(on, str) else list(on)
    if len(on_cols) != len(keys):
        raise ValueError(
            f"kind {kind!r} joins on {len(keys)} column(s) {list(keys)}; "
            f"got on={on_cols}"
        )
    missing_on = [c for c in on_cols if c not in df.columns]
    if missing_on:
        raise MissingColumnError(missing_on)

    lookup = lookup_fn(db)
    value_cols = [c for c in lookup.columns if c not in keys]
    if columns is not None:
        missing = [c for c in columns if c not in value_cols]
        if missing:
            raise MissingColumnError(missing, table)
        value_cols = list(columns)
    added = {c: f"{prefix}{c}" for c in value_cols}
    clash = [name for name in added.values() if name in df.columns]
    if clash:
        raise ValueError(
            f"join_reference would overwrite existing column(s) {clash}; "
            "pass prefix= or drop them first"
        )

    # Join on Int64 copies of the codes so the caller's columns keep their dtype
    tmp_keys = [f"__ref_key_{i}" for i in range(len(keys))]
    left = df.with_columns(
        pl.col(c).cast(pl.Int64).alias(t) for c, t in zip(on_cols, tmp_keys)
    )
    right = lookup.select(
        *[pl.col(k).alias(t) for k, t in zip(keys, tmp_keys)],
        *[pl.col(c).alias(added[c]) for c in value_cols],
    )

    if right.select(tmp_keys).is_duplicated().any():
        raise ValueError(f"{table} has duplicate {kind} codes; can't join")

    codes = left.select(tmp_keys).drop_nulls().unique()
    unknown = codes.join(right.select(tmp_keys), on=tmp_keys, how="anti")
    if unknown.height:
        rows = sorted(unknown.rows())
        raise UnknownCodeError(kind, [r[0] if len(r) == 1 else r for r in rows], table)

    return (
        left.with_row_index("__ref_row")
        .join(right, on=tmp_keys, how="left")
        .sort("__ref_row")
        .drop("__ref_row", *tmp_keys)
    )
