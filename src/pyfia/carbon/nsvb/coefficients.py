"""
NSVB coefficient table loaders and lookup precedence.

Loads the vendored CSVs from ``pyfia.carbon.nsvb.data`` via ``importlib.resources``
(wheel-safe) and provides per-tree coefficient resolution following the NSVB
lookup precedence documented in the GTR-WO-104 worked examples.

Lookup precedence (per ``gtr_wo104_westfall2023.md:684``):

1. ``SPCD + DIVISION + STDORGCD`` exact match
2. ``SPCD + DIVISION`` (STDORGCD null)
3. ``SPCD`` only (DIVISION and STDORGCD both null) — the species-level fallback
4. ``JENKINS_SPGRPCD`` fallback (Model 5, with WDSG multiplication required)

The CSVs already include species-level fallback rows; we do not need to synthesize
them. Phase 1.5 added the ``ECOSUBCD → Bailey DIVISION`` mapping via
:func:`ecosubcd_to_division`, activating Level 2 of the lookup precedence.
Level 1 (STDORGCD) is still unused (~10 rows across all 5 tables).
"""

from __future__ import annotations

import functools
from dataclasses import dataclass
from importlib import resources

import polars as pl

# Filename map for the vendored CSVs (relative to ``pyfia.carbon.nsvb.data``).
_CSV_FILES = {
    "volib_spcd": "volib_spcd.csv",
    "volib_jenkins": "volib_jenkins.csv",
    "volbk_spcd": "volbk_spcd.csv",
    "volbk_jenkins": "volbk_jenkins.csv",
    "bark_biomass_spcd": "bark_biomass_spcd.csv",
    "bark_biomass_jenkins": "bark_biomass_jenkins.csv",
    "branch_biomass_spcd": "branch_biomass_spcd.csv",
    "branch_biomass_jenkins": "branch_biomass_jenkins.csv",
    "total_biomass_spcd": "total_biomass_spcd.csv",
    "total_biomass_jenkins": "total_biomass_jenkins.csv",
}


@dataclass(frozen=True)
class CoefficientTables:
    """Bundle of all NSVB coefficient tables loaded as Polars DataFrames.

    Each ``*_spcd`` table has columns
    ``SPCD, DIVISION, STDORGCD, model, a, a1, b, b1, c, c1`` and each
    ``*_jenkins`` table has columns ``JENKINS_SPGRPCD, model, a, b, c``
    (or close to it — the schemas vary slightly across the S*b tables).
    """

    volib_spcd: pl.DataFrame
    volib_jenkins: pl.DataFrame
    volbk_spcd: pl.DataFrame
    volbk_jenkins: pl.DataFrame
    bark_biomass_spcd: pl.DataFrame
    bark_biomass_jenkins: pl.DataFrame
    branch_biomass_spcd: pl.DataFrame
    branch_biomass_jenkins: pl.DataFrame
    total_biomass_spcd: pl.DataFrame
    total_biomass_jenkins: pl.DataFrame


@dataclass(frozen=True)
class VectorizedLookupTables:
    """Bundle of join-ready NSVB coefficient lookup tables for the vectorized path.

    Each component is represented by three parallel lookups for Levels 2–4 of
    the NSVB precedence:

    - ``*_div``: DIVISION-specific rows (DIVISION non-null + STDORGCD null) with
      the columns ``(SPCD, DIVISION, model, a, b, b1, c)`` — joined on the
      ``(SPCD, DIVISION)`` composite key (Level 2).
    - ``*_spcd``: species-level rows (DIVISION null + STDORGCD null) with the
      columns ``(SPCD, model, a, b, b1, c)`` — ready for a left join on
      ``SPCD`` (Level 3).
    - ``*_jen``: Jenkins-group fallback rows with the columns
      ``(JENKINS_SPGRPCD, model, a, b, b1, c)`` — ready for a left join on
      ``JENKINS_SPGRPCD`` (Level 4). ``b1`` is synthesized as ``0.0`` because
      the Jenkins tables only carry ``(a, b, c)`` and Model 5 (the only form
      Jenkins rows dispatch to) does not use ``b1``.

    The vectorized orchestrator runs all three joins per component and then
    coalesces DIVISION first, species-level second, Jenkins third, replicating
    the NSVB lookup precedence (Level 2 → Level 3 → Level 4) without any
    Python-level loops. Level 1 (SPCD + DIVISION + STDORGCD) is still dead
    code in Phase 1.5 — only ~10 rows across all 5 tables — and is deferred
    until the validation gate justifies it.
    """

    volib_spcd: pl.DataFrame
    volib_jen: pl.DataFrame
    volib_div: pl.DataFrame
    volbk_spcd: pl.DataFrame
    volbk_jen: pl.DataFrame
    volbk_div: pl.DataFrame
    bark_bio_spcd: pl.DataFrame
    bark_bio_jen: pl.DataFrame
    bark_bio_div: pl.DataFrame
    branch_bio_spcd: pl.DataFrame
    branch_bio_jen: pl.DataFrame
    branch_bio_div: pl.DataFrame
    total_agb_spcd: pl.DataFrame
    total_agb_jen: pl.DataFrame
    total_agb_div: pl.DataFrame


# Common coefficient columns used by the vectorized path. Matches the inputs
# consumed by ``nsvb_biomass_expr``.
_VECTORIZED_COEF_COLS = ("model", "a", "b", "b1", "c")


def build_species_level_lookup(table_spcd: pl.DataFrame) -> pl.DataFrame:
    """Prepare a ``*_spcd`` coefficient table for vectorized SPCD joins.

    Filters to Phase 1's species-level rows (``DIVISION`` is null AND
    ``STDORGCD`` is null), selects the common coefficient columns used by
    the vectorized biomass expression, and returns a DataFrame keyed on
    ``SPCD``. The DIVISION-specific rows (Levels 1-2 of the NSVB lookup
    precedence) are deliberately dropped because Phase 1 has no
    ``PLOT.ECOSUBCD → DIVISION`` mapping.

    Also drops Model 2 / Model 4 rows with a null ``b1`` as a defensive
    measure. In the downstream ``_join_and_eval_component`` coalesce, a
    null ``b1`` on the species-level side would silently fall back to the
    Jenkins ``b1=0.0`` synthetic value and corrupt the Model 2/4 math
    row-wise. The current vendored CSVs have no such rows (all null
    ``b1`` entries are on Model 1 rows, which do not consume ``b1``),
    so this filter is a no-op today. It is a regression guard against
    future CSV re-vendor drift: a rogue null ``b1`` on a Model 2/4 row
    will be dropped here and the SPCD will fall through to Jenkins as a
    whole, rather than silently producing wrong per-row math.

    Parameters
    ----------
    table_spcd : pl.DataFrame
        One of the 5 ``*_spcd`` coefficient tables from
        :class:`CoefficientTables`.

    Returns
    -------
    pl.DataFrame
        Columns ``(SPCD, model, a, b, b1, c)``. One row per SPCD that has a
        species-level entry in the source table.
    """
    return table_spcd.filter(
        pl.col("DIVISION").is_null()
        & pl.col("STDORGCD").is_null()
        & ~(pl.col("model").is_in([2, 4]) & pl.col("b1").is_null())
    ).select(["SPCD", *_VECTORIZED_COEF_COLS])


def build_jenkins_lookup(table_jenkins: pl.DataFrame) -> pl.DataFrame:
    """Prepare a ``*_jenkins`` coefficient table for vectorized joins.

    Selects the common coefficient columns and synthesizes a ``b1`` column
    set to ``0.0`` — Jenkins tables only carry ``(a, b, c)`` because Model 5
    (the only form Jenkins rows ever dispatch to) has no ``b1`` parameter.
    The synthesized column keeps the schema identical to the species-level
    lookup so the downstream orchestrator can coalesce across both tables
    uniformly.

    Parameters
    ----------
    table_jenkins : pl.DataFrame
        One of the 5 ``*_jenkins`` coefficient tables from
        :class:`CoefficientTables`.

    Returns
    -------
    pl.DataFrame
        Columns ``(JENKINS_SPGRPCD, model, a, b, b1, c)``.
    """
    return table_jenkins.select(
        [
            pl.col("JENKINS_SPGRPCD"),
            pl.col("model"),
            pl.col("a"),
            pl.col("b"),
            pl.lit(0.0, dtype=pl.Float64).alias("b1"),
            pl.col("c"),
        ]
    )


def build_division_lookup(table_spcd: pl.DataFrame) -> pl.DataFrame:
    """Prepare a ``*_spcd`` coefficient table for vectorized ``(SPCD, DIVISION)``
    joins (Level 2 of the NSVB lookup precedence).

    Filters to rows where ``DIVISION`` is non-null AND ``STDORGCD`` is null
    (the DIVISION-specific Bailey ecoprovince refinements that Phase 1 of
    the carbon estimator skipped). Returns a DataFrame keyed on the
    composite ``(SPCD, DIVISION)``; the vectorized orchestrator joins this
    first, then falls through to the species-level lookup (Level 3) and
    the Jenkins fallback (Level 4) via a 3-way coalesce.

    Level 1 of the NSVB precedence (``SPCD + DIVISION + STDORGCD``) is still
    deliberately excluded — across all 5 coefficient tables it is only ~10
    rows and would require threading ``COND.STDORGCD`` through the pipeline.
    Revisit once the DIVISION closure has been measured.

    Applies the same defensive null-``b1`` filter as
    :func:`build_species_level_lookup`: a Model 2 / Model 4 row with a null
    ``b1`` would silently mix rows in the downstream coalesce, so such rows
    are dropped from the lookup.

    Parameters
    ----------
    table_spcd : pl.DataFrame
        One of the 5 ``*_spcd`` coefficient tables from
        :class:`CoefficientTables`.

    Returns
    -------
    pl.DataFrame
        Columns ``(SPCD, DIVISION, model, a, b, b1, c)``. One row per
        unique ``(SPCD, DIVISION)`` pair with a Level-2 entry.
    """
    return table_spcd.filter(
        pl.col("DIVISION").is_not_null()
        & pl.col("STDORGCD").is_null()
        & ~(pl.col("model").is_in([2, 4]) & pl.col("b1").is_null())
    ).select(["SPCD", "DIVISION", *_VECTORIZED_COEF_COLS])


def ecosubcd_to_division(ecosubcd: str | None) -> str | None:
    """Extract the Bailey DIVISION code from a ``PLOTGEOM.ECOSUBCD`` value.

    ``ECOSUBCD`` is a 5-7 character Bailey ecoprovince subsection code
    (e.g., ``"231Ae"`` for the Southeastern Mixed Forest section 231A,
    subsection 231Ae, or ``"M231Aa"`` for the Ouachita Mixed Forest
    mountain variant). The Bailey hierarchy is:

    ::

        Domain → Division → Province → Section → Subsection
        200    → 230      → 231      → 231A    → 231Ae

    This function walks one level up the hierarchy from Subsection to
    Division by:

    - extracting the 3-digit Province code (first 3 chars after any ``M``)
    - replacing its last digit with ``"0"`` to obtain the Division
    - preserving the ``"M"`` prefix for mountain Divisions

    Examples
    --------
    >>> ecosubcd_to_division("231Ae")
    '230'
    >>> ecosubcd_to_division("232Bh")
    '230'
    >>> ecosubcd_to_division("M231Aa")
    'M230'
    >>> ecosubcd_to_division("220Eb")
    '220'
    >>> ecosubcd_to_division(None) is None
    True
    >>> ecosubcd_to_division("") is None
    True
    >>> ecosubcd_to_division("XYZ") is None
    True

    Parameters
    ----------
    ecosubcd : str or None
        The ECOSUBCD value from ``PLOTGEOM.ECOSUBCD`` or ``PLOT.ECOSUBCD``
        (the column exists on both tables — pyfia's DataMart CSV downloads
        pull it reliably from ``PLOTGEOM``).

    Returns
    -------
    str or None
        The Bailey DIVISION code matching the ``DIVISION`` column in the
        NSVB coefficient tables (e.g., ``"230"``, ``"M230"``, ``"240"``),
        or ``None`` if the input is null, empty, or malformed.
    """
    if ecosubcd is None:
        return None
    s = ecosubcd.strip().upper()
    if not s:
        return None
    m_prefix = ""
    if s.startswith("M"):
        m_prefix = "M"
        s = s[1:]
    if len(s) < 3 or not s[:2].isdigit() or not s[2].isdigit():
        return None
    return m_prefix + s[:2] + "0"


def ecosubcd_to_division_expr(col: str = "ECOSUBCD") -> pl.Expr:
    """Vectorized Polars expression equivalent of :func:`ecosubcd_to_division`.

    Replaces ``pl.col(col).map_elements(ecosubcd_to_division)`` with pure
    Polars string operations — no per-row Python dispatch. Logic:

    1. Uppercase + strip the input.
    2. If it starts with ``"M"``, take ``"M" + chars[1:3] + "0"``.
    3. Otherwise take ``chars[0:2] + "0"``.
    4. Null / empty / malformed inputs produce null.

    Returns
    -------
    pl.Expr
        Expression producing the Bailey DIVISION code (Utf8, nullable).
    """
    raw = pl.col(col).str.strip_chars().str.to_uppercase()
    has_m = raw.str.starts_with("M")
    # Characters after any M prefix
    digits_m = raw.str.slice(1)  # "M231Aa" → "231Aa"
    digits_no_m = raw  # "231Ae" → "231Ae"

    # Build division: first 2 digits + "0", with M prefix preserved
    div_m = pl.lit("M") + digits_m.str.slice(0, 2) + pl.lit("0")
    div_no_m = digits_no_m.str.slice(0, 2) + pl.lit("0")

    # Validate: the 3rd character (after M if present) must be a digit,
    # and minimum 3 chars after prefix.  Use a regex match for the prefix
    # digits to detect malformed values.
    valid_m = digits_m.str.contains(r"^\d{3}")
    valid_no_m = digits_no_m.str.contains(r"^\d{3}")

    return (
        pl.when(raw.is_null() | (raw == ""))
        .then(pl.lit(None, dtype=pl.Utf8))
        .when(has_m & valid_m)
        .then(div_m)
        .when(~has_m & valid_no_m)
        .then(div_no_m)
        .otherwise(pl.lit(None, dtype=pl.Utf8))
    )


@functools.lru_cache(maxsize=1)
def get_vectorized_lookup_tables() -> VectorizedLookupTables:
    """Return the full bundle of join-ready NSVB lookup tables.

    Calls :func:`build_species_level_lookup`, :func:`build_jenkins_lookup`,
    and :func:`build_division_lookup` on each of the 5 component table
    pairs from :func:`load_nsvb_coefficients` and caches the result at
    process level. This is the single entry point the vectorized live-tree
    biomass orchestrator uses.

    The bundle is small (~1200 rows across all 15 DataFrames) and is shared
    across every ``LiveTreeEstimator`` invocation in the process.
    """
    raw = load_nsvb_coefficients()
    return VectorizedLookupTables(
        volib_spcd=build_species_level_lookup(raw.volib_spcd),
        volib_jen=build_jenkins_lookup(raw.volib_jenkins),
        volib_div=build_division_lookup(raw.volib_spcd),
        volbk_spcd=build_species_level_lookup(raw.volbk_spcd),
        volbk_jen=build_jenkins_lookup(raw.volbk_jenkins),
        volbk_div=build_division_lookup(raw.volbk_spcd),
        bark_bio_spcd=build_species_level_lookup(raw.bark_biomass_spcd),
        bark_bio_jen=build_jenkins_lookup(raw.bark_biomass_jenkins),
        bark_bio_div=build_division_lookup(raw.bark_biomass_spcd),
        branch_bio_spcd=build_species_level_lookup(raw.branch_biomass_spcd),
        branch_bio_jen=build_jenkins_lookup(raw.branch_biomass_jenkins),
        branch_bio_div=build_division_lookup(raw.branch_biomass_spcd),
        total_agb_spcd=build_species_level_lookup(raw.total_biomass_spcd),
        total_agb_jen=build_jenkins_lookup(raw.total_biomass_jenkins),
        total_agb_div=build_division_lookup(raw.total_biomass_spcd),
    )


# Explicit dtypes for the SPCD-keyed tables. DIVISION is a Bailey ecoprovince
# code (e.g., "M240", "240A") — always Utf8. STDORGCD is a nullable integer
# (1 or 2), stored as Int64. Passing these explicitly avoids relying on schema
# inference, which previously only worked because letters happened to appear
# in the first 10k rows of every table and would silently break if a future
# re-vendor reordered the rows.
_SPCD_DTYPES = {
    "SPCD": pl.Int64,
    "DIVISION": pl.Utf8,
    "STDORGCD": pl.Int64,
    "model": pl.Int64,
    "a": pl.Float64,
    "a1": pl.Float64,
    "b": pl.Float64,
    "b1": pl.Float64,
    "c": pl.Float64,
    "c1": pl.Float64,
}

# Jenkins-keyed tables have a narrower schema and no DIVISION/STDORGCD.
_JENKINS_DTYPES = {
    "JENKINS_SPGRPCD": pl.Int64,
    "model": pl.Int64,
    "a": pl.Float64,
    "b": pl.Float64,
    "c": pl.Float64,
}


@functools.lru_cache(maxsize=1)
def load_nsvb_coefficients() -> CoefficientTables:
    """Load all NSVB coefficient tables from the vendored CSV bundle.

    Cached at process level via ``@lru_cache``: subsequent calls return the same
    ``CoefficientTables`` instance in O(1). Uses ``importlib.resources.files``
    so it works in dev installs, installed wheels, zip-imported wheels, and
    PyOxidizer-style frozen builds without ``__file__`` path tricks.

    Schemas are passed explicitly via ``schema_overrides`` so DIVISION is always
    loaded as ``Utf8`` and STDORGCD as ``Int64`` regardless of the first-row
    content of the CSV. This is what lets the upcoming ``ECOSUBCD → DIVISION``
    lookup key-join cleanly against the coefficient tables.
    """
    data_pkg = resources.files("pyfia.carbon.nsvb.data")
    loaded: dict[str, pl.DataFrame] = {}
    for key, filename in _CSV_FILES.items():
        schema = _JENKINS_DTYPES if key.endswith("_jenkins") else _SPCD_DTYPES
        with resources.as_file(data_pkg / filename) as path:
            df = pl.read_csv(path, schema_overrides=schema)
        loaded[key] = df
    return CoefficientTables(**loaded)


def _row_to_dict(row: pl.DataFrame, source: str) -> dict:
    """Convert a single-row Polars DataFrame to a coefficient dict.

    Returns the row's columns as a plain Python dict, with NaN/null values
    coerced to 0.0 for numeric fields and the lookup ``source`` tag attached.
    """
    raw = row.to_dicts()[0]
    out: dict = {"source": source}
    for key in ("model", "a", "a1", "b", "b1", "c", "c1"):
        val = raw.get(key)
        if val is None:
            out[key] = 0.0
        else:
            out[key] = float(val)
    out["model"] = int(out["model"])
    return out


def lookup_coefficients(
    table_spcd: pl.DataFrame,
    table_jenkins: pl.DataFrame,
    spcd: int,
    jenkins_spgrpcd: int | None = None,
    division: str | None = None,
    stdorgcd: int | None = None,
) -> dict:
    """Resolve a coefficient row for one tree following NSVB lookup precedence.

    The Phase 1 implementation walks precedence levels 1-4 in order. Phase 1
    typically only hits levels 3 and 4: the SPCD-only species-level row, or
    the Jenkins fallback for unsupported species. The DIVISION-specific levels
    1 and 2 are wired but unused until pyFIA gains a ``PLOT.ECOSUBCD →
    DIVISION`` mapping.

    Parameters
    ----------
    table_spcd : pl.DataFrame
        The ``S*a`` species-keyed table (e.g., ``volib_spcd``).
    table_jenkins : pl.DataFrame
        The ``S*b`` Jenkins-keyed fallback table.
    spcd : int
        FIA species code from ``TREE.SPCD``.
    jenkins_spgrpcd : int, optional
        Jenkins species group code from ``REF_SPECIES.JENKINS_SPGRPCD``.
        Required if the species is not in the SPCD table (level 4 fallback).
    division : str, optional
        Bailey ecoprovince division code (e.g., ``"M240"``). Phase 1 always
        passes ``None`` here.
    stdorgcd : int, optional
        Stand origin code from ``COND.STDORGCD``. Phase 1 always passes ``None``.

    Returns
    -------
    dict
        Coefficient dict with keys ``model, a, a1, b, b1, c, c1, source``.
        ``source`` is one of ``"spcd_division_stdorg"``, ``"spcd_division"``,
        ``"spcd"``, or ``"jenkins"``.

    Raises
    ------
    KeyError
        If neither the SPCD nor any Jenkins fallback can be resolved.
    """
    # TODO(PR 2): Scalar reference implementation. The vectorized
    # LiveTreeEstimator must replace per-tree calls with a polars join on
    # SPCD against the coefficient tables, not call this function in a loop.
    # See `pyfia/carbon/__init__.py` "Architectural rules" rule 2.
    df = table_spcd.filter(pl.col("SPCD") == spcd)

    # Level 1: SPCD + DIVISION + STDORGCD exact match
    if division is not None and stdorgcd is not None:
        match = df.filter(
            (pl.col("DIVISION") == division) & (pl.col("STDORGCD") == stdorgcd)
        )
        if match.height > 0:
            return _row_to_dict(match.head(1), "spcd_division_stdorg")

    # Level 2: SPCD + DIVISION (STDORGCD null)
    if division is not None:
        match = df.filter(
            (pl.col("DIVISION") == division) & pl.col("STDORGCD").is_null()
        )
        if match.height > 0:
            return _row_to_dict(match.head(1), "spcd_division")

    # Level 3: SPCD only (DIVISION and STDORGCD both null) — the species-level row
    match = df.filter(pl.col("DIVISION").is_null() & pl.col("STDORGCD").is_null())
    if match.height > 0:
        return _row_to_dict(match.head(1), "spcd")

    # Level 4: Jenkins fallback. Note that S*b tables have different schemas
    # (no DIVISION/STDORGCD columns) and are keyed on JENKINS_SPGRPCD.
    if jenkins_spgrpcd is not None:
        jdf = table_jenkins.filter(pl.col("JENKINS_SPGRPCD") == jenkins_spgrpcd)
        if jdf.height > 0:
            return _row_to_dict(jdf.head(1), "jenkins")

    raise KeyError(
        f"No NSVB coefficients for SPCD={spcd}, JENKINS_SPGRPCD={jenkins_spgrpcd}, "
        f"DIVISION={division}, STDORGCD={stdorgcd}. Check the species coverage "
        "in src/pyfia/carbon/nsvb/data/."
    )
