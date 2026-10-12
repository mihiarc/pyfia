"""
NSVB coefficient table loaders and lookup precedence.

Loads the vendored CSVs from ``pyfia.carbon.nsvb.data`` via ``importlib.resources``
(wheel-safe) and provides per-tree coefficient resolution following the NSVB
lookup precedence documented in the GTR-WO-104 worked examples.

Lookup precedence (GTR-WO-104 p. 8, 11 — spcd tables are keyed on
species-ecodivision-stand origin; DIVISION falls back to "no ecodivision
noted"):

1.  ``SPCD + DIVISION + STDORGCD`` exact match (:func:`build_division_stdorg_lookup`)
1b. ``SPCD + STDORGCD`` (DIVISION null) — stand-origin, no ecodivision noted
    (:func:`build_stdorg_lookup`)
2.  ``SPCD + DIVISION`` (STDORGCD null) — ecodivision, no stand-origin split
    (:func:`build_division_lookup`)
3.  ``SPCD`` only (DIVISION and STDORGCD both null) — species-level fallback
    (:func:`build_species_level_lookup`)
4.  ``JENKINS_SPGRPCD`` fallback (Model 5, with WDSG multiplication required)

The CSVs already include species-level fallback rows; we do not need to synthesize
them. Phase 1.5 added the ``ECOSUBCD → Bailey DIVISION`` mapping via
:func:`ecosubcd_to_division`, activating Level 2. Levels 1/1b (STDORGCD) were
activated for issue #123: slash pine (SPCD 111) and loblolly pine (SPCD 131)
are fit separately for planted (STDORGCD=1) vs natural (0) stands and carry
*no* STDORGCD-null row, so without these tiers they collapse to the Jenkins
fallback. ``COND.STDORGCD`` is threaded through the estimators to drive them.
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

    Each component is represented by five parallel lookups spanning the full
    NSVB precedence (GTR-WO-104 p. 11), joined and coalesced most-specific
    first by ``_join_and_eval_component``:

    - ``*_divorg``: ``(SPCD, DIVISION, STDORGCD)`` rows (both non-null) —
      Level 1, the ecodivision + stand-origin coefficients.
    - ``*_org``: ``(SPCD, STDORGCD)`` rows (DIVISION null, STDORGCD non-null) —
      Level 1b, the stand-origin "no ecodivision noted" row.
    - ``*_div``: ``(SPCD, DIVISION)`` rows (DIVISION non-null, STDORGCD null) —
      Level 2.
    - ``*_spcd``: species-level rows (DIVISION null, STDORGCD null) — Level 3,
      joined on ``SPCD``.
    - ``*_jen``: Jenkins-group fallback rows joined on ``JENKINS_SPGRPCD`` —
      Level 4. ``b1`` is synthesized as ``0.0`` and ``a1``/``c1`` as null
      because the Jenkins tables only carry ``(a, b, c)`` and Model 5 (the only
      form Jenkins rows dispatch to) uses none of them.

    All ``*_divorg`` / ``*_org`` rows exist only for the two stand-origin
    species (slash pine SPCD 111, loblolly pine SPCD 131), which have no
    STDORGCD-null row and so depend on Levels 1/1b to avoid collapsing to the
    Jenkins fallback (issue #123). The Level 1/1b joins are skipped when the
    trees frame lacks a ``STDORGCD`` column, and Levels 1/2 when it lacks
    ``DIVISION`` — degrading gracefully to the remaining tiers.
    """

    volib_spcd: pl.DataFrame
    volib_jen: pl.DataFrame
    volib_div: pl.DataFrame
    volib_divorg: pl.DataFrame
    volib_org: pl.DataFrame
    volbk_spcd: pl.DataFrame
    volbk_jen: pl.DataFrame
    volbk_div: pl.DataFrame
    volbk_divorg: pl.DataFrame
    volbk_org: pl.DataFrame
    bark_bio_spcd: pl.DataFrame
    bark_bio_jen: pl.DataFrame
    bark_bio_div: pl.DataFrame
    bark_bio_divorg: pl.DataFrame
    bark_bio_org: pl.DataFrame
    branch_bio_spcd: pl.DataFrame
    branch_bio_jen: pl.DataFrame
    branch_bio_div: pl.DataFrame
    branch_bio_divorg: pl.DataFrame
    branch_bio_org: pl.DataFrame
    total_agb_spcd: pl.DataFrame
    total_agb_jen: pl.DataFrame
    total_agb_div: pl.DataFrame
    total_agb_divorg: pl.DataFrame
    total_agb_org: pl.DataFrame


# Common coefficient columns used by the vectorized path. Matches the inputs
# consumed by ``nsvb_biomass_expr`` (a1/c1 added for Model 3).
_VECTORIZED_COEF_COLS = ("model", "a", "a1", "b", "b1", "c", "c1")


def _valid_coef_filter() -> pl.Expr:
    """Filter expression dropping rows whose model needs a null coefficient.

    The downstream vectorized coalesce (:func:`nsvb_biomass_expr` via
    ``_join_and_eval_component``) picks each coefficient column independently
    across lookup tiers. That is only sound if, for the tier a row belongs to,
    every coefficient its model *reads* is non-null — otherwise a null required
    coefficient would be silently back-filled from a lower-precedence tier and
    corrupt the per-row math. Model 2/4 read ``b1``; Model 3 reads ``a1`` and
    ``c1``. Rows violating this are dropped from the lookup, so the whole SPCD
    falls through to the next precedence level (or Jenkins) rather than mixing
    coefficients. The current vendored CSVs have no such rows; this is a guard
    against future re-vendor drift.
    """
    return ~(
        (pl.col("model").is_in([2, 4]) & pl.col("b1").is_null())
        | ((pl.col("model") == 3) & (pl.col("a1").is_null() | pl.col("c1").is_null()))
    )


def build_species_level_lookup(table_spcd: pl.DataFrame) -> pl.DataFrame:
    """Prepare a ``*_spcd`` coefficient table for vectorized SPCD joins.

    Filters to the species-level rows (``DIVISION`` is null AND ``STDORGCD``
    is null — Level 3 of the NSVB precedence), selects the coefficient columns
    used by the vectorized biomass expression, and returns a DataFrame keyed
    on ``SPCD``. The more specific rows (Levels 1/1b/2, which match
    ``DIVISION`` and/or ``STDORGCD``) are handled by the ``build_*_lookup``
    siblings and coalesced ahead of this tier.

    Applies the shared defensive coefficient-null filter
    (:func:`_valid_coef_filter`), dropping rows whose model form reads a null
    coefficient so the SPCD falls through to the next tier rather than mixing
    coefficients in the downstream coalesce.

    Parameters
    ----------
    table_spcd : pl.DataFrame
        One of the 5 ``*_spcd`` coefficient tables from
        :class:`CoefficientTables`.

    Returns
    -------
    pl.DataFrame
        Columns ``(SPCD, model, a, a1, b, b1, c, c1)``. One row per SPCD that
        has a species-level entry in the source table.
    """
    return table_spcd.filter(
        pl.col("DIVISION").is_null()
        & pl.col("STDORGCD").is_null()
        & _valid_coef_filter()
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
            pl.lit(None, dtype=pl.Float64).alias("a1"),
            pl.col("b"),
            pl.lit(0.0, dtype=pl.Float64).alias("b1"),
            pl.col("c"),
            pl.lit(None, dtype=pl.Float64).alias("c1"),
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

    Levels 1 and 1b of the NSVB precedence (which also match ``STDORGCD``) are
    built separately by :func:`build_division_stdorg_lookup` and
    :func:`build_stdorg_lookup` and coalesced ahead of this tier.

    Applies the same defensive coefficient-null filter as
    :func:`build_species_level_lookup` (see :func:`_valid_coef_filter`).

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
        & _valid_coef_filter()
    ).select(["SPCD", "DIVISION", *_VECTORIZED_COEF_COLS])


def build_division_stdorg_lookup(table_spcd: pl.DataFrame) -> pl.DataFrame:
    """Prepare a ``*_spcd`` table for ``(SPCD, DIVISION, STDORGCD)`` joins
    (Level 1 of the NSVB lookup precedence — the most specific tier).

    Filters to rows where both ``DIVISION`` and ``STDORGCD`` are non-null:
    the ecodivision-specific, stand-origin-specific coefficients. In the
    vendored CSVs these rows exist only for the two species FIA fits
    separately by stand origin — slash pine (SPCD 111) and loblolly pine
    (SPCD 131) (GTR-WO-104 p. 8). The vectorized orchestrator joins this
    first (keyed on the ``(SPCD, DIVISION, STDORGCD)`` triple), then falls
    through to the ``(SPCD, STDORGCD)`` stand-origin lookup (Level 1b),
    ``(SPCD, DIVISION)`` (Level 2), species-level (Level 3), and Jenkins
    (Level 4).

    Requires the trees frame to carry both a ``DIVISION`` (from
    ``PLOTGEOM.ECOSUBCD``) and a ``STDORGCD`` (from ``COND``) column.

    Applies the same defensive coefficient-null filter as
    :func:`build_species_level_lookup`.

    Returns
    -------
    pl.DataFrame
        Columns ``(SPCD, DIVISION, STDORGCD, model, a, a1, b, b1, c, c1)``.
    """
    return table_spcd.filter(
        pl.col("DIVISION").is_not_null()
        & pl.col("STDORGCD").is_not_null()
        & _valid_coef_filter()
    ).select(["SPCD", "DIVISION", "STDORGCD", *_VECTORIZED_COEF_COLS])


def build_stdorg_lookup(table_spcd: pl.DataFrame) -> pl.DataFrame:
    """Prepare a ``*_spcd`` table for ``(SPCD, STDORGCD)`` joins (Level 1b of
    the NSVB lookup precedence — the stand-origin "no ecodivision noted" row).

    Filters to rows where ``STDORGCD`` is non-null AND ``DIVISION`` is null:
    the stand-origin-specific coefficients that apply when a tree's
    ecodivision is not explicitly listed for the species. Per GTR-WO-104
    p. 11, "if a species occurs in an ecodivision not explicitly listed, the
    entry having no ecodivision noted is used" — this is that entry, for the
    stand-origin species (slash/loblolly pine).

    This tier is essential: SPCD 111/131 have *no* STDORGCD-null row at all,
    so without it (and Level 1) they are dropped from every species-level and
    division lookup and collapse to the Jenkins fallback (issue #123). Keyed
    on ``(SPCD, STDORGCD)``; requires a ``STDORGCD`` column on the trees frame.

    Applies the same defensive coefficient-null filter as
    :func:`build_species_level_lookup`.

    Returns
    -------
    pl.DataFrame
        Columns ``(SPCD, STDORGCD, model, a, a1, b, b1, c, c1)``.
    """
    return table_spcd.filter(
        pl.col("DIVISION").is_null()
        & pl.col("STDORGCD").is_not_null()
        & _valid_coef_filter()
    ).select(["SPCD", "STDORGCD", *_VECTORIZED_COEF_COLS])


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
        volib_divorg=build_division_stdorg_lookup(raw.volib_spcd),
        volib_org=build_stdorg_lookup(raw.volib_spcd),
        volbk_spcd=build_species_level_lookup(raw.volbk_spcd),
        volbk_jen=build_jenkins_lookup(raw.volbk_jenkins),
        volbk_div=build_division_lookup(raw.volbk_spcd),
        volbk_divorg=build_division_stdorg_lookup(raw.volbk_spcd),
        volbk_org=build_stdorg_lookup(raw.volbk_spcd),
        bark_bio_spcd=build_species_level_lookup(raw.bark_biomass_spcd),
        bark_bio_jen=build_jenkins_lookup(raw.bark_biomass_jenkins),
        bark_bio_div=build_division_lookup(raw.bark_biomass_spcd),
        bark_bio_divorg=build_division_stdorg_lookup(raw.bark_biomass_spcd),
        bark_bio_org=build_stdorg_lookup(raw.bark_biomass_spcd),
        branch_bio_spcd=build_species_level_lookup(raw.branch_biomass_spcd),
        branch_bio_jen=build_jenkins_lookup(raw.branch_biomass_jenkins),
        branch_bio_div=build_division_lookup(raw.branch_biomass_spcd),
        branch_bio_divorg=build_division_stdorg_lookup(raw.branch_biomass_spcd),
        branch_bio_org=build_stdorg_lookup(raw.branch_biomass_spcd),
        total_agb_spcd=build_species_level_lookup(raw.total_biomass_spcd),
        total_agb_jen=build_jenkins_lookup(raw.total_biomass_jenkins),
        total_agb_div=build_division_lookup(raw.total_biomass_spcd),
        total_agb_divorg=build_division_stdorg_lookup(raw.total_biomass_spcd),
        total_agb_org=build_stdorg_lookup(raw.total_biomass_spcd),
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
        # The bark/branch biomass spcd tables carry no Model 3 rows and so ship
        # without the a1/c1 columns. Add them as null so every spcd table shares
        # the uniform schema the vectorized lookup builders select.
        if not key.endswith("_jenkins"):
            for col in ("a1", "c1"):
                if col not in df.columns:
                    df = df.with_columns(pl.lit(None, dtype=pl.Float64).alias(col))
        loaded[key] = df
    return CoefficientTables(**loaded)
