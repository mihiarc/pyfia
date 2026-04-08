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
them. Phase 1 ignores DIVISION-specific rows entirely and uses only the
species-level (DIVISION-null) row, because pyFIA does not yet have a
``PLOT.ECOSUBCD → Bailey ecoprovince division`` mapping. If the validation gate
in Phase 1 measures > 1% per-tree disagreement with FIADB ``CARBON_AG``, a minimal
ecoprovince mapping will be added in Phase 1.5.
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


@functools.lru_cache(maxsize=1)
def load_nsvb_coefficients() -> CoefficientTables:
    """Load all NSVB coefficient tables from the vendored CSV bundle.

    Cached at process level via ``@lru_cache``: subsequent calls return the same
    ``CoefficientTables`` instance in O(1). Uses ``importlib.resources.files``
    so it works in dev installs, installed wheels, zip-imported wheels, and
    PyOxidizer-style frozen builds without ``__file__`` path tricks.
    """
    data_pkg = resources.files("pyfia.carbon.nsvb.data")
    loaded: dict[str, pl.DataFrame] = {}
    for key, filename in _CSV_FILES.items():
        with resources.as_file(data_pkg / filename) as path:
            df = pl.read_csv(path, infer_schema_length=10_000)
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
