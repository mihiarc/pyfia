"""
Species-specific carbon fraction lookups for NSVB live and dead trees.

Implements the species-level carbon fraction lookup from Tables S10a (live) and
S10b (dead, by hardwood/softwood × DECAYCD) of GTR-WO-104. Live tree carbon
fractions average ~47.4% across species but vary from roughly 40% to 55%
depending on species; the flat 0.47 multiplier used by the existing pyFIA
``biomass()`` estimator is replaced here with a species-specific lookup.

S10a (live) powers ``pyfia.carbon.live_tree``. S10b (dead) and the related
``REF_TREE_DECAY_PROP`` density / bark / branch loss proportions power
``pyfia.carbon.standing_dead`` (Phase 2). Both pipelines hit the same
hardwood/softwood × DECAYCD lookup, so the loaders are colocated here.
"""

from __future__ import annotations

import functools
import logging
from importlib import resources
from typing import Any

import polars as pl

logger = logging.getLogger(__name__)

# Track which unknown SPCDs we've already warned about, to avoid log spam when
# processing 1M-tree state inventories.
_warned_unknown_spcds: set[int] = set()


@functools.cache
def _compute_default_live_carbon_fraction() -> float:
    """Compute the S10a arithmetic-mean live carbon fraction.

    Backs the ``DEFAULT_LIVE_CARBON_FRACTION`` module attribute via PEP 562
    ``__getattr__``. Calculated lazily (on first access) so that importing
    this module does no file I/O, and cached so subsequent accesses are O(1).

    The reason this is computed rather than hardcoded: the S10a CSV is
    vendored under ``pyfia.carbon.nsvb.data``, and a future re-vendor could
    perturb the population mean. A hardcoded constant would silently drift;
    a lazy computation always reflects the current CSV.
    """
    table = load_carbon_fractions_live()
    return sum(table.values()) / len(table)


def __getattr__(name: str) -> Any:
    """PEP 562 lazy module attribute resolver.

    Enables ``from pyfia.carbon.nsvb.carbon_fractions import DEFAULT_LIVE_CARBON_FRACTION``
    to work as if the constant were defined at the top of the module, while
    actually computing the value lazily from the S10a table on first access.
    After the first access the value is cached in ``globals()`` so lookups
    become standard attribute access.
    """
    if name == "DEFAULT_LIVE_CARBON_FRACTION":
        value = _compute_default_live_carbon_fraction()
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


@functools.lru_cache(maxsize=1)
def load_carbon_fractions_live() -> dict[int, float]:
    """Load Table S10a live tree carbon fractions as a SPCD-keyed dict.

    The vendored CSV stores values as percent (e.g., ``48.04`` for 48.04%);
    this loader divides by 100 so callers receive a fraction in the
    [0.40, 0.55] range. Cached at process level via ``@lru_cache``.

    This is the scalar reference loader used by ``get_carbon_fraction_live``
    and by the PR 1 test oracle. The vectorized live-tree estimator uses
    :func:`load_carbon_fractions_live_df` instead, which returns a Polars
    DataFrame ready for joining to a trees frame on ``SPCD``.

    Returns
    -------
    dict[int, float]
        Map from FIA species code to live carbon fraction (decimal, not percent).
    """
    data_pkg = resources.files("pyfia.carbon.nsvb.data")
    with resources.as_file(data_pkg / "carbon_fraction_live.csv") as path:
        df = pl.read_csv(path)
    return {
        int(row["SPCD"]): float(row["fia_wood_c"]) / 100.0
        for row in df.iter_rows(named=True)
    }


@functools.lru_cache(maxsize=1)
def load_carbon_fractions_live_df() -> pl.DataFrame:
    """Load Table S10a live tree carbon fractions as a join-ready DataFrame.

    Returns a 2-column DataFrame ``(SPCD Int64, CARBON_FRAC_LIVE Float64)``
    suitable for a left join against a trees LazyFrame in the vectorized
    live-tree estimator. The ``CARBON_FRAC_LIVE`` column is already
    converted from percent to decimal (e.g., 48.04 → 0.4804), matching
    the units returned by :func:`load_carbon_fractions_live`.

    Trees whose SPCD has no row in S10a should be filled with
    :data:`DEFAULT_LIVE_CARBON_FRACTION` after the join using
    ``pl.col("CARBON_FRAC_LIVE").fill_null(DEFAULT_LIVE_CARBON_FRACTION)``.

    Cached at process level via ``@lru_cache``.

    Returns
    -------
    pl.DataFrame
        Columns ``(SPCD, CARBON_FRAC_LIVE)`` with one row per FIA species
        code present in S10a (~2,676 rows).
    """
    data_pkg = resources.files("pyfia.carbon.nsvb.data")
    with resources.as_file(data_pkg / "carbon_fraction_live.csv") as path:
        df = pl.read_csv(path, schema_overrides={"SPCD": pl.Int64})
    return df.select(
        [
            pl.col("SPCD"),
            (pl.col("fia_wood_c").cast(pl.Float64) / 100.0).alias("CARBON_FRAC_LIVE"),
        ]
    )


def get_carbon_fraction_live(spcd: int, fallback: float | None = None) -> float:
    """Look up the live carbon fraction for a given species code.

    Parameters
    ----------
    spcd : int
        FIA species code from ``TREE.SPCD``.
    fallback : float, optional
        Carbon fraction to return if ``spcd`` has no row in S10a. Defaults
        to the S10a arithmetic mean (computed lazily from the vendored table,
        approximately 0.4741). The first time a given unknown SPCD is
        encountered, a warning is logged.

    Returns
    -------
    float
        Live carbon fraction in decimal form (e.g., 0.4804 for SPCD=10).
    """
    if fallback is None:
        fallback = _compute_default_live_carbon_fraction()
    table = load_carbon_fractions_live()
    if spcd in table:
        return table[spcd]
    if spcd not in _warned_unknown_spcds:
        _warned_unknown_spcds.add(spcd)
        logger.warning(
            "SPCD=%d not found in S10a live carbon fractions; falling back to "
            "S10a mean (%.4f). Future occurrences for this SPCD will not log.",
            spcd,
            fallback,
        )
    return fallback


@functools.lru_cache(maxsize=1)
def load_carbon_fractions_dead() -> dict[tuple[str, int], float]:
    """Load Table S10b dead tree carbon fractions, keyed by (hw_sw, DECAYCD).

    Returns
    -------
    dict[tuple[str, int], float]
        Map from ``("hardwood" | "softwood", decay_class_1_to_5)`` to carbon
        fraction (decimal). Source CSV stores values as percent; this loader
        divides by 100.
    """
    data_pkg = resources.files("pyfia.carbon.nsvb.data")
    with resources.as_file(data_pkg / "carbon_fraction_dead.csv") as path:
        df = pl.read_csv(path)
    out: dict[tuple[str, int], float] = {}
    for row in df.iter_rows(named=True):
        hw_sw = row["S/H"].strip().lower()
        decay = int(row["Decay code"])
        frac = float(row["C fraction"]) / 100.0
        out[(hw_sw, decay)] = frac
    return out


@functools.lru_cache(maxsize=1)
def load_carbon_fractions_dead_df() -> pl.DataFrame:
    """Load Table S10b dead tree carbon fractions as a join-ready DataFrame.

    Returns a 3-column DataFrame ``(hw_sw Utf8, DECAYCD Int64,
    CARBON_FRAC_DEAD Float64)`` suitable for a left join against a trees
    LazyFrame in the vectorized standing-dead estimator. The
    ``CARBON_FRAC_DEAD`` column is converted from percent to decimal
    (e.g., 47.0 → 0.4700) at load time, matching the units returned by
    :func:`load_carbon_fractions_dead`.

    Cached at process level via ``@lru_cache``.

    Returns
    -------
    pl.DataFrame
        Columns ``(hw_sw, DECAYCD, CARBON_FRAC_DEAD)`` with one row per
        ``(hardwood|softwood) × decay class 1-5`` combination (10 rows total).
    """
    data_pkg = resources.files("pyfia.carbon.nsvb.data")
    with resources.as_file(data_pkg / "carbon_fraction_dead.csv") as path:
        df = pl.read_csv(path)
    return df.select(
        [
            pl.col("S/H").str.strip_chars().str.to_lowercase().alias("hw_sw"),
            pl.col("Decay code").cast(pl.Int64).alias("DECAYCD"),
            (pl.col("C fraction").cast(pl.Float64) / 100.0).alias("CARBON_FRAC_DEAD"),
        ]
    )


def get_carbon_fraction_dead(hw_sw: str, decaycd: int) -> float:
    """Look up the dead tree carbon fraction by hardwood/softwood and decay class.

    Parameters
    ----------
    hw_sw : str
        ``"hardwood"`` or ``"softwood"``.
    decaycd : int
        FIA decay class (1-5).

    Returns
    -------
    float
        Dead carbon fraction in decimal form.

    Raises
    ------
    KeyError
        If the (hw_sw, decaycd) pair is not in S10b.
    """
    table = load_carbon_fractions_dead()
    return table[(hw_sw.lower(), decaycd)]


@functools.lru_cache(maxsize=1)
def load_dead_decay_proportions_df() -> pl.DataFrame:
    """Load the FIADB ``REF_TREE_DECAY_PROP`` density / loss proportions.

    The vendored ``dead_decay_proportions.csv`` file mirrors the canonical
    FIADB ``REF_TREE_DECAY_PROP`` table (FIADB User Guide v9.1 §11.36) and
    matches the consolidated NSVB hardwood/softwood × DECAYCD values published
    in GTR-WO-104 Table 1 (Westfall et al. 2023). It supplies the three
    multiplicative reduction factors that ``pyfia.carbon.standing_dead``
    applies to the gross NSVB component biomass:

    - ``DENSITY_PROP`` — fraction of stem wood biomass remaining after decay
    - ``BARK_LOSS_PROP`` — fraction of stem bark biomass remaining after decay
      (despite the ``LOSS`` suffix in the FIADB column name, the value is the
      *remaining* proportion, not the *lost* proportion — this matches the
      FIADB User Guide §11.36 description and the verbatim Appendix K formula
      ``BARK_LOSS_PROP * Bark``)
    - ``BRANCH_LOSS_PROP`` — fraction of branch biomass remaining after decay

    Returns
    -------
    pl.DataFrame
        Columns ``(hw_sw, DECAYCD, DENSITY_PROP, BARK_LOSS_PROP,
        BRANCH_LOSS_PROP)`` with one row per ``(hardwood|softwood) × decay
        class 1-5`` combination (10 rows total). Ready for a left join on
        ``["hw_sw", "DECAYCD"]``.

    Notes
    -----
    The five values per row are constants — they do not vary by species
    within the hardwood/softwood class. This is the FIADB-canonical
    parameterization; finer per-species DRFs from Harmon et al. (2011)
    Table 7 are not used by FIADB and are not vendored here.
    """
    data_pkg = resources.files("pyfia.carbon.nsvb.data")
    with resources.as_file(data_pkg / "dead_decay_proportions.csv") as path:
        df = pl.read_csv(
            path,
            schema_overrides={
                "hw_sw": pl.Utf8,
                "DECAYCD": pl.Int64,
                "DENSITY_PROP": pl.Float64,
                "BARK_LOSS_PROP": pl.Float64,
                "BRANCH_LOSS_PROP": pl.Float64,
            },
        )
    return df
