"""
Species-specific carbon fraction lookups for NSVB live and dead trees.

Implements the species-level carbon fraction lookup from Tables S10a (live) and
S10b (dead, by hardwood/softwood × DECAYCD) of GTR-WO-104. Live tree carbon
fractions average ~47.4% across species but vary from roughly 40% to 55%
depending on species; the flat 0.47 multiplier used by the existing pyFIA
``biomass()`` estimator is replaced here with a species-specific lookup.

S10b (dead trees) is loaded but unused in Phase 1 — Phase 1 is live tree only.
The dead tree loader is in place to support Phase 2 standing dead.
"""

from __future__ import annotations

import functools
import logging
from importlib import resources

import polars as pl

logger = logging.getLogger(__name__)

# National mean live carbon fraction (S10a population mean per spec section 3.1.2).
# Used as a fallback when an SPCD has no entry in S10a.
DEFAULT_LIVE_CARBON_FRACTION = 0.4716

# Track which unknown SPCDs we've already warned about, to avoid log spam when
# processing 1M-tree state inventories.
_warned_unknown_spcds: set[int] = set()


@functools.lru_cache(maxsize=1)
def load_carbon_fractions_live() -> dict[int, float]:
    """Load Table S10a live tree carbon fractions as a SPCD-keyed dict.

    The vendored CSV stores values as percent (e.g., ``48.04`` for 48.04%);
    this loader divides by 100 so callers receive a fraction in the
    [0.40, 0.55] range. Cached at process level via ``@lru_cache``.

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


def get_carbon_fraction_live(
    spcd: int, fallback: float = DEFAULT_LIVE_CARBON_FRACTION
) -> float:
    """Look up the live carbon fraction for a given species code.

    Parameters
    ----------
    spcd : int
        FIA species code from ``TREE.SPCD``.
    fallback : float, default DEFAULT_LIVE_CARBON_FRACTION
        Carbon fraction to return if ``spcd`` has no row in S10a. Defaults
        to the S10a population mean (~0.4716). The first time a given
        unknown SPCD is encountered, a warning is logged.

    Returns
    -------
    float
        Live carbon fraction in decimal form (e.g., 0.4804 for SPCD=10).
    """
    table = load_carbon_fractions_live()
    if spcd in table:
        return table[spcd]
    if spcd not in _warned_unknown_spcds:
        _warned_unknown_spcds.add(spcd)
        logger.warning(
            "SPCD=%d not found in S10a live carbon fractions; falling back to "
            "national mean (%.4f). Future occurrences for this SPCD will not log.",
            spcd,
            fallback,
        )
    return fallback


@functools.lru_cache(maxsize=1)
def load_carbon_fractions_dead() -> dict[tuple[str, int], float]:
    """Load Table S10b dead tree carbon fractions, keyed by (hw_sw, DECAYCD).

    Loaded but unused in Phase 1 (live tree only). Phase 2 standing dead
    will consume this table.

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


def get_carbon_fraction_dead(hw_sw: str, decaycd: int) -> float:
    """Look up the dead tree carbon fraction by hardwood/softwood and decay class.

    Phase 2 entry point — not used by Phase 1.

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
