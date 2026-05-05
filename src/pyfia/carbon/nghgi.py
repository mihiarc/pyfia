"""
NGHGI report reproduction — pool aggregation matching EPA Chapter 6 LULUCF.

The EPA NGHGI report (Inventory of U.S. GHG Emissions and Sinks) reports forest
ecosystem carbon under six pools whose decomposition does NOT line up 1:1 with
pyFIA's pool estimators:

    EPA pool                    pyFIA module(s)
    --------                    ---------------
    Aboveground Biomass         live_tree(pool='ag') + understory(pool='ag')
    Belowground Biomass         live_tree(pool='bg') + understory(pool='bg')
    Dead Wood                   standing_dead(pool='total') + downed_dead
    Litter                      litter
    Soil (Mineral)              soil_organic        [Domke 2017 mineral soil model]
    Soil (Organic)              not reproducible    [Histosols, IPCC defaults]

Use :func:`compile_state_stocks` to produce a state-level rollup matching the
EPA Table 6-10 row structure, and :func:`compile_conus_stocks` to aggregate
across multiple state databases.

Forest-remaining-forest only.  This is the Phase 1 reproduction target for
the CTrees x Schmidt grant; see project memory for scope.

Units
-----
pyFIA pool estimators return short tons.  EPA Table 6-10 reports MMT C
(million metric tons of carbon).  Conversion: 1 short ton = 0.907185 metric
tons.

References
----------
- USEPA (2024). Chapter 6 LULUCF, Tables 6-9 and 6-10.
- Westfall et al. (2023) GTR-WO-104 (NSVB).
- Domke et al. (2013, 2016, 2017) for downed dead, litter, soil.
"""

from __future__ import annotations

from importlib import resources
from typing import Iterable

import polars as pl

from ..core import FIA

SHORT_TONS_PER_METRIC_TON = 1.0 / 0.907185
METRIC_TONS_PER_SHORT_TON = 0.907185
MMT_C_PER_SHORT_TON = METRIC_TONS_PER_SHORT_TON / 1_000_000


EPA_POOLS = (
    "AGB",
    "BGB",
    "DEAD_WOOD",
    "LITTER",
    "SOIL_MINERAL",
    "SOIL_ORGANIC",
    "FOREST_ECOSYSTEM",
)


def _short_tons_to_mmt_c(short_tons: float) -> float:
    """Convert short tons of carbon to million metric tons (MMT C)."""
    return short_tons * MMT_C_PER_SHORT_TON


def compile_state_stocks(
    db: str | FIA,
    *,
    evalid: int | None = None,
    state_label: str | None = None,
    mode: str = "fiadb",
) -> pl.DataFrame:
    """
    Compute EPA-pool-aggregated carbon stocks for one state's evaluation.

    Runs six carbon pool estimators against the supplied EVALID, then
    re-aggregates them into the six EPA NGHGI pools (plus Forest Ecosystem
    total) used in EPA Chapter 6 Table 6-10.

    Parameters
    ----------
    db : str or FIA
        Path or open FIA instance.  If a path is given, the connection is
        opened, used, and closed by this function.
    evalid : int, optional
        EVALID to clip to.  If None, the caller is expected to have already
        clipped the FIA instance.
    state_label : str, optional
        Free-form label propagated into the output (e.g. "Georgia",
        state postal code, FIPS).  Useful when stacking multiple states.
    mode : {'fiadb', 'nsvb'}, default 'fiadb'
        Tree-level carbon source.  ``'fiadb'`` reads FIADB-stored
        ``TREE.CARBON_AG`` and ``TREE.CARBON_BG`` via
        ``pyfia.estimation.estimators.carbon_pool`` — this is the path
        EPA's report ultimately consumes and is the correct choice for
        report reproduction (especially for pre-2023 EVALIDs which were
        compiled before the NSVB framework transition). ``'nsvb'`` uses
        pyFIA's NSVB-recomputation pool estimators
        (``pyfia.carbon.live_tree`` / ``standing_dead``); use this only
        for methodology comparison or post-2023 inventories.

    Returns
    -------
    pl.DataFrame
        One row per EPA pool (AGB, BGB, DEAD_WOOD, LITTER, SOIL_MINERAL,
        FOREST_ECOSYSTEM).  Columns:

        - STATE: state_label (if provided)
        - EPA_POOL: EPA pool name
        - STOCK_SHORT_TONS: population total in short tons of C
        - STOCK_MMT_C: same total converted to million metric tons C
        - STOCK_PER_ACRE_ST: short tons per acre
        - N_PLOTS: plot count from the underlying estimator(s)
        - SOURCE: which estimator(s) contributed

        SOIL_ORGANIC is NOT included — it is not reproducible from FIADB
        attributes alone (EPA uses IPCC defaults applied to Histosol plots).
        Callers comparing to EPA totals must add it separately from
        ``nghgi_2024_table_6_10.csv``.

    Notes
    -----
    Forest-remaining-forest only is approximated via ``land_type='forest'``
    in each estimator.  This includes both forest-remaining-forest and
    land-converted-to-forest in EPA's CRT taxonomy.  EPA Section 6.2
    excludes the latter; reproducing that split is Phase 2 work.
    """
    from ..estimation.estimators.carbon_pools import carbon_pool
    from .downed_dead import downed_dead
    from .litter import litter
    from .soil_organic import soil_organic
    from .understory import understory

    mode = mode.lower()
    if mode not in ("fiadb", "nsvb"):
        raise ValueError(
            f"mode must be 'fiadb' or 'nsvb', got {mode!r}"
        )

    if isinstance(db, str):
        owns = True
        fia = FIA(db)
        fia.__enter__()
    else:
        owns = False
        fia = db

    try:
        if evalid is not None:
            fia.clip_by_evalid(evalid)

        common = dict(land_type="forest", totals=True, variance=False)

        if mode == "fiadb":
            live_ag = carbon_pool(fia, pool="ag", tree_type="live", **common)
            live_bg = carbon_pool(fia, pool="bg", tree_type="live", **common)
            std_dead = carbon_pool(fia, pool="total", tree_type="dead", **common)
            tree_source = "FIADB CARBON_AG/BG via carbon_pool"
        else:  # mode == "nsvb"
            from .live_tree import live_tree
            from .standing_dead import standing_dead
            live_ag = live_tree(fia, pool="ag", **common)
            live_bg = live_tree(fia, pool="bg", **common)
            std_dead = standing_dead(fia, pool="total", **common)
            tree_source = "NSVB recomputation"

        und_ag = understory(fia, pool="ag", **common)
        und_bg = understory(fia, pool="bg", **common)
        dwn_dead = downed_dead(fia, **common)
        lit = litter(fia, **common)
        soil = soil_organic(fia, **common)

        def _stock(df: pl.DataFrame) -> tuple[float, float, int]:
            if df is None or len(df) == 0:
                return 0.0, 0.0, 0
            total = float(df["CARBON_TOTAL"][0]) if "CARBON_TOTAL" in df.columns else 0.0
            acre = float(df["CARBON_ACRE"][0]) if "CARBON_ACRE" in df.columns else 0.0
            n = int(df["N_PLOTS"][0]) if "N_PLOTS" in df.columns else 0
            return total, acre, n

        lt_ag_t, lt_ag_a, n_lt = _stock(live_ag)
        lt_bg_t, lt_bg_a, _ = _stock(live_bg)
        sd_t, sd_a, n_sd = _stock(std_dead)
        u_ag_t, u_ag_a, n_u = _stock(und_ag)
        u_bg_t, u_bg_a, _ = _stock(und_bg)
        dd_t, dd_a, n_dd = _stock(dwn_dead)
        li_t, li_a, n_li = _stock(lit)
        sm_t, sm_a, n_sm = _stock(soil)

        agb_t = lt_ag_t + u_ag_t
        bgb_t = lt_bg_t + u_bg_t
        dw_t = sd_t + dd_t
        eco_t = agb_t + bgb_t + dw_t + li_t + sm_t

        agb_a = lt_ag_a + u_ag_a
        bgb_a = lt_bg_a + u_bg_a
        dw_a = sd_a + dd_a
        eco_a = agb_a + bgb_a + dw_a + li_a + sm_a

        rows = [
            ("AGB", agb_t, agb_a, max(n_lt, n_u),
             f"live[{tree_source}](ag) + understory(ag)"),
            ("BGB", bgb_t, bgb_a, max(n_lt, n_u),
             f"live[{tree_source}](bg) + understory(bg)"),
            ("DEAD_WOOD", dw_t, dw_a, max(n_sd, n_dd),
             f"standing_dead[{tree_source}](total) + downed_dead"),
            ("LITTER", li_t, li_a, n_li, "litter"),
            ("SOIL_MINERAL", sm_t, sm_a, n_sm,
             "soil_organic [Domke 2017 mineral]"),
            ("FOREST_ECOSYSTEM", eco_t, eco_a, max(n_lt, n_sm),
             "AGB+BGB+DEAD_WOOD+LITTER+SOIL_MINERAL"),
        ]

        df = pl.DataFrame(
            {
                "EPA_POOL": [r[0] for r in rows],
                "STOCK_SHORT_TONS": [r[1] for r in rows],
                "STOCK_MMT_C": [_short_tons_to_mmt_c(r[1]) for r in rows],
                "STOCK_PER_ACRE_ST": [r[2] for r in rows],
                "N_PLOTS": [r[3] for r in rows],
                "SOURCE": [r[4] for r in rows],
            }
        )

        if state_label is not None:
            df = df.with_columns(pl.lit(state_label).alias("STATE"))
            df = df.select(["STATE", *[c for c in df.columns if c != "STATE"]])

        return df

    finally:
        if owns:
            fia.__exit__(None, None, None)


def compile_conus_stocks(
    state_evalids: Iterable[tuple[str, str, int]],
) -> pl.DataFrame:
    """
    Compile per-state EPA-pool stocks across multiple state databases.

    Iterates over state databases, runs :func:`compile_state_stocks` on
    each, and concatenates results.  Caller is responsible for picking the
    correct EVALID per state (e.g. most recent annual VOL evaluation
    covering the report's reference year).

    Parameters
    ----------
    state_evalids : iterable of (state_label, db_path, evalid)
        One tuple per state.  ``state_label`` is propagated into the
        ``STATE`` column.

    Returns
    -------
    pl.DataFrame
        Long-format frame with columns ``STATE``, ``EPA_POOL``,
        ``STOCK_SHORT_TONS``, ``STOCK_MMT_C``, ``STOCK_PER_ACRE_ST``,
        ``N_PLOTS``, ``SOURCE``.  Use ``group_by('EPA_POOL').agg(...)``
        downstream to roll up to CONUS totals.
    """
    frames: list[pl.DataFrame] = []
    for state_label, db_path, evalid in state_evalids:
        df = compile_state_stocks(db_path, evalid=evalid, state_label=state_label)
        frames.append(df)
    if not frames:
        return pl.DataFrame()
    return pl.concat(frames, how="vertical_relaxed")


def load_published_targets(year: int = 2022) -> pl.DataFrame:
    """
    Load EPA Chapter 6 Table 6-10 published carbon stocks for a given year.

    Returns the published per-pool MMT C target values for comparison with
    pyFIA-computed totals.  Available years: 1990, 2005, 2019, 2020, 2021,
    2022, 2023.

    Parameters
    ----------
    year : int
        Reporting year matching a column in EPA Table 6-10.

    Returns
    -------
    pl.DataFrame
        Columns: EPA_POOL, YEAR, STOCK_MMT_C, NOTE.
    """
    with resources.files("pyfia.carbon.data").joinpath(
        "nghgi_2024_table_6_10.csv"
    ).open("r") as fh:
        df = pl.read_csv(fh)
    df = df.rename({"epa_pool": "EPA_POOL", "year": "YEAR",
                    "stock_mmt_c": "STOCK_MMT_C", "note": "NOTE"})
    return df.filter(pl.col("YEAR") == year)


def compare_to_published(
    pyfia_stocks: pl.DataFrame,
    *,
    year: int = 2022,
) -> pl.DataFrame:
    """
    Side-by-side compare a pyFIA stocks rollup to the EPA published targets.

    Parameters
    ----------
    pyfia_stocks : pl.DataFrame
        Output of :func:`compile_state_stocks` (single state) or a CONUS
        rollup with one row per ``EPA_POOL`` and a ``STOCK_MMT_C`` column.
    year : int
        EPA reporting year to compare against.

    Returns
    -------
    pl.DataFrame
        Columns: EPA_POOL, PYFIA_MMT_C, EPA_MMT_C, ABS_DIFF_MMT_C,
        PCT_DIFF.  Useful for quick eyeballing of where the rollup
        agrees vs diverges.
    """
    if "STATE" in pyfia_stocks.columns:
        rollup = (
            pyfia_stocks.group_by("EPA_POOL")
            .agg(pl.col("STOCK_MMT_C").sum())
            .rename({"STOCK_MMT_C": "PYFIA_MMT_C"})
        )
    else:
        rollup = pyfia_stocks.select(["EPA_POOL", "STOCK_MMT_C"]).rename(
            {"STOCK_MMT_C": "PYFIA_MMT_C"}
        )

    targets = load_published_targets(year=year).select(["EPA_POOL", "STOCK_MMT_C"]).rename(
        {"STOCK_MMT_C": "EPA_MMT_C"}
    )

    merged = rollup.join(targets, on="EPA_POOL", how="full", coalesce=True)
    merged = merged.with_columns(
        (pl.col("PYFIA_MMT_C") - pl.col("EPA_MMT_C")).alias("ABS_DIFF_MMT_C"),
        (
            100.0
            * (pl.col("PYFIA_MMT_C") - pl.col("EPA_MMT_C"))
            / pl.col("EPA_MMT_C")
        ).alias("PCT_DIFF"),
    )
    return merged.sort("EPA_POOL")
