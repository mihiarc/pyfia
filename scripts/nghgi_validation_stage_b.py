"""
NGHGI Stage B — state-level flux validation against EPA Annex 3.13 Table A-208.

Annex 3.13 publishes state-level *flux* (annual carbon stock change) for
all forest pools combined, 2022 only — no state-level stocks table exists.
This script runs pyFIA's ``stock_change`` (condition-level: understory,
downed dead wood, litter, soil organic) per CONUS-48 state and compares
to the EPA published total per state.

Two important caveats:

1. pyFIA's ``stock_change`` covers **only condition-level pools** —
   live tree and standing dead flux (the dominant component, ~70-80%
   of national flux) are deferred to Stage D (tree-level GRM
   decomposition). The per-state residual ``EPA − pyFIA_condition``
   is the expected tree-level component that Stage D would fill in.

2. EPA Table A-208's "Stock Change" sign convention:
   negative = net carbon uptake (sink).  pyFIA's stock_change column
   ``CARBON_TOTAL`` is ΔC = C(t₂) − C(t₁), so positive = accumulation
   (also a sink).  We negate pyFIA values to align with EPA's flux
   convention (negative = sink).

Run:
    uv run python scripts/nghgi_validation_stage_b.py
"""

from __future__ import annotations

import argparse
import time
from importlib import resources
from pathlib import Path

import polars as pl

from pyfia import FIA
from pyfia.carbon.stock_change import stock_change

DATA_DIR = Path("/Users/cmihiar/Projects/data/fiadb")

CONUS_48 = [
    "AL", "AR", "AZ", "CA", "CO", "CT", "DE", "FL", "GA",
    "IA", "ID", "IL", "IN", "KS", "KY", "LA", "MA", "MD",
    "ME", "MI", "MN", "MO", "MS", "MT", "NC", "ND", "NE",
    "NH", "NJ", "NM", "NV", "NY", "OH", "OK", "OR", "PA",
    "RI", "SC", "SD", "TN", "TX", "UT", "VA", "VT", "WA",
    "WI", "WV", "WY",
]

MMT_C_PER_SHORT_TON = 0.907185 / 1_000_000


def _select_2022_evalid(db: FIA) -> int | None:
    evals = db.query(
        """
        SELECT EVALID, END_INVYR
        FROM POP_EVAL
        WHERE EVAL_DESCR LIKE '%CURRENT AREA, CURRENT VOLUME%'
        ORDER BY END_INVYR DESC
        """
    )
    if evals.is_empty():
        return None
    le = evals.filter(pl.col("END_INVYR") <= 2022)
    return int(le["EVALID"][0]) if not le.is_empty() else int(evals["EVALID"][0])


def run_state(state: str) -> dict | None:
    db_path = DATA_DIR / f"{state}.duckdb"
    if not db_path.exists():
        return None
    t0 = time.perf_counter()
    with FIA(str(db_path)) as db:
        evalid = _select_2022_evalid(db)
        if evalid is None:
            return None
        db.clip_by_evalid(evalid)
        try:
            sc = stock_change(db, pool="all")
        except Exception as e:
            return {
                "state": state, "evalid": evalid,
                "error": f"{type(e).__name__}: {e}",
                "elapsed_s": time.perf_counter() - t0,
            }

    # stock_change returns one row per pool with CARBON_TOTAL = ΔC short tons/yr.
    # Sum across the 4 condition-level pools.
    if sc is None or len(sc) == 0:
        return {
            "state": state, "evalid": evalid,
            "pyfia_condition_flux_mmt_c": 0.0,
            "n_remeasured_plots": 0,
            "elapsed_s": time.perf_counter() - t0,
        }

    total_st = (
        float(sc["CARBON_CHANGE_TOTAL"].sum())
        if "CARBON_CHANGE_TOTAL" in sc.columns else 0.0
    )
    n_plots = (
        int(sc["N_PLOTS"][0]) if "N_PLOTS" in sc.columns and len(sc) > 0 else 0
    )

    # Annual flux: pyFIA stock_change already annualizes by REMPER.
    # Convert short tons → MMT C and negate to match EPA flux sign convention
    # (EPA: negative = net uptake; pyFIA ΔC: positive = accumulation)
    pyfia_mmt = -total_st * MMT_C_PER_SHORT_TON

    return {
        "state": state,
        "evalid": evalid,
        "pyfia_condition_flux_mmt_c": pyfia_mmt,
        "n_remeasured_plots": n_plots,
        "elapsed_s": time.perf_counter() - t0,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--states", default=",".join(CONUS_48))
    args = parser.parse_args()
    states = [s.strip().upper() for s in args.states.split(",") if s.strip()]

    print(f"\n=== NGHGI Stage B — state-level flux validation ===")
    print(f"    pyFIA: condition-level stock_change (4 pools)")
    print(f"    EPA:   Table A-208 all-pools state flux, 2022")
    print(f"    Expected residual: ~70-80% (= tree-level flux, Stage D)\n")

    rows: list[dict] = []
    failures: list[tuple[str, str]] = []
    for i, st in enumerate(states, 1):
        r = run_state(st)
        if r is None:
            failures.append((st, "no DB or no EVALID"))
            continue
        if "error" in r:
            failures.append((st, r["error"]))
            print(f"  [{i:>2}/{len(states)}] {st}: FAILED — {r['error']}")
            continue
        rows.append(r)
        print(
            f"  [{i:>2}/{len(states)}] {st}: EVALID={r['evalid']} "
            f"pyFIA_cond={r['pyfia_condition_flux_mmt_c']:+.2f} MMT C/yr "
            f"({r['n_remeasured_plots']} plots, {r['elapsed_s']:.1f}s)"
        )

    if not rows:
        print("\nNo successful runs. Failures:")
        for st, err in failures:
            print(f"  {st}: {err}")
        return 1

    pyfia_df = pl.DataFrame(rows).rename({"state": "STATE"})

    # Load EPA targets
    with resources.files("pyfia.carbon.data").joinpath(
        "nghgi_2024_table_a_208_state_flux_2022.csv"
    ).open("r") as fh:
        epa_df = pl.read_csv(fh)
    epa_df = epa_df.rename({
        "state": "STATE",
        "flux_mmt_c_2022": "EPA_FLUX_MMT_C",
        "lower_bound_mmt_c": "EPA_LOWER",
        "upper_bound_mmt_c": "EPA_UPPER",
    })

    # Join on state postal code
    cmp = pyfia_df.join(epa_df, on="STATE", how="left").select([
        "STATE",
        "evalid",
        "n_remeasured_plots",
        "pyfia_condition_flux_mmt_c",
        "EPA_FLUX_MMT_C",
        "EPA_LOWER",
        "EPA_UPPER",
    ])

    cmp = cmp.with_columns(
        (pl.col("EPA_FLUX_MMT_C") - pl.col("pyfia_condition_flux_mmt_c"))
        .alias("RESIDUAL_TREE_LEVEL_MMT_C")
    )

    # Reorder columns and rename for display
    out = cmp.rename({
        "evalid": "EVALID",
        "n_remeasured_plots": "N_REMEAS",
        "pyfia_condition_flux_mmt_c": "PYFIA_COND",
    }).select([
        "STATE", "EVALID", "N_REMEAS",
        "PYFIA_COND", "EPA_FLUX_MMT_C", "EPA_LOWER", "EPA_UPPER",
        "RESIDUAL_TREE_LEVEL_MMT_C",
    ]).sort("EPA_FLUX_MMT_C")

    print(f"\n=== Per-state flux comparison (2022, MMT C/yr) ===")
    print("    PYFIA_COND = pyFIA condition-level (4 pools) flux")
    print("    EPA_FLUX = EPA Table A-208 all-pools flux")
    print("    RESIDUAL = EPA - pyFIA_cond = expected tree-level flux")
    print("    Sign convention: negative = net uptake (sink)\n")
    with pl.Config(tbl_rows=60, tbl_cols=10, fmt_str_lengths=20, tbl_width_chars=160):
        print(out)

    # CONUS rollup
    print("\n=== CONUS-48 rollup ===")
    py_total = float(out["PYFIA_COND"].sum())
    epa_total = float(out["EPA_FLUX_MMT_C"].sum())
    res_total = float(out["RESIDUAL_TREE_LEVEL_MMT_C"].sum())
    print(f"  pyFIA condition flux total:  {py_total:+.1f} MMT C/yr")
    print(f"  EPA all-pools flux total:    {epa_total:+.1f} MMT C/yr")
    print(f"  Residual (= tree-level):     {res_total:+.1f} MMT C/yr")
    pct_cond = 100.0 * py_total / epa_total if epa_total else float("nan")
    pct_tree = 100.0 * res_total / epa_total if epa_total else float("nan")
    print(f"  Condition pools account for: {pct_cond:.1f}% of EPA total")
    print(f"  Tree pools account for:      {pct_tree:.1f}% of EPA total (Stage D target)")

    print("\n  Note: EPA Table A-208 total reported in Annex 3.13: -189.3 MMT C/yr")
    print(f"  Our CONUS-48 EPA sum:        {epa_total:+.1f} MMT C/yr")
    print(f"  (delta from -189.3 reflects HI/AK/territories in EPA total)\n")

    if failures:
        print(f"Failures ({len(failures)}):")
        for st, err in failures:
            print(f"  {st}: {err}")

    # Within-uncertainty check for condition-level component
    in_band = out.filter(
        (pl.col("PYFIA_COND") >= pl.col("EPA_LOWER"))
        & (pl.col("PYFIA_COND") <= pl.col("EPA_UPPER"))
    )
    print(f"\nStates where pyFIA condition flux falls within EPA all-pools "
          f"uncertainty band: {len(in_band)}/{len(out)}")
    print("(expected to be low — pyFIA condition flux is only part of EPA's "
          "all-pools flux; it would only fall in the band when tree-level "
          "flux is small or EPA uncertainty is huge)")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
