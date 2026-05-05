"""
Dead Wood diagnostic — isolate whether the +38% over-count vs EPA Chapter 6
comes from standing dead, downed dead, or both.

Compares three estimates per state, all routed through pyFIA's identical
stratification pipeline (EVALID, land_type='forest', BaseEstimator path) so
the only variable is the per-tree/per-condition carbon source:

  A) `carbon.standing_dead(pool='total')` — NSVB recomputation per tree
     (Westfall 2023) with REF_TREE_DECAY_PROP reductions and Appendix K
     broken-top corrections. AG via NSVB; BG via bridge to TREE.CARBON_BG.

  B) `estimation.carbon_pool(pool='total', tree_type='dead')` — uses
     FIADB-stored TREE.CARBON_AG + TREE.CARBON_BG where STATUSCD=2.
     This is the path EPA's report ultimately consumes.

  C) `carbon.downed_dead` — uses pre-computed COND.CARBON_DOWN_DEAD
     (Domke 2013 model). Shared between pyFIA and EPA, no recomputation.

If A >> B, the standing-dead NSVB recomputation is the over-count source.
If A ≈ B, the issue is elsewhere (downed_dead or pool definition).

Run:
    uv run python scripts/nghgi_dead_wood_diagnostic.py
    uv run python scripts/nghgi_dead_wood_diagnostic.py --states GA,OR,TX,WA
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path

import polars as pl

from pyfia import FIA
from pyfia.carbon.downed_dead import downed_dead
from pyfia.carbon.standing_dead import standing_dead
from pyfia.estimation.estimators.carbon_pools import carbon_pool

DATA_DIR = Path("/Users/cmihiar/Projects/data/fiadb")
DEFAULT_STATES = ["GA", "OR", "TX", "WA"]

MMT_C_PER_SHORT_TON = 0.907185 / 1_000_000


def _to_mmt(short_tons: float) -> float:
    return short_tons * MMT_C_PER_SHORT_TON


def _select_2022_evalid(db: FIA) -> int:
    evals = db.query(
        """
        SELECT EVALID, END_INVYR
        FROM POP_EVAL
        WHERE EVAL_DESCR LIKE '%CURRENT AREA, CURRENT VOLUME%'
        ORDER BY END_INVYR DESC
        """
    )
    match = evals.filter(pl.col("END_INVYR") == 2022)
    return int(match["EVALID"][0]) if not match.is_empty() else int(evals["EVALID"][0])


def diagnose_state(state: str) -> dict:
    db_path = DATA_DIR / f"{state}.duckdb"
    t0 = time.perf_counter()

    with FIA(str(db_path)) as db:
        evalid = _select_2022_evalid(db)
        db.clip_by_evalid(evalid)

        # A) NSVB-recomputed standing dead (pyFIA's new path)
        sd_nsvb = standing_dead(db, pool="total", land_type="forest", totals=True)
        a_total_st = float(sd_nsvb["CARBON_TOTAL"][0]) if len(sd_nsvb) else 0.0
        a_n_plots = int(sd_nsvb["N_PLOTS"][0]) if len(sd_nsvb) else 0
        a_n_trees = int(sd_nsvb["N_TREES"][0]) if "N_TREES" in sd_nsvb.columns else 0

        # B) FIADB-stored CARBON_AG+CARBON_BG for dead trees
        sd_fiadb = carbon_pool(
            db, pool="total", tree_type="dead", land_type="forest", totals=True
        )
        b_total_st = float(sd_fiadb["CARBON_TOTAL"][0]) if len(sd_fiadb) else 0.0
        b_n_plots = int(sd_fiadb["N_PLOTS"][0]) if "N_PLOTS" in sd_fiadb.columns else 0
        b_n_trees = int(sd_fiadb["N_TREES"][0]) if "N_TREES" in sd_fiadb.columns else 0

        # C) Downed dead (shared between pyFIA and EPA)
        dd = downed_dead(db, land_type="forest", totals=True)
        c_total_st = float(dd["CARBON_TOTAL"][0]) if len(dd) else 0.0

    return {
        "state": state,
        "evalid": evalid,
        "elapsed_s": time.perf_counter() - t0,
        # Standing dead, NSVB recomputation
        "A_standing_dead_nsvb_st": a_total_st,
        "A_standing_dead_nsvb_mmt": _to_mmt(a_total_st),
        "A_n_plots": a_n_plots,
        "A_n_trees": a_n_trees,
        # Standing dead, FIADB-stored CARBON_AG+BG
        "B_standing_dead_fiadb_st": b_total_st,
        "B_standing_dead_fiadb_mmt": _to_mmt(b_total_st),
        "B_n_plots": b_n_plots,
        "B_n_trees": b_n_trees,
        # Downed dead (shared method)
        "C_downed_dead_mmt": _to_mmt(c_total_st),
        # Diagnostics
        "A_minus_B_mmt": _to_mmt(a_total_st - b_total_st),
        "A_over_B_pct": (
            100.0 * (a_total_st - b_total_st) / b_total_st if b_total_st else float("nan")
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--states", default=",".join(DEFAULT_STATES),
        help=f"Comma-separated postal codes (default: {','.join(DEFAULT_STATES)})",
    )
    args = parser.parse_args()
    states = [s.strip().upper() for s in args.states.split(",") if s.strip()]

    print(f"\n=== Dead Wood diagnostic — {len(states)} states ===")
    print("    A = standing_dead (NSVB recomputation, pyFIA new path)")
    print("    B = carbon_pool(pool=total, tree_type=dead) (FIADB CARBON_AG+BG)")
    print("    C = downed_dead (Domke 2013, shared with EPA)\n")

    rows = []
    for st in states:
        r = diagnose_state(st)
        rows.append(r)
        print(
            f"  {st}: EVALID={r['evalid']} "
            f"A={r['A_standing_dead_nsvb_mmt']:.2f} "
            f"B={r['B_standing_dead_fiadb_mmt']:.2f} "
            f"A−B={r['A_minus_B_mmt']:+.2f} MMT C "
            f"({r['A_over_B_pct']:+.1f}%)  "
            f"plotsA/B={r['A_n_plots']}/{r['B_n_plots']}  "
            f"treesA/B={r['A_n_trees']}/{r['B_n_trees']}  "
            f"({r['elapsed_s']:.1f}s)"
        )

    df = pl.DataFrame(rows)

    # Per-state summary table
    print("\n=== Per-state results (MMT C) ===")
    summary = df.select([
        "state",
        "A_standing_dead_nsvb_mmt",
        "B_standing_dead_fiadb_mmt",
        "A_minus_B_mmt",
        "A_over_B_pct",
        "C_downed_dead_mmt",
    ])
    with pl.Config(tbl_rows=20, tbl_cols=10, fmt_str_lengths=40, tbl_width_chars=140):
        print(summary)

    # Aggregate diagnostic
    a_sum = df["A_standing_dead_nsvb_mmt"].sum()
    b_sum = df["B_standing_dead_fiadb_mmt"].sum()
    c_sum = df["C_downed_dead_mmt"].sum()
    print(f"\n=== Across {len(states)} states ===")
    print(f"  A (NSVB recompute) standing dead total: {a_sum:.1f} MMT C")
    print(f"  B (FIADB CARBON_AG+BG) standing dead total: {b_sum:.1f} MMT C")
    print(f"  Standing dead delta (A − B): {a_sum - b_sum:+.1f} MMT C "
          f"({100.0 * (a_sum - b_sum) / b_sum:+.1f}%)")
    print(f"  C (downed dead, shared method) total: {c_sum:.1f} MMT C")
    print()
    print("Interpretation:")
    if abs(100.0 * (a_sum - b_sum) / b_sum) > 5:
        print("  → standing_dead NSVB recomputation diverges materially from")
        print("    FIADB-stored CARBON_AG+CARBON_BG. EPA's report is built on the")
        print("    FIADB-stored values; reproducing the report requires using path B,")
        print("    not pyFIA's new NSVB recomputation.")
    else:
        print("  → standing_dead path is consistent with FIADB-stored values.")
        print("    The +38% Dead Wood gap must come from downed_dead or pool")
        print("    definition. Check downed_dead validation status.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
