"""
NGHGI Stage A — reproduce EPA Chapter 6 Table 6-10 forest ecosystem
carbon stocks across CONUS using pyfia.

Iterates the 48 conterminous US state DuckDB files (and optionally Alaska),
runs the EPA-pool-aggregated stock compilation per state, sums to CONUS, and
prints a side-by-side comparison vs the published EPA Table 6-10.

Usage:
    uv run python scripts/nghgi/stage_a.py
    uv run python scripts/nghgi/stage_a.py --include-ak
    uv run python scripts/nghgi/stage_a.py --states GA,FL,SC
    uv run python scripts/nghgi/stage_a.py --db-dir /path/to/fiadb

Database directory resolution (in order):
    1. --db-dir CLI argument
    2. $PYFIA_FIADB_DIR environment variable
    3. ./data/fiadb (relative to current working directory)

The output is intentionally "honest about what we're reproducing": each
state's selected EVALID and END_INVYR is logged so methodological
discrepancies vs the published 2022 target (which used the September 2023
FIADB snapshot) are visible.
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import polars as pl

# Allow `from _compile import ...` regardless of CWD: scripts in
# ``scripts/nghgi/`` add their own directory to sys.path automatically when
# invoked directly, but we add it explicitly for clarity.
sys.path.insert(0, str(Path(__file__).parent))

from _compile import compare_to_published, compile_state_stocks  # noqa: E402
from _paths import resolve_db_dir  # noqa: E402

from pyfia import FIA  # noqa: E402

# 48 conterminous US states. EPA's Section 6.2 reproduction target uses
# stock-difference here. AK uses a mix (coastal SD, interior gain-loss);
# HI / AS / GU / MP / PR / VI use gain-loss only and are out of scope for
# this reproduction.
CONUS_48 = [
    "AL",
    "AR",
    "AZ",
    "CA",
    "CO",
    "CT",
    "DE",
    "FL",
    "GA",
    "IA",
    "ID",
    "IL",
    "IN",
    "KS",
    "KY",
    "LA",
    "MA",
    "MD",
    "ME",
    "MI",
    "MN",
    "MO",
    "MS",
    "MT",
    "NC",
    "ND",
    "NE",
    "NH",
    "NJ",
    "NM",
    "NV",
    "NY",
    "OH",
    "OK",
    "OR",
    "PA",
    "RI",
    "SC",
    "SD",
    "TN",
    "TX",
    "UT",
    "VA",
    "VT",
    "WA",
    "WI",
    "WV",
    "WY",
]


def _select_evalid_for_state(db: FIA, prefer_end_year: int | None = None) -> int:
    """Pick the most recent annual VOL EVALID for the open state DB.

    If ``prefer_end_year`` is given, return the EVALID whose END_INVYR
    matches if present, else fall back to most-recent.
    """
    evals = db.query(
        """
        SELECT EVALID, END_INVYR, EVAL_DESCR
        FROM POP_EVAL
        WHERE EVAL_DESCR LIKE '%CURRENT AREA, CURRENT VOLUME%'
        ORDER BY END_INVYR DESC
        """
    )
    if evals.is_empty():
        raise RuntimeError("No CURRENT AREA, CURRENT VOLUME EVALIDs found")
    if prefer_end_year is not None:
        match = evals.filter(pl.col("END_INVYR") == prefer_end_year)
        if not match.is_empty():
            return int(match["EVALID"][0])
    return int(evals["EVALID"][0])


def run_state(
    state: str,
    *,
    db_dir: Path,
    prefer_end_year: int | None = None,
) -> tuple[pl.DataFrame, dict]:
    """Compile EPA-pool stocks for one state. Returns (df, meta)."""
    db_path = db_dir / f"{state}.duckdb"
    if not db_path.exists():
        raise FileNotFoundError(db_path)

    t0 = time.perf_counter()
    with FIA(str(db_path)) as db:
        evalid = _select_evalid_for_state(db, prefer_end_year=prefer_end_year)
        db.clip_by_evalid(evalid)
        end_yr = int(
            db.query(f"SELECT END_INVYR FROM POP_EVAL WHERE EVALID = {evalid}")[
                "END_INVYR"
            ][0]
        )
        df = compile_state_stocks(db, state_label=state)
    df = df.with_columns(
        pl.lit(evalid).alias("EVALID"),
        pl.lit(int(end_yr)).alias("END_INVYR"),
    )
    meta = {
        "state": state,
        "evalid": evalid,
        "end_invyr": int(end_yr),
        "elapsed_s": time.perf_counter() - t0,
    }
    return df, meta


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--states",
        type=str,
        default=None,
        help="Comma-separated state postal codes to run. Default: all CONUS-48.",
    )
    parser.add_argument(
        "--include-ak",
        action="store_true",
        help="Also include Alaska (mixes stock-difference and gain-loss methods in EPA report).",
    )
    parser.add_argument(
        "--prefer-end-year",
        type=int,
        default=2022,
        help="Prefer EVALIDs with this END_INVYR if available (default 2022 to match EPA Chapter 6 reporting year).",
    )
    parser.add_argument(
        "--target-year",
        type=int,
        default=2022,
        help="EPA Table 6-10 column to compare against (default 2022).",
    )
    parser.add_argument(
        "--db-dir",
        type=str,
        default=None,
        help="Directory containing per-state DuckDB files (overrides $PYFIA_FIADB_DIR).",
    )
    args = parser.parse_args()

    try:
        db_dir = resolve_db_dir(args.db_dir)
    except FileNotFoundError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 2

    if args.states:
        states = [s.strip().upper() for s in args.states.split(",") if s.strip()]
    else:
        states = list(CONUS_48)
        if args.include_ak:
            states.append("AK")

    print(f"\n=== NGHGI Stage A reproduction — {len(states)} states ===")
    print(f"    Target: EPA Chapter 6 Table 6-10, year {args.target_year}")
    print(f"    Prefer EVALID with END_INVYR={args.prefer_end_year} per state")
    print(f"    State DB directory: {db_dir}\n")

    all_frames: list[pl.DataFrame] = []
    metas: list[dict] = []
    failures: list[tuple[str, str]] = []

    for i, st in enumerate(states, 1):
        try:
            df, meta = run_state(
                st, db_dir=db_dir, prefer_end_year=args.prefer_end_year
            )
            all_frames.append(df)
            metas.append(meta)
            print(
                f"  [{i:>2}/{len(states)}] {st}: EVALID={meta['evalid']} "
                f"end_yr={meta['end_invyr']} ({meta['elapsed_s']:.1f}s)"
            )
        except Exception as e:
            print(f"  [{i:>2}/{len(states)}] {st}: FAILED — {type(e).__name__}: {e}")
            failures.append((st, str(e)))

    if not all_frames:
        print("\nNo successful state runs — aborting.")
        return 1

    print(
        f"\nSucceeded: {len(all_frames)}/{len(states)} states. "
        f"Failed: {len(failures)}.\n"
    )

    long_df = pl.concat(all_frames, how="vertical_relaxed")

    # Per-state pivot for inspection
    per_state = long_df.pivot(values="STOCK_MMT_C", index="STATE", on="EPA_POOL").sort(
        "STATE"
    )
    print("=== Per-state stocks (MMT C) ===")
    with pl.Config(tbl_rows=60, tbl_cols=10, fmt_str_lengths=80):
        print(per_state)

    # CONUS rollup
    print("\n=== CONUS rollup vs EPA Chapter 6 Table 6-10 ===")
    cmp = compare_to_published(long_df, year=args.target_year)
    with pl.Config(tbl_rows=12, tbl_cols=10, fmt_str_lengths=40):
        print(cmp)

    print(
        "\nNotes:\n"
        "  - SOIL_ORGANIC (Histosols) is NOT reproduced from FIADB; EPA uses\n"
        "    IPCC defaults for that pool. Compare the SOIL_ORGANIC row\n"
        "    against the constant published target only.\n"
        "  - EPA's Table 6-10 stock columns include AK + HI + territories.\n"
        "    This run is CONUS-48"
        + (" + AK" if "AK" in states else "")
        + ". Expect a gap from non-CONUS forest land.\n"
        "  - HWP (Harvested Wood) and Drained Organic Soils are out of scope.\n"
        "  - Pool decomposition: EPA AGB = pyfia live_tree(AG) + understory(AG);\n"
        "    EPA Dead Wood = standing_dead + downed_dead. Verify mapping in\n"
        "    scripts/nghgi/_compile.py if numbers look off.\n"
    )

    if failures:
        print("\nFailures:")
        for st, err in failures:
            print(f"  {st}: {err}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
