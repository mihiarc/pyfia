"""
NGHGI multi-year validation — confirm Stage A accuracy across years
2019-2023 (the inventory years for which EPA Table 6-10 publishes
distinct stock values; 1990 and 2005 are modeled by EPA via age-class
projection and not directly reproducible from FIADB).

For each target year, picks each CONUS-48 state's CURRENT AREA, CURRENT
VOLUME EVALID with the closest END_INVYR ≤ target year, runs the
EPA-pool-aggregated stock compilation in FIADB-fidelity mode, sums to
CONUS, and compares to the EPA Table 6-10 published value for that year.

Usage:
    uv run python scripts/nghgi/multi_year.py
    uv run python scripts/nghgi/multi_year.py --years 2020,2022
    uv run python scripts/nghgi/multi_year.py --db-dir /path/to/fiadb

Database directory resolution (in order):
    1. --db-dir CLI argument
    2. $PYFIA_FIADB_DIR environment variable
    3. ./data/fiadb (relative to current working directory)
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).parent))

from _compile import compile_state_stocks, load_published_targets  # noqa: E402
from _paths import resolve_db_dir  # noqa: E402

from pyfia import FIA  # noqa: E402

DEFAULT_YEARS = [2019, 2020, 2021, 2022, 2023]

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


def _select_evalid_for_year(db: FIA, target_year: int) -> tuple[int, int] | None:
    """Return (evalid, end_invyr) for the closest annual VOL EVALID with
    END_INVYR ≤ target_year. Returns None if no such EVALID exists."""
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
    le_target = evals.filter(pl.col("END_INVYR") <= target_year)
    if le_target.is_empty():
        row = evals.tail(1)
    else:
        row = le_target.head(1)
    return int(row["EVALID"][0]), int(row["END_INVYR"][0])


def run_state_year(
    state: str, year: int, *, db_dir: Path
) -> tuple[pl.DataFrame, dict] | None:
    db_path = db_dir / f"{state}.duckdb"
    if not db_path.exists():
        return None
    t0 = time.perf_counter()
    with FIA(str(db_path)) as db:
        sel = _select_evalid_for_year(db, year)
        if sel is None:
            return None
        evalid, end_yr = sel
        db.clip_by_evalid(evalid)
        df = compile_state_stocks(db, state_label=state, mode="fiadb")
    df = df.with_columns(
        pl.lit(evalid).alias("EVALID"),
        pl.lit(end_yr).alias("END_INVYR"),
        pl.lit(year).alias("TARGET_YEAR"),
    )
    return df, {
        "state": state,
        "year": year,
        "evalid": evalid,
        "end_invyr": end_yr,
        "elapsed_s": time.perf_counter() - t0,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--years", default=",".join(str(y) for y in DEFAULT_YEARS))
    parser.add_argument("--states", default=",".join(CONUS_48))
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

    years = [int(y) for y in args.years.split(",") if y.strip()]
    states = [s.strip().upper() for s in args.states.split(",") if s.strip()]

    print("\n=== NGHGI multi-year validation ===")
    print(f"    States: {len(states)} (CONUS-48 default)")
    print(f"    Years: {years}")
    print(f"    State DB directory: {db_dir}\n")

    all_rows: list[pl.DataFrame] = []
    skipped: list[tuple[str, int]] = []
    end_yr_summary: dict[int, dict[int, int]] = {y: {} for y in years}

    for year in years:
        print(f"[Year {year}]")
        t_year = time.perf_counter()
        n_ok = 0
        for state in states:
            res = run_state_year(state, year, db_dir=db_dir)
            if res is None:
                skipped.append((state, year))
                continue
            df, meta = res
            all_rows.append(df)
            end_yr_summary[year].setdefault(meta["end_invyr"], 0)
            end_yr_summary[year][meta["end_invyr"]] += 1
            n_ok += 1
        print(
            f"  {n_ok}/{len(states)} states ok in {time.perf_counter() - t_year:.1f}s. "
            f"END_INVYR distribution: "
            f"{dict(sorted(end_yr_summary[year].items(), reverse=True))}"
        )

    if not all_rows:
        print("No successful runs — aborting.")
        return 1

    combined = pl.concat(all_rows, how="vertical_relaxed")

    rollup = (
        combined.group_by(["TARGET_YEAR", "EPA_POOL"])
        .agg(pl.col("STOCK_MMT_C").sum().alias("PYFIA_MMT_C"))
        .sort(["TARGET_YEAR", "EPA_POOL"])
    )

    epa_all = pl.concat(
        [
            load_published_targets(year=y)
            .select(["EPA_POOL", "YEAR", "STOCK_MMT_C"])
            .rename({"STOCK_MMT_C": "EPA_MMT_C", "YEAR": "TARGET_YEAR"})
            for y in years
        ],
        how="vertical_relaxed",
    )

    cmp = rollup.join(epa_all, on=["TARGET_YEAR", "EPA_POOL"], how="left")
    cmp = cmp.with_columns(
        (pl.col("PYFIA_MMT_C") - pl.col("EPA_MMT_C")).alias("ABS_DIFF"),
        (
            100.0 * (pl.col("PYFIA_MMT_C") - pl.col("EPA_MMT_C")) / pl.col("EPA_MMT_C")
        ).alias("PCT_DIFF"),
    )

    # Synthesize a "Forest Ecosystem + EPA Soil Organic" row per year.
    eco_rows = []
    for y in years:
        py_eco = float(
            cmp.filter(
                (pl.col("TARGET_YEAR") == y)
                & (pl.col("EPA_POOL") == "FOREST_ECOSYSTEM")
            )["PYFIA_MMT_C"][0]
        )
        epa_eco = float(
            cmp.filter(
                (pl.col("TARGET_YEAR") == y)
                & (pl.col("EPA_POOL") == "FOREST_ECOSYSTEM")
            )["EPA_MMT_C"][0]
        )
        epa_so = float(
            load_published_targets(y).filter(pl.col("EPA_POOL") == "SOIL_ORGANIC")[
                "STOCK_MMT_C"
            ][0]
        )
        py_eco_full = py_eco + epa_so
        eco_rows.append(
            {
                "TARGET_YEAR": y,
                "EPA_POOL": "FOREST_ECO_PLUS_SO",
                "PYFIA_MMT_C": py_eco_full,
                "EPA_MMT_C": epa_eco,
                "ABS_DIFF": py_eco_full - epa_eco,
                "PCT_DIFF": 100.0 * (py_eco_full - epa_eco) / epa_eco,
            }
        )
    cmp_full = pl.concat([cmp, pl.DataFrame(eco_rows)], how="vertical_relaxed").sort(
        ["TARGET_YEAR", "EPA_POOL"]
    )

    print("\n=== Per-year, per-pool comparison (CONUS-48 vs EPA, MMT C) ===")
    with pl.Config(tbl_rows=80, tbl_cols=10, fmt_str_lengths=30, tbl_width_chars=140):
        print(cmp_full)

    print(
        "\n=== Headline: Forest Ecosystem (CONUS-48 + EPA Soil Organic) vs EPA Total ==="
    )
    headline = cmp_full.filter(pl.col("EPA_POOL") == "FOREST_ECO_PLUS_SO").select(
        ["TARGET_YEAR", "PYFIA_MMT_C", "EPA_MMT_C", "ABS_DIFF", "PCT_DIFF"]
    )
    with pl.Config(tbl_rows=20, tbl_cols=10, fmt_str_lengths=30, tbl_width_chars=120):
        print(headline)

    print("\n=== Combined Dead Organic Matter (Dead Wood + Litter) vs EPA ===")
    dom_rows = []
    for y in years:
        py_dom = float(
            cmp.filter(
                (pl.col("TARGET_YEAR") == y)
                & (pl.col("EPA_POOL").is_in(["DEAD_WOOD", "LITTER"]))
            )["PYFIA_MMT_C"].sum()
        )
        epa_dom = float(
            cmp.filter(
                (pl.col("TARGET_YEAR") == y)
                & (pl.col("EPA_POOL").is_in(["DEAD_WOOD", "LITTER"]))
            )["EPA_MMT_C"].sum()
        )
        dom_rows.append(
            {
                "TARGET_YEAR": y,
                "PYFIA_DOM": py_dom,
                "EPA_DOM": epa_dom,
                "ABS_DIFF": py_dom - epa_dom,
                "PCT_DIFF": 100.0 * (py_dom - epa_dom) / epa_dom,
            }
        )
    with pl.Config(tbl_rows=20, tbl_cols=10, fmt_str_lengths=30, tbl_width_chars=120):
        print(pl.DataFrame(dom_rows))

    if skipped:
        print(f"\nSkipped (no eligible EVALID): {len(skipped)} state-years")
        for st, yr in skipped[:10]:
            print(f"  {st} {yr}")
        if len(skipped) > 10:
            print(f"  ... and {len(skipped) - 10} more")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
