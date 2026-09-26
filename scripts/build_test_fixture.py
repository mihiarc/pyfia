"""Cut the committed FIADB test fixture from an Alabama FIADB DuckDB file.

Keeps every visit of every plot in three Alabama counties, plus every earlier
visit reached through PREV_PLT_CN, so every remeasured pair resolves. Tables
keyed by plot are subset to those plots; population and REF tables are kept
whole. Columns that pyFIA doesn't read are dropped from the tree tables.

Writes one zstd parquet file per table to ``tests/fixtures/fiadb_al/``. The
test session loads them into a temporary DuckDB (``tests/conftest.py``).

Usage::

    uv run python scripts/build_test_fixture.py /path/to/AL.duckdb
"""

from __future__ import annotations

import shutil
import sys
from pathlib import Path

import duckdb

OUT = Path(__file__).parents[1] / "tests" / "fixtures" / "fiadb_al"

# Tuscaloosa, Clarke and Washington counties (survey units 4, 2 and 1).
COUNTIES = (125, 25, 129)

# Keyed by PLOT.CN
CN_TABLES = ("PLOT", "PLOTGEOM")
# Keyed by PLT_CN
PLOT_TABLES = (
    "COND",
    "TREE",
    "SUBPLOT",
    "SUBP_COND",
    "SUBP_COND_CHNG_MTRX",
    "TREE_GRM_COMPONENT",
    "TREE_GRM_BEGIN",
    "TREE_GRM_MIDPT",
    "POP_PLOT_STRATUM_ASSGN",
)
WHOLE_TABLES = (
    "POP_EVAL",
    "POP_EVAL_GRP",
    "POP_EVAL_TYP",
    "POP_ESTN_UNIT",
    "POP_STRATUM",
    "POP_EVAL_ATTRIBUTE",
    "SURVEY",
    "COUNTY",
)

# Volume and biomass variants no pyFIA code path reads: stump, top and bark
# components, total-stem, sound and gross cubic-foot variants, sawlog biomass
# and Scribner board feet.
_UNUSED_TREE_COLUMNS = (
    "DRYBIO_BOLE_BARK",
    "DRYBIO_SAWLOG",
    "DRYBIO_SAWLOG_BARK",
    "DRYBIO_STEM",
    "DRYBIO_STEM_BARK",
    "DRYBIO_STUMP",
    "DRYBIO_STUMP_BARK",
    "VOLBSGRS",
    "VOLBSNET",
    "VOLCFGRS_BARK",
    "VOLCFGRS_STUMP",
    "VOLCFGRS_STUMP_BARK",
    "VOLCFGRS_TOP",
    "VOLCFGRS_TOP_BARK",
    "VOLCFNET_BARK",
    "VOLCFSND_BARK",
    "VOLCFSND_STUMP",
    "VOLCFSND_STUMP_BARK",
    "VOLCFSND_TOP",
    "VOLCFSND_TOP_BARK",
    "VOLCSGRS",
    "VOLCSGRS_BARK",
    "VOLCSNET_BARK",
    "VOLCSSND",
    "VOLCSSND_BARK",
    "VOLTSGRS",
    "VOLTSGRS_BARK",
    "VOLTSSND",
    "VOLTSSND_BARK",
)
DROP_COLUMNS = {
    t: _UNUSED_TREE_COLUMNS for t in ("TREE", "TREE_GRM_BEGIN", "TREE_GRM_MIDPT")
}


def main(src: str) -> None:
    con = duckdb.connect()
    con.execute(f"ATTACH '{src}' AS src (READ_ONLY)")

    tables = {
        r[0]
        for r in con.execute(
            "SELECT table_name FROM duckdb_tables() WHERE database_name = 'src'"
        ).fetchall()
    }
    ref_tables = sorted(t for t in tables if t.startswith("REF_"))

    counties = ", ".join(map(str, COUNTIES))
    con.execute(
        f"""
        CREATE TEMP TABLE keep_plt AS
        WITH RECURSIVE k(CN) AS (
            SELECT CN FROM src.PLOT WHERE STATECD = 1 AND COUNTYCD IN ({counties})
            UNION
            SELECT p.PREV_PLT_CN FROM src.PLOT p JOIN k ON p.CN = k.CN
            WHERE p.PREV_PLT_CN IS NOT NULL
        )
        SELECT DISTINCT k.CN FROM k JOIN src.PLOT p ON p.CN = k.CN
        """
    )

    def columns(table: str) -> tuple[str, str]:
        cols = [
            r[0]
            for r in con.execute(
                "SELECT column_name FROM duckdb_columns() WHERE database_name = 'src' "
                "AND table_name = ? ORDER BY column_index",
                [table],
            ).fetchall()
        ]
        key = next((k for k in ("CN", "TRE_CN") if k in cols), None)
        order = key or ", ".join(f'"{c}"' for c in cols)
        drop = set(DROP_COLUMNS.get(table, ()))
        return ", ".join(f'"{c}"' for c in cols if c not in drop), order

    plan = []
    for t in CN_TABLES:
        plan.append((t, "WHERE CN IN (SELECT CN FROM keep_plt)"))
    for t in PLOT_TABLES:
        plan.append((t, "WHERE PLT_CN IN (SELECT CN FROM keep_plt)"))
    for t in (*WHOLE_TABLES, *ref_tables):
        if t in tables:
            plan.append((t, ""))

    if OUT.exists():
        shutil.rmtree(OUT)
    OUT.mkdir(parents=True)
    total = 0
    for table, where in plan:
        select, order = columns(table)
        path = OUT / f"{table}.parquet"
        con.execute(
            f"COPY (SELECT {select} FROM src.{table} {where} ORDER BY {order}) "
            f"TO '{path}' (FORMAT parquet, COMPRESSION zstd, COMPRESSION_LEVEL 19)"
        )
        total += path.stat().st_size

    version = con.execute(
        "SELECT VERSION FROM src.REF_FIADB_VERSION ORDER BY CREATED_DATE DESC LIMIT 1"
    ).fetchone()
    plots = con.execute("SELECT count(*) FROM keep_plt").fetchone()
    print(f"{len(plan)} tables, {plots[0] if plots else 0} plot visits")
    print(f"{total / 1e6:.1f} MB written to {OUT}")
    print(f"Source: {version[0] if version else 'unknown'}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    main(sys.argv[1])
