"""All-live GRM estimates use the MICR_ columns of TREE_GRM_COMPONENT (#167).

EVALIDator's estimates for all live trees (at least 1 inch) read the MICR_
component, SUBPTYP_GRM and TPA columns, which include saplings tallied on the
microplot (SUBPTYP_GRM 2). The SUBP_ all-live columns cover trees at least 5
inches only. The oracle is EVALIDator's formula, written out in SQL, on the
committed Alabama fixture.
"""

from __future__ import annotations

import duckdb
import pytest

from pyfia import FIA, mortality, removals
from pyfia.estimation.grm import resolve_grm_columns

EVALID_GRM = 12403

COMPONENTS = {
    "removals": ("TPAREMV", "g.COMPONENT LIKE 'CUT%' OR g.COMPONENT LIKE 'DIVERSION%'"),
    "mortality": ("TPAMORT", "g.COMPONENT LIKE 'MORTALITY%'"),
}


def evalidator_trees(db_path, estimator: str, design: str, land: str) -> float:
    """Annual trees removed or dying, by EVALIDator's formula, using the
    ``design`` ('MICR' or 'SUBP') all-live columns."""
    tpa, component = COMPONENTS[estimator]
    sql = f"""
        SELECT SUM(v * EXPNS) FROM (
          SELECT ps.EXPNS, plot.CN,
            SUM(g.TPA * CASE WHEN COALESCE(g.SUBPTYP_GRM, 0) = 0 THEN 0
                             WHEN g.SUBPTYP_GRM = 1 THEN ps.ADJ_FACTOR_SUBP
                             WHEN g.SUBPTYP_GRM = 2 THEN ps.ADJ_FACTOR_MICR
                             WHEN g.SUBPTYP_GRM = 3 THEN ps.ADJ_FACTOR_MACR
                             ELSE 0 END
                * CASE WHEN {component} THEN 1 ELSE 0 END) AS v
          FROM POP_STRATUM ps
          JOIN POP_PLOT_STRATUM_ASSGN a ON a.STRATUM_CN = ps.CN
          JOIN PLOT plot ON plot.CN = a.PLT_CN
          JOIN COND c ON c.PLT_CN = plot.CN
          JOIN TREE t ON t.PLT_CN = c.PLT_CN AND t.CONDID = c.CONDID
          JOIN (SELECT TRE_CN,
                       {design}_COMPONENT_AL_{land} AS COMPONENT,
                       {design}_SUBPTYP_GRM_AL_{land} AS SUBPTYP_GRM,
                       {design}_{tpa}_UNADJ_AL_{land} AS TPA
                FROM TREE_GRM_COMPONENT) g ON g.TRE_CN = t.CN
          WHERE ps.EVALID = {EVALID_GRM}
          GROUP BY ps.EXPNS, plot.CN)
    """
    with duckdb.connect(str(db_path), read_only=True) as con:
        return con.sql(sql).fetchone()[0]


@pytest.mark.parametrize("tree_type", ["al", "live"])
def test_all_live_resolves_micr_columns(tree_type):
    cols = resolve_grm_columns("removals", tree_type, "timber")
    assert cols.component == "MICR_COMPONENT_AL_TIMBER"
    assert cols.subptyp == "MICR_SUBPTYP_GRM_AL_TIMBER"
    assert cols.tpa == "MICR_TPAREMV_UNADJ_AL_TIMBER"


@pytest.mark.parametrize("tree_type", ["gs", "sl", "sawtimber"])
def test_growing_stock_and_sawtimber_keep_subp_columns(tree_type):
    cols = resolve_grm_columns("mortality", tree_type, "forest")
    assert cols.component.startswith("SUBP_COMPONENT_")
    assert cols.subptyp.startswith("SUBP_SUBPTYP_GRM_")
    assert cols.tpa.startswith("SUBP_TPAMORT_UNADJ_")


@pytest.mark.parametrize("land_type", ["forest", "timber"])
@pytest.mark.parametrize(
    "estimator,fn", [("removals", removals), ("mortality", mortality)]
)
def test_all_live_trees_match_evalidator_formula(
    fiadb_fixture_path, estimator, fn, land_type
):
    land = land_type.upper()
    expected = evalidator_trees(fiadb_fixture_path, estimator, "MICR", land)
    at_least_5_inches = evalidator_trees(fiadb_fixture_path, estimator, "SUBP", land)
    # Saplings add trees, so the two column sets differ on the fixture.
    assert expected > at_least_5_inches > 0

    with FIA(str(fiadb_fixture_path)) as db:
        db.clip_by_evalid(EVALID_GRM)
        result = fn(db, tree_type="al", land_type=land_type, measure="tpa")
    total = next(c for c in result.columns if c.endswith("_TOTAL"))
    assert result[total][0] == pytest.approx(expected, rel=1e-9)


def test_saplings_on_the_microplot_are_counted(fiadb_fixture_path):
    """Some all-live mortality rows are microplot saplings (SUBPTYP_GRM 2)."""
    with duckdb.connect(str(fiadb_fixture_path), read_only=True) as con:
        n = con.sql(
            f"""
            SELECT count(*) FROM TREE_GRM_COMPONENT g
            JOIN POP_PLOT_STRATUM_ASSGN a ON a.PLT_CN = g.PLT_CN
            WHERE a.EVALID = {EVALID_GRM}
              AND g.MICR_COMPONENT_AL_FOREST LIKE 'MORTALITY%'
              AND g.MICR_SUBPTYP_GRM_AL_FOREST = 2
              AND g.MICR_TPAMORT_UNADJ_AL_FOREST > 0
            """
        ).fetchone()[0]
    assert n > 0
