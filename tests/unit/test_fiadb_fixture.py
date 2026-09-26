"""Smoke tests for the committed Alabama FIADB fixture."""

import duckdb

from pyfia import area, mortality


def test_fixture_is_consistent(fiadb_fixture_path):
    with duckdb.connect(str(fiadb_fixture_path), read_only=True) as con:
        (version,) = con.execute(
            "SELECT VERSION FROM REF_FIADB_VERSION "
            "ORDER BY CREATED_DATE DESC LIMIT 1"
        ).fetchone()
        (dangling,) = con.execute(
            "SELECT count(*) FROM PLOT p LEFT JOIN PLOT q ON q.CN = p.PREV_PLT_CN "
            "WHERE p.PREV_PLT_CN IS NOT NULL AND q.CN IS NULL"
        ).fetchone()
        (multi_cond,) = con.execute(
            "SELECT count(*) FROM (SELECT PLT_CN FROM COND GROUP BY 1 "
            "HAVING count(*) > 1)"
        ).fetchone()
    assert version == "FIADB_1.9.4.00"
    assert dangling == 0
    assert multi_cond > 0


def test_estimators_run(fiadb_fixture):
    fiadb_fixture.clip_by_evalid(12401)
    forest = area(fiadb_fixture, land_type="forest")
    assert forest["AREA"][0] > 0

    fiadb_fixture.clip_by_evalid(12403)
    mort = mortality(fiadb_fixture, land_type="forest", tree_type="gs")
    assert mort["MORT_TOTAL"][0] > 0
