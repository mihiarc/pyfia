"""site_index() standard errors on the committed Alabama FIADB subset.

The oracle is Bechtold & Patterson's post-stratified ratio-of-means variance,
written out in SQL independently of pyFIA, over every plot in the evaluation:
plots without site index area in the domain enter with zero numerator and
denominator. A grouped estimate and the same estimate restricted to the group
by area_domain must therefore have the same SE.

The fixture holds three counties of each estimation unit while AREA_USED is
the whole unit's area, so its SEs are larger than a full state's. The checks
are equalities, which don't depend on that.
"""

from __future__ import annotations

import duckdb
import polars as pl
import pytest

from pyfia import FIA, site_index

EVALID = 12401


def bechtold_patterson_ratio(db_path, domain: str = "TRUE") -> tuple[float, float]:
    """(mean site index, SE) for forest conditions meeting ``domain`` (SQL on ``c``)."""
    sql = f"""
        WITH cv AS (
          SELECT c.PLT_CN,
                 SUM(c.SICOND * c.CONDPROP_UNADJ * adj) AS y,
                 SUM(c.CONDPROP_UNADJ * adj) AS x
          FROM (
            SELECT c.*, CASE c.PROP_BASIS WHEN 'MACR' THEN ps.ADJ_FACTOR_MACR
                                          ELSE ps.ADJ_FACTOR_SUBP END AS adj
            FROM COND c
            JOIN POP_PLOT_STRATUM_ASSGN a ON a.PLT_CN = c.PLT_CN
            JOIN POP_STRATUM ps ON ps.CN = a.STRATUM_CN
            WHERE ps.EVALID = {EVALID}
          ) c
          WHERE c.COND_STATUS_CD = 1 AND c.SICOND IS NOT NULL AND ({domain})
          GROUP BY c.PLT_CN
        ),
        plots AS (
          SELECT ps.ESTN_UNIT_CN AS unit, ps.CN AS stratum, ps.EXPNS AS expns,
                 eu.AREA_USED AS area_used,
                 ps.P1POINTCNT * 1.0 / eu.P1PNTCNT_EU AS wgt,
                 COALESCE(cv.y, 0) AS y, COALESCE(cv.x, 0) AS x
          FROM POP_PLOT_STRATUM_ASSGN a
          JOIN POP_STRATUM ps ON ps.CN = a.STRATUM_CN
          JOIN POP_ESTN_UNIT eu ON eu.CN = ps.ESTN_UNIT_CN
          LEFT JOIN cv ON cv.PLT_CN = a.PLT_CN
          WHERE ps.EVALID = {EVALID}
        ),
        strata AS (
          SELECT unit, ANY_VALUE(area_used) AS area_used, ANY_VALUE(wgt) AS wgt,
                 COUNT(*) AS n_h, SUM(expns * y) AS y_total, SUM(expns * x) AS x_total,
                 CASE WHEN COUNT(*) > 1 THEN VAR_SAMP(y) ELSE 0 END AS s2_y,
                 CASE WHEN COUNT(*) > 1 THEN VAR_SAMP(x) ELSE 0 END AS s2_x,
                 CASE WHEN COUNT(*) > 1 THEN COVAR_SAMP(y, x) ELSE 0 END AS s_yx
          FROM plots GROUP BY unit, stratum
        ),
        units AS (
          SELECT ANY_VALUE(area_used) AS area_used, SUM(n_h) AS n,
                 SUM(y_total) AS y_total, SUM(x_total) AS x_total,
                 SUM(wgt * s2_y) AS a_y, SUM((1 - wgt) * s2_y) AS b_y,
                 SUM(wgt * s2_x) AS a_x, SUM((1 - wgt) * s2_x) AS b_x,
                 SUM(wgt * s_yx) AS a_yx, SUM((1 - wgt) * s_yx) AS b_yx
          FROM strata GROUP BY unit
        ),
        totals AS (
          SELECT SUM(y_total) AS y, SUM(x_total) AS x,
                 SUM(area_used * area_used * (a_y / n + b_y / (n * n))) AS v_y,
                 SUM(area_used * area_used * (a_x / n + b_x / (n * n))) AS v_x,
                 SUM(area_used * area_used * (a_yx / n + b_yx / (n * n))) AS c_yx
          FROM units
        )
        SELECT y / x,
               SQRT((v_y + (y / x) * (y / x) * v_x - 2 * (y / x) * c_yx) / (x * x))
        FROM totals
    """
    with duckdb.connect(str(db_path), read_only=True) as con:
        return con.sql(sql).fetchone()


def run_site_index(db_path, **kwargs) -> pl.DataFrame:
    with FIA(str(db_path)) as db:
        db.clip_by_evalid(EVALID)
        return site_index(db, **kwargs)


def test_ungrouped_se(fiadb_fixture_path):
    result = run_site_index(fiadb_fixture_path)
    assert result["SIBASE"].to_list() == [50]
    mean, se = bechtold_patterson_ratio(fiadb_fixture_path, "c.SIBASE = 50")
    assert result["SI_MEAN"][0] == pytest.approx(mean, rel=1e-9)
    assert result["SI_SE"][0] == pytest.approx(se, rel=1e-9)


def test_grouped_se_per_owner_group(fiadb_fixture_path):
    result = run_site_index(fiadb_fixture_path, grp_by="OWNGRPCD")
    assert result.height > 1
    for row in result.iter_rows(named=True):
        mean, se = bechtold_patterson_ratio(
            fiadb_fixture_path,
            f"c.SIBASE = {row['SIBASE']} AND c.OWNGRPCD = {row['OWNGRPCD']}",
        )
        assert row["SI_MEAN"] == pytest.approx(mean, rel=1e-9)
        assert row["SI_SE"] == pytest.approx(se, rel=1e-9)


def test_grouped_se_equals_domain_se(fiadb_fixture_path):
    grouped = run_site_index(fiadb_fixture_path, grp_by="OWNGRPCD")
    for row in grouped.iter_rows(named=True):
        domain = run_site_index(
            fiadb_fixture_path, area_domain=f"OWNGRPCD == {row['OWNGRPCD']}"
        ).filter(pl.col("SIBASE") == row["SIBASE"])
        assert domain["SI_MEAN"][0] == pytest.approx(row["SI_MEAN"], rel=1e-12)
        assert domain["SI_SE"][0] == pytest.approx(row["SI_SE"], rel=1e-9)
