"""
GRM estimators group each tree by its own condition (#138).

EVALIDator groups growth, removals and mortality by the condition of the
tree's time-2 record (``TREE.CONDID``). These tests compare pyFIA's grouped
totals with a direct SQL computation of that convention on the committed
Alabama fixture, which has plots whose trees sit on conditions with different
disturbance and treatment codes.
"""

import duckdb
import pytest

from pyfia import mortality, removals

EVALID = 12403

CASES = {
    "mortality": (
        mortality,
        "DSTRBCD1",
        ("MORTALITY",),
        "SUBP_TPAMORT_UNADJ_GS_FOREST",
    ),
    "removals": (
        removals,
        "TRTCD1",
        ("CUT", "DIVERSION"),
        "SUBP_TPAREMV_UNADJ_GS_FOREST",
    ),
}


def own_condition_totals(path, col, components, tpa_col):
    """Expanded GS-forest volume per value of ``col`` on each tree's condition."""
    component_filter = " OR ".join(
        f"g.SUBP_COMPONENT_GS_FOREST LIKE '{c}%'" for c in components
    )
    with duckdb.connect(str(path), read_only=True) as con:
        rows = con.execute(
            f"""
            WITH plots AS (
                SELECT a.PLT_CN, s.EXPNS, s.ADJ_FACTOR_SUBP, s.ADJ_FACTOR_MICR,
                       s.ADJ_FACTOR_MACR
                FROM POP_PLOT_STRATUM_ASSGN a
                JOIN POP_STRATUM s ON s.CN = a.STRATUM_CN
                WHERE a.EVALID = {EVALID}
            )
            SELECT c.{col}, sum(
                g.{tpa_col} * m.VOLCFNET * p.EXPNS *
                CASE g.SUBP_SUBPTYP_GRM_GS_FOREST
                    WHEN 1 THEN p.ADJ_FACTOR_SUBP
                    WHEN 2 THEN p.ADJ_FACTOR_MICR
                    WHEN 3 THEN p.ADJ_FACTOR_MACR
                    ELSE 0 END)
            FROM TREE_GRM_COMPONENT g
            JOIN plots p ON p.PLT_CN = g.PLT_CN
            JOIN TREE t ON t.CN = g.TRE_CN
            JOIN TREE_GRM_MIDPT m ON m.TRE_CN = g.TRE_CN
            LEFT JOIN COND c ON c.PLT_CN = t.PLT_CN AND c.CONDID = t.CONDID
            WHERE {component_filter}
            GROUP BY 1
            """
        ).fetchall()
        (split_plots,) = con.execute(
            f"""
            SELECT count(*) FROM (
                SELECT g.PLT_CN
                FROM TREE_GRM_COMPONENT g
                JOIN POP_PLOT_STRATUM_ASSGN a
                  ON a.PLT_CN = g.PLT_CN AND a.EVALID = {EVALID}
                JOIN TREE t ON t.CN = g.TRE_CN
                JOIN COND c ON c.PLT_CN = t.PLT_CN AND c.CONDID = t.CONDID
                WHERE {component_filter}
                GROUP BY 1
                HAVING count(DISTINCT coalesce(c.{col}, -1)) > 1
            )
            """
        ).fetchone()
    return {k: v for k, v in rows if v}, split_plots


def estimate(estimator, db, **kwargs):
    df = estimator(db, land_type="forest", tree_type="gs", measure="volume", **kwargs)
    total_col = next(c for c in df.columns if c.endswith("_TOTAL"))
    return df, total_col


@pytest.mark.parametrize("case", CASES)
def test_group_totals_follow_each_trees_condition(
    fiadb_fixture, fiadb_fixture_path, case
):
    estimator, col, components, tpa_col = CASES[case]
    expected, split_plots = own_condition_totals(
        fiadb_fixture_path, col, components, tpa_col
    )
    # Plots whose trees sit on conditions with different codes are what the
    # first-condition-per-plot join got wrong.
    assert split_plots > 0

    fiadb_fixture.clip_by_evalid(EVALID)
    df, total_col = estimate(estimator, fiadb_fixture, grp_by=[col])
    got = {k: v for k, v in df.select(col, total_col).iter_rows() if v}

    assert got.keys() == expected.keys()
    for key, value in expected.items():
        assert got[key] == pytest.approx(value, rel=1e-9), f"{col}={key}"


@pytest.mark.parametrize("case", CASES)
def test_groups_sum_to_ungrouped_total(fiadb_fixture, case):
    estimator, col, _, _ = CASES[case]
    fiadb_fixture.clip_by_evalid(EVALID)
    total, total_col = estimate(estimator, fiadb_fixture)
    grouped, _ = estimate(estimator, fiadb_fixture, grp_by=[col])
    assert grouped[total_col].sum() == pytest.approx(total[total_col][0], rel=1e-12)


def test_group_se_uses_the_groups_own_plot_totals(fiadb_fixture):
    """A group's SE equals the SE of the same domain estimated on its own."""
    fiadb_fixture.clip_by_evalid(EVALID)
    grouped, total_col = estimate(removals, fiadb_fixture, grp_by=["TRTCD1"])
    se_col = f"{total_col}_SE"
    cut = grouped.filter(grouped["TRTCD1"] == 10)
    alone, _ = estimate(removals, fiadb_fixture, area_domain="TRTCD1 == 10")

    assert cut[total_col][0] == pytest.approx(alone[total_col][0], rel=1e-12)
    assert cut[se_col][0] == pytest.approx(alone[se_col][0], rel=1e-9)


def test_null_group_has_an_se(fiadb_fixture):
    """Diverted trees on nonforest conditions (null TRTCD1) keep their SE."""
    fiadb_fixture.clip_by_evalid(EVALID)
    grouped, total_col = estimate(removals, fiadb_fixture, grp_by=["TRTCD1"])
    null_group = grouped.filter(grouped["TRTCD1"].is_null())
    assert null_group.height == 1
    assert null_group[total_col][0] > 0
    assert null_group[f"{total_col}_SE"][0] > 0
