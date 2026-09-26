"""Grouped exact Bechtold & Patterson variance agrees with the scalar version (#149).

``calculate_grouped_domain_total_variance`` takes the exact post-stratified
path when the B&P columns (ESTN_UNIT_CN, STRATUM_WGT, AREA_USED, P2POINTCNT)
are present. For any one group it must give the same variance as
``calculate_domain_total_variance`` and ``calculate_ratio_of_means_variance``
on that group's plots. The data are real stratified plots from the committed
Alabama fixture (EVALID 12401: 404 plots, 13 strata, 4 estimation units,
including a single-plot stratum).
"""

import duckdb
import polars as pl
import pytest

from pyfia.estimation.variance import (
    calculate_domain_total_variance,
    calculate_grouped_domain_total_variance,
    calculate_ratio_of_means_variance,
)

EVALID = 12401

# One row per (plot, owner group): forest area proportion of the plot in that
# owner group (y_i) and forest area proportion of the plot (x_i). Every plot
# appears once per owner group, with zeros where it has no such land, as
# domain estimation requires.
PLOT_SQL = f"""
WITH strat AS (
    SELECT a.PLT_CN, s.CN AS STRATUM_CN, s.ESTN_UNIT_CN, s.EXPNS,
           s.ADJ_FACTOR_SUBP, s.P2POINTCNT, e.AREA_USED,
           CAST(s.P1POINTCNT AS DOUBLE) / e.P1PNTCNT_EU AS STRATUM_WGT
    FROM POP_PLOT_STRATUM_ASSGN a
    JOIN POP_STRATUM s ON s.CN = a.STRATUM_CN
    JOIN POP_ESTN_UNIT e ON e.CN = s.ESTN_UNIT_CN
    WHERE a.EVALID = {EVALID}
),
forest AS (
    SELECT PLT_CN, OWNGRPCD, CONDPROP_UNADJ
    FROM COND WHERE COND_STATUS_CD = 1 AND OWNGRPCD IS NOT NULL
),
grps AS (SELECT DISTINCT OWNGRPCD FROM forest)
SELECT st.PLT_CN, g.OWNGRPCD, st.STRATUM_CN, st.ESTN_UNIT_CN, st.EXPNS,
       st.STRATUM_WGT, st.AREA_USED, st.P2POINTCNT,
       COALESCE((SELECT SUM(f.CONDPROP_UNADJ) FROM forest f
                 WHERE f.PLT_CN = st.PLT_CN AND f.OWNGRPCD = g.OWNGRPCD), 0)
           * st.ADJ_FACTOR_SUBP AS y_i,
       COALESCE((SELECT SUM(f.CONDPROP_UNADJ) FROM forest f
                 WHERE f.PLT_CN = st.PLT_CN), 0)
           * st.ADJ_FACTOR_SUBP AS x_i
FROM strat st CROSS JOIN grps g
"""


@pytest.fixture(scope="module")
def plot_groups(fiadb_fixture_path) -> pl.DataFrame:
    with duckdb.connect(str(fiadb_fixture_path), read_only=True) as con:
        return con.sql(PLOT_SQL).pl()


def _scalar(plots: pl.DataFrame) -> tuple[dict, dict]:
    total = calculate_domain_total_variance(plots, "y_i")
    ratio = calculate_ratio_of_means_variance(plots, "y_i", "x_i")
    return total, ratio


def test_fixture_frame_exercises_exact_path(plot_groups):
    one = plot_groups.filter(pl.col("OWNGRPCD") == 40)
    assert one.height == 404
    assert one["ESTN_UNIT_CN"].n_unique() > 1
    assert one.group_by("STRATUM_CN").len()["len"].min() == 1
    assert one["y_i"].sum() > 0


def test_single_group_matches_scalar(plot_groups):
    plots = plot_groups.filter(pl.col("OWNGRPCD") == 40).with_columns(
        pl.lit(1).alias("GROUP")
    )
    total, ratio = _scalar(plots)

    without_x = calculate_grouped_domain_total_variance(
        plots, group_cols=["GROUP"], y_col="y_i", x_col="absent"
    )
    assert without_x["se_total"][0] == pytest.approx(total["se_total"], rel=1e-12)
    assert without_x["variance_total"][0] == pytest.approx(
        total["variance_total"], rel=1e-12
    )

    with_x = calculate_grouped_domain_total_variance(
        plots, group_cols=["GROUP"], y_col="y_i", x_col="x_i"
    )
    assert with_x["se_total"][0] == pytest.approx(ratio["se_total"], rel=1e-12)
    assert with_x["se_acre"][0] == pytest.approx(ratio["se_ratio"], rel=1e-12)
    assert with_x["variance_acre"][0] == pytest.approx(
        ratio["variance_ratio"], rel=1e-12
    )


def test_each_group_matches_scalar_on_its_plots(plot_groups):
    grouped = calculate_grouped_domain_total_variance(
        plot_groups, group_cols=["OWNGRPCD"], y_col="y_i", x_col="x_i"
    )
    assert grouped.height == plot_groups["OWNGRPCD"].n_unique() > 1

    for row in grouped.iter_rows(named=True):
        plots = plot_groups.filter(pl.col("OWNGRPCD") == row["OWNGRPCD"])
        total, ratio = _scalar(plots)
        assert row["se_total"] == pytest.approx(total["se_total"], rel=1e-12)
        assert row["se_total"] == pytest.approx(ratio["se_total"], rel=1e-12)
        assert row["se_acre"] == pytest.approx(ratio["se_ratio"], rel=1e-12)
