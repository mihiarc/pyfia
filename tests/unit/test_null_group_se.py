"""A null group is a real group: grouped estimators keep its SE and counts.

Grouped SEs, volume's non-zero ``N_PLOTS`` and ``carbon_flux()``'s component
totals are computed per group and joined back to the results on the group
keys. Polars joins don't match null keys, so a null group (``OWNGRPCD`` on
nonforest land, ``STAND_STRUCTURE_SRS`` where it wasn't recorded) came back
with a null SE, a null count or missing components.

Each test compares the null group with an independent computation on the
committed Alabama fixture: the same estimate restricted to that group by
``area_domain``, or the same grouping after recoding the null to a sentinel
value, which takes the non-null path through identical code.
"""

import polars as pl
import pytest

import pyfia
from pyfia import FIA
from pyfia.estimation.estimators.carbon_flux import carbon_flux
from pyfia.estimation.estimators.carbon_pools import carbon_pool
from pyfia.estimation.variance import calculate_grouped_domain_total_variance

VOL_EVALID = 12401
GRM_EVALID = 12403
# Null on 146 of the 509 forest conditions in EVALID 12401.
GROUP = "STAND_STRUCTURE_SRS"
SENTINEL = 99


def _run(db_path, evalid, estimate, recode_null=False):
    with FIA(str(db_path)) as db:
        db.clip_by_evalid(evalid)
        if recode_null:
            db.load_table("COND")
            db.tables["COND"] = db.tables["COND"].with_columns(
                pl.col(GROUP).fill_null(SENTINEL)
            )
        return estimate(db)


def _only_row(df: pl.DataFrame) -> dict:
    assert df.height == 1
    return df.row(0, named=True)


def _assert_same(actual: dict, expected: dict, columns: list[str]) -> None:
    for col in columns:
        assert actual[col] is not None, col
        if col.startswith("N_"):
            assert actual[col] == expected[col], col
        else:
            assert actual[col] == pytest.approx(expected[col], rel=1e-9), col


def test_area_null_owner_group(fiadb_fixture_path):
    grouped = _run(
        fiadb_fixture_path,
        VOL_EVALID,
        lambda db: pyfia.area(db, land_type="all", grp_by="OWNGRPCD"),
    )
    domain = _run(
        fiadb_fixture_path,
        VOL_EVALID,
        lambda db: pyfia.area(db, land_type="all", area_domain="OWNGRPCD IS NULL"),
    )
    null_row = _only_row(grouped.filter(pl.col("OWNGRPCD").is_null()))
    _assert_same(null_row, _only_row(domain), ["AREA", "AREA_SE", "N_PLOTS"])
    assert null_row["AREA_SE"] > 0


@pytest.mark.parametrize(
    ("estimator", "columns"),
    [
        (pyfia.tpa, ["TPA", "TPA_SE", "BAA_SE", "N_PLOTS"]),
        (
            pyfia.volume,
            ["VOLCFNET_TOTAL", "VOLCFNET_TOTAL_SE", "VOLCFNET_ACRE_SE", "N_PLOTS"],
        ),
        (pyfia.biomass, ["BIO_TOTAL", "BIO_TOTAL_SE", "BIO_ACRE_SE", "N_PLOTS"]),
        (carbon_pool, ["CARBON_TOTAL", "CARBON_TOTAL_SE", "CARBON_ACRE_SE", "N_PLOTS"]),
    ],
    ids=["tpa", "volume", "biomass", "carbon_pool"],
)
def test_null_group_matches_domain_estimate(fiadb_fixture_path, estimator, columns):
    grouped = _run(
        fiadb_fixture_path, VOL_EVALID, lambda db: estimator(db, grp_by=GROUP)
    )
    domain = _run(
        fiadb_fixture_path,
        VOL_EVALID,
        lambda db: estimator(db, area_domain=f"{GROUP} IS NULL"),
    )
    null_row = _only_row(grouped.filter(pl.col(GROUP).is_null()))
    _assert_same(null_row, _only_row(domain), columns)


@pytest.mark.parametrize(
    ("evalid", "estimator", "columns"),
    [
        (VOL_EVALID, pyfia.site_index, ["SI_MEAN", "SI_SE", "N_PLOTS"]),
        (
            GRM_EVALID,
            carbon_flux,
            [
                "AREA_TOTAL",
                "NET_CARBON_FLUX_TOTAL",
                "NET_CARBON_FLUX_TOTAL_SE",
                "NET_CARBON_FLUX_ACRE_SE",
                "N_PLOTS",
            ],
        ),
    ],
    ids=["site_index", "carbon_flux"],
)
def test_null_group_matches_recoded_group(
    fiadb_fixture_path, evalid, estimator, columns
):
    grouped = _run(fiadb_fixture_path, evalid, lambda db: estimator(db, grp_by=GROUP))
    recoded = _run(
        fiadb_fixture_path,
        evalid,
        lambda db: estimator(db, grp_by=GROUP),
        recode_null=True,
    )
    assert recoded.filter(pl.col(GROUP).is_null()).height == 0
    _assert_same(
        _only_row(grouped.filter(pl.col(GROUP).is_null())),
        _only_row(recoded.filter(pl.col(GROUP) == SENTINEL)),
        columns,
    )


def _plot_frame(bp_columns: bool) -> pl.DataFrame:
    """Two estimation units, four strata, three groups (one null), every plot
    present in every group as domain estimation requires."""
    rows = []
    for plot in range(48):
        eu = plot % 2
        stratum = plot % 4
        for group in (1, 2, None):
            share = ((plot * 7 + (group or 5) * 3) % 11) / 10
            row = {
                "PLT_CN": plot,
                "GROUP": group,
                "STRATUM_CN": stratum,
                "EXPNS": 1000.0 + 250.0 * stratum,
                "y_i": share * (1 + plot % 3),
                "x_i": share,
            }
            if bp_columns:
                row |= {
                    "ESTN_UNIT_CN": eu,
                    "STRATUM_WGT": (0.2, 0.3, 0.3, 0.2)[stratum],
                    "AREA_USED": 50_000.0 + 10_000.0 * eu,
                    "P2POINTCNT": 12,
                }
            rows.append(row)
    return pl.DataFrame(rows)


@pytest.mark.parametrize("bp_columns", [True, False], ids=["exact", "simplified"])
def test_grouped_variance_null_group(bp_columns):
    plots = _plot_frame(bp_columns)
    recoded = plots.with_columns(pl.col("GROUP").fill_null(SENTINEL))

    result = calculate_grouped_domain_total_variance(plots, ["GROUP"], "y_i", "x_i")
    expected = calculate_grouped_domain_total_variance(recoded, ["GROUP"], "y_i", "x_i")

    stats = ["se_total", "se_acre", "variance_total", "variance_acre"]
    null_row = _only_row(result.filter(pl.col("GROUP").is_null()))
    _assert_same(
        null_row, _only_row(expected.filter(pl.col("GROUP") == SENTINEL)), stats
    )
    assert null_row["se_total"] > 0
    assert null_row["se_acre"] > 0
