"""condition_intervals() on the committed Alabama FIADB subset.

The oracle is area_change(), which matches EVALIDator: expanding each pair's
CHNG_AREA_SHARE by the plot's EXPNS and the adjustment factor of the time-2
condition's footprint, over pairs sampled at both times, reproduces its gross
loss (time-1 forest, time-2 nonforest or water) and gross gain.
"""

from __future__ import annotations

import duckdb
import polars as pl
import pytest

from pyfia import FIA, area_change
from pyfia.intervals import condition_intervals

EVALID_CHNG = 12403


@pytest.fixture(scope="module")
def strata(fiadb_fixture_path) -> pl.DataFrame:
    with duckdb.connect(str(fiadb_fixture_path), read_only=True) as con:
        return con.sql(
            f"""
            SELECT a.PLT_CN, s.EXPNS, s.ADJ_FACTOR_SUBP, s.ADJ_FACTOR_MACR
            FROM POP_PLOT_STRATUM_ASSGN a
            JOIN POP_STRATUM s ON s.CN = a.STRATUM_CN
            WHERE a.EVALID = {EVALID_CHNG}
            """
        ).pl()


def intervals(db_path, **kwargs) -> pl.DataFrame:
    with FIA(str(db_path)) as db:
        db.clip_by_evalid(EVALID_CHNG)
        return condition_intervals(db, **kwargs)


def expanded(pairs: pl.DataFrame, strata: pl.DataFrame, annual: bool) -> float:
    """Expand pairs as EVALIDator does: sampled at both times, footprint ADJ."""
    sampled = (pl.col("t1_COND_NONSAMPLE_REASN_CD").fill_null(0) == 0) & (
        pl.col("t2_COND_NONSAMPLE_REASN_CD").fill_null(0) == 0
    )
    rows = pairs.filter(
        sampled
        & pl.col("t2_PROP_BASIS").is_in(["SUBP", "MACR"])
        & pl.col("t2_CONDPROP_UNADJ").is_not_null()
        & (pl.col("LINK_METHOD") == "change_matrix")
    ).join(
        strata.with_columns(pl.col("PLT_CN").cast(pairs.schema["PLT_CN"])),
        on="PLT_CN",
    )
    adj = (
        pl.when(pl.col("t2_PROP_BASIS") == "MACR")
        .then(pl.col("ADJ_FACTOR_MACR"))
        .otherwise(pl.col("ADJ_FACTOR_SUBP"))
    )
    value = pl.col("CHNG_AREA_SHARE") * adj * pl.col("EXPNS")
    if annual:
        value = value / pl.col("REMPER")
    return rows.select(value.sum()).item()


def area_change_total(db_path, change_type: str, annual: bool) -> float:
    with FIA(str(db_path)) as db:
        db.clip_by_evalid(EVALID_CHNG)
        return area_change(db, change_type=change_type, annual=annual)[
            "AREA_CHANGE_TOTAL"
        ][0]


@pytest.mark.parametrize("annual", [True, False], ids=["annual", "period"])
class TestAreaChangeIdentity:
    def test_forest_losses_reproduce_gross_loss(
        self, fiadb_fixture_path, strata, annual
    ):
        pairs = intervals(fiadb_fixture_path)
        losses = pairs.filter(pl.col("t2_OUTCOME").is_in(["nonforest", "water"]))
        assert losses.height > 0
        assert expanded(losses, strata, annual) == pytest.approx(
            area_change_total(fiadb_fixture_path, "gross_loss", annual), rel=1e-9
        )

    def test_gains_reproduce_gross_gain(self, fiadb_fixture_path, strata, annual):
        pairs = intervals(fiadb_fixture_path, at_risk_land="all")
        gains = pairs.filter(
            (pl.col("t1_COND_STATUS_CD") != 1) & (pl.col("t2_OUTCOME") == "forest")
        )
        assert gains.height > 0
        assert expanded(gains, strata, annual) == pytest.approx(
            area_change_total(fiadb_fixture_path, "gross_gain", annual), rel=1e-9
        )

    def test_timberland_losses(self, fiadb_fixture_path, strata, annual):
        pairs = intervals(fiadb_fixture_path, at_risk_land="timber")
        timber_t2 = (
            (pl.col("t2_COND_STATUS_CD") == 1)
            & pl.col("t2_SITECLCD").is_in([1, 2, 3, 4, 5, 6])
            & (pl.col("t2_RESERVCD") == 0)
        ).fill_null(False)
        losses = pairs.filter(~timber_t2 & (pl.col("t2_OUTCOME") != "no_t2_condition"))
        with FIA(str(fiadb_fixture_path)) as db:
            db.clip_by_evalid(EVALID_CHNG)
            expected = area_change(
                db, land_type="timber", change_type="gross_loss", annual=annual
            )["AREA_CHANGE_TOTAL"][0]
        assert expanded(losses, strata, annual) == pytest.approx(expected, rel=1e-9)


class TestStructure:
    def test_every_previous_plot_resolves(self, fiadb_fixture_path):
        pairs = intervals(fiadb_fixture_path, at_risk_land="all")
        assert pairs["t1_INVYR"].null_count() == 0

    def test_at_risk_set_is_defined_at_time_1(self, fiadb_fixture_path):
        pairs = intervals(fiadb_fixture_path)
        assert (pairs["t1_COND_STATUS_CD"] == 1).all()
        assert set(pairs["t2_OUTCOME"]) <= {
            "forest",
            "nonforest",
            "water",
            "nonsampled",
            "no_t2_condition",
        }

    def test_every_at_risk_condition_appears(self, fiadb_fixture_path):
        """Each time-1 forest condition on an evaluation plot has a row."""
        with duckdb.connect(str(fiadb_fixture_path), read_only=True) as con:
            expected = con.sql(
                f"""
                SELECT count(*) FROM COND c
                JOIN PLOT p ON p.PREV_PLT_CN = c.PLT_CN
                WHERE c.COND_STATUS_CD = 1 AND p.CN IN (
                  SELECT PLT_CN FROM POP_PLOT_STRATUM_ASSGN WHERE EVALID = {EVALID_CHNG})
                """
            ).fetchone()[0]
        pairs = intervals(fiadb_fixture_path)
        assert pairs.select("PREV_PLT_CN", "t1_CONDID").unique().height == expected

    def test_shares_of_a_time_1_condition_sum_to_its_proportion(
        self, fiadb_fixture_path
    ):
        """Across its time-2 pairs, a time-1 condition's area shares sum to its
        subplot-based proportion (CONDPROP_UNADJ where PROP_BASIS is SUBP)."""
        pairs = intervals(fiadb_fixture_path, at_risk_land="all").filter(
            (pl.col("LINK_METHOD") == "change_matrix")
            & (pl.col("t1_PROP_BASIS") == "SUBP")
            & (pl.col("t2_PROP_BASIS") == "SUBP")
        )
        per_t1 = pairs.group_by("PLT_CN", "PREV_PLT_CN", "t1_CONDID").agg(
            pl.col("CHNG_AREA_SHARE").sum(),
            pl.col("t1_SUBPPROP_UNADJ").first(),
        )
        close = (per_t1["CHNG_AREA_SHARE"] - per_t1["t1_SUBPPROP_UNADJ"]).abs() < 1e-3
        assert close.mean() > 0.99

    def test_same_condid_fallback_outside_the_change_matrix(self, fiadb_fixture_path):
        with FIA(str(fiadb_fixture_path)) as db:
            pairs = condition_intervals(db, at_risk_land="all")
        fallback = pairs.filter(pl.col("LINK_METHOD") == "same_condid")
        assert fallback.height > 0
        linked = fallback.filter(pl.col("t2_CONDID").is_not_null())
        assert (linked["t1_CONDID"] == linked["t2_CONDID"]).all()
        assert fallback["CHNG_AREA_SHARE"].null_count() == fallback.height

    def test_change_matrix_false_uses_the_fallback_everywhere(self, fiadb_fixture_path):
        pairs = intervals(fiadb_fixture_path, change_matrix=False)
        assert set(pairs["LINK_METHOD"]) == {"same_condid"}

    def test_remper_and_invyr_bounds(self, fiadb_fixture_path):
        pairs = intervals(fiadb_fixture_path, min_remper=5.5, max_remper=7.0)
        assert pairs["REMPER"].is_between(5.5, 7.0).all()
        later = intervals(fiadb_fixture_path, min_invyr=2020)
        assert (later["t2_INVYR"] >= 2020).all()

    def test_extra_columns_at_both_times(self, fiadb_fixture_path):
        pairs = intervals(fiadb_fixture_path, columns=["ALSTKCD"])
        assert {"t1_ALSTKCD", "t2_ALSTKCD"} <= set(pairs.columns)

    def test_unknown_column_raises(self, fiadb_fixture_path):
        with pytest.raises(ValueError, match="NOT_A_COLUMN"):
            intervals(fiadb_fixture_path, columns=["NOT_A_COLUMN"])
