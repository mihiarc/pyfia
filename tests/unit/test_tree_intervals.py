"""tree_intervals() on the committed Alabama FIADB subset.

The oracles are the GRM estimators: over the change evaluation's plots, each
row's weight times the adjustment factor of its SUBPTYP_GRM times the plot's
EXPNS times a TREE_GRM_MIDPT value sums to removals() (CUT and DIVERSION
rows) and mortality() (MORTALITY rows).
"""

from __future__ import annotations

import duckdb
import polars as pl
import pytest

from pyfia import FIA, mortality, removals
from pyfia.intervals import tree_intervals

EVALID_GRM = 12403


@pytest.fixture(scope="module")
def strata(fiadb_fixture_path) -> pl.DataFrame:
    with duckdb.connect(str(fiadb_fixture_path), read_only=True) as con:
        return (
            con.sql(
                f"""
                SELECT a.PLT_CN, s.EXPNS,
                       s.ADJ_FACTOR_SUBP, s.ADJ_FACTOR_MICR, s.ADJ_FACTOR_MACR
                FROM POP_PLOT_STRATUM_ASSGN a
                JOIN POP_STRATUM s ON s.CN = a.STRATUM_CN
                WHERE a.EVALID = {EVALID_GRM}
                """
            )
            .pl()
            .with_columns(pl.col("PLT_CN").cast(pl.Utf8))
        )


def intervals(db_path, **kwargs) -> pl.DataFrame:
    with FIA(str(db_path)) as db:
        db.clip_by_evalid(EVALID_GRM)
        return tree_intervals(db, **kwargs)


def expanded(
    rows: pl.DataFrame, strata: pl.DataFrame, weight: str, value: str | None
) -> float:
    adj = (
        pl.when(pl.col("SUBPTYP_GRM") == 1)
        .then(pl.col("ADJ_FACTOR_SUBP"))
        .when(pl.col("SUBPTYP_GRM") == 2)
        .then(pl.col("ADJ_FACTOR_MICR"))
        .when(pl.col("SUBPTYP_GRM") == 3)
        .then(pl.col("ADJ_FACTOR_MACR"))
        .otherwise(0.0)
    )
    amount = pl.col(value).fill_null(0.0) if value else pl.lit(1.0)
    return (
        rows.filter(pl.col(weight) > 0)
        .join(strata, on="PLT_CN")
        .select((pl.col(weight) * adj * pl.col("EXPNS") * amount).sum())
        .item()
    )


def gs_size(rows: pl.DataFrame, tree_basis: str) -> pl.DataFrame:
    """The GRM estimators keep growing-stock trees of 5 inches and larger."""
    return rows.filter(pl.col("DIA_MIDPT") >= 5) if tree_basis == "gs" else rows


@pytest.mark.parametrize("tree_basis", ["gs", "al"])
@pytest.mark.parametrize("land_basis", ["forest", "timber"])
class TestEstimatorIdentity:
    @pytest.mark.parametrize(
        "measure,value", [("volume", "MIDPT_VOLCFNET"), ("tpa", None)]
    )
    def test_removals(
        self, fiadb_fixture_path, strata, tree_basis, land_basis, measure, value
    ):
        rows = intervals(
            fiadb_fixture_path,
            tree_basis=tree_basis,
            land_basis=land_basis,
            components=["CUT", "DIVERSION"],
        )
        assert rows.height > 0
        with FIA(str(fiadb_fixture_path)) as db:
            db.clip_by_evalid(EVALID_GRM)
            expected = removals(
                db, tree_type=tree_basis, land_type=land_basis, measure=measure
            )["REMOVALS_TOTAL"][0]
        got = expanded(gs_size(rows, tree_basis), strata, "TPAREMV_UNADJ", value)
        assert got == pytest.approx(expected, rel=1e-9)

    def test_mortality(self, fiadb_fixture_path, strata, tree_basis, land_basis):
        rows = intervals(
            fiadb_fixture_path,
            tree_basis=tree_basis,
            land_basis=land_basis,
            components=["MORTALITY"],
        )
        assert rows.height > 0
        with FIA(str(fiadb_fixture_path)) as db:
            db.clip_by_evalid(EVALID_GRM)
            expected = mortality(
                db, tree_type=tree_basis, land_type=land_basis, measure="volume"
            )["MORT_TOTAL"][0]
        got = expanded(
            gs_size(rows, tree_basis), strata, "TPAMORT_UNADJ", "MIDPT_VOLCFNET"
        )
        assert got == pytest.approx(expected, rel=1e-9)


class TestWeights:
    @pytest.mark.parametrize(
        "fates,rate",
        [(["cut", "diversion"], "TPAREMV_UNADJ"), (["mortality"], "TPAMORT_UNADJ")],
    )
    def test_annual_rate_times_remper_is_the_interval_weight(
        self, fiadb_fixture_path, fates, rate
    ):
        rows = intervals(fiadb_fixture_path).filter(pl.col("FATE").is_in(fates))
        assert rows.height > 0
        ratio = rows[rate] * rows["REMPER"] / rows["TPAGROW_UNADJ"]
        assert ((ratio - 1).abs() < 5e-6).all()

    def test_every_component_carries_an_interval_weight(self, fiadb_fixture_path):
        rows = intervals(fiadb_fixture_path)
        assert rows["TPAGROW_UNADJ"].null_count() == 0


class TestTreeAccounting:
    """Within a GRM evaluation, time-1 live trees on forest land and the
    SURVIVOR, CUT, MORTALITY and DIVERSION rows correspond one to one, except
    for trees FIA reconciled as missed at time 1 and a few without a GRM row."""

    @pytest.fixture(scope="class")
    def at_risk(self, fiadb_fixture_path) -> pl.DataFrame:
        with duckdb.connect(str(fiadb_fixture_path), read_only=True) as con:
            return (
                con.sql(
                    f"""
                    SELECT t.CN AS PREV_TRE_CN
                    FROM PLOT p
                    JOIN TREE t ON t.PLT_CN = p.PREV_PLT_CN
                    JOIN COND c ON c.PLT_CN = t.PLT_CN AND c.CONDID = t.CONDID
                    WHERE p.CN IN (SELECT PLT_CN FROM POP_PLOT_STRATUM_ASSGN
                                   WHERE EVALID = {EVALID_GRM})
                      AND t.STATUSCD = 1 AND t.DIA >= 1 AND c.COND_STATUS_CD = 1
                    """
                )
                .pl()
                .with_columns(pl.col("PREV_TRE_CN").cast(pl.Utf8))
            )

    @pytest.fixture(scope="class")
    def fates(self, fiadb_fixture_path) -> pl.DataFrame:
        return intervals(fiadb_fixture_path, tree_basis="al").filter(
            pl.col("FATE").is_in(["survivor", "cut", "mortality", "diversion"])
        )

    def test_each_at_risk_tree_has_one_fate(self, at_risk, fates):
        linked = at_risk.join(fates, on="PREV_TRE_CN", how="left")
        assert linked.height == at_risk.height  # never more than one fate
        assert linked["TRE_CN"].null_count() / at_risk.height < 0.005

    def test_each_fate_follows_an_at_risk_tree(self, at_risk, fates):
        with_prev = fates.filter(pl.col("PREV_TRE_CN").is_not_null())
        unmatched = with_prev.join(at_risk, on="PREV_TRE_CN", how="anti")
        assert unmatched.height / fates.height < 0.001
        assert fates["PREV_TRE_CN"].null_count() / fates.height < 0.005


class TestStructure:
    def test_keys(self, fiadb_fixture_path):
        rows = intervals(fiadb_fixture_path)
        assert rows["TRE_CN"].is_unique().all()
        assert rows.schema["PLT_CN"] == pl.Utf8
        assert rows.schema["t2_CONDID"] == pl.Int64
        assert rows["PREV_PLT_CN"].null_count() == 0
        assert rows["t1_CONDID"].null_count() == 0

    def test_fates(self, fiadb_fixture_path):
        rows = intervals(fiadb_fixture_path)
        assert set(rows["FATE"]) <= {
            "survivor",
            "ingrowth",
            "cut",
            "mortality",
            "diversion",
            "reversion",
        }
        assert "NOT USED" not in set(rows["COMPONENT"])

    def test_components_filter(self, fiadb_fixture_path):
        rows = intervals(fiadb_fixture_path, components=["CUT"])
        assert set(rows["FATE"]) == {"cut"}

    def test_time_1_attributes(self, fiadb_fixture_path):
        rows = intervals(fiadb_fixture_path)
        survivors = rows.filter(
            (pl.col("FATE") == "survivor") & pl.col("PREV_TRE_CN").is_not_null()
        )
        # FIADB has the odd survivor whose time-1 record isn't live
        assert (survivors["t1_STATUSCD"] == 1).mean() > 0.999
        assert (survivors["t1_SPCD"] == survivors["t2_SPCD"]).mean() > 0.99
        lean = intervals(fiadb_fixture_path, t1_attributes=False)
        assert "t1_SPCD" not in lean.columns

    def test_extra_columns(self, fiadb_fixture_path):
        rows = intervals(fiadb_fixture_path, columns=["CR"])
        assert {"t1_CR", "t2_CR"} <= set(rows.columns)
        with pytest.raises(ValueError, match="NOT_A_COLUMN"):
            intervals(fiadb_fixture_path, columns=["NOT_A_COLUMN"])

    def test_basis_validation(self, fiadb_fixture_path):
        with pytest.raises(ValueError, match="tree_basis"):
            intervals(fiadb_fixture_path, tree_basis="live")
        with pytest.raises(ValueError, match="land_basis"):
            intervals(fiadb_fixture_path, land_basis="all")
