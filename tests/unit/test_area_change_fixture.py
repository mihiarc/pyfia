"""area_change() on the committed Alabama FIADB subset, checked against the
SQL EVALIDator runs for its area change estimates (snum 127/128, 136/137).

The oracle below is that SQL, restricted to the fixture's change evaluation.
EVALIDator's "forest at either measurement" minus "forest at both" is the area
that was forest at exactly one measurement, so it splits into pyFIA's
gross_gain and gross_loss.
"""

from __future__ import annotations

import duckdb
import polars as pl
import pytest

from pyfia import FIA, area_change

EVALID_CHNG = 12403

STATUS = {
    "both": "cond.COND_STATUS_CD = 1 AND pcond.COND_STATUS_CD = 1",
    "either": "cond.COND_STATUS_CD = 1 OR pcond.COND_STATUS_CD = 1",
    "gain": "cond.COND_STATUS_CD = 1 AND pcond.COND_STATUS_CD <> 1",
    "loss": "cond.COND_STATUS_CD <> 1 AND pcond.COND_STATUS_CD = 1",
}


def plot_change_sql(status: str, annual: bool, domain: str = "TRUE") -> str:
    """Adjusted, unexpanded change per plot (PLT_CN, EXPNS, v), as EVALIDator
    computes it. ``domain`` is extra SQL on the current condition (``cond``)."""
    per_year = "/ plot.REMPER" if annual else ""
    return f"""
          SELECT plot.CN AS PLT_CN, ps.EXPNS,
            SUM(COALESCE(sccm.SUBPTYP_PROP_CHNG / 4 *
                CASE cond.PROP_BASIS WHEN 'MACR' THEN ps.ADJ_FACTOR_MACR
                                     ELSE ps.ADJ_FACTOR_SUBP END, 0) {per_year}) AS v
          FROM POP_STRATUM ps
          JOIN POP_PLOT_STRATUM_ASSGN a ON a.STRATUM_CN = ps.CN
          JOIN PLOT plot ON a.PLT_CN = plot.CN
          JOIN COND cond ON cond.PLT_CN = plot.CN
          JOIN COND pcond ON pcond.PLT_CN = plot.PREV_PLT_CN
          JOIN SUBP_COND_CHNG_MTRX sccm
            ON sccm.PLT_CN = cond.PLT_CN AND sccm.PREV_PLT_CN = pcond.PLT_CN
           AND sccm.CONDID = cond.CONDID AND sccm.PREVCOND = pcond.CONDID
          WHERE cond.CONDPROP_UNADJ IS NOT NULL
            AND ((sccm.SUBPTYP = 3 AND cond.PROP_BASIS = 'MACR')
              OR (sccm.SUBPTYP = 1 AND cond.PROP_BASIS = 'SUBP'))
            AND COALESCE(cond.COND_NONSAMPLE_REASN_CD, 0) = 0
            AND COALESCE(pcond.COND_NONSAMPLE_REASN_CD, 0) = 0
            AND ({STATUS[status]})
            AND ({domain})
            AND ps.EVALID = {EVALID_CHNG}
          GROUP BY plot.CN, ps.EXPNS
    """


def evalidator_change_total(
    db_path, status: str, annual: bool, domain: str = "TRUE"
) -> float:
    """Expanded area change total, as EVALIDator's SQL computes it."""
    sql = f"SELECT SUM(v * EXPNS) FROM ({plot_change_sql(status, annual, domain)})"
    with duckdb.connect(str(db_path), read_only=True) as con:
        return con.sql(sql).fetchone()[0]


def bechtold_patterson_se(
    db_path, status: str, annual: bool, domain: str = "TRUE"
) -> float:
    """SE of the change total by Bechtold & Patterson's post-stratified domain
    total variance, written out independently of pyFIA:

        V = sum_EU A^2 [ (1/n) sum_h W_h s2_h + (1/n^2) sum_h (1 - W_h) s2_h ]

    over every plot in the evaluation, with zero change where a plot has none.
    """
    sql = f"""
        WITH y AS ({plot_change_sql(status, annual, domain)}),
        plots AS (
          SELECT ps.ESTN_UNIT_CN AS eu, ps.CN AS h, eu.AREA_USED AS A,
                 ps.P1POINTCNT * 1.0 / eu.P1PNTCNT_EU AS W, COALESCE(y.v, 0) AS v
          FROM POP_PLOT_STRATUM_ASSGN a
          JOIN POP_STRATUM ps ON a.STRATUM_CN = ps.CN
          JOIN POP_ESTN_UNIT eu ON eu.CN = ps.ESTN_UNIT_CN
          LEFT JOIN y ON y.PLT_CN = a.PLT_CN
          WHERE ps.EVALID = {EVALID_CHNG}
        ),
        strata AS (
          SELECT eu, h, ANY_VALUE(A) AS A, ANY_VALUE(W) AS W, COUNT(*) AS n_h,
                 COALESCE(VAR_SAMP(v), 0) AS s2
          FROM plots GROUP BY eu, h
        ),
        units AS (
          SELECT ANY_VALUE(A) AS A, SUM(n_h) AS n,
                 SUM(W * s2) AS sw, SUM((1 - W) * s2) AS s1w
          FROM strata GROUP BY eu
        )
        SELECT SQRT(SUM(A * A / n * sw + A * A / (n * n) * s1w)) FROM units
    """
    with duckdb.connect(str(db_path), read_only=True) as con:
        return con.sql(sql).fetchone()[0]


def pyfia_change_total(
    db_path, change_type: str, annual: bool, area_domain: str | None = None
) -> float:
    with FIA(str(db_path)) as db:
        db.clip_by_evalid(EVALID_CHNG)
        result = area_change(
            db, change_type=change_type, annual=annual, area_domain=area_domain
        )
    return result["AREA_CHANGE_TOTAL"][0]


@pytest.mark.parametrize("annual", [True, False], ids=["annual", "period"])
class TestAreaChangeMatchesEvalidatorSQL:
    """Each subplot transition counts once, on the condition's footprint (#151)."""

    def test_gross_gain(self, fiadb_fixture_path, annual):
        expected = evalidator_change_total(fiadb_fixture_path, "gain", annual)
        assert expected > 0
        assert pyfia_change_total(
            fiadb_fixture_path, "gross_gain", annual
        ) == pytest.approx(expected, rel=1e-9)

    def test_gross_loss(self, fiadb_fixture_path, annual):
        expected = evalidator_change_total(fiadb_fixture_path, "loss", annual)
        assert expected > 0
        assert pyfia_change_total(
            fiadb_fixture_path, "gross_loss", annual
        ) == pytest.approx(expected, rel=1e-9)

    def test_net(self, fiadb_fixture_path, annual):
        expected = evalidator_change_total(
            fiadb_fixture_path, "gain", annual
        ) - evalidator_change_total(fiadb_fixture_path, "loss", annual)
        assert pyfia_change_total(fiadb_fixture_path, "net", annual) == pytest.approx(
            expected, rel=1e-9
        )

    def test_gain_plus_loss_is_either_minus_both(self, fiadb_fixture_path, annual):
        """The comparison the EVALIDator validation test makes (snum 137 - 136)."""
        transition_area = evalidator_change_total(
            fiadb_fixture_path, "either", annual
        ) - evalidator_change_total(fiadb_fixture_path, "both", annual)
        gain = pyfia_change_total(fiadb_fixture_path, "gross_gain", annual)
        loss = pyfia_change_total(fiadb_fixture_path, "gross_loss", annual)
        assert gain + loss == pytest.approx(transition_area, rel=1e-9)


class TestAreaDomain:
    """area_domain filters the current (time-2) condition (#148)."""

    @pytest.mark.parametrize("change_type", ["gross_gain", "gross_loss"])
    def test_counties_partition_the_change(self, fiadb_fixture_path, change_type):
        status = "gain" if change_type == "gross_gain" else "loss"
        by_county = []
        for countycd in (25, 125, 129):
            expected = evalidator_change_total(
                fiadb_fixture_path, status, True, domain=f"cond.COUNTYCD = {countycd}"
            )
            got = pyfia_change_total(
                fiadb_fixture_path,
                change_type,
                True,
                area_domain=f"COUNTYCD == {countycd}",
            )
            assert got == pytest.approx(expected or 0.0, rel=1e-9, abs=1e-9)
            by_county.append(got)
        overall = pyfia_change_total(fiadb_fixture_path, change_type, True)
        assert max(by_county) < overall
        assert sum(by_county) == pytest.approx(overall, rel=1e-9)

    def test_forest_only_attribute_drops_losses(self, fiadb_fixture_path):
        """OWNGRPCD is null on the fixture's nonforest conditions (documented)."""
        assert pyfia_change_total(fiadb_fixture_path, "gross_loss", True) > 0
        assert (
            pyfia_change_total(
                fiadb_fixture_path, "gross_loss", True, area_domain="OWNGRPCD == 40"
            )
            == 0
        )

    def test_domain_on_a_column_not_otherwise_loaded(self, fiadb_fixture_path):
        expected = evalidator_change_total(
            fiadb_fixture_path, "gain", False, domain="cond.STDORGCD = 1"
        )
        assert expected > 0
        assert pyfia_change_total(
            fiadb_fixture_path, "gross_gain", False, area_domain="STDORGCD == 1"
        ) == pytest.approx(expected, rel=1e-9)

    def test_status_domain_is_the_current_status(self, fiadb_fixture_path):
        """Losses end on nonforest land, so a current-forest domain has none."""
        assert pyfia_change_total(fiadb_fixture_path, "gross_loss", True) > 0
        assert (
            pyfia_change_total(
                fiadb_fixture_path,
                "gross_loss",
                True,
                area_domain="COND_STATUS_CD == 1",
            )
            == 0
        )
        assert pyfia_change_total(
            fiadb_fixture_path, "gross_gain", True, area_domain="COND_STATUS_CD == 1"
        ) == pytest.approx(pyfia_change_total(fiadb_fixture_path, "gross_gain", True))


class TestAreaChangeSE:
    """AREA_CHANGE_SE uses unexpanded plot values over every plot (#147)."""

    @pytest.mark.parametrize("annual", [True, False], ids=["annual", "period"])
    @pytest.mark.parametrize(
        "change_type,status", [("gross_gain", "gain"), ("gross_loss", "loss")]
    )
    def test_se_matches_bechtold_patterson(
        self, fiadb_fixture_path, change_type, status, annual
    ):
        expected = bechtold_patterson_se(fiadb_fixture_path, status, annual)
        with FIA(str(fiadb_fixture_path)) as db:
            db.clip_by_evalid(EVALID_CHNG)
            result = area_change(db, change_type=change_type, annual=annual)
        assert expected > 0
        assert result["AREA_CHANGE_SE"][0] == pytest.approx(expected, rel=1e-9)

    def test_grouped_se_per_county(self, fiadb_fixture_path):
        with FIA(str(fiadb_fixture_path)) as db:
            db.clip_by_evalid(EVALID_CHNG)
            result = area_change(db, change_type="gross_loss", grp_by="COUNTYCD")
        assert result.height == 3
        for countycd, se in result.select("COUNTYCD", "AREA_CHANGE_SE").rows():
            expected = bechtold_patterson_se(
                fiadb_fixture_path, "loss", True, domain=f"cond.COUNTYCD = {countycd}"
            )
            assert se == pytest.approx(expected, rel=1e-9)

    def test_null_group_keeps_its_se(self, fiadb_fixture_path):
        """Losses end on nonforest land, whose OWNGRPCD is null on the fixture."""
        with FIA(str(fiadb_fixture_path)) as db:
            db.clip_by_evalid(EVALID_CHNG)
            result = area_change(db, change_type="net", grp_by="OWNGRPCD")
        null_group = result.filter(pl.col("OWNGRPCD").is_null())
        assert null_group.height == 1
        expected = bechtold_patterson_se(
            fiadb_fixture_path, "loss", True, domain="cond.OWNGRPCD IS NULL"
        )
        assert null_group["AREA_CHANGE_SE"][0] == pytest.approx(expected, rel=1e-9)
