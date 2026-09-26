"""condition_stand_metrics() on the committed Alabama FIADB subset.

Oracles:
- basal area per acre of condition equals FIADB's COND.BALIVE;
- summed back over a plot's conditions with each footprint's proportion,
  every additive metric equals the plot's per-acre sum taken straight from
  TREE;
- every metric equals an independent SQL implementation of the same
  normalization;
- forest conditions without qualifying trees come back as zeros.
"""

from __future__ import annotations

import duckdb
import polars as pl
import pytest

from pyfia import FIA, condition_stand_metrics
from pyfia.stand_metrics import METRIC_COLUMNS, METRICS

KEYS = ["PLT_CN", "CONDID"]


def sql_frame(db_path, sql: str) -> pl.DataFrame:
    with duckdb.connect(str(db_path), read_only=True) as con:
        return con.sql(sql).pl()


def stand_metrics(db_path, **kwargs) -> pl.DataFrame:
    with FIA(str(db_path)) as db:
        return condition_stand_metrics(db, **kwargs)


def oracle(db_path, min_dia: float = 1.0, gs: bool = False) -> pl.DataFrame:
    """Per-condition metrics written directly in SQL."""
    gs_clause = "AND t.TREECLCD = 2" if gs else ""
    return sql_frame(
        db_path,
        f"""
        WITH w AS (
          SELECT CAST(t.PLT_CN AS VARCHAR) AS PLT_CN, t.CONDID, t.DIA,
                 t.TPA_UNADJ / (CASE
                   WHEN t.DIA < 5 THEN c.MICRPROP_UNADJ
                   WHEN t.DIA >= TRY_CAST(p.MACRO_BREAKPOINT_DIA AS DOUBLE)
                     THEN TRY_CAST(c.MACRPROP_UNADJ AS DOUBLE)
                   ELSE c.SUBPPROP_UNADJ END) AS tpa,
                 t.DRYBIO_AG, t.VOLCFNET, t.VOLCSNET, r.SFTWD_HRDWD
          FROM TREE t
          JOIN COND c ON c.PLT_CN = t.PLT_CN AND c.CONDID = t.CONDID
          JOIN PLOT p ON p.CN = t.PLT_CN
          LEFT JOIN REF_SPECIES r ON r.SPCD = CAST(t.SPCD AS BIGINT)
          WHERE t.STATUSCD = 1 AND t.DIA >= {min_dia} AND t.TPA_UNADJ > 0
            AND c.COND_STATUS_CD = 1 AND c.MICRPROP_UNADJ IS NOT NULL {gs_clause}
        )
        SELECT PLT_CN, CONDID,
               SUM(0.005454154 * DIA * DIA * tpa) AS BAA,
               SUM(tpa) AS TPA,
               SQRT(SUM(DIA * DIA * tpa) / SUM(tpa)) AS QMD,
               SUM(COALESCE(DRYBIO_AG, 0) * tpa) / 2000 AS DRYBIO_AG_ACRE,
               SUM(COALESCE(VOLCFNET, 0) * tpa) AS VOLCFNET_ACRE,
               SUM(COALESCE(VOLCSNET, 0) * tpa) AS VOLCSNET_ACRE,
               SUM(CASE WHEN SFTWD_HRDWD = 'S' THEN 0.005454154 * DIA * DIA * tpa
                        ELSE 0 END) / SUM(0.005454154 * DIA * DIA * tpa)
                 AS SOFTWOOD_BA_SHARE
        FROM w GROUP BY PLT_CN, CONDID
        """,
    )


class TestBasalAreaMatchesBalive:
    def test_every_recent_forest_condition(self, fiadb_fixture_path):
        balive = sql_frame(
            fiadb_fixture_path,
            """
            SELECT CAST(c.PLT_CN AS VARCHAR) AS PLT_CN, c.CONDID, c.BALIVE
            FROM COND c JOIN PLOT p ON p.CN = c.PLT_CN
            WHERE c.COND_STATUS_CD = 1 AND p.INVYR >= 2015 AND c.BALIVE IS NOT NULL
            """,
        )
        stands = stand_metrics(fiadb_fixture_path, metrics=["ba"])
        joined = balive.join(stands, on=KEYS, how="left")
        assert joined.height == balive.height > 700
        assert joined["BAA"].null_count() == 0
        assert (joined["BAA"] - joined["BALIVE"]).abs().max() < 0.5

    def test_condprop_normalization_would_not(self, fiadb_fixture_path):
        """The footprint proportion matters: CONDPROP_UNADJ alone misses."""
        misses = sql_frame(
            fiadb_fixture_path,
            """
            WITH t AS (
              SELECT t.PLT_CN, t.CONDID,
                     SUM(0.005454154 * t.DIA * t.DIA * t.TPA_UNADJ / c.CONDPROP_UNADJ) ba
              FROM TREE t JOIN COND c ON c.PLT_CN = t.PLT_CN AND c.CONDID = t.CONDID
              WHERE t.STATUSCD = 1 AND t.DIA >= 1 GROUP BY 1, 2)
            SELECT COUNT(*) FROM COND c JOIN PLOT p ON p.CN = c.PLT_CN
            JOIN t ON t.PLT_CN = c.PLT_CN AND t.CONDID = c.CONDID
            WHERE c.COND_STATUS_CD = 1 AND p.INVYR >= 2015
              AND ABS(t.ba - c.BALIVE) >= 0.5
            """,
        ).item()
        assert misses > 0


class TestMetricsMatchIndependentSQL:
    @pytest.mark.parametrize(
        "kwargs,oracle_kwargs",
        [
            ({}, {}),
            ({"min_dia": 5}, {"min_dia": 5.0}),
            ({"tree_type": "gs"}, {"gs": True}),
        ],
        ids=["live", "min_dia_5", "growing_stock"],
    )
    def test_all_metrics(self, fiadb_fixture_path, kwargs, oracle_kwargs):
        expected = oracle(fiadb_fixture_path, **oracle_kwargs)
        got = stand_metrics(fiadb_fixture_path, zero_fill=False, **kwargs)
        joined = expected.join(got, on=KEYS, how="left", suffix="_got")
        assert joined.height == expected.height > 1000
        for metric in METRICS:
            column = METRIC_COLUMNS[metric]
            assert joined[f"{column}_got"].to_numpy() == pytest.approx(
                joined[column].to_numpy(), rel=1e-6, nan_ok=True
            ), column

    def test_merchantable_qmd_is_larger(self, fiadb_fixture_path):
        all_trees = stand_metrics(fiadb_fixture_path, metrics=["qmd"], zero_fill=False)
        merch = stand_metrics(
            fiadb_fixture_path, metrics=["qmd"], min_dia=5, zero_fill=False
        )
        joined = all_trees.join(merch, on=KEYS, suffix="_5")
        assert (joined["QMD_5"] >= joined["QMD"] - 1e-9).all()
        assert joined["QMD_5"].min() >= 5


class TestPlotSum:
    """Summed over a plot's conditions with each footprint's proportion,
    a metric equals the plot's per-acre sum straight from TREE."""

    ADDITIVE = ["BAA", "TPA", "DRYBIO_AG_ACRE", "VOLCFNET_ACRE", "VOLCSNET_ACRE"]

    def test_additive_metrics(self, fiadb_fixture_path):
        metrics = ["ba", "tpa", "drybio_ag", "volcfnet", "volcsnet"]
        everything = stand_metrics(fiadb_fixture_path, metrics=metrics)
        merchantable = stand_metrics(fiadb_fixture_path, metrics=metrics, min_dia=5)
        proportions = sql_frame(
            fiadb_fixture_path,
            """
            SELECT CAST(PLT_CN AS VARCHAR) AS PLT_CN, CONDID,
                   MICRPROP_UNADJ, SUBPPROP_UNADJ
            FROM COND WHERE COND_STATUS_CD = 1 AND MICRPROP_UNADJ IS NOT NULL
            """,
        )
        # Saplings sit on the microplot and larger trees on the subplot; the
        # fixture's plots have no macroplot.
        by_plot = (
            everything.join(merchantable, on=KEYS, suffix="_5")
            .join(proportions, on=KEYS)
            .group_by("PLT_CN")
            .agg(
                (
                    (pl.col(m) - pl.col(f"{m}_5")) * pl.col("MICRPROP_UNADJ")
                    + pl.col(f"{m}_5") * pl.col("SUBPPROP_UNADJ")
                )
                .sum()
                .alias(m)
                for m in self.ADDITIVE
            )
        )
        expected = sql_frame(
            fiadb_fixture_path,
            """
            SELECT CAST(t.PLT_CN AS VARCHAR) AS PLT_CN,
                   SUM(0.005454154 * t.DIA * t.DIA * t.TPA_UNADJ) AS BAA,
                   SUM(t.TPA_UNADJ) AS TPA,
                   SUM(COALESCE(t.DRYBIO_AG, 0) * t.TPA_UNADJ) / 2000 AS DRYBIO_AG_ACRE,
                   SUM(COALESCE(t.VOLCFNET, 0) * t.TPA_UNADJ) AS VOLCFNET_ACRE,
                   SUM(COALESCE(t.VOLCSNET, 0) * t.TPA_UNADJ) AS VOLCSNET_ACRE
            FROM TREE t JOIN COND c ON c.PLT_CN = t.PLT_CN AND c.CONDID = t.CONDID
            WHERE t.STATUSCD = 1 AND t.DIA >= 1 AND t.TPA_UNADJ > 0
              AND c.COND_STATUS_CD = 1 AND c.MICRPROP_UNADJ IS NOT NULL
            GROUP BY 1
            """,
        )
        joined = expected.join(by_plot, on="PLT_CN", how="left", suffix="_sum")
        assert joined.height > 1000
        for m in self.ADDITIVE:
            assert joined[f"{m}_sum"].to_numpy() == pytest.approx(
                joined[m].to_numpy(), rel=1e-6, abs=1e-9
            ), m


class TestPeriodicInventories:
    def test_normalized_by_condition_proportion(self, fiadb_fixture_path):
        """Periodic plots record no footprint proportions (1972-1990 here)."""
        expected = sql_frame(
            fiadb_fixture_path,
            """
            SELECT CAST(t.PLT_CN AS VARCHAR) AS PLT_CN, t.CONDID,
                   SUM(t.TPA_UNADJ / c.CONDPROP_UNADJ) AS TPA,
                   SUM(0.005454154 * t.DIA * t.DIA * t.TPA_UNADJ / c.CONDPROP_UNADJ)
                     AS BAA
            FROM TREE t JOIN COND c ON c.PLT_CN = t.PLT_CN AND c.CONDID = t.CONDID
            WHERE t.STATUSCD = 1 AND t.DIA >= 1 AND t.TPA_UNADJ > 0
              AND c.COND_STATUS_CD = 1 AND c.MICRPROP_UNADJ IS NULL
            GROUP BY 1, 2
            """,
        )
        got = stand_metrics(fiadb_fixture_path, metrics=["ba", "tpa"])
        joined = expected.join(got, on=KEYS, how="left", suffix="_got")
        assert joined.height == expected.height > 500
        for column in ["TPA", "BAA"]:
            assert joined[f"{column}_got"].to_numpy() == pytest.approx(
                joined[column].to_numpy(), rel=1e-6
            )


class TestZeroFill:
    def test_forest_conditions_without_trees(self, fiadb_fixture_path):
        empty = sql_frame(
            fiadb_fixture_path,
            """
            SELECT CAST(c.PLT_CN AS VARCHAR) AS PLT_CN, c.CONDID FROM COND c
            WHERE c.COND_STATUS_CD = 1 AND c.CONDPROP_UNADJ IS NOT NULL
              AND NOT EXISTS (SELECT 1 FROM TREE t WHERE t.PLT_CN = c.PLT_CN
                  AND t.CONDID = c.CONDID AND t.STATUSCD = 1 AND t.DIA >= 1
                  AND t.TPA_UNADJ > 0)
            """,
        )
        assert empty.height > 0
        stands = stand_metrics(fiadb_fixture_path)
        rows = empty.join(stands, on=KEYS, how="left")
        assert rows["N_TREES"].to_list() == [0] * empty.height
        for column in [
            "BAA",
            "TPA",
            "DRYBIO_AG_ACRE",
            "VOLCFNET_ACRE",
            "VOLCSNET_ACRE",
        ]:
            assert rows[column].to_list() == [0.0] * empty.height
        # Undefined without trees
        assert rows["QMD"].null_count() == empty.height
        assert rows["SOFTWOOD_BA_SHARE"].null_count() == empty.height

        without = stand_metrics(fiadb_fixture_path, zero_fill=False)
        assert without.height == stands.height - empty.height
        assert without["N_TREES"].min() >= 1

    def test_one_row_per_forest_condition(self, fiadb_fixture_path):
        forest = sql_frame(
            fiadb_fixture_path,
            "SELECT COUNT(*) FROM COND WHERE COND_STATUS_CD = 1 "
            "AND CONDPROP_UNADJ IS NOT NULL",
        ).item()
        stands = stand_metrics(fiadb_fixture_path)
        assert stands.height == forest
        assert stands.select(KEYS).is_duplicated().sum() == 0


class TestPlotSelection:
    def test_plot_cns(self, fiadb_fixture_path):
        stands = stand_metrics(fiadb_fixture_path)
        chosen = stands["PLT_CN"].unique().sort().head(5).to_list()
        subset = stand_metrics(fiadb_fixture_path, plot_cns=chosen)
        assert sorted(subset["PLT_CN"].unique().to_list()) == chosen
        expected = stands.filter(pl.col("PLT_CN").is_in(chosen))
        assert subset.equals(expected)

    def test_integer_plot_cns(self, fiadb_fixture_path):
        stands = stand_metrics(fiadb_fixture_path)
        chosen = stands["PLT_CN"].head(1).to_list()
        subset = stand_metrics(fiadb_fixture_path, plot_cns=[int(chosen[0])])
        assert subset["PLT_CN"].unique().to_list() == chosen

    def test_evalid_clip(self, fiadb_fixture_path):
        in_eval = set(
            sql_frame(
                fiadb_fixture_path,
                "SELECT DISTINCT CAST(PLT_CN AS VARCHAR) AS PLT_CN "
                "FROM POP_PLOT_STRATUM_ASSGN WHERE EVALID = 12401",
            )["PLT_CN"].to_list()
        )
        with FIA(str(fiadb_fixture_path)) as db:
            db.clip_by_evalid(12401)
            clipped = condition_stand_metrics(db)
        assert clipped.height > 0
        assert set(clipped["PLT_CN"].unique().to_list()) <= in_eval

    def test_path_argument(self, fiadb_fixture_path):
        by_path = condition_stand_metrics(
            str(fiadb_fixture_path), metrics=["ba"], zero_fill=False
        )
        assert by_path.height > 0


class TestValidation:
    def test_unknown_metric(self, fiadb_fixture_path):
        with pytest.raises(ValueError, match="Unknown metrics"):
            stand_metrics(fiadb_fixture_path, metrics=["height"])

    def test_unknown_tree_type(self, fiadb_fixture_path):
        with pytest.raises(ValueError, match="tree_type"):
            stand_metrics(fiadb_fixture_path, tree_type="dead")

    def test_requested_columns_only(self, fiadb_fixture_path):
        stands = stand_metrics(fiadb_fixture_path, metrics=["qmd", "ba"])
        assert stands.columns == [
            "PLT_CN",
            "CONDID",
            "STATECD",
            "INVYR",
            "CONDPROP_UNADJ",
            "N_TREES",
            "BAA",
            "QMD",
        ]
