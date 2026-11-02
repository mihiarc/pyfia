"""Mortality measures, the tree-level frame and domain totals on the AL fixture.

The oracle for each measure is EVALIDator's formula, written out in SQL from
the attribute's ``metadata.sql`` (snum 11160 for sound total-stem volume,
222 for sawlog volume), on the committed Alabama fixture. Green weight has no
EVALIDator attribute; its oracle is the same formula with the REF_SPECIES
moisture contents.
"""

from __future__ import annotations

import duckdb
import polars as pl
import pytest

from pyfia import FIA, domain_total, mortality, mortality_trees
from pyfia.core.exceptions import NoEVALIDError
from pyfia.validation import MORTALITY_MEASURES

EVALID_GRM = 12403


def evalidator_formula(
    db_path,
    population: str,
    value: str,
    where: str = "TRUE",
    design: str = "SUBP",
) -> float:
    """EVALIDator's GRM mortality total on timberland, written out in SQL.

    ``population`` is the column code (``AL``, ``SL``), ``design`` the plot
    footprint (``MICR`` or ``SUBP``), ``value`` the per-tree amount (``m.`` is
    TREE_GRM_MIDPT, ``s.`` REF_SPECIES) and ``where`` an extra tree filter.
    """
    sql = f"""
        SELECT SUM(v * EXPNS) FROM (
          SELECT ps.EXPNS, plot.CN,
            SUM(g.TPA * CASE WHEN COALESCE(g.SUBPTYP_GRM, 0) = 0 THEN 0
                             WHEN g.SUBPTYP_GRM = 1 THEN ps.ADJ_FACTOR_SUBP
                             WHEN g.SUBPTYP_GRM = 2 THEN ps.ADJ_FACTOR_MICR
                             WHEN g.SUBPTYP_GRM = 3 THEN ps.ADJ_FACTOR_MACR
                             ELSE 0 END
                * CASE WHEN g.COMPONENT LIKE 'MORTALITY%' THEN {value} ELSE 0 END) v
          FROM POP_STRATUM ps
          JOIN POP_PLOT_STRATUM_ASSGN a ON a.STRATUM_CN = ps.CN
          JOIN PLOT plot ON plot.CN = a.PLT_CN
          JOIN COND c ON c.PLT_CN = plot.CN
          JOIN TREE t ON t.PLT_CN = c.PLT_CN AND t.CONDID = c.CONDID
          LEFT JOIN TREE_GRM_MIDPT m ON m.TRE_CN = t.CN
          JOIN REF_SPECIES s ON s.SPCD = t.SPCD
          JOIN (SELECT TRE_CN,
                       {design}_COMPONENT_{population}_TIMBER AS COMPONENT,
                       {design}_SUBPTYP_GRM_{population}_TIMBER AS SUBPTYP_GRM,
                       {design}_TPAMORT_UNADJ_{population}_TIMBER AS TPA
                FROM TREE_GRM_COMPONENT) g ON g.TRE_CN = t.CN
          WHERE ps.EVALID = {EVALID_GRM} AND {where}
          GROUP BY ps.EXPNS, plot.CN)
    """
    with duckdb.connect(str(db_path), read_only=True) as con:
        return con.sql(sql).fetchone()[0] or 0.0


@pytest.fixture
def db(fiadb_fixture):
    fiadb_fixture.clip_by_evalid(EVALID_GRM)
    return fiadb_fixture


class TestMeasures:
    """Every measure is reachable through the public mortality()."""

    @pytest.mark.parametrize("measure", MORTALITY_MEASURES)
    def test_public_function_accepts_measure(self, db, measure):
        result = mortality(db, tree_type="al5", measure=measure)
        assert result.height == 1
        assert result["MORT_TOTAL"][0] > 0
        assert result["MORT_TOTAL_SE"][0] > 0
        assert result["MEASURE"][0] == measure.upper()

    def test_carbon_raises(self, db):
        with pytest.raises(ValueError, match="measure='biomass'"):
            mortality(db, measure="carbon")

    def test_count_equals_tpa(self, db):
        count = mortality(db, tree_type="al", measure="count")
        tpa = mortality(db, tree_type="al", measure="tpa")
        assert count["MORT_TOTAL"][0] == pytest.approx(tpa["MORT_TOTAL"][0], rel=1e-12)

    def test_total_stem_matches_evalidator_formula(self, db, fiadb_fixture_path):
        """Sound total-stem wood volume of timber species at least 1 inch on
        timberland (snum 11160): MICR all-live columns, VOLTSSND."""
        expected = evalidator_formula(
            fiadb_fixture_path,
            "AL",
            "m.VOLTSSND",
            where="s.WOODLAND = 'N'",
            design="MICR",
        )
        result = mortality(db, tree_type="al", measure="total_stem")
        assert result["MORT_TOTAL"][0] == pytest.approx(expected, rel=1e-9)

    def test_total_stem_saplings_have_volume(self, db, fiadb_fixture_path):
        """Trees under 5 inches get FIA's total-stem volume; a null VOLTSSND
        adds nothing."""
        expected = evalidator_formula(
            fiadb_fixture_path,
            "AL",
            "COALESCE(m.VOLTSSND, 0)",
            where="g.TRE_CN IN (SELECT TRE_CN FROM TREE_GRM_COMPONENT "
            "WHERE DIA_MIDPT < 5)",
            design="MICR",
        )
        result = mortality(
            db, tree_type="al", measure="total_stem", tree_domain="DIA_MIDPT < 5"
        )
        assert expected > 0
        assert result["MORT_TOTAL"][0] == pytest.approx(expected, rel=1e-9)

        trees = mortality_trees(
            db, tree_type="al", tree_domain="DIA_MIDPT < 5", columns="VOLTSSND"
        )
        n_null = trees["VOLTSSND"].null_count()
        assert n_null < trees.height * 0.05, f"{n_null} of {trees.height} null"

    def test_sawlog_matches_evalidator_formula(self, db, fiadb_fixture_path):
        """Sawlog volume of sawtimber trees on timberland (snum 222)."""
        expected = evalidator_formula(fiadb_fixture_path, "SL", "m.VOLCSNET")
        result = mortality(db, tree_type="sl", measure="sawlog")
        assert result["MORT_TOTAL"][0] == pytest.approx(expected, rel=1e-9)

    def test_sawlog_is_part_of_volume(self, db):
        sawlog = mortality(db, tree_type="al5", measure="sawlog")["MORT_TOTAL"][0]
        volume = mortality(db, tree_type="al5", measure="volume")["MORT_TOTAL"][0]
        assert 0 < sawlog < volume

    def test_green_weight_matches_formula(self, db, fiadb_fixture_path):
        value = (
            "(COALESCE(m.DRYBIO_BOLE, 0) * (1 + s.MC_PCT_GREEN_WOOD / 100)"
            " + COALESCE(m.DRYBIO_BOLE_BARK, 0) * (1 + s.MC_PCT_GREEN_BARK / 100))"
            " / 2000"
        )
        expected = evalidator_formula(fiadb_fixture_path, "AL", value)
        result = mortality(db, tree_type="al5", measure="green_weight")
        assert result["MORT_TOTAL"][0] == pytest.approx(expected, rel=1e-9)

    def test_green_weight_exceeds_dry_bole(self, db, fiadb_fixture_path):
        dry = evalidator_formula(
            fiadb_fixture_path,
            "AL",
            "(COALESCE(m.DRYBIO_BOLE, 0) + COALESCE(m.DRYBIO_BOLE_BARK, 0)) / 2000",
        )
        green = mortality(db, tree_type="al5", measure="green_weight")
        assert green["MORT_TOTAL"][0] > dry

    def test_green_weight_missing_moisture_raises(self, fiadb_fixture_path):
        with FIA(str(fiadb_fixture_path)) as db:
            db.clip_by_evalid(EVALID_GRM)
            db.load_table("REF_SPECIES")
            ref = db.tables["REF_SPECIES"]
            ref = ref.collect() if isinstance(ref, pl.LazyFrame) else ref
            db.tables["REF_SPECIES"] = ref.with_columns(
                pl.when(pl.col("SPCD") == 131)
                .then(None)
                .otherwise(pl.col("MC_PCT_GREEN_WOOD"))
                .alias("MC_PCT_GREEN_WOOD")
            ).lazy()
            with pytest.raises(ValueError, match=r"SPCD \[131\]"):
                mortality(db, tree_type="al5", measure="green_weight")

    def test_six_way_cells_sum_to_total(self, db):
        cells = mortality(
            db,
            tree_type="al5",
            measure="sawlog",
            grp_by=["STATECD", "UNITCD", "SPCD", "AGENTCD", "DSTRBCD1"],
            by_size_class=True,
        )
        total = mortality(db, tree_type="al5", measure="sawlog")
        assert cells["MORT_TOTAL"].sum() == pytest.approx(
            total["MORT_TOTAL"][0], rel=1e-9
        )


class TestMortalityTrees:
    """The tree-level frame."""

    def test_columns_from_every_table(self, db):
        trees = mortality_trees(
            db,
            tree_type="al5",
            columns=["VOLTSGRS", "DSTRBCD1", "ELEV", "AGENTCD"],
        )
        for col in ["PLT_CN", "TRE_CN", "STRATUM_CN", "EXPNS", "TPA_ADJ"]:
            assert trees[col].null_count() == 0
        assert {"VOLTSGRS", "DSTRBCD1", "ELEV", "AGENTCD"} <= set(trees.columns)
        assert trees["AGENTCD"].null_count() == 0
        assert trees["TRE_CN"].is_unique().all()

    def test_tpa_adj(self, db):
        trees = mortality_trees(db, tree_type="al")
        assert (
            trees["TPA_ADJ"] - trees["TPA_UNADJ"] * trees["ADJ_FACTOR"]
        ).abs().max() == 0
        assert set(trees["SUBPTYP_GRM"].unique()) >= {1, 2}

    def test_same_trees_as_mortality(self, db):
        trees = mortality_trees(db, tree_type="al5")
        result = mortality(db, tree_type="al5", measure="tpa")
        assert trees.height == result["N_DEAD_TREES"][0]

    def test_frame_columns_can_be_requested(self, db):
        trees = mortality_trees(db, columns=["EXPNS", "TPA_ADJ", "VOLCFNET"])
        assert trees.columns.count("EXPNS") == 1
        assert "VOLCFNET" in trees.columns

    def test_unknown_column_raises(self, db):
        with pytest.raises(ValueError, match="NOT_A_COLUMN"):
            mortality_trees(db, columns=["NOT_A_COLUMN"])

    def test_condid_raises(self, db):
        with pytest.raises(ValueError, match="CONDID"):
            mortality_trees(db, columns="CONDID")

    def test_requires_evaluation(self, fiadb_fixture):
        with pytest.raises(NoEVALIDError):
            mortality_trees(fiadb_fixture)


class TestDomainTotal:
    """Totals and standard errors of a caller's per-tree value."""

    @pytest.fixture
    def trees(self, db):
        return mortality_trees(
            db, tree_type="al5", columns=["VOLCFNET", "DSTRBCD1", "AGENTCD"]
        ).with_columns(pl.col("VOLCFNET").fill_null(0.0))

    def test_reproduces_mortality_grouped(self, db, trees):
        expected = mortality(db, tree_type="al5", measure="volume", grp_by="SPCD")
        got = domain_total(db, trees, "VOLCFNET", grp_by="SPCD")
        joined = expected.join(got, on="SPCD", how="full", coalesce=True)
        assert joined.height == expected.height == got.height
        for a, b in [
            ("MORT_TOTAL", "TOTAL"),
            ("MORT_TOTAL_SE", "TOTAL_SE"),
        ]:
            rel = ((joined[a] - joined[b]).abs() / joined[a].abs()).max()
            assert rel < 1e-9, (a, rel)
        assert joined["N_PLOTS"].to_list() == joined["N_PLOTS_right"].to_list()
        assert joined["N_DEAD_TREES"].to_list() == joined["N_TREES"].to_list()

    def test_reproduces_mortality_ungrouped(self, db, trees):
        expected = mortality(db, tree_type="al5", measure="volume")
        got = domain_total(db, trees, "VOLCFNET")
        assert got["TOTAL"][0] == pytest.approx(expected["MORT_TOTAL"][0], rel=1e-9)
        assert got["TOTAL_SE"][0] == pytest.approx(
            expected["MORT_TOTAL_SE"][0], rel=1e-9
        )
        assert got["N_PLOTS"][0] == expected["N_PLOTS"][0]
        assert got["N_TREES"][0] == expected["N_DEAD_TREES"][0]

    def test_counts_trees_without_value(self, db, trees):
        expected = mortality(db, tree_type="al5", measure="tpa")
        got = domain_total(db, trees)
        assert got["TOTAL"][0] == pytest.approx(expected["MORT_TOTAL"][0], rel=1e-9)
        assert got["TOTAL_SE"][0] == pytest.approx(
            expected["MORT_TOTAL_SE"][0], rel=1e-9
        )

    def test_constant_multiplier_scales_se(self, db, trees):
        c = 37.5
        base = domain_total(db, trees, "VOLCFNET", grp_by="STATECD")
        scaled = domain_total(
            db,
            trees.with_columns((pl.col("VOLCFNET") * c).alias("V")),
            "V",
            grp_by="STATECD",
        )
        assert scaled["TOTAL"][0] == pytest.approx(c * base["TOTAL"][0], rel=1e-12)
        assert scaled["TOTAL_SE"][0] == pytest.approx(
            c * base["TOTAL_SE"][0], rel=1e-12
        )

    def test_derived_groups_sum_to_total(self, db, trees):
        trees = trees.with_columns(
            pl.when(pl.col("AGENTCD") // 10 == 3)
            .then(pl.lit("fire"))
            .when(pl.col("DSTRBCD1").is_between(30, 32))
            .then(pl.lit("fire_disturbance"))
            .otherwise(pl.lit("other"))
            .alias("LOSS")
        )
        grouped = domain_total(db, trees, "VOLCFNET", grp_by=["STATECD", "LOSS"])
        total = domain_total(db, trees, "VOLCFNET")
        assert grouped["TOTAL"].sum() == pytest.approx(total["TOTAL"][0], rel=1e-12)
        assert grouped["N_TREES"].sum() == total["N_TREES"][0]
        # Shared plots: group variances don't add up to the total's variance.
        assert (grouped["TOTAL_SE"] ** 2).sum() != pytest.approx(
            total["TOTAL_SE"][0] ** 2, rel=1e-6
        )

    def test_sparse_group_zero_fills_all_plots(self, db, trees):
        """A group on few plots gets the SE of the full evaluation: filtering
        trees, grouping, and mortality()'s own domain give the same answer."""
        n_plots = trees.group_by("SPCD").agg(pl.col("PLT_CN").n_unique().alias("n"))
        spcd = n_plots.filter(pl.col("n") >= 2).sort(["n", "SPCD"])["SPCD"][0]

        grouped = domain_total(db, trees, "VOLCFNET", grp_by="SPCD").filter(
            pl.col("SPCD") == spcd
        )
        filtered = domain_total(db, trees.filter(pl.col("SPCD") == spcd), "VOLCFNET")
        zeroed = domain_total(
            db,
            trees.with_columns(
                pl.when(pl.col("SPCD") == spcd)
                .then(pl.col("VOLCFNET"))
                .otherwise(0.0)
                .alias("VOLCFNET")
            ),
            "VOLCFNET",
        )
        direct = mortality(
            db, tree_type="al5", measure="volume", tree_domain=f"SPCD == {spcd}"
        )
        se = direct["MORT_TOTAL_SE"][0]
        assert se > 0
        for frame in (grouped, filtered, zeroed):
            assert frame["TOTAL"][0] == pytest.approx(direct["MORT_TOTAL"][0], rel=1e-9)
            assert frame["TOTAL_SE"][0] == pytest.approx(se, rel=1e-9)

    def test_null_group_is_a_group(self, db, trees):
        trees = trees.with_columns(
            pl.when(pl.col("SPCD") == trees["SPCD"][0])
            .then(None)
            .otherwise(pl.col("SPCD"))
            .alias("SPCD")
        )
        grouped = domain_total(db, trees, "VOLCFNET", grp_by="SPCD")
        assert grouped["SPCD"].null_count() == 1
        assert grouped["TOTAL_SE"].null_count() == 0

    def test_group_named_like_design_column(self, db, trees):
        """Grouping by the estimation unit, a column the plot frame also holds."""
        grouped = domain_total(db, trees, "VOLCFNET", grp_by="ESTN_UNIT_CN")
        total = domain_total(db, trees, "VOLCFNET")
        assert grouped.columns[0] == "ESTN_UNIT_CN"
        assert grouped["TOTAL"].sum() == pytest.approx(total["TOTAL"][0], rel=1e-12)
        # Estimation units hold disjoint plots, so their variances add up.
        assert (grouped["TOTAL_SE"] ** 2).sum() == pytest.approx(
            total["TOTAL_SE"][0] ** 2, rel=1e-9
        )

    def test_variance_column(self, db, trees):
        got = domain_total(db, trees, "VOLCFNET", variance=True)
        assert got["TOTAL_VARIANCE"][0] == pytest.approx(got["TOTAL_SE"][0] ** 2)

    def test_null_value_raises(self, db, trees):
        trees = trees.with_columns(
            pl.when(pl.int_range(pl.len()) == 0)
            .then(None)
            .otherwise(pl.col("VOLCFNET"))
            .alias("VOLCFNET")
        )
        with pytest.raises(ValueError, match="null"):
            domain_total(db, trees, "VOLCFNET")

    def test_duplicated_trees_raise(self, db, trees):
        with pytest.raises(ValueError, match="TRE_CN"):
            domain_total(db, pl.concat([trees, trees.head(1)]), "VOLCFNET")

    def test_missing_column_raises(self, db, trees):
        with pytest.raises(ValueError, match="LOSS"):
            domain_total(db, trees, "VOLCFNET", grp_by="LOSS")

    def test_plots_outside_evaluation_raise(self, db, trees):
        trees = trees.with_columns(pl.lit("999").alias("PLT_CN"))
        with pytest.raises(ValueError, match="not in the evaluation"):
            domain_total(db, trees, "VOLCFNET")
