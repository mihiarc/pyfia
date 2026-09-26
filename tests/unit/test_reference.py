"""pyfia.reference lookups and join_reference on the committed Alabama fixture.

Each lookup is compared with its REF table read directly in SQL, and every
code the fixture's COND, TREE and PLOT rows hold must resolve.
"""

from __future__ import annotations

import duckdb
import polars as pl
import pytest
from polars.testing import assert_frame_equal

from pyfia import FIA, MissingColumnError, UnknownCodeError, reference
from pyfia.constants import codes
from pyfia.reference import (
    REFERENCE_KINDS,
    SPECIES_COLUMNS,
    join_reference,
    label_codes,
)


def sql(db_path, query: str) -> pl.DataFrame:
    with duckdb.connect(str(db_path), read_only=True) as con:
        return con.sql(query).pl()


def as_int64(df: pl.DataFrame, *cols: str) -> pl.DataFrame:
    return df.with_columns(pl.col(c).cast(pl.Int64) for c in cols)


class TestLookupsEqualTheirTables:
    def test_forest_types(self, fiadb_fixture):
        expected = as_int64(
            sql(
                fiadb_fixture.db_path,
                """
                SELECT f.VALUE AS FORTYPCD, f.MEANING AS FORTYPCD_NAME, f.TYPGRPCD,
                       g.MEANING AS TYPGRPCD_NAME, f.ALLOWED_IN_FIELD
                FROM REF_FOREST_TYPE f
                LEFT JOIN REF_FOREST_TYPE_GROUP g ON g.VALUE = f.TYPGRPCD
                ORDER BY f.VALUE
                """,
            ),
            "FORTYPCD",
            "TYPGRPCD",
        )
        result = reference.forest_types(fiadb_fixture)
        assert result.height == 207
        assert_frame_equal(result, expected)

    def test_forest_type_groups(self, fiadb_fixture):
        expected = as_int64(
            sql(
                fiadb_fixture.db_path,
                "SELECT VALUE AS TYPGRPCD, MEANING AS TYPGRPCD_NAME "
                "FROM REF_FOREST_TYPE_GROUP ORDER BY VALUE",
            ),
            "TYPGRPCD",
        )
        assert_frame_equal(reference.forest_type_groups(fiadb_fixture), expected)

    def test_species_default_columns(self, fiadb_fixture):
        plain = [
            c for c in SPECIES_COLUMNS if c not in ("E_SPGRPCD_NAME", "W_SPGRPCD_NAME")
        ]
        expected = sql(
            fiadb_fixture.db_path,
            f"""
            SELECT {", ".join(f"s.{c}" for c in plain)},
                   e.NAME AS E_SPGRPCD_NAME, w.NAME AS W_SPGRPCD_NAME
            FROM REF_SPECIES s
            LEFT JOIN REF_SPECIES_GROUP e ON e.SPGRPCD = s.E_SPGRPCD
            LEFT JOIN REF_SPECIES_GROUP w ON w.SPGRPCD = s.W_SPGRPCD
            ORDER BY s.SPCD
            """,
        )
        expected = as_int64(expected, "SPCD", "E_SPGRPCD", "W_SPGRPCD").select(
            SPECIES_COLUMNS
        )
        result = reference.species(fiadb_fixture)
        assert result.columns == list(SPECIES_COLUMNS)
        assert_frame_equal(result, expected, check_dtypes=False)

    def test_species_selected_columns(self, fiadb_fixture):
        result = reference.species(
            fiadb_fixture, columns=["COMMON_NAME", "JENKINS_SPGRPCD", "E_SPGRPCD_NAME"]
        )
        assert result.columns == [
            "SPCD",
            "COMMON_NAME",
            "JENKINS_SPGRPCD",
            "E_SPGRPCD_NAME",
        ]
        loblolly = result.filter(pl.col("SPCD") == 131).row(0, named=True)
        assert loblolly["COMMON_NAME"] == "loblolly pine"
        assert loblolly["E_SPGRPCD_NAME"] == "Loblolly and shortleaf pines"

    def test_species_unknown_column(self, fiadb_fixture):
        with pytest.raises(MissingColumnError, match="NOT_A_COLUMN"):
            reference.species(fiadb_fixture, columns=["NOT_A_COLUMN"])

    def test_species_groups(self, fiadb_fixture):
        expected = as_int64(
            sql(
                fiadb_fixture.db_path,
                "SELECT SPGRPCD, NAME AS SPGRPCD_NAME FROM REF_SPECIES_GROUP "
                "ORDER BY SPGRPCD",
            ),
            "SPGRPCD",
        )
        assert_frame_equal(reference.species_groups(fiadb_fixture), expected)

    def test_owner_groups(self, fiadb_fixture):
        expected = as_int64(
            sql(
                fiadb_fixture.db_path,
                "SELECT OWNGRPCD, MEANING AS OWNGRPCD_NAME FROM REF_OWNGRPCD "
                "ORDER BY OWNGRPCD",
            ),
            "OWNGRPCD",
        )
        result = reference.owner_groups(fiadb_fixture)
        assert result["OWNGRPCD"].to_list() == [10, 20, 30, 40]
        assert_frame_equal(result, expected)

    def test_survey_units(self, fiadb_fixture):
        expected = as_int64(
            sql(
                fiadb_fixture.db_path,
                "SELECT STATECD, VALUE AS UNITCD, MEANING AS UNIT_NAME FROM REF_UNIT "
                "ORDER BY STATECD, VALUE",
            ),
            "STATECD",
            "UNITCD",
        )
        assert_frame_equal(reference.survey_units(fiadb_fixture), expected)

    def test_counties(self, fiadb_fixture):
        expected = as_int64(
            sql(
                fiadb_fixture.db_path,
                "SELECT STATECD, COUNTYCD, COUNTYNM, UNITCD FROM COUNTY "
                "ORDER BY STATECD, COUNTYCD",
            ),
            "STATECD",
            "COUNTYCD",
            "UNITCD",
        )
        assert_frame_equal(reference.counties(fiadb_fixture), expected)

    def test_states(self, fiadb_fixture):
        result = reference.states(fiadb_fixture)
        assert result.rows() == [(1, "Alabama", "AL")]

    def test_accepts_a_path(self, fiadb_fixture_path):
        assert reference.owner_groups(str(fiadb_fixture_path)).height == 4

    def test_ignores_the_evalid_clip(self, fiadb_fixture):
        fiadb_fixture.clip_by_evalid(12401)
        assert reference.forest_types(fiadb_fixture).height == 207


class TestFixtureCodesResolve:
    """Every code in the fixture's COND, TREE and PLOT rows is in the REF tables."""

    @pytest.mark.parametrize(
        "kind,query",
        [
            ("forest_type", "SELECT DISTINCT FORTYPCD FROM COND"),
            ("owner_group", "SELECT DISTINCT OWNGRPCD FROM COND"),
            ("species", "SELECT DISTINCT SPCD FROM TREE"),
            ("species_group", "SELECT DISTINCT SPGRPCD FROM TREE"),
            ("survey_unit", "SELECT DISTINCT STATECD, UNITCD FROM PLOT"),
            ("county", "SELECT DISTINCT STATECD, COUNTYCD FROM PLOT"),
            ("state", "SELECT DISTINCT STATECD FROM PLOT"),
        ],
    )
    def test_codes_resolve(self, fiadb_fixture, kind, query):
        codes = sql(fiadb_fixture.db_path, query)
        labeled = join_reference(codes, fiadb_fixture, kind)
        added = [c for c in labeled.columns if c not in codes.columns]
        with_code = labeled.drop_nulls(codes.columns)
        assert with_code.height > 0
        assert with_code.select(added).null_count().sum_horizontal().item() == 0


class TestJoinReference:
    def test_labels_in_row_order_and_keeps_dtype(self, fiadb_fixture):
        df = pl.DataFrame(
            {"FORTYPCD": [503, None, 161, 503], "AREA": [1.0, 2.0, 3.0, 4.0]},
            schema={"FORTYPCD": pl.Int32, "AREA": pl.Float64},
        )
        result = join_reference(df, fiadb_fixture, "forest_type")
        assert result["AREA"].to_list() == [1.0, 2.0, 3.0, 4.0]
        assert result.schema["FORTYPCD"] == pl.Int32
        assert result["FORTYPCD_NAME"].to_list() == [
            "White oak / red oak / hickory",
            None,
            "Loblolly pine",
            "White oak / red oak / hickory",
        ]
        assert result["TYPGRPCD_NAME"][2] == "Loblolly / shortleaf pine group"

    def test_unknown_code_raises(self, fiadb_fixture):
        df = pl.DataFrame({"FORTYPCD": [161, 998, 997, 998]})
        with pytest.raises(UnknownCodeError, match="REF_FOREST_TYPE") as err:
            join_reference(df, fiadb_fixture, "forest_type")
        assert err.value.codes == [997, 998]
        assert isinstance(err.value, ValueError)

    def test_unknown_composite_code_raises(self, fiadb_fixture):
        df = pl.DataFrame({"STATECD": [1, 1], "COUNTYCD": [125, 999]})
        with pytest.raises(UnknownCodeError) as err:
            join_reference(df, fiadb_fixture, "county")
        assert err.value.codes == [(1, 999)]

    def test_on_and_prefix_label_both_times(self, fiadb_fixture):
        df = pl.DataFrame({"t1_OWNGRPCD": [40, 10], "t2_OWNGRPCD": [40, 40]})
        df = join_reference(
            df, fiadb_fixture, "owner_group", on="t1_OWNGRPCD", prefix="t1_"
        )
        df = join_reference(
            df, fiadb_fixture, "owner_group", on="t2_OWNGRPCD", prefix="t2_"
        )
        assert df.select("t1_OWNGRPCD_NAME", "t2_OWNGRPCD_NAME").rows() == [
            ("Private", "Private"),
            ("Forest Service", "Private"),
        ]

    def test_columns_subset(self, fiadb_fixture):
        df = pl.DataFrame({"SPCD": [131, 802]})
        result = join_reference(
            df, fiadb_fixture, "species", columns=["COMMON_NAME", "SFTWD_HRDWD"]
        )
        assert result.rows() == [(131, "loblolly pine", "S"), (802, "white oak", "H")]

    def test_refuses_to_overwrite(self, fiadb_fixture):
        df = pl.DataFrame({"OWNGRPCD": [40], "OWNGRPCD_NAME": ["mine"]})
        with pytest.raises(ValueError, match="overwrite"):
            join_reference(df, fiadb_fixture, "owner_group")

    def test_missing_on_column(self, fiadb_fixture):
        with pytest.raises(MissingColumnError, match="FORTYPCD"):
            join_reference(pl.DataFrame({"X": [1]}), fiadb_fixture, "forest_type")

    def test_wrong_number_of_on_columns(self, fiadb_fixture):
        df = pl.DataFrame({"COUNTYCD": [125]})
        with pytest.raises(ValueError, match="2 column"):
            join_reference(df, fiadb_fixture, "county", on="COUNTYCD")

    def test_unknown_kind(self, fiadb_fixture):
        with pytest.raises(ValueError, match="kind must be one of"):
            join_reference(pl.DataFrame({"X": [1]}), fiadb_fixture, "ecoregion")

    def test_every_kind_is_documented(self):
        doc = join_reference.__doc__ or ""
        assert all(f"'{kind}'" in doc for kind in REFERENCE_KINDS)

    def test_after_an_estimator(self, fiadb_fixture):
        from pyfia import area

        fiadb_fixture.clip_by_evalid(12401)
        result = join_reference(
            area(fiadb_fixture, grp_by="OWNGRPCD"), fiadb_fixture, "owner_group"
        )
        assert "OWNGRPCD_NAME" in result.columns
        assert result.filter(pl.col("OWNGRPCD") == 40)["OWNGRPCD_NAME"][0] == "Private"


class TestLabelCodes:
    def test_labels_every_coded_column_by_default(self):
        df = pl.DataFrame(
            {"TRTCD1": [10, 0, None], "STATUSCD": [2, 1, 3], "PLT_CN": [1, 2, 3]}
        )
        result = label_codes(df)
        assert result.columns == [*df.columns, "TRTCD1_NAME", "STATUSCD_NAME"]
        assert result["TRTCD1_NAME"].to_list() == [
            "Cutting",
            "No observable treatment",
            None,
        ]
        assert result["STATUSCD_NAME"].to_list() == [
            "Dead tree",
            "Live tree",
            "Removed",
        ]

    def test_time_prefixes(self):
        df = pl.DataFrame({"t1_COND_STATUS_CD": [1], "t2_COND_STATUS_CD": [2]})
        result = label_codes(df, ["t1_COND_STATUS_CD", "t2_COND_STATUS_CD"])
        assert result.row(0)[2:] == ("Accessible forest land", "Nonforest land")

    def test_grm_components(self):
        df = pl.DataFrame({"COMPONENT": ["SURVIVOR", "CUT1", "NOT USED"]})
        names = label_codes(df)["COMPONENT_NAME"].to_list()
        assert (
            names[0] == "Tree has remained live and in the estimate from T1 through T2"
        )
        assert names[1].startswith(
            "Tree was previously in estimate at T1 and was killed"
        )
        assert names[2] == "Tree was either live or dead at T1 and has no status at T2"

    def test_agentcd_takes_its_range_meaning(self):
        df = pl.DataFrame({"AGENTCD": [0, 11, 19, 25, 85]})
        assert label_codes(df)["AGENTCD_NAME"].to_list() == [
            "No agent recorded (only allowed on live trees in data prior to 1999)",
            "Insect",
            "Insect",
            "Disease",
            codes.AGENTCD[80],
        ]

    def test_text_codes(self):
        df = pl.DataFrame({"TRTCD1": ["10", "00"]})
        assert label_codes(df)["TRTCD1_NAME"].to_list() == [
            "Cutting",
            "No observable treatment",
        ]

    def test_unknown_code_raises(self):
        df = pl.DataFrame({"TRTCD1": [10, 60, 70, 60]})
        with pytest.raises(UnknownCodeError, match="TRTCD1") as err:
            label_codes(df)
        assert err.value.codes == [60, 70]

    def test_unknown_agentcd_range_raises(self):
        with pytest.raises(UnknownCodeError) as err:
            label_codes(pl.DataFrame({"AGENTCD": [10, 95]}))
        assert err.value.codes == [95]

    def test_uncoded_column(self):
        with pytest.raises(ValueError, match="No code table"):
            label_codes(pl.DataFrame({"FORTYPCD": [161]}), "FORTYPCD")

    def test_missing_column(self):
        with pytest.raises(MissingColumnError, match="TRTCD1"):
            label_codes(pl.DataFrame({"X": [1]}), "TRTCD1")

    def test_refuses_to_overwrite(self):
        df = pl.DataFrame({"TRTCD1": [10], "TRTCD1_NAME": ["mine"]})
        with pytest.raises(ValueError, match="overwrite"):
            label_codes(df, "TRTCD1")

    def test_suffix(self):
        df = pl.DataFrame({"RESERVCD": [0, 1]})
        result = label_codes(df, suffix="_LABEL")
        assert result["RESERVCD_LABEL"].to_list() == ["Not reserved", "Reserved"]


def test_fia_db_path_attribute(fiadb_fixture):
    """The tests above read ``fiadb_fixture.db_path`` directly."""
    assert isinstance(fiadb_fixture, FIA)
    assert str(fiadb_fixture.db_path).endswith(".duckdb")
