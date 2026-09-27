"""FIADB column types: the generated schema, the typed CSV import and the casts.

The schema comes from the DataMart SQLite export (``scripts/gen_fiadb_schema.py``).
Downloads load each CSV with those types, and the unit-level builders cast
what they read to them, so every database gives the same dtypes.
"""

from __future__ import annotations

import importlib.util
import logging
from pathlib import Path

import duckdb
import polars as pl
import pytest

from pyfia import condition_intervals, condition_stand_metrics, tree_intervals
from pyfia.constants.fiadb_schema import COLUMN_TYPES, FIADB_VERSION
from pyfia.core.fiadb_types import POLARS_TYPES, cast_to_fiadb_types, fiadb_dtype
from pyfia.downloader import _convert_csvs_to_duckdb, _import_csv, _table_name
from pyfia.downloader.client import DataMartClient
from pyfia.downloader.exceptions import DownloadError, NetworkError, TableNotFoundError
from pyfia.downloader.tables import ALL_TABLES, COMMON_TABLES, REFERENCE_TABLES

_SCRIPT = Path(__file__).parents[2] / "scripts" / "gen_fiadb_schema.py"


def _generator():
    spec = importlib.util.spec_from_file_location("gen_fiadb_schema", _SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# -- the generated schema -----------------------------------------------------


class TestSchema:
    def test_version_and_types(self):
        assert FIADB_VERSION.startswith("FIADB_")
        types = {t for cols in COLUMN_TYPES.values() for t in cols.values()}
        assert types <= set(POLARS_TYPES)

    def test_every_cn_column_is_varchar(self):
        cns = {
            (t, c): typ
            for t, cols in COLUMN_TYPES.items()
            for c, typ in cols.items()
            if c == "CN" or c.endswith("_CN")
        }
        assert cns and set(cns.values()) == {"VARCHAR"}

    def test_codes_join_their_keys(self):
        # TREE.SPCD is declared FLOAT but joins REF_SPECIES.SPCD (INTEGER)
        assert (
            COLUMN_TYPES["TREE"]["SPCD"]
            == COLUMN_TYPES["REF_SPECIES"]["SPCD"]
            == "BIGINT"
        )
        assert COLUMN_TYPES["PLOTGEOM"]["STATECD"] == COLUMN_TYPES["PLOT"]["STATECD"]
        assert COLUMN_TYPES["COND"]["STDORGSP"] == "BIGINT"

    def test_measurements_keep_their_declared_type(self):
        assert COLUMN_TYPES["TREE"]["DIA"] == "DOUBLE"
        assert COLUMN_TYPES["COND"]["BALIVE"] == "DOUBLE"
        assert COLUMN_TYPES["FVS_TREEINIT_PLOT"]["HT"] == "DOUBLE"
        assert COLUMN_TYPES["PLOT"]["CREATED_DATE"] == "TIMESTAMP"

    def test_generator_rules(self):
        declared = {
            "A": [
                ("CN", "INTEGER"),
                ("X_CN", "VARCHAR"),
                ("SPCD", "INTEGER"),
                ("N", "INTEGER"),
            ],
            "B": [("SPCD", "FLOAT"), ("N", "FLOAT"), ("W", "FLOAT"), ("D", "DATETIME")],
        }
        types = _generator().column_types(declared)
        assert types["A"] == {
            "CN": "VARCHAR",
            "X_CN": "VARCHAR",
            "SPCD": "BIGINT",
            "N": "BIGINT",
        }
        # SPCD is a code that joins A.SPCD; N and W are measurements
        assert types["B"] == {
            "SPCD": "BIGINT",
            "N": "DOUBLE",
            "W": "DOUBLE",
            "D": "TIMESTAMP",
        }

    def test_generator_rejects_unknown_types(self):
        with pytest.raises(ValueError, match="unexpected declared type"):
            _generator().column_types({"A": [("X", "BLOB")]})


# -- typed CSV import ---------------------------------------------------------


def _csv(path: Path, text: str) -> Path:
    path.write_text(text, encoding="utf-8")
    return path


@pytest.fixture
def conn():
    with duckdb.connect() as c:
        yield c


class TestImportCsv:
    def test_columns_take_fiadb_types(self, tmp_path, conn):
        f = _csv(
            tmp_path / "AL_PLOT.csv",
            "CN,PREV_PLT_CN,INVYR,REMPER,CREATED_DATE\n"
            "15618850201085432000001,,2024,5.2,2025-01-21 08:15:39\n",
        )
        assert _import_csv(conn, f, "PLOT", state_code=1) == 1
        row = conn.execute(
            "SELECT typeof(CN), CN, typeof(INVYR), typeof(REMPER), "
            "typeof(CREATED_DATE), STATE_ADDED FROM PLOT"
        ).fetchone()
        # a 23-digit CN survives intact
        assert row == (
            "VARCHAR",
            "15618850201085432000001",
            "BIGINT",
            "DOUBLE",
            "TIMESTAMP",
            1,
        )

    def test_integer_written_with_a_decimal_point(self, tmp_path, conn):
        f = _csv(tmp_path / "AL_TREE.csv", "CN,SPCD\n1,693.0\n2,131\n")
        _import_csv(conn, f, "TREE")
        assert conn.execute("SELECT SPCD FROM TREE ORDER BY CN").fetchall() == [
            (693,),
            (131,),
        ]

    def test_fractional_value_in_an_integer_column_fails(self, tmp_path, conn):
        f = _csv(tmp_path / "AL_TREE.csv", "CN,SPCD\n1,693.5\n")
        with pytest.raises(
            DownloadError, match="TREE.SPCD holds a value that is not an integer"
        ):
            _import_csv(conn, f, "TREE")

    def test_text_in_a_numeric_column_fails(self, tmp_path, conn):
        f = _csv(tmp_path / "AL_COND.csv", "CN,BALIVE\n1,abc\n")
        with pytest.raises(DownloadError, match="COND"):
            _import_csv(conn, f, "COND")

    def test_unknown_columns_and_tables_load_as_varchar(self, tmp_path, conn, caplog):
        caplog.set_level(logging.WARNING)
        f = _csv(tmp_path / "AL_PLOT.csv", "CN,NEW_COLUMN\n1,7\n")
        _import_csv(conn, f, "PLOT")
        g = _csv(tmp_path / "AL_NEW_TABLE.csv", "A,B\n1,2\n")
        _import_csv(conn, g, "NEW_TABLE")
        assert conn.execute("SELECT typeof(NEW_COLUMN) FROM PLOT").fetchone() == (
            "VARCHAR",
        )
        assert conn.execute(
            "SELECT typeof(A), typeof(B) FROM NEW_TABLE"
        ).fetchone() == ("VARCHAR", "VARCHAR")
        assert (
            "NEW_COLUMN" in caplog.text
            and "NEW_TABLE is not in the FIADB schema" in caplog.text
        )

    def test_second_state_appends_by_column_name(self, tmp_path, conn):
        _import_csv(
            conn, _csv(tmp_path / "AL_PLOT.csv", "CN,INVYR\n1,2020\n"), "PLOT", 1
        )
        _import_csv(
            conn, _csv(tmp_path / "GA_PLOT.csv", "INVYR,CN\n2021,2\n"), "PLOT", 13
        )
        rows = conn.execute(
            "SELECT CN, INVYR, STATE_ADDED FROM PLOT ORDER BY CN"
        ).fetchall()
        assert rows == [("1", 2020, 1), ("2", 2021, 13)]

    def test_header_only_file_gives_an_empty_typed_table(self, tmp_path, conn):
        _import_csv(conn, _csv(tmp_path / "AL_PLOT.csv", "CN,INVYR\n"), "PLOT")
        assert conn.execute("SELECT count(*) FROM PLOT").fetchone() == (0,)
        types = dict(
            conn.execute(
                "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'PLOT'"
            ).fetchall()
        )
        assert types == {"CN": "VARCHAR", "INVYR": "BIGINT"}

    def test_rows_repeating_the_header_are_dropped(self, tmp_path, conn, caplog):
        caplog.set_level(logging.WARNING)
        f = _csv(tmp_path / "RI_SOILS_LAB.csv", "CN,INVYR\n1,2020\nCN,INVYR\n2,2021\n")
        assert _import_csv(conn, f, "SOILS_LAB") == 2
        assert conn.execute(
            "SELECT typeof(INVYR) FROM SOILS_LAB LIMIT 1"
        ).fetchone() == ("BIGINT",)
        assert "dropped 1 row(s) repeating the header" in caplog.text

    def test_quoted_line_breaks(self, tmp_path, conn):
        f = _csv(tmp_path / "REF_CITATION.csv", 'CN,CITATION\n1,"two\nlines"\n2,one\n')
        assert _import_csv(conn, f, "REF_CITATION") == 2


class TestTableLists:
    def test_downloaded_tables_are_published_ones(self):
        assert set(ALL_TABLES) <= set(COLUMN_TYPES)
        assert set(COMMON_TABLES) <= set(ALL_TABLES)
        assert set(REFERENCE_TABLES) <= set(COLUMN_TYPES)
        assert "COUNTY" in COMMON_TABLES  # pyfia.reference.counties

    def test_network_error_fails_the_download(self, tmp_path, monkeypatch):
        client = DataMartClient()

        def fail(state, table, dest_dir, show_progress=True):
            raise NetworkError(f"{state}_{table}: connection reset")

        monkeypatch.setattr(client, "download_table", fail)
        with pytest.raises(NetworkError):
            client.download_tables(
                "RI", tables=["PLOT"], dest_dir=tmp_path, show_progress=False
            )

    def test_unpublished_table_is_skipped(self, tmp_path, monkeypatch):
        client = DataMartClient()

        def missing(state, table, dest_dir, show_progress=True):
            raise TableNotFoundError(table, state)

        monkeypatch.setattr(client, "download_table", missing)
        assert (
            client.download_tables(
                "RI", tables=["PLOT"], dest_dir=tmp_path, show_progress=False
            )
            == {}
        )


class TestStateArchive:
    def test_archive_holds_every_published_table(self, tmp_path, monkeypatch):
        import zipfile

        def fake_download(url, dest, description="", show_progress=True):
            assert url.endswith("/RI_CSV.zip")
            with zipfile.ZipFile(dest, "w") as zf:
                zf.writestr("RI_PLOT.csv", "CN,INVYR\n1,2020\n")
                zf.writestr("RI_TREE_GRM_COMPONENT.csv", "TRE_CN\n1\n")
            return dest

        client = DataMartClient()
        monkeypatch.setattr(client, "_download_file", fake_download)
        paths = client.download_state_archive("RI", tmp_path, show_progress=False)
        assert sorted(paths) == ["PLOT", "TREE_GRM_COMPONENT"]
        assert all(p.exists() for p in paths.values())

    def test_full_download_uses_the_archive(self, tmp_path, monkeypatch):
        from pyfia import downloader

        calls = []
        client = DataMartClient()
        monkeypatch.setattr(
            client,
            "download_state_archive",
            lambda *a, **k: calls.append("archive") or {"PLOT": tmp_path},
        )
        monkeypatch.setattr(
            client,
            "download_tables",
            lambda *a, **k: calls.append("tables") or {"PLOT": tmp_path},
        )
        downloader._download_state_csvs(client, "RI", None, False, tmp_path, False)
        downloader._download_state_csvs(client, "RI", None, True, tmp_path, False)
        downloader._download_state_csvs(client, "RI", ["PLOT"], False, tmp_path, False)
        assert calls == ["archive", "tables", "tables"]


class TestTableNames:
    def test_state_prefix_is_stripped_only_for_the_state(self):
        assert _table_name(Path("GA_TREE_GRM_COMPONENT.csv"), "GA") == (
            "TREE_GRM_COMPONENT",
            True,
        )
        assert _table_name(Path("REF_SPECIES.csv"), "GA") == ("REF_SPECIES", False)
        assert _table_name(Path("EVALIDATOR_POP_ESTIMATE.csv"), "GA") == (
            "EVALIDATOR_POP_ESTIMATE",
            False,
        )
        assert _table_name(Path("BEGINEND.csv"), None) == ("BEGINEND", False)


class TestConvert:
    def test_a_failed_table_discards_the_database(self, tmp_path):
        csv_dir = tmp_path / "csv"
        csv_dir.mkdir()
        _csv(csv_dir / "AL_PLOT.csv", "CN,INVYR\n1,2020\n")
        _csv(csv_dir / "AL_TREE.csv", "CN,SPCD\n1,1.5\n")
        out = tmp_path / "al.duckdb"
        with pytest.raises(DownloadError):
            _convert_csvs_to_duckdb(
                csv_dir, out, state_code=1, state="AL", show_progress=False
            )
        assert not out.exists()

    def test_state_added_only_on_state_tables(self, tmp_path):
        csv_dir = tmp_path / "csv"
        csv_dir.mkdir()
        _csv(csv_dir / "AL_PLOT.csv", "CN,INVYR\n1,2020\n")
        _csv(csv_dir / "REF_UNIT.csv", "CN,STATECD,VALUE,MEANING\n9,1,1,Southwest\n")
        out = _convert_csvs_to_duckdb(
            csv_dir,
            tmp_path / "al.duckdb",
            state_code=1,
            state="AL",
            show_progress=False,
        )
        with duckdb.connect(str(out), read_only=True) as c:
            cols = {
                t: {r[0] for r in c.execute(f"DESCRIBE {t}").fetchall()}
                for t in ("PLOT", "REF_UNIT")
            }
        assert "STATE_ADDED" in cols["PLOT"] and "STATE_ADDED" not in cols["REF_UNIT"]


# -- casts of what is read ----------------------------------------------------


class TestCast:
    def test_text_and_floats_take_fiadb_types(self):
        df = pl.DataFrame(
            {
                "CN": [15, 16],
                "BALIVE": ["3.5", None],
                "HARVEST_TYPE3_SRS": ["16", None],
                "STDORGSP": [131.0, None],
                "MODIFIED_DATE": ["2025-01-21 08:15:39", None],
                "NOT_FIADB": ["x", "y"],
            }
        )
        out = cast_to_fiadb_types(df, "COND")
        assert out.schema == {
            "CN": pl.Utf8,
            "BALIVE": pl.Float64,
            "HARVEST_TYPE3_SRS": pl.Int64,
            "STDORGSP": pl.Int64,
            "MODIFIED_DATE": pl.Datetime("us"),
            "NOT_FIADB": pl.Utf8,
        }
        assert out["HARVEST_TYPE3_SRS"].to_list() == [16, None]

    def test_integer_text_with_a_decimal_point(self):
        out = cast_to_fiadb_types(pl.DataFrame({"SPCD": ["693.0"]}), "TREE")
        assert out["SPCD"].to_list() == [693]

    @pytest.mark.parametrize("value", ["693.5", "abc"])
    def test_values_that_dont_fit_raise(self, value):
        with pytest.raises(ValueError, match="TREE.SPCD"):
            cast_to_fiadb_types(pl.DataFrame({"SPCD": [value]}), "TREE")

    def test_lookup(self):
        assert fiadb_dtype("TREE", "HT") == pl.Int64
        assert fiadb_dtype("TREE", "NOPE") is None


# -- builders give the same output whatever the database stores ---------------

# Columns stored with other types in databases built by CSV type inference
_RETYPED = {
    "PLOT": {"INTENSITY": "VARCHAR", "QA_STATUS": "VARCHAR"},
    "COND": {
        "BALIVE": "VARCHAR",
        "SUBPPROP_UNADJ": "VARCHAR",
        "HARVEST_TYPE3_SRS": "VARCHAR",
        "TRTYR1": "VARCHAR",
    },
    "TREE": {
        "HT": "VARCHAR",
        "VOLCSNET": "VARCHAR",
        "TREEGRCD": "VARCHAR",
        "PREV_TRE_CN": "VARCHAR",
    },
}


@pytest.fixture(scope="module")
def retyped_db(fiadb_fixture_path, tmp_path_factory):
    path = tmp_path_factory.mktemp("retyped") / "fiadb_al_retyped.duckdb"
    with duckdb.connect(str(path)) as con:
        con.execute(f"ATTACH '{fiadb_fixture_path}' AS src (READ_ONLY)")
        tables = [
            r[0]
            for r in con.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_catalog = 'src'"
            ).fetchall()
        ]
        for table in tables:
            cols = [r[0] for r in con.execute(f'DESCRIBE src."{table}"').fetchall()]
            retype = _RETYPED.get(table, {})
            select = ", ".join(
                f'CAST("{c}" AS {retype[c]}) AS "{c}"' if c in retype else f'"{c}"'
                for c in cols
            )
            con.execute(f'CREATE TABLE "{table}" AS SELECT {select} FROM src."{table}"')
    return path


@pytest.mark.parametrize(
    "build",
    [
        lambda db: condition_intervals(db, at_risk_land="timber"),
        lambda db: tree_intervals(db, tree_basis="al", land_basis="forest"),
        lambda db: condition_stand_metrics(db, min_dia=5),
    ],
    ids=["condition_intervals", "tree_intervals", "condition_stand_metrics"],
)
def test_builders_ignore_how_columns_are_stored(fiadb_fixture_path, retyped_db, build):
    expected = build(str(fiadb_fixture_path))
    got = build(str(retyped_db))
    assert got.schema == expected.schema
    assert got.equals(expected)


def test_builder_columns_have_fiadb_types(fiadb_fixture_path):
    pairs = condition_intervals(str(fiadb_fixture_path), at_risk_land="timber")
    for col, dtype in pairs.schema.items():
        base = col.removeprefix("t1_").removeprefix("t2_")
        want = fiadb_dtype("COND", base) or fiadb_dtype("PLOT", base)
        if want is not None:
            assert dtype == want, col
