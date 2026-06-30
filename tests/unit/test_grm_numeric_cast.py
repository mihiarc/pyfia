"""Unit tests for GRM numeric-column casting at load (issue #106).

Some state DuckDBs store FIADB numeric fields (SUBP_TPA*_UNADJ_*, REMPER,
DIA*, DRYBIO_*, ...) as VARCHAR, which broke GRM arithmetic (division on
String, comparing String to numeric). The load_grm_* helpers cast these to
their declared numeric types. These tests feed VARCHAR-typed frames through a
lightweight mock db and assert the loaded dtypes, with no database required.
"""

from types import SimpleNamespace

import polars as pl

from pyfia.estimation.grm import (
    load_grm_begin,
    load_grm_component,
    load_grm_midpt,
    resolve_grm_columns,
)


def _mock_db(table_name, frame):
    """A minimal db whose .tables holds a single lazy frame."""
    return SimpleNamespace(tables={table_name: frame.lazy()})


class TestLoadGRMComponentCast:
    def test_varchar_columns_cast_to_numeric(self):
        cols = resolve_grm_columns("removals", tree_type="gs", land_type="forest")
        frame = pl.DataFrame(
            {
                "TRE_CN": ["1"],
                "PLT_CN": ["10"],
                "DIA_BEGIN": ["5.0"],
                "DIA_MIDPT": ["6.5"],
                "DIA_END": ["7.0"],
                cols.component: ["CUT1"],
                cols.tpa: ["0.595846"],  # VARCHAR TPA, as seen in WY
                cols.subptyp: ["1"],
            }
        )
        db = _mock_db("TREE_GRM_COMPONENT", frame)

        out = load_grm_component(db, cols, include_dia_end=True).collect()

        assert out.schema["TPA_UNADJ"] == pl.Float64
        assert out.schema["SUBPTYP_GRM"] == pl.Int64
        assert out.schema["DIA_MIDPT"] == pl.Float64
        assert out.schema["DIA_BEGIN"] == pl.Float64
        assert out.schema["DIA_END"] == pl.Float64
        # Value preserved through the cast.
        assert abs(out["TPA_UNADJ"][0] - 0.595846) < 1e-9
        assert out["SUBPTYP_GRM"][0] == 1

    def test_already_numeric_unchanged(self):
        cols = resolve_grm_columns("growth", tree_type="gs", land_type="forest")
        frame = pl.DataFrame(
            {
                "TRE_CN": ["1"],
                "PLT_CN": ["10"],
                "DIA_BEGIN": [5.0],
                "DIA_MIDPT": [6.5],
                cols.component: ["SURVIVOR"],
                cols.tpa: [0.5],
                cols.subptyp: [1],
            }
        )
        db = _mock_db("TREE_GRM_COMPONENT", frame)
        out = load_grm_component(db, cols, include_dia_end=False).collect()
        assert out.schema["TPA_UNADJ"] == pl.Float64
        assert out["TPA_UNADJ"][0] == 0.5


class TestLoadGRMMidptCast:
    def test_varchar_measure_columns_cast(self):
        frame = pl.DataFrame(
            {
                "TRE_CN": ["1"],
                "DIA": ["6.5"],
                "SPCD": ["131"],
                "STATUSCD": ["1"],
                "DRYBIO_BOLE": ["100.0"],
                "DRYBIO_BRANCH": ["25.0"],
                "DRYBIO_AG": ["140.0"],
            }
        )
        db = _mock_db("TREE_GRM_MIDPT", frame)

        out = load_grm_midpt(db, measure="biomass").collect()

        assert out.schema["DIA"] == pl.Float64
        assert out.schema["DRYBIO_BOLE"] == pl.Float64
        assert out.schema["DRYBIO_BRANCH"] == pl.Float64
        assert out.schema["DRYBIO_AG"] == pl.Float64
        assert out.schema["SPCD"] == pl.Int64
        assert out.schema["STATUSCD"] == pl.Int64


class TestLoadGRMBeginCast:
    def test_varchar_value_column_cast(self):
        frame = pl.DataFrame({"TRE_CN": ["1"], "DRYBIO_AG": ["120.0"]})
        db = _mock_db("TREE_GRM_BEGIN", frame)
        out = load_grm_begin(db, measure="biomass").collect()
        assert out.schema["DRYBIO_AG"] == pl.Float64
        assert out["DRYBIO_AG"][0] == 120.0
