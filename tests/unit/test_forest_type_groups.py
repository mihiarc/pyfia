"""
Forest type groups equal FIADB's REF tables (#135).

``pyfia.constants.forest_types`` is generated from ``REF_FOREST_TYPE`` and
``REF_FOREST_TYPE_GROUP``. These tests compare it, and the functions built on
it, with the REF tables in the committed Alabama fixture.
"""

import warnings

import duckdb
import polars as pl
import pytest

from pyfia import area
from pyfia.constants.forest_types import (
    FIADB_VERSION,
    FOREST_TYPE_GROUP_CODES,
    FOREST_TYPE_GROUP_NAMES,
)
from pyfia.filtering.utils import (
    add_forest_type_group,
    get_forest_type_group,
    get_forest_type_group_code,
)


@pytest.fixture(scope="module")
def ref(fiadb_fixture_path):
    with duckdb.connect(str(fiadb_fixture_path), read_only=True) as con:
        codes = dict(
            con.execute("SELECT VALUE, TYPGRPCD FROM REF_FOREST_TYPE").fetchall()
        )
        names = dict(
            con.execute("SELECT VALUE, MEANING FROM REF_FOREST_TYPE_GROUP").fetchall()
        )
        (version,) = con.execute(
            "SELECT VERSION FROM REF_FIADB_VERSION ORDER BY CREATED_DATE DESC LIMIT 1"
        ).fetchone()
        cond_codes = [
            r[0]
            for r in con.execute(
                "SELECT DISTINCT FORTYPCD FROM COND WHERE FORTYPCD IS NOT NULL"
            ).fetchall()
        ]
    return {"codes": codes, "names": names, "version": version, "cond": cond_codes}


def test_constants_equal_ref_tables(ref):
    assert ref["version"] == FIADB_VERSION
    assert ref["codes"] == FOREST_TYPE_GROUP_CODES
    assert ref["names"] == FOREST_TYPE_GROUP_NAMES


def test_every_ref_code_maps_to_its_typgrpcd(ref):
    # The #135 reproduction: 102 of 207 codes disagreed in 1.4.3.
    bad = [
        (code, group)
        for code, group in ref["codes"].items()
        if get_forest_type_group_code(code) != group
        or get_forest_type_group(code) != ref["names"][group]
    ]
    assert bad == []


def test_every_fixture_condition_maps(ref):
    df = pl.DataFrame({"FORTYPCD": ref["cond"]})
    with warnings.catch_warnings():
        warnings.simplefilter("error", UserWarning)
        result = add_forest_type_group(df)
    assert "Unknown" not in result["FOREST_TYPE_GROUP"].to_list()


def test_estimator_output_gets_ref_labels(fiadb_fixture, ref):
    """area(grp_by="FORTYPCD") attaches FOREST_TYPE_GROUP from REF."""
    fiadb_fixture.clip_by_evalid(12401)
    result = area(fiadb_fixture, grp_by="FORTYPCD", land_type="forest")
    pairs = result.select("FORTYPCD", "FOREST_TYPE_GROUP").drop_nulls("FORTYPCD")
    assert pairs.height > 0
    for code, label in pairs.iter_rows():
        assert label == ref["names"][ref["codes"][code]]
    southern = pairs.filter(pl.col("FORTYPCD").is_in([141, 161]))
    assert set(southern["FOREST_TYPE_GROUP"]) <= {
        "Longleaf / slash pine group",
        "Loblolly / shortleaf pine group",
    }
