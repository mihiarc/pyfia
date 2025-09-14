"""
Real-data tests for merchantable bole volume totals using FIA DuckDB.

Requires environment variable PYFIA_DATABASE_PATH pointing to fia.duckdb.
"""

import os
import math
import polars as pl
import pytest

from pyfia import FIA
from pyfia.estimation import volume
import duckdb


def _get_total_column(df: pl.DataFrame) -> str:
    # Prefer explicit net bole total column; otherwise any *_TOTAL
    preferred = [
        "VOLCFNET_ACRE_TOTAL",
        "VOLUME_TOTAL",
        "VOLCFNET_TOTAL",
    ]
    for col in preferred:
        if col in df.columns:
            return col
    for c in df.columns:
        if c.endswith("_TOTAL"):
            return c
    raise AssertionError("No TOTAL column found in volume results")


PUBLISHED_GA_TOTAL_CUFT = 49_706_497_327.0599
PUBLISHED_SC_TOTAL_CUFT = 28_617_126_475.8494


@pytest.mark.usefixtures("use_real_data")
def test_ga_volume_totals_real(use_real_data):
    if not use_real_data:
        pytest.skip("Real database not configured")

    db_path = os.getenv("PYFIA_DATABASE_PATH")
    fia = FIA(db_path)
    # Georgia (explicit EVALID from published query)
    evalid = 132301
    fia.clip_by_state(13, most_recent=True)
    fia.clip_by_evalid(evalid)

    # Total merchantable bole volume on timberland (net cubic feet)
    res_total = volume(fia, vol_type="net", land_type="timber", totals=True)
    total_col = _get_total_column(res_total)
    ga_total_bil = float(res_total[total_col].sum()) / 1e9

    # Loblolly pine SPCD=131
    res_sp = volume(fia, vol_type="net", land_type="timber", by_species=True, totals=True)
    lob = res_sp.filter(pl.col("SPCD") == 131)
    assert lob.height > 0, "No loblolly rows returned"
    lob_col = total_col if total_col in lob.columns else _get_total_column(lob)
    ga_lob_bil = float(lob[lob_col].sum()) / 1e9

    # Assert against published GA total (cubic feet)
    est_total = float(res_total[total_col].sum())
    assert math.isclose(est_total, PUBLISHED_GA_TOTAL_CUFT, rel_tol=0.01)


@pytest.mark.usefixtures("use_real_data")
def test_sc_volume_totals_real(use_real_data):
    if not use_real_data:
        pytest.skip("Real database not configured")

    db_path = os.getenv("PYFIA_DATABASE_PATH")
    fia = FIA(db_path)
    # South Carolina (explicit EVALID from published query)
    evalid = 452301
    fia.clip_by_state(45, most_recent=True)
    fia.clip_by_evalid(evalid)

    # Total merchantable bole volume on timberland (net cubic feet)
    res_total = volume(fia, vol_type="net", land_type="timber", totals=True)
    total_col = _get_total_column(res_total)
    sc_total_bil = float(res_total[total_col].sum()) / 1e9

    # Loblolly pine SPCD=131
    res_sp = volume(fia, vol_type="net", land_type="timber", by_species=True, totals=True)
    lob = res_sp.filter(pl.col("SPCD") == 131)
    assert lob.height > 0, "No loblolly rows returned"
    lob_col = total_col if total_col in lob.columns else _get_total_column(lob)
    sc_lob_bil = float(lob[lob_col].sum()) / 1e9

    # Assert against published SC total (cubic feet)
    est_total = float(res_total[total_col].sum())
    assert math.isclose(est_total, PUBLISHED_SC_TOTAL_CUFT, rel_tol=0.01)


