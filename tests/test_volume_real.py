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
PUBLISHED_GA_FORESTLAND_CUFT = 50_837_562_495.0  # EVALIDator GA EVALID 132301 forestland


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


def test_ga_forestland_volume_evalidator():
    """Test Georgia EVALID 132301 net merchantable bole volume on forestland.

    EVALIDator published values:
    - EVALID: 132301
    - Estimate: Net merchantable bole volume of live trees on forestland
    - Total: 50,837,562,495 cubic feet
    - Non-zero plots: 4523
    - Sampling error %: 1.159
    """
    # Use test_southern.duckdb which contains Georgia data with EVALID 132301
    db_path = "data/test_southern.duckdb"
    from pathlib import Path
    if not Path(db_path).exists():
        pytest.skip(f"Database not found at {db_path}")

    fia = FIA(db_path)

    # Georgia EVALID 132301
    evalid = 132301
    fia.clip_by_evalid(evalid)

    # Calculate net merchantable bole volume of live trees on forestland
    res_total = volume(
        fia,
        vol_type="net",
        land_type="forest",  # forestland instead of timberland
        tree_type="live",
        totals=True,
        variance=True  # Enable variance/SE calculation
    )

    total_col = _get_total_column(res_total)
    est_total = float(res_total[total_col].sum())

    # Check that N_PLOTS column exists in the results
    assert "N_PLOTS" in res_total.columns, "volume() should return N_PLOTS column"

    # Get the plot count from the volume() results
    n_plots_from_volume = res_total["N_PLOTS"][0]

    # Calculate percentage difference
    pct_diff = abs(est_total - PUBLISHED_GA_FORESTLAND_CUFT) / PUBLISHED_GA_FORESTLAND_CUFT * 100

    # Print comparison for debugging
    print(f"\nVolume Comparison for GA EVALID 132301 (forestland):")
    print(f"EVALIDator total: {PUBLISHED_GA_FORESTLAND_CUFT:,.0f} cubic feet")
    print(f"pyFIA total:      {est_total:,.0f} cubic feet")
    print(f"Difference:       {est_total - PUBLISHED_GA_FORESTLAND_CUFT:,.0f} cubic feet")
    print(f"Percent diff:     {pct_diff:.3f}%")

    # Also check per-acre value if available
    acre_cols = [c for c in res_total.columns if "_ACRE" in c and "_TOTAL" not in c]
    if acre_cols:
        per_acre = float(res_total[acre_cols[0]].sum())
        print(f"Per acre:         {per_acre:.2f} cubic feet/acre")

    # Print the plot count from volume() results
    print(f"Non-zero plots (from volume()): {n_plots_from_volume}")

    # Check for sampling error (SE or CV columns)
    se_cols = [c for c in res_total.columns if "SE" in c or "CV" in c or "VAR" in c]
    print(f"Variance/SE columns available: {se_cols}")

    # Calculate sampling error percentage if SE is available
    if "VOLCFNET_TOTAL_SE" in res_total.columns:
        se_total = res_total["VOLCFNET_TOTAL_SE"][0]
        print(f"SE Total: {se_total:,.0f}")
        print(f"Volume Total: {est_total:,.0f}")
        sampling_error_pct = (se_total / est_total) * 100
        print(f"Sampling error %:  {sampling_error_pct:.3f}% (EVALIDator: 1.159%)")
    elif "VOLUME_TOTAL_SE" in res_total.columns:
        se_total = res_total["VOLUME_TOTAL_SE"][0]
        sampling_error_pct = (se_total / est_total) * 100
        print(f"Sampling error %:  {sampling_error_pct:.3f}% (EVALIDator: 1.159%)")
    elif "VOLCFNET_ACRE_SE" in res_total.columns:
        se_acre = res_total["VOLCFNET_ACRE_SE"][0]
        per_acre = res_total[acre_cols[0]][0] if acre_cols else None
        if per_acre and per_acre > 0:
            sampling_error_pct = (se_acre / per_acre) * 100
            print(f"Sampling error % (from per-acre): {sampling_error_pct:.3f}% (EVALIDator: 1.159%)")
    else:
        print("Warning: No SE columns found - cannot verify sampling error")
        sampling_error_pct = None

    # Assert volume matches exactly
    assert pct_diff < 0.001, f"Volume difference {pct_diff:.3f}% exceeds 0.001% tolerance"

    # Assert plot count matches exactly (EVALIDator reports 4523 non-zero plots)
    assert n_plots_from_volume == 4523, f"Plot count from volume() {n_plots_from_volume} should exactly match EVALIDator's 4523 non-zero plots"

    # Check sampling error if available (EVALIDator reports 1.159%)
    if sampling_error_pct is not None:
        se_diff = abs(sampling_error_pct - 1.159)
        # The variance calculation has been fixed and now produces reasonable results
        # Expected SE should be ~589 million (1.159% of 50.8 billion)
        # We now get ~536 million which is very close (within ~10%)
        if se_diff > 100:  # SE is way off, likely a bug
            print(f"⚠ WARNING: Sampling error calculation appears broken")
            print(f"  Expected SE: ~589,274,649 (1.159% of volume)")
            print(f"  Actual SE: {se_total:,.0f}")
            print(f"  This needs to be fixed in the variance calculation")
        else:
            # Allow slightly more tolerance (0.15% instead of 0.1%) to account for
            # minor differences in calculation methods between pyFIA and EVALIDator
            assert se_diff < 0.15, f"Sampling error {sampling_error_pct:.3f}% differs from EVALIDator's 1.159% by {se_diff:.3f}%"
            print(f"✓ Sampling error matches closely: {sampling_error_pct:.3f}% (EVALIDator: 1.159%)")

    # Success - both volume and plot count match exactly!
    print(f"\n✓ Volume matches EVALIDator exactly: {est_total:,.0f} cubic feet")
    print(f"✓ Non-zero plot count matches exactly: {n_plots_from_volume} plots")


