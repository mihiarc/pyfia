"""
Published biomass totals (green weight, tons) integration checks.

These tests assert statewide timberland biomass totals and a loblolly
forest type subset for Georgia (GA) and South Carolina (SC), based on
published values shared by domain experts.

Notes
- Current biomass() implementation uses DRYBIO_* (dry weight) rather than
  green weight conversions. Until green-weight is implemented, these
  assertions are marked xfail to document targets and prevent regressions
  when the implementation is completed.
- Tests run only when a real FIA DuckDB is configured via PYFIA_DATABASE_PATH.
"""

import os
import pytest
import polars as pl

from pyfia.estimation import biomass


REQUIRES_REAL_DB = not bool(os.getenv("PYFIA_DATABASE_PATH"))


@pytest.mark.skipif(
    REQUIRES_REAL_DB,
    reason="Requires real FIA database set via PYFIA_DATABASE_PATH",
)
@pytest.mark.xfail(reason="Green-weight biomass not yet implemented; using DRYBIO_* fields")
@pytest.mark.parametrize(
    "statecd, total_expected, loblolly_expected",
    [
        (13, 2_400_000_000.0, 700_000_000.0),  # GA: 2.4B total, 0.7B loblolly
        (45, 1_300_000_000.0, 500_000_000.0),  # SC: 1.3B total, 0.5B loblolly
    ],
)
def test_published_biomass_totals_green_weight(real_fia_instance, statecd, total_expected, loblolly_expected):
    """Check statewide timberland biomass totals vs published figures.

    - Total biomass on timberland statewide (green tons)
    - Loblolly forest type (FORTYPCD == 161) subset
    """
    if real_fia_instance is None:
        pytest.skip("No real FIA instance available")

    db = real_fia_instance

    # Clip to state and use most recent evaluations to match publication context
    try:
        db.clip_by_state(statecd, most_recent=True)
    except Exception:
        pytest.skip("Could not clip database to requested state")

    # Statewide totals on timberland (all forest types)
    total_df: pl.DataFrame = biomass(
        db,
        component="TOTAL",
        land_type="timber",
        totals=True,
        by_species=False,
        grp_by=None,
    )
    assert isinstance(total_df, pl.DataFrame)
    assert len(total_df) >= 1
    total_val = float(total_df.select("BIO_ACRE").to_series()[0])

    # Loblolly forest type (FORTYPCD == 161) totals on timberland
    lob_df: pl.DataFrame = biomass(
        db,
        component="TOTAL",
        land_type="timber",
        area_domain="FORTYPCD == 161",
        totals=True,
        by_species=False,
        grp_by=None,
    )
    assert isinstance(lob_df, pl.DataFrame)
    assert len(lob_df) >= 1
    lob_val = float(lob_df.select("BIO_ACRE").to_series()[0])

    # Allow 5% tolerance around published values
    def approx_equal(actual: float, expected: float, pct_tol: float = 0.05) -> bool:
        return abs(actual - expected) <= pct_tol * expected

    assert approx_equal(total_val, total_expected), f"Total biomass {total_val} vs expected {total_expected}"
    assert approx_equal(lob_val, loblolly_expected), f"Loblolly biomass {lob_val} vs expected {loblolly_expected}"


