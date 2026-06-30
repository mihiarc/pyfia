"""CI-safe unit tests for the variance-column output contract (#109).

pyFIA always reports standard errors (``*_SE``). Variance columns are opt-in
via ``variance=True``, added as ``*_VARIANCE = (*_SE) ** 2`` without ever
dropping the standard-error columns. This is enforced centrally by
``apply_variance_columns`` so the contract is uniform across every estimator.
"""

import polars as pl
import pytest

from pyfia.estimation.utils import apply_variance_columns


@pytest.fixture
def sample() -> pl.DataFrame:
    """Mimic a formatted estimator output (per-acre + total estimates + SE)."""
    return pl.DataFrame(
        {
            "YEAR": [2023],
            "VOLCFNET_ACRE": [100.0],
            "VOLCFNET_TOTAL": [1_000_000.0],
            "VOLCFNET_ACRE_SE": [3.0],
            "VOLCFNET_TOTAL_SE": [50_000.0],
            "N_PLOTS": [42],
        }
    )


class TestApplyVarianceColumns:
    def test_default_is_standard_errors_only(self, sample):
        """variance=False: SE columns kept, no variance columns added."""
        out = apply_variance_columns(sample, variance=False)
        assert "VOLCFNET_TOTAL_SE" in out.columns
        assert not [c for c in out.columns if "_VARIANCE" in c]

    def test_variance_true_adds_variance_keeps_se(self, sample):
        """variance=True: adds *_VARIANCE = SE**2 and keeps every *_SE column."""
        out = apply_variance_columns(sample, variance=True)
        # SE columns are never removed.
        assert "VOLCFNET_ACRE_SE" in out.columns
        assert "VOLCFNET_TOTAL_SE" in out.columns
        # Matching variance columns are added with SE-mirrored names.
        assert out["VOLCFNET_ACRE_VARIANCE"][0] == pytest.approx(3.0**2)
        assert out["VOLCFNET_TOTAL_VARIANCE"][0] == pytest.approx(50_000.0**2)
        # Non-uncertainty columns are untouched.
        assert out["VOLCFNET_TOTAL"][0] == 1_000_000.0
        assert out["N_PLOTS"][0] == 42

    def test_percentage_se_naming(self):
        """AREA_SE_PERCENT -> AREA_VARIANCE_PERCENT (the '_SE' token is replaced)."""
        df = pl.DataFrame({"AREA_SE": [10.0], "AREA_SE_PERCENT": [0.5]})
        out = apply_variance_columns(df, variance=True)
        assert out["AREA_VARIANCE"][0] == pytest.approx(100.0)
        assert out["AREA_VARIANCE_PERCENT"][0] == pytest.approx(0.25)

    def test_preexisting_variance_dropped_when_off(self):
        """Inconsistent pre-existing variance columns are removed when off."""
        df = pl.DataFrame({"VOLCFNET_TOTAL_SE": [5.0], "VOLUME_TOTAL_VARIANCE": [99.0]})
        out = apply_variance_columns(df, variance=False)
        assert "VOLUME_TOTAL_VARIANCE" not in out.columns
        assert "VOLCFNET_TOTAL_SE" in out.columns

    def test_preexisting_variance_renamed_when_on(self):
        """Pre-existing variance columns are replaced by SE-derived ones."""
        df = pl.DataFrame({"VOLCFNET_TOTAL_SE": [5.0], "VOLUME_TOTAL_VARIANCE": [99.0]})
        out = apply_variance_columns(df, variance=True)
        # Old inconsistent name gone; SE-mirrored name present and correct.
        assert "VOLUME_TOTAL_VARIANCE" not in out.columns
        assert out["VOLCFNET_TOTAL_VARIANCE"][0] == pytest.approx(25.0)

    def test_no_se_columns_is_noop(self):
        """A frame with no SE columns is returned unchanged when variance=True."""
        df = pl.DataFrame({"YEAR": [2023], "TPA": [150.0]})
        out = apply_variance_columns(df, variance=True)
        assert out.columns == ["YEAR", "TPA"]
