"""Unit tests for StandingDeadEstimator's NSVB woodland handling (issue #8).

The standing-dead carbon math and FIADB parity are validated in
``tests/validation/test_standing_dead_nsvb.py`` against a real database; this
file only checks the estimator plumbing for the woodland-species fix that was
ported from ``live_tree`` (issue #6/#8) — that ``CARBON_AG`` is requested and
that the shared base-class guard/substitution are wired in. These run in CI
unconditionally against a MockDB.
"""

from __future__ import annotations

import polars as pl

from pyfia.carbon.standing_dead import StandingDeadEstimator


class MockDB:
    """Minimal stand-in matching the pattern in ``test_live_tree_estimator``."""

    def __init__(self):
        self.db_path = "/fake/path"
        self.tables = {}
        self.evalid = None
        self.evalids = None
        self._state_filter = None
        self._reader = None


class TestGetTreeColumns:
    def test_loads_carbon_ag_for_woodland_substitution(self):
        """CARBON_AG is requested so woodland standing-dead trees can be routed
        to the FIADB-stored value instead of recomputing to 0 (issue #8)."""
        estimator = StandingDeadEstimator(MockDB(), {"pool": "ag"})
        cols = estimator.get_tree_columns()
        assert "CARBON_AG" in cols
        # Still loads the standing-dead-specific columns + the BG bridge.
        assert "STANDING_DEAD_CD" in cols
        assert "DECAYCD" in cols
        assert "CARBON_BG" in cols

    def test_inherits_shared_woodland_helpers(self):
        """The estimator uses the base-class guard + substitution (shared with
        live_tree) rather than redefining them."""
        estimator = StandingDeadEstimator(MockDB(), {"pool": "ag"})
        assert hasattr(estimator, "_guard_nsvb_coverage")
        assert hasattr(estimator, "_substitute_woodland_carbon_ag")


class TestStandingDeadCdNumericMatch:
    """``STANDING_DEAD_CD`` is matched numerically, not as a string (issue #126).

    The previous ``cast(Utf8) == "1"`` compared against the literal ``"1"``,
    so a backend that loads the column as Float64 (``1.0`` -> ``"1.0"``) would
    silently drop *every* standing-dead tree and return ~0 carbon. The fix
    (``cast(Int64) == 1``) matches across VARCHAR / BIGINT / Float64 backends.
    ``apply_filters`` is column-conditional, so a minimal synthetic frame drives
    exactly the standing-dead filters.
    """

    def _apply(self, standing_dead_cd_values):
        estimator = StandingDeadEstimator(MockDB(), {"pool": "ag", "tree_type": "dead"})
        n = len(standing_dead_cd_values)
        frame = pl.LazyFrame(
            {
                "STATUSCD": [2] * n,
                "STANDING_DEAD_CD": standing_dead_cd_values,
                "DECAYCD": [3] * n,
                "DIA": [10.0] * n,
            }
        )
        return estimator.apply_filters(frame).collect()

    def test_float_backend_retains_standing_dead(self):
        # Float64 backend: values arrive as 1.0 / 0.0 / 2.0 / null.
        result = self._apply([1.0, 0.0, 2.0, None])
        assert result.height == 1
        assert result["STANDING_DEAD_CD"].to_list() == [1.0]

    def test_int_backend_retains_standing_dead(self):
        result = self._apply([1, 0, 2])
        assert result.height == 1

    def test_string_backend_retains_standing_dead(self):
        result = self._apply(["1", "0", "2"])
        assert result.height == 1
