"""Unit tests for StandingDeadEstimator's NSVB woodland handling (issue #8).

The standing-dead carbon math and FIADB parity are validated in
``tests/validation/test_standing_dead_nsvb.py`` against a real database; this
file only checks the estimator plumbing for the woodland-species fix that was
ported from ``live_tree`` (issue #6/#8) — that ``CARBON_AG`` is requested and
that the shared base-class guard/substitution are wired in. These run in CI
unconditionally against a MockDB.
"""

from __future__ import annotations

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
