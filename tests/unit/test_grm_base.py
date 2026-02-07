"""Unit tests for GRM base estimator functionality.

Tests for the GRMBaseEstimator class and shared GRM estimation logic.
"""

from unittest.mock import MagicMock

import pytest

from pyfia.core.exceptions import NoEVALIDError


class TestGRMEvalidFilter:
    """Test EVALID filtering in GRM estimation.

    GRM estimates (growth, mortality, removals) require filtering to a specific
    EVALID to avoid counting trees multiple times across different evaluations.

    Bug context (GitHub issue): Without EVALID filtering, MORT_TOTAL can be ~60x
    too high because the same trees appear in multiple EVALIDs across annual
    evaluations. Per-acre values remain correct (ratio cancels), but totals are
    inflated.
    """

    def _create_test_estimator(self, mock_db, component_type="mortality"):
        """Create a minimal test estimator that uses _ensure_grm_evalid_filter."""
        from pyfia.estimation.grm_base import GRMBaseEstimator

        class TestEstimator:
            def __init__(self, db):
                self.db = db
                self._grm_columns = None

            @property
            def component_type(self):
                return component_type

            _ensure_grm_evalid_filter = GRMBaseEstimator._ensure_grm_evalid_filter

        return TestEstimator(mock_db)

    def test_raises_error_when_no_evalid_set(self):
        """Should raise NoEVALIDError when no EVALID is set."""
        mock_db = MagicMock()
        mock_db.evalid = None

        estimator = self._create_test_estimator(mock_db)

        with pytest.raises(NoEVALIDError, match="mortality estimation"):
            estimator._ensure_grm_evalid_filter()

    def test_no_action_when_evalid_already_set(self):
        """Should not raise when EVALID is already set."""
        mock_db = MagicMock()
        mock_db.evalid = [132303]  # EVALID already set

        estimator = self._create_test_estimator(mock_db)

        # Should not raise
        estimator._ensure_grm_evalid_filter()

    def test_error_mentions_correct_component_type(self):
        """Error message should include the specific component type."""
        for component_type in ["mortality", "growth", "removals"]:
            mock_db = MagicMock()
            mock_db.evalid = None

            estimator = self._create_test_estimator(mock_db, component_type)

            with pytest.raises(NoEVALIDError, match=component_type):
                estimator._ensure_grm_evalid_filter()

    def test_error_includes_resolution_instructions(self):
        """Error message should tell users how to fix the issue."""
        mock_db = MagicMock()
        mock_db.evalid = None

        estimator = self._create_test_estimator(mock_db)

        with pytest.raises(NoEVALIDError, match="clip_by_evalid"):
            estimator._ensure_grm_evalid_filter()

    def test_no_auto_filter_side_effect(self):
        """Should NOT call clip_most_recent (no auto-filtering)."""
        mock_db = MagicMock()
        mock_db.evalid = None

        estimator = self._create_test_estimator(mock_db)

        with pytest.raises(NoEVALIDError):
            estimator._ensure_grm_evalid_filter()

        # clip_most_recent should NOT have been called
        mock_db.clip_most_recent.assert_not_called()
