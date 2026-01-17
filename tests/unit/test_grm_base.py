"""Unit tests for GRM base estimator functionality.

Tests for the GRMBaseEstimator class and shared GRM estimation logic.
"""

import warnings
from unittest.mock import MagicMock

import pytest


class TestGRMEvalidAutoFilter:
    """Test EVALID auto-filtering in GRM estimation.

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

    def test_auto_filters_when_no_evalid_set(self):
        """Should auto-filter to GRM evaluation when no EVALID is set."""
        mock_db = MagicMock()
        mock_db.evalid = None
        clip_calls = []

        def mock_clip_most_recent(eval_type):
            clip_calls.append(eval_type)
            mock_db.evalid = [132303]
            return mock_db

        mock_db.clip_most_recent = mock_clip_most_recent

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")

            estimator = self._create_test_estimator(mock_db)
            estimator._ensure_grm_evalid_filter()

            # Should issue a warning
            assert len(w) == 1
            assert "No EVALID filter set" in str(w[0].message)
            assert "mortality" in str(w[0].message)

            # Should call clip_most_recent with 'GRM'
            assert clip_calls == ["GRM"]

            # EVALID should now be set
            assert mock_db.evalid is not None

    def test_no_action_when_evalid_already_set(self):
        """Should not issue warning or auto-filter when EVALID is already set."""
        mock_db = MagicMock()
        mock_db.evalid = [132303]  # EVALID already set
        mock_db.clip_most_recent = MagicMock()

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")

            estimator = self._create_test_estimator(mock_db)
            estimator._ensure_grm_evalid_filter()

            # Should NOT issue any warning
            assert len(w) == 0

            # Should NOT call clip_most_recent
            mock_db.clip_most_recent.assert_not_called()

    def test_raises_error_when_auto_filter_fails(self):
        """Should raise clear error when auto-filter fails."""
        mock_db = MagicMock()
        mock_db.evalid = None
        mock_db.clip_most_recent.side_effect = Exception("No GRM evaluations found")

        estimator = self._create_test_estimator(mock_db)

        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            with pytest.raises(ValueError, match="Could not auto-filter"):
                estimator._ensure_grm_evalid_filter()

    def test_warning_mentions_correct_component_type(self):
        """Warning message should include the specific component type."""
        for component_type in ["mortality", "growth", "removals"]:
            mock_db = MagicMock()
            mock_db.evalid = None

            def make_clip_fn(db):
                def clip_fn(eval_type):
                    db.evalid = [123]
                    return db

                return clip_fn

            mock_db.clip_most_recent = make_clip_fn(mock_db)

            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")

                estimator = self._create_test_estimator(mock_db, component_type)
                estimator._ensure_grm_evalid_filter()

                assert len(w) == 1
                assert component_type in str(w[0].message)
