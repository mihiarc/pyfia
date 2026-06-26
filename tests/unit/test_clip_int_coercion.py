"""Unit tests for int coercion in clip_by_evalid / clip_by_state (#87).

The clip methods are typed ``int | list[int]`` but historically did no
coercion, so a non-int value would flow raw into an f-string ``IN (...)``
clause. Coercing to int enforces the contract and neutralizes any untrusted
value (e.g. ``"37 OR 1=1"`` raises instead of being interpolated).
"""

from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture
def mock_fia():
    """A FIA instance without a real database connection."""
    from pyfia.core.fia import FIA

    with patch.object(FIA, "__init__", lambda self: None):
        db = FIA()
        db.tables = {}
        db.evalid = None
        db.state_filter = None
        db._valid_plot_cns = None
        db._spatial_plot_cns = None
        db._reader = MagicMock()
        return db


class TestClipByEvalidCoercion:
    def test_single_int(self, mock_fia):
        mock_fia.clip_by_evalid(132401)
        assert mock_fia.evalid == [132401]

    def test_list_of_ints(self, mock_fia):
        mock_fia.clip_by_evalid([132401, 132301])
        assert mock_fia.evalid == [132401, 132301]

    def test_numeric_string_is_coerced(self, mock_fia):
        mock_fia.clip_by_evalid("132401")
        assert mock_fia.evalid == [132401]
        assert all(isinstance(e, int) for e in mock_fia.evalid)

    def test_mixed_list_coerced_to_int(self, mock_fia):
        mock_fia.clip_by_evalid([132401, "132301"])
        assert mock_fia.evalid == [132401, 132301]

    def test_injection_attempt_rejected(self, mock_fia):
        with pytest.raises(ValueError, match="clip_by_evalid"):
            mock_fia.clip_by_evalid("132401 OR 1=1")
        # Nothing should have been stored from the bad call.
        assert mock_fia.evalid is None


class TestClipByStateCoercion:
    def test_bad_value_rejected_before_db_access(self, mock_fia):
        with patch.object(mock_fia, "find_evalid") as find:
            with pytest.raises(ValueError, match="clip_by_state"):
                mock_fia.clip_by_state("37; DROP TABLE PLOT")
            find.assert_not_called()

    def test_numeric_inputs_coerced(self, mock_fia):
        with patch.object(mock_fia, "find_evalid", return_value=[]):
            mock_fia.clip_by_state(["37", 13])
        assert mock_fia.state_filter == [37, 13]
        assert all(isinstance(s, int) for s in mock_fia.state_filter)
