"""Unit tests for find_evalid() EVALID selection logic.

Verifies that find_evalid(most_recent=True) correctly selects the most
recent evaluation using END_INVYR, especially for states with single-digit
FIPS codes that produce 5-digit EVALIDs.
"""

from unittest.mock import MagicMock, patch

import polars as pl
import pytest


@pytest.fixture
def mock_fia():
    """Create a mock FIA instance with the real find_evalid method."""
    from pyfia.core.fia import FIA

    with patch.object(FIA, "__init__", lambda self: None):
        db = FIA()
        db.tables = {}
        db.evalid = None
        db.state_filter = None
        db._reader = MagicMock()
        return db


def _setup_eval_tables(mock_fia, pop_eval_rows, pop_eval_typ_rows):
    """Helper to set up POP_EVAL and POP_EVAL_TYP tables on mock FIA."""
    mock_fia.tables["POP_EVAL"] = pl.DataFrame(pop_eval_rows).lazy()
    mock_fia.tables["POP_EVAL_TYP"] = pl.DataFrame(pop_eval_typ_rows).lazy()


class TestSingleDigitFIPSCode:
    """find_evalid must handle 5-digit EVALIDs from single-digit FIPS codes."""

    def test_alabama_selects_most_recent_by_end_invyr(self, mock_fia):
        """Alabama (FIPS=1) produces 5-digit EVALIDs. Most recent should win."""
        _setup_eval_tables(
            mock_fia,
            pop_eval_rows={
                "CN": ["eval_old", "eval_new"],
                "EVALID": [10301, 12401],
                "STATECD": [1, 1],
                "END_INVYR": [2003, 2024],
                "LOCATION_NM": ["Alabama", "Alabama"],
            },
            pop_eval_typ_rows={
                "CN": ["typ_old", "typ_new"],
                "EVAL_CN": ["eval_old", "eval_new"],
                "EVAL_TYP": ["EXPVOL", "EXPVOL"],
            },
        )

        result = mock_fia.find_evalid(most_recent=True, eval_type="VOL")

        assert result == [12401], (
            f"Expected EVALID 12401 (END_INVYR=2024), got {result}. "
            "5-digit EVALID parsing may be broken."
        )

    def test_arkansas_selects_most_recent(self, mock_fia):
        """Arkansas (FIPS=5) also produces 5-digit EVALIDs."""
        _setup_eval_tables(
            mock_fia,
            pop_eval_rows={
                "CN": ["eval_old", "eval_new"],
                "EVALID": [50501, 52201],
                "STATECD": [5, 5],
                "END_INVYR": [2005, 2022],
                "LOCATION_NM": ["Arkansas", "Arkansas"],
            },
            pop_eval_typ_rows={
                "CN": ["typ_old", "typ_new"],
                "EVAL_CN": ["eval_old", "eval_new"],
                "EVAL_TYP": ["EXPVOL", "EXPVOL"],
            },
        )

        result = mock_fia.find_evalid(most_recent=True, eval_type="VOL")

        assert result == [52201]


class TestTwoDigitFIPSCode:
    """Standard 2-digit FIPS codes should continue to work."""

    def test_georgia_selects_most_recent(self, mock_fia):
        """Georgia (FIPS=13) produces standard 6-digit EVALIDs."""
        _setup_eval_tables(
            mock_fia,
            pop_eval_rows={
                "CN": ["eval_old", "eval_new"],
                "EVALID": [131901, 132301],
                "STATECD": [13, 13],
                "END_INVYR": [2019, 2023],
                "LOCATION_NM": ["Georgia", "Georgia"],
            },
            pop_eval_typ_rows={
                "CN": ["typ_old", "typ_new"],
                "EVAL_CN": ["eval_old", "eval_new"],
                "EVAL_TYP": ["EXPVOL", "EXPVOL"],
            },
        )

        result = mock_fia.find_evalid(most_recent=True, eval_type="VOL")

        assert result == [132301]


class TestMultiStateSelection:
    """find_evalid should pick the most recent per state in multi-state DBs."""

    def test_picks_most_recent_per_state(self, mock_fia):
        """Each state should get its own most recent evaluation."""
        _setup_eval_tables(
            mock_fia,
            pop_eval_rows={
                "CN": ["al_old", "al_new", "ga_old", "ga_new"],
                "EVALID": [10301, 12401, 131901, 132301],
                "STATECD": [1, 1, 13, 13],
                "END_INVYR": [2003, 2024, 2019, 2023],
                "LOCATION_NM": ["Alabama", "Alabama", "Georgia", "Georgia"],
            },
            pop_eval_typ_rows={
                "CN": ["t1", "t2", "t3", "t4"],
                "EVAL_CN": ["al_old", "al_new", "ga_old", "ga_new"],
                "EVAL_TYP": ["EXPVOL", "EXPVOL", "EXPVOL", "EXPVOL"],
            },
        )

        result = mock_fia.find_evalid(most_recent=True, eval_type="VOL")

        assert sorted(result) == [12401, 132301]


class TestTiebreaking:
    """When END_INVYR ties, EVALID descending should break the tie."""

    def test_same_end_invyr_picks_higher_evalid(self, mock_fia):
        """Higher EVALID wins when END_INVYR is the same."""
        _setup_eval_tables(
            mock_fia,
            pop_eval_rows={
                "CN": ["eval_a", "eval_b"],
                "EVALID": [132300, 132301],
                "STATECD": [13, 13],
                "END_INVYR": [2023, 2023],
                "LOCATION_NM": ["Georgia", "Georgia"],
            },
            pop_eval_typ_rows={
                "CN": ["typ_a", "typ_b"],
                "EVAL_CN": ["eval_a", "eval_b"],
                "EVAL_TYP": ["EXPVOL", "EXPVOL"],
            },
        )

        result = mock_fia.find_evalid(most_recent=True, eval_type="VOL")

        assert result == [132301]
