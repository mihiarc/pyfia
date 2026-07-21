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


class TestNullEndInvyrExclusion:
    """A periodic eval's NULL END_INVYR must not outrank a dated annual eval.

    Regression test for issue #130: polars' default nulls_last=False sorts
    NULL first under descending=True, so the periodic eval (no END_INVYR)
    was winning over the current annual eval.
    """

    def test_california_skips_null_end_invyr_periodic_eval(self, mock_fia):
        """CA has a 1994 periodic eval (NULL END_INVYR, EVALID 69401) that
        must lose to the 2021 annual eval (END_INVYR=2021, EVALID 62101)
        despite 69401 > 62101 numerically.
        """
        _setup_eval_tables(
            mock_fia,
            pop_eval_rows={
                "CN": ["ca_periodic", "ca_annual"],
                "EVALID": [69401, 62101],
                "STATECD": [6, 6],
                "END_INVYR": [None, 2021],
                "LOCATION_NM": ["California", "California"],
            },
            pop_eval_typ_rows={
                "CN": ["t1", "t2"],
                "EVAL_CN": ["ca_periodic", "ca_annual"],
                "EVAL_TYP": ["EXPVOL", "EXPVOL"],
            },
        )

        result = mock_fia.find_evalid(most_recent=True, eval_type="VOL")

        assert result == [62101], (
            f"Expected the 2021 annual EVALID 62101, got {result}. "
            "NULL END_INVYR periodic eval is winning the sort again."
        )

    def test_texas_full_state_skips_null_end_invyr_periodic_eval(self, mock_fia):
        """Texas has its own sort branch (full-state vs East/West); it must
        get the same NULL-END_INVYR fix as the general branch.
        """
        _setup_eval_tables(
            mock_fia,
            pop_eval_rows={
                "CN": ["tx_periodic", "tx_annual"],
                "EVALID": [489901, 482301],
                "STATECD": [48, 48],
                "END_INVYR": [None, 2023],
                "LOCATION_NM": ["Texas", "Texas"],
            },
            pop_eval_typ_rows={
                "CN": ["t1", "t2"],
                "EVAL_CN": ["tx_periodic", "tx_annual"],
                "EVAL_TYP": ["EXPVOL", "EXPVOL"],
            },
        )

        result = mock_fia.find_evalid(most_recent=True, eval_type="VOL")

        assert result == [482301]


class TestShortLegacyEvalid:
    """Sub-5-digit legacy EVALIDs (Alaska 110/111) must not crash or win the sort.

    Regression test for issue #119: the old positional EVALID parser cast the
    integer EVALID to a string and sliced it into state/year/type fields; a
    3-digit EVALID like 111 produced an empty ``[4:6]`` slice that raised on the
    subsequent ``cast(Int32)``, crashing ``area()`` (and any estimator routed
    through ``clip_most_recent``) on real Alaska databases. find_evalid now sorts
    on the unambiguous 4-digit END_INVYR and never parses EVALID positionally, so
    the short 2003 periodic eval both survives and correctly loses to the 2021
    annual eval. These values mirror Alaska's actual POP_EVAL rows.
    """

    def test_alaska_short_evalid_does_not_crash_or_win(self, mock_fia):
        """AK (FIPS=2) has 3-digit 2003 periodic EVALIDs alongside the 2021
        coastal annual eval (22101). The annual eval must win, and parsing the
        short EVALID must not raise."""
        _setup_eval_tables(
            mock_fia,
            pop_eval_rows={
                "CN": ["ak_periodic", "ak_annual"],
                "EVALID": [111, 22101],
                "STATECD": [2, 2],
                "END_INVYR": [2003, 2021],
                "LOCATION_NM": ["Alaska", "Alaska Coastal"],
            },
            pop_eval_typ_rows={
                "CN": ["t1", "t2"],
                "EVAL_CN": ["ak_periodic", "ak_annual"],
                "EVAL_TYP": ["EXPVOL", "EXPVOL"],
            },
        )

        result = mock_fia.find_evalid(most_recent=True, eval_type="VOL")

        assert result == [22101], (
            f"Expected the 2021 annual EVALID 22101, got {result}. "
            "Short 3-digit EVALID handling (issue #119) may have regressed."
        )

    def test_alaska_short_evalid_listed_without_error(self, mock_fia):
        """most_recent=False returns every EVALID, including the short legacy
        ones, without attempting to positionally parse them into fields."""
        _setup_eval_tables(
            mock_fia,
            pop_eval_rows={
                "CN": ["ak_periodic", "ak_annual"],
                "EVALID": [111, 22101],
                "STATECD": [2, 2],
                "END_INVYR": [2003, 2021],
                "LOCATION_NM": ["Alaska", "Alaska Coastal"],
            },
            pop_eval_typ_rows={
                "CN": ["t1", "t2"],
                "EVAL_CN": ["ak_periodic", "ak_annual"],
                "EVAL_TYP": ["EXPVOL", "EXPVOL"],
            },
        )

        result = mock_fia.find_evalid(most_recent=False, eval_type="VOL")

        assert sorted(result) == [111, 22101]
