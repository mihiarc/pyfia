"""Unit tests for eval_type token resolution and the "GRM" alias (issue #102).

clip_most_recent(eval_type="GRM") previously built a non-existent "EXPGRM"
EVAL_TYP and always raised NoEVALIDError. These tests pin the token->EVAL_TYP
mapping, the "GRM" family alias, raw-code passthrough, and the helpful error
for unknown tokens. All are CI-safe (no database required).
"""

from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from pyfia.core.fia import resolve_eval_typ_codes


class TestResolveEvalTypCodes:
    """Pure-function token resolution."""

    def test_grm_expands_to_family(self):
        assert resolve_eval_typ_codes("GRM") == ["EXPGROW", "EXPMORT", "EXPREMV"]

    def test_single_tokens(self):
        assert resolve_eval_typ_codes("VOL") == ["EXPVOL"]
        assert resolve_eval_typ_codes("GROW") == ["EXPGROW"]
        assert resolve_eval_typ_codes("MORT") == ["EXPMORT"]
        assert resolve_eval_typ_codes("REMV") == ["EXPREMV"]
        assert resolve_eval_typ_codes("ALL") == ["EXPALL"]

    def test_case_insensitive(self):
        assert resolve_eval_typ_codes("grm") == ["EXPGROW", "EXPMORT", "EXPREMV"]
        assert resolve_eval_typ_codes("  Vol ") == ["EXPVOL"]

    def test_raw_code_passthrough(self):
        # Callers may pass a raw EVAL_TYP code directly.
        assert resolve_eval_typ_codes("EXPALL") == ["EXPALL"]
        assert resolve_eval_typ_codes("EXPVOL") == ["EXPVOL"]

    def test_unknown_token_raises_with_options(self):
        with pytest.raises(ValueError) as exc:
            resolve_eval_typ_codes("EXPGRM")
        msg = str(exc.value)
        assert "EXPGRM" in msg
        # The message must list valid options including the GRM alias.
        assert "GRM" in msg
        assert "GROW" in msg

    def test_other_unknown_token_raises(self):
        with pytest.raises(ValueError):
            resolve_eval_typ_codes("NONSENSE")


@pytest.fixture
def mock_fia():
    """Mock FIA instance exposing the real find_evalid method."""
    from pyfia.core.fia import FIA

    with patch.object(FIA, "__init__", lambda self: None):
        db = FIA()
        db.tables = {}
        db.evalid = None
        db.state_filter = None
        db._reader = MagicMock()
        return db


class TestGRMAliasResolution:
    """find_evalid(eval_type="GRM") resolves to the shared family EVALID."""

    def test_grm_resolves_to_shared_family_evalid(self, mock_fia):
        # EXPGROW/EXPMORT/EXPREMV share one EVALID per state; a separate VOL
        # evaluation must not be selected by the GRM alias.
        mock_fia.tables["POP_EVAL"] = pl.DataFrame(
            {
                "CN": ["grow", "mort", "remv", "vol"],
                "EVALID": [132403, 132403, 132403, 132401],
                "STATECD": [13, 13, 13, 13],
                "END_INVYR": [2024, 2024, 2024, 2024],
                "LOCATION_NM": ["Georgia"] * 4,
            }
        ).lazy()
        mock_fia.tables["POP_EVAL_TYP"] = pl.DataFrame(
            {
                "CN": ["t1", "t2", "t3", "t4"],
                "EVAL_CN": ["grow", "mort", "remv", "vol"],
                "EVAL_TYP": ["EXPGROW", "EXPMORT", "EXPREMV", "EXPVOL"],
            }
        ).lazy()

        result = mock_fia.find_evalid(most_recent=True, eval_type="GRM", state=[13])

        assert result == [132403], (
            f"GRM alias should resolve to the shared family EVALID 132403, got {result}"
        )

    def test_grm_picks_most_recent_family_evalid(self, mock_fia):
        # Two annual GRM evaluations; the most recent shared EVALID wins.
        mock_fia.tables["POP_EVAL"] = pl.DataFrame(
            {
                "CN": ["g23", "m23", "r23", "g24", "m24", "r24"],
                "EVALID": [132303, 132303, 132303, 132403, 132403, 132403],
                "STATECD": [13, 13, 13, 13, 13, 13],
                "END_INVYR": [2023, 2023, 2023, 2024, 2024, 2024],
                "LOCATION_NM": ["Georgia"] * 6,
            }
        ).lazy()
        mock_fia.tables["POP_EVAL_TYP"] = pl.DataFrame(
            {
                "CN": ["a", "b", "c", "d", "e", "f"],
                "EVAL_CN": ["g23", "m23", "r23", "g24", "m24", "r24"],
                "EVAL_TYP": [
                    "EXPGROW",
                    "EXPMORT",
                    "EXPREMV",
                    "EXPGROW",
                    "EXPMORT",
                    "EXPREMV",
                ],
            }
        ).lazy()

        result = mock_fia.find_evalid(most_recent=True, eval_type="GRM", state=[13])

        assert result == [132403]
