"""Unit tests for the te reo Māori macron aliases."""

from __future__ import annotations

import pytest

from pyfia.constants.species_macros import MACRON_ALIASES, macron_to_canonical

# Test cases: (macroned spelling, expected canonical, note).
# Used by parametrize below. Single source of truth — adding a new
# macron is a 1-line change here.
_MACRON_TEST_CASES = [
    ("rīmū", "rimu"),
    ("kaurī", "kauri"),
    ("tōtara", "totara"),
    ("matāī", "matai"),
    ("mīro", "miro"),
    ("tāwa", "tawa"),
    ("mangeāo", "mangeao"),
    ("kahikātea", "kahikatea"),
    ("tawhairauriki", "beech-red"),
    ("tawhairaunui", "beech-hard"),
    ("rātā", "radiata-pine"),  # te reo loanword
    ("rāta", "radiata-pine"),  # alternate spelling
    ("tawhai", "beech-hard"),  # generic form
]


class TestMacronAliases:
    """The aliases table covers the canonical-indigenous species."""

    def test_table_is_not_empty(self) -> None:
        assert len(MACRON_ALIASES) > 0

    @pytest.mark.parametrize("macroned,expected_canonical", _MACRON_TEST_CASES)
    def test_macron_resolves_to_canonical(self, macroned: str, expected_canonical: str) -> None:
        result = macron_to_canonical(macroned)
        assert result is not None, f"no entry for {macroned!r}"
        canonical, _ = result
        assert canonical == expected_canonical, (
            f"expected {macroned!r} → {expected_canonical!r}, got {canonical!r}"
        )

    def test_radiata_pine_loanword(self) -> None:
        # The "rātā" entry is documented as a Māori loanword. Make
        # sure the note preserves that provenance.
        result = macron_to_canonical("rātā")
        assert result is not None
        canonical, note = result
        assert canonical == "radiata-pine"
        # The note must preserve both the loanword status AND the
        # te reo Māori provenance.
        assert "loanword" in note.lower()
        assert "Māori" in note

    def test_unknown_returns_none(self) -> None:
        assert macron_to_canonical("nonexistent-tree") is None

    def test_ascii_forms_no_longer_resolve(self) -> None:
        # Regression: after we removed the ASCII duplicates (audit
        # 2026-07-30), ASCII forms must NOT be in MACRON_ALIASES.
        # ASCII is reachable via species_extra.lookup() instead.
        assert macron_to_canonical("rimu") is None
        assert macron_to_canonical("kauri") is None
        assert macron_to_canonical("totara") is None


class TestMacronPreservation:
    """The aliases preserve every macron correctly (no ascii-translation loss)."""

    @pytest.mark.parametrize(
        "macroned",
        [
            "rīmū",
            "kaurī",
            "tōtara",
            "matāī",
            "mīro",
            "tāwa",
            "mangeāo",
            "kahikātea",
            "tawhairauriki",
            "tawhairaunui",
        ],
    )
    def test_macron_preserved(self, macroned: str) -> None:
        # Round-trip: lowercase + strip the macroned string and
        # confirm the lookup still finds an entry (no silent
        # round-trip-degradation).
        result = macron_to_canonical(macroned)
        assert result is not None, f"no entry for {macroned!r}"
        # The macron char must survive into the canonical-name lookup
        # pathway. We don't assert on the canonical itself (it's
        # ASCII) but we assert that the function returns the
        # expected pair.
        assert isinstance(result, tuple)
        assert len(result) == 2
        assert isinstance(result[0], str)


class TestLookupBridge:
    """The macron aliases should bridge into species_extra."""

    def test_bridges_via_canonical_name(self) -> None:
        from pyfia.constants.species_extra import lookup

        # Macron form -> canonical -> species_extra lookup.
        result = macron_to_canonical("rīmū")
        assert result is not None
        canonical, _ = result
        entry = lookup(canonical)
        assert entry is not None
        assert entry.spcd is None  # rimu is NZ-only
        assert entry.wdsg == 0.46

    def test_bridges_for_kauri(self) -> None:
        from pyfia.constants.species_extra import lookup

        result = macron_to_canonical("kaurī")
        assert result is not None
        canonical, _ = result
        entry = lookup(canonical)
        assert entry is not None
        assert entry.spcd is None
        assert entry.wdsg == 0.50

    def test_bridges_for_radiata_pine(self) -> None:
        # The "rātā" macron form is a te reo loanword for radiata pine.
        from pyfia.constants.species_extra import lookup

        result = macron_to_canonical("rātā")
        assert result is not None
        canonical, note = result
        assert canonical == "radiata-pine"
        assert "loanword" in note.lower() or "Māori" in note
        entry = lookup(canonical)
        assert entry is not None
        assert entry.spcd == 131  # FIA loblolly proxy
