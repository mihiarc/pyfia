"""
Unit tests for the NSVB carbon fraction lookup module.

Verifies:

1. S10a CSV loads correctly with the trimmed schema (SPCD, hw_sw, fia_wood_c)
2. Percent → decimal normalization (the source CSV stores e.g. 48.04, not 0.4804)
3. SPCD=202 (Douglas-fir) and SPCD=316 (red maple) lookups match worked-example values
4. Unknown SPCD falls back to the national mean and emits exactly one warning
5. S10b dead carbon fractions load with the (hw_sw, decaycd) keying
"""

from __future__ import annotations

import logging

from pyfia.carbon.nsvb import carbon_fractions
from pyfia.carbon.nsvb.carbon_fractions import (
    DEFAULT_LIVE_CARBON_FRACTION,
    get_carbon_fraction_dead,
    get_carbon_fraction_live,
    load_carbon_fractions_dead,
    load_carbon_fractions_live,
)


class TestLoadCarbonFractionsLive:
    """S10a loader: ``carbon_fraction_live.csv`` → dict[SPCD] -> float."""

    def test_returns_dict(self):
        result = load_carbon_fractions_live()
        assert isinstance(result, dict)

    def test_has_2676_species(self):
        """The trimmed S10a has 2676 species rows."""
        result = load_carbon_fractions_live()
        assert len(result) == 2676

    def test_lru_cache(self):
        first = load_carbon_fractions_live()
        second = load_carbon_fractions_live()
        assert first is second

    def test_values_in_decimal_form(self):
        """All values must be in decimal form, not percent.

        Bounds are [0.30, 0.65] — wide enough to capture the actual S10a
        range (min 0.365, max 0.609 across the 2676 species) but tight
        enough to detect a percent-vs-decimal unit error (which would put
        values at 30-65 instead of 0.30-0.65). The spec section 3.1.2
        cited "~0.474 mean" but the per-species range is wider than I
        initially assumed — measured directly from the vendored CSV.
        """
        result = load_carbon_fractions_live()
        for spcd, frac in result.items():
            assert 0.30 <= frac <= 0.65, f"SPCD={spcd} has out-of-range fraction {frac}"

    def test_mean_close_to_population_mean(self):
        """The S10a population mean is ~0.474 per spec section 3.1.2."""
        result = load_carbon_fractions_live()
        mean = sum(result.values()) / len(result)
        assert 0.46 <= mean <= 0.49, (
            f"Population mean {mean:.4f} outside expected range"
        )


class TestGetCarbonFractionLive:
    """Per-species lookup with fallback."""

    def test_douglas_fir_matches_worked_example(self):
        """SPCD=202 should be ~0.5156 per S10a (worked example line 680)."""
        result = get_carbon_fraction_live(202)
        # Expected: 51.55958333 / 100 = 0.5155958333
        assert abs(result - 0.5155958333) < 1e-6

    def test_red_maple_matches_s10a(self):
        """SPCD=316 red maple should be ~0.4857 per S10a."""
        result = get_carbon_fraction_live(316)
        # Expected: 48.57333333 / 100 = 0.4857333333
        assert abs(result - 0.4857333333) < 1e-6

    def test_unknown_spcd_returns_fallback(self):
        """SPCD with no S10a entry returns the national-mean default."""
        result = get_carbon_fraction_live(99999)
        assert result == DEFAULT_LIVE_CARBON_FRACTION

    def test_unknown_spcd_warns_only_once(self, caplog):
        """First lookup of an unknown SPCD logs a warning; subsequent
        lookups for the same SPCD do not (to avoid log spam)."""
        # Reset the warned-set so this test is reproducible
        carbon_fractions._warned_unknown_spcds.clear()

        with caplog.at_level(
            logging.WARNING, logger="pyfia.carbon.nsvb.carbon_fractions"
        ):
            get_carbon_fraction_live(99998)
            get_carbon_fraction_live(99998)
            get_carbon_fraction_live(99998)

        warning_count = sum(1 for r in caplog.records if "99998" in r.message)
        assert warning_count == 1

    def test_default_fallback_value(self):
        """The default fallback is the documented national mean."""
        # Per spec section 3.1.2: ~0.4716
        assert abs(DEFAULT_LIVE_CARBON_FRACTION - 0.4716) < 1e-4

    def test_custom_fallback(self):
        """Caller can override the fallback value."""
        result = get_carbon_fraction_live(99997, fallback=0.50)
        assert result == 0.50


class TestLoadCarbonFractionsDead:
    """S10b loader: dead tree carbon fractions by hw/sw × decay class."""

    def test_returns_dict(self):
        result = load_carbon_fractions_dead()
        assert isinstance(result, dict)

    def test_ten_rows(self):
        """S10b has 10 entries: 2 hw/sw × 5 decay classes."""
        result = load_carbon_fractions_dead()
        assert len(result) == 10

    def test_lookup_hardwood_decay_3(self):
        """Verify the hardwood/decay-3 entry exists and is reasonable."""
        result = get_carbon_fraction_dead("hardwood", 3)
        # Expected: ~0.473 (47.3%) per S10b
        assert 0.46 <= result <= 0.49

    def test_lookup_softwood_decay_5(self):
        """Verify the softwood/decay-5 entry exists."""
        result = get_carbon_fraction_dead("softwood", 5)
        assert 0.50 <= result <= 0.55

    def test_case_insensitive_hw_sw(self):
        """``Hardwood`` and ``hardwood`` should both work."""
        a = get_carbon_fraction_dead("Hardwood", 1)
        b = get_carbon_fraction_dead("hardwood", 1)
        assert a == b

    def test_all_decay_classes_present(self):
        """Every decay class 1-5 must exist for both hardwood and softwood."""
        for hw_sw in ("hardwood", "softwood"):
            for decay in range(1, 6):
                result = get_carbon_fraction_dead(hw_sw, decay)
                assert 0.40 <= result <= 0.55, (
                    f"({hw_sw}, decay={decay}) -> {result} out of range"
                )
