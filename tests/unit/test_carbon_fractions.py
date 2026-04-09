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

import polars as pl

from pyfia.carbon.nsvb import carbon_fractions
from pyfia.carbon.nsvb.carbon_fractions import (
    DEFAULT_LIVE_CARBON_FRACTION,
    get_carbon_fraction_dead,
    get_carbon_fraction_live,
    load_carbon_fractions_dead,
    load_carbon_fractions_live,
    load_carbon_fractions_live_df,
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
        """The default fallback is the S10a arithmetic mean.

        Computed lazily from the vendored S10a table on first access (PEP 562
        ``__getattr__``). The value tracks the CSV so a future re-vendor
        automatically updates the constant — this is the regression guard
        against the hardcoded 0.4716 value that had drifted from the actual
        table mean (~0.4741).
        """
        # Matches GTR-WO-104 "live ~47.4% mean" notation.
        assert 0.47 < DEFAULT_LIVE_CARBON_FRACTION < 0.48
        # Must exactly equal the arithmetic mean of the loaded dict.
        table = load_carbon_fractions_live()
        expected = sum(table.values()) / len(table)
        assert DEFAULT_LIVE_CARBON_FRACTION == expected

    def test_custom_fallback(self):
        """Caller can override the fallback value."""
        result = get_carbon_fraction_live(99997, fallback=0.50)
        assert result == 0.50


class TestLoadCarbonFractionsLiveDf:
    """S10a polars loader: the join-ready DataFrame used by the vectorized path."""

    def test_returns_polars_dataframe(self):
        result = load_carbon_fractions_live_df()
        assert isinstance(result, pl.DataFrame)

    def test_columns(self):
        """Schema is exactly (SPCD Int64, CARBON_FRAC_LIVE Float64)."""
        result = load_carbon_fractions_live_df()
        assert result.columns == ["SPCD", "CARBON_FRAC_LIVE"]
        assert result.schema["SPCD"] == pl.Int64
        assert result.schema["CARBON_FRAC_LIVE"] == pl.Float64

    def test_matches_dict_loader(self):
        """The DataFrame loader must agree with the dict loader row-for-row.

        Both loaders consume the same vendored CSV, so every SPCD in one
        must appear in the other with the same float value. This catches
        drift if someone modifies one loader without the other.
        """
        df = load_carbon_fractions_live_df()
        table = load_carbon_fractions_live()
        assert df.height == len(table)
        for row in df.iter_rows(named=True):
            assert row["SPCD"] in table
            assert abs(row["CARBON_FRAC_LIVE"] - table[row["SPCD"]]) < 1e-12

    def test_percent_to_decimal_conversion(self):
        """Values are in decimal form (e.g., 0.4804), not percent (48.04)."""
        df = load_carbon_fractions_live_df()
        # All fractions should be in the [0.3, 0.7] range
        assert df["CARBON_FRAC_LIVE"].min() > 0.3
        assert df["CARBON_FRAC_LIVE"].max() < 0.7

    def test_lru_cache_returns_same_instance(self):
        first = load_carbon_fractions_live_df()
        second = load_carbon_fractions_live_df()
        assert first is second

    def test_join_ready(self):
        """The loader output is suitable for a left join to a trees frame."""
        df = load_carbon_fractions_live_df()
        trees = pl.DataFrame({"SPCD": [202, 316, 99999]})
        joined = trees.join(df, on="SPCD", how="left")
        # Douglas-fir and red maple should have non-null CARBON_FRAC_LIVE
        assert joined.filter(pl.col("SPCD") == 202)["CARBON_FRAC_LIVE"][0] is not None
        assert joined.filter(pl.col("SPCD") == 316)["CARBON_FRAC_LIVE"][0] is not None
        # Unknown SPCD should have null (caller fills with DEFAULT)
        assert joined.filter(pl.col("SPCD") == 99999)["CARBON_FRAC_LIVE"][0] is None


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
