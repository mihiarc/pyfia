"""
Unit tests for NSVB coefficient table loaders and lookup precedence.

Verifies:

1. CSV bundle loads via ``importlib.resources`` (wheel-safe)
2. All 10 SPCD/Jenkins tables are present with reasonable row counts
3. The ``@lru_cache`` returns the same instance on repeat calls
4. ``lookup_coefficients`` walks the precedence levels in the documented order
5. Species-level fallback works for SPCDs that lack DIVISION-specific rows
6. Jenkins fallback works for the level-4 case
7. KeyError is raised for SPCDs with no coverage at any level
"""

from __future__ import annotations

import polars as pl
import pytest

from pyfia.carbon.nsvb.coefficients import (
    CoefficientTables,
    load_nsvb_coefficients,
    lookup_coefficients,
)


class TestLoadNSVBCoefficients:
    """CSV bundle loading via importlib.resources."""

    def test_returns_coefficient_tables_dataclass(self):
        coefs = load_nsvb_coefficients()
        assert isinstance(coefs, CoefficientTables)

    def test_lru_cache_returns_same_instance(self):
        """Repeat calls hit the cache and return the same object."""
        first = load_nsvb_coefficients()
        second = load_nsvb_coefficients()
        assert first is second

    def test_all_ten_tables_present(self):
        coefs = load_nsvb_coefficients()
        assert isinstance(coefs.volib_spcd, pl.DataFrame)
        assert isinstance(coefs.volib_jenkins, pl.DataFrame)
        assert isinstance(coefs.volbk_spcd, pl.DataFrame)
        assert isinstance(coefs.volbk_jenkins, pl.DataFrame)
        assert isinstance(coefs.bark_biomass_spcd, pl.DataFrame)
        assert isinstance(coefs.bark_biomass_jenkins, pl.DataFrame)
        assert isinstance(coefs.branch_biomass_spcd, pl.DataFrame)
        assert isinstance(coefs.branch_biomass_jenkins, pl.DataFrame)
        assert isinstance(coefs.total_biomass_spcd, pl.DataFrame)
        assert isinstance(coefs.total_biomass_jenkins, pl.DataFrame)

    def test_table_row_counts(self):
        """Verify the vendored tables have the expected number of rows.

        These counts come directly from the WO-104 supplementary archive
        and serve as a regression check that we haven't accidentally
        replaced or corrupted a CSV during vendoring.
        """
        coefs = load_nsvb_coefficients()
        assert coefs.volib_spcd.height == 406
        assert coefs.volib_jenkins.height == 9
        assert coefs.volbk_spcd.height == 339
        assert coefs.volbk_jenkins.height == 9
        assert coefs.bark_biomass_spcd.height == 206
        assert coefs.bark_biomass_jenkins.height == 9
        assert coefs.branch_biomass_spcd.height == 175
        assert coefs.branch_biomass_jenkins.height == 9
        assert coefs.total_biomass_spcd.height == 173
        assert coefs.total_biomass_jenkins.height == 9

    def test_spcd_table_schema(self):
        """All ``*_spcd`` tables share the same column structure."""
        coefs = load_nsvb_coefficients()
        expected = {
            "SPCD",
            "DIVISION",
            "STDORGCD",
            "model",
            "a",
            "a1",
            "b",
            "b1",
            "c",
            "c1",
        }
        for tbl in (
            coefs.volib_spcd,
            coefs.volbk_spcd,
            coefs.total_biomass_spcd,
        ):
            cols = set(tbl.columns)
            assert expected.issubset(cols), f"Missing columns: {expected - cols}"


class TestLookupCoefficients:
    """NSVB coefficient lookup precedence (4 levels)."""

    def test_spcd_only_fallback_douglas_fir(self):
        """SPCD=202 species-level row exists in S1a (volib).

        With ``division=None``, lookup should hit Level 3 and return the
        species-level row.
        """
        coefs = load_nsvb_coefficients()
        result = lookup_coefficients(
            coefs.volib_spcd,
            coefs.volib_jenkins,
            spcd=202,
            jenkins_spgrpcd=10,  # arbitrary, won't be used
        )
        assert result["source"] == "spcd"
        assert result["model"] == 2  # Douglas-fir uses Model 2 for stem wood vol
        # Coefficients should be non-zero — exact values depend on the species-level row
        assert result["a"] > 0
        assert result["b"] > 0

    def test_spcd_only_fallback_red_maple(self):
        """SPCD=316 species-level row exists in S1a (volib).

        Red maple uses Model 1 for stem wood volume at the species level.
        """
        coefs = load_nsvb_coefficients()
        result = lookup_coefficients(
            coefs.volib_spcd,
            coefs.volib_jenkins,
            spcd=316,
            jenkins_spgrpcd=8,  # arbitrary
        )
        assert result["source"] == "spcd"
        assert result["model"] == 1

    def test_division_lookup_when_division_provided(self):
        """When DIVISION is provided AND a row exists, prefer Level 2."""
        coefs = load_nsvb_coefficients()
        result = lookup_coefficients(
            coefs.volib_spcd,
            coefs.volib_jenkins,
            spcd=202,
            division="240",  # Marine division — Douglas-fir has a row here
            jenkins_spgrpcd=10,
        )
        assert result["source"] == "spcd_division"
        assert result["model"] == 2

    def test_division_lookup_falls_back_to_species_when_missing(self):
        """When DIVISION is provided BUT no row exists, fall through to Level 3.

        Per the red maple worked example: SPCD=316 has no DIVISION=M210 row
        in S1a, so lookup falls back to the species-level row.
        """
        coefs = load_nsvb_coefficients()
        result = lookup_coefficients(
            coefs.volib_spcd,
            coefs.volib_jenkins,
            spcd=316,
            division="M210",  # No row exists for 316/M210 — should fall back
            jenkins_spgrpcd=8,
        )
        assert result["source"] == "spcd"
        assert result["model"] == 1  # red maple species-level uses Model 1

    def test_total_biomass_red_maple_uses_model_4(self):
        """Critical regression: SPCD=316 in S8a uses Model 4, not Model 1.

        This pins the Model 4 wiring — if someone accidentally changes the
        equation dispatch, this test will catch it.
        """
        coefs = load_nsvb_coefficients()
        result = lookup_coefficients(
            coefs.total_biomass_spcd,
            coefs.total_biomass_jenkins,
            spcd=316,
            jenkins_spgrpcd=8,
        )
        assert result["source"] == "spcd"
        assert result["model"] == 4
        # Model 4 uses b1; the species-level row should have a non-zero b1
        assert result["b1"] != 0.0

    def test_keyerror_for_unsupported_spcd(self):
        """SPCD with no coverage at any level should raise KeyError."""
        coefs = load_nsvb_coefficients()
        with pytest.raises(KeyError, match="No NSVB coefficients"):
            lookup_coefficients(
                coefs.volib_spcd,
                coefs.volib_jenkins,
                spcd=99999,  # impossible SPCD
                jenkins_spgrpcd=None,
            )

    def test_jenkins_fallback_when_jenkins_table_keyed(self):
        """Level 4 Jenkins fallback path.

        Note: the Jenkins tables in the vendored CSVs use ``JENKINS_SPGRPCD``
        as the key column. We verify the fallback returns a Jenkins-tagged
        result when a non-existent SPCD is paired with a valid Jenkins group.
        """
        coefs = load_nsvb_coefficients()
        # Pick a Jenkins group that exists in the table
        jenkins_groups = coefs.volib_jenkins["JENKINS_SPGRPCD"].to_list()
        valid_jenkins = jenkins_groups[0]
        result = lookup_coefficients(
            coefs.volib_spcd,
            coefs.volib_jenkins,
            spcd=99999,  # not in S1a
            jenkins_spgrpcd=valid_jenkins,
        )
        assert result["source"] == "jenkins"
