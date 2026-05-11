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
    VectorizedLookupTables,
    build_division_lookup,
    build_jenkins_lookup,
    build_species_level_lookup,
    ecosubcd_to_division,
    get_vectorized_lookup_tables,
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

    def test_spcd_table_explicit_dtypes(self):
        """All ``*_spcd`` tables are loaded with explicit schemas.

        Regression test for the fix that replaced ``infer_schema_length=10_000``
        with ``schema_overrides``. DIVISION must be ``Utf8`` (ecoprovince codes
        like "M240") and STDORGCD must be ``Int64`` — the upcoming vectorized
        lookup joins rely on these dtypes being stable regardless of which
        rows happen to be at the top of the CSV on a future re-vendor.

        Note: the bark/branch biomass tables have a narrower schema
        (``a, b, b1, c`` only, no ``a1`` / ``c1``) than volume/total biomass.
        """
        coefs = load_nsvb_coefficients()
        for tbl in (
            coefs.volib_spcd,
            coefs.volbk_spcd,
            coefs.bark_biomass_spcd,
            coefs.branch_biomass_spcd,
            coefs.total_biomass_spcd,
        ):
            assert tbl.schema["SPCD"] == pl.Int64
            assert tbl.schema["DIVISION"] == pl.Utf8
            assert tbl.schema["STDORGCD"] == pl.Int64
            assert tbl.schema["model"] == pl.Int64
            # Common numeric columns present in every table
            for col in ("a", "b", "b1", "c"):
                assert tbl.schema[col] == pl.Float64, (
                    f"{col} in {tbl.columns}: expected Float64, got {tbl.schema[col]}"
                )
            # a1 / c1 only exist in volib_spcd, volbk_spcd, total_biomass_spcd
            for col in ("a1", "c1"):
                if col in tbl.columns:
                    assert tbl.schema[col] == pl.Float64

    def test_jenkins_table_explicit_dtypes(self):
        """All ``*_jenkins`` tables are loaded with explicit schemas."""
        coefs = load_nsvb_coefficients()
        for tbl in (
            coefs.volib_jenkins,
            coefs.volbk_jenkins,
            coefs.bark_biomass_jenkins,
            coefs.branch_biomass_jenkins,
            coefs.total_biomass_jenkins,
        ):
            assert tbl.schema["JENKINS_SPGRPCD"] == pl.Int64
            assert tbl.schema["model"] == pl.Int64
            assert tbl.schema["a"] == pl.Float64
            assert tbl.schema["b"] == pl.Float64
            assert tbl.schema["c"] == pl.Float64


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


class TestBuildSpeciesLevelLookup:
    """Vectorized path helper: species-level row filter for *_spcd tables."""

    def test_selected_columns(self):
        """Returns only the columns the vectorized biomass expression needs."""
        coefs = load_nsvb_coefficients()
        result = build_species_level_lookup(coefs.volib_spcd)
        assert result.columns == ["SPCD", "model", "a", "b", "b1", "c"]

    def test_keeps_only_species_level_rows(self):
        """Rows where DIVISION or STDORGCD is non-null are filtered out.

        Phase 1 has no ECOSUBCD → DIVISION mapping, so DIVISION-specific
        rows (Levels 1-2 of the NSVB precedence) are dead code and must be
        dropped before the vectorized join runs.
        """
        coefs = load_nsvb_coefficients()
        result = build_species_level_lookup(coefs.volib_spcd)
        # Original table has 406 rows; species-level is a strict subset
        assert result.height < coefs.volib_spcd.height
        assert result.height > 0

    def test_douglas_fir_row_present(self):
        """SPCD=202 Douglas-fir has a species-level row with Model 2 for volib."""
        coefs = load_nsvb_coefficients()
        result = build_species_level_lookup(coefs.volib_spcd)
        dougfir = result.filter(pl.col("SPCD") == 202)
        assert dougfir.height == 1
        assert dougfir["model"][0] == 2

    def test_spcd_unique_in_output(self):
        """Each SPCD appears at most once after filtering — single row per species."""
        coefs = load_nsvb_coefficients()
        for tbl in (
            coefs.volib_spcd,
            coefs.volbk_spcd,
            coefs.bark_biomass_spcd,
            coefs.branch_biomass_spcd,
            coefs.total_biomass_spcd,
        ):
            result = build_species_level_lookup(tbl)
            assert result["SPCD"].is_unique().all()

    def test_works_on_narrow_biomass_tables(self):
        """bark_biomass_spcd / branch_biomass_spcd have no a1/c1 but still
        expose (a, b, b1, c) — the builder must handle both schemas."""
        coefs = load_nsvb_coefficients()
        for tbl in (coefs.bark_biomass_spcd, coefs.branch_biomass_spcd):
            result = build_species_level_lookup(tbl)
            assert result.columns == ["SPCD", "model", "a", "b", "b1", "c"]
            assert result.height > 0


class TestBuildJenkinsLookup:
    """Vectorized path helper: Jenkins fallback table for *_jenkins tables."""

    def test_selected_columns(self):
        """Returns uniform columns including synthesized b1=0."""
        coefs = load_nsvb_coefficients()
        result = build_jenkins_lookup(coefs.volib_jenkins)
        assert result.columns == [
            "JENKINS_SPGRPCD",
            "model",
            "a",
            "b",
            "b1",
            "c",
        ]

    def test_b1_is_always_zero(self):
        """Jenkins tables have no b1 column; the synthesized value is 0.0.

        Model 5 (the only form Jenkins rows dispatch to) does not use b1,
        so any value is correct; 0.0 keeps the coalesce in the orchestrator
        from emitting a null when the downstream expression reads b1.
        """
        coefs = load_nsvb_coefficients()
        result = build_jenkins_lookup(coefs.volib_jenkins)
        assert (result["b1"] == 0.0).all()
        assert result.schema["b1"] == pl.Float64

    def test_all_jenkins_rows_preserved(self):
        """No filtering — Jenkins tables are used as-is (all 9 groups)."""
        coefs = load_nsvb_coefficients()
        result = build_jenkins_lookup(coefs.volib_jenkins)
        assert result.height == coefs.volib_jenkins.height
        assert result.height == 9


class TestGetVectorizedLookupTables:
    """Cached bundle of all 5 component lookup pairs."""

    def test_returns_vectorized_bundle(self):
        bundle = get_vectorized_lookup_tables()
        assert isinstance(bundle, VectorizedLookupTables)

    def test_lru_cache_returns_same_instance(self):
        first = get_vectorized_lookup_tables()
        second = get_vectorized_lookup_tables()
        assert first is second

    def test_all_ten_tables_present_and_ready_to_join(self):
        """Each table has the vectorized-path column layout."""
        bundle = get_vectorized_lookup_tables()

        spcd_expected = ["SPCD", "model", "a", "b", "b1", "c"]
        jen_expected = ["JENKINS_SPGRPCD", "model", "a", "b", "b1", "c"]

        for name in (
            "volib_spcd",
            "volbk_spcd",
            "bark_bio_spcd",
            "branch_bio_spcd",
            "total_agb_spcd",
        ):
            tbl = getattr(bundle, name)
            assert tbl.columns == spcd_expected, f"{name} has wrong columns"
            assert tbl.height > 0

        for name in (
            "volib_jen",
            "volbk_jen",
            "bark_bio_jen",
            "branch_bio_jen",
            "total_agb_jen",
        ):
            tbl = getattr(bundle, name)
            assert tbl.columns == jen_expected, f"{name} has wrong columns"
            assert tbl.height == 9  # 9 Jenkins species groups

    def test_douglas_fir_volib_model_2(self):
        """Regression sentinel: SPCD=202 volib row is Model 2 (stem wood k=9)."""
        bundle = get_vectorized_lookup_tables()
        row = bundle.volib_spcd.filter(pl.col("SPCD") == 202).to_dicts()[0]
        assert row["model"] == 2

    def test_red_maple_total_agb_model_4(self):
        """Regression sentinel: SPCD=316 total_agb row is Model 4."""
        bundle = get_vectorized_lookup_tables()
        row = bundle.total_agb_spcd.filter(pl.col("SPCD") == 316).to_dicts()[0]
        assert row["model"] == 4

    def test_division_lookups_present(self):
        """Each component now has a DIVISION-keyed lookup (Level 2)."""
        bundle = get_vectorized_lookup_tables()
        div_expected = ["SPCD", "DIVISION", "model", "a", "b", "b1", "c"]
        for name in (
            "volib_div",
            "volbk_div",
            "bark_bio_div",
            "branch_bio_div",
            "total_agb_div",
        ):
            tbl = getattr(bundle, name)
            assert tbl.columns == div_expected, f"{name} has wrong columns"
            # Every division row has a non-null DIVISION code
            assert tbl["DIVISION"].null_count() == 0
            # Every division row has a non-null SPCD
            assert tbl["SPCD"].null_count() == 0

    def test_division_230_has_georgia_species(self):
        """DIVISION 230 (Subtropical, covering Georgia) has species-level rows
        in the S8a (total_agb) table, including loblolly pine (SPCD=131)."""
        bundle = get_vectorized_lookup_tables()
        div_230 = bundle.total_agb_div.filter(pl.col("DIVISION") == "230")
        spcds = set(div_230["SPCD"].to_list())
        # Loblolly pine (131), slash pine (111), and longleaf pine (121) are
        # the dominant southern pines in Georgia — if the Phase 1.5 fix is
        # to land, these need a DIVISION=230 row.
        assert 131 in spcds or 121 in spcds or 111 in spcds, (
            f"no southern pines in DIVISION 230 S8a rows: {sorted(spcds)}"
        )


class TestBuildDivisionLookup:
    """Vectorized path helper: DIVISION-keyed row filter for *_spcd tables."""

    def test_selected_columns(self):
        """Returns a (SPCD, DIVISION, model, a, b, b1, c) lookup."""
        coefs = load_nsvb_coefficients()
        result = build_division_lookup(coefs.volib_spcd)
        assert result.columns == ["SPCD", "DIVISION", "model", "a", "b", "b1", "c"]

    def test_only_division_rows(self):
        """Rows where DIVISION is null are filtered out."""
        coefs = load_nsvb_coefficients()
        result = build_division_lookup(coefs.volib_spcd)
        assert result["DIVISION"].null_count() == 0
        # Volib_spcd has 256 DIVISION rows in the raw table; the defensive
        # Model 2/4 null-b1 filter may drop a few, but we should still have
        # most of them.
        assert result.height > 200

    def test_stdorgcd_null_only(self):
        """Phase 1.5 skips Level 1 (STDORGCD-specific rows)."""
        coefs = load_nsvb_coefficients()
        raw_div = coefs.volib_spcd.filter(pl.col("DIVISION").is_not_null())
        lookup_div = build_division_lookup(coefs.volib_spcd)
        # lookup should have <= raw (Level 1 rows dropped by STDORGCD.is_null())
        assert lookup_div.height <= raw_div.height

    def test_composite_key_uniqueness(self):
        """Each (SPCD, DIVISION) appears at most once after filtering."""
        coefs = load_nsvb_coefficients()
        result = build_division_lookup(coefs.volib_spcd)
        pairs = result.select(["SPCD", "DIVISION"])
        assert pairs.n_unique() == pairs.height


class TestEcosubcdToDivision:
    """Bailey ECOSUBCD → DIVISION crosswalk."""

    def test_southeastern_mixed_forest(self):
        """SE Mixed Forest provinces 231, 232 → Division 230 (Subtropical)."""
        assert ecosubcd_to_division("231Ae") == "230"
        assert ecosubcd_to_division("231Aa") == "230"
        assert ecosubcd_to_division("232Bh") == "230"
        assert ecosubcd_to_division("232Ba") == "230"

    def test_mountain_prefix(self):
        """Mountain prefix M is preserved: M231Aa → M230."""
        assert ecosubcd_to_division("M231Aa") == "M230"
        assert ecosubcd_to_division("M261Ea") == "M260"
        assert ecosubcd_to_division("M220Ad") == "M220"

    def test_non_subtropical_divisions(self):
        """Sanity-check other domains."""
        assert ecosubcd_to_division("211Aa") == "210"  # Warm Continental
        assert ecosubcd_to_division("221Aa") == "220"  # Hot Continental
        assert ecosubcd_to_division("242Bb") == "240"  # Marine

    def test_null_and_empty_inputs(self):
        """Null, empty, or whitespace input → None."""
        assert ecosubcd_to_division(None) is None
        assert ecosubcd_to_division("") is None
        assert ecosubcd_to_division("   ") is None

    def test_malformed_input(self):
        """Non-digit or too-short prefix → None."""
        assert ecosubcd_to_division("XYZ") is None
        assert ecosubcd_to_division("M") is None
        assert ecosubcd_to_division("12") is None
        assert ecosubcd_to_division("A23") is None

    def test_case_insensitive(self):
        """Lowercase input is normalized to uppercase (M prefix preserved)."""
        assert ecosubcd_to_division("m231aa") == "M230"
        assert ecosubcd_to_division("231ae") == "230"

    def test_whitespace_stripped(self):
        """Leading/trailing whitespace is stripped."""
        assert ecosubcd_to_division("  231Ae  ") == "230"
