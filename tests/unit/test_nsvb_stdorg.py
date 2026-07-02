"""Stand-origin (STDORGCD) coefficient tiers and Model 3 — issues #123/#124.

Slash pine (SPCD 111) and loblolly pine (SPCD 131) are the only species
GTR-WO-104 fits separately for planted (STDORGCD=1) vs natural (0) stands
(p. 8). In the vendored CSVs they carry *no* STDORGCD-null row, so before the
fix they were dropped from the species-level and DIVISION lookups and
collapsed to the generic Jenkins fallback. These tests lock:

- the new Level 1 / Level 1b lookup builders isolate exactly {111, 131};
- the vectorized pipeline, given STDORGCD, resolves 111/131 to their
  species/stand-origin coefficients (matching the scalar oracle) instead of
  Jenkins — and produces a *different* answer than the STDORGCD-less path;
- Model 3 (planted slash total AGB) evaluates to a finite, non-null biomass;
- the coverage/non-null guards (#124) fire on unimplemented models and
  produced nulls.
"""

from __future__ import annotations

import dataclasses
import math
from types import SimpleNamespace

import polars as pl
import pytest

from pyfia.carbon._estimator_base import (
    _IMPLEMENTED_MODELS,
    CarbonEstimatorBase,
    _unimplemented_lookup_models,
)
from pyfia.carbon.nsvb.coefficients import (
    build_division_stdorg_lookup,
    build_species_level_lookup,
    build_stdorg_lookup,
    get_vectorized_lookup_tables,
    load_nsvb_coefficients,
    lookup_coefficients,
)
from pyfia.carbon.nsvb.equations import (
    Coefficients,
    compute_nsvb_biomass,
    predict_tree_biomass,
)

_STDORG_SPECIES = {111, 131}


# ---------------------------------------------------------------------------
# Lookup builders (Level 1 / Level 1b)
# ---------------------------------------------------------------------------


class TestStandOriginLookups:
    def test_stdorg_lookup_isolates_slash_and_loblolly(self):
        """(SPCD, STDORGCD) DIVISION-null rows exist only for 111 and 131."""
        coefs = load_nsvb_coefficients()
        for tbl in (
            coefs.volib_spcd,
            coefs.total_biomass_spcd,
        ):
            org = build_stdorg_lookup(tbl)
            assert set(org["SPCD"].to_list()) <= _STDORG_SPECIES
            assert org["STDORGCD"].null_count() == 0
            assert (
                org["DIVISION"].is_null().all() if "DIVISION" in org.columns else True
            )

    def test_division_stdorg_lookup_isolates_slash_and_loblolly(self):
        """(SPCD, DIVISION, STDORGCD) rows exist only for 111 and 131."""
        coefs = load_nsvb_coefficients()
        divorg = build_division_stdorg_lookup(coefs.total_biomass_spcd)
        assert set(divorg["SPCD"].to_list()) <= _STDORG_SPECIES
        assert divorg["STDORGCD"].null_count() == 0
        assert divorg["DIVISION"].null_count() == 0

    def test_species_level_excludes_stand_origin_species(self):
        """111/131 have no STDORGCD-null row, so the species-level (Level 3)
        lookup must NOT contain them — this is the #123 root cause."""
        coefs = load_nsvb_coefficients()
        spcd = build_species_level_lookup(coefs.total_biomass_spcd)
        present = set(spcd["SPCD"].to_list())
        assert 111 not in present
        assert 131 not in present

    def test_planted_slash_total_agb_is_model_3(self):
        """Planted slash pine (111, STDORGCD=1) total AGB dispatches to Model 3
        with a1/c1 populated — the case that couples #123 to #124."""
        coefs = load_nsvb_coefficients()
        org = build_stdorg_lookup(coefs.total_biomass_spcd)
        row = org.filter(
            (pl.col("SPCD") == 111) & (pl.col("STDORGCD") == 1)
        ).to_dicts()[0]
        assert row["model"] == 3
        assert row["a1"] is not None
        assert row["c1"] is not None


# ---------------------------------------------------------------------------
# Vectorized tier resolution vs scalar oracle
# ---------------------------------------------------------------------------


def _scalar_bundle(
    spcd: int, jenkins: int, division: str, stdorgcd: int
) -> Coefficients:
    """Scalar oracle bundle walking the CSV lookup WITH division + stand origin."""
    t = load_nsvb_coefficients()
    pairs = [
        (t.volib_spcd, t.volib_jenkins),
        (t.volbk_spcd, t.volbk_jenkins),
        (t.bark_biomass_spcd, t.bark_biomass_jenkins),
        (t.branch_biomass_spcd, t.branch_biomass_jenkins),
        (t.total_biomass_spcd, t.total_biomass_jenkins),
    ]
    resolved = [
        lookup_coefficients(
            s,
            j,
            spcd=spcd,
            jenkins_spgrpcd=jenkins,
            division=division,
            stdorgcd=stdorgcd,
        )
        for s, j in pairs
    ]
    return Coefficients(*resolved)


def _one_tree_vectorized(spcd, jenkins, wdsg, dia, ht, division, stdorgcd) -> dict:
    frame = pl.LazyFrame(
        {
            "SPCD": [spcd],
            "DIA": [dia],
            "HT": [ht],
            "CULL": [0.0],
            "WDSG": [wdsg],
            "JENKINS_SPGRPCD": [jenkins],
            "DIVISION": [division],
            "STDORGCD": [stdorgcd],
        },
        schema={
            "SPCD": pl.Int64,
            "DIA": pl.Float64,
            "HT": pl.Float64,
            "CULL": pl.Float64,
            "WDSG": pl.Float64,
            "JENKINS_SPGRPCD": pl.Int64,
            "DIVISION": pl.Utf8,
            "STDORGCD": pl.Int64,
        },
    )
    return compute_nsvb_biomass(frame).collect().to_dicts()[0]


@pytest.mark.parametrize(
    "spcd,jenkins,wdsg,stdorgcd",
    [
        (131, 4, 0.47, 0),  # natural loblolly — Model 2 (all components)
        (131, 4, 0.47, 1),  # planted loblolly — Model 2
        (111, 4, 0.54, 0),  # natural slash — Model 2
        (111, 4, 0.54, 1),  # planted slash — total AGB is Model 3
    ],
)
def test_vectorized_matches_scalar_for_stand_origin(spcd, jenkins, wdsg, stdorgcd):
    """With STDORGCD threaded, the vectorized pipeline resolves 111/131 to
    their stand-origin coefficients and matches the scalar oracle exactly."""
    dia, ht, division = 10.0, 60.0, "230"
    vec = _one_tree_vectorized(spcd, jenkins, wdsg, dia, ht, division, stdorgcd)
    scalar = predict_tree_biomass(
        spcd=spcd,
        dia=dia,
        ht=ht,
        coefficients=_scalar_bundle(spcd, jenkins, division, stdorgcd),
        wdsg=wdsg,
        hw_sw="softwood",
        cull=0.0,
    )
    assert vec["agb"] is not None
    assert math.isfinite(vec["agb"])
    assert vec["agb"] == pytest.approx(scalar.agb, rel=1e-9)


def test_stand_origin_changes_the_answer_vs_jenkins_fallback():
    """Threading STDORGCD moves 131 off the Jenkins fallback: the AGB with the
    STDORGCD column present differs from the AGB without it (issue #123)."""
    dia, ht, wdsg, jenkins = 10.0, 60.0, 0.47, 4
    with_org = _one_tree_vectorized(131, jenkins, wdsg, dia, ht, "230", 1)["agb"]

    # No STDORGCD / DIVISION column → degrades to species-level + Jenkins. For
    # 131 the species-level tier is empty, so this is the pre-fix Jenkins path.
    frame = pl.LazyFrame(
        {
            "SPCD": [131],
            "DIA": [dia],
            "HT": [ht],
            "CULL": [0.0],
            "WDSG": [wdsg],
            "JENKINS_SPGRPCD": [jenkins],
        },
        schema={
            "SPCD": pl.Int64,
            "DIA": pl.Float64,
            "HT": pl.Float64,
            "CULL": pl.Float64,
            "WDSG": pl.Float64,
            "JENKINS_SPGRPCD": pl.Int64,
        },
    )
    jenkins_agb = compute_nsvb_biomass(frame).collect().to_dicts()[0]["agb"]
    assert with_org is not None and jenkins_agb is not None
    assert abs(with_org / jenkins_agb - 1.0) > 0.01  # materially different


def test_planted_slash_model3_is_nonnull_end_to_end():
    """The planted-slash Model-3 total-AGB path yields a finite, positive AGB
    (before the fix this was a silent null → dropped from the sum, #124)."""
    out = _one_tree_vectorized(111, 4, 0.54, 12.0, 55.0, "230", 1)
    assert out["agb"] is not None
    assert out["agb"] > 0
    assert math.isfinite(out["agb"])


# ---------------------------------------------------------------------------
# Guards (#124)
# ---------------------------------------------------------------------------


class TestNsvbGuards:
    def test_real_lookup_has_no_unimplemented_models(self):
        lookup = get_vectorized_lookup_tables()
        assert _unimplemented_lookup_models(lookup) == []
        # sanity: all component models are within the implemented set
        assert 6 not in _IMPLEMENTED_MODELS

    def test_injected_model_6_is_detected(self):
        lookup = get_vectorized_lookup_tables()
        bad = lookup.total_agb_spcd.with_columns(
            pl.when(pl.arange(0, pl.len()) == 0)
            .then(6)
            .otherwise(pl.col("model"))
            .alias("model")
        )
        tampered = dataclasses.replace(lookup, total_agb_spcd=bad)
        assert _unimplemented_lookup_models(tampered) == [6]

    def test_assert_biomass_nonnull_raises_on_null(self):
        stub = SimpleNamespace(_estimator_label="live_tree")
        frame = pl.LazyFrame(
            {"SPCD": [131, 999], "_CARBON_AG_LB": [12.3, None]},
            schema={"SPCD": pl.Int64, "_CARBON_AG_LB": pl.Float64},
        )
        with pytest.raises(ValueError, match="null _CARBON_AG_LB.*999"):
            CarbonEstimatorBase._assert_biomass_nonnull(stub, frame)

    def test_assert_biomass_nonnull_passes_when_clean(self):
        stub = SimpleNamespace(_estimator_label="live_tree")
        frame = pl.LazyFrame(
            {"SPCD": [131, 111], "_CARBON_AG_LB": [12.3, 4.5]},
            schema={"SPCD": pl.Int64, "_CARBON_AG_LB": pl.Float64},
        )
        CarbonEstimatorBase._assert_biomass_nonnull(stub, frame)  # no raise
