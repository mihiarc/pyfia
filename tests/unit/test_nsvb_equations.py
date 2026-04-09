"""
Unit tests for the NSVB equation library.

These tests verify the pure-math equation forms (Models 1, 2, 4, 5) and the
``predict_tree_biomass`` orchestrator against the worked numerical examples in
the GTR-WO-104 source PDF (transcribed in
``references_md/tier1_fcaf/gtr_wo104_westfall2023.md`` lines 394-960).

Two worked examples are reproduced:

1. **Douglas-fir** (SPCD=202, D=20", H=110', no cull, DIVISION=240) — exercises
   Model 2 (stem wood vol, k=9 softwood) and Model 1 (everything else).
   Expected total AGB: 3154.5539926725 lb. Carbon: 1626.4748946459 lb.

2. **Red maple** (SPCD=316, D=11.1", H=38', CULL=3, DIVISION=M210→species-level
   fallback) — exercises Model 1 (volib), Model 2 (volbk, k=11 hardwood),
   Model 4 (total AGB with exp(-b1*D) factor), and the cull-reduction path.
   Expected total AGB after cull-adjusted harmonization: 528.135964525863 lb.

Coefficients are hand-coded from the worked-example prose to high precision
(12+ significant figures). The CSV-loaded coefficients in
``src/pyfia/carbon/nsvb/data/`` are stored at ~9 significant figures and would
introduce rounding error at the 7th-8th decimal — the regression tests need
the prose precision to verify the equation math itself, separate from CSV
fidelity.
"""

from __future__ import annotations

import math

import pytest

from pyfia.carbon.nsvb.coefficients import (
    load_nsvb_coefficients,
    lookup_coefficients,
)
from pyfia.carbon.nsvb.equations import (
    Coefficients,
    _model_k,
    harmonize_components,
    model_1,
    model_2,
    model_4,
    model_5_jenkins,
    predict_tree_biomass,
)

# ---------------------------------------------------------------------------
# Hand-coded high-precision coefficients from the WO-104 worked examples
# ---------------------------------------------------------------------------


# Douglas-fir SPCD=202, DIVISION=240, no cull (worked example lines 394-680)
DOUGFIR_INPUTS = {
    "spcd": 202,
    "dia": 20.0,
    "ht": 110.0,
    "wdsg": 0.45,
    "hw_sw": "softwood",
    "cull": 0.0,
}

DOUGFIR_COEFS = Coefficients(
    volib={
        "model": 2,
        "a": 0.001929099661,
        "a1": 0.0,
        "b": 2.162413104203,
        "b1": 1.690400253097,
        "c": 0.985444005253,
        "c1": 0.0,
        "source": "test",
    },
    volbk={
        "model": 1,
        "a": 0.000031886237,
        "a1": 0.0,
        "b": 1.21260513951,
        "b1": 0.0,
        "c": 1.978577263767,
        "c1": 0.0,
        "source": "test",
    },
    bark_bio={
        "model": 1,
        "a": 0.009106538193,
        "a1": 0.0,
        "b": 1.437894424586,
        "b1": 0.0,
        "c": 1.336514272981,
        "c1": 0.0,
        "source": "test",
    },
    branch_bio={
        "model": 1,
        "a": 9.521330809106,
        "a1": 0.0,
        "b": 1.762316117442,
        "b1": 0.0,
        "c": -0.40574259177,
        "c1": 0.0,
        "source": "test",
    },
    total_agb={
        "model": 1,
        "a": 0.135206506787,
        "a1": 0.0,
        "b": 1.713527048035,
        "b1": 0.0,
        "c": 1.047613377046,
        "c1": 0.0,
        "source": "test",
    },
)

DOUGFIR_EXPECTED = {
    "v_wood_ib": 88.452275544288,
    "v_bark": 13.191436232306,
    "w_wood_gross": 2483.739897283610,
    "w_bark_gross": 361.782496100100,
    "w_branch_gross": 277.487756904646,
    "agb_predicted": 3154.5539926725,
    "agb_component": 3123.010150288360,
    "wood_harmonized": 2508.826815376370,
    "bark_harmonized": 365.436666110811,
    "branch_harmonized": 280.290511185328,
    "carbon_total": 1626.474894645920,
    "carbon_fraction": 0.515595833333,
}

# Red maple SPCD=316, species-level fallback, CULL=3 (worked example lines 682-960)
REDMAPLE_INPUTS = {
    "spcd": 316,
    "dia": 11.1,
    "ht": 38.0,
    "wdsg": 0.49,
    "hw_sw": "hardwood",
    "cull": 3.0,
}

REDMAPLE_COEFS = Coefficients(
    volib={
        "model": 1,
        "a": 0.001983918881,
        "a1": 0.0,
        "b": 1.810559393287,
        "b1": 0.0,
        "c": 1.129417635145,
        "c1": 0.0,
        "source": "test",
    },
    volbk={
        "model": 2,
        "a": 0.003743084443,
        "a1": 0.0,
        "b": 2.226890355309,
        "b1": 1.685993125661,
        "c": 0.275066356213,
        "c1": 0.0,
        "source": "test",
    },
    bark_bio={
        "model": 1,
        "a": 0.061595466174,
        "a1": 0.0,
        "b": 1.818642599217,
        "b1": 0.0,
        "c": 0.654020672095,
        "c1": 0.0,
        "source": "test",
    },
    branch_bio={
        "model": 1,
        "a": 0.011144618401,
        "a1": 0.0,
        "b": 3.269520661293,
        "b1": 0.0,
        "c": 0.421304343724,
        "c1": 0.0,
        "source": "test",
    },
    total_agb={
        "model": 4,
        "a": 0.31573027567,
        "a1": 0.0,
        "b": 1.853839844372,
        "b1": -0.024745684975,
        "c": 0.740557378679,
        "c1": 0.0,
        "source": "test",
    },
)

REDMAPLE_EXPECTED = {
    "v_wood_ib": 9.427112777611,
    "v_bark": 2.155106401987,
    "w_wood_gross": 288.243400288234,
    "w_wood_red": 284.265641364256,
    "w_bark": 52.945466015848,
    "w_branch": 135.001927997271,
    "agb_predicted": 532.584798820042,
    "agb_component_red": 472.213035377375,
    "agb_reduce": 0.991646711840,
    "agb_predicted_red": 528.135964525863,
    "wood_harmonized": 317.930462388645,
    "bark_harmonized": 59.215656211618,
    "branch_harmonized": 150.989845925600,
}

# Tolerance: 1e-9 relative error matches the worked-example precision
TOL = 1e-9


def _close(actual: float, expected: float, tol: float = TOL) -> bool:
    if expected == 0:
        return abs(actual) < tol
    return abs(actual - expected) / abs(expected) < tol


# ---------------------------------------------------------------------------
# Model 1 — power form
# ---------------------------------------------------------------------------


class TestModel1:
    """Pure power form: ``y = a * D^b * H^c``."""

    def test_douglas_fir_stem_bark_volume(self):
        """S2a Model 1, line 424 of worked example.

        Vtot_bk_Gross = 0.000031886237 * 20^1.21260513951 * 110^1.978577263767
                      = 13.191436232306
        """
        result = model_1(20.0, 110.0, 0.000031886237, 1.21260513951, 1.978577263767)
        assert _close(result, 13.191436232306)

    def test_douglas_fir_stem_bark_biomass(self):
        """S6a Model 1, line 560 of worked example.

        Wtot_bk = 0.009106538193 * 20^1.437894424586 * 110^1.336514272981
                = 361.782496100100
        """
        result = model_1(20.0, 110.0, 0.009106538193, 1.437894424586, 1.336514272981)
        assert _close(result, 361.782496100100)

    def test_douglas_fir_branch_biomass(self):
        """S7a Model 1, line 570 of worked example.

        Wbranch = 9.521330809106 * 20^1.762316117442 * 110^-0.40574259177
                = 277.487756904646

        Note negative exponent on H — branch biomass declines with height
        for trees of fixed diameter (taller trees have less branch mass).
        """
        result = model_1(20.0, 110.0, 9.521330809106, 1.762316117442, -0.40574259177)
        assert _close(result, 277.487756904646)

    def test_douglas_fir_total_agb(self):
        """S8a Model 1, line 578 of worked example.

        AGB_Predicted = 0.135206506787 * 20^1.713527048035 * 110^1.047613377046
                      = 3154.5539926725
        """
        result = model_1(20.0, 110.0, 0.135206506787, 1.713527048035, 1.047613377046)
        assert _close(result, 3154.5539926725)


# ---------------------------------------------------------------------------
# Model 2 — power form with k constant
# ---------------------------------------------------------------------------


class TestModel2:
    """Power form with species-class base constant k.

    Verifies the k=9 (softwood) and k=11 (hardwood) discovery from the
    worked examples.
    """

    def test_softwood_k_constant(self):
        """SPCD < 300 → k = 9.0."""
        assert _model_k(202) == 9.0
        assert _model_k(1) == 9.0
        assert _model_k(299) == 9.0

    def test_hardwood_k_constant(self):
        """SPCD >= 300 → k = 11.0."""
        assert _model_k(316) == 11.0
        assert _model_k(300) == 11.0
        assert _model_k(998) == 11.0

    def test_douglas_fir_stem_wood_volume(self):
        """S1a Model 2, line 418 of worked example. SPCD=202 (softwood, k=9).

        Vtot_ib_Gross = 0.001929099661 * 9^(2.162413104203 - 1.690400253097)
                        * 20^1.690400253097 * 110^0.985444005253
                      = 88.452275544288
        """
        result = model_2(
            d=20.0,
            h=110.0,
            a=0.001929099661,
            b=2.162413104203,
            b1=1.690400253097,
            c=0.985444005253,
            k=9.0,
        )
        assert _close(result, 88.452275544288)

    def test_red_maple_stem_bark_volume(self):
        """S2a Model 2, line 698 of worked example. SPCD=316 (hardwood, k=11).

        This is the test that pinned down k=11 for hardwoods. With k=9 the
        result would be 1.94, not 2.16.
        """
        result = model_2(
            d=11.1,
            h=38.0,
            a=0.003743084443,
            b=2.226890355309,
            b1=1.685993125661,
            c=0.275066356213,
            k=11.0,
        )
        assert _close(result, 2.155106401987)


# ---------------------------------------------------------------------------
# Model 4 — exp-modulated power form
# ---------------------------------------------------------------------------


class TestModel4:
    """Form: ``y = a * D^b * H^c * exp(-b1 * D)``.

    Discovered from the red maple S8a worked example (line 860). The
    sidecar's documentation of Model 4 was incomplete.
    """

    def test_red_maple_total_agb(self):
        """S8a Model 4, line 862 of worked example.

        AGB_Predicted = 0.31573027567 * 11.1^1.853839844372 * 38^0.740557378679
                        * exp(-(-0.024745684975 * 11.1))
                      = 532.584798820042

        The negative b1 means the exp factor is > 1.
        """
        result = model_4(
            d=11.1,
            h=38.0,
            a=0.31573027567,
            b=1.853839844372,
            b1=-0.024745684975,
            c=0.740557378679,
        )
        assert _close(result, 532.584798820042)

    def test_model_4_reduces_to_model_1_when_b1_zero(self):
        """When b1=0, exp(0)=1, so Model 4 should equal Model 1."""
        m1 = model_1(15.0, 80.0, 0.1, 1.5, 1.2)
        m4 = model_4(15.0, 80.0, 0.1, 1.5, 0.0, 1.2)
        assert _close(m4, m1)


# ---------------------------------------------------------------------------
# Model 5 — Jenkins fallback
# ---------------------------------------------------------------------------


class TestModel5Jenkins:
    """Form: ``y = a * D^b * H^c * WDSG``."""

    def test_jenkins_form_multiplies_by_wdsg(self):
        """The only difference from Model 1 is the WDSG multiplication."""
        m1 = model_1(15.0, 80.0, 0.1, 1.5, 1.2)
        m5 = model_5_jenkins(15.0, 80.0, 0.1, 1.5, 1.2, wdsg=0.55)
        assert _close(m5, m1 * 0.55)


# ---------------------------------------------------------------------------
# Harmonization
# ---------------------------------------------------------------------------


class TestHarmonization:
    """Component proportional redistribution to sum exactly to predicted AGB."""

    def test_douglas_fir_harmonization(self):
        """No-cull harmonization, line 602-612 of worked example.

        AGB_predicted = 3154.5539926725
        Components (gross, no cull): 2483.739897283610, 361.782496100100, 277.487756904646
        Component sum:               3123.010150288360
        Expected harmonized: 2508.826815376370, 365.436666110811, 280.290511185328
        """
        wood_h, bark_h, branch_h = harmonize_components(
            agb_predicted=3154.5539926725,
            w_wood=2483.739897283610,
            w_bark=361.782496100100,
            w_branch=277.487756904646,
        )
        assert _close(wood_h, 2508.826815376370)
        assert _close(bark_h, 365.436666110811)
        assert _close(branch_h, 280.290511185328)

    def test_harmonization_invariant(self):
        """wood_h + bark_h + branch_h must equal agb_predicted exactly."""
        wood_h, bark_h, branch_h = harmonize_components(
            agb_predicted=1000.0,
            w_wood=400.0,
            w_bark=200.0,
            w_branch=300.0,
        )
        assert math.isclose(wood_h + bark_h + branch_h, 1000.0, rel_tol=1e-15)

    def test_harmonization_handles_zero_components(self):
        """Degenerate case: all components zero. Should return predicted in wood slot."""
        wood_h, bark_h, branch_h = harmonize_components(
            agb_predicted=500.0,
            w_wood=0.0,
            w_bark=0.0,
            w_branch=0.0,
        )
        assert wood_h == 500.0
        assert bark_h == 0.0
        assert branch_h == 0.0

    def test_harmonization_preserves_relative_ratios(self):
        """If components are 4:2:3 in raw form, harmonized should also be 4:2:3."""
        wood_h, bark_h, branch_h = harmonize_components(
            agb_predicted=900.0,
            w_wood=400.0,
            w_bark=200.0,
            w_branch=300.0,
        )
        assert math.isclose(wood_h / bark_h, 400.0 / 200.0, rel_tol=1e-12)
        assert math.isclose(bark_h / branch_h, 200.0 / 300.0, rel_tol=1e-12)


# ---------------------------------------------------------------------------
# Full pipeline orchestrator
# ---------------------------------------------------------------------------


class TestPredictTreeBiomass:
    """End-to-end NSVB pipeline regression vs the worked examples."""

    def test_douglas_fir_full_pipeline(self):
        """No-cull case (CULL=0). AGBReduce=1.0, simple harmonization."""
        result = predict_tree_biomass(
            coefficients=DOUGFIR_COEFS,
            **DOUGFIR_INPUTS,
        )
        assert _close(result.v_wood_ib, DOUGFIR_EXPECTED["v_wood_ib"])
        assert _close(result.v_bark, DOUGFIR_EXPECTED["v_bark"])
        assert _close(result.w_wood, DOUGFIR_EXPECTED["wood_harmonized"])
        assert _close(result.w_bark, DOUGFIR_EXPECTED["bark_harmonized"])
        assert _close(result.w_branch, DOUGFIR_EXPECTED["branch_harmonized"])
        assert _close(result.agb, DOUGFIR_EXPECTED["agb_predicted"])

    def test_red_maple_full_pipeline_with_cull(self):
        """With-cull case (CULL=3). AGBReduce<1, cull-adjusted harmonization."""
        result = predict_tree_biomass(
            coefficients=REDMAPLE_COEFS,
            **REDMAPLE_INPUTS,
        )
        assert _close(result.v_wood_ib, REDMAPLE_EXPECTED["v_wood_ib"])
        assert _close(result.v_bark, REDMAPLE_EXPECTED["v_bark"])
        assert _close(result.w_wood, REDMAPLE_EXPECTED["wood_harmonized"])
        assert _close(result.w_bark, REDMAPLE_EXPECTED["bark_harmonized"])
        assert _close(result.w_branch, REDMAPLE_EXPECTED["branch_harmonized"])
        assert _close(result.agb, REDMAPLE_EXPECTED["agb_predicted_red"])

    def test_pipeline_harmonization_invariant(self):
        """Component sum equals total AGB to floating-point precision."""
        result = predict_tree_biomass(
            coefficients=DOUGFIR_COEFS,
            **DOUGFIR_INPUTS,
        )
        assert math.isclose(
            result.w_wood + result.w_bark + result.w_branch,
            result.agb,
            rel_tol=1e-12,
        )

    def test_pipeline_carbon_conversion(self):
        """Multiplying AGB by the species carbon fraction reproduces the
        worked-example carbon value (line 680)."""
        result = predict_tree_biomass(
            coefficients=DOUGFIR_COEFS,
            **DOUGFIR_INPUTS,
        )
        carbon = result.agb * DOUGFIR_EXPECTED["carbon_fraction"]
        assert _close(carbon, DOUGFIR_EXPECTED["carbon_total"])

    def test_unsupported_model_raises(self):
        """Model 3 and Model 6 are not implemented in Phase 1."""
        bad_coefs = Coefficients(
            volib={
                "model": 3,  # Model 3 not implemented
                "a": 0.001,
                "a1": 0.0,
                "b": 1.5,
                "b1": 0.0,
                "c": 1.0,
                "c1": 0.0,
                "source": "test",
            },
            volbk=DOUGFIR_COEFS.volbk,
            bark_bio=DOUGFIR_COEFS.bark_bio,
            branch_bio=DOUGFIR_COEFS.branch_bio,
            total_agb=DOUGFIR_COEFS.total_agb,
        )
        with pytest.raises(ValueError, match="model 3 not supported"):
            predict_tree_biomass(
                coefficients=bad_coefs,
                **DOUGFIR_INPUTS,
            )

    def test_small_tree_uses_same_pipeline(self):
        """Per WO-104 line 1624, saplings (DBH < 5) use the same pipeline.

        We don't have a worked example to compare against, but we can verify
        that the pipeline runs without raising and returns a positive AGB
        for a 2.5" sapling.
        """
        result = predict_tree_biomass(
            spcd=202,
            dia=2.5,  # sapling, well below 5"
            ht=20.0,
            coefficients=DOUGFIR_COEFS,
            wdsg=0.45,
            hw_sw="softwood",
            cull=0.0,
        )
        assert result.agb > 0
        # Sapling AGB should be much smaller than the 20" mature tree AGB
        assert result.agb < 100  # 20" tree was 3154 lb; 2.5" sapling should be << 100


# ---------------------------------------------------------------------------
# CSV → pipeline regression sentinels (the production data path)
# ---------------------------------------------------------------------------


def _bundle_via_csv(spcd: int, jenkins_spgrpcd: int) -> Coefficients:
    """Build a ``Coefficients`` bundle by walking the real CSV lookup path.

    This is exactly what PR 2's ``LiveTreeEstimator`` will reach for at
    runtime (modulo the planned vectorization). Reusing it here ensures the
    regression sentinels exercise the same code path the production estimator
    will hit, not a hand-coded shortcut.
    """
    tables = load_nsvb_coefficients()
    return Coefficients(
        volib=lookup_coefficients(
            tables.volib_spcd,
            tables.volib_jenkins,
            spcd=spcd,
            jenkins_spgrpcd=jenkins_spgrpcd,
        ),
        volbk=lookup_coefficients(
            tables.volbk_spcd,
            tables.volbk_jenkins,
            spcd=spcd,
            jenkins_spgrpcd=jenkins_spgrpcd,
        ),
        bark_bio=lookup_coefficients(
            tables.bark_biomass_spcd,
            tables.bark_biomass_jenkins,
            spcd=spcd,
            jenkins_spgrpcd=jenkins_spgrpcd,
        ),
        branch_bio=lookup_coefficients(
            tables.branch_biomass_spcd,
            tables.branch_biomass_jenkins,
            spcd=spcd,
            jenkins_spgrpcd=jenkins_spgrpcd,
        ),
        total_agb=lookup_coefficients(
            tables.total_biomass_spcd,
            tables.total_biomass_jenkins,
            spcd=spcd,
            jenkins_spgrpcd=jenkins_spgrpcd,
        ),
    )


class TestPipelineViaCSV:
    """End-to-end NSVB pipeline regression vs the worked examples using the
    actual CSV-loaded coefficients (the production data path).

    Unlike :class:`TestPredictTreeBiomass` above — which uses hand-coded
    high-precision coefficients to verify the equation form — these tests
    walk the full ``lookup_coefficients`` → ``predict_tree_biomass`` path so a
    CSV corruption, schema drift, or species-level row recalibration will
    fail the suite.

    **Tolerance: 0.5%.** Currently measured agreement is:

    - Douglas-fir (SPCD=202): ~0.09% — Phase 1 uses the species-level
      (DIVISION=null) row, while the worked example uses DIVISION=240. The
      species-level row is well-calibrated such that the harmonized AGB
      lands within 0.1% of the DIVISION=240 result.
    - Red maple (SPCD=316): ~0.00% — the worked example itself uses the
      species-level fallback, so the CSV path matches exactly modulo the
      ~9-significant-figure storage precision in the CSV.

    The 0.5% tolerance leaves ~5x headroom over the Douglas-fir species-level
    fallback gap. PR 2 (which adds the ``ECOSUBCD → DIVISION`` mapping if
    PR 3's validation gate flags it) can tighten this if appropriate.

    See ``pyfia/carbon/__init__.py`` "Items deferred from PR 1 review" for
    why the species-level fallback is used in Phase 1.
    """

    def test_douglas_fir_pipeline_via_csv(self):
        """Worked example expected AGB: 3154.55 lb (DIVISION=240).

        With Phase 1's species-level fallback the CSV pipeline lands at
        ~3151.78 lb — within 0.1% of the worked example. Asserting at 0.5%
        tolerance gives headroom for re-vendoring rounding noise while still
        catching any catastrophic CSV corruption or species-level row drift.

        Also asserts every component of the bundle was resolved at the
        ``"spcd"`` precedence level (not Jenkins fallback) — catches
        mis-routing through Level 4 if the SPCD-keyed tables get truncated.
        """
        bundle = _bundle_via_csv(spcd=202, jenkins_spgrpcd=10)
        # Every component must hit the species-level row, not the Jenkins fallback
        assert bundle.volib["source"] == "spcd"
        assert bundle.volbk["source"] == "spcd"
        assert bundle.bark_bio["source"] == "spcd"
        assert bundle.branch_bio["source"] == "spcd"
        assert bundle.total_agb["source"] == "spcd"

        result = predict_tree_biomass(coefficients=bundle, **DOUGFIR_INPUTS)
        expected = DOUGFIR_EXPECTED["agb_predicted"]
        rel_err = abs(result.agb - expected) / expected
        assert rel_err < 0.005, (
            f"CSV pipeline AGB={result.agb:.4f} differs from worked-example "
            f"{expected:.4f} by {rel_err * 100:.3f}% (>0.5% tolerance)"
        )

    def test_red_maple_pipeline_via_csv(self):
        """Worked example expected AGB (cull-reduced): 528.14 lb.

        The red maple worked example uses the species-level fallback for
        both volib (Model 1) and total_agb (Model 4), so this test exercises
        the cull-reduction path AND the Model 4 dispatch via the real CSV
        lookup — not just the equation form.
        """
        bundle = _bundle_via_csv(spcd=316, jenkins_spgrpcd=8)
        assert bundle.volib["source"] == "spcd"
        assert bundle.volbk["source"] == "spcd"
        assert bundle.bark_bio["source"] == "spcd"
        assert bundle.branch_bio["source"] == "spcd"
        assert bundle.total_agb["source"] == "spcd"
        # Critical regression: total_agb for SPCD=316 must dispatch to Model 4
        assert bundle.total_agb["model"] == 4

        result = predict_tree_biomass(coefficients=bundle, **REDMAPLE_INPUTS)
        expected = REDMAPLE_EXPECTED["agb_predicted_red"]
        rel_err = abs(result.agb - expected) / expected
        assert rel_err < 0.005, (
            f"CSV pipeline AGB={result.agb:.4f} differs from worked-example "
            f"{expected:.4f} by {rel_err * 100:.3f}% (>0.5% tolerance)"
        )
