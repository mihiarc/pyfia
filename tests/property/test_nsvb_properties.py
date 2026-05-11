"""
Property-based tests for the NSVB equation library.

Uses Hypothesis to generate random valid inputs (DBH, height, coefficient
shapes, etc.) and verifies invariants that must hold for all valid inputs:

1. **Harmonization additivity**: ``wood_h + bark_h + branch_h == agb_predicted``
   to floating-point precision, for any positive component values.
2. **Carbon fraction bounds**: every value returned by
   ``get_carbon_fraction_live`` is in [0.30, 0.65] (the actual S10a range).
3. **Model 1 monotonicity in D**: for fixed H, ``model_1`` is monotonically
   increasing in D when b > 0 (which it always is in the NSVB tables).
4. **Model 1 monotonicity in H**: for fixed D, ``model_1`` is monotonically
   increasing in H when c > 0 (the common case).
5. **Model 4 reduces to Model 1 when b1=0**: an algebraic invariant that
   pins the exp factor to 1 in the degenerate case.
6. **Pipeline produces positive AGB for any valid tree**: smoke test that
   ``predict_tree_biomass`` doesn't return negative or NaN values for the
   Douglas-fir coefficient bundle across the full DBH range [1, 60].
"""

from __future__ import annotations

import math

from hypothesis import assume, given, settings
from hypothesis import strategies as st

from pyfia.carbon.nsvb.carbon_fractions import (
    get_carbon_fraction_live,
    load_carbon_fractions_live,
)
from pyfia.carbon.nsvb.equations import (
    Coefficients,
    harmonize_components,
    model_1,
    model_4,
    predict_tree_biomass,
)

# Reuse the Douglas-fir coefficients from the equations test as a stable
# coefficient bundle for pipeline property tests.
_DOUGFIR_COEFS = Coefficients(
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


# ---------------------------------------------------------------------------
# Harmonization invariant
# ---------------------------------------------------------------------------


@given(
    agb=st.floats(min_value=0.1, max_value=1e6, allow_nan=False, allow_infinity=False),
    w_wood=st.floats(
        min_value=0.01, max_value=1e6, allow_nan=False, allow_infinity=False
    ),
    w_bark=st.floats(
        min_value=0.01, max_value=1e6, allow_nan=False, allow_infinity=False
    ),
    w_branch=st.floats(
        min_value=0.01, max_value=1e6, allow_nan=False, allow_infinity=False
    ),
)
@settings(max_examples=200)
def test_harmonization_additivity(agb, w_wood, w_bark, w_branch):
    """Harmonized components must sum exactly to the predicted AGB.

    This is the load-bearing invariant for any downstream code that needs
    additive decomposition (e.g., merchantable subdivisions in Phase 2+).
    """
    wood_h, bark_h, branch_h = harmonize_components(agb, w_wood, w_bark, w_branch)
    total = wood_h + bark_h + branch_h
    assert math.isclose(total, agb, rel_tol=1e-12)


@given(
    agb=st.floats(min_value=1.0, max_value=1e5),
    w_wood=st.floats(min_value=1.0, max_value=1e5),
    w_bark=st.floats(min_value=1.0, max_value=1e5),
    w_branch=st.floats(min_value=1.0, max_value=1e5),
)
@settings(max_examples=200)
def test_harmonization_preserves_relative_ratios(agb, w_wood, w_bark, w_branch):
    """The harmonized components preserve the relative ratios of the inputs.

    If raw wood:bark = 4:2, harmonized wood:bark must also be 4:2.
    """
    wood_h, bark_h, _ = harmonize_components(agb, w_wood, w_bark, w_branch)
    raw_ratio = w_wood / w_bark
    harmonized_ratio = wood_h / bark_h
    assert math.isclose(raw_ratio, harmonized_ratio, rel_tol=1e-12)


# ---------------------------------------------------------------------------
# Carbon fraction bounds
# ---------------------------------------------------------------------------


def test_all_carbon_fractions_in_decimal_range():
    """Every value in S10a must be in the decimal range, not percent.

    This is a smoke test of the percent→decimal normalization. We use a
    direct iteration rather than hypothesis because we want to check
    every species, not a random sample.
    """
    table = load_carbon_fractions_live()
    for spcd, frac in table.items():
        assert 0.30 <= frac <= 0.65, f"SPCD={spcd} carbon fraction {frac} out of range"


@given(spcd=st.sampled_from(sorted(load_carbon_fractions_live().keys())))
@settings(max_examples=200)
def test_get_carbon_fraction_live_returns_known_value(spcd):
    """For any SPCD known to S10a, the lookup returns the table value
    (not the fallback)."""
    table = load_carbon_fractions_live()
    result = get_carbon_fraction_live(spcd)
    assert result == table[spcd]


# ---------------------------------------------------------------------------
# Model 1 monotonicity
# ---------------------------------------------------------------------------


@given(
    d_lo=st.floats(min_value=1.0, max_value=30.0),
    d_hi=st.floats(min_value=1.0, max_value=30.0),
    h=st.floats(min_value=10.0, max_value=200.0),
    a=st.floats(min_value=1e-6, max_value=10.0),
    b=st.floats(min_value=0.5, max_value=3.0),
    c=st.floats(min_value=0.5, max_value=2.0),
)
@settings(max_examples=200)
def test_model_1_monotonic_in_d(d_lo, d_hi, h, a, b, c):
    """For fixed H and positive coefficients, Model 1 is increasing in D.

    Models with b > 0 (the universal case in the NSVB tables) must satisfy
    ``y(D_lo, H) < y(D_hi, H)`` whenever ``D_lo < D_hi``.

    Note on the 1e-6 gap: the strict-inequality assertion holds mathematically
    but fails at floating-point ULP-level differences (e.g., d_lo=1.0 and
    d_hi=1.0000000000000002 map to indistinguishable f64 outputs). We only
    test the property when the two diameters differ by at least 1e-6 inches,
    which is well below any meaningful tree-measurement precision.
    """
    assume(abs(d_hi - d_lo) > 1e-6)
    lo, hi = sorted([d_lo, d_hi])
    y_lo = model_1(lo, h, a, b, c)
    y_hi = model_1(hi, h, a, b, c)
    assert y_lo < y_hi


@given(
    d=st.floats(min_value=1.0, max_value=30.0),
    h_lo=st.floats(min_value=10.0, max_value=200.0),
    h_hi=st.floats(min_value=10.0, max_value=200.0),
    a=st.floats(min_value=1e-6, max_value=10.0),
    b=st.floats(min_value=0.5, max_value=3.0),
    c=st.floats(min_value=0.5, max_value=2.0),
)
@settings(max_examples=200)
def test_model_1_monotonic_in_h_when_c_positive(d, h_lo, h_hi, a, b, c):
    """For fixed D and positive c, Model 1 is increasing in H.

    Note that c can be negative for some components (e.g., branch biomass
    has c≈-0.41 — branch mass declines with height for fixed DBH). This test
    only covers the positive-c case; the negative-c case is the dual.

    Uses a 1e-6 ft minimum gap between heights for the same f64-ULP reason
    documented in ``test_model_1_monotonic_in_d``.
    """
    assume(abs(h_hi - h_lo) > 1e-6)
    lo, hi = sorted([h_lo, h_hi])
    y_lo = model_1(d, lo, a, b, c)
    y_hi = model_1(d, hi, a, b, c)
    assert y_lo < y_hi


# ---------------------------------------------------------------------------
# Model 4 algebraic invariant
# ---------------------------------------------------------------------------


@given(
    d=st.floats(min_value=1.0, max_value=30.0),
    h=st.floats(min_value=10.0, max_value=200.0),
    a=st.floats(min_value=1e-6, max_value=10.0),
    b=st.floats(min_value=0.5, max_value=3.0),
    c=st.floats(min_value=-1.0, max_value=2.0),
)
@settings(max_examples=200)
def test_model_4_reduces_to_model_1_when_b1_zero(d, h, a, b, c):
    """When b1 == 0, exp(0) == 1 and Model 4 collapses to Model 1."""
    m1 = model_1(d, h, a, b, c)
    m4 = model_4(d, h, a, b, b1=0.0, c=c)
    assert math.isclose(m1, m4, rel_tol=1e-12)


# ---------------------------------------------------------------------------
# Pipeline robustness
# ---------------------------------------------------------------------------


@given(
    dia=st.floats(min_value=1.0, max_value=60.0),
    ht=st.floats(min_value=10.0, max_value=200.0),
)
@settings(max_examples=100)
def test_pipeline_returns_positive_agb_for_valid_inputs(dia, ht):
    """Pipeline must return positive, finite AGB across the practical
    DBH and height range for the Douglas-fir coefficient bundle."""
    result = predict_tree_biomass(
        spcd=202,
        dia=dia,
        ht=ht,
        coefficients=_DOUGFIR_COEFS,
        wdsg=0.45,
        hw_sw="softwood",
        cull=0.0,
    )
    assert result.agb > 0
    assert math.isfinite(result.agb)
    # Component sum must equal AGB to floating-point precision
    assert math.isclose(
        result.w_wood + result.w_bark + result.w_branch,
        result.agb,
        rel_tol=1e-12,
    )


@given(
    dia=st.floats(min_value=5.0, max_value=40.0),
    ht=st.floats(min_value=20.0, max_value=150.0),
    cull=st.floats(min_value=0.0, max_value=50.0),
)
@settings(max_examples=100)
def test_cull_reduces_agb(dia, ht, cull):
    """Increasing cull percentage must monotonically reduce the harmonized AGB.

    For live trees, cull only deducts from wood weight (not bark or branch),
    so AGBReduce < 1 whenever CULL > 0.
    """
    no_cull = predict_tree_biomass(
        spcd=202,
        dia=dia,
        ht=ht,
        coefficients=_DOUGFIR_COEFS,
        wdsg=0.45,
        hw_sw="softwood",
        cull=0.0,
    )
    with_cull = predict_tree_biomass(
        spcd=202,
        dia=dia,
        ht=ht,
        coefficients=_DOUGFIR_COEFS,
        wdsg=0.45,
        hw_sw="softwood",
        cull=cull,
    )
    if cull == 0.0:
        assert math.isclose(no_cull.agb, with_cull.agb, rel_tol=1e-12)
    else:
        # Cull must reduce or hold AGB equal (it never increases it)
        assert with_cull.agb <= no_cull.agb
