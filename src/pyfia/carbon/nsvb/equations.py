"""
NSVB allometric equation forms (Westfall et al. 2023, GTR-WO-104).

Pure-math equation library for the National Scale Volume and Biomass framework.
No I/O, no Polars dependency — just floats in and floats out. Coefficient lookup
lives in coefficients.py; carbon fractions live in carbon_fractions.py.

The four model forms used in Phase 1 (live tree biomass) are:

- **Model 1**: ``y = a * D^b * H^c`` — power form, the most common.
- **Model 2**: ``y = a * k^(b-b1) * D^b1 * H^c`` — with ``k = 9`` for softwoods
  (SPCD < 300) and ``k = 11`` for hardwoods (SPCD >= 300). The constants 9 and 11
  match the sawlog top-diameter cutoffs used elsewhere in NSVB. Verified against
  the Douglas-fir (SPCD=202) wood-volume worked example and back-solved against
  the red maple (SPCD=316) bark-volume worked example.
- **Model 4**: ``y = a * D^b * H^c * exp(-b1 * D)`` — power form modulated by an
  exponential of D. The b1 parameter is typically a small negative number, so
  the multiplicative factor is slightly greater than 1 across the practical D
  range. Verified against the red maple S8a (total AGB) worked example.
- **Model 5 (Jenkins fallback)**: ``y = a * D^b * H^c * WDSG`` — used when a
  species lacks SPCD-specific coefficients and falls back to its Jenkins species
  group. Wood density (WDSG) comes from FIADB ``REF_SPECIES.WOOD_SPGR_GREENVOL_DRYWT``.

**Model 3 and Model 6 are NOT implemented in Phase 1**. Model 3 is a three-parameter
form whose exact shape is picture-omitted in the source PDF and unverified.
Model 6 is the iterative Schumacher-Hall volume-ratio model used for merchantable
subdivision (DRYBIO_BOLE / DRYBIO_TOP / DRYBIO_STUMP); total above-ground biomass
and total carbon do not require it. Both will arrive in a later phase if downstream
work needs them.

The orchestrator :func:`predict_tree_biomass` runs the full per-tree pipeline:
predict component biomasses (wood, bark, branches), predict directly the total AGB,
then harmonize the components so they sum exactly to the predicted total. The
harmonization is documented in ``gtr_wo104_westfall2023.md`` lines 600-612 (live tree
worked example) and lines 866-898 (cull-adjusted variant for the red maple example).

References
----------
- Westfall, J.A. et al. (2023). GTR-WO-104. DOI: 10.2737/WO-GTR-104
- Worked examples: ``gtr_wo104_westfall2023.md`` lines 394-680 (Douglas-fir,
  no cull) and lines 682-960 (red maple, with 3% cull).
"""

from __future__ import annotations

import math
from dataclasses import dataclass

# Sawlog top-diameter cutoffs that double as the Model 2 ``k`` constant.
# Per worked example line 462: softwood top diameter is 7-9 inches and hardwood is
# 9-11 inches, depending on species. The Model 2 base constant matches the upper
# bound of these cutoffs.
_K_SOFTWOOD = 9.0
_K_HARDWOOD = 11.0
_HARDWOOD_SPCD_THRESHOLD = 300


def _model_k(spcd: int) -> float:
    """Return the Model 2 base constant ``k`` for a given species code.

    Softwoods (SPCD < 300) use k=9; hardwoods (SPCD >= 300) use k=11. The split
    point matches the FIA hardwood/softwood classification used throughout NSVB.
    """
    return _K_HARDWOOD if spcd >= _HARDWOOD_SPCD_THRESHOLD else _K_SOFTWOOD


def model_1(d: float, h: float, a: float, b: float, c: float) -> float:
    """NSVB Model 1: ``y = a * D^b * H^c``.

    The most common NSVB form, used for stem wood volume (S1a), stem bark volume
    (S2a), stem bark biomass (S6a), branch biomass (S7a), total AGB (S8a), and
    foliage biomass (S9a) for the majority of FIA species.

    Parameters
    ----------
    d : float
        Diameter at breast height (inches).
    h : float
        Total tree height (feet).
    a, b, c : float
        Model 1 coefficients from the relevant ``S*a`` table.

    Returns
    -------
    float
        Predicted quantity in the units of the source table (cubic feet for
        volume tables, pounds for biomass tables).
    """
    return float(a * (d**b) * (h**c))


def model_2(
    d: float, h: float, a: float, b: float, b1: float, c: float, k: float
) -> float:
    """NSVB Model 2: ``y = a * k^(b - b1) * D^b1 * H^c``.

    Power form with a species-class base constant ``k``. Used for stem wood
    volume and stem bark volume on a subset of species. The ``k`` constant
    is 9 for softwoods (SPCD < 300) and 11 for hardwoods (SPCD >= 300); use
    :func:`_model_k` (or pass it explicitly) when calling from the orchestrator.

    Parameters
    ----------
    d : float
        Diameter at breast height (inches).
    h : float
        Total tree height (feet).
    a, b, b1, c : float
        Model 2 coefficients from the relevant ``S*a`` table.
    k : float
        Species-class base constant — typically 9.0 (softwood) or 11.0 (hardwood).

    Returns
    -------
    float
        Predicted quantity in source-table units.
    """
    return float(a * (k ** (b - b1)) * (d**b1) * (h**c))


def model_4(d: float, h: float, a: float, b: float, b1: float, c: float) -> float:
    """NSVB Model 4: ``y = a * D^b * H^c * exp(-b1 * D)``.

    Power form modulated by an exponential of D. Used for total AGB on a small
    set of species (e.g., red maple SPCD=316). The b1 coefficient is typically
    small (often slightly negative), so the exp factor is close to 1.

    Parameters
    ----------
    d : float
        Diameter at breast height (inches).
    h : float
        Total tree height (feet).
    a, b, b1, c : float
        Model 4 coefficients from the relevant ``S*a`` table.

    Returns
    -------
    float
        Predicted quantity in source-table units.
    """
    return float(a * (d**b) * (h**c) * math.exp(-b1 * d))


def model_5_jenkins(
    d: float, h: float, a: float, b: float, c: float, wdsg: float
) -> float:
    """NSVB Model 5 (Jenkins-group fallback): ``y = a * D^b * H^c * WDSG``.

    Used when a species lacks SPCD-specific coefficients in S1a-S8a and falls
    back to its Jenkins species group from S1b-S8b. Wood density (WDSG) is
    sourced from FIADB ``REF_SPECIES.WOOD_SPGR_GREENVOL_DRYWT``.

    Parameters
    ----------
    d : float
        Diameter at breast height (inches).
    h : float
        Total tree height (feet).
    a, b, c : float
        Model 5 coefficients from the relevant ``S*b`` Jenkins table.
    wdsg : float
        Wood specific gravity (green volume, dry weight basis) for the species.

    Returns
    -------
    float
        Predicted quantity in source-table units.
    """
    return float(a * (d**b) * (h**c) * wdsg)


def harmonize_components(
    agb_predicted: float,
    w_wood: float,
    w_bark: float,
    w_branch: float,
) -> tuple[float, float, float]:
    """Proportionally redistribute component weights to sum to the predicted AGB.

    NSVB makes two independent predictions of total above-ground biomass: one
    by summing the component predictions (wood + bark + branch) and one by
    directly predicting AGB from D and H via S8a/S8b. The two estimates rarely
    agree exactly. Westfall et al. (2023) chose the directly-predicted AGB as
    the truth and rescale the component sum proportionally so the harmonized
    components sum exactly to ``agb_predicted`` while preserving their relative
    ratios.

    Per worked example lines 600-612 (Douglas-fir, no cull) and 886-898 (red
    maple, with cull). For trees with cull or decay, the caller should pass
    the *cull-reduced* component weights and the *cull-reduced* predicted AGB
    (``agb_predicted = AGB_predicted * AGBReduce``); see
    :func:`predict_tree_biomass` for the full pipeline.

    Parameters
    ----------
    agb_predicted : float
        Directly-predicted total above-ground biomass (lb).
    w_wood, w_bark, w_branch : float
        Component biomass predictions (lb).

    Returns
    -------
    tuple[float, float, float]
        Harmonized (wood, bark, branch) weights such that
        ``wood_h + bark_h + branch_h == agb_predicted`` to numerical precision.
        If the component sum is zero or negative, returns
        ``(agb_predicted, 0.0, 0.0)`` as a degenerate fallback.
    """
    component_sum = w_wood + w_bark + w_branch
    if component_sum <= 0:
        return (agb_predicted, 0.0, 0.0)
    wood_h = agb_predicted * (w_wood / component_sum)
    bark_h = agb_predicted * (w_bark / component_sum)
    branch_h = agb_predicted * (w_branch / component_sum)
    return wood_h, bark_h, branch_h


# DECAYCD=3 wood density proportions used to discount cull wood weight, per
# worked example line 550 and Harmon et al. (2011) Table 1. Cull wood is
# typically partially rotten, so its weight is reduced (but not removed entirely).
_CULL_DENS_PROP = {"hardwood": 0.54, "softwood": 0.92}


@dataclass(frozen=True)
class Coefficients:
    """A bundle of coefficients for one tree's NSVB pipeline.

    Each component table (volib, volbk, bark biomass, branch biomass, total
    AGB) supplies its own coefficient row, looked up via
    :func:`pyfia.carbon.nsvb.coefficients.lookup_coefficients`. The
    ``volib`` and ``volbk`` entries can be Model 1 or Model 2; ``bark_bio``
    and ``branch_bio`` are Model 1 in the data we've inspected;
    ``total_agb`` can be Model 1 or Model 4.

    Each entry is a dict with keys ``model, a, a1, b, b1, c, c1, source``.
    ``source`` is the lookup-precedence outcome (``spcd_division_stdorg``,
    ``spcd_division``, ``spcd``, or ``jenkins``).
    """

    volib: dict
    volbk: dict
    bark_bio: dict
    branch_bio: dict
    total_agb: dict


@dataclass(frozen=True)
class TreeBiomassResult:
    """Per-tree NSVB biomass output (component-harmonized).

    All weights are in pounds. ``agb`` equals ``w_wood + w_bark + w_branch``
    by construction (harmonization invariant).
    """

    w_wood: float
    w_bark: float
    w_branch: float
    agb: float
    v_wood_ib: float  # for adjusted wood density downstream (Phase 2+)
    v_bark: float


def _eval_component(coef: dict, d: float, h: float, spcd: int, wdsg: float) -> float:
    """Dispatch a single component prediction to the right model form."""
    model = int(coef["model"])
    if model == 1:
        return model_1(d, h, coef["a"], coef["b"], coef["c"])
    if model == 2:
        return model_2(
            d, h, coef["a"], coef["b"], coef["b1"], coef["c"], _model_k(spcd)
        )
    if model == 4:
        return model_4(d, h, coef["a"], coef["b"], coef["b1"], coef["c"])
    if model == 5:
        return model_5_jenkins(d, h, coef["a"], coef["b"], coef["c"], wdsg)
    raise ValueError(
        f"NSVB model {model} not supported in Phase 1 — only models 1, 2, 4, 5 are implemented."
    )


def predict_tree_biomass(
    spcd: int,
    dia: float,
    ht: float,
    coefficients: Coefficients,
    wdsg: float,
    hw_sw: str,
    cull: float = 0.0,
) -> TreeBiomassResult:
    """Run the full NSVB per-tree biomass pipeline for one live tree.

    Pipeline (matches the Douglas-fir example at ``gtr_wo104_westfall2023.md:394``
    and the red maple example at ``:682``):

    1. Predict total stem inside-bark wood volume from S1a/S1b.
    2. Predict total stem bark volume from S2a/S2b.
    3. Convert wood volume to gross weight: ``W_w = V_w * WDSG * 62.4``.
    4. Apply cull deduction to wood weight: ``W_w_red = V_w * (1 - CULL/100 * (1 - DensProp)) * WDSG * 62.4``
       where ``DensProp`` is 0.54 for hardwoods and 0.92 for softwoods (the
       DECAYCD=3 wood density proportion from Harmon et al. 2011, used as the
       standard cull-density assumption per worked example line 550).
    5. Predict stem bark biomass from S6a/S6b.
    6. Predict branch biomass from S7a.
    7. Predict total AGB directly from S8a/S8b.
    8. Compute the cull-reduction factor:
       ``AGBReduce = (W_w_red + W_b + W_br) / (W_w + W_b + W_br)``.
    9. Reduce predicted AGB: ``AGB_predicted_red = AGB_predicted * AGBReduce``.
    10. Harmonize components against ``AGB_predicted_red`` so they sum exactly
        to it (proportional redistribution).

    Note: Phase 1 assumes live trees with intact tops, so bark and branch
    weights have no broken-top deductions. Standing dead trees and broken-top
    deductions arrive in Phase 2.

    Foliage is **not** part of AGB and is not computed here.

    Belowground (coarse root) biomass is **not** computed here. Phase 1 reads
    FIADB ``CARBON_BG`` directly via the live-tree estimator's BG bridge.

    Parameters
    ----------
    spcd : int
        FIA species code. Used to select the Model 2 ``k`` constant.
    dia : float
        Diameter at breast height (inches). Must be >= 1.0.
    ht : float
        Total tree height (feet).
    coefficients : Coefficients
        Bundle of coefficient rows for the five tables (volib, volbk,
        bark_bio, branch_bio, total_agb), looked up upstream by
        :func:`pyfia.carbon.nsvb.coefficients.lookup_coefficients`.
    wdsg : float
        Wood specific gravity (green volume, dry weight) from FIADB
        ``REF_SPECIES.WOOD_SPGR_GREENVOL_DRYWT``.
    hw_sw : str
        ``"hardwood"`` or ``"softwood"`` — controls the cull density proportion.
    cull : float, default 0.0
        Cull percentage from FIADB ``TREE.CULL`` (0-100). Defaults to 0 for
        live trees with no cull.

    Returns
    -------
    TreeBiomassResult
        Harmonized component weights and total AGB in pounds, plus the gross
        wood and bark volumes (cubic feet) for downstream adjusted-density
        calculations.

    Raises
    ------
    ValueError
        If a coefficient row specifies a Model 3 or Model 6 (not implemented
        in Phase 1).
    """
    # TODO(PR 2): Scalar reference implementation. PR 2's LiveTreeEstimator
    # must implement this pipeline as polars expressions on a LazyFrame
    # joined to the coefficient tables on SPCD, not by calling this function
    # per tree. See `pyfia/carbon/__init__.py` "Architectural rules" rule 2.
    # Step 1: Total stem inside-bark wood volume (cubic feet)
    v_wood_ib = _eval_component(coefficients.volib, dia, ht, spcd, wdsg)

    # Step 2: Total stem bark volume (cubic feet)
    v_bark = _eval_component(coefficients.volbk, dia, ht, spcd, wdsg)

    # Step 3-4: Convert wood volume to weight, with cull-reduced variant
    dens_prop = _CULL_DENS_PROP[hw_sw]
    w_wood_gross = v_wood_ib * wdsg * 62.4
    w_wood_red = v_wood_ib * (1.0 - cull / 100.0 * (1.0 - dens_prop)) * wdsg * 62.4

    # Step 5: Stem bark biomass (live, no broken-top deduction)
    w_bark = _eval_component(coefficients.bark_bio, dia, ht, spcd, wdsg)

    # Step 6: Branch biomass (live, no broken-top deduction)
    w_branch = _eval_component(coefficients.branch_bio, dia, ht, spcd, wdsg)

    # Step 7: Directly-predicted total AGB
    agb_predicted = _eval_component(coefficients.total_agb, dia, ht, spcd, wdsg)

    # Step 8: Cull-reduction factor (NB: only wood is cull-reduced for live trees;
    # bark and branch use their gross values in the denominator per worked example
    # lines 870-880).
    component_gross_sum = w_wood_gross + w_bark + w_branch
    component_red_sum = w_wood_red + w_bark + w_branch
    if component_gross_sum <= 0:
        agb_reduce = 0.0
    else:
        agb_reduce = component_red_sum / component_gross_sum

    # Step 9: Reduce predicted AGB
    agb_predicted_red = agb_predicted * agb_reduce

    # Step 10: Harmonize the cull-reduced components against the reduced predicted AGB
    w_wood_h, w_bark_h, w_branch_h = harmonize_components(
        agb_predicted_red, w_wood_red, w_bark, w_branch
    )

    return TreeBiomassResult(
        w_wood=w_wood_h,
        w_bark=w_bark_h,
        w_branch=w_branch_h,
        agb=w_wood_h + w_bark_h + w_branch_h,
        v_wood_ib=v_wood_ib,
        v_bark=v_bark,
    )
