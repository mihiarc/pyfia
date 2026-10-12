"""Scalar NSVB reference implementation — the test oracle (issue #127).

This is a **test-only** independent, per-tree scalar reimplementation of the
NSVB biomass math. The production data path is the vectorized polars pipeline
in :mod:`pyfia.carbon.nsvb.equations` (``compute_nsvb_biomass`` /
``compute_nsvb_dead_biomass`` / ``nsvb_biomass_expr``). This module lets the
test suite cross-check the vectorized path tree-for-tree against a deliberately
separate implementation of the same GTR-WO-104 equations, and both are anchored
to the hand-computed worked-example values in ``test_nsvb_equations.py``.

It lived in ``src/`` as the ``equations.py`` scalar functions and the
``coefficients.py`` ``lookup_coefficients`` / ``ecosubcd_to_division`` reference
lookup, until issue #127.1 moved it here so the shipped library no longer carries
a second implementation. Shared constants stay in src and are imported below.

(The ``carbon_fractions.py`` dict loaders stayed in src: they are production
dependencies — ``_compute_default_{live,dead}_carbon_fraction`` derive their
means from them — so only the equations + coefficients scalar reference moved.)

Imported by test modules via ``import nsvb_oracle`` (``pythonpath = ["tests"]``).
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Literal

import polars as pl

from pyfia.carbon.nsvb.equations import (
    _CULL_DENS_PROP,
    _HARDWOOD_SPCD_THRESHOLD,
    _K_HARDWOOD,
    _K_SOFTWOOD,
)

# ===== moved from equations.py =====


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
    """NSVB Model 2 (Segmented): a two-branch power form split at ``k``.

    Per GTR-WO-104 eq. 2::

        y = a * D^b * H^c                    for D < k
        y = a * k^(b - b1) * D^b1 * H^c       for D >= k

    ``k`` is the segmentation point: 9 inches for softwoods (SPCD < 300) and
    11 inches for hardwoods (SPCD >= 300). The two branches are continuous at
    ``D = k`` (both equal ``a * k^b * H^c``). Below the segmentation point the
    model reduces to the plain Schumacher-Hall form (Model 1); above it, the
    diameter exponent switches to ``b1``. Use :func:`_model_k` (or pass ``k``
    explicitly) when calling from the orchestrator.

    Parameters
    ----------
    d : float
        Diameter at breast height (inches).
    h : float
        Total tree height (feet).
    a, b, b1, c : float
        Model 2 coefficients from the relevant ``S*a`` table.
    k : float
        Species-class segmentation point — 9.0 (softwood) or 11.0 (hardwood).

    Returns
    -------
    float
        Predicted quantity in source-table units.
    """
    if d < k:
        return float(a * (d**b) * (h**c))
    return float(a * (k ** (b - b1)) * (d**b1) * (h**c))


def model_3(
    d: float, h: float, a: float, a1: float, b: float, c1: float, c: float
) -> float:
    """NSVB Model 3 (Continuously Variable): ``y = a * D^(a1*(1-exp(-b*D))^c1) * H^c``.

    Per GTR-WO-104 eq. 3 (the "Continuously Variable model"): the diameter
    exponent varies continuously with ``D`` through the factor
    ``a1 * (1 - exp(-b * D))^c1``, which rises from 0 toward ``a1`` as ``D``
    grows. Used for a handful of species/components (e.g. planted slash pine
    total AGB, SPCD 111 STDORGCD=1; oak spp. SPCD 800 stem wood volume). Uses
    the ``a1`` and ``c1`` coefficients (not ``b1``).

    Parameters
    ----------
    d : float
        Diameter at breast height (inches).
    h : float
        Total tree height (feet).
    a, a1, b, c1, c : float
        Model 3 coefficients from the relevant ``S*a`` table.

    Returns
    -------
    float
        Predicted quantity in source-table units.
    """
    exponent = a1 * ((1.0 - math.exp(-b * d)) ** c1)
    return float(a * (d**exponent) * (h**c))


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
    if model == 3:
        return model_3(d, h, coef["a"], coef["a1"], coef["b"], coef["c1"], coef["c"])
    if model == 4:
        return model_4(d, h, coef["a"], coef["b"], coef["b1"], coef["c"])
    if model == 5:
        return model_5_jenkins(d, h, coef["a"], coef["b"], coef["c"], wdsg)
    raise ValueError(
        f"NSVB model {model} not supported — only models 1, 2, 3, 4, 5 are "
        "implemented (model 6 is the volume-ratio model, used for merchantable "
        "volume/broken-top corrections, not for biomass components)."
    )


def predict_tree_biomass(
    spcd: int,
    dia: float,
    ht: float,
    coefficients: Coefficients,
    wdsg: float,
    hw_sw: Literal["hardwood", "softwood"],
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
    hw_sw : {"hardwood", "softwood"}
        Hardwood/softwood classification for the cull density proportion.
        Case-insensitive at runtime (``"Hardwood"`` and ``"HARDWOOD"`` are
        accepted and normalized). Callers should derive this from
        ``"softwood" if spcd < 300 else "hardwood"`` to stay consistent with
        :func:`_model_k`, which uses the same SPCD threshold to select the
        Model 2 base constant. This rule also resolves the SPCD=10 ("fir spp.")
        edge case — S10a misclassifies SPCD=10 as hardwood, but the SPCD<300
        rule correctly classifies it as softwood.
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
        If ``dia < 1.0`` (NSVB is not parameterized below the FIA minimum
        tally diameter of 1.0 inch, and some Model 1 forms would produce
        complex-number results for d<1 with fractional b).
        If ``hw_sw`` is not one of ``"hardwood"``/``"softwood"`` (after
        case-insensitive normalization).
        If a coefficient row specifies Model 6 (the volume-ratio model, not
        used for biomass components and not implemented).
    """
    # TODO(PR 2): Scalar reference implementation. PR 2's LiveTreeEstimator
    # must implement this pipeline as polars expressions on a LazyFrame
    # joined to the coefficient tables on SPCD, not by calling this function
    # per tree. See `pyfia/carbon/__init__.py` "Architectural rules" rule 2.

    # Boundary validation — give clear errors instead of letting the math
    # functions raise cryptic complex-number TypeErrors or KeyErrors.
    if dia < 1.0:
        raise ValueError(
            f"dia must be >= 1.0 inches (FIA minimum tally diameter); got {dia}. "
            "NSVB Models 1-5 are not parameterized below 1.0 and can produce "
            "complex-number results for fractional b exponents."
        )
    hw_sw_norm = hw_sw.lower()
    if hw_sw_norm not in ("hardwood", "softwood"):
        raise ValueError(
            f"hw_sw must be 'hardwood' or 'softwood' (case-insensitive); got {hw_sw!r}"
        )

    # Step 1: Total stem inside-bark wood volume (cubic feet)
    v_wood_ib = _eval_component(coefficients.volib, dia, ht, spcd, wdsg)

    # Step 2: Total stem bark volume (cubic feet)
    v_bark = _eval_component(coefficients.volbk, dia, ht, spcd, wdsg)

    # Step 3-4: Convert wood volume to weight, with cull-reduced variant
    dens_prop = _CULL_DENS_PROP[hw_sw_norm]
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


# ===== moved from coefficients.py =====


def ecosubcd_to_division(ecosubcd: str | None) -> str | None:
    """Extract the Bailey DIVISION code from a ``PLOTGEOM.ECOSUBCD`` value.

    ``ECOSUBCD`` is a 5-7 character Bailey ecoprovince subsection code
    (e.g., ``"231Ae"`` for the Southeastern Mixed Forest section 231A,
    subsection 231Ae, or ``"M231Aa"`` for the Ouachita Mixed Forest
    mountain variant). The Bailey hierarchy is:

    ::

        Domain → Division → Province → Section → Subsection
        200    → 230      → 231      → 231A    → 231Ae

    This function walks one level up the hierarchy from Subsection to
    Division by:

    - extracting the 3-digit Province code (first 3 chars after any ``M``)
    - replacing its last digit with ``"0"`` to obtain the Division
    - preserving the ``"M"`` prefix for mountain Divisions

    Examples
    --------
    >>> ecosubcd_to_division("231Ae")
    '230'
    >>> ecosubcd_to_division("232Bh")
    '230'
    >>> ecosubcd_to_division("M231Aa")
    'M230'
    >>> ecosubcd_to_division("220Eb")
    '220'
    >>> ecosubcd_to_division(None) is None
    True
    >>> ecosubcd_to_division("") is None
    True
    >>> ecosubcd_to_division("XYZ") is None
    True

    Parameters
    ----------
    ecosubcd : str or None
        The ECOSUBCD value from ``PLOTGEOM.ECOSUBCD`` or ``PLOT.ECOSUBCD``
        (the column exists on both tables — pyfia's DataMart CSV downloads
        pull it reliably from ``PLOTGEOM``).

    Returns
    -------
    str or None
        The Bailey DIVISION code matching the ``DIVISION`` column in the
        NSVB coefficient tables (e.g., ``"230"``, ``"M230"``, ``"240"``),
        or ``None`` if the input is null, empty, or malformed.
    """
    if ecosubcd is None:
        return None
    s = ecosubcd.strip().upper()
    if not s:
        return None
    m_prefix = ""
    if s.startswith("M"):
        m_prefix = "M"
        s = s[1:]
    if len(s) < 3 or not s[:2].isdigit() or not s[2].isdigit():
        return None
    return m_prefix + s[:2] + "0"


def _row_to_dict(row: pl.DataFrame, source: str) -> dict:
    """Convert a single-row Polars DataFrame to a coefficient dict.

    Returns the row's columns as a plain Python dict, with NaN/null values
    coerced to 0.0 for numeric fields and the lookup ``source`` tag attached.
    """
    raw = row.to_dicts()[0]
    out: dict = {"source": source}
    for key in ("model", "a", "a1", "b", "b1", "c", "c1"):
        val = raw.get(key)
        if val is None:
            out[key] = 0.0
        else:
            out[key] = float(val)
    out["model"] = int(out["model"])
    return out


def lookup_coefficients(
    table_spcd: pl.DataFrame,
    table_jenkins: pl.DataFrame,
    spcd: int,
    jenkins_spgrpcd: int | None = None,
    division: str | None = None,
    stdorgcd: int | None = None,
) -> dict:
    """Resolve a coefficient row for one tree following NSVB lookup precedence.

    The Phase 1 implementation walks precedence levels 1-4 in order. Phase 1
    typically only hits levels 3 and 4: the SPCD-only species-level row, or
    the Jenkins fallback for unsupported species. The DIVISION-specific levels
    1 and 2 are wired but unused until pyFIA gains a ``PLOT.ECOSUBCD →
    DIVISION`` mapping.

    Parameters
    ----------
    table_spcd : pl.DataFrame
        The ``S*a`` species-keyed table (e.g., ``volib_spcd``).
    table_jenkins : pl.DataFrame
        The ``S*b`` Jenkins-keyed fallback table.
    spcd : int
        FIA species code from ``TREE.SPCD``.
    jenkins_spgrpcd : int, optional
        Jenkins species group code from ``REF_SPECIES.JENKINS_SPGRPCD``.
        Required if the species is not in the SPCD table (level 4 fallback).
    division : str, optional
        Bailey ecoprovince division code (e.g., ``"M240"``). Phase 1 always
        passes ``None`` here.
    stdorgcd : int, optional
        Stand origin code from ``COND.STDORGCD``. Phase 1 always passes ``None``.

    Returns
    -------
    dict
        Coefficient dict with keys ``model, a, a1, b, b1, c, c1, source``.
        ``source`` is one of ``"spcd_division_stdorg"``, ``"spcd_division"``,
        ``"spcd"``, or ``"jenkins"``.

    Raises
    ------
    KeyError
        If neither the SPCD nor any Jenkins fallback can be resolved.
    """
    # TODO(PR 2): Scalar reference implementation. The vectorized
    # LiveTreeEstimator must replace per-tree calls with a polars join on
    # SPCD against the coefficient tables, not call this function in a loop.
    # See `pyfia/carbon/__init__.py` "Architectural rules" rule 2.
    df = table_spcd.filter(pl.col("SPCD") == spcd)

    # Level 1: SPCD + DIVISION + STDORGCD exact match
    if division is not None and stdorgcd is not None:
        match = df.filter(
            (pl.col("DIVISION") == division) & (pl.col("STDORGCD") == stdorgcd)
        )
        if match.height > 0:
            return _row_to_dict(match.head(1), "spcd_division_stdorg")

    # Level 2: SPCD + DIVISION (STDORGCD null)
    if division is not None:
        match = df.filter(
            (pl.col("DIVISION") == division) & pl.col("STDORGCD").is_null()
        )
        if match.height > 0:
            return _row_to_dict(match.head(1), "spcd_division")

    # Level 3: SPCD only (DIVISION and STDORGCD both null) — the species-level row
    match = df.filter(pl.col("DIVISION").is_null() & pl.col("STDORGCD").is_null())
    if match.height > 0:
        return _row_to_dict(match.head(1), "spcd")

    # Level 4: Jenkins fallback. Note that S*b tables have different schemas
    # (no DIVISION/STDORGCD columns) and are keyed on JENKINS_SPGRPCD.
    if jenkins_spgrpcd is not None:
        jdf = table_jenkins.filter(pl.col("JENKINS_SPGRPCD") == jenkins_spgrpcd)
        if jdf.height > 0:
            return _row_to_dict(jdf.head(1), "jenkins")

    raise KeyError(
        f"No NSVB coefficients for SPCD={spcd}, JENKINS_SPGRPCD={jenkins_spgrpcd}, "
        f"DIVISION={division}, STDORGCD={stdorgcd}. Check the species coverage "
        "in src/pyfia/carbon/nsvb/data/."
    )
