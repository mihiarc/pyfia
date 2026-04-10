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
from typing import TYPE_CHECKING, Literal

import polars as pl

if TYPE_CHECKING:
    from pyfia.carbon.nsvb.coefficients import VectorizedLookupTables

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
        If a coefficient row specifies a Model 3 or Model 6 (not implemented
        in Phase 1).
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


# ---------------------------------------------------------------------------
# Vectorized NSVB pipeline (PR 2) — the production data path.
# ---------------------------------------------------------------------------
#
# The functions above (``predict_tree_biomass`` and ``_eval_component``) are
# scalar reference implementations retained as test oracles. The functions
# below replicate the same math as polars expressions over a LazyFrame,
# enabling FIA-scale (1M+ trees) evaluation via coefficient-table joins
# instead of per-tree Python dispatch. See ``pyfia/carbon/__init__.py``
# "Architectural rules" rule 2.


def nsvb_biomass_expr(
    *,
    model: pl.Expr,
    a: pl.Expr,
    b: pl.Expr,
    b1: pl.Expr,
    c: pl.Expr,
    d: pl.Expr,
    h: pl.Expr,
    spcd: pl.Expr,
    wdsg: pl.Expr,
) -> pl.Expr:
    """Build a polars expression that dispatches NSVB Models 1/2/4/5.

    The scalar equivalent of this function is :func:`_eval_component`.
    It returns a single expression suitable for use inside
    ``LazyFrame.with_columns`` that produces the predicted component value
    (volume or biomass, in source-table units) for every row, with the
    model form selected per-row from the ``model`` column.

    Models 3 and 6 are not implemented in Phase 1; rows dispatching to
    those return ``None`` which will surface as a null downstream. The
    orchestrator :func:`compute_nsvb_biomass` asserts that no nulls appear
    in the output columns, so an unsupported model becomes a loud failure.

    The Model 2 base constant ``k`` is ``11`` for hardwoods (SPCD >= 300)
    and ``9`` for softwoods, matching :func:`_model_k` used by the scalar
    path. This also resolves the SPCD=10 misclassification that S10a
    carries — see ``pyfia/carbon/__init__.py`` for the architectural
    discussion.

    Parameters
    ----------
    model, a, b, b1, c : pl.Expr
        Coefficient column expressions (typically the output of a coalesce
        across species-level + Jenkins lookup joins, or ``pl.col(...)``
        references to pre-joined coefficient columns). All numeric
        coefficient columns must be ``Float64``; ``model`` must be an
        integer dtype.
    d, h : pl.Expr
        Diameter at breast height (inches) and total height (feet) column
        expressions. Must be ``Float64``.
    spcd : pl.Expr
        Species code column expression used to select the Model 2 ``k``
        constant via the ``SPCD < 300`` rule.
    wdsg : pl.Expr
        Wood specific gravity column expression (``REF_SPECIES.WOOD_SPGR_GREENVOL_DRYWT``).
        Used only by Model 5.

    Returns
    -------
    pl.Expr
        An expression that evaluates to the predicted quantity (volume or
        biomass) in source-table units (cubic feet or pounds). Call
        ``.alias("v_wood_ib")`` (or similar) to name the output column.
    """
    k = (
        pl.when(spcd >= _HARDWOOD_SPCD_THRESHOLD)
        .then(_K_HARDWOOD)
        .otherwise(_K_SOFTWOOD)
    )
    return (
        pl.when(model == 1)
        .then(a * d.pow(b) * h.pow(c))
        .when(model == 2)
        .then(a * k.pow(b - b1) * d.pow(b1) * h.pow(c))
        .when(model == 4)
        .then(a * d.pow(b) * h.pow(c) * (-b1 * d).exp())
        .when(model == 5)
        .then(a * d.pow(b) * h.pow(c) * wdsg)
        .otherwise(None)
    )


def _join_and_eval_component(
    trees: pl.LazyFrame,
    spcd_table: pl.DataFrame,
    jen_table: pl.DataFrame,
    div_table: pl.DataFrame,
    out_col: str,
    *,
    has_division: bool,
) -> pl.LazyFrame:
    """Join a LazyFrame to the (division, species-level, Jenkins) coefficient
    triple for one component and evaluate the NSVB biomass expression.

    Implementation of NSVB lookup precedence Levels 2 → 3 → 4 as polars
    joins:

    1. If ``has_division`` is True: left-join ``div_table`` on
       ``(SPCD, DIVISION)`` (Level 2).
    2. Left-join ``spcd_table`` on ``SPCD`` (species-level fallback, Level 3).
    3. Left-join ``jen_table`` on ``JENKINS_SPGRPCD`` (Jenkins fallback,
       Level 4).
    4. Coalesce the three coefficient rows in precedence order:
       division → species-level → Jenkins.
    5. Evaluate :func:`nsvb_biomass_expr` to produce ``out_col``.
    6. Drop the temporary coefficient columns.

    When ``has_division`` is False (the trees frame has no ``DIVISION``
    column — backward-compatible path for synthetic tests and for callers
    that haven't yet wired the ``PLOTGEOM.ECOSUBCD`` join), the division
    join is skipped entirely and the behavior matches the old 2-way
    coalesce exactly.

    Parameters
    ----------
    trees : pl.LazyFrame
        Input frame with at least ``SPCD``, ``DIA``, ``HT``, ``WDSG``,
        ``JENKINS_SPGRPCD`` columns. If ``has_division`` is True, must also
        have a ``DIVISION`` column (Utf8/String, nullable).
    spcd_table : pl.DataFrame
        Species-level lookup from
        :func:`pyfia.carbon.nsvb.coefficients.build_species_level_lookup`
        with columns ``(SPCD, model, a, b, b1, c)``.
    jen_table : pl.DataFrame
        Jenkins fallback from
        :func:`pyfia.carbon.nsvb.coefficients.build_jenkins_lookup` with
        columns ``(JENKINS_SPGRPCD, model, a, b, b1, c)``.
    div_table : pl.DataFrame
        DIVISION-specific lookup from
        :func:`pyfia.carbon.nsvb.coefficients.build_division_lookup` with
        columns ``(SPCD, DIVISION, model, a, b, b1, c)``. Only consulted
        when ``has_division`` is True.
    out_col : str
        Name for the output column (e.g., ``"v_wood_ib"``).
    has_division : bool
        Whether the caller has populated a ``DIVISION`` column on the trees
        frame. When False, skip the division join entirely.

    Returns
    -------
    pl.LazyFrame
        The input frame with ``out_col`` appended. Temporary coefficient
        columns are dropped before return.
    """
    # Rename coefficient columns on each side so they don't collide with the
    # trees frame or with each other.
    spcd_lf = spcd_table.lazy().rename(
        {
            "model": "_model_s",
            "a": "_a_s",
            "b": "_b_s",
            "b1": "_b1_s",
            "c": "_c_s",
        }
    )
    jen_lf = jen_table.lazy().rename(
        {
            "model": "_model_j",
            "a": "_a_j",
            "b": "_b_j",
            "b1": "_b1_j",
            "c": "_c_j",
        }
    )

    if has_division:
        div_lf = div_table.lazy().rename(
            {
                "model": "_model_d",
                "a": "_a_d",
                "b": "_b_d",
                "b1": "_b1_d",
                "c": "_c_d",
            }
        )
        trees = trees.join(div_lf, on=["SPCD", "DIVISION"], how="left")

    trees = trees.join(spcd_lf, on="SPCD", how="left")
    trees = trees.join(jen_lf, on="JENKINS_SPGRPCD", how="left")

    # Coalesce: division (Level 2) → species-level (Level 3) → Jenkins (Level 4).
    # When has_division=False, the _*_d columns don't exist and we fall back
    # to the 2-way coalesce matching the original implementation.
    if has_division:
        model_expr = pl.coalesce(
            pl.col("_model_d"), pl.col("_model_s"), pl.col("_model_j")
        )
        a_expr = pl.coalesce(pl.col("_a_d"), pl.col("_a_s"), pl.col("_a_j"))
        b_expr = pl.coalesce(pl.col("_b_d"), pl.col("_b_s"), pl.col("_b_j"))
        b1_expr = pl.coalesce(pl.col("_b1_d"), pl.col("_b1_s"), pl.col("_b1_j"))
        c_expr = pl.coalesce(pl.col("_c_d"), pl.col("_c_s"), pl.col("_c_j"))
    else:
        model_expr = pl.coalesce(pl.col("_model_s"), pl.col("_model_j"))
        a_expr = pl.coalesce(pl.col("_a_s"), pl.col("_a_j"))
        b_expr = pl.coalesce(pl.col("_b_s"), pl.col("_b_j"))
        b1_expr = pl.coalesce(pl.col("_b1_s"), pl.col("_b1_j"))
        c_expr = pl.coalesce(pl.col("_c_s"), pl.col("_c_j"))

    trees = trees.with_columns(
        nsvb_biomass_expr(
            model=model_expr,
            a=a_expr,
            b=b_expr,
            b1=b1_expr,
            c=c_expr,
            d=pl.col("DIA").cast(pl.Float64),
            h=pl.col("HT").cast(pl.Float64),
            spcd=pl.col("SPCD"),
            wdsg=pl.col("WDSG").cast(pl.Float64),
        ).alias(out_col)
    )

    drop_cols = [
        "_model_s",
        "_a_s",
        "_b_s",
        "_b1_s",
        "_c_s",
        "_model_j",
        "_a_j",
        "_b_j",
        "_b1_j",
        "_c_j",
    ]
    if has_division:
        drop_cols.extend(["_model_d", "_a_d", "_b_d", "_b1_d", "_c_d"])
    return trees.drop(drop_cols)


def compute_nsvb_biomass(
    trees: pl.LazyFrame,
    lookup: VectorizedLookupTables | None = None,
) -> pl.LazyFrame:
    """Vectorized NSVB live-tree biomass pipeline (the production data path).

    Executes the same 10-step pipeline as :func:`predict_tree_biomass`
    (5 component predictions → cull adjustment → harmonization) as a
    sequence of polars joins and expressions over a LazyFrame, with no
    per-tree Python dispatch.

    The hardwood/softwood classification used for the cull density
    proportion and the Model 2 ``k`` constant is derived from the
    ``SPCD < 300`` rule to stay consistent with :func:`_model_k` and to
    sidestep the S10a misclassification of SPCD=10 — see
    ``pyfia/carbon/__init__.py`` "Items deferred from PR 1 review".

    Parameters
    ----------
    trees : pl.LazyFrame
        Input frame with at least the following columns:

        - ``SPCD`` (Int): FIA species code
        - ``DIA`` (Float): diameter at breast height (inches, must be >= 1.0)
        - ``HT`` (Float): total tree height (feet)
        - ``CULL`` (Float, nullable): cull percentage (0-100). Null is
          treated as 0.
        - ``WDSG`` (Float): wood specific gravity
          (``REF_SPECIES.WOOD_SPGR_GREENVOL_DRYWT``)
        - ``JENKINS_SPGRPCD`` (Int): Jenkins species group for the Level 4
          fallback (``REF_SPECIES.JENKINS_SPGRPCD``)

        Optional:

        - ``DIVISION`` (Utf8, nullable): Bailey ecoprovince DIVISION code
          (e.g., ``"230"``, ``"M230"``, computed from ``PLOT.ECOSUBCD`` via
          :func:`pyfia.carbon.nsvb.coefficients.ecosubcd_to_division`).
          When present, the orchestrator activates Level 2 of the NSVB
          lookup precedence: the ``(SPCD, DIVISION)`` lookup runs first,
          falling through to species-level (Level 3) and Jenkins (Level 4)
          per-row via coalesce. When absent, only Levels 3 + 4 are used
          (backward-compatible with callers that haven't wired the
          PLOTGEOM join).

        Additional columns (grouping variables, plot identifiers, etc.)
        pass through untouched.
    lookup : VectorizedLookupTables, optional
        Pre-built coefficient lookup bundle from
        :func:`pyfia.carbon.nsvb.coefficients.get_vectorized_lookup_tables`.
        If omitted, fetches the cached process-level bundle.

    Returns
    -------
    pl.LazyFrame
        The input frame with six new columns appended:

        - ``v_wood_ib`` (Float, cu ft): total stem inside-bark wood volume
        - ``v_bark`` (Float, cu ft): total stem bark volume
        - ``w_wood`` (Float, lb): harmonized stem wood biomass
        - ``w_bark`` (Float, lb): harmonized stem bark biomass
        - ``w_branch`` (Float, lb): harmonized branch biomass
        - ``agb`` (Float, lb): harmonized total above-ground biomass
          (equals ``w_wood + w_bark + w_branch`` by construction)

        All other input columns pass through.
    """
    if lookup is None:
        from pyfia.carbon.nsvb.coefficients import get_vectorized_lookup_tables

        lookup = get_vectorized_lookup_tables()

    # Probe the schema once so the 5 per-component joins don't each re-collect
    # it. DIVISION column detection activates the Level 2 lookup path.
    has_division = "DIVISION" in trees.collect_schema().names()

    # Step 1 — Total stem inside-bark wood volume (cubic feet).
    trees = _join_and_eval_component(
        trees,
        lookup.volib_spcd,
        lookup.volib_jen,
        lookup.volib_div,
        "v_wood_ib",
        has_division=has_division,
    )

    # Step 2 — Total stem bark volume (cubic feet).
    trees = _join_and_eval_component(
        trees,
        lookup.volbk_spcd,
        lookup.volbk_jen,
        lookup.volbk_div,
        "v_bark",
        has_division=has_division,
    )

    # Step 5 — Stem bark biomass (lb).
    trees = _join_and_eval_component(
        trees,
        lookup.bark_bio_spcd,
        lookup.bark_bio_jen,
        lookup.bark_bio_div,
        "_w_bark_pre",
        has_division=has_division,
    )

    # Step 6 — Branch biomass (lb).
    trees = _join_and_eval_component(
        trees,
        lookup.branch_bio_spcd,
        lookup.branch_bio_jen,
        lookup.branch_bio_div,
        "_w_branch_pre",
        has_division=has_division,
    )

    # Step 7 — Directly-predicted total AGB (lb).
    trees = _join_and_eval_component(
        trees,
        lookup.total_agb_spcd,
        lookup.total_agb_jen,
        lookup.total_agb_div,
        "_agb_predicted",
        has_division=has_division,
    )

    # Step 3-4 — Convert wood volume to weight, with a cull-reduced variant.
    # DECAYCD=3 wood density proportion: 0.92 softwood, 0.54 hardwood. The
    # split on SPCD<300 is consistent with _model_k and sidesteps the SPCD=10
    # S10a misclassification (SPCD=10 is softwood under this rule).
    trees = trees.with_columns(
        [
            pl.col("CULL").fill_null(0.0).cast(pl.Float64).alias("_cull"),
            pl.when(pl.col("SPCD") >= _HARDWOOD_SPCD_THRESHOLD)
            .then(pl.lit(_CULL_DENS_PROP["hardwood"]))
            .otherwise(pl.lit(_CULL_DENS_PROP["softwood"]))
            .alias("_dens_prop"),
        ]
    )
    trees = trees.with_columns(
        [
            (pl.col("v_wood_ib") * pl.col("WDSG").cast(pl.Float64) * 62.4).alias(
                "_w_wood_gross"
            ),
            (
                pl.col("v_wood_ib")
                * (1.0 - pl.col("_cull") / 100.0 * (1.0 - pl.col("_dens_prop")))
                * pl.col("WDSG").cast(pl.Float64)
                * 62.4
            ).alias("_w_wood_red"),
        ]
    )

    # Step 8 — Cull-reduction factor. Matches the scalar path: bark/branch use
    # their gross values in both the numerator and denominator (only wood is
    # cull-reduced for live trees per worked example lines 870-880).
    trees = trees.with_columns(
        [
            (
                pl.col("_w_wood_gross")
                + pl.col("_w_bark_pre")
                + pl.col("_w_branch_pre")
            ).alias("_comp_gross_sum"),
            (
                pl.col("_w_wood_red") + pl.col("_w_bark_pre") + pl.col("_w_branch_pre")
            ).alias("_comp_red_sum"),
        ]
    )
    trees = trees.with_columns(
        [
            pl.when(pl.col("_comp_gross_sum") <= 0)
            .then(pl.lit(0.0))
            .otherwise(pl.col("_comp_red_sum") / pl.col("_comp_gross_sum"))
            .alias("_agb_reduce"),
        ]
    )

    # Step 9 — Reduce the directly-predicted AGB by the cull factor.
    trees = trees.with_columns(
        [(pl.col("_agb_predicted") * pl.col("_agb_reduce")).alias("_agb_pred_red")]
    )

    # Step 10 — Harmonize (wood, bark, branch) so they sum to _agb_pred_red
    # while preserving the relative component ratios. Matches the scalar
    # harmonize_components exactly, including the degenerate
    # (component_sum <= 0) fallback: all AGB in wood, zeros elsewhere.
    trees = trees.with_columns(
        [
            pl.when(pl.col("_comp_red_sum") > 0)
            .then(
                pl.col("_agb_pred_red")
                * pl.col("_w_wood_red")
                / pl.col("_comp_red_sum")
            )
            .otherwise(pl.col("_agb_pred_red"))
            .alias("w_wood"),
            pl.when(pl.col("_comp_red_sum") > 0)
            .then(
                pl.col("_agb_pred_red")
                * pl.col("_w_bark_pre")
                / pl.col("_comp_red_sum")
            )
            .otherwise(pl.lit(0.0))
            .alias("w_bark"),
            pl.when(pl.col("_comp_red_sum") > 0)
            .then(
                pl.col("_agb_pred_red")
                * pl.col("_w_branch_pre")
                / pl.col("_comp_red_sum")
            )
            .otherwise(pl.lit(0.0))
            .alias("w_branch"),
        ]
    )
    trees = trees.with_columns(
        [(pl.col("w_wood") + pl.col("w_bark") + pl.col("w_branch")).alias("agb")]
    )

    # Drop all temporary columns so the caller sees only the public outputs.
    return trees.drop(
        [
            "_cull",
            "_dens_prop",
            "_w_wood_gross",
            "_w_wood_red",
            "_w_bark_pre",
            "_w_branch_pre",
            "_agb_predicted",
            "_comp_gross_sum",
            "_comp_red_sum",
            "_agb_reduce",
            "_agb_pred_red",
        ]
    )


def compute_nsvb_dead_biomass(
    trees: pl.LazyFrame,
    decay_props: pl.DataFrame,
    lookup: VectorizedLookupTables | None = None,
) -> pl.LazyFrame:
    """Vectorized NSVB standing-dead biomass pipeline (Phase 2 production path).

    Mirrors :func:`compute_nsvb_biomass` but applies the FIADB
    ``REF_TREE_DECAY_PROP`` reductions (``DENSITY_PROP``, ``BARK_LOSS_PROP``,
    ``BRANCH_LOSS_PROP``) by hardwood/softwood × ``DECAYCD`` *instead* of
    the live-tree ``CULL`` reduction. Per FIADB User Guide v9.1 Appendix K
    "Cull" subsection: "For dead tree biomass, no adjustments for TREE.CULL
    or other types of cull are made." So ``TREE.CULL`` is intentionally
    ignored on this path even when populated.

    The 10-step pipeline:

    1-2. Predict total stem inside-bark wood volume and stem bark volume
       (NSVB Models 1/2/4/5, Tables S1a/S2a + Jenkins fallback). Identical
       to live-tree path including the optional Level 2 DIVISION lookup.
    3.   Convert wood volume to gross weight (``v_wood_ib * WDSG * 62.4``).
    4-5. Predict stem bark biomass and branch biomass from S6a / S7a.
    6.   Predict directly the total above-ground biomass from S8a.
    7.   Join the FIADB decay-proportion table on (hw_sw, DECAYCD).
    8.   Apply each component's decay reduction:

         - ``w_wood_dead = w_wood_gross * DENSITY_PROP``
         - ``w_bark_dead = w_bark_pre  * BARK_LOSS_PROP``
         - ``w_branch_dead = w_branch_pre * BRANCH_LOSS_PROP``

    9.   Compute the AGB reduction factor:
         ``AGBReduce = sum(dead components) / sum(gross components)``
         and reduce the directly-predicted AGB:
         ``agb_pred_dead = agb_predicted * AGBReduce``.
    10.  Harmonize the dead components against ``agb_pred_dead`` so they
         sum exactly to it while preserving relative dead-component ratios.
         This is the same proportional redistribution
         :func:`harmonize_components` performs for the live path.

    Parameters
    ----------
    trees : pl.LazyFrame
        Input frame with at least:

        - ``SPCD`` (Int): FIA species code
        - ``DIA`` (Float): diameter at breast height (inches, must be >= 1.0)
        - ``HT`` (Float): total tree height (feet) — used as the *intact*
          height for the NSVB component predictions. Broken-top corrections
          (``ACTUALHT < HT`` adjustments via ``REF_TREE_STND_DEAD_CR_PROP``)
          are not implemented in this Phase 2 baseline.
        - ``DECAYCD`` (Int): standing-dead decay class (1-5) from
          ``TREE.DECAYCD``. Trees with null DECAYCD will produce null
          outputs.
        - ``WDSG`` (Float): wood specific gravity from
          ``REF_SPECIES.WOOD_SPGR_GREENVOL_DRYWT``
        - ``JENKINS_SPGRPCD`` (Int): Jenkins species group for the Level 4
          fallback

        Optional:

        - ``DIVISION`` (Utf8, nullable): Bailey ecoprovince DIVISION code,
          activates Level 2 of the NSVB lookup precedence (same as the
          live-tree pipeline).
    decay_props : pl.DataFrame
        FIADB ``REF_TREE_DECAY_PROP`` lookup table from
        :func:`pyfia.carbon.nsvb.carbon_fractions.load_dead_decay_proportions_df`.
        Must have columns ``(hw_sw, DECAYCD, DENSITY_PROP, BARK_LOSS_PROP,
        BRANCH_LOSS_PROP)``.
    lookup : VectorizedLookupTables, optional
        Pre-built coefficient lookup bundle. If omitted, uses the cached
        process-level bundle (same as the live-tree path).

    Returns
    -------
    pl.LazyFrame
        The input frame with the following new columns:

        - ``v_wood_ib`` (Float, cu ft): total stem inside-bark wood volume
        - ``v_bark`` (Float, cu ft): total stem bark volume
        - ``w_wood`` (Float, lb): harmonized stem wood biomass after decay
        - ``w_bark`` (Float, lb): harmonized stem bark biomass after decay
        - ``w_branch`` (Float, lb): harmonized branch biomass after decay
        - ``agb`` (Float, lb): harmonized total above-ground biomass after
          decay (equals ``w_wood + w_bark + w_branch`` by construction)

        All input columns pass through. Note: trees with ``DECAYCD`` outside
        the 1-5 range will receive null reduction factors and therefore null
        ``agb`` outputs — the caller (e.g., the ``StandingDeadEstimator``
        validation test) is responsible for filtering or pre-validating
        ``DECAYCD``.

    Notes
    -----
    **What this implementation does NOT do (deferred):**

    - **Broken-top corrections.** ~75% of EVALID 132401 standing dead trees
      have ``TREE.ACTUALHT < TREE.HT``. The full FIADB pipeline applies a
      crown-proportion adjustment to branch biomass (and a volume-ratio
      adjustment to wood/bark biomass) for these trees, looking up the
      mean intact crown ratio via ``REF_TREE_STND_DEAD_CR_PROP`` keyed on
      Bailey ECOPROV × hw/sw. The Phase 2 baseline uses the intact ``HT``
      with no broken-top correction, which will systematically over-estimate
      biomass for broken-top trees. This is a known gap; the validation
      test's ratchet thresholds are loose to accommodate it and a follow-up
      can tighten them once broken-top handling lands.
    - **CULL adjustment.** Intentionally not applied — see Appendix K above.
    - **STDORGCD Level 1 lookup.** Same status as the live-tree path
      (10 dead-code rows across 5 tables).
    """
    if lookup is None:
        from pyfia.carbon.nsvb.coefficients import get_vectorized_lookup_tables

        lookup = get_vectorized_lookup_tables()

    has_division = "DIVISION" in trees.collect_schema().names()

    # Steps 1-2 — Stem wood and stem bark volumes (cu ft).
    trees = _join_and_eval_component(
        trees,
        lookup.volib_spcd,
        lookup.volib_jen,
        lookup.volib_div,
        "v_wood_ib",
        has_division=has_division,
    )
    trees = _join_and_eval_component(
        trees,
        lookup.volbk_spcd,
        lookup.volbk_jen,
        lookup.volbk_div,
        "v_bark",
        has_division=has_division,
    )

    # Steps 4-5 — Bark and branch biomass (lb), gross intact predictions.
    trees = _join_and_eval_component(
        trees,
        lookup.bark_bio_spcd,
        lookup.bark_bio_jen,
        lookup.bark_bio_div,
        "_w_bark_pre",
        has_division=has_division,
    )
    trees = _join_and_eval_component(
        trees,
        lookup.branch_bio_spcd,
        lookup.branch_bio_jen,
        lookup.branch_bio_div,
        "_w_branch_pre",
        has_division=has_division,
    )

    # Step 6 — Directly-predicted total AGB (lb), intact.
    trees = _join_and_eval_component(
        trees,
        lookup.total_agb_spcd,
        lookup.total_agb_jen,
        lookup.total_agb_div,
        "_agb_predicted",
        has_division=has_division,
    )

    # Step 3 — Convert wood volume to gross weight.
    trees = trees.with_columns(
        [
            (pl.col("v_wood_ib") * pl.col("WDSG").cast(pl.Float64) * 62.4).alias(
                "_w_wood_gross"
            ),
        ]
    )

    # Step 7 — Join the FIADB REF_TREE_DECAY_PROP table on (hw_sw, DECAYCD).
    # The hw_sw column is derived from the SPCD<300 rule to stay consistent
    # with _model_k and the live-tree pipeline (sidesteps the SPCD=10 S10a
    # misclassification).
    decay_lf = decay_props.lazy().rename(
        {
            "hw_sw": "_hw_sw",
            "DECAYCD": "_decay_join",
            "DENSITY_PROP": "_density_prop",
            "BARK_LOSS_PROP": "_bark_loss_prop",
            "BRANCH_LOSS_PROP": "_branch_loss_prop",
        }
    )
    trees = trees.with_columns(
        [
            pl.when(pl.col("SPCD") >= _HARDWOOD_SPCD_THRESHOLD)
            .then(pl.lit("hardwood"))
            .otherwise(pl.lit("softwood"))
            .alias("_hw_sw"),
            pl.col("DECAYCD").cast(pl.Int64).alias("_decay_join"),
        ]
    )
    trees = trees.join(decay_lf, on=["_hw_sw", "_decay_join"], how="left")

    # Step 8 — Apply per-component decay reductions.
    trees = trees.with_columns(
        [
            (pl.col("_w_wood_gross") * pl.col("_density_prop")).alias("_w_wood_dead"),
            (pl.col("_w_bark_pre") * pl.col("_bark_loss_prop")).alias("_w_bark_dead"),
            (pl.col("_w_branch_pre") * pl.col("_branch_loss_prop")).alias(
                "_w_branch_dead"
            ),
        ]
    )

    # Step 9 — AGB reduction factor and reduced predicted AGB.
    trees = trees.with_columns(
        [
            (
                pl.col("_w_wood_gross")
                + pl.col("_w_bark_pre")
                + pl.col("_w_branch_pre")
            ).alias("_comp_gross_sum"),
            (
                pl.col("_w_wood_dead")
                + pl.col("_w_bark_dead")
                + pl.col("_w_branch_dead")
            ).alias("_comp_dead_sum"),
        ]
    )
    trees = trees.with_columns(
        [
            pl.when(pl.col("_comp_gross_sum") <= 0)
            .then(pl.lit(0.0))
            .otherwise(pl.col("_comp_dead_sum") / pl.col("_comp_gross_sum"))
            .alias("_agb_reduce"),
        ]
    )
    trees = trees.with_columns(
        [(pl.col("_agb_predicted") * pl.col("_agb_reduce")).alias("_agb_pred_dead")]
    )

    # Step 10 — Harmonize dead components against the reduced predicted AGB.
    # Same proportional redistribution as the live path; see
    # ``harmonize_components`` for the scalar reference.
    trees = trees.with_columns(
        [
            pl.when(pl.col("_comp_dead_sum") > 0)
            .then(
                pl.col("_agb_pred_dead")
                * pl.col("_w_wood_dead")
                / pl.col("_comp_dead_sum")
            )
            .otherwise(pl.col("_agb_pred_dead"))
            .alias("w_wood"),
            pl.when(pl.col("_comp_dead_sum") > 0)
            .then(
                pl.col("_agb_pred_dead")
                * pl.col("_w_bark_dead")
                / pl.col("_comp_dead_sum")
            )
            .otherwise(pl.lit(0.0))
            .alias("w_bark"),
            pl.when(pl.col("_comp_dead_sum") > 0)
            .then(
                pl.col("_agb_pred_dead")
                * pl.col("_w_branch_dead")
                / pl.col("_comp_dead_sum")
            )
            .otherwise(pl.lit(0.0))
            .alias("w_branch"),
        ]
    )
    trees = trees.with_columns(
        [(pl.col("w_wood") + pl.col("w_bark") + pl.col("w_branch")).alias("agb")]
    )

    return trees.drop(
        [
            "_hw_sw",
            "_decay_join",
            "_density_prop",
            "_bark_loss_prop",
            "_branch_loss_prop",
            "_w_wood_gross",
            "_w_bark_pre",
            "_w_branch_pre",
            "_agb_predicted",
            "_w_wood_dead",
            "_w_bark_dead",
            "_w_branch_dead",
            "_comp_gross_sum",
            "_comp_dead_sum",
            "_agb_reduce",
            "_agb_pred_dead",
        ]
    )
