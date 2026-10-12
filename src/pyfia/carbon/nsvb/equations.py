"""
NSVB allometric equation forms (Westfall et al. 2023, GTR-WO-104).

Pure-math equation library for the National Scale Volume and Biomass framework.
No I/O, no Polars dependency — just floats in and floats out. Coefficient lookup
lives in coefficients.py; carbon fractions live in carbon_fractions.py.

The five model forms used for biomass components (GTR-WO-104 eqs. 1-5) are:

- **Model 1 (Schumacher-Hall)**: ``y = a * D^b * H^c`` — power form, the most common.
- **Model 2 (Segmented)**: ``y = a * D^b * H^c`` for ``D < k``, else
  ``y = a * k^(b-b1) * D^b1 * H^c`` — with ``k = 9`` for softwoods (SPCD < 300)
  and ``k = 11`` for hardwoods (SPCD >= 300). The two branches are continuous at
  ``D = k``. Verified against the Douglas-fir (SPCD=202) wood-volume worked example
  and back-solved against the red maple (SPCD=316) bark-volume worked example.
- **Model 3 (Continuously Variable)**: ``y = a * D^(a1*(1-exp(-b*D))^c1) * H^c`` —
  the diameter exponent varies continuously with D. Used for a few species/
  components (e.g. planted slash pine SPCD=111 total AGB, oak spp. SPCD=800 stem
  wood volume). Transcribed from GTR-WO-104 eq. 3 (source PDF p. 7).
- **Model 4 (Modified Wiley)**: ``y = a * D^b * H^c * exp(-b1 * D)`` — power form
  modulated by an exponential of D. Verified against the red maple S8a worked example.
- **Model 5 (Jenkins fallback)**: ``y = a * D^b * H^c * WDSG`` — used when a
  species lacks SPCD-specific coefficients and falls back to its Jenkins species
  group. Wood density (WDSG) comes from FIADB ``REF_SPECIES.WOOD_SPGR_GREENVOL_DRYWT``.

**Model 6 is not implemented** — it is the iterative Schumacher-Hall volume-ratio
model used for merchantable subdivision (DRYBIO_BOLE / DRYBIO_TOP / DRYBIO_STUMP)
and broken-top reductions, and never appears in the five biomass-component tables;
total above-ground biomass and total carbon do not require it. A coefficient row
dispatching to any unimplemented model is rejected up front by the carbon
estimator's coverage guard rather than producing a silent null.

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

from typing import TYPE_CHECKING

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


# DECAYCD=3 wood density proportions used to discount cull wood weight, per
# worked example line 550 and Harmon et al. (2011) Table 1. Cull wood is
# typically partially rotten, so its weight is reduced (but not removed entirely).
_CULL_DENS_PROP = {"hardwood": 0.54, "softwood": 0.92}


# ---------------------------------------------------------------------------
# Vectorized NSVB pipeline — the production data path.
# ---------------------------------------------------------------------------
#
# These functions evaluate the NSVB models as polars expressions over a
# LazyFrame, enabling FIA-scale (1M+ trees) evaluation via coefficient-table
# joins instead of per-tree Python dispatch. The independent scalar reference
# implementation used to cross-check them in the test suite lives in
# ``tests/nsvb_oracle.py`` (issue #127); see ``pyfia/carbon/__init__.py``
# "Architectural rules" rule 2.


def nsvb_biomass_expr(
    *,
    model: pl.Expr,
    a: pl.Expr,
    a1: pl.Expr,
    b: pl.Expr,
    b1: pl.Expr,
    c: pl.Expr,
    c1: pl.Expr,
    d: pl.Expr,
    h: pl.Expr,
    spcd: pl.Expr,
    wdsg: pl.Expr,
) -> pl.Expr:
    """Build a polars expression that dispatches NSVB Models 1/2/3/4/5.

    The scalar equivalent of this function is :func:`_eval_component`.
    It returns a single expression suitable for use inside
    ``LazyFrame.with_columns`` that produces the predicted component value
    (volume or biomass, in source-table units) for every row, with the
    model form selected per-row from the ``model`` column.

    Models 1-5 are implemented (see :func:`model_1`-:func:`model_5_jenkins`).
    Model 6 (the volume-ratio model) is not used for biomass components — it
    predicts merchantable volume / broken-top reductions — and never appears
    in the five component tables consumed here; a row dispatching to any
    unhandled model returns ``None``, which the coverage guard
    (:meth:`CarbonEstimatorBase._guard_nsvb_coverage`) and the post-pipeline
    non-null assertion turn into a loud failure rather than a silent 0.

    The Model 2 segmentation point ``k`` is ``11`` for hardwoods (SPCD >= 300)
    and ``9`` for softwoods, matching :func:`_model_k` used by the scalar
    path. This also resolves the SPCD=10 misclassification that S10a
    carries — see ``pyfia/carbon/__init__.py`` for the architectural
    discussion.

    Parameters
    ----------
    model, a, a1, b, b1, c, c1 : pl.Expr
        Coefficient column expressions (typically the output of a coalesce
        across the stand-origin / division / species-level / Jenkins lookup
        joins, or ``pl.col(...)`` references to pre-joined coefficient
        columns). All numeric coefficient columns must be ``Float64``;
        ``model`` must be an integer dtype. ``a1``/``c1`` are used only by
        Model 3; ``b1`` by Models 2 and 4.
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
    # Model 2 is segmented at k: Schumacher-Hall below, b1-exponent above.
    model_2_expr = (
        pl.when(d < k)
        .then(a * d.pow(b) * h.pow(c))
        .otherwise(a * k.pow(b - b1) * d.pow(b1) * h.pow(c))
    )
    # Model 3 (Continuously Variable): D exponent = a1 * (1 - exp(-b*D))^c1.
    model_3_expr = a * d.pow(a1 * (1.0 - (-b * d).exp()).pow(c1)) * h.pow(c)
    return (
        pl.when(model == 1)
        .then(a * d.pow(b) * h.pow(c))
        .when(model == 2)
        .then(model_2_expr)
        .when(model == 3)
        .then(model_3_expr)
        .when(model == 4)
        .then(a * d.pow(b) * h.pow(c) * (-b1 * d).exp())
        .when(model == 5)
        .then(a * d.pow(b) * h.pow(c) * wdsg)
        .otherwise(None)
    )


# Coefficient columns carried through every lookup tier and coalesced per-row.
# Must match the inputs consumed by ``nsvb_biomass_expr`` (a1/c1 added for Model 3).
_COEF_COLS = ("model", "a", "a1", "b", "b1", "c", "c1")


def _join_and_eval_component(
    trees: pl.LazyFrame,
    spcd_table: pl.DataFrame,
    jen_table: pl.DataFrame,
    div_table: pl.DataFrame,
    out_col: str,
    *,
    has_division: bool,
    divorg_table: pl.DataFrame | None = None,
    org_table: pl.DataFrame | None = None,
    has_stdorgcd: bool = False,
) -> pl.LazyFrame:
    """Join a LazyFrame to the NSVB coefficient tiers for one component and
    evaluate the biomass expression.

    Implements the full NSVB lookup precedence (GTR-WO-104 p. 11) as a chain
    of left joins followed by a per-column coalesce that picks the most
    specific matching tier for every tree:

    1. ``(SPCD, DIVISION, STDORGCD)`` — Level 1, ``divorg_table`` (needs both
       a ``DIVISION`` and a ``STDORGCD`` column on the trees frame).
    2. ``(SPCD, STDORGCD)`` — Level 1b, ``org_table``, the stand-origin
       "no ecodivision noted" row (needs ``STDORGCD``). Slash/loblolly pine
       (SPCD 111/131) are the only species fit separately by stand origin and
       have *no* STDORGCD-null row, so Levels 1/1b are what keeps them off the
       Jenkins fallback (issue #123).
    3. ``(SPCD, DIVISION)`` — Level 2, ``div_table`` (needs ``DIVISION``).
    4. ``(SPCD)`` — Level 3, ``spcd_table`` (species-level, always joined).
    5. ``(JENKINS_SPGRPCD)`` — Level 4, ``jen_table`` (always joined).

    Each tier that matches a row populates *all* of its coefficient columns
    together, so the per-column coalesce is self-consistent: the model form
    and its coefficients always come from the same winning tier. The lookup
    builders drop rows whose model's required coefficients are null (Model
    2/4 ``b1``, Model 3 ``a1``/``c1``), so a coalesced coefficient is never
    silently back-filled from a lower tier for a coefficient the model uses.

    When ``has_stdorgcd`` / ``has_division`` are False (synthetic tests, or
    callers that haven't wired the COND / PLOTGEOM joins), the corresponding
    tiers are skipped and behavior degrades gracefully to the remaining
    levels — with neither, it matches the original species-level + Jenkins
    2-way coalesce exactly.

    Parameters
    ----------
    trees : pl.LazyFrame
        Input frame with at least ``SPCD``, ``DIA``, ``HT``, ``WDSG``,
        ``JENKINS_SPGRPCD``. With ``has_division`` a ``DIVISION`` column
        (Utf8, nullable); with ``has_stdorgcd`` a ``STDORGCD`` column (Int64).
    spcd_table, jen_table, div_table : pl.DataFrame
        Level 3 / Level 4 / Level 2 lookups from the ``build_*`` helpers in
        :mod:`pyfia.carbon.nsvb.coefficients`, each with the ``_COEF_COLS``.
    out_col : str
        Name for the output column (e.g., ``"v_wood_ib"``).
    has_division : bool
        Whether the trees frame carries a populated ``DIVISION`` column.
    divorg_table, org_table : pl.DataFrame, optional
        Level 1 / Level 1b stand-origin lookups from
        :func:`build_division_stdorg_lookup` / :func:`build_stdorg_lookup`.
    has_stdorgcd : bool
        Whether the trees frame carries a populated ``STDORGCD`` column.

    Returns
    -------
    pl.LazyFrame
        The input frame with ``out_col`` appended and temporary coefficient
        columns dropped.
    """

    def _rename(table: pl.DataFrame, suffix: str) -> pl.LazyFrame:
        return table.lazy().rename({col: f"_{col}_{suffix}" for col in _COEF_COLS})

    # Join each available tier, recording its suffix in precedence order.
    tiers: list[str] = []
    if has_stdorgcd and has_division and divorg_table is not None:
        trees = trees.join(
            _rename(divorg_table, "do"), on=["SPCD", "DIVISION", "STDORGCD"], how="left"
        )
        tiers.append("do")
    if has_stdorgcd and org_table is not None:
        trees = trees.join(_rename(org_table, "o"), on=["SPCD", "STDORGCD"], how="left")
        tiers.append("o")
    if has_division:
        trees = trees.join(_rename(div_table, "d"), on=["SPCD", "DIVISION"], how="left")
        tiers.append("d")
    trees = trees.join(_rename(spcd_table, "s"), on="SPCD", how="left")
    tiers.append("s")
    trees = trees.join(_rename(jen_table, "j"), on="JENKINS_SPGRPCD", how="left")
    tiers.append("j")

    def _coalesce(col: str) -> pl.Expr:
        return pl.coalesce([pl.col(f"_{col}_{suf}") for suf in tiers])

    trees = trees.with_columns(
        nsvb_biomass_expr(
            model=_coalesce("model"),
            a=_coalesce("a"),
            a1=_coalesce("a1"),
            b=_coalesce("b"),
            b1=_coalesce("b1"),
            c=_coalesce("c"),
            c1=_coalesce("c1"),
            d=pl.col("DIA").cast(pl.Float64),
            h=pl.col("HT").cast(pl.Float64),
            spcd=pl.col("SPCD"),
            wdsg=pl.col("WDSG").cast(pl.Float64),
        ).alias(out_col)
    )

    drop_cols = [f"_{col}_{suf}" for suf in tiers for col in _COEF_COLS]
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
    # it. DIVISION activates the Level 2 lookup path; STDORGCD activates the
    # Level 1/1b stand-origin path (issue #123).
    schema_names = trees.collect_schema().names()
    has_division = "DIVISION" in schema_names
    has_stdorgcd = "STDORGCD" in schema_names

    # Step 1 — Total stem inside-bark wood volume (cubic feet).
    trees = _join_and_eval_component(
        trees,
        lookup.volib_spcd,
        lookup.volib_jen,
        lookup.volib_div,
        "v_wood_ib",
        has_division=has_division,
        divorg_table=lookup.volib_divorg,
        org_table=lookup.volib_org,
        has_stdorgcd=has_stdorgcd,
    )

    # Step 2 — Total stem bark volume (cubic feet).
    trees = _join_and_eval_component(
        trees,
        lookup.volbk_spcd,
        lookup.volbk_jen,
        lookup.volbk_div,
        "v_bark",
        has_division=has_division,
        divorg_table=lookup.volbk_divorg,
        org_table=lookup.volbk_org,
        has_stdorgcd=has_stdorgcd,
    )

    # Step 5 — Stem bark biomass (lb).
    trees = _join_and_eval_component(
        trees,
        lookup.bark_bio_spcd,
        lookup.bark_bio_jen,
        lookup.bark_bio_div,
        "_w_bark_pre",
        has_division=has_division,
        divorg_table=lookup.bark_bio_divorg,
        org_table=lookup.bark_bio_org,
        has_stdorgcd=has_stdorgcd,
    )

    # Step 6 — Branch biomass (lb).
    trees = _join_and_eval_component(
        trees,
        lookup.branch_bio_spcd,
        lookup.branch_bio_jen,
        lookup.branch_bio_div,
        "_w_branch_pre",
        has_division=has_division,
        divorg_table=lookup.branch_bio_divorg,
        org_table=lookup.branch_bio_org,
        has_stdorgcd=has_stdorgcd,
    )

    # Step 7 — Directly-predicted total AGB (lb).
    trees = _join_and_eval_component(
        trees,
        lookup.total_agb_spcd,
        lookup.total_agb_jen,
        lookup.total_agb_div,
        "_agb_predicted",
        has_division=has_division,
        divorg_table=lookup.total_agb_divorg,
        org_table=lookup.total_agb_org,
        has_stdorgcd=has_stdorgcd,
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
    cr_prop_table: pl.DataFrame | None = None,
) -> pl.LazyFrame:
    """Vectorized NSVB standing-dead biomass pipeline.

    Mirrors :func:`compute_nsvb_biomass` but applies the FIADB
    ``REF_TREE_DECAY_PROP`` reductions (``DENSITY_PROP``, ``BARK_LOSS_PROP``,
    ``BRANCH_LOSS_PROP``) by hardwood/softwood × ``DECAYCD`` *instead* of
    the live-tree ``CULL`` reduction. Per FIADB User Guide v9.1 Appendix K
    "Cull" subsection: "For dead tree biomass, no adjustments for TREE.CULL
    or other types of cull are made." So ``TREE.CULL`` is intentionally
    ignored on this path even when populated.

    **Broken-top corrections** (Phase 2.5): when the trees frame has an
    ``ACTUALHT`` column and ``cr_prop_table`` is provided, trees with
    ``ACTUALHT < HT`` receive two adjustments before the decay reductions:

    - *Branch biomass* is multiplied by ``Broken_crn_prop``, the fraction
      of the intact crown remaining below the break, computed from the
      mean intact crown ratio in ``REF_TREE_STND_DEAD_CR_PROP`` (Table S11)
      keyed on Bailey ecoregion province × hardwood/softwood.
    - *Wood and bark* are reduced by ``(ACTUALHT / HT) ** (2/3)``, a
      paraboloid taper approximation of the stem volume ratio below the
      break (a stem tapers, so volume falls faster than height near the tip
      but slower than a cone). This replaces the Model 6 (Schumacher-Hall)
      volume-ratio computation that FIADB uses for sub-stem partitioning,
      which is not implemented.

    Parameters
    ----------
    trees : pl.LazyFrame
        Input frame with at least: ``SPCD``, ``DIA``, ``HT``, ``DECAYCD``,
        ``WDSG``, ``JENKINS_SPGRPCD``.

        Optional:

        - ``DIVISION`` (Utf8, nullable): activates Level 2 NSVB lookup.
        - ``ACTUALHT`` (Float, nullable): actual measured height for
          broken-top trees. When present and ``cr_prop_table`` is not None,
          trees with ``ACTUALHT < HT`` receive broken-top corrections.
        - ``ECOSUBCD`` (Utf8, nullable): used to derive Bailey ecoregion
          province for the ``REF_TREE_STND_DEAD_CR_PROP`` lookup. When
          absent, all trees fall back to the UNDEFINED mean crown ratio.
    decay_props : pl.DataFrame
        FIADB ``REF_TREE_DECAY_PROP`` lookup with columns ``(hw_sw,
        DECAYCD, DENSITY_PROP, BARK_LOSS_PROP, BRANCH_LOSS_PROP)``.
    lookup : VectorizedLookupTables, optional
        Pre-built coefficient lookup bundle. If omitted, uses the cached
        process-level bundle.
    cr_prop_table : pl.DataFrame, optional
        Table S11 mean crown ratios from
        :func:`pyfia.carbon.nsvb.carbon_fractions.load_dead_cr_prop_df`.
        Columns ``(ECOPROV, hw_sw, CR_MEAN)``. When None, broken-top
        corrections are skipped (Phase 2 baseline behavior).

    Returns
    -------
    pl.LazyFrame
        The input frame with new columns: ``v_wood_ib``, ``v_bark``,
        ``w_wood``, ``w_bark``, ``w_branch``, ``agb``. For broken-top
        trees, volumes and biomass reflect the remaining stem/crown below
        the break point.
    """
    if lookup is None:
        from pyfia.carbon.nsvb.coefficients import get_vectorized_lookup_tables

        lookup = get_vectorized_lookup_tables()

    schema_names_in = trees.collect_schema().names()
    has_division = "DIVISION" in schema_names_in
    has_stdorgcd = "STDORGCD" in schema_names_in

    # Steps 1-2 — Stem wood and stem bark volumes (cu ft).
    trees = _join_and_eval_component(
        trees,
        lookup.volib_spcd,
        lookup.volib_jen,
        lookup.volib_div,
        "v_wood_ib",
        has_division=has_division,
        divorg_table=lookup.volib_divorg,
        org_table=lookup.volib_org,
        has_stdorgcd=has_stdorgcd,
    )
    trees = _join_and_eval_component(
        trees,
        lookup.volbk_spcd,
        lookup.volbk_jen,
        lookup.volbk_div,
        "v_bark",
        has_division=has_division,
        divorg_table=lookup.volbk_divorg,
        org_table=lookup.volbk_org,
        has_stdorgcd=has_stdorgcd,
    )

    # Steps 4-5 — Bark and branch biomass (lb), gross intact predictions.
    trees = _join_and_eval_component(
        trees,
        lookup.bark_bio_spcd,
        lookup.bark_bio_jen,
        lookup.bark_bio_div,
        "_w_bark_pre",
        has_division=has_division,
        divorg_table=lookup.bark_bio_divorg,
        org_table=lookup.bark_bio_org,
        has_stdorgcd=has_stdorgcd,
    )
    trees = _join_and_eval_component(
        trees,
        lookup.branch_bio_spcd,
        lookup.branch_bio_jen,
        lookup.branch_bio_div,
        "_w_branch_pre",
        has_division=has_division,
        divorg_table=lookup.branch_bio_divorg,
        org_table=lookup.branch_bio_org,
        has_stdorgcd=has_stdorgcd,
    )

    # Step 6 — Directly-predicted total AGB (lb), intact.
    trees = _join_and_eval_component(
        trees,
        lookup.total_agb_spcd,
        lookup.total_agb_jen,
        lookup.total_agb_div,
        "_agb_predicted",
        has_division=has_division,
        divorg_table=lookup.total_agb_divorg,
        org_table=lookup.total_agb_org,
        has_stdorgcd=has_stdorgcd,
    )

    # Step 3 — Convert wood volume to gross weight.
    trees = trees.with_columns(
        [
            (pl.col("v_wood_ib") * pl.col("WDSG").cast(pl.Float64) * 62.4).alias(
                "_w_wood_gross"
            ),
        ]
    )

    # ----- Broken-top corrections (Phase 2.5) -----
    # For trees with ACTUALHT < HT, reduce branch biomass by the crown
    # proportion remaining below the break, and reduce wood/bark by the
    # paraboloid volume ratio (ACTUALHT/HT)^(2/3).  Corrections applied BEFORE the
    # decay reductions so both adjustments compound.  The intact gross sum
    # is saved BEFORE the correction so AGBReduce = broken_decayed / intact
    # captures both the broken-top and decay reductions against the intact
    # AGB prediction.
    schema_names = trees.collect_schema().names()
    has_actualht = "ACTUALHT" in schema_names
    _has_broken_top_correction = False
    if has_actualht and cr_prop_table is not None:
        has_ecosubcd = "ECOSUBCD" in schema_names

        # Derive Bailey ecoregion province from ECOSUBCD.  Province is the
        # 3-digit numeric prefix (with optional M) — NOT the same as
        # DIVISION, which replaces the last digit with 0.
        if has_ecosubcd:
            ecoprov_expr = (
                pl.when(pl.col("ECOSUBCD").is_null() | (pl.col("ECOSUBCD") == ""))
                .then(pl.lit("UNDEFINED"))
                .when(pl.col("ECOSUBCD").str.to_uppercase().str.starts_with("M"))
                .then(
                    pl.lit("M") + pl.col("ECOSUBCD").str.to_uppercase().str.slice(1, 3)
                )
                .otherwise(pl.col("ECOSUBCD").str.to_uppercase().str.slice(0, 3))
            )
        else:
            ecoprov_expr = pl.lit("UNDEFINED")

        hw_sw_expr = (
            pl.when(pl.col("SPCD") >= _HARDWOOD_SPCD_THRESHOLD)
            .then(pl.lit("hardwood"))
            .otherwise(pl.lit("softwood"))
        )

        _has_broken_top_correction = True

        # Save intact gross component sum BEFORE applying corrections.
        # AGBReduce will use this as the denominator so that it captures
        # both the broken-top and decay reductions against the intact
        # AGB prediction from S8a/S8b.
        trees = trees.with_columns(
            (
                pl.col("_w_wood_gross")
                + pl.col("_w_bark_pre")
                + pl.col("_w_branch_pre")
            ).alias("_comp_gross_sum_intact")
        )

        trees = trees.with_columns(
            [
                ecoprov_expr.alias("_ecoprov"),
                hw_sw_expr.alias("_bt_hw_sw"),
            ]
        )

        # Join CR_MEAN from Table S11 on (ECOPROV, hw_sw).
        cr_lf = cr_prop_table.lazy().rename(
            {"ECOPROV": "_ecoprov", "hw_sw": "_bt_hw_sw", "CR_MEAN": "_cr_mean"}
        )
        trees = trees.join(cr_lf, on=["_ecoprov", "_bt_hw_sw"], how="left")

        # Fall back to UNDEFINED CR_MEAN for unmatched provinces.
        undef = cr_prop_table.filter(pl.col("ECOPROV") == "UNDEFINED")
        undef_sw = float(undef.filter(pl.col("hw_sw") == "softwood")["CR_MEAN"][0])
        undef_hw = float(undef.filter(pl.col("hw_sw") == "hardwood")["CR_MEAN"][0])
        trees = trees.with_columns(
            pl.col("_cr_mean")
            .fill_null(
                pl.when(pl.col("SPCD") >= _HARDWOOD_SPCD_THRESHOLD)
                .then(pl.lit(undef_hw))
                .otherwise(pl.lit(undef_sw))
            )
            .alias("_cr_mean")
        )

        # Identify broken-top trees: ACTUALHT < HT and ACTUALHT not null.
        actualht = pl.col("ACTUALHT").cast(pl.Float64, strict=False)
        ht = pl.col("HT").cast(pl.Float64)
        is_broken = actualht.is_not_null() & (actualht < ht) & (actualht > 0)

        # Appendix K crown-proportion formula:
        #   CRprop_HT = (HT - ACTUALHT * (1 - CR/100)) / HT
        #   Broken_crn_prop = max(0, (ACTUALHT - (1-CRprop_HT)*HT) / (CRprop_HT*HT))
        cr_frac = pl.col("_cr_mean") / 100.0
        crprop_ht = (ht - actualht * (1.0 - cr_frac)) / ht

        # Guard against zero/negative CRprop_HT (degenerate cases where
        # ACTUALHT ≈ 0 or CR_MEAN ≈ 100).
        broken_crn = ((actualht - (1.0 - crprop_ht) * ht) / (crprop_ht * ht)).clip(
            lower_bound=0.0
        )

        # Volume ratio: paraboloid taper approximation of stem volume below
        # break.  Real stems have a shape between a paraboloid (exponent 2/3)
        # and a cone (exponent 1).  The 2/3 exponent captures the fact that
        # the wider lower stem contains a disproportionately large fraction of
        # total stem volume, preventing the over-correction a linear ratio
        # produces.  This replaces the Schumacher-Hall Model 6 volume-ratio
        # calculation that FIADB uses for sub-stem partitioning.
        vol_ratio = (actualht / ht).pow(2.0 / 3.0)

        trees = trees.with_columns(
            [
                pl.when(is_broken).then(broken_crn).otherwise(1.0).alias("_crn_prop"),
                pl.when(is_broken).then(vol_ratio).otherwise(1.0).alias("_vol_ratio"),
            ]
        )

        # Apply broken-top reductions to gross components.
        trees = trees.with_columns(
            [
                (pl.col("_w_wood_gross") * pl.col("_vol_ratio")).alias("_w_wood_gross"),
                (pl.col("_w_bark_pre") * pl.col("_vol_ratio")).alias("_w_bark_pre"),
                (pl.col("_w_branch_pre") * pl.col("_crn_prop")).alias("_w_branch_pre"),
                (pl.col("v_wood_ib") * pl.col("_vol_ratio")).alias("v_wood_ib"),
                (pl.col("v_bark") * pl.col("_vol_ratio")).alias("v_bark"),
            ]
        )

        trees = trees.drop(
            ["_ecoprov", "_bt_hw_sw", "_cr_mean", "_crn_prop", "_vol_ratio"]
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
    # When broken-top corrections were applied, the denominator must use
    # the intact gross sum (saved before the corrections) so that
    # AGBReduce = broken_decayed / intact captures both the broken-top
    # reduction and the decay reduction against the intact AGB prediction.
    trees = trees.with_columns(
        [
            (
                pl.col("_w_wood_dead")
                + pl.col("_w_bark_dead")
                + pl.col("_w_branch_dead")
            ).alias("_comp_dead_sum"),
        ]
    )
    if _has_broken_top_correction:
        gross_denom = pl.col("_comp_gross_sum_intact")
    else:
        trees = trees.with_columns(
            (
                pl.col("_w_wood_gross")
                + pl.col("_w_bark_pre")
                + pl.col("_w_branch_pre")
            ).alias("_comp_gross_sum"),
        )
        gross_denom = pl.col("_comp_gross_sum")
    trees = trees.with_columns(
        pl.when(gross_denom <= 0)
        .then(pl.lit(0.0))
        .otherwise(pl.col("_comp_dead_sum") / gross_denom)
        .alias("_agb_reduce"),
    )
    trees = trees.with_columns(
        (pl.col("_agb_predicted") * pl.col("_agb_reduce")).alias("_agb_pred_dead")
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

    drop_cols = [
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
        "_comp_dead_sum",
        "_agb_reduce",
        "_agb_pred_dead",
    ]
    if _has_broken_top_correction:
        drop_cols.append("_comp_gross_sum_intact")
    else:
        drop_cols.append("_comp_gross_sum")
    return trees.drop(drop_cols)
