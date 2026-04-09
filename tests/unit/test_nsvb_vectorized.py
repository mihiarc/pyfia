"""
Equivalence tests for the vectorized NSVB biomass pipeline.

The scalar reference pipeline (``predict_tree_biomass`` + ``lookup_coefficients``)
is locked against the GTR-WO-104 worked examples in ``test_nsvb_equations.py``.
The vectorized pipeline in ``pyfia.carbon.nsvb.equations.compute_nsvb_biomass``
must produce identical (f64-tight) outputs for every tree, because it is
intentionally a polars-expression rewrite of the same math.

This module is the cross-check: for a diverse synthetic tree dataset covering
Models 1, 2, and 4, hardwoods and softwoods, SPCDs that hit the species-level
lookup and SPCDs that fall through to the Jenkins fallback, and trees with
and without cull, each tree's vectorized output must equal its scalar oracle
output at ``rel_tol=1e-9`` (six columns: ``v_wood_ib``, ``v_bark``,
``w_wood``, ``w_bark``, ``w_branch``, ``agb``).

Any divergence breaks the test with exact per-tree diagnostics, which is the
fastest way to track down a bug introduced into the vectorized pipeline —
finer-grained than comparing aggregated per-acre estimates.

See ``pyfia/carbon/__init__.py`` "Architectural rules" rule 2 for why the
scalar path stays around as an oracle even though the vectorized path is
the production data path.
"""

from __future__ import annotations

import math
import random

import polars as pl
import pytest

from pyfia.carbon.nsvb.coefficients import (
    load_nsvb_coefficients,
    lookup_coefficients,
)
from pyfia.carbon.nsvb.equations import (
    Coefficients,
    compute_nsvb_biomass,
    predict_tree_biomass,
)

# ---------------------------------------------------------------------------
# Synthetic tree generator
# ---------------------------------------------------------------------------

# SPCDs with species-level rows in ALL 5 coefficient tables. Picked to span
# the Model 1, 2, 4 dispatches and both hardwood/softwood branches.
#
# (SPCD, WDSG_proxy, JENKINS_SPGRPCD_proxy, hw_sw):
#   SPCD=110 shortleaf pine     (softwood, Model 1/2 for volib)
#   SPCD=202 Douglas-fir        (softwood, Model 2 for volib — worked example 1)
#   SPCD=121 longleaf pine      (softwood, species-level)
#   SPCD=131 loblolly pine      (softwood, species-level)
#   SPCD=316 red maple          (hardwood, Model 1 volib, Model 4 total_agb — worked example 2)
#   SPCD=802 white oak          (hardwood, species-level)
#   SPCD=833 chestnut oak       (hardwood, species-level)
#   SPCD=621 yellow-poplar      (hardwood, species-level)
#
# The Jenkins fallback is exercised by injecting a few trees with an SPCD that
# isn't in the species-level tables (e.g., 9999 → falls through to Jenkins).
_SPECIES_LEVEL_SPCDS: list[tuple[int, float, int]] = [
    # (SPCD, WDSG, JENKINS_SPGRPCD) — WDSG and JENKINS are arbitrary but must
    # be consistent between the vectorized and scalar runs.
    (110, 0.46, 4),  # shortleaf pine
    (202, 0.45, 3),  # Douglas-fir
    (121, 0.54, 4),  # longleaf pine
    (131, 0.47, 4),  # loblolly pine
    (316, 0.49, 8),  # red maple
    (802, 0.60, 6),  # white oak
    (833, 0.57, 6),  # chestnut oak
    (621, 0.40, 8),  # yellow-poplar
]

# At least one Jenkins-fallback SPCD so the (Level 4) path is exercised.
# 9999 has no row in any *_spcd table, so the lookup cascades to the
# Jenkins group provided. Group 3 is "soft maple/birch" — arbitrary.
_JENKINS_FALLBACK_SPCDS: list[tuple[int, float, int]] = [
    (9999, 0.50, 3),
]


def _build_synthetic_trees(n: int, seed: int = 42) -> pl.DataFrame:
    """Build a synthetic tree DataFrame for equivalence testing.

    Each tree gets random DIA (1.0-30.0 in), HT (5-130 ft), CULL (0 or 0-20),
    sampled uniformly, with SPCD drawn from ``_SPECIES_LEVEL_SPCDS`` plus a
    small fraction of Jenkins-fallback SPCDs. WDSG and JENKINS_SPGRPCD are
    carried along unchanged so both the vectorized and scalar evaluators
    see identical inputs.

    Also carries a pre-computed ``hw_sw`` column derived from SPCD<300 —
    this matches the rule used by both ``_model_k`` and the vectorized
    orchestrator, keeping the scalar oracle call consistent with the
    vectorized path.
    """
    rng = random.Random(seed)
    all_species = _SPECIES_LEVEL_SPCDS + _JENKINS_FALLBACK_SPCDS

    rows: list[dict] = []
    for _ in range(n):
        spcd, wdsg, jen = rng.choice(all_species)
        dia = rng.uniform(1.0, 30.0)
        ht = rng.uniform(5.0, 130.0)
        # 30% of trees get nonzero cull (0-20%), 10% get NULL cull, rest 0.
        r = rng.random()
        if r < 0.1:
            cull = None  # test null handling
        elif r < 0.4:
            cull = rng.uniform(0.0, 20.0)
        else:
            cull = 0.0
        rows.append(
            {
                "SPCD": spcd,
                "DIA": dia,
                "HT": ht,
                "CULL": cull,
                "WDSG": wdsg,
                "JENKINS_SPGRPCD": jen,
            }
        )

    return pl.DataFrame(
        rows,
        schema={
            "SPCD": pl.Int64,
            "DIA": pl.Float64,
            "HT": pl.Float64,
            "CULL": pl.Float64,
            "WDSG": pl.Float64,
            "JENKINS_SPGRPCD": pl.Int64,
        },
    )


# ---------------------------------------------------------------------------
# Scalar oracle helpers
# ---------------------------------------------------------------------------


def _bundle_via_csv(spcd: int, jenkins_spgrpcd: int) -> Coefficients:
    """Build a scalar ``Coefficients`` bundle by walking the real CSV lookup.

    Mirrors the helper of the same name in ``test_nsvb_equations.py``. Duplicated
    here so this file is self-contained and doesn't depend on test-module import
    order.
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


def _scalar_per_tree(trees: pl.DataFrame) -> pl.DataFrame:
    """Run the scalar oracle pipeline tree-by-tree and return a DataFrame.

    Matches the vectorized output schema: ``v_wood_ib``, ``v_bark``,
    ``w_wood``, ``w_bark``, ``w_branch``, ``agb``.
    """
    out = []
    for row in trees.iter_rows(named=True):
        bundle = _bundle_via_csv(row["SPCD"], row["JENKINS_SPGRPCD"])
        hw_sw = "hardwood" if row["SPCD"] >= 300 else "softwood"
        cull = row["CULL"] if row["CULL"] is not None else 0.0
        result = predict_tree_biomass(
            spcd=row["SPCD"],
            dia=row["DIA"],
            ht=row["HT"],
            coefficients=bundle,
            wdsg=row["WDSG"],
            hw_sw=hw_sw,
            cull=cull,
        )
        out.append(
            {
                "SPCD": row["SPCD"],
                "v_wood_ib": result.v_wood_ib,
                "v_bark": result.v_bark,
                "w_wood": result.w_wood,
                "w_bark": result.w_bark,
                "w_branch": result.w_branch,
                "agb": result.agb,
            }
        )
    return pl.DataFrame(out)


# ---------------------------------------------------------------------------
# Equivalence tests
# ---------------------------------------------------------------------------


class TestVectorizedMatchesScalarOracle:
    """The vectorized pipeline must match the scalar oracle tree-for-tree."""

    @pytest.mark.parametrize("n_trees", [50, 500])
    def test_equivalence_on_synthetic_trees(self, n_trees):
        """For a diverse random tree set, vectorized == scalar at f64 precision.

        Covers Models 1, 2, 4 (via SPCDs that dispatch to each), hardwood
        and softwood paths, trees with and without cull, nullable cull
        values, and at least one Jenkins-fallback SPCD per batch.

        ``rel_tol=1e-9`` is tighter than the existing ``TestPipelineViaCSV``
        sentinels and is achievable because polars and the scalar math use
        the same IEEE-754 operations on the same coefficient floats — there
        is no semantic reordering.
        """
        trees = _build_synthetic_trees(n=n_trees, seed=2026)

        vec = compute_nsvb_biomass(trees.lazy()).collect()
        scalar = _scalar_per_tree(trees)

        # Row counts must match (nothing is filtered out)
        assert vec.height == scalar.height == n_trees

        # Element-wise equivalence on all six output columns.
        # We preserve row order on both sides (neither pipeline reorders),
        # so positional comparison is valid.
        for col in ("v_wood_ib", "v_bark", "w_wood", "w_bark", "w_branch", "agb"):
            vec_vals = vec[col].to_list()
            sc_vals = scalar[col].to_list()
            for i, (v, s) in enumerate(zip(vec_vals, sc_vals)):
                assert math.isclose(v, s, rel_tol=1e-9), (
                    f"{col} divergence at row {i}: vectorized={v} scalar={s} "
                    f"(SPCD={trees['SPCD'][i]}, DIA={trees['DIA'][i]}, "
                    f"HT={trees['HT'][i]}, CULL={trees['CULL'][i]})"
                )

    def test_douglas_fir_worked_example_exact(self):
        """Worked example 1: Douglas-fir intact tree matches scalar sentinel.

        This is the same tree locked in ``TestPipelineViaCSV`` for the scalar
        path. The vectorized path must hit the same f64-tight sentinel values.
        """
        trees = pl.DataFrame(
            {
                "SPCD": [202],
                "DIA": [20.0],
                "HT": [110.0],
                "CULL": [0.0],
                "WDSG": [0.45],
                "JENKINS_SPGRPCD": [10],
            }
        )
        result = compute_nsvb_biomass(trees.lazy()).collect()
        # Sentinels from DOUGFIR_CSV_EXPECTED in test_nsvb_equations.py
        assert math.isclose(result["agb"][0], 3151.7844814148, rel_tol=1e-9)
        assert math.isclose(result["w_wood"][0], 2442.4991973909, rel_tol=1e-9)
        assert math.isclose(result["w_bark"][0], 387.9115217065, rel_tol=1e-9)
        assert math.isclose(result["w_branch"][0], 321.3737623174, rel_tol=1e-9)
        assert math.isclose(result["v_wood_ib"][0], 83.6844822743, rel_tol=1e-9)
        assert math.isclose(result["v_bark"][0], 18.1718836702, rel_tol=1e-9)

    def test_red_maple_worked_example_exact(self):
        """Worked example 2: red maple with 3% cull, exercises Model 4 + cull path.

        Same tree as in the scalar ``TestPipelineViaCSV`` regression sentinels.
        Exercises the full cull-reduction + harmonization arithmetic under
        the vectorized path.
        """
        trees = pl.DataFrame(
            {
                "SPCD": [316],
                "DIA": [11.1],
                "HT": [38.0],
                "CULL": [3.0],
                "WDSG": [0.49],
                "JENKINS_SPGRPCD": [8],
            }
        )
        result = compute_nsvb_biomass(trees.lazy()).collect()
        # Sentinels from REDMAPLE_CSV_EXPECTED in test_nsvb_equations.py
        assert math.isclose(result["agb"][0], 528.1359652182, rel_tol=1e-9)
        assert math.isclose(result["w_wood"][0], 317.9304736164, rel_tol=1e-9)
        assert math.isclose(result["w_bark"][0], 59.2156546044, rel_tol=1e-9)
        assert math.isclose(result["w_branch"][0], 150.9898369973, rel_tol=1e-9)
        assert math.isclose(result["v_wood_ib"][0], 9.4271133316, rel_tol=1e-9)
        assert math.isclose(result["v_bark"][0], 2.1551061437, rel_tol=1e-9)

    def test_agb_equals_component_sum(self):
        """Harmonization invariant: agb == w_wood + w_bark + w_branch per tree."""
        trees = _build_synthetic_trees(n=100, seed=99)
        result = compute_nsvb_biomass(trees.lazy()).collect()
        summed = result["w_wood"] + result["w_bark"] + result["w_branch"]
        for i, (agb, s) in enumerate(zip(result["agb"].to_list(), summed.to_list())):
            assert math.isclose(agb, s, rel_tol=1e-12), (
                f"row {i}: agb={agb} != sum of components={s}"
            )

    def test_cull_nulls_treated_as_zero(self):
        """A null CULL is equivalent to CULL=0.

        The vectorized path calls ``.fill_null(0.0)`` on CULL; asserting that
        here prevents silent behavior change if the null-handling is removed.
        """
        trees = pl.DataFrame(
            {
                "SPCD": [202, 202],
                "DIA": [20.0, 20.0],
                "HT": [110.0, 110.0],
                "CULL": [None, 0.0],
                "WDSG": [0.45, 0.45],
                "JENKINS_SPGRPCD": [3, 3],
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
        result = compute_nsvb_biomass(trees.lazy()).collect()
        # Both rows should produce identical outputs
        assert result["agb"][0] == result["agb"][1]
        assert result["w_wood"][0] == result["w_wood"][1]

    def test_jenkins_fallback_tree(self):
        """A tree with an SPCD absent from all *_spcd tables uses Jenkins fallback.

        The vectorized orchestrator's species-level join will return null
        coefficients for SPCD=9999, and the coalesce with the Jenkins join
        must fill them in from the JENKINS_SPGRPCD match. If this path is
        broken, the output will be null, and the final AGB will also be null.
        """
        trees = pl.DataFrame(
            {
                "SPCD": [9999],  # no species-level row anywhere
                "DIA": [15.0],
                "HT": [60.0],
                "CULL": [0.0],
                "WDSG": [0.5],
                "JENKINS_SPGRPCD": [3],  # soft maple/birch group
            }
        )
        result = compute_nsvb_biomass(trees.lazy()).collect()
        # Must not be null — Jenkins fallback resolved all 5 components
        assert result["agb"][0] is not None
        assert result["agb"][0] > 0
        # Match the scalar oracle exactly
        scalar = _scalar_per_tree(trees)
        assert math.isclose(result["agb"][0], scalar["agb"][0], rel_tol=1e-9)
