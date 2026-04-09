"""NSVB live tree carbon parity validation against FIADB TREE.CARBON_AG.

This is the PR 2 validation gate — see PR 2 (``pyfia.carbon.live_tree``).
The test here answers the question PR 2's in-repo unit/equivalence suite
cannot: *how closely does the NSVB pipeline agree with FIADB's pre-computed
carbon values on real inventory data?*

Baseline finding (Georgia EVALID 132401, end_invyr 2024, ~1.25M live trees):

- Biomass ratio (pyfia NSVB AGB / FIADB DRYBIO_AG):
  median **1.030**, mean **1.093**, p95 **1.411**
- Carbon ratio (pyfia CARBON_AG / FIADB CARBON_AG):
  median **1.015**, p95 **1.360**
- ~50% of trees agree within 5%; ~34% within 1%; ~29% within 0.1%
- ~0.2% of trees (SPCD coverage gaps) get ``agb=None`` from the vectorized
  NSVB pipeline and are excluded from the aggregated stats

The systematic 3% median biomass overestimate is consistent with PR 2's
Phase 1 choice to skip the DIVISION-specific coefficient rows (S1a-S8a
columns keyed on Bailey ecoprovince) and use only species-level rows.
FIADB applies the appropriate DIVISION row for trees in each ecoprovince,
producing small systematic differences. Closing the gap requires the
``PLOT.ECOSUBCD → Bailey DIVISION`` mapping that the PR 2 contract
defers to a Phase 1.5 PR.

The assertion thresholds here are **ratchet thresholds** — set slightly
looser than the current baseline so the test passes today, but tight
enough that any regression surfaces. As the DIVISION gap closes
(Phase 1.5), the test should be re-run and the thresholds tightened
commit-by-commit. Each ratchet is a measurable improvement.

FIADB implied carbon fraction (``CARBON_AG / DRYBIO_AG``) is **0.42-0.53**
with median 0.478, exactly the S10a range — confirming FIADB uses
species-specific NSVB carbon fractions, so the fraction layer is not
the source of the gap.
"""

from __future__ import annotations

import duckdb
import polars as pl

from pyfia.carbon.nsvb.carbon_fractions import (
    _compute_default_live_carbon_fraction,
    load_carbon_fractions_live_df,
)
from pyfia.carbon.nsvb.equations import compute_nsvb_biomass

# Ratchet thresholds — set to pass at the current Phase 1 (species-level
# only) baseline measured on Georgia EVALID 132401. Tighten as the Phase
# 1.5 DIVISION-level lookup lands. Any future commit that LOOSENS these
# is a regression; any commit that tightens them is a measurable
# improvement.
_BASELINE_MEDIAN_REL_ERR = 0.060  # current: 0.0487
_BASELINE_P99_REL_ERR = 0.70  # current: 0.6523
_BASELINE_WITHIN_5PCT_FRAC = 0.45  # current: 0.5036 (lower bound)
_BASELINE_BIOMASS_RATIO_MEDIAN_MIN = 1.00  # current: 1.0298
_BASELINE_BIOMASS_RATIO_MEDIAN_MAX = 1.05  # current: 1.0298

# Trees with SPCD not covered by any NSVB coefficient path (species-level
# OR Jenkins fallback) will get ``agb=None`` from compute_nsvb_biomass.
# Currently ~0.2% on Georgia; allow a little headroom.
_MAX_NULL_FRAC = 0.01  # current: 0.0020


class TestLiveTreeNSVBParity:
    """Per-tree NSVB AG vs FIADB ``TREE.CARBON_AG`` on real Georgia data.

    The baseline locks PR 2's species-level-only Phase 1 behavior. Each
    subsequent commit that improves the NSVB pipeline's fidelity (DIVISION
    lookup, missing SPCD coverage, etc.) should tighten the thresholds
    below.
    """

    def test_per_tree_ag_agrees_with_fiadb(self, fia_db):
        """Per-tree AG carbon parity between PR 2 and FIADB.

        Runs the vectorized NSVB pipeline on every eligible live Georgia
        tree, converts to carbon via species-specific S10a fractions, and
        diffs against FIADB's pre-computed ``TREE.CARBON_AG`` column.

        Reports the relative-error distribution plus the biomass ratio
        (pyfia / FIADB) and the FIADB-implied carbon fraction as separate
        diagnostic layers — if future validation reveals a regression,
        the layered stats localize which layer moved.
        """
        # Load every eligible live tree in one go. Polars handles 1.5M
        # rows comfortably in a single LazyFrame pass.
        conn = duckdb.connect(fia_db, read_only=True)
        try:
            trees = conn.execute("""
                SELECT
                    t.CN,
                    t.SPCD,
                    t.DIA,
                    t.HT,
                    t.CULL,
                    t.CARBON_AG AS FIADB_CARBON_AG,
                    t.DRYBIO_AG AS FIADB_DRYBIO_AG,
                    rs.JENKINS_SPGRPCD,
                    rs.WOOD_SPGR_GREENVOL_DRYWT AS WDSG
                FROM TREE t
                LEFT JOIN REF_SPECIES rs ON t.SPCD = rs.SPCD
                WHERE t.STATUSCD = 1
                  AND t.DIA IS NOT NULL AND t.DIA >= 1.0
                  AND t.HT IS NOT NULL
                  AND t.SPCD IS NOT NULL
                  AND t.CARBON_AG IS NOT NULL
                  AND rs.JENKINS_SPGRPCD IS NOT NULL
                  AND rs.WOOD_SPGR_GREENVOL_DRYWT IS NOT NULL
            """).pl()
        finally:
            conn.close()

        n_total = trees.height
        assert n_total > 0, f"no eligible trees found in {fia_db}"

        # Normalize dtypes — see PR 2 commit 12a87c9 for why SPCD must be
        # cast to Int64 here (CSV-loaded TREE.SPCD lands as Float64).
        trees = trees.with_columns(
            [
                pl.col("SPCD").cast(pl.Int64),
                pl.col("JENKINS_SPGRPCD").cast(pl.Int64),
                pl.col("DIA").cast(pl.Float64),
                pl.col("HT").cast(pl.Float64),
                pl.col("CULL").cast(pl.Float64),
                pl.col("WDSG").cast(pl.Float64),
                pl.col("FIADB_CARBON_AG").cast(pl.Float64),
                pl.col("FIADB_DRYBIO_AG").cast(pl.Float64),
            ]
        )

        # Run the vectorized NSVB biomass pipeline (same entry point
        # LiveTreeEstimator.calculate_values uses).
        result = compute_nsvb_biomass(trees.lazy()).collect()

        # Count trees with null agb (SPCD coverage gaps). Report then drop.
        n_null = int(result["agb"].null_count())
        null_frac = n_null / n_total
        print(f"\n=== Live tree NSVB parity vs FIADB (Georgia, {n_total:,} trees) ===")
        print(
            f"  trees with null NSVB agb (SPCD coverage gap): "
            f"{n_null:,} ({null_frac:.3%})"
        )

        result = result.filter(pl.col("agb").is_not_null())

        # Apply species-specific S10a carbon fractions with default-mean
        # fallback, matching LiveTreeEstimator.calculate_values exactly.
        cf_df = load_carbon_fractions_live_df()
        default_frac = _compute_default_live_carbon_fraction()
        result = result.join(cf_df, on="SPCD", how="left")
        result = result.with_columns(
            [
                pl.col("CARBON_FRAC_LIVE")
                .fill_null(default_frac)
                .alias("CARBON_FRAC_LIVE"),
            ]
        )
        result = result.with_columns(
            [
                (pl.col("agb") * pl.col("CARBON_FRAC_LIVE")).alias("pyfia_CARBON_AG"),
                # Biomass ratio: localizes whether disagreement is in the
                # biomass layer or the carbon-fraction layer.
                (pl.col("agb") / pl.col("FIADB_DRYBIO_AG")).alias("biomass_ratio"),
                # FIADB implied carbon fraction — should be in the S10a
                # range [0.40, 0.55]. If it's flat 0.50, FIADB is pre-NSVB.
                (pl.col("FIADB_CARBON_AG") / pl.col("FIADB_DRYBIO_AG")).alias(
                    "fiadb_implied_frac"
                ),
            ]
        )

        # Only compute rel-error stats on trees with valid pyfia output and
        # positive FIADB CARBON_AG (a zero denominator is degenerate).
        result = result.filter(
            (pl.col("pyfia_CARBON_AG").is_not_null()) & (pl.col("FIADB_CARBON_AG") > 0)
        )
        n_compared = result.height

        result = result.with_columns(
            [
                (
                    (pl.col("pyfia_CARBON_AG") - pl.col("FIADB_CARBON_AG")).abs()
                    / pl.col("FIADB_CARBON_AG").abs()
                ).alias("rel_error"),
            ]
        )

        rel = result["rel_error"]
        n_within_0p1 = int((rel < 0.001).sum())
        n_within_1 = int((rel < 0.01).sum())
        n_within_5 = int((rel < 0.05).sum())

        stats = {
            "mean": float(rel.mean()),
            "median": float(rel.median()),
            "p95": float(rel.quantile(0.95)),
            "p99": float(rel.quantile(0.99)),
            "max": float(rel.max()),
        }
        br = result["biomass_ratio"]
        biomass_stats = {
            "median": float(br.median()),
            "mean": float(br.mean()),
            "p5": float(br.quantile(0.05)),
            "p95": float(br.quantile(0.95)),
        }
        frac_stats = {
            "median": float(result["fiadb_implied_frac"].median()),
            "min": float(result["fiadb_implied_frac"].min()),
            "max": float(result["fiadb_implied_frac"].max()),
        }

        print(
            f"  trees compared (non-null pyfia, positive FIADB_CARBON_AG): "
            f"{n_compared:,}"
        )
        print()
        print("  === per-tree carbon rel_error ===")
        print(f"    mean    : {stats['mean']:.4%}")
        print(f"    median  : {stats['median']:.4%}")
        print(f"    p95     : {stats['p95']:.4%}")
        print(f"    p99     : {stats['p99']:.4%}")
        print(f"    max     : {stats['max']:.4%}")
        print(
            f"    within 0.1% : {n_within_0p1:>10,} / {n_compared:,} "
            f"({n_within_0p1 / n_compared:.2%})"
        )
        print(
            f"    within 1%   : {n_within_1:>10,} / {n_compared:,} "
            f"({n_within_1 / n_compared:.2%})"
        )
        print(
            f"    within 5%   : {n_within_5:>10,} / {n_compared:,} "
            f"({n_within_5 / n_compared:.2%})"
        )
        print()
        print("  === biomass ratio (pyfia_NSVB_AGB / FIADB_DRYBIO_AG) ===")
        print(f"    median  : {biomass_stats['median']:.4f}  (target: 1.000)")
        print(f"    mean    : {biomass_stats['mean']:.4f}")
        print(f"    p5      : {biomass_stats['p5']:.4f}")
        print(f"    p95     : {biomass_stats['p95']:.4f}")
        print()
        print("  === FIADB implied carbon fraction (CARBON_AG / DRYBIO_AG) ===")
        print(
            f"    median  : {frac_stats['median']:.4f}  "
            f"(S10a range 0.40-0.55 → NSVB, flat 0.50 → pre-NSVB)"
        )
        print(f"    min     : {frac_stats['min']:.4f}")
        print(f"    max     : {frac_stats['max']:.4f}")

        # Top-10 worst offenders for diagnosis, filtering out any lingering
        # nulls (shouldn't be any after the earlier filter, but belt-and-
        # suspenders against NaN pass-through).
        worst = (
            result.filter(pl.col("rel_error").is_not_null())
            .sort("rel_error", descending=True)
            .head(10)
        )
        if worst.height > 0:
            print("\n  Top 10 worst disagreements:")
            print(
                "    SPCD    DIA    HT   CULL      pyfia_AG       FIADB_AG    rel_err"
            )
            for row in worst.iter_rows(named=True):
                cull_val = row["CULL"] if row["CULL"] is not None else 0.0
                print(
                    f"    {row['SPCD']:>4}  {row['DIA']:>5.1f}  "
                    f"{row['HT']:>4.1f}  {cull_val:>5.1f}  "
                    f"{row['pyfia_CARBON_AG']:>12.2f}  "
                    f"{row['FIADB_CARBON_AG']:>12.2f}  "
                    f"{row['rel_error']:>8.2%}"
                )

        # === Ratchet assertions — loosely above baseline so this commit ===
        # passes at the current Phase 1 (species-level only) behavior. Each
        # Phase 1.5 improvement should tighten these constants.
        assert null_frac < _MAX_NULL_FRAC, (
            f"SPCD coverage null rate {null_frac:.3%} exceeds "
            f"{_MAX_NULL_FRAC:.1%} — new SPCDs falling out of the "
            "coefficient tables?"
        )
        assert stats["median"] < _BASELINE_MEDIAN_REL_ERR, (
            f"median rel error {stats['median']:.4%} exceeds baseline "
            f"{_BASELINE_MEDIAN_REL_ERR:.2%} — PR 2 NSVB regression?"
        )
        assert stats["p99"] < _BASELINE_P99_REL_ERR, (
            f"p99 rel error {stats['p99']:.4%} exceeds baseline "
            f"{_BASELINE_P99_REL_ERR:.2%} — regression in the tail?"
        )
        assert n_within_5 / n_compared > _BASELINE_WITHIN_5PCT_FRAC, (
            f"only {n_within_5 / n_compared:.2%} of trees agree within 5% "
            f"(baseline: ≥{_BASELINE_WITHIN_5PCT_FRAC:.0%}) — regression?"
        )
        assert (
            _BASELINE_BIOMASS_RATIO_MEDIAN_MIN
            <= biomass_stats["median"]
            <= _BASELINE_BIOMASS_RATIO_MEDIAN_MAX
        ), (
            f"biomass ratio median {biomass_stats['median']:.4f} outside "
            f"baseline window [{_BASELINE_BIOMASS_RATIO_MEDIAN_MIN:.3f}, "
            f"{_BASELINE_BIOMASS_RATIO_MEDIAN_MAX:.3f}] — systematic shift?"
        )
