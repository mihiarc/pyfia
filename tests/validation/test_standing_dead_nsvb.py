"""NSVB standing dead carbon parity validation against FIADB TREE.CARBON_AG.

Phase 2.5 validation gate for ``pyfia.carbon.standing_dead`` — the dead-tree
analogue of ``test_live_tree_nsvb.py``. The test answers the question the
in-repo unit/equivalence suite cannot: *how closely does the NSVB dead-tree
pipeline + broken-top corrections + decay reductions + S10b carbon fractions
agree with FIADB's pre-computed CARBON_AG values on real Georgia inventory
data for the official 2024 EXPVOL evaluation (EVALID 132401)?*

**Validation scope** (locked from PR 3 Phase 1.6 lessons):

The query joins ``POP_PLOT_STRATUM_ASSGN`` and filters to ``EVALID =
132401`` from the start. Without that filter, the query would pull in
pre-1989 periodic-inventory standing dead trees (1972/1982/1989 panels)
that have FIADB ``CARBON_AG`` / ``DRYBIO_AG`` computed via the legacy
Component Ratio Method (CRM, flat 0.5 carbon fraction; see Appendix K of
FIADB User Guide v9.1, K-1) rather than NSVB. See PR 3 commit ``4ae5bd0``
for the full rationale.

**Population filter** for standing dead trees:

- ``STATUSCD = 2`` (dead)
- ``STANDING_DEAD_CD = '1'`` (excludes downed dead trees, which are part
  of the down dead wood pool, and dead saplings, which FIADB tracks but
  doesn't compute biomass for)
- ``DECAYCD IS NOT NULL`` (the join key for ``REF_TREE_DECAY_PROP``)
- ``DIA >= 1.0`` (NSVB Models 1-5 are not parameterized below 1.0")

This filter recovers the ~6,870 EVALID 132401 standing dead Georgia trees
that have a FIADB ``CARBON_AG`` value to compare against.

**Broken-top corrections (Phase 2.5):**

~75% (5,132 / 6,870) of EVALID 132401 standing dead Georgia trees have
``ACTUALHT < HT`` (broken top). The pipeline applies two adjustments:

- Branch biomass is multiplied by ``Broken_crn_prop`` — the fraction of
  the intact crown remaining below the break, using the mean intact crown
  ratio from Table S11 (``REF_TREE_STND_DEAD_CR_PROP``) keyed on Bailey
  ecoregion province × hw/sw.
- Wood and bark are multiplied by ``(ACTUALHT/HT)^(2/3)`` — a paraboloid
  taper approximation of the stem volume ratio below the break. This
  replaces the Schumacher-Hall Model 6 volume-ratio computation that
  FIADB uses.

**Phase 2.5 measurement (with broken-top corrections):**

- 6,870 trees compared (100% with DIVISION resolved from ECOSUBCD)
- median rel_err: **10.89%**, mean: 12.14%, p95: 27.5%, p99: 36.5%
- biomass ratio (pyfia/FIADB) median: **0.905** (~10% under-estimate)
- within 1%: 15.02%, within 10%: 47.74%, within 50%: 99.88%
- 0 null trees (full SPCD/DECAYCD coverage on the EVALID 132401 set)

The remaining ~10% under-estimate (biomass ratio 0.905) comes from the
paraboloid taper approximation vs FIADB's Model 6. Top-10 worst offenders
are now small trees with peculiar HT/ACTUALHT configurations, not the
extreme broken-top snags that dominated the Phase 2 tail.

**Requires** the ``PLOTGEOM`` table in the test database for the
DIVISION lookup. Same setup as ``test_live_tree_nsvb.py`` — see that
file's docstring for the one-off PLOTGEOM import script for older test
databases. ``PLOTGEOM`` ships in fresh ``pyfia.download()`` outputs as
of the Phase 1.5 ``COMMON_TABLES`` PR.
"""

from __future__ import annotations

import duckdb
import polars as pl
import pytest

from pyfia.carbon.nsvb.carbon_fractions import (
    load_carbon_fractions_dead_df,
    load_dead_cr_prop_df,
    load_dead_decay_proportions_df,
)
from pyfia.carbon.nsvb.coefficients import ecosubcd_to_division_expr
from pyfia.carbon.nsvb.equations import compute_nsvb_dead_biomass

# Ratchet thresholds — tightened for Phase 2.5 broken-top corrections.
#
# Phase 2.5 measurement (Georgia EVALID 132401, 6,870 trees, with
# Appendix K broken-top corrections — crown proportion + paraboloid
# volume ratio):
#   median rel_err = 10.89%, mean = 12.14%, p95 = 27.51%, p99 = 36.49%,
#   max = 90.59%, within 1% = 15.02%, within 10% = 47.74%,
#   within 50% = 99.88%, biomass ratio median = 0.905,
#   null fraction = 0.000%
#
# Improvement over Phase 2 baseline (intact-HT, no corrections):
#   median 17.89% → 10.89%  (-39%)
#   p99    441.5% → 36.5%   (-92%)
#   within-10% 36.8% → 47.7% (+30%)
#   within-50% 71.6% → 99.9% (+39%)
#   biomass ratio 1.174 → 0.905 (moved from 17% over to 10% under)
#
# The remaining ~10% under-estimate (biomass ratio 0.905) is the
# paraboloid taper approximation (exponent 2/3) vs FIADB's Model 6
# Schumacher-Hall volume-ratio model. Top-10 worst offenders are now
# small trees with peculiar HT/ACTUALHT configurations rather than the
# extreme broken-top snags that dominated the Phase 2 tail.
_BASELINE_MEDIAN_REL_ERR = 0.12  # Phase 2.5: 0.1089
_BASELINE_P99_REL_ERR = 0.40  # Phase 2.5: 0.3649
_BASELINE_WITHIN_50PCT_FRAC = 0.99  # Phase 2.5: 0.9988 (lower bound)
_BASELINE_WITHIN_10PCT_FRAC = 0.45  # Phase 2.5: 0.4774 (lower bound)
_BASELINE_WITHIN_1PCT_FRAC = 0.13  # Phase 2.5: 0.1502 (lower bound)
_BASELINE_BIOMASS_RATIO_MEDIAN_MIN = 0.85  # Phase 2.5: 0.9052
_BASELINE_BIOMASS_RATIO_MEDIAN_MAX = 0.95  # Phase 2.5: 0.9052

# Trees with SPCD not covered by any NSVB coefficient path (species-level
# OR Jenkins fallback) get null agb. Allow a little headroom — the
# EVALID-filtered dead set is small (~6,870 trees) so a few nulls are OK.
_MAX_NULL_FRAC = 0.005


class TestStandingDeadNSVBParity:
    """Per-tree NSVB AG dead vs FIADB ``TREE.CARBON_AG`` on real Georgia data.

    The Phase 2 baseline locks the intact-HT approximation behavior. Each
    subsequent commit that improves the NSVB dead pipeline's fidelity
    (broken-top corrections, etc.) should tighten the thresholds below.
    """

    def test_per_tree_ag_agrees_with_fiadb(self, fia_db):
        """Per-tree AG dead carbon parity between pyfia and FIADB.

        Runs the vectorized NSVB dead pipeline (intact-HT + decay
        reductions + S10b fractions) on every eligible standing dead
        Georgia tree and diffs against FIADB's pre-computed
        ``TREE.CARBON_AG`` column.

        Reports the relative-error distribution plus the biomass ratio
        (pyfia / FIADB) and the FIADB-implied dead carbon fraction as
        separate diagnostic layers — if future validation reveals a
        regression, the layered stats localize which layer moved.
        """
        conn = duckdb.connect(fia_db, read_only=True)
        try:
            existing = set(
                conn.execute("SELECT table_name FROM information_schema.tables")
                .pl()["table_name"]
                .to_list()
            )
            if "PLOTGEOM" not in existing:
                pytest.skip(
                    "PLOTGEOM table missing from the test database. The "
                    "DIVISION lookup requires ECOSUBCD. As of the Phase 1.5 "
                    "PR, PLOTGEOM is in pyfia.downloader.COMMON_TABLES and "
                    "new pyfia.download() calls include it automatically. "
                    "For older test databases, see the live-tree validation "
                    "test's module docstring for the one-off PLOTGEOM "
                    "import script."
                )

            # Phase 2 SD-specific filter:
            #   STATUSCD = 2          (dead)
            #   STANDING_DEAD_CD='1'  (excludes downed dead + dead saplings)
            #   DECAYCD IS NOT NULL   (the REF_TREE_DECAY_PROP join key)
            #   DIA  >= 1.0           (NSVB Models 1-5 floor)
            #   CARBON_AG > 0         (FIADB has a value to compare against)
            # Plus the same EVALID 132401 scope filter the live-tree test
            # uses (see PR 3 Phase 1.6 commit ``4ae5bd0`` for why).
            trees = conn.execute("""
                SELECT DISTINCT
                    t.CN,
                    t.SPCD,
                    t.DIA,
                    t.HT,
                    t.ACTUALHT,
                    t.DECAYCD,
                    t.STANDING_DEAD_CD,
                    t.CARBON_AG AS FIADB_CARBON_AG,
                    t.DRYBIO_AG AS FIADB_DRYBIO_AG,
                    rs.JENKINS_SPGRPCD,
                    rs.WOOD_SPGR_GREENVOL_DRYWT AS WDSG,
                    pg.ECOSUBCD
                FROM TREE t
                JOIN POP_PLOT_STRATUM_ASSGN ppsa
                  ON t.PLT_CN = ppsa.PLT_CN
                LEFT JOIN REF_SPECIES rs ON t.SPCD = rs.SPCD
                LEFT JOIN PLOTGEOM pg ON t.PLT_CN = pg.CN
                WHERE ppsa.EVALID = 132401
                  AND t.STATUSCD = 2
                  AND t.STANDING_DEAD_CD = '1'
                  AND t.DECAYCD IS NOT NULL
                  AND t.DIA IS NOT NULL AND t.DIA >= 1.0
                  AND t.HT IS NOT NULL
                  AND t.SPCD IS NOT NULL
                  AND t.CARBON_AG IS NOT NULL AND t.CARBON_AG > 0
                  AND rs.JENKINS_SPGRPCD IS NOT NULL
                  AND rs.WOOD_SPGR_GREENVOL_DRYWT IS NOT NULL
            """).pl()
        finally:
            conn.close()

        n_total = trees.height
        assert n_total > 0, f"no eligible standing dead trees found in {fia_db}"

        # Derive Bailey DIVISION from ECOSUBCD (Level 2 NSVB lookup).
        trees = trees.with_columns(
            ecosubcd_to_division_expr("ECOSUBCD").alias("DIVISION")
        )
        n_with_division = int(trees["DIVISION"].is_not_null().sum())

        # Normalize dtypes. DECAYCD comes off DuckDB as Utf8 because the
        # source CSV column has nulls; cast to Int64 for the join. ACTUALHT
        # is cast to Float64 for the broken-top correction pipeline.
        trees = trees.with_columns(
            [
                pl.col("SPCD").cast(pl.Int64),
                pl.col("JENKINS_SPGRPCD").cast(pl.Int64),
                pl.col("DIA").cast(pl.Float64),
                pl.col("HT").cast(pl.Float64),
                pl.col("ACTUALHT").cast(pl.Float64, strict=False),
                pl.col("DECAYCD").cast(pl.Int64, strict=False),
                pl.col("WDSG").cast(pl.Float64),
                pl.col("FIADB_CARBON_AG").cast(pl.Float64),
                pl.col("FIADB_DRYBIO_AG").cast(pl.Float64),
            ]
        )

        # Diagnostic: how many of the SDs are broken-top vs intact?
        broken_top_count = int(
            trees.filter(
                pl.col("ACTUALHT").cast(pl.Float64, strict=False) < pl.col("HT")
            ).height
        )

        # Run the vectorized NSVB dead biomass pipeline with broken-top
        # corrections (Phase 2.5).
        decay_props = load_dead_decay_proportions_df()
        cr_prop_table = load_dead_cr_prop_df()
        result = compute_nsvb_dead_biomass(
            trees.lazy(), decay_props, cr_prop_table=cr_prop_table
        ).collect()

        # Count trees with null agb (SPCD coverage gaps OR DECAYCD outside 1-5).
        n_null = int(result["agb"].null_count())
        null_frac = n_null / n_total
        print(
            f"\n=== Standing dead NSVB parity vs FIADB "
            f"(Georgia EVALID 132401, {n_total:,} trees) ==="
        )
        print(
            f"  trees with DIVISION resolved from ECOSUBCD: "
            f"{n_with_division:,} ({n_with_division / n_total:.2%})"
        )
        print(
            f"  trees with broken top (ACTUALHT < HT): "
            f"{broken_top_count:,} ({broken_top_count / n_total:.2%})"
        )
        print(
            f"  trees with null NSVB agb (SPCD/DECAYCD coverage gap): "
            f"{n_null:,} ({null_frac:.3%})"
        )

        result = result.filter(pl.col("agb").is_not_null())

        # Apply S10b dead carbon fractions joined on (hw_sw, DECAYCD).
        # The hw_sw expression mirrors the SPCD<300 rule used inside
        # compute_nsvb_dead_biomass for consistency.
        cf_df = load_carbon_fractions_dead_df()
        result = result.with_columns(
            [
                pl.when(pl.col("SPCD") >= 300)
                .then(pl.lit("hardwood"))
                .otherwise(pl.lit("softwood"))
                .alias("_hw_sw_cf"),
            ]
        )
        result = result.join(
            cf_df.rename({"hw_sw": "_hw_sw_cf"}),
            on=["_hw_sw_cf", "DECAYCD"],
            how="left",
        )
        result = result.with_columns(
            [
                (pl.col("agb") * pl.col("CARBON_FRAC_DEAD")).alias("pyfia_CARBON_AG"),
                # Biomass ratio: localizes whether disagreement is in the
                # biomass layer or the carbon-fraction layer.
                (pl.col("agb") / pl.col("FIADB_DRYBIO_AG")).alias("biomass_ratio"),
                # FIADB implied dead carbon fraction — should be in the S10b
                # range (0.47 hardwood / 0.50-0.53 softwood). If it's flat 0.50,
                # FIADB is using legacy CRM rather than NSVB S10b.
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
        n_within_1 = int((rel < 0.01).sum())
        n_within_10 = int((rel < 0.10).sum())
        n_within_50 = int((rel < 0.50).sum())

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
            f"    within 1%   : {n_within_1:>6,} / {n_compared:,} "
            f"({n_within_1 / n_compared:.2%})"
        )
        print(
            f"    within 10%  : {n_within_10:>6,} / {n_compared:,} "
            f"({n_within_10 / n_compared:.2%})"
        )
        print(
            f"    within 50%  : {n_within_50:>6,} / {n_compared:,} "
            f"({n_within_50 / n_compared:.2%})"
        )
        print()
        print("  === biomass ratio (pyfia_NSVB_dead_AGB / FIADB_DRYBIO_AG) ===")
        print(f"    median  : {biomass_stats['median']:.4f}  (target: 1.000)")
        print(f"    mean    : {biomass_stats['mean']:.4f}")
        print(f"    p5      : {biomass_stats['p5']:.4f}")
        print(f"    p95     : {biomass_stats['p95']:.4f}")
        print()
        print("  === FIADB implied dead carbon fraction (CARBON_AG / DRYBIO_AG) ===")
        print(
            f"    median  : {frac_stats['median']:.4f}  "
            f"(S10b range 0.47-0.53 → NSVB, flat 0.50 → pre-NSVB CRM)"
        )
        print(f"    min     : {frac_stats['min']:.4f}")
        print(f"    max     : {frac_stats['max']:.4f}")

        # Top-10 worst offenders for diagnosis. The intact-HT approximation
        # should make broken-top trees the worst offenders — they should
        # cluster at the high end with pyfia / FIADB ratios well above 1.
        worst = (
            result.filter(pl.col("rel_error").is_not_null())
            .sort("rel_error", descending=True)
            .head(10)
        )
        if worst.height > 0:
            print("\n  Top 10 worst disagreements:")
            print("    SPCD    DIA   HT  ACTHT  DC   pyfia_AG    FIADB_AG    rel_err")
            for row in worst.iter_rows(named=True):
                actht_str = row["ACTUALHT"] if row["ACTUALHT"] else "-"
                print(
                    f"    {row['SPCD']:>4}  {row['DIA']:>5.1f} "
                    f"{row['HT']:>4.0f}  {actht_str:>5}  "
                    f"{row['DECAYCD']:>2}  "
                    f"{row['pyfia_CARBON_AG']:>10.2f}  "
                    f"{row['FIADB_CARBON_AG']:>10.2f}  "
                    f"{row['rel_error']:>8.2%}"
                )

        # === Ratchet assertions — locked at the Phase 2 baseline ===
        # See the comment block at the top of this file for the full
        # rationale on why these are loose. Future commits that improve
        # broken-top handling should tighten them.
        assert null_frac < _MAX_NULL_FRAC, (
            f"SPCD/DECAYCD coverage null rate {null_frac:.3%} exceeds "
            f"{_MAX_NULL_FRAC:.1%} — new SPCDs falling out of the "
            "coefficient tables, or new DECAYCD outside the 1-5 range?"
        )
        assert stats["median"] < _BASELINE_MEDIAN_REL_ERR, (
            f"median rel error {stats['median']:.4%} exceeds baseline "
            f"{_BASELINE_MEDIAN_REL_ERR:.2%} — Phase 2 NSVB dead regression?"
        )
        assert stats["p99"] < _BASELINE_P99_REL_ERR, (
            f"p99 rel error {stats['p99']:.4%} exceeds baseline "
            f"{_BASELINE_P99_REL_ERR:.2%} — regression in the tail?"
        )
        assert n_within_50 / n_compared > _BASELINE_WITHIN_50PCT_FRAC, (
            f"only {n_within_50 / n_compared:.2%} of trees agree within 50% "
            f"(baseline: ≥{_BASELINE_WITHIN_50PCT_FRAC:.0%}) — regression?"
        )
        assert n_within_10 / n_compared > _BASELINE_WITHIN_10PCT_FRAC, (
            f"only {n_within_10 / n_compared:.2%} of trees agree within 10% "
            f"(baseline: ≥{_BASELINE_WITHIN_10PCT_FRAC:.0%}) — regression?"
        )
        assert n_within_1 / n_compared > _BASELINE_WITHIN_1PCT_FRAC, (
            f"only {n_within_1 / n_compared:.2%} of trees agree within 1% "
            f"(baseline: ≥{_BASELINE_WITHIN_1PCT_FRAC:.0%}) — regression?"
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
