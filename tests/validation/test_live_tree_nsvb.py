"""NSVB live tree carbon parity validation against FIADB TREE.CARBON_AG.

This is the PR 2+ validation gate — see PR 2 (``pyfia.carbon.live_tree``)
and the ongoing PR 3 closure work. The test answers the question the
in-repo unit/equivalence suite cannot: *how closely does the NSVB
pipeline agree with FIADB's pre-computed carbon values on real inventory
data for the official Georgia 2024 EXPVOL evaluation (EVALID 132401)?*

History of the ratchet thresholds in this file:

**Phase 1 baseline** (species-level fallback only, no EVALID filter,
measured before the ECOSUBCD lookup landed — commit e1f0254):

- Biomass ratio (pyfia NSVB AGB / FIADB DRYBIO_AG):
  median **1.030**, mean **1.093**, p95 **1.411**
- ~50% of trees agree within 5%; ~34% within 1%; ~29% within 0.1%
- Rooted in the PR 2 Phase 1 choice to skip DIVISION-specific
  coefficient rows (S1a-S8a columns keyed on Bailey ecoprovince)

**Phase 1.5 closure** (no EVALID filter, ECOSUBCD → DIVISION lookup
wired in — commit adf3635):

- Joins ``PLOTGEOM.ECOSUBCD`` → derives the Bailey DIVISION via
  :func:`pyfia.carbon.nsvb.coefficients.ecosubcd_to_division` →
  passes the ``DIVISION`` column through ``compute_nsvb_biomass``, which
  then activates the Level 2 coalesce.
- median rel_err 4.87% → 3.55%, biomass ratio median 1.0179 → 1.0000.

**Phase 1.6 finding — validation scope correction** (this commit):

- The Phase 1 / 1.5 numbers above were measured on the FULL Georgia DB
  (1.46M live trees), which includes 575k pre-1989 periodic-inventory
  trees (1972, 1982, 1989 panels) NOT in the current annual evaluation.
- Those periodic-era trees have FIADB ``CARBON_AG`` / ``DRYBIO_AG``
  computed via the legacy Component Ratio Method (CRM, flat 0.5 carbon
  fraction; see Appendix K of FIADB User Guide v9.1, K-1) rather than
  NSVB. Comparing pyfia's NSVB recompute against legacy-CRM data was
  producing spurious 1,000-12,000% rel_err outliers — almost all
  CULL ≥ 90 TREECLCD=4 hardwoods from the periodic panels.
- The Phase 1.6 task as originally framed (TREECLCD=4 cull formula
  investigation) was misdiagnosed: the pyfia cull formula
  ``(1 - (1 - DENSITY_PROP) * CULL/100) * Stem Wood`` matches Appendix K
  exactly, with no TREECLCD dispatch. The actual fix is to scope the
  validation test to the EVALID set, which we now do via
  ``JOIN POP_PLOT_STRATUM_ASSGN ppsa ON t.PLT_CN = ppsa.PLT_CN
  AND ppsa.EVALID = 132401`` in the SQL above.
- Massive measured improvement on the EVALID-filtered set
  (130,952 trees): median rel_err **3.55% → 0.085%** (42x), within-1%
  **43% → 63%**, within-0.1% **39% → 58%**, max rel_err
  **12,425% → 478%**, mean **9.6% → 4.4%**.

FIADB implied carbon fraction (``CARBON_AG / DRYBIO_AG``) on the
EVALID-filtered set is **0.42-0.53** with median 0.477, exactly the
S10a range — confirming the EVALID 132401 trees are all NSVB-era and
the fraction layer is not the source of any residual gap.

**Requires** the ``PLOTGEOM`` table in the test database. ``PLOTGEOM``
is in ``pyfia.downloader.COMMON_TABLES`` as of the Phase 1.5 PR, so any
fresh ``pyfia.download(state)`` call pulls it automatically. Existing
test databases downloaded *before* that change need a one-off import:

.. code-block:: python

    # Only needed for test databases downloaded before PLOTGEOM was
    # added to COMMON_TABLES. New downloads include it automatically.
    from pyfia.downloader.client import DataMartClient
    import duckdb, tempfile
    from pathlib import Path

    client = DataMartClient()
    with tempfile.TemporaryDirectory() as tmp:
        p = Path(tmp)
        client.download_tables("GA", tables=["PLOTGEOM"], common=False, dest_dir=p)
        conn = duckdb.connect("data/georgia.duckdb")
        csv = next(p.glob("*.csv"))
        conn.execute(f\"\"\"
            CREATE OR REPLACE TABLE PLOTGEOM AS
            SELECT * FROM read_csv_auto('{csv}', header=true, ignore_errors=true)
        \"\"\")
        conn.close()

If ``PLOTGEOM`` is missing the test skips with a helpful message.
"""

from __future__ import annotations

import duckdb
import polars as pl
import pytest

from pyfia.carbon.nsvb.carbon_fractions import (
    _compute_default_live_carbon_fraction,
    load_carbon_fractions_live_df,
)
from pyfia.carbon.nsvb.coefficients import ecosubcd_to_division_expr
from pyfia.carbon.nsvb.equations import compute_nsvb_biomass

# Ratchet thresholds — locked at the Phase 1.6 baseline (EVALID-filtered
# scope, ECOSUBCD → DIVISION lookup active). Any commit that LOOSENS these
# is a regression; any commit that tightens them is a measurable improvement.
#
# Phase 1 baseline (no EVALID filter, before DIVISION lookup, commit e1f0254):
#   median rel_err = 4.87%, p99 = 65.23%, within 5% = 50.36%,
#   biomass ratio median = 1.0179
#
# Phase 1.5 baseline (no EVALID filter, DIVISION lookup wired, commit adf3635):
#   median rel_err = 3.55% (-27%), p99 = 65.55% (~unchanged), within 5% = 53.90%
#   biomass ratio median = 1.0000, within 1% = 43.07%, within 0.1% = 38.83%
#
# Phase 1.6 baseline (EVALID 132401 filter applied, this commit):
#   The unfiltered baselines above were polluted by 575k pre-1989
#   periodic-inventory trees that FIADB computed via legacy CRM. Filtering
#   to the official EVALID 132401 evaluation (130,952 NSVB-era trees)
#   reveals the real Phase 1.5/1.7 quality:
#   median rel_err = 0.0846% (42x better), p99 = 40.37% (1.6x better),
#   within 5% = 73.32%, within 1% = 62.91%, within 0.1% = 58.20%,
#   max rel_err = 478.62% (down from 12,425%), biomass ratio median = 1.0000
_BASELINE_MEDIAN_REL_ERR = 0.005  # Phase 1.6: 0.000846
_BASELINE_P99_REL_ERR = 0.50  # Phase 1.6: 0.4037
_BASELINE_WITHIN_5PCT_FRAC = 0.70  # Phase 1.6: 0.7332 (lower bound)
_BASELINE_WITHIN_1PCT_FRAC = 0.60  # Phase 1.6: 0.6291 (lower bound)
_BASELINE_WITHIN_0P1PCT_FRAC = 0.55  # Phase 1.6: 0.5820 (lower bound)
_BASELINE_BIOMASS_RATIO_MEDIAN_MIN = 0.999  # Phase 1.6: 1.0000
_BASELINE_BIOMASS_RATIO_MEDIAN_MAX = 1.001  # Phase 1.6: 1.0000

# Trees with SPCD not covered by any NSVB coefficient path (species-level
# OR Jenkins fallback) will get ``agb=None`` from compute_nsvb_biomass.
# Allow a little headroom — the EVALID-filtered set has even fewer null
# trees than the unfiltered set since it's all annual-inventory data.
_MAX_NULL_FRAC = 0.005  # Phase 1.6 EVALID-filtered: well below 0.001


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
        # Load every eligible live tree in one go, joined to PLOTGEOM to
        # get ECOSUBCD for the Phase 1.5 DIVISION lookup. Skip the test
        # gracefully when PLOTGEOM is missing so CI runs without it don't
        # fail — this file is gated on the ``validation`` marker and is
        # opt-in by design.
        conn = duckdb.connect(fia_db, read_only=True)
        try:
            existing = set(
                conn.execute("SELECT table_name FROM information_schema.tables")
                .pl()["table_name"]
                .to_list()
            )
            if "PLOTGEOM" not in existing:
                pytest.skip(
                    "PLOTGEOM table missing from the test database. "
                    "Phase 1.5 validation requires ECOSUBCD. As of the "
                    "Phase 1.5 PR, PLOTGEOM is in pyfia.downloader"
                    ".COMMON_TABLES and new pyfia.download() calls "
                    "include it automatically. For older test databases, "
                    "see this file's module docstring for the one-off "
                    "PLOTGEOM import script."
                )

            # Phase 1.6 fix: filter to EVALID 132401 (the official Georgia
            # 2024 EXPVOL evaluation, ~131k trees). Without this filter, the
            # query pulled in 575k pre-1989 periodic-inventory trees from
            # 1972/1982/1989 panels — those have FIADB CARBON_AG/DRYBIO_AG
            # computed via the legacy Component Ratio Method (CRM, flat 0.5
            # carbon fraction; see Appendix K of FIADB User Guide v9.1, K-1)
            # rather than NSVB. Comparing pyfia's NSVB recompute against
            # legacy-CRM data was producing spurious 1,000-12,000% rel_err
            # outliers (almost all CULL ≥ 90 TREECLCD=4 hardwoods from the
            # periodic inventory) that masked the real Phase 1.5/1.7 quality.
            # The DISTINCT is required because POP_PLOT_STRATUM_ASSGN can
            # have duplicate (PLT_CN, EVALID) rows in some states.
            trees = conn.execute("""
                SELECT DISTINCT
                    t.CN,
                    t.SPCD,
                    t.DIA,
                    t.HT,
                    t.CULL,
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
                  AND t.STATUSCD = 1
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

        # Derive the Bailey DIVISION from ECOSUBCD. ``ecosubcd_to_division``
        # returns None for null/malformed ECOSUBCDs, which the downstream
        # coalesce handles correctly by falling through to species-level
        # and Jenkins.
        trees = trees.with_columns(
            ecosubcd_to_division_expr("ECOSUBCD").alias("DIVISION")
        )
        n_with_division = int(trees["DIVISION"].is_not_null().sum())

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
            f"  trees with DIVISION resolved from ECOSUBCD: "
            f"{n_with_division:,} ({n_with_division / n_total:.2%})"
        )
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

        # === Ratchet assertions — locked at the Phase 1.5 baseline. ===
        # Each assertion failure should be interpreted as either a
        # regression (if the violation is on the wrong side) or an
        # opportunity to tighten the constant (if the new measurement
        # beats the baseline).
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
        assert n_within_1 / n_compared > _BASELINE_WITHIN_1PCT_FRAC, (
            f"only {n_within_1 / n_compared:.2%} of trees agree within 1% "
            f"(baseline: ≥{_BASELINE_WITHIN_1PCT_FRAC:.0%}) — regression?"
        )
        assert n_within_0p1 / n_compared > _BASELINE_WITHIN_0P1PCT_FRAC, (
            f"only {n_within_0p1 / n_compared:.2%} of trees agree within "
            f"0.1% (baseline: ≥{_BASELINE_WITHIN_0P1PCT_FRAC:.0%}) — "
            "regression?"
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
