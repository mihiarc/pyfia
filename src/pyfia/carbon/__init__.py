"""
NSVB-based forest carbon estimation for pyFIA.

This subpackage implements forest carbon stock estimation across the IPCC and
NGHGI reporting pools using the National Scale Volume and Biomass framework
(NSVB; Westfall et al. 2023, GTR-WO-104) for tree-level biomass and species-
specific carbon fractions for the carbon conversion. It is the estimation
engine for the Schmidt Sciences "Synthetic Inventory" project, a publicly
auditable Python reconstruction of the U.S. NGHGI LULUCF forest carbon time
series.

Status roadmap
==============

Phase 1 — merged (PR 1 + PR 2)
-------------------------------
The NSVB equation library, coefficient loaders, and the live-tree estimator
are in place:

- :mod:`pyfia.carbon.nsvb.equations` — pure-math NSVB Models 1, 2, 4, 5 plus
  the harmonization algorithm, a scalar reference ``predict_tree_biomass``,
  and the vectorized ``compute_nsvb_biomass`` polars pipeline.
- :mod:`pyfia.carbon.nsvb.coefficients` — vendored S1a–S8b coefficient table
  loaders, species-level and Jenkins lookups, and the lookup precedence
  resolver.
- :mod:`pyfia.carbon.nsvb.carbon_fractions` — vendored S10a/S10b carbon
  fraction loaders with species-specific lookup, a PEP 562 lazy default,
  and a warn-once fallback for unknown SPCDs.
- :mod:`pyfia.carbon.live_tree` — ``live_tree(db, pool='ag'|'bg'|'total')``
  public function + ``LiveTreeEstimator(BaseEstimator)``. Re-exported from
  ``pyfia``. Uses NSVB for AG; bridges to FIADB ``TREE.CARBON_BG`` for BG.

Phase 1.5 — in progress (PR 3: carbon live-tree validation gate)
-----------------------------------------------------------------
Real-data validation against FIADB ``TREE.CARBON_AG`` on the Georgia
EVALID 132401 database (end_invyr 2024, NSVB-era, ~1.25M live trees) is
wired into ``tests/validation/test_live_tree_nsvb.py``. The initial baseline
measurement (commit ``e1f0254``) surfaced a systematic 3.2% biomass
overestimate for growing-stock trees and a catastrophic disagreement on
rotten-cull trees.

**Landed:**

- *DIVISION coefficient lookup* (commit ``adf3635``): implements Level 2 of
  the NSVB lookup precedence. ``build_division_lookup`` emits DIVISION-keyed
  rows from the ``*_spcd`` tables, ``ecosubcd_to_division`` is the Bailey
  subsection→division crosswalk, and ``_join_and_eval_component`` now does a
  3-way coalesce (DIVISION → species-level → Jenkins) when the trees frame
  has a ``DIVISION`` column. Activates ~63% of the vendored coefficient rows
  that Phase 1 was skipping as dead code. Measured gap closure on Georgia:
  median rel_err 4.87% → 3.55%, biomass ratio median 1.0179 → **1.0000
  exact**, within-1% coverage 34% → 43%.

- *PLOTGEOM in COMMON_TABLES* (commit ``ecd5128``): adds ``PLOTGEOM`` to
  ``pyfia.downloader.tables.COMMON_TABLES`` so fresh ``pyfia.download()``
  calls pull ECOSUBCD automatically; avoids the one-off import script that
  the Phase 1.5 validation test previously required.

- *Phase 1.7 — ECOSUBCD wired into ``LiveTreeEstimator``* (this commit):
  :meth:`pyfia.carbon.live_tree.LiveTreeEstimator.calculate_values` now
  loads ``PLOTGEOM`` via the new ``_load_plotgeom`` helper, joins on
  ``PLT_CN``, and adds a ``DIVISION`` column derived from ``ECOSUBCD``
  before calling :func:`compute_nsvb_biomass`. The production
  ``pyfia.carbon.live_tree()`` API now benefits from the same Level 2
  lookup that the validation test was exercising directly, closing the
  Phase 1 ~3.2% growing-stock bias on the production estimator path.
  ``PLOTGEOM`` is loaded out-of-band (not via
  :meth:`get_required_tables`) because pyfia's
  :class:`~pyfia.estimation.data_loading.DataLoader` doesn't have a slot
  for spatial / reference tables in its TREE/COND/PLOT join graph; the
  estimator falls back gracefully (logs a one-shot warning, runs at
  Phase 1 quality) when ``PLOTGEOM`` is missing from older test
  databases.

**Still pending in Phase 1.5:**

- **TREECLCD=4 rotten-cull methodology investigation (Phase 1.6)**: the
  top-10 worst per-tree disagreements are all CULL ≥ 95%, TREECLCD=4
  (rotten cull), with pyfia predicting 46×-125× more carbon than FIADB.
  The literal GTR-WO-104 formula in
  ``pyfia.carbon.nsvb.equations.compute_nsvb_biomass``::

      W_wood_red = V_wood_ib × [1 - CULL/100 × (1 - DensProp)] × WDSG × 62.4

  uses DensProp = 0.54 (hardwood) / 0.92 (softwood) from Harmon et al. 2011
  Table 1 DECAYCD=3. For CULL=95% hardwood this retains 56.3% of wood
  weight; FIADB's implied retention is ~5%. Needs FIA Appendix K of the
  user guide (not yet in the Schmidt references library) to resolve —
  likely a TREECLCD-based dispatch or a different DensProp model for
  rotten cull. This disagreement dominates the p99/max tail of the
  validation but only affects ~9,354 trees (0.75%) in the Georgia sample.

Phase 2+ — deferred
-------------------
Each additional IPCC pool will land as its own ``pyfia.carbon.<pool>(db, ...)``
function on the flat ``pyfia.carbon/`` layout, following the same
architectural rules.

- Standing dead trees (uses S10b dead-tree carbon fractions, already loaded
  by :func:`pyfia.carbon.nsvb.carbon_fractions.load_carbon_fractions_dead`)
- Understory (Birdsey 1992 ratios, EPA NGHGI Annex 3.13)
- Downed dead wood (Domke et al. 2013)
- Litter + duff (Domke et al. 2016)
- Soil organic carbon (Domke et al. 2017)
- Native NSVB belowground (coarse-root) model (replaces the Phase 1 FIADB
  ``CARBON_BG`` bridge; Heath et al. 2009)

Architectural rules (frozen in Phase 1, preserved going forward)
=================================================================
1. **Public API is functions, not classes.** Match the pyfia convention
   (``biomass(db, ...)``, ``mortality(db, ...)``). No ``CarbonEstimator``
   container class. Captured in CLAUDE.md.

2. **Vectorize coefficient lookups via polars joins.** The scalar
   ``lookup_coefficients`` and ``predict_tree_biomass`` functions are
   *reference implementations* used to lock numerical correctness against
   the GTR-WO-104 worked examples. They are NOT the production data path.
   The production path is ``compute_nsvb_biomass``, a pure polars pipeline
   over a LazyFrame joined to the coefficient tables.

3. **Inherit from ``BaseEstimator``.** New pool estimators must follow the
   template-method pattern in :mod:`pyfia.estimation.base`
   (``load_data → apply_filters → calculate_values → aggregate_results →
   calculate_variance → format_output``). Take ``(db: str | FIA, config: dict)``
   in ``__init__``, not ``(db, *, year)``.

4. **Match the ``mortality()`` docstring quality.** ``mortality`` is the
   documentation gold standard per CLAUDE.md. Follow its parameter section,
   return value table, examples block, and notes layout.

5. **Bridge belowground (BG) carbon to FIADB ``CARBON_BG`` for now.** Phase 1
   does not implement the Heath et al. (2009) coarse-root model. The bridge
   is acknowledged tech debt; revisit when the native BG model lands.

Items resolved from Phase 1 review
===================================
All six blockers from the PR 1/PR 2 critical reviews are closed:

- **Schema fragility** — ``load_nsvb_coefficients`` passes explicit
  ``schema_overrides`` so DIVISION is always ``Utf8`` and STDORGCD is
  always ``Int64``.
- **Boundary types** — ``predict_tree_biomass`` validates ``dia >= 1.0``
  with a clear ``ValueError``, normalizes ``hw_sw`` casing, and types
  ``hw_sw`` as ``Literal["hardwood", "softwood"]``.
- **Default carbon fraction** — ``DEFAULT_LIVE_CARBON_FRACTION`` is a
  PEP 562 ``__getattr__`` lazy module attribute (~0.4741, computed from
  the current S10a CSV), replacing the hardcoded 0.4716 that had drifted.
- **SPCD 10 misclassification** — resolved by deriving ``hw_sw`` from the
  ``SPCD < 300`` rule (consistent with ``_model_k``), sidestepping S10a's
  hardwood/softwood column which had SPCD=10 wrong.
- **Lookup precedence Level 2 (DIVISION)** — implemented in Phase 1.5
  (this file's "in progress" section above).
- **Lookup precedence Level 1 (STDORGCD)** — still unused, only ~10 rows
  across all 5 tables. Revisit once the full Phase 1.5 validation is
  complete.
- **SPCD Float64 join mismatch** — ``LiveTreeEstimator.calculate_values``
  casts SPCD to Int64 at the entry to match the coefficient-table dtype
  (FIA CSV-loaded TREE.SPCD lands as Float64 when the raw data has any
  null). See PR 2 follow-up commit ``12a87c9``.

Pointers for the next session
==============================
- **Current gap-closure work**: PR 3 at
  ``https://github.com/ctrees-products/pyfia/pull/3`` (branch
  ``feat/carbon-live-tree-validation``).
- **Validation measurement instrument**:
  ``tests/validation/test_live_tree_nsvb.py`` — runs on the Georgia
  EVALID 132401 database, reports layered diagnostics (carbon rel_error
  + biomass ratio + FIADB implied fraction) and asserts against ratchet
  thresholds. Locks the Phase 1.5 baseline.
- **Schmidt references library**:
  ``/Users/cmihiar/Documents/Claude/Projects/schmidt/references_md/`` —
  ``tier1_fcaf/gtr_wo104_westfall2023.md`` for the NSVB equations,
  ``tier3_fiadb/fiadb_database_description_v9_2/fia_section_3_1_tree_table.md``
  for FIADB column definitions. Appendix K is NOT yet in the library
  and would need to be fetched for the Phase 1.6 TREECLCD investigation.

References
----------
- Westfall, J.A. et al. (2023). GTR-WO-104. DOI: 10.2737/WO-GTR-104
- Woodall, C.W. et al. (2015). GTR-NRS-154 (FCAF methodology blueprint).
- Harmon, M.E. et al. (2011). GTR-WO-104 Table 1 (dead-tree density).
- USEPA (2024). NGHGI Annex 3.13.
"""

from __future__ import annotations

from pyfia.carbon.live_tree import LiveTreeEstimator, live_tree

__all__ = ["LiveTreeEstimator", "live_tree"]
