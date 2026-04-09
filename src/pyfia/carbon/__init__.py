"""
NSVB-based forest carbon estimation for pyFIA.

This subpackage implements forest carbon stock estimation across the IPCC and
NGHGI reporting pools using the National Scale Volume and Biomass framework
(NSVB; Westfall et al. 2023, GTR-WO-104) for tree-level biomass and species-
specific carbon fractions for the carbon conversion. It is the estimation
engine for the Schmidt Sciences "Synthetic Inventory" project, a publicly
auditable Python reconstruction of the U.S. NGHGI LULUCF forest carbon time
series.

Currently in this PR (PR 1)
---------------------------
The math and data layer only — no estimator entry points yet:

- :mod:`pyfia.carbon.nsvb.equations` — pure-math NSVB Models 1, 2, 4, 5 plus
  the harmonization algorithm and the ``predict_tree_biomass`` orchestrator
- :mod:`pyfia.carbon.nsvb.coefficients` — vendored S1a–S8b coefficient table
  loaders and the lookup precedence resolver
- :mod:`pyfia.carbon.nsvb.carbon_fractions` — vendored S10a/S10b carbon
  fraction loaders with species-specific lookup and warn-once fallback

``pyfia.carbon`` does not yet expose any public functions. Until PR 2 lands,
callers wanting carbon pool estimates should continue to use ``pyfia.biomass``
which reads FIA's pre-computed ``CARBON_AG``/``CARBON_BG`` columns directly.

PR 2 contract — what the next PR adds
--------------------------------------
PR 2 (``feat/carbon-nsvb-live-tree-estimator``) wires the live tree pool:

- ``pyfia.carbon.live_tree(db, ...)`` — public function, signature mirrors
  ``pyfia.biomass()``: ``grp_by``, ``by_species``, ``by_size_class``,
  ``land_type``, ``tree_domain``, ``area_domain``, ``totals``, ``variance``,
  ``most_recent``, plus ``pool='ag'|'bg'|'total'``.
- Internal ``LiveTreeEstimator(BaseEstimator)`` class in
  ``src/pyfia/carbon/live_tree.py``, following the existing
  ``pyfia.estimation.estimators.biomass.BiomassEstimator`` template.
- Re-export ``live_tree`` from ``pyfia/__init__.py`` alongside ``biomass``,
  ``mortality``, etc.
- Real-data validation gate in PR 3 against FIADB ``CARBON_AG``.

Architectural rules for PR 2 (and beyond)
------------------------------------------
1. **Public API is functions, not classes.** Match the pyfia convention
   (``biomass(db, ...)``, ``mortality(db, ...)``). The convention is captured
   in CLAUDE.md ("Direct functions: ``volume(db)`` not
   ``VolumeEstimatorFactory.create().estimate()``"). No ``CarbonEstimator``
   container class.

2. **Vectorize coefficient lookups via polars joins.** The scalar
   ``lookup_coefficients`` and ``predict_tree_biomass`` functions in PR 1 are
   *reference implementations* used to lock in numerical correctness against
   the GTR-WO-104 worked examples. They are NOT the production data path.
   PR 2 must implement equation evaluation as polars expressions on a
   ``LazyFrame`` joined to the coefficient tables on ``SPCD``, not by calling
   ``predict_tree_biomass`` per tree. At FIA scale (1M+ trees per state) the
   scalar path is too slow.

3. **Inherit from ``BaseEstimator``.** ``LiveTreeEstimator`` must follow the
   template-method pattern in :mod:`pyfia.estimation.base`: ``load_data →
   apply_filters → calculate_values → aggregate_results → calculate_variance →
   format_output``. Take ``(db: str | FIA, config: dict)`` in ``__init__``,
   not ``(db, *, year)``.

4. **Match the ``mortality()`` docstring quality.** Per CLAUDE.md,
   ``mortality`` is the documentation gold standard. Follow its parameter
   section, return value table, examples block, and notes layout.

5. **Bridge belowground (BG) carbon to FIADB ``CARBON_BG`` for now.** Phase 1
   does not implement the Heath et al. (2009) coarse-root model. Use FIADB's
   pre-computed ``CARBON_BG`` column directly when ``pool='bg'`` or
   ``pool='total'``. The bridge is acknowledged tech debt; revisit if PR 3's
   validation gate flags it.

Items deferred from PR 1 review (address in PR 2 unless noted)
---------------------------------------------------------------
- **Schema fragility** *(resolved in PR 2)*: ``load_nsvb_coefficients`` now
  passes explicit ``schema_overrides`` so DIVISION is always ``Utf8`` and
  STDORGCD is always ``Int64``. The ``infer_schema_length=10_000`` workaround
  is removed.
- **Boundary types** *(resolved in PR 2)*: ``predict_tree_biomass`` now
  validates ``dia >= 1.0`` with a clear ``ValueError``, normalizes ``hw_sw``
  casing at the function boundary (``"Hardwood"`` / ``"SOFTWOOD"`` are both
  accepted), and types ``hw_sw`` as ``Literal["hardwood", "softwood"]``.
- **Default carbon fraction** *(resolved in PR 2)*: ``DEFAULT_LIVE_CARBON_FRACTION``
  is now a lazily-computed module attribute (PEP 562 ``__getattr__``) that
  reports the arithmetic mean of the S10a table (~0.4741). Previously it was
  hardcoded at 0.4716, which had drifted from the actual population mean.
- **SPCD 10 (``fir spp.``) misclassification** *(resolved in PR 2)*: S10a
  lists SPCD=10 as hardwood, but the SPCD<300 rule used by ``_model_k``
  correctly classifies it as softwood. PR 2's architectural decision: derive
  ``hw_sw`` from the ``SPCD < 300`` rule rather than reading S10a's ``hw_sw``
  column. This is internally consistent with NSVB's own Model 2 base-constant
  selection and sidesteps the S10a classification error entirely.
- **Null coercion**: ``_row_to_dict`` silently maps null coefficients to
  ``0.0``. The vectorized path validates that required parameters are non-null
  after joining. Addressed in the vectorized pipeline contract.
- **Lookup precedence Levels 1–2 are dead code in PR 1** (STDORGCD is null in
  396/406 ``volib_spcd`` rows; Phase 1 always passes ``division=None``).
  The vectorized path in PR 2 skips levels 1-2 entirely and works directly
  against the species-level + Jenkins fallback rows. A future PR that adds
  the ``PLOT.ECOSUBCD → DIVISION`` mapping will re-enable levels 1-2.
- **Phase 2+ pools** (standing dead, understory, downed dead, litter, SOC):
  no skeleton in this PR. Add ``pyfia.carbon.<pool>(db, ...)`` functions
  incrementally as each phase lands, following the same architectural rules.

References
----------
- Westfall, J.A. et al. (2023). GTR-WO-104. DOI: 10.2737/WO-GTR-104
- Woodall, C.W. et al. (2015). GTR-NRS-154 (FCAF methodology blueprint).
- USEPA (2024). NGHGI Annex 3.13.
"""

from __future__ import annotations

from pyfia.carbon.live_tree import LiveTreeEstimator, live_tree

__all__ = ["LiveTreeEstimator", "live_tree"]
