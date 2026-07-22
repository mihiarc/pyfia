"""
NSVB-recompute tree carbon estimation for pyFIA (live tree, standing dead).

This subpackage recomputes above-ground tree carbon from scratch using the
National Scale Volume and Biomass framework (NSVB; Westfall et al. 2023,
GTR-WO-104) — the same framework USDA FIA uses to populate the FIADB
``TREE.CARBON_AG`` column for inventories from September 2023 onward — with
species-specific carbon fractions from Tables S10a/S10b.

It is the NSVB-native counterpart to the FIADB-stored tree-carbon path
exposed by :func:`pyfia.estimation.estimators.carbon_pools.carbon_pool`
(which reads the pre-computed ``CARBON_AG`` / ``CARBON_BG`` columns
directly). The two should agree at the tree level for NSVB-era inventories;
the downstream ``forest-carbon`` package uses both for its NSVB-vs-FIADB
reconciliation.

Pools implemented
=================

Live tree
---------
:func:`live_tree` — above-ground live tree carbon via the vectorized NSVB
pipeline (Models 1/2/4/5, 3-level coefficient lookup precedence: Bailey
DIVISION → species-level → Jenkins fallback), with species-specific S10a
carbon fractions. Cull adjustment per Appendix K. BG bridges to FIADB
``TREE.CARBON_BG``.

Validated against FIADB ``TREE.CARBON_AG`` on Georgia EVALID 132401
(130,952 trees): median per-tree relative error 0.085%.

Standing dead
-------------
:func:`standing_dead` — standing dead tree carbon via the same NSVB pipeline
with ``REF_TREE_DECAY_PROP`` decay reductions (DENSITY_PROP × wood,
BARK_LOSS_PROP × bark, BRANCH_LOSS_PROP × branch) and S10b dead carbon
fractions. No ``TREE.CULL`` adjustment for dead trees (per Appendix K).

Broken-top corrections (``ACTUALHT < HT``) apply the Appendix K
crown-proportion adjustment to branch biomass and a paraboloid taper
volume-ratio approximation to wood/bark, using mean intact crown ratios
from Table S11 (``REF_TREE_STND_DEAD_CR_PROP``).

Validated against FIADB on Georgia EVALID 132401 (6,870 trees): median
per-tree relative error 10.89%.

Woodland-species coverage
=========================
NSVB has **no** models for woodland species (``REF_SPECIES.WOODLAND='Y'`` —
pinyon, juniper, mountain-mahogany, Gambel oak; measured at diameter at root
collar, GTR-WO-104 p. 6). Rather than let them recompute to zero — which
collapses interior-West / Great Basin tree carbon — both estimators route
woodland species to the FIADB-stored ``TREE.CARBON_AG`` (FIA's production
legacy/CRM woodland biomass, Woodall et al. 2011). Any *non-woodland* SPCD
that matches neither an NSVB species-level row nor a Jenkins 1-9 group
fallback raises rather than silently zeroing.

Pools deferred
==============
Condition-level pools (understory, downed dead wood, litter, soil organic),
``total_ecosystem``, and stock-change accounting are held pending
domain-verification of their NULL handling and are not part of this
subpackage. The native NSVB belowground coarse-root model (which will
replace the ``TREE.CARBON_BG`` bridge) is also deferred.

Architectural rules
===================
1. **Public API is functions, not classes.** ``live_tree(db, ...)``,
   ``standing_dead(db, ...)``. Match the pyfia convention.

2. **Vectorize coefficient lookups via polars joins.** The production path
   is ``compute_nsvb_biomass`` / ``compute_nsvb_dead_biomass``. An
   independent scalar reimplementation (``predict_tree_biomass`` and the
   dict/`lookup_coefficients` reference loaders) is kept only as a test
   oracle and lives in ``tests/nsvb_oracle.py``, not in the shipped
   library (issue #127).

3. **Inherit from ``CarbonEstimatorBase``** (which itself inherits the
   pyfia ``BaseEstimator`` template method: ``load_data → apply_filters →
   calculate_values → aggregate_results → calculate_variance →
   format_output``).

4. **Match the ``mortality()`` docstring quality.**

5. **Bridge BG carbon to FIADB ``CARBON_BG`` for now.** The bridge is
   acknowledged tech debt; a native NSVB root model will replace it.

References
----------
- Westfall, J.A. et al. (2023). GTR-WO-104. DOI: 10.2737/WO-GTR-104
- Harmon, M.E. et al. (2011). GTR-WO-104 Table 1 (dead-tree density).
- Woodall, C.W. et al. (2011). GTR-NRS-88 (legacy/CRM woodland biomass).
"""

from __future__ import annotations

from pyfia.carbon.live_tree import LiveTreeEstimator, live_tree
from pyfia.carbon.standing_dead import StandingDeadEstimator, standing_dead

__all__ = [
    "live_tree",
    "standing_dead",
    "LiveTreeEstimator",
    "StandingDeadEstimator",
]
