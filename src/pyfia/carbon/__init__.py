"""
NSVB-based forest carbon estimation for pyFIA.

This subpackage implements forest carbon stock estimation across the IPCC and
NGHGI reporting pools using the National Scale Volume and Biomass framework
(NSVB; Westfall et al. 2023, GTR-WO-104) for tree-level biomass and species-
specific carbon fractions for the carbon conversion.

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

Pools deferred
==============
Each additional pool will land as its own ``pyfia.carbon.<pool>(db, ...)``
function following the architectural rules below.

- Understory (Birdsey 1992 ratios, EPA NGHGI Annex 3.13)
- Downed dead wood (Domke et al. 2013)
- Litter + duff (Domke et al. 2016)
- Soil organic carbon (Domke et al. 2017)
- Native NSVB belowground coarse-root model (replaces the BG bridge)

Architectural rules
===================
1. **Public API is functions, not classes.** ``live_tree(db, ...)``,
   ``standing_dead(db, ...)``. Match the pyfia convention.

2. **Vectorize coefficient lookups via polars joins.** The scalar
   ``predict_tree_biomass`` is a test oracle only. The production path
   is ``compute_nsvb_biomass`` / ``compute_nsvb_dead_biomass``.

3. **Inherit from ``CarbonEstimatorBase``.** New pool estimators must
   follow the template-method pattern (``load_data → apply_filters →
   calculate_values → aggregate_results → calculate_variance →
   format_output``).

4. **Match the ``mortality()`` docstring quality.**

5. **Bridge BG carbon to FIADB ``CARBON_BG`` for now.** The bridge is
   acknowledged tech debt; a native NSVB root model will replace it.

References
----------
- Westfall, J.A. et al. (2023). GTR-WO-104. DOI: 10.2737/WO-GTR-104
- Woodall, C.W. et al. (2015). GTR-NRS-154 (FCAF methodology blueprint).
- Harmon, M.E. et al. (2011). GTR-WO-104 Table 1 (dead-tree density).
- USEPA (2024). NGHGI Annex 3.13.
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
