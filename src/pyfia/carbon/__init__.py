"""
Forest carbon estimation for pyFIA — all six IPCC/NGHGI reporting pools.

This subpackage implements forest carbon stock estimation across the IPCC and
NGHGI reporting pools.  Tree-level pools (live tree, standing dead) use the
National Scale Volume and Biomass framework (NSVB; Westfall et al. 2023,
GTR-WO-104) with species-specific carbon fractions.  Condition-level pools
(understory, downed dead wood, litter, soil organic carbon) read pre-computed
attributes from the FIADB COND table.  :func:`total_ecosystem` sums all six
pools into a single estimate.

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

Understory vegetation
---------------------
:func:`understory` — understory vegetation carbon (seedlings + woody
shrubs < 2.54 cm DBH) via the Smith & Heath (2008) model, reading
pre-computed ``COND.CARBON_UNDERSTORY_AG`` and ``COND.CARBON_UNDERSTORY_BG``
from the FIADB.  The model descends from the Birdsey (1996) ratios by
forest type group and region; the AG/BG split is 90/10 (Smith et al. 2006).

This is a **condition-level** estimator (no TREE table); it inherits from
``BaseEstimator`` directly rather than ``CarbonEstimatorBase``.

Validated against manual SQL replication on Georgia EVALID 132301:
population totals match exactly.

Downed dead wood
-----------------
:func:`downed_dead` — downed dead wood (coarse woody debris) carbon via
the Domke et al. (2013) model, reading pre-computed
``COND.CARBON_DOWN_DEAD`` from the FIADB.  No AG/BG split.

This is a **condition-level** estimator (no TREE table); it inherits from
``BaseEstimator`` directly rather than ``CarbonEstimatorBase``.

Validated against manual SQL replication on Georgia EVALID 132301:
population totals match exactly.

Litter
------
:func:`litter` — litter and duff carbon via the Domke et al. (2016)
model, reading pre-computed ``COND.CARBON_LITTER`` from the FIADB.
No AG/BG split.

This is a **condition-level** estimator (no TREE table); it inherits from
``BaseEstimator`` directly rather than ``CarbonEstimatorBase``.

Validated against manual SQL replication on Georgia EVALID 132301:
population totals match exactly.

Soil organic carbon
-------------------
:func:`soil_organic` — soil organic carbon (mineral soil to 1 m depth)
via the Domke et al. (2017) model, reading pre-computed
``COND.CARBON_SOIL_ORG`` from the FIADB.  No AG/BG split.

This is a **condition-level** estimator (no TREE table); it inherits from
``BaseEstimator`` directly rather than ``CarbonEstimatorBase``.

Validated against manual SQL replication on Georgia EVALID 132301:
population totals match exactly.

Total ecosystem
---------------
:func:`total_ecosystem` — convenience function that estimates all six pools
independently and sums per-acre and population totals.  The result contains
one row per pool plus a ``TOTAL_ECOSYSTEM`` summary row.

Validated against sum of individual pool calls on Georgia EVALID 132301:
exact match (0.00 difference).

Pools deferred
==============
- Native NSVB belowground coarse-root model (replaces the BG bridge)

Architectural rules
===================
1. **Public API is functions, not classes.** ``live_tree(db, ...)``,
   ``standing_dead(db, ...)``. Match the pyfia convention.

2. **Vectorize coefficient lookups via polars joins.** The scalar
   ``predict_tree_biomass`` is a test oracle only. The production path
   is ``compute_nsvb_biomass`` / ``compute_nsvb_dead_biomass``.

3. **Inherit from ``CarbonEstimatorBase`` (tree-level) or ``BaseEstimator``
   (condition-level).** New pool estimators must follow the template-method
   pattern (``load_data → apply_filters → calculate_values →
   aggregate_results → calculate_variance → format_output``).

4. **Match the ``mortality()`` docstring quality.**

5. **Bridge BG carbon to FIADB ``CARBON_BG`` for now.** The bridge is
   acknowledged tech debt; a native NSVB root model will replace it.

References
----------
- Westfall, J.A. et al. (2023). GTR-WO-104. DOI: 10.2737/WO-GTR-104
- Woodall, C.W. et al. (2015). GTR-NRS-154 (FCAF methodology blueprint).
- Harmon, M.E. et al. (2011). GTR-WO-104 Table 1 (dead-tree density).
- Smith, J.E. et al. (2006). GTR-NE-343 (understory yield tables).
- Smith, J.E.; Heath, L.S. (2008). GTR-NRS-13 (FIADB carbon attributes).
- Birdsey, R.A. (1996). Forests and Global Change Vol. 2 (understory ratios).
- Domke, G.M. et al. (2013). Forest Ecol. Manage. 292, 50-57 (downed dead).
- Domke, G.M. et al. (2016). Sci. Total Environ. 557-558, 469-478 (litter).
- Domke, G.M. et al. (2017). Ecol. Appl. 27(4), 1223-1235 (soil organic C).
- USEPA (2024). NGHGI Annex 3.13.
"""

from __future__ import annotations

from pyfia.carbon.downed_dead import DownedDeadEstimator, downed_dead
from pyfia.carbon.litter import LitterEstimator, litter
from pyfia.carbon.live_tree import LiveTreeEstimator, live_tree
from pyfia.carbon.soil_organic import SoilOrganicEstimator, soil_organic
from pyfia.carbon.standing_dead import StandingDeadEstimator, standing_dead
from pyfia.carbon.stock_change import CarbonStockChangeEstimator, stock_change
from pyfia.carbon.total_ecosystem import total_ecosystem
from pyfia.carbon.understory import UnderstoryEstimator, understory

__all__ = [
    "downed_dead",
    "litter",
    "live_tree",
    "soil_organic",
    "standing_dead",
    "stock_change",
    "total_ecosystem",
    "understory",
    "CarbonStockChangeEstimator",
    "DownedDeadEstimator",
    "LitterEstimator",
    "LiveTreeEstimator",
    "SoilOrganicEstimator",
    "StandingDeadEstimator",
    "UnderstoryEstimator",
]
