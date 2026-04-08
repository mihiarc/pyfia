"""
NSVB-based forest carbon estimation for pyFIA.

This subpackage implements forest carbon stock estimation across the IPCC and
NGHGI reporting pools using the National Scale Volume and Biomass framework
(NSVB; Westfall et al. 2023, GTR-WO-104) for tree-level biomass and species-
specific carbon fractions for the carbon conversion. The full design is
documented in ``pyfia_carbon_tech_spec.md`` (external) — this package is the
estimation engine for the Schmidt Sciences "Synthetic Inventory" project, a
publicly auditable Python reconstruction of the U.S. NGHGI LULUCF forest
carbon time series.

**Phase 1 status (current):** ``CarbonEstimator`` is a skeleton class. Only
the equation library, coefficient loaders, and carbon-fraction lookups under
``pyfia.carbon.nsvb`` are implemented. The pool methods (``live_tree``,
``standing_dead``, etc.) all raise ``NotImplementedError`` — they will be
wired up in subsequent PRs:

- PR 2 (``feat/carbon-nsvb-live-tree-estimator``): wires ``live_tree``.
- Phase 2 PRs: ``standing_dead``, ``understory``, ``downed_dead``.
- Phase 3 PRs: ``litter``, ``soil_organic_carbon``.
- Phase 4 PRs: ``stock_change``, ``attribute``.

Until ``live_tree`` is implemented, users wanting carbon pool estimates
should continue to use the existing
``pyfia.estimation.estimators.carbon_pools.CarbonPoolEstimator``, which
reads FIA's pre-computed ``CARBON_AG``/``CARBON_BG`` columns directly.

References
----------
- Westfall, J.A. et al. (2023). GTR-WO-104. DOI: 10.2737/WO-GTR-104
- Woodall, C.W. et al. (2015). GTR-NRS-154 (FCAF methodology blueprint).
- USEPA (2024). NGHGI Annex 3.13.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyfia.core.fia import FIA


class CarbonEstimator:
    """Public unified entry point for ``pyfia.carbon`` pool estimation.

    Phase 1 of ``pyfia.carbon`` only ships the NSVB equation library; the
    pool methods on this class all raise ``NotImplementedError`` and will
    be wired up incrementally as later PRs land. The class exists now so
    downstream code can import it stably and so test fixtures, docstrings,
    and the public API surface can be reviewed before any pool implementation
    is in place.

    Parameters
    ----------
    db : str | FIA
        Database connection or path to a FIADB file. Same shape as the
        existing ``pyfia.biomass()`` and other estimator entry points.
    year : int, optional
        Optional reference year override. If omitted, the estimator infers
        the year from the loaded EVALID.

    Examples
    --------
    >>> from pyfia import FIA
    >>> from pyfia.carbon import CarbonEstimator
    >>> fia = FIA("data/ga_2023.duckdb")  # doctest: +SKIP
    >>> fia.clip_most_recent()  # doctest: +SKIP
    >>> ce = CarbonEstimator(fia)  # doctest: +SKIP
    >>> # Phase 1 wired up in PR 2:
    >>> # result = ce.live_tree(by_species=True, pool="ag")  # doctest: +SKIP
    """

    def __init__(self, db: "str | FIA", *, year: int | None = None) -> None:
        self.db = db
        self.year = year

    def live_tree(self, **kwargs: Any) -> Any:
        """Estimate live tree carbon (NSVB AGB + FIADB-bridge BG).

        **Not yet implemented.** Wired up in PR 2 of the
        ``feat/carbon-nsvb-*`` series. The arguments will mirror the existing
        ``pyfia.biomass()`` function (``grp_by``, ``by_species``,
        ``by_size_class``, ``land_type``, ``tree_domain``, ``area_domain``,
        ``totals``, ``variance``, ``most_recent``, plus ``pool='ag'|'bg'|'total'``).
        """
        raise NotImplementedError(
            "CarbonEstimator.live_tree arrives in PR 2 of pyfia.carbon Phase 1 "
            "(branch: feat/carbon-nsvb-live-tree-estimator). The current PR ships "
            "only the NSVB equation library."
        )

    def standing_dead(self, **kwargs: Any) -> Any:
        """Estimate standing dead tree carbon (NSVB with decay/structural loss).

        **Not yet implemented.** Arrives in Phase 2 — see
        ``pyfia_carbon_tech_spec.md`` section 3.2.
        """
        raise NotImplementedError(
            "CarbonEstimator.standing_dead arrives in Phase 2. "
            "See pyfia_carbon_tech_spec.md section 3.2."
        )

    def understory(self, **kwargs: Any) -> Any:
        """Estimate understory vegetation carbon (Birdsey 1996 ratios).

        **Not yet implemented.** Arrives in Phase 2 — see
        ``pyfia_carbon_tech_spec.md`` section 3.3.
        """
        raise NotImplementedError(
            "CarbonEstimator.understory arrives in Phase 2. "
            "See pyfia_carbon_tech_spec.md section 3.3."
        )

    def downed_dead(self, **kwargs: Any) -> Any:
        """Estimate downed dead wood carbon (Domke et al. 2013 hybrid).

        **Not yet implemented.** Arrives in Phase 2 — see
        ``pyfia_carbon_tech_spec.md`` section 3.4.
        """
        raise NotImplementedError(
            "CarbonEstimator.downed_dead arrives in Phase 2. "
            "See pyfia_carbon_tech_spec.md section 3.4."
        )

    def litter(self, **kwargs: Any) -> Any:
        """Estimate litter carbon (Domke et al. 2016 Random Forest model).

        **Not yet implemented.** Arrives in Phase 3 — see
        ``pyfia_carbon_tech_spec.md`` section 3.5.
        """
        raise NotImplementedError(
            "CarbonEstimator.litter arrives in Phase 3. "
            "See pyfia_carbon_tech_spec.md section 3.5."
        )

    def soil_organic_carbon(self, **kwargs: Any) -> Any:
        """Estimate soil organic carbon (Domke et al. 2017 two-phase RF model).

        **Not yet implemented.** Arrives in Phase 3 — see
        ``pyfia_carbon_tech_spec.md`` section 3.6.
        """
        raise NotImplementedError(
            "CarbonEstimator.soil_organic_carbon arrives in Phase 3. "
            "See pyfia_carbon_tech_spec.md section 3.6."
        )

    def stock_change(self, **kwargs: Any) -> Any:
        """Estimate carbon stock change between inventory periods.

        **Not yet implemented.** Arrives in Phase 4 — see
        ``pyfia_carbon_tech_spec.md`` section 4.
        """
        raise NotImplementedError(
            "CarbonEstimator.stock_change arrives in Phase 4. "
            "See pyfia_carbon_tech_spec.md section 4."
        )

    def attribute(self, **kwargs: Any) -> Any:
        """Attribute carbon stock change to disturbance categories.

        **Not yet implemented.** Arrives in Phase 4 — see
        ``pyfia_carbon_tech_spec.md`` section 5.
        """
        raise NotImplementedError(
            "CarbonEstimator.attribute arrives in Phase 4. "
            "See pyfia_carbon_tech_spec.md section 5."
        )


__all__ = ["CarbonEstimator"]
