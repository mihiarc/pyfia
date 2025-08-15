"""
High-level mortality estimation function for pyFIA.

This module provides the main mortality() function that serves as the
primary interface for mortality estimation in pyFIA.
"""

from typing import List, Optional, Union

import polars as pl

from ...core import FIA
from ..config import MortalityConfig
from ..mortality_lazy import mortality_lazy


def mortality(
    db: Union[str, FIA],
    config: MortalityConfig = None,
    by_species: bool = None,
    by_size_class: bool = None,
    tree_domain: Optional[str] = None,
    area_domain: Optional[str] = None,
    tree_class: str = None,
    land_type: str = None,
    grp_by: Optional[Union[str, List[str]]] = None,
    totals: bool = None,
    variance: bool = None,
    by_species_group: bool = None,
    by_ownership: bool = None,
    by_agent: bool = None,
    by_disturbance: bool = None,
    include_components: bool = None,
    mortality_type: str = None,
    include_natural: bool = None,
    include_harvest: bool = None,
    variance_method: str = None,
) -> pl.DataFrame:
    """
    Estimate tree mortality using FIA data.
    
    This function implements design-based estimation of tree mortality
    following Bechtold & Patterson (2005) procedures. It supports various
    grouping variables and calculates mortality in terms of both trees
    per acre (TPA) and volume.
    
    Parameters
    ----------
    db : Union[str, FIA]
        FIA database instance or path to database
    config : MortalityConfig, optional
        Configuration for mortality estimation. If provided, individual
        parameters will override config values when specified.
    by_species : bool, optional
        Include species-level grouping (SPCD). Defaults to False.
    by_size_class : bool, optional
        Include size class grouping. Defaults to False.
    tree_domain : str, optional
        SQL filter for tree selection (e.g., "SPCD == 131")
    area_domain : str, optional
        SQL filter for area selection (e.g., "STATECD == 48")
    tree_class : str, optional
        Tree class filter: "growing_stock", "all", or "timber". 
        Defaults to "all".
    land_type : str, optional
        Land type filter: "forest", "timber", or "all". 
        Defaults to "forest".
    grp_by : str or List[str], optional
        Additional grouping columns (e.g., ["UNITCD", "COUNTYCD"])
    totals : bool, optional
        Include total estimates in addition to per-acre values.
        Defaults to False.
    variance : bool, optional
        Return variance instead of standard error. Defaults to False.
    by_species_group : bool, optional
        Include species group grouping (SPGRPCD). Defaults to False.
    by_ownership : bool, optional
        Include ownership group grouping (OWNGRPCD). Defaults to False.
    by_agent : bool, optional
        Include mortality agent grouping (AGENTCD). Defaults to False.
    by_disturbance : bool, optional
        Include disturbance code grouping (DSTRBCD1, DSTRBCD2, DSTRBCD3).
        Defaults to False.
    include_components : bool, optional
        Include basal area and volume mortality components. 
        Defaults to False.
    mortality_type : str, optional
        Type of mortality to calculate: "tpa", "volume", or "both".
        Defaults to "tpa".
    include_natural : bool, optional
        Include natural mortality in calculations. Defaults to True.
    include_harvest : bool, optional
        Include harvest mortality in calculations. Defaults to True.
    variance_method : str, optional
        Variance calculation method: "standard", "ratio", or "hybrid".
        Defaults to "ratio".
        
    Returns
    -------
    pl.DataFrame
        DataFrame containing mortality estimates with the following columns:
        - Grouping columns (if specified)
        - MORTALITY_TPA: Trees per acre mortality
        - MORTALITY_BA: Basal area mortality (if include_components=True)
        - MORTALITY_VOL: Volume mortality (if mortality_type includes volume)
        - Standard errors or variances
        - N_PLOTS: Number of plots in estimate
        - YEAR: Inventory year
        
    Examples
    --------
    >>> from pyfia import FIA
    >>> from pyfia.estimation.config import MortalityConfig
    >>> 
    >>> # Initialize database
    >>> db = FIA("fia.duckdb")
    >>> 
    >>> # Config-based usage
    >>> config = MortalityConfig(
    ...     grp_by=["SPCD", "OWNGRPCD"],
    ...     mortality_type="both",
    ...     tree_class="all",
    ...     totals=True,
    ...     variance=True
    ... )
    >>> results = mortality(db, config)
    >>> 
    >>> # Parameter-based usage (backward compatible)
    >>> results = mortality(
    ...     db, 
    ...     by_species=True,
    ...     by_ownership=True,
    ...     mortality_type="both",
    ...     variance=True
    ... )
    >>> 
    >>> # Mixed usage (parameters override config)
    >>> config = MortalityConfig(mortality_type="tpa")
    >>> results = mortality(
    ...     db,
    ...     config,
    ...     mortality_type="both",  # This overrides config
    ...     by_species=True
    ... )
    """
    # Delegate to lazy implementation for improved performance
    # This maintains backward compatibility while leveraging lazy evaluation
    return mortality_lazy(
        db=db,
        config=config,
        by_species=by_species,
        by_size_class=by_size_class,
        tree_domain=tree_domain,
        area_domain=area_domain,
        tree_class=tree_class,
        land_type=land_type,
        grp_by=grp_by,
        totals=totals,
        variance=variance,
        by_species_group=by_species_group,
        by_ownership=by_ownership,
        by_agent=by_agent,
        by_disturbance=by_disturbance,
        include_components=include_components,
        mortality_type=mortality_type,
        include_natural=include_natural,
        include_harvest=include_harvest,
        variance_method=variance_method,
        show_progress=True,  # Default to showing progress
    )

