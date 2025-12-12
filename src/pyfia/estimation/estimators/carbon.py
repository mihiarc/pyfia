"""
Carbon pool estimation for FIA data.

Provides unified carbon estimation across all 5 IPCC carbon pools:
- Aboveground live tree (AG)
- Belowground live tree (BG)
- Dead wood (standing dead + down woody material)
- Litter (forest floor litter + duff)
- Soil organic carbon

Live tree carbon estimation uses FIA's pre-calculated CARBON_AG and CARBON_BG
columns which incorporate species-specific carbon conversion factors. This
matches EVALIDator snum=55000 exactly.
"""

from typing import List, Optional, Union

import polars as pl

from ...core import FIA
from .biomass import biomass
from .carbon_pools import carbon_pool


def carbon(
    db: Union[str, FIA],
    pool: str = "live",
    grp_by: Optional[Union[str, List[str]]] = None,
    by_species: bool = False,
    land_type: str = "forest",
    tree_type: str = "live",
    tree_domain: Optional[str] = None,
    area_domain: Optional[str] = None,
    totals: bool = True,
    variance: bool = False,
    most_recent: bool = False,
) -> pl.DataFrame:
    """
    Estimate carbon stocks by pool from FIA data.

    Provides unified access to forest carbon estimation across different
    carbon pools following IPCC guidelines. Live tree carbon pools use FIA's
    pre-calculated CARBON_AG and CARBON_BG columns with species-specific
    conversion factors, matching EVALIDator snum=55000 exactly.

    Parameters
    ----------
    db : Union[str, FIA]
        Database connection or path to FIA database. Can be either a path
        string to a DuckDB/SQLite file or an existing FIA connection object.
    pool : {'ag', 'bg', 'live', 'dead', 'litter', 'soil', 'total'}, default 'live'
        Carbon pool to estimate:

        - 'ag': Aboveground live tree carbon (stems, branches, foliage)
        - 'bg': Belowground live tree carbon (roots)
        - 'live': Total live tree carbon (AG + BG) - default
        - 'dead': Dead wood carbon (standing dead + down woody material)
          **Note**: Currently only supports standing dead trees. Full DWM
          requires COND_DWM_CALC table with EXPDWM evaluation type.
        - 'litter': Forest floor carbon (litter + duff)
          **Note**: Not yet implemented. Requires DWM evaluation data.
        - 'soil': Soil organic carbon
          **Note**: Not yet implemented. Requires Phase 3 plot data.
        - 'total': Total live tree carbon (alias for 'live')
    grp_by : str or list of str, optional
        Column name(s) to group results by. Common grouping columns include:

        - 'FORTYPCD': Forest type code
        - 'OWNGRPCD': Ownership group
        - 'STATECD': State FIPS code
        - 'COUNTYCD': County code

        For complete column descriptions, see USDA FIA Database User Guide.
    by_species : bool, default False
        If True, group results by species code (SPCD). Only applicable for
        live tree and standing dead pools.
    land_type : {'forest', 'timber'}, default 'forest'
        Land type to include in estimation:

        - 'forest': All forestland
        - 'timber': Productive timberland only (unreserved, productive)
    tree_type : {'live', 'dead', 'all'}, default 'live'
        Tree type to include (for tree-based pools):

        - 'live': Live trees only (STATUSCD = 1)
        - 'dead': Standing dead trees only (STATUSCD = 2)
        - 'all': All trees
    tree_domain : str, optional
        SQL-like filter expression for tree-level filtering.
        Example: "DIA >= 10.0 AND SPCD == 131".
    area_domain : str, optional
        SQL-like filter expression for area/condition-level filtering.
        Example: "OWNGRPCD == 40 AND FORTYPCD == 161".
    totals : bool, default True
        If True, include population-level total estimates in addition to
        per-acre values.
    variance : bool, default False
        If True, calculate and include variance and standard error estimates.
    most_recent : bool, default False
        If True, automatically filter to the most recent evaluation for
        each state in the database.

    Returns
    -------
    pl.DataFrame
        Carbon estimates with the following columns:

        - **CARBON_ACRE** : float
            Carbon per acre (short tons/acre)
        - **CARBON_TOTAL** : float (if totals=True)
            Total carbon expanded to population level (short tons)
        - **CARBON_ACRE_SE** : float (if variance=True)
            Standard error of per-acre estimate
        - **CARBON_TOTAL_SE** : float (if variance=True and totals=True)
            Standard error of total estimate
        - **POOL** : str
            Carbon pool identifier
        - **N_PLOTS** : int
            Number of FIA plots included in the estimation
        - **YEAR** : int
            Representative year for the estimation
        - **[grouping columns]** : various
            Any columns specified in grp_by or from by_species

    See Also
    --------
    biomass : Estimate tree biomass (dry weight in tons)
    carbon_flux : Estimate net carbon flux (growth - mortality - removals)
    carbon_pool : Direct access to CarbonPoolEstimator

    Examples
    --------
    Basic live tree carbon estimation:

    >>> results = carbon(db, pool="live")
    >>> total_c = results['CARBON_TOTAL'][0]
    >>> print(f"Total carbon: {total_c/1e6:.2f} million short tons")

    Aboveground carbon by ownership:

    >>> results = carbon(db, pool="ag", grp_by="OWNGRPCD")
    >>> for row in results.iter_rows(named=True):
    ...     print(f"Ownership {row['OWNGRPCD']}: {row['CARBON_ACRE']:.2f} tons/acre")

    Standing dead tree carbon:

    >>> results = carbon(db, pool="dead")

    Notes
    -----
    Live tree carbon (pools: 'ag', 'bg', 'live', 'total') uses FIA's
    pre-calculated CARBON_AG and CARBON_BG columns which incorporate
    species-specific carbon conversion factors. This matches EVALIDator
    snum=55000 exactly.

    Dead tree carbon currently uses biomass-derived estimation (47% of dry
    biomass) as FIA does not provide pre-calculated carbon for dead trees.

    **Current Implementation Status:**

    - Live tree pools (AG, BG, live, total): Fully implemented using CARBON columns
    - Standing dead trees: Implemented using biomass-derived carbon
    - Down woody material: Not yet implemented (requires COND_DWM_CALC)
    - Litter/duff: Not yet implemented (requires DWM evaluation)
    - Soil carbon: Not yet implemented (requires Phase 3 data)

    **EVALIDator Validation:**

    Results can be validated against EVALIDator carbon pool estimates:
    - snum=55000: Total live tree carbon (AG + BG) - matches exactly

    Raises
    ------
    ValueError
        If an invalid pool is specified.
    NotImplementedError
        If 'litter' or 'soil' pools are requested.
    """
    pool = pool.lower()
    valid_pools = ["ag", "bg", "live", "dead", "litter", "soil", "total"]

    if pool not in valid_pools:
        raise ValueError(
            f"Invalid pool '{pool}'. Must be one of: {', '.join(valid_pools)}"
        )

    # Route to appropriate estimator based on pool
    if pool in ["ag", "bg", "total"]:
        # Use FIA's pre-calculated CARBON_AG and CARBON_BG columns
        return carbon_pool(
            db=db,
            pool=pool,
            grp_by=grp_by,
            by_species=by_species,
            land_type=land_type,
            tree_type=tree_type,
            tree_domain=tree_domain,
            area_domain=area_domain,
            totals=totals,
            variance=variance,
            most_recent=most_recent,
        )
    elif pool == "live":
        # "live" is an alias for "total" (AG + BG)
        return carbon_pool(
            db=db,
            pool="total",
            grp_by=grp_by,
            by_species=by_species,
            land_type=land_type,
            tree_type=tree_type,
            tree_domain=tree_domain,
            area_domain=area_domain,
            totals=totals,
            variance=variance,
            most_recent=most_recent,
        )
    elif pool == "dead":
        return _estimate_dead_carbon(
            db=db,
            grp_by=grp_by,
            by_species=by_species,
            land_type=land_type,
            tree_domain=tree_domain,
            area_domain=area_domain,
            totals=totals,
            variance=variance,
            most_recent=most_recent,
        )
    elif pool == "litter":
        raise NotImplementedError(
            "Litter carbon pool estimation not yet implemented. "
            "Requires COND_DWM_CALC table with EXPDWM evaluation type."
        )
    elif pool == "soil":
        raise NotImplementedError(
            "Soil organic carbon estimation not yet implemented. "
            "Requires SUBP_SOIL_SAMPLE_LOC table (Phase 3 data)."
        )

    raise ValueError(f"Unhandled pool type: {pool}")


def _estimate_dead_carbon(
    db: Union[str, FIA],
    grp_by: Optional[Union[str, List[str]]],
    by_species: bool,
    land_type: str,
    tree_domain: Optional[str],
    area_domain: Optional[str],
    totals: bool,
    variance: bool,
    most_recent: bool,
) -> pl.DataFrame:
    """
    Estimate dead wood carbon.

    Currently only estimates standing dead tree carbon using biomass-derived
    carbon (47% of dry biomass). FIA does not provide pre-calculated carbon
    columns for dead trees.

    Full implementation would include down woody material from COND_DWM_CALC.
    """
    import warnings

    warnings.warn(
        "Dead carbon pool currently only includes standing dead trees. "
        "Down woody material (CWD, FWD) requires COND_DWM_CALC table.",
        stacklevel=3,
    )

    # Estimate standing dead tree carbon using biomass estimator
    result = biomass(
        db=db,
        component="TOTAL",
        grp_by=grp_by,
        by_species=by_species,
        land_type=land_type,
        tree_type="dead",
        tree_domain=tree_domain,
        area_domain=area_domain,
        totals=totals,
        variance=variance,
        most_recent=most_recent,
    )

    # Rename columns for carbon context
    rename_map = {}
    if "CARB_ACRE" in result.columns:
        rename_map["CARB_ACRE"] = "CARBON_ACRE"
    if "CARB_TOTAL" in result.columns:
        rename_map["CARB_TOTAL"] = "CARBON_TOTAL"
    if "CARB_ACRE_SE" in result.columns:
        rename_map["CARB_ACRE_SE"] = "CARBON_ACRE_SE"
    if "CARB_TOTAL_SE" in result.columns:
        rename_map["CARB_TOTAL_SE"] = "CARBON_TOTAL_SE"

    if rename_map:
        result = result.rename(rename_map)

    # Add pool identifier
    result = result.with_columns([pl.lit("DEAD").alias("POOL")])

    return result
