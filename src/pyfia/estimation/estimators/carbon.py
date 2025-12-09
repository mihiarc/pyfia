"""
Carbon pool estimation for FIA data.

Provides unified carbon estimation across all 5 IPCC carbon pools:
- Aboveground live tree (AG)
- Belowground live tree (BG)
- Dead wood (standing dead + down woody material)
- Litter (forest floor litter + duff)
- Soil organic carbon

Uses biomass-to-carbon conversion factor of 0.47 (IPCC standard).
"""

from typing import List, Optional, Union

import polars as pl

from ...core import FIA
from .biomass import biomass

# IPCC standard carbon fraction of dry biomass
CARBON_FRACTION = 0.47


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
    carbon pools following IPCC guidelines. Currently supports live tree
    carbon pools (aboveground and belowground). Dead wood, litter, and
    soil pools require additional FIA tables (COND_DWM_CALC, etc.).

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
        - 'total': Total ecosystem carbon (all pools combined)
          **Note**: Currently returns only live tree carbon.
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
            Carbon per acre (tons C/acre)
        - **CARBON_TOTAL** : float (if totals=True)
            Total carbon expanded to population level (tons C)
        - **CARBON_ACRE_SE** : float (if variance=True)
            Standard error of per-acre estimate
        - **CARBON_TOTAL_SE** : float (if variance=True and totals=True)
            Standard error of total estimate
        - **POOL** : str
            Carbon pool identifier
        - **AREA_TOTAL** : float
            Total area (acres) represented by the estimation
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

    Examples
    --------
    Basic live tree carbon estimation:

    >>> results = carbon(db, pool="live")
    >>> total_c = results['CARBON_TOTAL'][0]
    >>> print(f"Total carbon: {total_c/1e9:.2f} billion tons C")

    Aboveground carbon by ownership:

    >>> results = carbon(db, pool="ag", grp_by="OWNGRPCD")
    >>> for row in results.iter_rows(named=True):
    ...     print(f"Ownership {row['OWNGRPCD']}: {row['CARBON_ACRE']:.2f} tons C/acre")

    Standing dead tree carbon:

    >>> results = carbon(db, pool="dead", tree_type="dead")

    Notes
    -----
    Carbon is calculated as 47% of dry biomass following IPCC guidelines.
    This conversion factor is applied uniformly across all tree components.

    **Current Implementation Status:**

    - Live tree pools (AG, BG, live): Fully implemented using TREE table
    - Standing dead trees: Implemented via tree_type="dead" filter
    - Down woody material: Not yet implemented (requires COND_DWM_CALC)
    - Litter/duff: Not yet implemented (requires DWM evaluation)
    - Soil carbon: Not yet implemented (requires Phase 3 data)

    **EVALIDator Validation:**

    Results can be validated against EVALIDator carbon pool estimates
    using snum codes:
    - 98: Aboveground live tree carbon
    - 99: Belowground live tree carbon
    - 100: Dead wood carbon
    - 101: Litter carbon
    - 102: Soil organic carbon
    - 103: Total forest ecosystem carbon

    Warnings
    --------
    The 'dead', 'litter', 'soil', and 'total' pools are not yet fully
    implemented. Currently 'dead' returns standing dead tree carbon only.
    Full implementation requires additional FIA tables that may not be
    present in all databases.

    Raises
    ------
    ValueError
        If an invalid pool is specified or required tables are not available.
    """
    pool = pool.lower()
    valid_pools = ["ag", "bg", "live", "dead", "litter", "soil", "total"]

    if pool not in valid_pools:
        raise ValueError(
            f"Invalid pool '{pool}'. Must be one of: {', '.join(valid_pools)}"
        )

    # Route to appropriate estimator based on pool
    if pool in ["ag", "bg", "live"]:
        return _estimate_live_tree_carbon(
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
    elif pool == "total":
        # For now, return live tree carbon as proxy
        # Full implementation would sum all pools
        import warnings

        warnings.warn(
            "Total ecosystem carbon not fully implemented. "
            "Returning live tree carbon only. Dead wood, litter, and soil "
            "pools require additional tables.",
            stacklevel=2,
        )
        return _estimate_live_tree_carbon(
            db=db,
            pool="live",
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

    raise ValueError(f"Unhandled pool type: {pool}")


def _estimate_live_tree_carbon(
    db: Union[str, FIA],
    pool: str,
    grp_by: Optional[Union[str, List[str]]],
    by_species: bool,
    land_type: str,
    tree_type: str,
    tree_domain: Optional[str],
    area_domain: Optional[str],
    totals: bool,
    variance: bool,
    most_recent: bool,
) -> pl.DataFrame:
    """Estimate live tree carbon using biomass estimator."""
    # Map pool to biomass component
    component_map = {
        "ag": "AG",
        "bg": "BG",
        "live": "TOTAL",
    }
    component = component_map.get(pool, "TOTAL")

    # Call biomass estimator
    result = biomass(
        db=db,
        component=component,
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

    # Rename columns for carbon context
    rename_map = {}

    # The biomass estimator returns CARB_ACRE and CARB_TOTAL
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
    result = result.with_columns([pl.lit(pool.upper()).alias("POOL")])

    return result


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

    Currently only estimates standing dead tree carbon.
    Full implementation would include down woody material from COND_DWM_CALC.
    """
    import warnings

    warnings.warn(
        "Dead carbon pool currently only includes standing dead trees. "
        "Down woody material (CWD, FWD) requires COND_DWM_CALC table.",
        stacklevel=3,
    )

    # Estimate standing dead tree carbon
    # Use biomass estimator with tree_type="dead"
    result = biomass(
        db=db,
        component="TOTAL",  # AG + BG
        grp_by=grp_by,
        by_species=by_species,
        land_type=land_type,
        tree_type="dead",  # Filter to standing dead trees
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
