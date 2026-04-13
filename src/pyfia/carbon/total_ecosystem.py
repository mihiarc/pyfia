"""
Total ecosystem carbon estimation — sum of all six IPCC/NGHGI pools.

Combines live tree, standing dead, understory vegetation, downed dead
wood, litter, and soil organic carbon into a single total ecosystem
estimate.  Each pool is estimated independently via its own estimator,
then the per-acre and population totals are summed.

The summed total can be validated against EVALIDator snum=103 ("Total
all pools") and snum=97 ("Total forest carbon").

Public API: :func:`total_ecosystem`.
"""

from __future__ import annotations

import logging

import polars as pl

from ..core import FIA
from ..estimation.utils import ensure_evalid_set, ensure_fia_instance

logger = logging.getLogger(__name__)


def total_ecosystem(
    db: str | FIA,
    land_type: str = "forest",
    area_domain: str | None = None,
    plot_domain: str | None = None,
    totals: bool = True,
    variance: bool = False,
    most_recent: bool = False,
) -> pl.DataFrame:
    """
    Estimate total ecosystem carbon across all six IPCC/NGHGI pools.

    Sums per-acre and population total estimates from:

    1. Live tree (AG + BG)
    2. Standing dead (AG + BG)
    3. Understory vegetation (AG + BG)
    4. Downed dead wood
    5. Litter
    6. Soil organic carbon

    Each pool is estimated independently, then results are summed into
    a single row.  This produces the total forest ecosystem carbon stock
    comparable to EVALIDator snum=103 ("Total all pools").

    Parameters
    ----------
    db : str | FIA
        Database connection or path to FIA database.
    land_type : {'forest', 'timber', 'all'}, default 'forest'
        Land type to include in estimation.
    area_domain : str, optional
        SQL-like filter expression for area/condition-level filtering.
    plot_domain : str, optional
        SQL-like filter expression for plot-level filtering.
    totals : bool, default True
        If True, include population-level total estimates.
    variance : bool, default False
        If True, calculate and include standard error estimates.
    most_recent : bool, default False
        If True, automatically filter to the most recent evaluation.

    Returns
    -------
    pl.DataFrame
        Total ecosystem carbon with one row per pool plus a TOTAL row.
        Columns: POOL, CARBON_ACRE, CARBON_TOTAL (if totals), and
        SE columns (if variance).

    See Also
    --------
    live_tree : Live tree carbon.
    standing_dead : Standing dead tree carbon.
    understory : Understory vegetation carbon.
    downed_dead : Downed dead wood carbon.
    litter : Litter carbon.
    soil_organic : Soil organic carbon.

    Examples
    --------
    >>> results = total_ecosystem(db)
    >>> total_row = results.filter(pl.col("POOL") == "TOTAL_ECOSYSTEM")
    >>> print(f"Total: {total_row['CARBON_ACRE'][0]:.2f} tons/acre")
    """
    from ..validation import (
        validate_boolean,
        validate_domain_expression,
        validate_land_type,
    )
    from .downed_dead import downed_dead
    from .litter import litter
    from .live_tree import live_tree
    from .soil_organic import soil_organic
    from .standing_dead import standing_dead
    from .understory import understory

    # ----- Validate inputs -----
    land_type = validate_land_type(land_type)
    area_domain = validate_domain_expression(area_domain, "area_domain")
    plot_domain = validate_domain_expression(plot_domain, "plot_domain")
    totals = validate_boolean(totals, "totals")
    variance = validate_boolean(variance, "variance")
    most_recent = validate_boolean(most_recent, "most_recent")

    # ----- Resolve db + EVALID -----
    db, owns_db = ensure_fia_instance(db)
    if most_recent and db.evalid is None:
        db.clip_most_recent(eval_type="VOL")
    else:
        ensure_evalid_set(db, eval_type="VOL", estimator_name="total_ecosystem")

    common_kwargs: dict = {
        "land_type": land_type,
        "area_domain": area_domain,
        "plot_domain": plot_domain,
        "totals": totals,
        "variance": variance,
    }

    try:
        # Estimate each pool
        pool_results = []

        lt = live_tree(db, pool="total", **common_kwargs)
        lt = lt.with_columns(pl.lit("LIVE_TREE").alias("POOL"))
        pool_results.append(lt)

        sd = standing_dead(db, pool="total", **common_kwargs)
        sd = sd.with_columns(pl.lit("STANDING_DEAD").alias("POOL"))
        pool_results.append(sd)

        us = understory(db, pool="total", **common_kwargs)
        us = us.with_columns(pl.lit("UNDERSTORY").alias("POOL"))
        pool_results.append(us)

        dd = downed_dead(db, pool="total", **common_kwargs)
        dd = dd.with_columns(pl.lit("DOWNED_DEAD").alias("POOL"))
        pool_results.append(dd)

        li = litter(db, pool="total", **common_kwargs)
        li = li.with_columns(pl.lit("LITTER").alias("POOL"))
        pool_results.append(li)

        so = soil_organic(db, pool="total", **common_kwargs)
        so = so.with_columns(pl.lit("SOIL_ORGANIC").alias("POOL"))
        pool_results.append(so)

        # Build summary columns
        sum_cols = ["CARBON_ACRE"]
        if totals:
            sum_cols.append("CARBON_TOTAL")

        # Compute total row by summing across pools
        total_acre = sum(
            float(r["CARBON_ACRE"][0]) for r in pool_results
        )
        total_row: dict = {
            "POOL": ["TOTAL_ECOSYSTEM"],
            "CARBON_ACRE": [total_acre],
        }

        if totals:
            total_total = sum(
                float(r["CARBON_TOTAL"][0]) for r in pool_results
            )
            total_row["CARBON_TOTAL"] = [total_total]

        # Extract YEAR from first pool result
        if "YEAR" in pool_results[0].columns:
            total_row["YEAR"] = [pool_results[0]["YEAR"][0]]

        total_df = pl.DataFrame(total_row)

        # Normalize columns across all pool results and total
        all_frames = pool_results + [total_df]
        all_cols = set()
        for frame in all_frames:
            all_cols.update(frame.columns)

        # Add missing columns as null to each frame
        normalized = []
        for frame in all_frames:
            for col in all_cols:
                if col not in frame.columns:
                    frame = frame.with_columns(pl.lit(None).alias(col))
            normalized.append(frame)

        result = pl.concat(normalized, how="diagonal_relaxed")

        # Order columns
        col_order = ["YEAR", "POOL", "CARBON_ACRE", "CARBON_TOTAL",
                      "CARBON_ACRE_SE", "CARBON_TOTAL_SE", "N_PLOTS", "N_TREES"]
        final_cols = [c for c in col_order if c in result.columns]
        for c in result.columns:
            if c not in final_cols:
                final_cols.append(c)
        return result.select(final_cols)

    finally:
        if owns_db and hasattr(db, "close"):
            db.close()
