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
from typing import Callable

import polars as pl

from ..core import FIA
from ..estimation.utils import ensure_evalid_set, ensure_fia_instance

logger = logging.getLogger(__name__)


def total_ecosystem(
    db: str | FIA,
    grp_by: str | list[str] | None = None,
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

    Each pool is estimated independently, then results are stacked and
    summed across pools.  This produces the total forest ecosystem carbon
    stock comparable to EVALIDator snum=103 ("Total all pools").

    Parameters
    ----------
    db : str | FIA
        Database connection or path to FIA database.
    grp_by : str or list of str, optional
        Column name(s) to group results by (e.g., 'FORTYPCD', 'OWNGRPCD').
        Forwarded to every pool estimator; the ``TOTAL_ECOSYSTEM`` summary
        row is summed per group.
    land_type : {'forest', 'timber', 'all'}, default 'forest'
        Land type to include in estimation.
    area_domain : str, optional
        SQL-like filter expression for area/condition-level filtering.
    plot_domain : str, optional
        SQL-like filter expression for plot-level filtering.
    totals : bool, default True
        If True, include population-level total estimates.
    variance : bool, default False
        If True, calculate and include standard error estimates for the
        individual pool rows.  The ``TOTAL_ECOSYSTEM`` summary row does
        NOT receive a combined SE — see Notes below.
    most_recent : bool, default False
        If True, automatically filter to the most recent evaluation.

    Returns
    -------
    pl.DataFrame
        One row per pool plus a ``TOTAL_ECOSYSTEM`` row (one per group, if
        ``grp_by`` is set).  Columns: POOL, CARBON_ACRE, CARBON_TOTAL
        (if totals), and SE columns (if variance, on pool rows only).

    Notes
    -----
    Standard error is intentionally NOT computed for the ``TOTAL_ECOSYSTEM``
    row.  The naive ``sqrt(sum(SE_pool^2))`` assumes independence between
    pools, which is incorrect: pool estimates share plot- and stratum-level
    sampling variance through the common post-stratification.  Reporting a
    combined SE without modelling that covariance would overstate
    precision.  Callers needing a combined SE should compute one explicitly
    against their own assumptions, or use ``stock_change`` for paired
    plot-level inference.

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

    >>> grouped = total_ecosystem(db, grp_by="FORTYPCD")
    >>> totals = grouped.filter(pl.col("POOL") == "TOTAL_ECOSYSTEM")
    """
    from ..validation import (
        validate_boolean,
        validate_domain_expression,
        validate_grp_by,
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
    grp_by_norm = validate_grp_by(grp_by)
    # validate_grp_by returns None for empty input; normalize to list form
    grp_cols: list[str] = (
        [grp_by_norm]
        if isinstance(grp_by_norm, str)
        else list(grp_by_norm)
        if grp_by_norm
        else []
    )

    # ----- Resolve db + EVALID -----
    db, owns_db = ensure_fia_instance(db)
    if most_recent and db.evalid is None:
        db.clip_most_recent(eval_type="VOL")
    else:
        ensure_evalid_set(db, eval_type="VOL", estimator_name="total_ecosystem")

    common_kwargs: dict = {
        "grp_by": grp_by_norm,
        "land_type": land_type,
        "area_domain": area_domain,
        "plot_domain": plot_domain,
        "totals": totals,
        "variance": variance,
    }

    try:
        pool_specs: list[tuple[str, Callable[..., pl.DataFrame]]] = [
            ("LIVE_TREE", live_tree),
            ("STANDING_DEAD", standing_dead),
            ("UNDERSTORY", understory),
            ("DOWNED_DEAD", downed_dead),
            ("LITTER", litter),
            ("SOIL_ORGANIC", soil_organic),
        ]

        pool_results: list[pl.DataFrame] = []
        for label, fn in pool_specs:
            frame = fn(db, pool="total", **common_kwargs)
            frame = frame.with_columns(pl.lit(label).alias("POOL"))
            pool_results.append(frame)

        # Guard against empty results (e.g. filters eliminate all data).
        non_empty = [r for r in pool_results if len(r) > 0]
        if not non_empty:
            return pl.DataFrame({"POOL": ["TOTAL_ECOSYSTEM"], "CARBON_ACRE": [0.0]})

        sum_cols = ["CARBON_ACRE"]
        if totals:
            sum_cols.append("CARBON_TOTAL")

        # Build the TOTAL_ECOSYSTEM row(s) by summing across pools.
        # Stack only the columns we need so disparate per-pool extras
        # (N_TREES, by_species columns, etc.) don't interfere with the sum.
        keep_cols = grp_cols + [
            c for c in sum_cols if all(c in r.columns for r in non_empty)
        ]
        if not keep_cols or keep_cols == grp_cols:
            # No sum columns present — fall back to fallback row.
            return pl.DataFrame({"POOL": ["TOTAL_ECOSYSTEM"], "CARBON_ACRE": [0.0]})

        stacked = pl.concat(
            [r.select(keep_cols) for r in non_empty],
            how="vertical_relaxed",
        )

        if grp_cols:
            total_df = stacked.group_by(grp_cols).agg(
                [pl.col(c).sum().alias(c) for c in sum_cols if c in stacked.columns]
            )
        else:
            total_df = stacked.select(
                [pl.col(c).sum().alias(c) for c in sum_cols if c in stacked.columns]
            )

        total_df = total_df.with_columns(pl.lit("TOTAL_ECOSYSTEM").alias("POOL"))

        # Propagate YEAR from first non-empty pool, when present.
        if "YEAR" in non_empty[0].columns and "YEAR" not in grp_cols:
            year_val = non_empty[0]["YEAR"][0]
            total_df = total_df.with_columns(pl.lit(year_val).alias("YEAR"))

        # Normalize columns across all pool results and total
        all_frames = pool_results + [total_df]
        all_cols: set[str] = set()
        for frame in all_frames:
            all_cols.update(frame.columns)

        normalized = []
        for frame in all_frames:
            for col in all_cols:
                if col not in frame.columns:
                    frame = frame.with_columns(pl.lit(None).alias(col))
            normalized.append(frame)

        result = pl.concat(normalized, how="diagonal_relaxed")

        # Order columns: grp_by columns first, then standard layout.
        col_order = grp_cols + [
            "YEAR",
            "POOL",
            "CARBON_ACRE",
            "CARBON_TOTAL",
            "CARBON_ACRE_SE",
            "CARBON_TOTAL_SE",
            "N_PLOTS",
            "N_TREES",
        ]
        final_cols = [c for c in col_order if c in result.columns]
        for c in result.columns:
            if c not in final_cols:
                final_cols.append(c)
        return result.select(final_cols)

    finally:
        if owns_db and hasattr(db, "close"):
            db.close()
