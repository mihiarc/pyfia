"""
Carbon flux estimation for FIA data.

Calculates net carbon sequestration as:
    Net Carbon Flux = Growth_carbon - Mortality_carbon - Removals_carbon

Where each component = Biomass x 0.47 (IPCC standard carbon fraction).

Positive values indicate net carbon sequestration (carbon sink).
Negative values indicate net carbon emission (carbon source).
"""

from typing import Any, Dict, List, Optional, Union

import polars as pl

from ...core import FIA
from .area import area
from .growth import growth
from .mortality import mortality
from .removals import removals

# IPCC standard carbon fraction of dry biomass
CARBON_FRACTION = 0.47


def carbon_flux(
    db: Union[str, FIA],
    grp_by: Optional[Union[str, List[str]]] = None,
    by_species: bool = False,
    land_type: str = "forest",
    tree_type: str = "gs",
    tree_domain: Optional[str] = None,
    area_domain: Optional[str] = None,
    totals: bool = True,
    variance: bool = True,
    most_recent: bool = False,
    include_components: bool = False,
) -> pl.DataFrame:
    """
    Estimate annual net carbon flux from FIA data.

    Calculates net carbon sequestration as the difference between annual
    carbon accumulation (growth) and annual carbon loss (mortality + removals):

        Net Carbon Flux = Growth_carbon - Mortality_carbon - Removals_carbon

    Positive values indicate net carbon sequestration (carbon sink).
    Negative values indicate net carbon emission (carbon source).

    Parameters
    ----------
    db : Union[str, FIA]
        Database connection or path to FIA database.
    grp_by : str or list of str, optional
        Column name(s) to group results by (e.g., 'FORTYPCD', 'OWNGRPCD').
    by_species : bool, default False
        If True, group results by species code (SPCD).
    land_type : {'forest', 'timber'}, default 'forest'
        Land type to include in estimation.
    tree_type : {'gs', 'al'}, default 'gs'
        Tree type: 'gs' for growing stock, 'al' for all live trees.
    tree_domain : str, optional
        SQL-like filter for tree-level filtering.
    area_domain : str, optional
        SQL-like filter for condition-level filtering.
    totals : bool, default True
        If True, include population-level total estimates.
    variance : bool, default True
        If True, include variance estimates.
    most_recent : bool, default False
        If True, filter to most recent evaluation.
    include_components : bool, default False
        If True, include growth, mortality, removals carbon columns.

    Returns
    -------
    pl.DataFrame
        Carbon flux estimates with columns:
        - NET_CARBON_FLUX_ACRE: Net carbon per acre (tons C/acre/year)
        - NET_CARBON_FLUX_TOTAL: Total net carbon (tons C/year)
        - AREA_TOTAL: Forest area (acres)
        - N_PLOTS: Number of plots
        - YEAR: Representative year

    Notes
    -----
    Per-acre values are calculated as NET_TOTAL / AREA to ensure consistency,
    since growth, mortality, and removals use different base areas internally.
    """
    # Normalize grouping columns
    group_cols = _normalize_group_cols(grp_by, by_species)

    # Get forest area for per-acre calculation
    area_result = area(
        db=db,
        grp_by=grp_by,
        land_type=land_type,
        area_domain=area_domain,
        most_recent=most_recent,
    )

    # Get biomass totals from each GRM component
    growth_result = growth(
        db=db,
        grp_by=grp_by,
        by_species=by_species,
        land_type=land_type,
        tree_type=tree_type,
        measure="biomass",
        tree_domain=tree_domain,
        area_domain=area_domain,
        totals=True,  # Always need totals
        variance=variance,
        most_recent=most_recent,
    )

    mortality_result = mortality(
        db=db,
        grp_by=grp_by,
        by_species=by_species,
        land_type=land_type,
        tree_type=tree_type,
        measure="biomass",
        tree_domain=tree_domain,
        area_domain=area_domain,
        totals=True,
        variance=variance,
        most_recent=most_recent,
    )

    removals_result = removals(
        db=db,
        grp_by=grp_by,
        by_species=by_species,
        land_type=land_type,
        tree_type=tree_type,
        measure="biomass",
        tree_domain=tree_domain,
        area_domain=area_domain,
        totals=True,
        variance=variance,
        most_recent=most_recent,
    )

    # Calculate carbon flux
    return _calculate_carbon_flux(
        area_result=area_result,
        growth_result=growth_result,
        mortality_result=mortality_result,
        removals_result=removals_result,
        group_cols=group_cols,
        totals=totals,
        variance=variance,
        include_components=include_components,
    )


def _normalize_group_cols(
    grp_by: Optional[Union[str, List[str]]], by_species: bool
) -> List[str]:
    """Normalize grouping columns to a list."""
    group_cols: List[str] = []

    if grp_by:
        if isinstance(grp_by, str):
            group_cols = [grp_by]
        else:
            group_cols = list(grp_by)

    if by_species and "SPCD" not in group_cols:
        group_cols.append("SPCD")

    return group_cols


def _calculate_carbon_flux(
    area_result: pl.DataFrame,
    growth_result: pl.DataFrame,
    mortality_result: pl.DataFrame,
    removals_result: pl.DataFrame,
    group_cols: List[str],
    totals: bool,
    variance: bool,
    include_components: bool,
) -> pl.DataFrame:
    """Calculate net carbon flux from component estimates."""
    # Handle empty results
    if growth_result.is_empty() or area_result.is_empty():
        return _empty_result(group_cols, totals, variance, include_components)

    # No grouping: simple scalar calculation
    if not group_cols:
        return _scalar_flux(
            area_result,
            growth_result,
            mortality_result,
            removals_result,
            totals,
            variance,
            include_components,
        )

    # With grouping: join and calculate
    return _grouped_flux(
        area_result,
        growth_result,
        mortality_result,
        removals_result,
        group_cols,
        totals,
        variance,
        include_components,
    )


def _scalar_flux(
    area_result: pl.DataFrame,
    growth_result: pl.DataFrame,
    mortality_result: pl.DataFrame,
    removals_result: pl.DataFrame,
    totals: bool,
    variance: bool,
    include_components: bool,
) -> pl.DataFrame:
    """Calculate carbon flux without grouping."""
    # Get totals (biomass in tons)
    growth_total = _safe_get(growth_result, "GROWTH_TOTAL", 0.0)
    mort_total = _safe_get(mortality_result, "MORT_TOTAL", 0.0)
    remv_total = _safe_get(removals_result, "REMOVALS_TOTAL", 0.0)
    forest_area = _safe_get(area_result, "AREA", 0.0)

    # Convert to carbon
    growth_c = growth_total * CARBON_FRACTION
    mort_c = mort_total * CARBON_FRACTION
    remv_c = remv_total * CARBON_FRACTION

    # Net flux = growth - mortality - removals
    net_total = growth_c - mort_c - remv_c
    net_acre = net_total / forest_area if forest_area > 0 else 0.0

    result = {
        "NET_CARBON_FLUX_ACRE": [net_acre],
        "AREA_TOTAL": [forest_area],
    }

    if totals:
        result["NET_CARBON_FLUX_TOTAL"] = [net_total]

    if variance:
        # Combined SE (sum of variances - conservative)
        g_se = _safe_get(growth_result, "GROWTH_TOTAL_SE", 0.0) * CARBON_FRACTION
        m_se = _safe_get(mortality_result, "MORT_TOTAL_SE", 0.0) * CARBON_FRACTION
        r_se = _safe_get(removals_result, "REMOVALS_TOTAL_SE", 0.0) * CARBON_FRACTION

        combined_var = g_se**2 + m_se**2 + r_se**2
        net_se = combined_var**0.5

        if totals:
            result["NET_CARBON_FLUX_TOTAL_SE"] = [net_se]

        # Per-acre SE
        if forest_area > 0:
            acre_se = net_se / forest_area
            result["NET_CARBON_FLUX_ACRE_SE"] = [acre_se]
            cv_value: Optional[float] = (
                abs(acre_se / net_acre) * 100 if net_acre != 0 else None
            )
            result["NET_CARBON_FLUX_CV"] = [cv_value]  # type: ignore[list-item]
        else:
            result["NET_CARBON_FLUX_ACRE_SE"] = [None]  # type: ignore[list-item]
            result["NET_CARBON_FLUX_CV"] = [None]  # type: ignore[list-item]

    if include_components:
        result["GROWTH_CARBON_TOTAL"] = [growth_c]
        result["MORT_CARBON_TOTAL"] = [mort_c]
        result["REMV_CARBON_TOTAL"] = [remv_c]

    # Metadata
    result["N_PLOTS"] = [int(_safe_get(growth_result, "N_PLOTS", 0))]
    result["YEAR"] = [int(_safe_get(growth_result, "YEAR", 2023))]

    return pl.DataFrame(result)


def _grouped_flux(
    area_result: pl.DataFrame,
    growth_result: pl.DataFrame,
    mortality_result: pl.DataFrame,
    removals_result: pl.DataFrame,
    group_cols: List[str],
    totals: bool,
    variance: bool,
    include_components: bool,
) -> pl.DataFrame:
    """Calculate carbon flux with grouping."""
    # Select relevant columns from each result
    # Area column is named "AREA" in area estimator output, rename to AREA_TOTAL
    area_cols = [c for c in group_cols if c in area_result.columns]
    if "AREA" in area_result.columns:
        area_df = area_result.select(area_cols + ["AREA"]).rename(
            {"AREA": "AREA_TOTAL"}
        )
    else:
        area_df = area_result.select(
            [c for c in area_cols + ["AREA_TOTAL"] if c in area_result.columns]
        )

    growth_cols = [c for c in group_cols if c in growth_result.columns] + [
        "GROWTH_TOTAL",
        "N_PLOTS",
        "YEAR",
    ]
    if variance and "GROWTH_TOTAL_SE" in growth_result.columns:
        growth_cols.append("GROWTH_TOTAL_SE")
    growth_df = growth_result.select(
        [c for c in growth_cols if c in growth_result.columns]
    )

    mort_cols = [c for c in group_cols if c in mortality_result.columns] + [
        "MORT_TOTAL"
    ]
    if variance and "MORT_TOTAL_SE" in mortality_result.columns:
        mort_cols.append("MORT_TOTAL_SE")
    mort_df = mortality_result.select(
        [c for c in mort_cols if c in mortality_result.columns]
    )

    remv_cols = [c for c in group_cols if c in removals_result.columns] + [
        "REMOVALS_TOTAL"
    ]
    if variance and "REMOVALS_TOTAL_SE" in removals_result.columns:
        remv_cols.append("REMOVALS_TOTAL_SE")
    remv_df = removals_result.select(
        [c for c in remv_cols if c in removals_result.columns]
    )

    # Cast group columns to consistent types
    for col in group_cols:
        for df_name, df in [
            ("area", area_df),
            ("growth", growth_df),
            ("mort", mort_df),
            ("remv", remv_df),
        ]:
            if col in df.columns:
                if df_name == "area":
                    area_df = df.with_columns(pl.col(col).cast(pl.Int64))
                elif df_name == "growth":
                    growth_df = df.with_columns(pl.col(col).cast(pl.Int64))
                elif df_name == "mort":
                    mort_df = df.with_columns(pl.col(col).cast(pl.Int64))
                else:
                    remv_df = df.with_columns(pl.col(col).cast(pl.Int64))

    # Join on group columns
    join_cols = [c for c in group_cols if c in growth_df.columns]

    if join_cols:
        result = growth_df
        if join_cols[0] in area_df.columns:
            result = result.join(area_df, on=join_cols, how="left")
        if join_cols[0] in mort_df.columns:
            result = result.join(mort_df, on=join_cols, how="left")
        if join_cols[0] in remv_df.columns:
            result = result.join(remv_df, on=join_cols, how="left")
    else:
        # No common grouping columns, use cross join
        result = growth_df

    # Fill nulls
    result = result.with_columns(
        [
            pl.col("GROWTH_TOTAL").fill_null(0.0),
            pl.col("MORT_TOTAL").fill_null(0.0),
            pl.col("REMOVALS_TOTAL").fill_null(0.0),
            pl.col("AREA_TOTAL").fill_null(0.0),
        ]
    )

    # Calculate carbon totals
    result = result.with_columns(
        [
            (pl.col("GROWTH_TOTAL") * CARBON_FRACTION).alias("_growth_c"),
            (pl.col("MORT_TOTAL") * CARBON_FRACTION).alias("_mort_c"),
            (pl.col("REMOVALS_TOTAL") * CARBON_FRACTION).alias("_remv_c"),
        ]
    )

    # Net carbon flux
    result = result.with_columns(
        [
            (pl.col("_growth_c") - pl.col("_mort_c") - pl.col("_remv_c")).alias(
                "NET_CARBON_FLUX_TOTAL"
            ),
        ]
    )

    # Per-acre (from total / area)
    result = result.with_columns(
        [
            pl.when(pl.col("AREA_TOTAL") > 0)
            .then(pl.col("NET_CARBON_FLUX_TOTAL") / pl.col("AREA_TOTAL"))
            .otherwise(0.0)
            .alias("NET_CARBON_FLUX_ACRE"),
        ]
    )

    # Variance
    if variance:
        se_cols = []
        if "GROWTH_TOTAL_SE" in result.columns:
            se_cols.append(
                (pl.col("GROWTH_TOTAL_SE").fill_null(0.0) * CARBON_FRACTION).alias(
                    "_g_se"
                )
            )
        if "MORT_TOTAL_SE" in result.columns:
            se_cols.append(
                (pl.col("MORT_TOTAL_SE").fill_null(0.0) * CARBON_FRACTION).alias(
                    "_m_se"
                )
            )
        if "REMOVALS_TOTAL_SE" in result.columns:
            se_cols.append(
                (pl.col("REMOVALS_TOTAL_SE").fill_null(0.0) * CARBON_FRACTION).alias(
                    "_r_se"
                )
            )

        if se_cols:
            result = result.with_columns(se_cols)
            result = result.with_columns(
                [
                    (
                        pl.col("_g_se").fill_null(0.0) ** 2
                        + pl.col("_m_se").fill_null(0.0) ** 2
                        + pl.col("_r_se").fill_null(0.0) ** 2
                    )
                    .sqrt()
                    .alias("NET_CARBON_FLUX_TOTAL_SE"),
                ]
            )

            result = result.with_columns(
                [
                    pl.when(pl.col("AREA_TOTAL") > 0)
                    .then(pl.col("NET_CARBON_FLUX_TOTAL_SE") / pl.col("AREA_TOTAL"))
                    .otherwise(None)
                    .alias("NET_CARBON_FLUX_ACRE_SE"),
                    pl.when(pl.col("NET_CARBON_FLUX_ACRE").abs() > 0)
                    .then(
                        (
                            pl.col("NET_CARBON_FLUX_TOTAL_SE")
                            / pl.col("AREA_TOTAL")
                            / pl.col("NET_CARBON_FLUX_ACRE").abs()
                        )
                        * 100
                    )
                    .otherwise(None)
                    .alias("NET_CARBON_FLUX_CV"),
                ]
            )

    # Select output columns
    output_cols = group_cols + ["NET_CARBON_FLUX_ACRE", "AREA_TOTAL"]

    if totals:
        output_cols.append("NET_CARBON_FLUX_TOTAL")

    if variance:
        if "NET_CARBON_FLUX_ACRE_SE" in result.columns:
            output_cols.append("NET_CARBON_FLUX_ACRE_SE")
        if "NET_CARBON_FLUX_CV" in result.columns:
            output_cols.append("NET_CARBON_FLUX_CV")
        if totals and "NET_CARBON_FLUX_TOTAL_SE" in result.columns:
            output_cols.append("NET_CARBON_FLUX_TOTAL_SE")

    if include_components:
        result = result.with_columns(
            [
                pl.col("_growth_c").alias("GROWTH_CARBON_TOTAL"),
                pl.col("_mort_c").alias("MORT_CARBON_TOTAL"),
                pl.col("_remv_c").alias("REMV_CARBON_TOTAL"),
            ]
        )
        output_cols.extend(
            ["GROWTH_CARBON_TOTAL", "MORT_CARBON_TOTAL", "REMV_CARBON_TOTAL"]
        )

    if "N_PLOTS" in result.columns:
        output_cols.append("N_PLOTS")
    if "YEAR" in result.columns:
        output_cols.append("YEAR")

    output_cols = [c for c in output_cols if c in result.columns]
    return result.select(output_cols)


def _empty_result(
    group_cols: List[str],
    totals: bool,
    variance: bool,
    include_components: bool,
) -> pl.DataFrame:
    """Return empty DataFrame with correct schema."""
    schema: Dict[str, Any] = {col: pl.Int64 for col in group_cols}
    schema["NET_CARBON_FLUX_ACRE"] = pl.Float64
    schema["AREA_TOTAL"] = pl.Float64

    if totals:
        schema["NET_CARBON_FLUX_TOTAL"] = pl.Float64

    if variance:
        schema["NET_CARBON_FLUX_ACRE_SE"] = pl.Float64
        schema["NET_CARBON_FLUX_CV"] = pl.Float64
        if totals:
            schema["NET_CARBON_FLUX_TOTAL_SE"] = pl.Float64

    if include_components:
        schema["GROWTH_CARBON_TOTAL"] = pl.Float64
        schema["MORT_CARBON_TOTAL"] = pl.Float64
        schema["REMV_CARBON_TOTAL"] = pl.Float64

    schema["N_PLOTS"] = pl.Int64
    schema["YEAR"] = pl.Int64

    return pl.DataFrame(schema=schema)


def _safe_get(df: pl.DataFrame, col: str, default: float = 0.0) -> float:
    """Safely get first value from column."""
    if df.is_empty() or col not in df.columns:
        return default
    val = df[col][0]
    return float(val) if val is not None else default
