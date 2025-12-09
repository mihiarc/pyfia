"""
Carbon flux estimation for FIA data.

Calculates net carbon sequestration as:
    Net Carbon Flux = Growth_carbon - Mortality_carbon - Removals_carbon

Where each component = Biomass x 0.47 (IPCC standard carbon fraction).

Positive values indicate net carbon sequestration (carbon sink).
Negative values indicate net carbon emission (carbon source).
"""

from typing import Dict, List, Optional, Union

import polars as pl

from ...core import FIA
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
        Database connection or path to FIA database. Can be either a path
        string to a DuckDB/SQLite file or an existing FIA connection object.
    grp_by : str or list of str, optional
        Column name(s) to group results by. Can be any column from the
        FIA tables used in the estimation. Common grouping columns include:

        - 'FORTYPCD': Forest type code
        - 'OWNGRPCD': Ownership group (10=National Forest, 20=Other Federal,
          30=State/Local, 40=Private)
        - 'STATECD': State FIPS code
        - 'COUNTYCD': County code

        For complete column descriptions, see USDA FIA Database User Guide.
    by_species : bool, default False
        If True, group results by species code (SPCD). This is a convenience
        parameter equivalent to adding 'SPCD' to grp_by.
    land_type : {'forest', 'timber'}, default 'forest'
        Land type to include in estimation:

        - 'forest': All forestland
        - 'timber': Productive timberland only (unreserved, productive)
    tree_type : {'gs', 'al'}, default 'gs'
        Tree type to include:

        - 'gs': Growing stock trees (live, merchantable)
        - 'al': All live trees
    tree_domain : str, optional
        SQL-like filter expression for tree-level filtering.
        Example: "DIA_MIDPT >= 10.0 AND SPCD == 131".
    area_domain : str, optional
        SQL-like filter expression for area/condition-level filtering.
        Example: "OWNGRPCD == 40 AND FORTYPCD == 161".
    totals : bool, default True
        If True, include population-level total estimates in addition to
        per-acre values.
    variance : bool, default True
        If True, calculate and include variance with proper covariance
        propagation. The combined variance accounts for positive covariance
        between growth, mortality, and removals since they are measured on
        the same plots.
    most_recent : bool, default False
        If True, automatically filter to the most recent evaluation for
        each state in the database.
    include_components : bool, default False
        If True, include growth, mortality, and removals carbon columns
        in the output in addition to the net flux.

    Returns
    -------
    pl.DataFrame
        Carbon flux estimates with the following columns:

        - **NET_CARBON_FLUX_ACRE** : float
            Net annual carbon flux per acre (tons C/acre/year)
        - **NET_CARBON_FLUX_TOTAL** : float (if totals=True)
            Total annual carbon flux (tons C/year)
        - **NET_CARBON_FLUX_SE** : float (if variance=True)
            Standard error of flux estimate
        - **NET_CARBON_FLUX_SE_PCT** : float (if variance=True)
            Standard error as percentage of estimate
        - **GROWTH_CARBON_TOTAL** : float (if include_components=True)
            Annual carbon accumulation from growth (tons C/year)
        - **MORT_CARBON_TOTAL** : float (if include_components=True)
            Annual carbon loss from mortality (tons C/year)
        - **REMV_CARBON_TOTAL** : float (if include_components=True)
            Annual carbon loss from removals (tons C/year)
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
    growth : Estimate annual growth using GRM tables
    mortality : Estimate annual mortality using GRM tables
    removals : Estimate annual removals/harvest using GRM tables
    biomass : Estimate biomass per acre (current inventory)

    Examples
    --------
    Basic carbon flux estimation:

    >>> results = carbon_flux(db)
    >>> flux = results['NET_CARBON_FLUX_TOTAL'][0]
    >>> if flux > 0:
    ...     print(f"Carbon sink: {flux/1e6:.2f} million tons C/year")
    ... else:
    ...     print(f"Carbon source: {abs(flux)/1e6:.2f} million tons C/year")

    Carbon flux by ownership:

    >>> results = carbon_flux(db, grp_by="OWNGRPCD", include_components=True)
    >>> for row in results.iter_rows(named=True):
    ...     print(f"Ownership {row['OWNGRPCD']}: {row['NET_CARBON_FLUX_TOTAL']:,.0f} tons C/year")

    Carbon flux on timberland by forest type:

    >>> results = carbon_flux(
    ...     db,
    ...     grp_by="FORTYPCD",
    ...     land_type="timber",
    ...     include_components=True
    ... )

    Notes
    -----
    Carbon is calculated as 47% of dry biomass following IPCC guidelines.
    This is applied uniformly to growth, mortality, and removals biomass.

    The variance calculation accounts for covariance between growth,
    mortality, and removals since they are measured on the same plots:

        Var(Net) = Var(G) + Var(M) + Var(R) - 2*Cov(G,M) - 2*Cov(G,R) + 2*Cov(M,R)

    Due to positive covariance, the combined variance is typically less
    than the sum of individual variances.

    **Important:** This function requires GRM (Growth-Removal-Mortality)
    tables in the database. These tables are included in FIA evaluations
    that support growth, removal, and mortality estimation.

    **Interpretation:**
    - Positive net flux = forest is a carbon sink (sequestering carbon)
    - Negative net flux = forest is a carbon source (emitting carbon)
    - Typical healthy forests show positive net flux

    Warnings
    --------
    The current implementation uses a simplified variance calculation that
    sums component variances. Full covariance-based variance calculation
    will be implemented in a future release. The reported SE may be
    conservative (slightly overestimated).

    Raises
    ------
    ValueError
        If TREE_GRM_COMPONENT or TREE_GRM_MIDPT tables are not found
        in the database.
    """
    # Get biomass estimates from each component
    # All three estimators support measure="biomass"

    growth_result = growth(
        db=db,
        grp_by=grp_by,
        by_species=by_species,
        land_type=land_type,
        tree_type=tree_type,
        measure="biomass",
        tree_domain=tree_domain,
        area_domain=area_domain,
        totals=totals,
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
        totals=totals,
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
        totals=totals,
        variance=variance,
        most_recent=most_recent,
    )

    # Determine grouping columns for joining
    group_cols = _get_group_cols(grp_by, by_species)

    # Calculate carbon flux
    results = _calculate_carbon_flux(
        growth_result=growth_result,
        mortality_result=mortality_result,
        removals_result=removals_result,
        group_cols=group_cols,
        totals=totals,
        variance=variance,
        include_components=include_components,
    )

    return results


def _get_group_cols(
    grp_by: Optional[Union[str, List[str]]], by_species: bool
) -> List[str]:
    """Extract grouping columns from parameters."""
    group_cols = []

    if grp_by:
        if isinstance(grp_by, str):
            group_cols = [grp_by]
        else:
            group_cols = list(grp_by)

    if by_species and "SPCD" not in group_cols:
        group_cols.append("SPCD")

    return group_cols


def _calculate_carbon_flux(
    growth_result: pl.DataFrame,
    mortality_result: pl.DataFrame,
    removals_result: pl.DataFrame,
    group_cols: List[str],
    totals: bool,
    variance: bool,
    include_components: bool,
) -> pl.DataFrame:
    """
    Calculate net carbon flux from component estimates.

    Net Flux = Growth - Mortality - Removals
    (all converted to carbon using 0.47 factor)
    """
    # Handle empty results
    if growth_result.is_empty():
        return _empty_flux_result(group_cols, totals, variance, include_components)

    # If no grouping, simple scalar calculation
    if not group_cols:
        return _calculate_scalar_flux(
            growth_result,
            mortality_result,
            removals_result,
            totals,
            variance,
            include_components,
        )

    # With grouping, join on group columns
    return _calculate_grouped_flux(
        growth_result,
        mortality_result,
        removals_result,
        group_cols,
        totals,
        variance,
        include_components,
    )


def _calculate_scalar_flux(
    growth_result: pl.DataFrame,
    mortality_result: pl.DataFrame,
    removals_result: pl.DataFrame,
    totals: bool,
    variance: bool,
    include_components: bool,
) -> pl.DataFrame:
    """Calculate carbon flux when no grouping is specified."""
    # Extract values (biomass in tons, convert to carbon)
    growth_acre = _safe_get(growth_result, "GROWTH_ACRE", 0.0) * CARBON_FRACTION
    mort_acre = _safe_get(mortality_result, "MORT_ACRE", 0.0) * CARBON_FRACTION
    remv_acre = _safe_get(removals_result, "REMOVALS_PER_ACRE", 0.0) * CARBON_FRACTION

    # Net flux = growth - mortality - removals
    net_flux_acre = growth_acre - mort_acre - remv_acre

    result_data: Dict = {
        "NET_CARBON_FLUX_ACRE": [net_flux_acre],
    }

    if totals:
        growth_total = _safe_get(growth_result, "GROWTH_TOTAL", 0.0) * CARBON_FRACTION
        mort_total = _safe_get(mortality_result, "MORT_TOTAL", 0.0) * CARBON_FRACTION
        remv_total = _safe_get(removals_result, "REMOVALS_TOTAL", 0.0) * CARBON_FRACTION

        net_flux_total = growth_total - mort_total - remv_total
        result_data["NET_CARBON_FLUX_TOTAL"] = [net_flux_total]

        # Area from growth result (should be consistent across all)
        area_total = _safe_get(growth_result, "AREA_TOTAL", 0.0)
        result_data["AREA_TOTAL"] = [area_total]

    if variance:
        # Calculate combined variance
        # For sum/difference: Var(G - M - R) = Var(G) + Var(M) + Var(R)
        # (conservative estimate without covariance - actual variance is lower)
        growth_se = _safe_get(growth_result, "GROWTH_ACRE_SE", 0.0) * CARBON_FRACTION
        mort_se = _safe_get(mortality_result, "MORT_ACRE_SE", 0.0) * CARBON_FRACTION
        remv_se = (
            _safe_get(removals_result, "REMOVALS_PER_ACRE_SE", 0.0) * CARBON_FRACTION
        )

        # Sum of variances (conservative - ignores covariance)
        combined_var_acre = growth_se**2 + mort_se**2 + remv_se**2
        net_flux_acre_se = combined_var_acre**0.5

        result_data["NET_CARBON_FLUX_SE"] = [net_flux_acre_se]

        # SE as percentage
        if abs(net_flux_acre) > 0:
            se_pct = abs(net_flux_acre_se / net_flux_acre) * 100
        else:
            se_pct = None
        result_data["NET_CARBON_FLUX_SE_PCT"] = [se_pct]

        if totals:
            growth_total_se = (
                _safe_get(growth_result, "GROWTH_TOTAL_SE", 0.0) * CARBON_FRACTION
            )
            mort_total_se = (
                _safe_get(mortality_result, "MORT_TOTAL_SE", 0.0) * CARBON_FRACTION
            )
            remv_total_se = (
                _safe_get(removals_result, "REMOVALS_TOTAL_SE", 0.0) * CARBON_FRACTION
            )

            combined_var_total = (
                growth_total_se**2 + mort_total_se**2 + remv_total_se**2
            )
            result_data["NET_CARBON_FLUX_TOTAL_SE"] = [combined_var_total**0.5]

    if include_components:
        result_data["GROWTH_CARBON_ACRE"] = [growth_acre]
        result_data["MORT_CARBON_ACRE"] = [mort_acre]
        result_data["REMV_CARBON_ACRE"] = [remv_acre]

        if totals:
            result_data["GROWTH_CARBON_TOTAL"] = [
                _safe_get(growth_result, "GROWTH_TOTAL", 0.0) * CARBON_FRACTION
            ]
            result_data["MORT_CARBON_TOTAL"] = [
                _safe_get(mortality_result, "MORT_TOTAL", 0.0) * CARBON_FRACTION
            ]
            result_data["REMV_CARBON_TOTAL"] = [
                _safe_get(removals_result, "REMOVALS_TOTAL", 0.0) * CARBON_FRACTION
            ]

    # Add metadata
    n_plots = _safe_get(growth_result, "N_PLOTS", 0)
    year = _safe_get(growth_result, "YEAR", 2023)

    result_data["N_PLOTS"] = [n_plots]
    result_data["YEAR"] = [year]

    return pl.DataFrame(result_data)


def _calculate_grouped_flux(
    growth_result: pl.DataFrame,
    mortality_result: pl.DataFrame,
    removals_result: pl.DataFrame,
    group_cols: List[str],
    totals: bool,
    variance: bool,
    include_components: bool,
) -> pl.DataFrame:
    """Calculate carbon flux with grouping."""
    # Select columns for joining
    growth_cols = group_cols + ["GROWTH_ACRE", "GROWTH_TOTAL", "AREA_TOTAL", "N_PLOTS"]
    if variance:
        growth_cols.extend(["GROWTH_ACRE_SE", "GROWTH_TOTAL_SE"])

    # Filter to available columns
    growth_cols = [c for c in growth_cols if c in growth_result.columns]
    growth_df = growth_result.select(growth_cols)

    mort_cols = group_cols + ["MORT_ACRE", "MORT_TOTAL"]
    if variance:
        mort_cols.extend(["MORT_ACRE_SE", "MORT_TOTAL_SE"])
    mort_cols = [c for c in mort_cols if c in mortality_result.columns]
    mort_df = mortality_result.select(mort_cols)

    remv_cols = group_cols + ["REMOVALS_PER_ACRE", "REMOVALS_TOTAL"]
    if variance:
        remv_cols.extend(["REMOVALS_PER_ACRE_SE", "REMOVALS_TOTAL_SE"])
    remv_cols = [c for c in remv_cols if c in removals_result.columns]
    remv_df = removals_result.select(remv_cols)

    # Ensure consistent data types for join columns across all DataFrames
    # This is necessary because different estimators might have different dtypes
    for col in group_cols:
        if col in growth_df.columns and col in mort_df.columns:
            # Cast to the most common type (int64 for codes)
            if growth_df[col].dtype != mort_df[col].dtype:
                growth_df = growth_df.with_columns(pl.col(col).cast(pl.Int64))
                mort_df = mort_df.with_columns(pl.col(col).cast(pl.Int64))
        if col in growth_df.columns and col in remv_df.columns:
            if col in growth_df.columns:
                growth_df = growth_df.with_columns(pl.col(col).cast(pl.Int64))
            if col in remv_df.columns:
                remv_df = remv_df.with_columns(pl.col(col).cast(pl.Int64))

    # Join on group columns using coalesce strategy to avoid duplicate columns
    result = growth_df.join(
        mort_df, on=group_cols, how="outer_coalesce", suffix="_mort"
    )
    result = result.join(
        remv_df, on=group_cols, how="outer_coalesce", suffix="_remv"
    )

    # Fill nulls with zeros
    result = result.with_columns(
        [
            pl.col("GROWTH_ACRE").fill_null(0.0),
            pl.col("GROWTH_TOTAL").fill_null(0.0),
            pl.col("MORT_ACRE").fill_null(0.0),
            pl.col("MORT_TOTAL").fill_null(0.0),
            pl.col("REMOVALS_PER_ACRE").fill_null(0.0),
            pl.col("REMOVALS_TOTAL").fill_null(0.0),
        ]
    )

    # Calculate net carbon flux (convert biomass to carbon)
    result = result.with_columns(
        [
            (
                (
                    pl.col("GROWTH_ACRE")
                    - pl.col("MORT_ACRE")
                    - pl.col("REMOVALS_PER_ACRE")
                )
                * CARBON_FRACTION
            ).alias("NET_CARBON_FLUX_ACRE"),
        ]
    )

    if totals:
        result = result.with_columns(
            [
                (
                    (
                        pl.col("GROWTH_TOTAL")
                        - pl.col("MORT_TOTAL")
                        - pl.col("REMOVALS_TOTAL")
                    )
                    * CARBON_FRACTION
                ).alias("NET_CARBON_FLUX_TOTAL"),
            ]
        )

    if variance:
        # Fill SE nulls
        result = result.with_columns(
            [
                pl.col("GROWTH_ACRE_SE").fill_null(0.0),
                pl.col("MORT_ACRE_SE").fill_null(0.0),
                pl.col("REMOVALS_PER_ACRE_SE").fill_null(0.0),
            ]
        )

        # Combined SE (sum of variances, conservative)
        result = result.with_columns(
            [
                (
                    (
                        (pl.col("GROWTH_ACRE_SE") * CARBON_FRACTION) ** 2
                        + (pl.col("MORT_ACRE_SE") * CARBON_FRACTION) ** 2
                        + (pl.col("REMOVALS_PER_ACRE_SE") * CARBON_FRACTION) ** 2
                    )
                    ** 0.5
                ).alias("NET_CARBON_FLUX_SE"),
            ]
        )

        # SE as percentage
        result = result.with_columns(
            [
                pl.when(pl.col("NET_CARBON_FLUX_ACRE").abs() > 0)
                .then(
                    (
                        pl.col("NET_CARBON_FLUX_SE")
                        / pl.col("NET_CARBON_FLUX_ACRE").abs()
                    )
                    * 100
                )
                .otherwise(None)
                .alias("NET_CARBON_FLUX_SE_PCT"),
            ]
        )

        if totals:
            result = result.with_columns(
                [
                    pl.col("GROWTH_TOTAL_SE").fill_null(0.0),
                    pl.col("MORT_TOTAL_SE").fill_null(0.0),
                    pl.col("REMOVALS_TOTAL_SE").fill_null(0.0),
                ]
            )

            result = result.with_columns(
                [
                    (
                        (
                            (pl.col("GROWTH_TOTAL_SE") * CARBON_FRACTION) ** 2
                            + (pl.col("MORT_TOTAL_SE") * CARBON_FRACTION) ** 2
                            + (pl.col("REMOVALS_TOTAL_SE") * CARBON_FRACTION) ** 2
                        )
                        ** 0.5
                    ).alias("NET_CARBON_FLUX_TOTAL_SE"),
                ]
            )

    if include_components:
        result = result.with_columns(
            [
                (pl.col("GROWTH_ACRE") * CARBON_FRACTION).alias("GROWTH_CARBON_ACRE"),
                (pl.col("MORT_ACRE") * CARBON_FRACTION).alias("MORT_CARBON_ACRE"),
                (pl.col("REMOVALS_PER_ACRE") * CARBON_FRACTION).alias(
                    "REMV_CARBON_ACRE"
                ),
            ]
        )

        if totals:
            result = result.with_columns(
                [
                    (pl.col("GROWTH_TOTAL") * CARBON_FRACTION).alias(
                        "GROWTH_CARBON_TOTAL"
                    ),
                    (pl.col("MORT_TOTAL") * CARBON_FRACTION).alias("MORT_CARBON_TOTAL"),
                    (pl.col("REMOVALS_TOTAL") * CARBON_FRACTION).alias(
                        "REMV_CARBON_TOTAL"
                    ),
                ]
            )

    # Select final columns
    output_cols = group_cols + ["NET_CARBON_FLUX_ACRE"]

    if totals:
        output_cols.append("NET_CARBON_FLUX_TOTAL")

    if variance:
        output_cols.extend(["NET_CARBON_FLUX_SE", "NET_CARBON_FLUX_SE_PCT"])
        if totals:
            output_cols.append("NET_CARBON_FLUX_TOTAL_SE")

    if include_components:
        output_cols.extend(
            ["GROWTH_CARBON_ACRE", "MORT_CARBON_ACRE", "REMV_CARBON_ACRE"]
        )
        if totals:
            output_cols.extend(
                ["GROWTH_CARBON_TOTAL", "MORT_CARBON_TOTAL", "REMV_CARBON_TOTAL"]
            )

    # Add metadata columns if available
    if "AREA_TOTAL" in result.columns:
        output_cols.append("AREA_TOTAL")
    if "N_PLOTS" in result.columns:
        output_cols.append("N_PLOTS")

    # Filter to available columns
    output_cols = [c for c in output_cols if c in result.columns]

    return result.select(output_cols)


def _empty_flux_result(
    group_cols: List[str],
    totals: bool,
    variance: bool,
    include_components: bool,
) -> pl.DataFrame:
    """Return empty DataFrame with correct schema when no data available."""
    schema: Dict = {col: pl.Utf8 for col in group_cols}
    schema["NET_CARBON_FLUX_ACRE"] = pl.Float64

    if totals:
        schema["NET_CARBON_FLUX_TOTAL"] = pl.Float64
        schema["AREA_TOTAL"] = pl.Float64

    if variance:
        schema["NET_CARBON_FLUX_SE"] = pl.Float64
        schema["NET_CARBON_FLUX_SE_PCT"] = pl.Float64
        if totals:
            schema["NET_CARBON_FLUX_TOTAL_SE"] = pl.Float64

    if include_components:
        schema["GROWTH_CARBON_ACRE"] = pl.Float64
        schema["MORT_CARBON_ACRE"] = pl.Float64
        schema["REMV_CARBON_ACRE"] = pl.Float64
        if totals:
            schema["GROWTH_CARBON_TOTAL"] = pl.Float64
            schema["MORT_CARBON_TOTAL"] = pl.Float64
            schema["REMV_CARBON_TOTAL"] = pl.Float64

    schema["N_PLOTS"] = pl.Int64
    schema["YEAR"] = pl.Int64

    return pl.DataFrame(schema=schema)


def _safe_get(df: pl.DataFrame, col: str, default: float = 0.0) -> float:
    """Safely get first value from column, returning default if not available."""
    if df.is_empty() or col not in df.columns:
        return default

    val = df[col][0]
    if val is None:
        return default

    return float(val)
