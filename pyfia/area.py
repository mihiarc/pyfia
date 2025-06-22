"""
Forest area estimation following FIA methodology.

This module implements area estimation for calculating forest area,
land type proportions, and other area-based metrics from FIA data.
"""

from typing import List, Optional

import polars as pl

from pyfia.estimation_utils import ratio_var


def area(
    db,
    grp_by: Optional[List[str]] = None,
    by_land_type: bool = False,
    land_type: str = "forest",
    tree_domain: Optional[str] = None,
    area_domain: Optional[str] = None,
    method: str = "TI",
    lambda_: float = 0.5,
    totals: bool = False,
    variance: bool = False,
    most_recent: bool = False
) -> pl.DataFrame:
    """
    Estimate forest area and land proportions from FIA data.

    Calculates the area (acres) and/or proportion of land meeting specified
    criteria. Can group by land type categories or user-defined variables.

    Parameters
    ----------
    db : FIA
        FIA database object with data loaded
    grp_by : list of str, optional
        Columns to group estimates by
    by_land_type : bool, default False
        Group by land type (timber, non-timber forest, non-forest, water)
    land_type : str, default "forest"
        Land type filter: "forest", "timber", or "all"
    tree_domain : str, optional
        SQL-like condition to filter trees. If specified, only conditions
        containing at least one qualifying tree are included
    area_domain : str, optional
        SQL-like condition to filter area (e.g., "FORTYPCD == 171")
    method : str, default "TI"
        Estimation method (currently only "TI" supported)
    lambda_ : float, default 0.5
        Temporal weighting parameter (not used for TI)
    totals : bool, default False
        Include total area in addition to percentages
    variance : bool, default False
        Return variance instead of standard error
    most_recent : bool, default False
        Use only most recent evaluation

    Returns
    -------
    pl.DataFrame
        DataFrame with area estimates. Columns include:
        - AREA_PERC: Percentage of total area meeting criteria
        - AREA: Total acres (if totals=True)
        - Standard errors or variances
        - N_PLOTS: Number of plots
    """
    # Ensure tables are loaded
    db.load_table('PLOT')
    db.load_table('COND')
    db.load_table('POP_STRATUM')
    db.load_table('POP_PLOT_STRATUM_ASSGN')

    # Load TREE table only if tree_domain is specified
    if tree_domain:
        db.load_table('TREE')

    # Get filtered data
    plots = db.get_plots()
    data = {
        "PLOT": plots,
        "COND": db.get_conditions(),
        "POP_STRATUM": db.tables['POP_STRATUM'].collect()
    }

    # Get tree data if needed
    if tree_domain:
        data["TREE"] = db.get_trees()

    # Get plot-stratum assignments filtered by current evaluation
    if db.evalid:
        ppsa = db.tables['POP_PLOT_STRATUM_ASSGN'].filter(
            pl.col('EVALID').is_in(db.evalid)
        ).collect()
    else:
        plot_cns = plots['CN'].to_list()
        ppsa = db.tables['POP_PLOT_STRATUM_ASSGN'].filter(
            pl.col('PLT_CN').is_in(plot_cns)
        ).collect()

    data["POP_PLOT_STRATUM_ASSGN"] = ppsa

    # Apply domain filters
    cond_df = _apply_area_filters(data["COND"], land_type, area_domain)

    # Handle tree domain if specified
    if tree_domain:
        cond_df = _apply_tree_domain_to_conditions(
            cond_df, data["TREE"], tree_domain
        )

    # Add land type categories if requested
    if by_land_type:
        cond_df = _add_land_type_categories(cond_df)
        if grp_by is None:
            grp_by = []
        grp_by.append("LAND_TYPE")

    # Calculate domain indicators
    cond_df = _calculate_domain_indicators(cond_df, land_type, by_land_type)

    # Join stratification data
    strat_df = _prepare_area_stratification(
        data["POP_STRATUM"],
        data["POP_PLOT_STRATUM_ASSGN"]
    )

    # Calculate plot-level estimates
    plot_est = _calculate_plot_area_estimates(
        plot_df=data["PLOT"],
        cond_df=cond_df,
        strat_df=strat_df,
        grp_by=grp_by
    )

    # Calculate stratum-level estimates
    stratum_est = _calculate_stratum_area_estimates(plot_est, grp_by)

    # Calculate population estimates
    pop_est = _calculate_population_area_estimates(
        stratum_est, grp_by, totals, variance
    )

    return pop_est


def _apply_area_filters(
    cond_df: pl.DataFrame,
    land_type: str,
    area_domain: Optional[str]
) -> pl.DataFrame:
    """Apply land type and area domain filters."""
    # For area estimation, we don't filter by land type here
    # Instead, we calculate indicators for ratio estimation

    # Apply user-defined area domain if specified
    if area_domain:
        cond_df = cond_df.filter(pl.sql_expr(area_domain))

    return cond_df


def _apply_tree_domain_to_conditions(
    cond_df: pl.DataFrame,
    tree_df: pl.DataFrame,
    tree_domain: str
) -> pl.DataFrame:
    """
    Apply tree domain at the condition level.

    If any tree in a condition meets the criteria, the entire
    condition is included.
    """
    # Filter trees by domain
    qualifying_trees = tree_df.filter(pl.sql_expr(tree_domain))

    # Get unique PLT_CN/CONDID combinations with qualifying trees
    qualifying_conds = (
        qualifying_trees
        .select(["PLT_CN", "CONDID"])
        .unique()
        .with_columns(pl.lit(1).alias("HAS_QUALIFYING_TREE"))
    )

    # Join back to conditions
    cond_df = cond_df.join(
        qualifying_conds,
        on=["PLT_CN", "CONDID"],
        how="left"
    ).with_columns(
        pl.col("HAS_QUALIFYING_TREE").fill_null(0)
    )

    return cond_df


def _add_land_type_categories(cond_df: pl.DataFrame) -> pl.DataFrame:
    """Add land type categories for grouping."""
    return cond_df.with_columns(
        pl.when((pl.col("COND_STATUS_CD") == 1) &
                pl.col("SITECLCD").is_in([1, 2, 3, 4, 5, 6]) &
                (pl.col("RESERVCD") == 0))
        .then(pl.lit("Timber"))
        .when(pl.col("COND_STATUS_CD") == 1)
        .then(pl.lit("Non-Timber Forest"))
        .when(pl.col("COND_STATUS_CD") == 2)
        .then(pl.lit("Non-Forest"))
        .when(pl.col("COND_STATUS_CD").is_in([3, 4]))
        .then(pl.lit("Water"))
        .otherwise(pl.lit("Other"))
        .alias("LAND_TYPE")
    )


def _calculate_domain_indicators(
    cond_df: pl.DataFrame,
    land_type: str,
    by_land_type: bool = False
) -> pl.DataFrame:
    """Calculate domain indicators for area estimation."""
    # When grouping by land type, we want each category as a percentage of total
    if by_land_type and "LAND_TYPE" in cond_df.columns:
        # For by_land_type, landD is 1 for each specific land type category
        # This will be handled in the aggregation by grouping
        cond_df = cond_df.with_columns(
            pl.lit(1).alias("landD")
        )
    else:
        # Land type domain indicator
        if land_type == "forest":
            cond_df = cond_df.with_columns(
                (pl.col("COND_STATUS_CD") == 1).cast(pl.Int32).alias("landD")
            )
        elif land_type == "timber":
            cond_df = cond_df.with_columns(
                ((pl.col("COND_STATUS_CD") == 1) &
                 pl.col("SITECLCD").is_in([1, 2, 3, 4, 5, 6]) &
                 (pl.col("RESERVCD") == 0)).cast(pl.Int32).alias("landD")
            )
        else:  # "all"
            cond_df = cond_df.with_columns(
                pl.lit(1).alias("landD")
            )

    # Area domain indicator (already filtered)
    cond_df = cond_df.with_columns(
        pl.lit(1).alias("aD")
    )

    # Tree domain indicator
    if "HAS_QUALIFYING_TREE" in cond_df.columns:
        cond_df = cond_df.with_columns(
            pl.col("HAS_QUALIFYING_TREE").alias("tD")
        )
    else:
        cond_df = cond_df.with_columns(
            pl.lit(1).alias("tD")
        )

    # Comprehensive domain indicator (numerator)
    # For by_land_type, this will be 1 only for conditions of that land type
    if by_land_type:
        # Each land type category gets its own indicator
        cond_df = cond_df.with_columns(
            pl.col("aD").alias("aDI")
        )
    else:
        cond_df = cond_df.with_columns(
            (pl.col("landD") * pl.col("aD") * pl.col("tD")).alias("aDI")
        )

    # Partial domain indicator (denominator)
    # Based on FIA documentation: denominator depends on the analysis type
    if by_land_type:
        # For byLandType=TRUE: percentages should be of land area only (status 1+2)
        # Use only land conditions for denominator (excludes water)
        cond_df = cond_df.with_columns(
            pl.when(pl.col("COND_STATUS_CD").is_in([1, 2]))
            .then(pl.col("aD"))
            .otherwise(0)
            .alias("pDI")
        )
    else:
        # For specific land_type: denominator is the same as numerator domain
        # This gives percentages relative to the specified domain
        cond_df = cond_df.with_columns(
            (pl.col("landD") * pl.col("aD")).alias("pDI")
        )

    return cond_df


def _prepare_area_stratification(
    stratum_df: pl.DataFrame,
    assgn_df: pl.DataFrame
) -> pl.DataFrame:
    """Prepare stratification data for area estimation."""
    # Filter assignments to current evaluation if EVALID is present
    if "EVALID" in assgn_df.columns and len(assgn_df) > 0:
        current_evalid = assgn_df["EVALID"][0]
        assgn_df = assgn_df.filter(pl.col("EVALID") == current_evalid)

    # Join assignment with stratum info - include both SUBP and MACR adjustment factors
    strat_df = assgn_df.join(
        stratum_df.select([
            "CN", "EXPNS", "ADJ_FACTOR_SUBP", "ADJ_FACTOR_MACR", "P2POINTCNT"
        ]),
        left_on="STRATUM_CN",
        right_on="CN",
        how="inner"
    )

    return strat_df


def _calculate_plot_area_estimates(
    plot_df: pl.DataFrame,
    cond_df: pl.DataFrame,
    strat_df: pl.DataFrame,
    grp_by: Optional[List[str]]
) -> pl.DataFrame:
    """Calculate plot-level area estimates."""
    # Ensure plot_df has PLT_CN column
    if "PLT_CN" not in plot_df.columns:
        plot_df = plot_df.rename({"CN": "PLT_CN"})

    # Calculate area proportions for each plot
    if grp_by:
        cond_groups = ["PLT_CN"] + grp_by
    else:
        cond_groups = ["PLT_CN"]

    # Area meeting criteria (numerator) - include PROP_BASIS for adjustment factor selection
    area_num = (
        cond_df
        .group_by(cond_groups)
        .agg([
            (pl.col("CONDPROP_UNADJ") * pl.col("aDI")).sum().alias("fa"),
            # Get dominant PROP_BASIS for the plot (most common or first)
            pl.col("PROP_BASIS").mode().first().alias("PROP_BASIS")
        ])
    )

    # Total area in domain (denominator) - also need PROP_BASIS
    area_den = (
        cond_df
        .group_by("PLT_CN")
        .agg([
            (pl.col("CONDPROP_UNADJ") * pl.col("pDI")).sum().alias("fad"),
            pl.col("PROP_BASIS").mode().first().alias("PROP_BASIS_DEN")
        ])
    )

    # Join numerator and denominator
    plot_est = area_num.join(area_den, on="PLT_CN", how="left")

    # Use consistent PROP_BASIS (prefer from numerator if available)
    plot_est = plot_est.with_columns(
        pl.coalesce(["PROP_BASIS", "PROP_BASIS_DEN"]).alias("PROP_BASIS")
    ).drop("PROP_BASIS_DEN")

    # Join with stratification - now includes both adjustment factors
    plot_est = plot_est.join(
        strat_df.select(["PLT_CN", "STRATUM_CN", "EXPNS", "ADJ_FACTOR_SUBP", "ADJ_FACTOR_MACR"]),
        on="PLT_CN",
        how="left"
    )

    # Select appropriate adjustment factor based on PROP_BASIS
    plot_est = plot_est.with_columns(
        pl.when(pl.col("PROP_BASIS") == "MACR")
        .then(pl.col("ADJ_FACTOR_MACR"))
        .otherwise(pl.col("ADJ_FACTOR_SUBP"))
        .alias("ADJ_FACTOR")
    )

    # Apply adjustment factor and calculate expanded values
    # CRITICAL: Use direct expansion (plot proportion × adjustment × EXPNS)
    # NOT post-stratified means
    plot_est = plot_est.with_columns([
        (pl.col("fa") * pl.col("ADJ_FACTOR") * pl.col("EXPNS")).alias("fa_expanded"),
        (pl.col("fad") * pl.col("ADJ_FACTOR") * pl.col("EXPNS")).alias("fad_expanded"),
        # Keep original values for variance calculation
        (pl.col("fa") * pl.col("ADJ_FACTOR")).alias("fa"),
        (pl.col("fad") * pl.col("ADJ_FACTOR")).alias("fad")
    ])

    # Fill missing values
    plot_est = plot_est.with_columns([
        pl.col("fa").fill_null(0),
        pl.col("fad").fill_null(0),
        pl.col("fa_expanded").fill_null(0),
        pl.col("fad_expanded").fill_null(0)
    ])

    return plot_est


def _calculate_stratum_area_estimates(
    plot_est: pl.DataFrame,
    grp_by: Optional[List[str]]
) -> pl.DataFrame:
    """Calculate stratum-level area estimates."""
    if grp_by:
        strat_groups = ["STRATUM_CN"] + grp_by
    else:
        strat_groups = ["STRATUM_CN"]

    # Calculate stratum totals and variance components
    stratum_est = (
        plot_est
        .group_by(strat_groups)
        .agg([
            # Sample size
            pl.len().alias("n_h"),
            # Direct expansion totals (sum of expanded values)
            pl.sum("fa_expanded").alias("fa_expanded_total"),
            pl.sum("fad_expanded").alias("fad_expanded_total"),
            # Area estimates for variance (adjusted values)
            pl.mean("fa").alias("fa_bar_h"),
            pl.std("fa", ddof=1).alias("s_fa_h"),
            # Total area for variance (adjusted values)
            pl.mean("fad").alias("fad_bar_h"),
            pl.std("fad", ddof=1).alias("s_fad_h"),
            # Correlation for ratio variance
            pl.corr("fa", "fad").fill_null(0).alias("corr_fa_fad"),
            # Stratum weight
            pl.first("EXPNS").alias("w_h")
        ])
    )

    # Calculate covariance from correlation
    stratum_est = stratum_est.with_columns(
        (pl.col("corr_fa_fad") * pl.col("s_fa_h") * pl.col("s_fad_h")).alias("s_fa_fad_h")
    )

    # Replace null std devs with 0
    stratum_est = stratum_est.with_columns([
        pl.col(c).fill_null(0) for c in ["s_fa_h", "s_fad_h", "s_fa_fad_h"]
    ])

    return stratum_est


def _calculate_population_area_estimates(
    stratum_est: pl.DataFrame,
    grp_by: Optional[List[str]],
    totals: bool,
    variance: bool
) -> pl.DataFrame:
    """Calculate population-level area estimates using direct expansion."""
    if grp_by:
        pop_groups = grp_by
    else:
        pop_groups = []

    # Check if we're doing by_land_type analysis
    # Only treat as by_land_type if the exact column "LAND_TYPE" is the only or primary grouping
    by_land_type = pop_groups == ["LAND_TYPE"] if pop_groups else False

    # Calculate totals using DIRECT EXPANSION, not stratum means
    agg_exprs = [
        # Direct expansion totals
        pl.col("fa_expanded_total").sum().alias("FA_TOTAL"),
        pl.col("fad_expanded_total").sum().alias("FAD_TOTAL"),
        # Variance components (still use stratum-based variance)
        ((pl.col("w_h") ** 2) * (pl.col("s_fa_h") ** 2) / pl.col("n_h")).sum().alias("FA_VAR"),
        ((pl.col("w_h") ** 2) * (pl.col("s_fad_h") ** 2) / pl.col("n_h")).sum().alias("FAD_VAR"),
        # Covariance term
        ((pl.col("w_h") ** 2) * pl.col("s_fa_fad_h") / pl.col("n_h")).sum().alias("COV_FA_FAD"),
        # Sample size
        pl.col("n_h").sum().alias("N_PLOTS")
    ]

    if pop_groups:
        pop_est = stratum_est.group_by(pop_groups).agg(agg_exprs)
    else:
        pop_est = stratum_est.select(agg_exprs)

    # Calculate percentage (ratio estimate)
    # Handle division by zero to avoid infinity values
    if by_land_type:
        # For by_land_type, we need a common denominator across all land types
        # Calculate total land area (sum of all non-water FAD_TOTAL)
        # First, identify water rows if LAND_TYPE column exists
        if "LAND_TYPE" in pop_est.columns:
            # Get total land area (excluding water)
            land_area_total = (
                pop_est
                .filter(~pl.col("LAND_TYPE").str.contains("Water"))
                .select(pl.sum("FAD_TOTAL").alias("TOTAL_LAND_AREA"))
            )[0, 0]

            # Use total land area as denominator for all land types
            pop_est = pop_est.with_columns(
                pl.when(land_area_total == 0)
                .then(0.0)
                .otherwise((pl.col("FA_TOTAL") / land_area_total) * 100)
                .alias("AREA_PERC")
            )
        else:
            # Fallback to regular calculation
            pop_est = pop_est.with_columns(
                pl.when(pl.col("FAD_TOTAL") == 0)
                .then(0.0)
                .otherwise((pl.col("FA_TOTAL") / pl.col("FAD_TOTAL")) * 100)
                .alias("AREA_PERC")
            )
    else:
        # Regular percentage calculation
        pop_est = pop_est.with_columns(
            pl.when(pl.col("FAD_TOTAL") == 0)
            .then(0.0)
            .otherwise((pl.col("FA_TOTAL") / pl.col("FAD_TOTAL")) * 100)
            .alias("AREA_PERC")
        )

    # Calculate ratio variance
    pop_est = pop_est.with_columns(
        ratio_var(
            pl.col("FA_TOTAL"), pl.col("FAD_TOTAL"),
            pl.col("FA_VAR"), pl.col("FAD_VAR"),
            pl.col("COV_FA_FAD")
        ).alias("PERC_VAR_RATIO")
    )

    # Convert to percentage variance
    pop_est = pop_est.with_columns(
        (pl.col("PERC_VAR_RATIO") * 10000).alias("AREA_PERC_VAR")  # (100)^2
    )

    # Calculate standard errors
    pop_est = pop_est.with_columns([
        (pl.col("AREA_PERC_VAR").sqrt()).alias("AREA_PERC_SE"),
        (pl.col("FA_VAR").sqrt() / pl.col("FA_TOTAL") * 100).alias("AREA_SE")
    ])

    # Select final columns
    cols = pop_groups + ["AREA_PERC", "N_PLOTS"]

    if totals:
        # Add total area in acres
        pop_est = pop_est.with_columns(
            pl.col("FA_TOTAL").alias("AREA")
        )
        cols.append("AREA")

    if variance:
        cols.append("AREA_PERC_VAR")
        if totals:
            cols.append("FA_VAR")
    else:
        cols.append("AREA_PERC_SE")
        if totals:
            cols.append("AREA_SE")

    return pop_est.select(cols)
