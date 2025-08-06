"""
Trees Per Acre (TPA) estimation following FIA methodology.

This module implements the post-stratified ratio-of-means estimator for
calculating trees per acre from FIA data, following Bechtold & Patterson (2005).
"""

from typing import List, Optional

import polars as pl

from ..constants.constants import (
    MathConstants,
    PlotBasis,
)
from ..filters.common import (
    apply_area_filters_common,
    apply_tree_filters_common,
)
from .utils import ratio_var


def tpa(
    db,
    grp_by: Optional[List[str]] = None,
    by_species: bool = False,
    by_size_class: bool = False,
    land_type: str = "forest",
    tree_type: str = "live",
    tree_domain: Optional[str] = None,
    area_domain: Optional[str] = None,
    method: str = "TI",
    lambda_: float = 0.5,
    totals: bool = False,
    variance: bool = False,
    most_recent: bool = False,
) -> pl.DataFrame:
    """
    Estimate Trees Per Acre (TPA) and Basal Area per Acre (BAA) from FIA data.

    Uses the post-stratified ratio-of-means estimator following FIA methodology.

    Parameters
    ----------
    db : FIA
        FIA database object with data loaded
    grp_by : list of str, optional
        Columns to group estimates by
    by_species : bool, default False
        Include species-level estimates
    by_size_class : bool, default False
        Include size class estimates (2-inch diameter classes)
    land_type : str, default "forest"
        Land type filter: "forest" or "timber"
    tree_type : str, default "live"
        Tree type filter: "live", "dead", "gs", or "all"
    tree_domain : str, optional
        SQL-like condition to filter trees (e.g., "DIA > 10")
    area_domain : str, optional
        SQL-like condition to filter area (e.g., "FORTYPCD == 171")
    method : str, default "TI"
        Estimation method (currently only "TI" supported)
    lambda_ : float, default 0.5
        Temporal weighting parameter (not used for TI)
    totals : bool, default False
        Include total tree/BA counts in addition to per-acre values
    variance : bool, default False
        Return variance instead of standard error
    most_recent : bool, default False
        Use only most recent evaluation

    Returns
    -------
    pl.DataFrame
        DataFrame with TPA and BAA estimates by group
    """
    # Ensure tables are loaded
    db.load_table("PLOT")
    db.load_table("COND")
    db.load_table("TREE")
    db.load_table("POP_STRATUM")
    db.load_table("POP_PLOT_STRATUM_ASSGN")

    # Get filtered data
    plots = db.get_plots()
    data = {
        "PLOT": plots,
        "COND": db.get_conditions(),
        "TREE": db.get_trees(),
        "POP_STRATUM": db.tables["POP_STRATUM"].collect(),
    }

    # Get plot-stratum assignments filtered by current evaluation
    if db.evalid:
        ppsa = (
            db.tables["POP_PLOT_STRATUM_ASSGN"]
            .filter(pl.col("EVALID").is_in(db.evalid))
            .collect()
        )
    else:
        # Filter to plots we have
        plot_cns = (
            plots["CN"].to_list()
            if "CN" in plots.columns
            else plots["PLT_CN"].to_list()
        )
        ppsa = (
            db.tables["POP_PLOT_STRATUM_ASSGN"]
            .filter(pl.col("PLT_CN").is_in(plot_cns))
            .collect()
        )

    data["POP_PLOT_STRATUM_ASSGN"] = ppsa

    # Apply domain filters
    tree_df = apply_tree_filters_common(data["TREE"], tree_type, tree_domain, require_diameter_thresholds=True)
    cond_df = apply_area_filters_common(data["COND"], land_type, area_domain)

    # Calculate TREE_BASIS for each tree
    from ..filters.classification import assign_tree_basis
    tree_df = assign_tree_basis(tree_df, data["PLOT"])

    # Calculate basal area for each tree
    tree_df = tree_df.with_columns(
        (MathConstants.BASAL_AREA_FACTOR * pl.col("DIA") ** 2).alias("BASAL_AREA")
    )

    # Add species information if requested
    if by_species:
        tree_df = _add_species_info(tree_df, db)
        if grp_by is None:
            grp_by = []
        grp_by.extend(["SPCD", "COMMON_NAME", "SCIENTIFIC_NAME"])

    # Add size class if requested
    if by_size_class:
        from ..filters.classification import assign_size_class
        tree_df = assign_size_class(tree_df)
        if grp_by is None:
            grp_by = []
        grp_by.append("SIZE_CLASS")

    # Join stratification data
    strat_df = _prepare_stratification(
        data["POP_STRATUM"], data["POP_PLOT_STRATUM_ASSGN"]
    )

    # Calculate plot-level estimates
    plot_est = _calculate_plot_estimates(
        plot_df=data["PLOT"],
        cond_df=cond_df,
        tree_df=tree_df,
        strat_df=strat_df,
        grp_by=grp_by,
    )

    # Calculate stratum-level estimates
    stratum_est = _calculate_stratum_estimates(plot_est, grp_by)

    # Calculate population estimates
    pop_est = _calculate_population_estimates(stratum_est, grp_by, totals, variance)

    return pop_est


# _assign_tree_basis moved to classification.py module


def _add_species_info(tree_df: pl.DataFrame, db) -> pl.DataFrame:
    """Add species common and scientific names."""
    # This would typically join with REF_SPECIES table
    # For now, just ensure SPCD is included
    if "SPCD" not in tree_df.columns:
        raise ValueError("SPCD column not found in TREE table")

    # Placeholder for species names - in real implementation would join with reference table
    tree_df = tree_df.with_columns(
        [
            pl.col("SPCD").cast(pl.Utf8).alias("COMMON_NAME"),
            pl.col("SPCD").cast(pl.Utf8).alias("SCIENTIFIC_NAME"),
        ]
    )

    return tree_df


# _add_size_class moved to classification.py module


def _prepare_stratification(
    stratum_df: pl.DataFrame, assgn_df: pl.DataFrame
) -> pl.DataFrame:
    """Prepare stratification data with adjustment factors."""
    # Filter assignments to current evaluation if EVALID is present
    if "EVALID" in assgn_df.columns and len(assgn_df) > 0:
        # Get the EVALID from the first row (they should all be the same after clipping)
        current_evalid = assgn_df["EVALID"][0]
        assgn_df = assgn_df.filter(pl.col("EVALID") == current_evalid)

    # Join assignment with stratum info
    strat_df = assgn_df.join(
        stratum_df.select(
            [
                "CN",
                "EXPNS",
                "ADJ_FACTOR_MICR",
                "ADJ_FACTOR_SUBP",
                "ADJ_FACTOR_MACR",
                "P2POINTCNT",
            ]
        ),
        left_on="STRATUM_CN",
        right_on="CN",
        how="inner",
    )

    return strat_df


def _calculate_plot_estimates(
    plot_df: pl.DataFrame,
    cond_df: pl.DataFrame,
    tree_df: pl.DataFrame,
    strat_df: pl.DataFrame,
    grp_by: Optional[List[str]],
) -> pl.DataFrame:
    """Calculate plot-level TPA and BAA estimates."""
    # Ensure plot_df has PLT_CN column
    if "PLT_CN" not in plot_df.columns:
        plot_df = plot_df.rename({"CN": "PLT_CN"})

    # First, calculate forest area proportion for each plot
    area_by_plot = cond_df.group_by("PLT_CN").agg(
        pl.sum("CONDPROP_UNADJ").alias("PROP_FOREST")
    )

    # Calculate tree-level estimates with proper expansion
    if grp_by:
        tree_groups = ["PLT_CN", "TREE_BASIS"] + grp_by
    else:
        tree_groups = ["PLT_CN", "TREE_BASIS"]

    tree_est = tree_df.group_by(tree_groups).agg(
        [
            pl.sum("TPA_UNADJ").alias("TPA_UNADJ_SUM"),
            (pl.col("TPA_UNADJ") * pl.col("BASAL_AREA")).sum().alias("BAA_UNADJ_SUM"),
        ]
    )

    # Join with stratification to get adjustment factors
    tree_est = tree_est.join(
        strat_df.select(
            [
                "PLT_CN",
                "STRATUM_CN",
                "EXPNS",
                "ADJ_FACTOR_MICR",
                "ADJ_FACTOR_SUBP",
                "ADJ_FACTOR_MACR",
            ]
        ),
        on="PLT_CN",
        how="left",
    )

    # Apply adjustment factors using the new adjustment module
    from ..filters.adjustment import apply_adjustment_factors

    tree_est = apply_adjustment_factors(
        tree_est,
        value_columns=["TPA_UNADJ_SUM", "BAA_UNADJ_SUM"],
        basis_column="TREE_BASIS",
        adj_factor_columns={
            PlotBasis.MICROPLOT: "ADJ_FACTOR_MICR",
            PlotBasis.SUBPLOT: "ADJ_FACTOR_SUBP",
            PlotBasis.MACROPLOT: "ADJ_FACTOR_MACR",
        }
    ).rename({
        "TPA_UNADJ_SUM_ADJ": "TPA_ADJ",
        "BAA_UNADJ_SUM_ADJ": "BAA_ADJ"
    })

    # Sum across TREE_BASIS within plot
    if grp_by:
        plot_groups = ["PLT_CN", "STRATUM_CN", "EXPNS"] + grp_by
    else:
        plot_groups = ["PLT_CN", "STRATUM_CN", "EXPNS"]

    plot_est = tree_est.group_by(plot_groups).agg(
        [pl.sum("TPA_ADJ").alias("TPA_PLT"), pl.sum("BAA_ADJ").alias("BAA_PLT")]
    )

    # Join with forest area proportion
    plot_est = plot_est.join(area_by_plot, on="PLT_CN", how="left")

    # Fill missing values with 0
    plot_est = plot_est.with_columns(
        [
            pl.col("TPA_PLT").fill_null(0),
            pl.col("BAA_PLT").fill_null(0),
            pl.col("PROP_FOREST").fill_null(0),
        ]
    )

    return plot_est


def _calculate_stratum_estimates(
    plot_est: pl.DataFrame, grp_by: Optional[List[str]]
) -> pl.DataFrame:
    """Calculate stratum-level estimates."""
    if grp_by:
        strat_groups = ["STRATUM_CN"] + grp_by
    else:
        strat_groups = ["STRATUM_CN"]

    # Calculate stratum means and variance components
    stratum_est = plot_est.group_by(strat_groups).agg(
        [
            # Sample size
            pl.count().alias("n_h"),
            # Tree estimates
            pl.mean("TPA_PLT").alias("y_bar_h"),
            pl.std("TPA_PLT", ddof=1).alias("s_yh"),
            # Basal area estimates
            pl.mean("BAA_PLT").alias("b_bar_h"),
            pl.std("BAA_PLT", ddof=1).alias("s_bh"),
            # Forest area
            pl.mean("PROP_FOREST").alias("x_bar_h"),
            pl.std("PROP_FOREST", ddof=1).alias("s_xh"),
            # Covariances for ratio estimation - using correlation and std devs
            # cov(X,Y) = corr(X,Y) * std(X) * std(Y)
            pl.corr("TPA_PLT", "PROP_FOREST").fill_null(0).alias("corr_yx"),
            pl.corr("BAA_PLT", "PROP_FOREST").fill_null(0).alias("corr_bx"),
            # Stratum weight (first EXPNS value - they should all be the same within stratum)
            pl.first("EXPNS").alias("w_h"),
        ]
    )

    # Calculate covariances from correlations
    stratum_est = stratum_est.with_columns(
        [
            (pl.col("corr_yx") * pl.col("s_yh") * pl.col("s_xh")).alias("s_yxh"),
            (pl.col("corr_bx") * pl.col("s_bh") * pl.col("s_xh")).alias("s_bxh"),
        ]
    )

    # Replace null std devs with 0
    stratum_est = stratum_est.with_columns(
        [pl.col(c).fill_null(0) for c in ["s_yh", "s_bh", "s_xh", "s_yxh", "s_bxh"]]
    )

    return stratum_est


def _calculate_population_estimates(
    stratum_est: pl.DataFrame, grp_by: Optional[List[str]], totals: bool, variance: bool
) -> pl.DataFrame:
    """Calculate population-level estimates using ratio-of-means estimator."""
    if grp_by:
        pop_groups = grp_by
    else:
        pop_groups = []

    # Calculate totals across strata
    agg_exprs = [
        # Weighted means
        (pl.col("y_bar_h") * pl.col("w_h")).sum().alias("TREE_TOTAL"),
        (pl.col("b_bar_h") * pl.col("w_h")).sum().alias("BA_TOTAL"),
        (pl.col("x_bar_h") * pl.col("w_h")).sum().alias("AREA_TOTAL"),
        # Variance components
        ((pl.col("w_h") ** 2) * (pl.col("s_yh") ** 2) / pl.col("n_h"))
        .sum()
        .alias("TREE_VAR"),
        ((pl.col("w_h") ** 2) * (pl.col("s_bh") ** 2) / pl.col("n_h"))
        .sum()
        .alias("BA_VAR"),
        ((pl.col("w_h") ** 2) * (pl.col("s_xh") ** 2) / pl.col("n_h"))
        .sum()
        .alias("AREA_VAR"),
        # Covariance terms for ratio variance
        ((pl.col("w_h") ** 2) * pl.col("s_yxh") / pl.col("n_h")).sum().alias("COV_YX"),
        ((pl.col("w_h") ** 2) * pl.col("s_bxh") / pl.col("n_h")).sum().alias("COV_BX"),
        # Sample size
        pl.col("n_h").sum().alias("N_PLOTS"),
    ]

    if pop_groups:
        pop_est = stratum_est.group_by(pop_groups).agg(agg_exprs)
    else:
        # No grouping - calculate overall totals
        pop_est = stratum_est.select(agg_exprs)

    # Calculate ratios (per acre values)
    pop_est = pop_est.with_columns(
        [
            (pl.col("TREE_TOTAL") / pl.col("AREA_TOTAL")).alias("TPA"),
            (pl.col("BA_TOTAL") / pl.col("AREA_TOTAL")).alias("BAA"),
        ]
    )

    # Calculate ratio variances using delta method
    pop_est = pop_est.with_columns(
        [
            ratio_var(
                pl.col("TREE_TOTAL"),
                pl.col("AREA_TOTAL"),
                pl.col("TREE_VAR"),
                pl.col("AREA_VAR"),
                pl.col("COV_YX"),
            ).alias("TPA_VAR"),
            ratio_var(
                pl.col("BA_TOTAL"),
                pl.col("AREA_TOTAL"),
                pl.col("BA_VAR"),
                pl.col("AREA_VAR"),
                pl.col("COV_BX"),
            ).alias("BAA_VAR"),
        ]
    )

    # Calculate standard errors
    pop_est = pop_est.with_columns(
        [
            (pl.col("TPA_VAR").sqrt() / pl.col("TPA") * 100).alias("TPA_SE"),
            (pl.col("BAA_VAR").sqrt() / pl.col("BAA") * 100).alias("BAA_SE"),
            (pl.col("TREE_VAR").sqrt() / pl.col("TREE_TOTAL") * 100).alias(
                "TREE_TOTAL_SE"
            ),
            (pl.col("BA_VAR").sqrt() / pl.col("BA_TOTAL") * 100).alias("BA_TOTAL_SE"),
            (pl.col("AREA_VAR").sqrt() / pl.col("AREA_TOTAL") * 100).alias(
                "AREA_TOTAL_SE"
            ),
        ]
    )

    # Select final columns
    cols = pop_groups + ["TPA", "BAA", "N_PLOTS"]

    if totals:
        cols.extend(["TREE_TOTAL", "BA_TOTAL", "AREA_TOTAL"])

    if variance:
        cols.extend(["TPA_VAR", "BAA_VAR"])
        if totals:
            cols.extend(["TREE_VAR", "BA_VAR", "AREA_VAR"])
    else:
        cols.extend(["TPA_SE", "BAA_SE"])
        if totals:
            cols.extend(["TREE_TOTAL_SE", "BA_TOTAL_SE", "AREA_TOTAL_SE"])

    return pop_est.select(cols)
