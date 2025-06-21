"""
Mortality estimation functions for pyFIA.

This module implements design-based estimation of tree mortality
following Bechtold & Patterson (2005) procedures.
"""

import polars as pl
import numpy as np
from typing import Optional, Union, List, Dict, Any
from .core import FIA
from .estimation_utils import (
    merge_estimation_data,
    calculate_adjustment_factors,
    calculate_stratum_estimates,
    calculate_population_estimates,
    apply_domain_filter,
    calculate_ratio_estimates,
    summarize_by_groups
)


def mortality(db: Union[FIA, str],
              grpBy: Optional[Union[str, List[str]]] = None,
              polys: Optional[Any] = None,
              returnSpatial: bool = False,
              bySpecies: bool = False,
              bySizeClass: bool = False,
              landType: str = 'forest',
              treeType: str = 'all',
              method: str = 'TI',
              lambda_: float = 0.5,
              treeDomain: Optional[str] = None,
              areaDomain: Optional[str] = None,
              totals: bool = False,
              variance: bool = False,
              byPlot: bool = False,
              returnComponents: bool = False,
              nCores: int = 1,
              remote: bool = False,
              mr: bool = False,
              **kwargs) -> pl.DataFrame:
    """
    Estimate tree mortality from FIA data.
    
    This function produces estimates of annual tree mortality rates using 
    remeasurement data from the FIA database. Mortality is calculated as
    the annual rate of trees dying between measurements.
    
    Args:
        db: FIA database object or path to database
        grpBy: Grouping variables for estimation
        polys: Spatial polygons for estimation
        returnSpatial: Return sf spatial object
        bySpecies: Report estimates by species
        bySizeClass: Report estimates by size class
        landType: Land type filter ('forest' or 'all')
        treeType: Tree type filter (typically 'all' for mortality)
        method: Estimation method ('TI', 'SMA', 'LMA', 'EMA', 'ANNUAL')
        lambda_: Lambda parameter for moving average
        treeDomain: Logical expression for tree subset
        areaDomain: Logical expression for area subset
        totals: Return total estimates
        variance: Return variance components
        byPlot: Return plot-level estimates
        returnComponents: Return component values (mortality, survivor trees)
        nCores: Number of cores for parallel processing
        remote: Use remote database
        mr: Most recent subset
        
    Returns:
        DataFrame with mortality estimates
    """
    # Initialize FIA database if needed
    if isinstance(db, str):
        fia = FIA(db)
    else:
        fia = db
    
    # Apply most recent filter if requested
    # For mortality, we need GRM (Growth, Removal, Mortality) evaluations
    if mr:
        fia.clip_most_recent(eval_type='GRM')
    
    # Get estimation data
    data = fia.prepare_estimation_data()
    
    # For mortality, we need TREE_GRM tables
    # These contain remeasurement information
    tree_grm_begin = fia._reader.read_table('TREE_GRM_BEGIN', lazy=False)
    tree_grm_component = fia._reader.read_table('TREE_GRM_COMPONENT', lazy=False)
    
    # Ensure we have DataFrames (for mypy)
    assert isinstance(tree_grm_begin, pl.DataFrame)
    assert isinstance(tree_grm_component, pl.DataFrame)
    
    # Merge with plot data
    plot_data = data['plot']
    cond_data = data['cond']
    
    # Join GRM data
    tree_mort = tree_grm_component.filter(
        pl.col('COMPONENT').is_in(['MORTALITY', 'MORT'])
    )
    
    # Join with beginning tree data
    tree_mort = tree_mort.join(
        tree_grm_begin.select(['TRE_CN', 'PLT_CN', 'CONDID', 'SPCD', 'DIA_BEGIN', 
                              'TPA_UNADJ_BEGIN', 'SUBPTYP_BEGIN']),
        on='TRE_CN',
        how='inner'
    )
    
    # Join with plot data
    tree_plot = tree_mort.join(
        plot_data.select(['CN', 'INVYR', 'STATECD', 'UNITCD', 'COUNTYCD', 
                         'PLOT', 'DESIGNCD', 'EVALID', 'REMPER']),
        left_on='PLT_CN',
        right_on='CN',
        how='inner'
    )
    
    # Join with condition data
    tree_plot_cond = tree_plot.join(
        cond_data.select(['PLT_CN', 'CONDID', 'COND_STATUS_CD', 'FORTYPCD', 
                         'STDSZCD', 'SITECLCD']),
        on=['PLT_CN', 'CONDID'],
        how='left'
    )
    
    # Apply land type filter
    if landType == 'forest':
        tree_plot_cond = tree_plot_cond.filter(pl.col('COND_STATUS_CD') == 1)
    
    # Apply domain filters
    if treeDomain:
        tree_plot_cond = tree_plot_cond.filter(pl.Expr.from_json(treeDomain))
    
    if areaDomain:
        tree_plot_cond = tree_plot_cond.filter(pl.Expr.from_json(areaDomain))
    
    # Calculate annual mortality
    # Mortality per acre = TPA_UNADJ * adjustment factors / REMPER
    tree_plot_cond = tree_plot_cond.with_columns([
        (pl.col('TPA_UNADJ_COMPONENT') * pl.col('ADJ_FACTOR_SUBP') / 
         pl.col('REMPER')).alias('MORT_TPA_YR'),
        (pl.col('VOLCFNET_COMPONENT') * pl.col('TPA_UNADJ_COMPONENT') * 
         pl.col('ADJ_FACTOR_SUBP') / pl.col('REMPER')).alias('MORT_VOL_YR'),
        (pl.col('DRYBIO_AG_COMPONENT') * pl.col('TPA_UNADJ_COMPONENT') * 
         pl.col('ADJ_FACTOR_SUBP') / pl.col('REMPER')).alias('MORT_BIO_YR')
    ])
    
    # Create grouping columns
    group_cols = ['EVALID']
    if grpBy:
        if isinstance(grpBy, str):
            group_cols.append(grpBy)
        else:
            group_cols.extend(grpBy)
    
    if bySpecies:
        group_cols.append('SPCD')
    
    if bySizeClass:
        # Add size class based on beginning diameter
        tree_plot_cond = tree_plot_cond.with_columns(
            pl.when(pl.col('DIA_BEGIN') < 5.0).then(pl.lit('Saplings'))
            .when(pl.col('DIA_BEGIN') < 10.0).then(pl.lit('Small'))
            .when(pl.col('DIA_BEGIN') < 20.0).then(pl.lit('Medium'))
            .otherwise(pl.lit('Large'))
            .alias('sizeClass')
        )
        group_cols.append('sizeClass')
    
    # Aggregate to plot level
    agg_exprs = [
        pl.col('MORT_TPA_YR').sum().alias('MORT_TPA_PLOT'),
        pl.col('MORT_VOL_YR').sum().alias('MORT_VOL_PLOT'),
        pl.col('MORT_BIO_YR').sum().alias('MORT_BIO_PLOT')
    ]
    
    plot_mort = (tree_plot_cond
        .group_by(['PLT_CN'] + group_cols)
        .agg(agg_exprs)
    )
    
    # Get all plots (including those with no mortality)
    all_plots = plot_data.select(['CN', 'EVALID']).rename({'CN': 'PLT_CN'})
    
    # Join with stratum info
    ppsa = data['pop_plot_stratum_assgn']
    pop_stratum = data['pop_stratum']
    
    plot_stratum = all_plots.join(
        ppsa.select(['PLT_CN', 'STRATUM_CN']),
        on='PLT_CN',
        how='left'
    ).join(
        pop_stratum.select(['CN', 'EXPNS', 'ADJ_FACTOR_SUBP']),
        left_on='STRATUM_CN',
        right_on='CN',
        how='left'
    )
    
    # Full join to include all plots
    full_data = plot_stratum.join(
        plot_mort,
        on=['PLT_CN', 'EVALID'],
        how='left'
    ).with_columns([
        pl.col('MORT_TPA_PLOT').fill_null(0.0),
        pl.col('MORT_VOL_PLOT').fill_null(0.0),
        pl.col('MORT_BIO_PLOT').fill_null(0.0)
    ])
    
    # If we need survivor trees for mortality rate calculation
    if returnComponents:
        # Get survivor tree counts
        tree_surv = tree_grm_component.filter(
            pl.col('COMPONENT') == 'SURVIVOR'
        )
        
        # Similar processing for survivors...
        # This would follow the same pattern as mortality
    
    # Calculate stratum-level estimates
    stratum_estimates = (full_data
        .group_by(['STRATUM_CN'] + group_cols)
        .agg([
            pl.count().alias('n_plots'),
            pl.col('MORT_TPA_PLOT').mean().alias('mean_mort_tpa'),
            pl.col('MORT_TPA_PLOT').var().alias('var_mort_tpa'),
            pl.col('MORT_VOL_PLOT').mean().alias('mean_mort_vol'),
            pl.col('MORT_VOL_PLOT').var().alias('var_mort_vol'),
            pl.col('MORT_BIO_PLOT').mean().alias('mean_mort_bio'),
            pl.col('MORT_BIO_PLOT').var().alias('var_mort_bio'),
            pl.col('EXPNS').first().alias('expns')
        ])
    )
    
    # Calculate population estimates
    if totals:
        # Total mortality estimate
        pop_estimates = (stratum_estimates
            .group_by(group_cols)
            .agg([
                (pl.col('mean_mort_tpa') * pl.col('expns') * pl.col('n_plots')).sum().alias('MORT_TPA_TOTAL'),
                ((pl.col('expns')**2 * pl.col('var_mort_tpa') / pl.col('n_plots'))).sum().alias('MORT_TPA_VAR'),
                (pl.col('mean_mort_vol') * pl.col('expns') * pl.col('n_plots')).sum().alias('MORT_VOL_TOTAL'),
                ((pl.col('expns')**2 * pl.col('var_mort_vol') / pl.col('n_plots'))).sum().alias('MORT_VOL_VAR'),
                (pl.col('mean_mort_bio') * pl.col('expns') * pl.col('n_plots')).sum().alias('MORT_BIO_TOTAL'),
                ((pl.col('expns')**2 * pl.col('var_mort_bio') / pl.col('n_plots'))).sum().alias('MORT_BIO_VAR'),
                (pl.col('n_plots')).sum().alias('nPlots')
            ])
        )
        
        # Add SE and CV
        pop_estimates = pop_estimates.with_columns([
            pl.col('MORT_TPA_VAR').sqrt().alias('MORT_TPA_SE'),
            (pl.col('MORT_TPA_VAR').sqrt() / pl.col('MORT_TPA_TOTAL') * 100).alias('MORT_TPA_CV'),
            pl.col('MORT_VOL_VAR').sqrt().alias('MORT_VOL_SE'),
            (pl.col('MORT_VOL_VAR').sqrt() / pl.col('MORT_VOL_TOTAL') * 100).alias('MORT_VOL_CV'),
            pl.col('MORT_BIO_VAR').sqrt().alias('MORT_BIO_SE'),
            (pl.col('MORT_BIO_VAR').sqrt() / pl.col('MORT_BIO_TOTAL') * 100).alias('MORT_BIO_CV')
        ])
        
    else:
        # Per acre estimates
        # Get total forest area
        area_data = _calculate_forest_area(data, landType, areaDomain)
        
        # Join area with estimates
        pop_estimates = (stratum_estimates
            .group_by(group_cols)
            .agg([
                (pl.col('mean_mort_tpa') * pl.col('expns')).sum().alias('MORT_TPA_TOTAL'),
                ((pl.col('expns')**2 * pl.col('var_mort_tpa') / pl.col('n_plots'))).sum().alias('MORT_TPA_VAR'),
                (pl.col('mean_mort_vol') * pl.col('expns')).sum().alias('MORT_VOL_TOTAL'),
                ((pl.col('expns')**2 * pl.col('var_mort_vol') / pl.col('n_plots'))).sum().alias('MORT_VOL_VAR'),
                (pl.col('mean_mort_bio') * pl.col('expns')).sum().alias('MORT_BIO_TOTAL'),
                ((pl.col('expns')**2 * pl.col('var_mort_bio') / pl.col('n_plots'))).sum().alias('MORT_BIO_VAR'),
                (pl.col('n_plots')).sum().alias('nPlots')
            ])
        ).join(
            area_data,
            on='EVALID',
            how='left'
        )
        
        # Calculate per acre values
        pop_estimates = pop_estimates.with_columns([
            (pl.col('MORT_TPA_TOTAL') / pl.col('AREA_TOTAL')).alias('MORT_TPA_AC'),
            ((pl.col('MORT_TPA_VAR') / (pl.col('AREA_TOTAL')**2))).alias('MORT_TPA_VAR_AC'),
            (pl.col('MORT_VOL_TOTAL') / pl.col('AREA_TOTAL')).alias('MORT_VOL_AC'),
            ((pl.col('MORT_VOL_VAR') / (pl.col('AREA_TOTAL')**2))).alias('MORT_VOL_VAR_AC'),
            (pl.col('MORT_BIO_TOTAL') / pl.col('AREA_TOTAL')).alias('MORT_BIO_AC'),
            ((pl.col('MORT_BIO_VAR') / (pl.col('AREA_TOTAL')**2))).alias('MORT_BIO_VAR_AC')
        ]).with_columns([
            pl.col('MORT_TPA_VAR_AC').sqrt().alias('MORT_TPA_SE'),
            (pl.col('MORT_TPA_VAR_AC').sqrt() / pl.col('MORT_TPA_AC') * 100).alias('MORT_TPA_CV'),
            pl.col('MORT_VOL_VAR_AC').sqrt().alias('MORT_VOL_SE'),
            (pl.col('MORT_VOL_VAR_AC').sqrt() / pl.col('MORT_VOL_AC') * 100).alias('MORT_VOL_CV'),
            pl.col('MORT_BIO_VAR_AC').sqrt().alias('MORT_BIO_SE'),
            (pl.col('MORT_BIO_VAR_AC').sqrt() / pl.col('MORT_BIO_AC') * 100).alias('MORT_BIO_CV')
        ])
    
    # Select final columns
    if totals:
        result_cols = group_cols + [
            'MORT_TPA_TOTAL', 'MORT_TPA_SE', 'MORT_TPA_CV',
            'MORT_VOL_TOTAL', 'MORT_VOL_SE', 'MORT_VOL_CV',
            'MORT_BIO_TOTAL', 'MORT_BIO_SE', 'MORT_BIO_CV',
            'nPlots'
        ]
    else:
        result_cols = group_cols + [
            'MORT_TPA_AC', 'MORT_TPA_SE', 'MORT_TPA_CV',
            'MORT_VOL_AC', 'MORT_VOL_SE', 'MORT_VOL_CV',
            'MORT_BIO_AC', 'MORT_BIO_SE', 'MORT_BIO_CV',
            'nPlots', 'AREA_TOTAL'
        ]
    
    result = pop_estimates.select(result_cols)
    
    # Add species names if grouped by species
    if bySpecies:
        # Would join with REF_SPECIES table here
        pass
    
    return result


def _calculate_forest_area(data: Dict[str, pl.DataFrame], 
                          landType: str,
                          areaDomain: Optional[str]) -> pl.DataFrame:
    """Calculate forest area for mortality denominators."""
    cond_data = data['cond']
    plot_data = data['plot']
    ppsa = data['pop_plot_stratum_assgn']
    pop_stratum = data['pop_stratum']
    
    # Filter conditions
    if landType == 'forest':
        cond_data = cond_data.filter(pl.col('COND_STATUS_CD') == 1)
    
    if areaDomain:
        cond_data = cond_data.filter(pl.Expr.from_json(areaDomain))
    
    # Calculate condition proportions
    cond_props = (cond_data
        .group_by(['PLT_CN'])
        .agg(
            pl.col('CONDPROP_UNADJ').sum().alias('COND_PROP')
        )
    )
    
    # Join with plot and stratum data
    plot_area = (plot_data
        .select(['CN', 'EVALID'])
        .rename({'CN': 'PLT_CN'})
        .join(cond_props, on='PLT_CN', how='left')
        .with_columns(
            pl.col('COND_PROP').fill_null(0.0)
        )
        .join(ppsa.select(['PLT_CN', 'STRATUM_CN']), on='PLT_CN', how='left')
        .join(pop_stratum.select(['CN', 'EXPNS']), left_on='STRATUM_CN', right_on='CN', how='left')
    )
    
    # Calculate total area
    area_estimates = (plot_area
        .group_by('EVALID')
        .agg(
            (pl.col('COND_PROP') * pl.col('EXPNS')).sum().alias('AREA_TOTAL')
        )
    )
    
    return area_estimates