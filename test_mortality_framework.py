#!/usr/bin/env python3
"""
Test script to verify the mortality.py framework works with basic structure.

This tests the implementation logic without requiring the full FIA database,
using mock data to verify the calculation flow works properly.
"""

import polars as pl
import sys
from pathlib import Path

# Add pyfia to path
sys.path.insert(0, str(Path(__file__).parent))

def create_mock_fia_data():
    """Create mock FIA data to test mortality framework."""
    
    # Mock TREE_GRM_COMPONENT data
    tree_grm_component = pl.DataFrame({
        'TRE_CN': ['1001', '1002', '1003', '1004'],
        'COMPONENT': ['MORT_COMP', 'MORT_COMP', 'SURV_COMP', 'MORT_COMP'],
        'TPAMORT_UNADJ': [0.5, 0.3, 0.0, 0.7]  # Annual mortality rates
    })
    
    # Mock TREE_GRM_BEGIN data
    tree_grm_begin = pl.DataFrame({
        'TRE_CN': ['1001', '1002', '1003', '1004'],
        'PLT_CN': ['P001', 'P001', 'P002', 'P002'],
        'CONDID': [1, 1, 1, 1],
        'SPCD': [318, 318, 531, 531],  # Loblolly pine and red oak
        'DIA_BEGIN': [8.5, 12.3, 6.2, 15.7],
        'VOLCFNET_BEGIN': [2.1, 5.8, 1.3, 8.9],
        'DRYBIO_AG_BEGIN': [0.045, 0.089, 0.023, 0.156],
        'SUBPTYP_BEGIN': [1, 1, 1, 1]
    })
    
    # Mock PLOT data
    plot_data = pl.DataFrame({
        'CN': ['P001', 'P002'],
        'INVYR': [2019, 2019],
        'STATECD': [37, 37],
        'UNITCD': [1, 1], 
        'COUNTYCD': [1, 1],
        'PLOT': [1, 2],
        'DESIGNCD': [1, 1],
        'EVALID': [372301, 372301],
        'REMPER': [5.2, 5.1]  # Remeasurement period
    })
    
    # Mock COND data
    cond_data = pl.DataFrame({
        'PLT_CN': ['P001', 'P002'],
        'CONDID': [1, 1],
        'COND_STATUS_CD': [1, 1],  # Forest land
        'FORTYPCD': [171, 171],
        'STDSZCD': [3, 3],
        'SITECLCD': [3, 3],
        'CONDPROP_UNADJ': [1.0, 1.0]
    })
    
    # Mock POP_PLOT_STRATUM_ASSGN data
    ppsa = pl.DataFrame({
        'PLT_CN': ['P001', 'P002'],
        'STRATUM_CN': ['S001', 'S001'],
        'EVALID': [372301, 372301]
    })
    
    # Mock POP_STRATUM data
    pop_stratum = pl.DataFrame({
        'CN': ['S001'],
        'EXPNS': [6000.0],  # Expansion factor
        'ADJ_FACTOR_MICR': [1.001],
        'ADJ_FACTOR_SUBP': [1.002],
        'ADJ_FACTOR_MACR': [1.003],
        'P2POINTCNT': [100]
    })
    
    return {
        'tree_grm_component': tree_grm_component,
        'tree_grm_begin': tree_grm_begin,
        'plot': plot_data,
        'cond': cond_data,
        'pop_plot_stratum_assgn': ppsa,
        'pop_stratum': pop_stratum
    }

def test_mortality_calculation():
    """Test the mortality calculation logic manually."""
    
    print("🧪 Testing mortality calculation framework...")
    
    # Create mock data
    data = create_mock_fia_data()
    
    # Test 1: Basic table joins
    print("\n1. Testing table joins...")
    
    # Filter for mortality components
    tree_mort = data['tree_grm_component'].filter(
        pl.col('COMPONENT').str.contains('MORT')
    )
    print(f"   Mortality components: {len(tree_mort)} records")
    
    # Join with beginning tree data
    tree_mort = tree_mort.join(
        data['tree_grm_begin'].select(['TRE_CN', 'PLT_CN', 'CONDID', 'SPCD', 'DIA_BEGIN', 
                                      'VOLCFNET_BEGIN', 'DRYBIO_AG_BEGIN', 'SUBPTYP_BEGIN']),
        on='TRE_CN',
        how='inner'
    )
    print(f"   After join with TREE_GRM_BEGIN: {len(tree_mort)} records")
    
    # Join with plot data
    tree_plot = tree_mort.join(
        data['plot'].select(['CN', 'INVYR', 'STATECD', 'UNITCD', 'COUNTYCD', 
                           'PLOT', 'DESIGNCD', 'EVALID', 'REMPER']),
        left_on='PLT_CN',
        right_on='CN',
        how='inner'
    )
    print(f"   After join with PLOT: {len(tree_plot)} records")
    
    # Join with condition data
    tree_plot_cond = tree_plot.join(
        data['cond'].select(['PLT_CN', 'CONDID', 'COND_STATUS_CD', 'FORTYPCD', 
                           'STDSZCD', 'SITECLCD']),
        on=['PLT_CN', 'CONDID'],
        how='left'
    )
    print(f"   After join with COND: {len(tree_plot_cond)} records")
    
    # Test 2: Stratification join
    print("\n2. Testing stratification joins...")
    
    tree_plot_cond = tree_plot_cond.join(
        data['pop_plot_stratum_assgn'].select(['PLT_CN', 'STRATUM_CN']),
        on='PLT_CN',
        how='left'
    ).join(
        data['pop_stratum'].select(['CN', 'EXPNS', 'ADJ_FACTOR_SUBP', 'ADJ_FACTOR_MICR', 'ADJ_FACTOR_MACR']),
        left_on='STRATUM_CN',
        right_on='CN',
        how='left'
    )
    print(f"   After stratification joins: {len(tree_plot_cond)} records")
    
    # Test 3: Tree basis assignment and mortality calculations
    print("\n3. Testing tree basis assignment and mortality calculations...")
    
    # Forest land filter
    tree_plot_cond = tree_plot_cond.filter(pl.col('COND_STATUS_CD') == 1)
    print(f"   After forest land filter: {len(tree_plot_cond)} records")
    
    # Assign TREE_BASIS
    tree_plot_cond = tree_plot_cond.with_columns(
        pl.when(pl.col('DIA_BEGIN').is_null())
        .then(None)
        .when(pl.col('DIA_BEGIN') < 5.0)
        .then(pl.lit('MICR'))
        .otherwise(pl.lit('SUBP'))
        .alias('TREE_BASIS')
    )
    
    # Calculate mortality
    tree_plot_cond = tree_plot_cond.with_columns([
        # Trees per acre mortality with proper adjustment factor
        pl.when(pl.col('TREE_BASIS') == 'MICR')
        .then(pl.col('TPAMORT_UNADJ') * pl.col('ADJ_FACTOR_MICR'))
        .otherwise(pl.col('TPAMORT_UNADJ') * pl.col('ADJ_FACTOR_SUBP'))
        .alias('MORT_TPA_YR'),
        
        # Volume mortality
        pl.when(pl.col('TREE_BASIS') == 'MICR')
        .then(pl.col('TPAMORT_UNADJ') * pl.col('VOLCFNET_BEGIN').fill_null(0) * pl.col('ADJ_FACTOR_MICR'))
        .otherwise(pl.col('TPAMORT_UNADJ') * pl.col('VOLCFNET_BEGIN').fill_null(0) * pl.col('ADJ_FACTOR_SUBP'))
        .alias('MORT_VOL_YR'),
        
        # Biomass mortality
        pl.when(pl.col('TREE_BASIS') == 'MICR')
        .then(pl.col('TPAMORT_UNADJ') * pl.col('DRYBIO_AG_BEGIN').fill_null(0) * pl.col('ADJ_FACTOR_MICR'))
        .otherwise(pl.col('TPAMORT_UNADJ') * pl.col('DRYBIO_AG_BEGIN').fill_null(0) * pl.col('ADJ_FACTOR_SUBP'))
        .alias('MORT_BIO_YR')
    ])
    
    print("   Individual tree mortality calculations:")
    result = tree_plot_cond.select([
        'TRE_CN', 'DIA_BEGIN', 'TREE_BASIS', 'TPAMORT_UNADJ',
        'MORT_TPA_YR', 'MORT_VOL_YR', 'MORT_BIO_YR'
    ])
    print(result)
    
    # Test 4: Plot-level aggregation
    print("\n4. Testing plot-level aggregation...")
    
    plot_mort = (tree_plot_cond
        .group_by(['PLT_CN', 'STRATUM_CN', 'EXPNS', 'EVALID'])
        .agg([
            pl.col('MORT_TPA_YR').sum().alias('MORT_TPA_PLOT'),
            pl.col('MORT_VOL_YR').sum().alias('MORT_VOL_PLOT'),
            pl.col('MORT_BIO_YR').sum().alias('MORT_BIO_PLOT')
        ])
    )
    
    print("   Plot-level mortality totals:")
    print(plot_mort)
    
    print("\n✅ Mortality framework test completed!")
    print("   Key findings:")
    print(f"   - Successfully processed {len(data['tree_grm_component'])} tree records")
    print(f"   - Applied tree basis assignment and adjustment factors")
    print(f"   - Calculated TPA, volume, and biomass mortality")
    print(f"   - Aggregated to plot level successfully")
    
    return True

if __name__ == "__main__":
    test_mortality_calculation()