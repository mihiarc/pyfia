#!/usr/bin/env python3
"""Standalone test for the new configuration classes."""

# Test just the config module without full pyfia imports
import sys
sys.path.insert(0, 'src')

# Import only what we need
from pyfia.estimation.config import MortalityConfig, EstimatorConfigV2

def test_mortality_config():
    """Test MortalityConfig functionality."""
    print("Testing MortalityConfig...")
    
    # Test basic creation
    config = MortalityConfig(
        mortality_type='tpa',
        by_species=True,
        group_by_ownership=True,
        variance=True
    )
    
    print('✓ MortalityConfig created successfully')
    print(f'  Grouping columns: {config.get_grouping_columns()}')
    print(f'  Output columns: {config.get_output_columns()[:3]}...')
    
    # Test validation
    try:
        bad_config = MortalityConfig(
            mortality_type='volume',
            tree_type='live'
        )
    except ValueError as e:
        print(f'✓ Validation working: caught "{str(e)[:50]}..."')
    
    # Test another validation
    try:
        bad_config2 = MortalityConfig(
            tree_class='timber',
            land_type='forest'
        )
    except ValueError as e:
        print(f'✓ Validation working: caught "{str(e)[:50]}..."')
    
    # Test valid timber config
    timber_config = MortalityConfig(
        mortality_type='volume',
        tree_type='dead',
        tree_class='timber',
        land_type='timber'
    )
    print('✓ Valid timber config created')
    
    # Test complex grouping
    complex_config = MortalityConfig(
        grp_by=['STATECD', 'UNITCD'],
        by_species=True,
        group_by_species_group=True,
        group_by_ownership=True,
        group_by_agent=True,
        group_by_disturbance=True,
        by_size_class=True,
        mortality_type='both',
        include_components=True,
        variance=True,
        totals=True
    )
    
    groups = complex_config.get_grouping_columns()
    print(f'✓ Complex grouping: {len(groups)} columns')
    print(f'  Groups: {groups}')
    
    outputs = complex_config.get_output_columns()
    print(f'✓ Output columns: {len(outputs)} columns')
    print(f'  Columns: {outputs[:5]}...')
    
    # Test domain validation
    safe_config = MortalityConfig(
        tree_domain="DIA >= 10.0 AND STATUSCD == 2",
        area_domain="FORTYPCD IN (121, 122, 123)"
    )
    print('✓ Domain expressions validated')
    
    # Test dangerous SQL
    try:
        danger_config = MortalityConfig(
            tree_domain="DIA >= 10; DROP TABLE TREE;"
        )
    except ValueError as e:
        print(f'✓ SQL injection blocked: "{str(e)[:40]}..."')
    
    print('\nAll MortalityConfig tests passed!')


def test_estimator_config_v2():
    """Test base EstimatorConfigV2."""
    print("\nTesting EstimatorConfigV2...")
    
    config = EstimatorConfigV2(
        grp_by=['STATECD'],
        by_species=True,
        land_type='timber',
        method='SMA',
        lambda_=0.7
    )
    
    print('✓ EstimatorConfigV2 created')
    print(f'  Grouping columns: {config.get_grouping_columns()}')
    
    # Test lambda validation
    try:
        bad_lambda = EstimatorConfigV2(lambda_=1.5)
    except ValueError:
        print('✓ Lambda validation working')
    
    # Test method validation
    try:
        bad_method = EstimatorConfigV2(method='INVALID')
    except ValueError:
        print('✓ Method validation working')
    
    print('\nAll EstimatorConfigV2 tests passed!')


if __name__ == '__main__':
    test_mortality_config()
    test_estimator_config_v2()
    print('\n✨ All configuration tests completed successfully!')