#!/usr/bin/env python3
"""
Test mortality estimator with real NC GRM evaluation data.

This tests the actual mortality.py implementation with EVALID 372303
(NC 2023 mortality evaluation) to see if it works with real FIA data.
"""

import polars as pl
import sys
from pathlib import Path

# Add pyfia to path
sys.path.insert(0, str(Path(__file__).parent))

def test_real_mortality():
    """Test mortality calculation with real FIA data."""
    
    print("🎯 Testing mortality estimator with real NC GRM data...")
    print("   EVALID: 372303 (NC 2023 Mortality Evaluation)")
    print("   Type: EXPMORT")
    print("   Period: 2009-2019 growth, 2016-2023 inventory")
    
    try:
        from pyfia.core import FIA
        
        # Load database and clip to mortality evaluation
        fia = FIA('./SQLite_FIADB_NC.db')
        fia.clip_by_evalid(372303)
        
        print(f"✅ Loaded database and clipped to EVALID {fia.evalid}")
        
        # Test the actual mortality function
        print("\n🧪 Testing mortality() function...")
        
        # Import the mortality function
        from pyfia.mortality import mortality
        
        # Run mortality estimation
        result = mortality(
            db=fia,
            landType='forest',
            method='TI'
        )
        
        print("✅ Mortality function completed!")
        print(f"   Result shape: {result.shape}")
        print(f"   Result columns: {result.columns}")
        
        if len(result) > 0:
            print("\n📊 Mortality Results:")
            print(result)
        else:
            print("❌ No results returned")
            
        return result
        
    except Exception as e:
        print(f"❌ Error running mortality estimator: {e}")
        import traceback
        traceback.print_exc()
        return None

def analyze_grm_data_structure():
    """Analyze the GRM data structure to understand column mappings."""
    
    print("\n🔍 Analyzing GRM data structure...")
    
    try:
        from pyfia.core import FIA
        fia = FIA('./SQLite_FIADB_NC.db')
        fia.clip_by_evalid(372303)
        
        # Get GRM component data
        grm_comp = fia._reader.read_table('TREE_GRM_COMPONENT', lazy=False)
        
        print(f"   TREE_GRM_COMPONENT: {len(grm_comp)} rows")
        
        # Check mortality data
        forest_mortality = grm_comp.filter(
            (pl.col('SUBP_TPAMORT_UNADJ_AL_FOREST') > 0) |
            (pl.col('MICR_TPAMORT_UNADJ_AL_FOREST') > 0)
        )
        
        print(f"   Records with forest mortality: {len(forest_mortality)}")
        
        if len(forest_mortality) > 0:
            print("\n   Sample mortality data:")
            sample = forest_mortality.select([
                'TRE_CN', 'DIA_BEGIN', 
                'MICR_TPAMORT_UNADJ_AL_FOREST',
                'SUBP_TPAMORT_UNADJ_AL_FOREST',
                'MORTCFAL_FOREST'
            ]).head(5)
            print(sample)
            
        # Calculate total mortality for validation
        total_micr_mort = grm_comp['MICR_TPAMORT_UNADJ_AL_FOREST'].sum()
        total_subp_mort = grm_comp['SUBP_TPAMORT_UNADJ_AL_FOREST'].sum()
        
        print(f"\n📊 Total mortality summary:")
        print(f"   Microplot mortality: {total_micr_mort:.1f} TPA")
        print(f"   Subplot mortality: {total_subp_mort:.1f} TPA") 
        print(f"   Combined mortality: {total_micr_mort + total_subp_mort:.1f} TPA")
        
    except Exception as e:
        print(f"❌ Error analyzing GRM data: {e}")

if __name__ == "__main__":
    print("=" * 60)
    print("TESTING MORTALITY ESTIMATOR WITH REAL FIA DATA")
    print("=" * 60)
    
    # First analyze the data structure
    analyze_grm_data_structure()
    
    # Then test the mortality function
    result = test_real_mortality()
    
    if result is not None:
        print("\n✅ SUCCESS: Mortality estimator working with real data!")
    else:
        print("\n❌ FAILED: Need to fix mortality implementation for real data structure")