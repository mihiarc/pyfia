#!/usr/bin/env python3
"""
Find the correct GRM evaluation ID for North Carolina mortality estimation.

This script explores the FIA database to identify available evaluations,
specifically looking for GRM (Growth, Removal, Mortality) evaluations.
"""

import polars as pl
import sys
from pathlib import Path

# Add pyfia to path
sys.path.insert(0, str(Path(__file__).parent))

def find_nc_grm_evaluations():
    """Find NC GRM evaluations in the FIA database."""
    
    print("🔍 Searching for NC GRM evaluations...")
    
    # Try to find the FIA database
    possible_paths = [
        "/opt/FIA/FIADB_NC/NC_FIADB.db",
        "/Users/mihiarc/repos/pyfia/NC_FIADB.db",
        "NC_FIADB.db"
    ]
    
    db_path = None
    for path in possible_paths:
        if Path(path).exists():
            db_path = path
            break
    
    if not db_path:
        print("❌ No FIA database found. Trying to use pyFIA core...")
        # Try using pyFIA to find database
        try:
            from pyfia.core import FIA
            # This might work if database is configured elsewhere
            fia = FIA()
            db_path = str(fia.db_path)
        except Exception as e:
            print(f"❌ Could not find FIA database: {e}")
            print("\n💡 To complete this task, we need:")
            print("1. FIA database file (NC_FIADB.db)")
            print("2. Or configure database path in pyFIA")
            return None
    
    print(f"✅ Found database: {db_path}")
    
    try:
        from pyfia.core import FIA
        fia = FIA(db_path)
        
        # Load evaluation tables
        print("\n📊 Loading evaluation tables...")
        fia.load_table('POP_EVAL')
        fia.load_table('POP_EVAL_TYP')
        
        pop_eval = fia.tables['POP_EVAL'].collect()
        pop_eval_typ = fia.tables['POP_EVAL_TYP'].collect()
        
        print(f"POP_EVAL rows: {len(pop_eval)}")
        print(f"POP_EVAL_TYP rows: {len(pop_eval_typ)}")
        
        # Join evaluation data with types
        eval_data = pop_eval.join(
            pop_eval_typ,
            left_on='CN',
            right_on='EVAL_CN',
            how='left'
        )
        
        # Filter for NC (state code 37)
        nc_evals = eval_data.filter(pl.col('STATECD') == 37)
        
        print(f"\n🏷️  Found {len(nc_evals)} NC evaluations")
        
        # Show all NC evaluations
        if len(nc_evals) > 0:
            print("\n📋 All NC Evaluations:")
            print("=" * 80)
            
            result = nc_evals.select([
                'EVALID', 'EVAL_TYP', 'START_INVYR', 'END_INVYR', 
                'EVAL_DESCR', 'NPLOTS'
            ]).sort('EVALID')
            
            print(result)
            
            # Filter for GRM evaluations
            grm_evals = nc_evals.filter(
                pl.col('EVAL_TYP').str.contains('GRM')
            )
            
            if len(grm_evals) > 0:
                print(f"\n🎯 Found {len(grm_evals)} NC GRM evaluations:")
                print("=" * 50)
                
                grm_result = grm_evals.select([
                    'EVALID', 'EVAL_TYP', 'START_INVYR', 'END_INVYR',
                    'EVAL_DESCR', 'NPLOTS'
                ]).sort('END_INVYR', descending=True)
                
                print(grm_result)
                
                # Get the most recent GRM evaluation
                most_recent_grm = grm_result.row(0, named=True)
                
                print(f"\n✅ Most Recent GRM Evaluation:")
                print(f"   EVALID: {most_recent_grm['EVALID']}")
                print(f"   Type: {most_recent_grm['EVAL_TYP']}")
                print(f"   Period: {most_recent_grm['START_INVYR']}-{most_recent_grm['END_INVYR']}")
                print(f"   Description: {most_recent_grm['EVAL_DESCR']}")
                print(f"   Plots: {most_recent_grm['NPLOTS']}")
                
                return most_recent_grm['EVALID']
            else:
                print("❌ No GRM evaluations found for NC")
                
                # Show what types are available
                types = nc_evals['EVAL_TYP'].unique().sort()
                print(f"\n💡 Available evaluation types in NC: {types.to_list()}")
                
        return None
        
    except Exception as e:
        print(f"❌ Error exploring database: {e}")
        return None

def test_grm_tables(evalid):
    """Test if TREE_GRM tables have data for the given evaluation."""
    
    print(f"\n🧪 Testing TREE_GRM tables for EVALID {evalid}...")
    
    try:
        from pyfia.core import FIA
        fia = FIA()
        fia.clip_evalid(evalid)
        
        # Check TREE_GRM_COMPONENT
        try:
            grm_comp = fia._reader.read_table('TREE_GRM_COMPONENT', lazy=False)
            print(f"✅ TREE_GRM_COMPONENT: {len(grm_comp)} rows")
            
            if len(grm_comp) > 0:
                print("   Columns:", grm_comp.columns)
                
                # Check component types
                if 'COMPONENT' in grm_comp.columns:
                    components = grm_comp['COMPONENT'].unique().sort()
                    print(f"   Component types: {components.to_list()}")
                    
                    # Look for mortality components
                    mort_components = grm_comp.filter(
                        pl.col('COMPONENT').str.contains('MORT')
                    )
                    print(f"   Mortality components: {len(mort_components)} rows")
                
        except Exception as e:
            print(f"❌ TREE_GRM_COMPONENT error: {e}")
        
        # Check TREE_GRM_BEGIN
        try:
            grm_begin = fia._reader.read_table('TREE_GRM_BEGIN', lazy=False)
            print(f"✅ TREE_GRM_BEGIN: {len(grm_begin)} rows")
            
            if len(grm_begin) > 0:
                print("   Columns:", grm_begin.columns)
                
        except Exception as e:
            print(f"❌ TREE_GRM_BEGIN error: {e}")
            
        return True
        
    except Exception as e:
        print(f"❌ Error testing GRM tables: {e}")
        return False

if __name__ == "__main__":
    print("🎯 Finding NC GRM Evaluation for Mortality Estimation")
    print("=" * 60)
    
    # Find GRM evaluations
    grm_evalid = find_nc_grm_evaluations()
    
    if grm_evalid:
        print(f"\n🎯 Testing GRM tables with EVALID {grm_evalid}...")
        test_grm_tables(grm_evalid)
        
        print(f"\n✅ Ready to test mortality estimator!")
        print(f"   Use: fia.clip_evalid({grm_evalid})")
        print(f"   Then: mortality_result = fia.mortality()")
    else:
        print("\n❌ Could not find suitable GRM evaluation")
        print("   Need to identify correct GRM evaluation for mortality estimation")