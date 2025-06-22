#!/usr/bin/env python3
"""
Test script to verify mortality estimation with treeClass parameter.
Tests both 'all' (default) and 'growing_stock' modes.
"""

from pyfia import FIA
from pyfia.mortality import mortality
import polars as pl

# Path to test database (adjust as needed)
db_path = "path/to/your/fia_database.db"

# Initialize FIA object
fia = FIA(db_path)

# Find GRM (mortality) evaluations
grm_evals = fia.find_evalid(eval_type="GRM")
print(f"Found {len(grm_evals)} GRM evaluations")

if grm_evals:
    # Use first GRM evaluation
    evalid = list(grm_evals.keys())[0]
    print(f"\nTesting with EVALID: {evalid}")
    
    # Clip to this evaluation
    fia.clip_by_evalid(evalid)
    
    # Test 1: All live trees mortality (default)
    print("\n1. Testing mortality with treeClass='all' (default):")
    mort_all = mortality(fia, mr=True)
    print(mort_all.select(["EVALID", "MORT_TPA_AC", "MORT_VOL_AC", "MORT_BIO_AC", "nPlots"]))
    
    # Test 2: Growing stock mortality
    print("\n2. Testing mortality with treeClass='growing_stock':")
    mort_gs = mortality(fia, treeClass="growing_stock", mr=True)
    print(mort_gs.select(["EVALID", "MORT_TPA_AC", "MORT_VOL_AC", "MORT_BIO_AC", "nPlots"]))
    
    # Compare results
    print("\n3. Comparison:")
    print(f"All live trees mortality: {mort_all['MORT_VOL_AC'][0]:.3f} cu ft/acre/year")
    print(f"Growing stock mortality: {mort_gs['MORT_VOL_AC'][0]:.3f} cu ft/acre/year")
    print(f"Difference: {abs(mort_all['MORT_VOL_AC'][0] - mort_gs['MORT_VOL_AC'][0]):.3f} cu ft/acre/year")
    
else:
    print("No GRM evaluations found in database")