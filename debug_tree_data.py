#!/usr/bin/env python
"""Debug tree data loading"""

from pyfia import FIA

# Connect to the database
db = FIA("nfi_south.duckdb")
db.clip_by_evalid([402301])

# Load tables
db.load_table("PLOT")
db.load_table("TREE")
db.load_table("COND")

# Get trees and conditions
trees = db.get_trees()
conds = db.get_conditions()

print(f"Trees shape: {trees.shape}")
print(f"Tree columns (first 10): {trees.columns[:10]}")
print(f"\nConditions shape: {conds.shape}")
print(f"Condition columns (first 10): {conds.columns[:10]}")

# Check if DIA is in tree table
if 'DIA' in trees.columns:
    print(f"\n✓ DIA column found in TREE table")
    print(f"  DIA range: {trees['DIA'].min():.1f} - {trees['DIA'].max():.1f}")
else:
    print(f"\n✗ DIA column NOT found in TREE table")
    
# Check TPA_UNADJ
if 'TPA_UNADJ' in trees.columns:
    print(f"✓ TPA_UNADJ column found in TREE table")
    print(f"  Non-null TPA_UNADJ count: {trees.filter(trees['TPA_UNADJ'].is_not_null()).shape[0]}")
else:
    print(f"✗ TPA_UNADJ column NOT found in TREE table")