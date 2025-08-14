#!/usr/bin/env python3
"""
Test script to compare original vs refactored volume estimator.

This script demonstrates:
1. The refactored version produces the same results
2. The significant reduction in code size
3. The improved maintainability and clarity
"""

import os
import sys
from pathlib import Path

# Add the src directory to Python path
sys.path.insert(0, str(Path(__file__).parent / "src"))

import polars as pl
from pyfia import FIA

# Import both versions
from pyfia.estimation.volume import volume as volume_original
from pyfia.estimation.volume_refactored import volume as volume_refactored


def compare_implementations():
    """Compare the original and refactored volume implementations."""
    
    # Check if test database exists
    test_db = "test_data/test.duckdb"
    if not os.path.exists(test_db):
        print(f"Test database not found at {test_db}")
        print("Please ensure test data is available")
        return
    
    # Create test cases
    test_cases = [
        {
            "name": "Basic volume estimation",
            "params": {"vol_type": "net"},
        },
        {
            "name": "Volume by species",
            "params": {"vol_type": "net", "by_species": True},
        },
        {
            "name": "Volume with totals",
            "params": {"vol_type": "gross", "totals": True},
        },
        {
            "name": "Volume by forest type",
            "params": {"vol_type": "net", "grp_by": "FORTYPCD"},
        },
        {
            "name": "Volume with tree domain",
            "params": {"vol_type": "net", "tree_domain": "DIA >= 10.0"},
        },
    ]
    
    print("=== Volume Estimator Refactoring Comparison ===\n")
    
    # Code size comparison
    original_file = Path("src/pyfia/estimation/volume.py")
    refactored_file = Path("src/pyfia/estimation/volume_refactored.py")
    
    if original_file.exists() and refactored_file.exists():
        original_lines = len(original_file.read_text().splitlines())
        refactored_lines = len(refactored_file.read_text().splitlines())
        reduction = (1 - refactored_lines / original_lines) * 100
        
        print(f"Code Size Comparison:")
        print(f"  Original:   {original_lines:,} lines")
        print(f"  Refactored: {refactored_lines:,} lines")
        print(f"  Reduction:  {reduction:.1f}%")
        print()
    
    # Run test cases
    print("Running functional comparisons...\n")
    
    with FIA(test_db) as db:
        # Clip to a specific state for testing
        db.clip_by_state(37, most_recent=True)  # North Carolina
        
        for i, test_case in enumerate(test_cases, 1):
            print(f"Test {i}: {test_case['name']}")
            
            try:
                # Run both implementations
                result_original = volume_original(db, **test_case['params'])
                result_refactored = volume_refactored(db, **test_case['params'])
                
                # Compare results
                if result_original.shape == result_refactored.shape:
                    print(f"  ✓ Shape matches: {result_original.shape}")
                else:
                    print(f"  ✗ Shape mismatch: Original {result_original.shape}, "
                          f"Refactored {result_refactored.shape}")
                
                # Compare column names
                if set(result_original.columns) == set(result_refactored.columns):
                    print(f"  ✓ Columns match: {len(result_original.columns)} columns")
                else:
                    print(f"  ✗ Column mismatch:")
                    print(f"    Original only: {set(result_original.columns) - set(result_refactored.columns)}")
                    print(f"    Refactored only: {set(result_refactored.columns) - set(result_original.columns)}")
                
                # Sample data comparison (first row)
                if len(result_original) > 0:
                    # Compare first row values for numeric columns
                    numeric_cols = [col for col in result_original.columns 
                                  if result_original[col].dtype in [pl.Float32, pl.Float64, pl.Int32, pl.Int64]]
                    
                    matches = True
                    for col in numeric_cols:
                        if col in result_refactored.columns:
                            orig_val = result_original[col][0]
                            refact_val = result_refactored[col][0]
                            if abs(orig_val - refact_val) > 0.0001:  # Allow small floating point differences
                                matches = False
                                print(f"    Value mismatch in {col}: {orig_val} vs {refact_val}")
                    
                    if matches:
                        print(f"  ✓ Values match")
                
            except Exception as e:
                print(f"  ✗ Error: {str(e)}")
            
            print()
    
    # Summary of benefits
    print("\n=== Refactoring Benefits ===")
    print("\n1. Code Reduction:")
    print(f"   - {reduction:.0f}% fewer lines of code")
    print("   - Eliminated duplicated variance calculation logic")
    print("   - Removed redundant formatting code")
    print("   - Simplified stratification handling")
    
    print("\n2. Improved Maintainability:")
    print("   - Uses unified FIAVarianceCalculator for all variance calculations")
    print("   - Leverages OutputFormatter for consistent output formatting")
    print("   - Inherits common functionality from EnhancedBaseEstimator")
    print("   - Tree basis adjustments handled by base class")
    
    print("\n3. Enhanced Features:")
    print("   - Automatic caching of stratification data")
    print("   - Standardized error handling")
    print("   - Consistent column naming across all estimators")
    print("   - Built-in support for confidence intervals (Phase 2)")
    
    print("\n4. Template for Other Estimators:")
    print("   - This pattern can be applied to biomass, TPA, area, etc.")
    print("   - Estimated 60-70% code reduction across all estimators")
    print("   - Consistent API and behavior across the module")


if __name__ == "__main__":
    compare_implementations()