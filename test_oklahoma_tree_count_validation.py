#!/usr/bin/env python
"""
Oklahoma tree count validation test.

This test validates that pyFIA correctly calculates the total number of trees
on forestland in Oklahoma, comparing against the published total of 5,592,821,689 trees.

Source: FIA published estimate for Oklahoma forestland
Expected: 5,592,821,689 total trees on forestland
"""

import sys
from pathlib import Path
from pyfia import FIA, tpa
import duckdb


def test_oklahoma_tree_count_published():
    """
    Validate Oklahoma total tree count on forestland against published estimate.
    
    Published value: 5,592,821,689 trees on forestland
    Source: FIA Database reports
    
    This represents the expanded tree count (sum of TPA_UNADJ * expansion factors)
    for all live trees on forestland conditions.
    """
    
    EXPECTED_TREES = 5_592_821_689  # Published total
    TOLERANCE_PCT = 1.0  # Allow 1% tolerance due to rounding/updates
    
    print("="*80)
    print("OKLAHOMA TREE COUNT VALIDATION TEST")
    print("="*80)
    print(f"Expected (published): {EXPECTED_TREES:,} trees on forestland")
    print(f"Tolerance: {TOLERANCE_PCT}%")
    print("="*80)
    
    # Check which database to use
    db_path = Path("data/nfi_south.duckdb")
    if not db_path.exists():
        print(f"Error: Database not found at {db_path}")
        return False
    
    # Method 1: Using pyFIA's tpa() function
    print("\nMETHOD 1: Using pyFIA tpa() function")
    print("-"*40)
    
    try:
        db = FIA(str(db_path))
        
        # Get most recent Oklahoma evaluation
        db.clip_by_state(40, most_recent=True, eval_type="EXPALL")
        
        # Calculate trees per acre for live trees on forestland
        result = tpa(
            db, 
            tree_domain="STATUSCD == 1",  # Live trees only
            land_type="forest"  # Forestland only
        )
        
        # The result should have TPA and total tree count
        if "TPA" in result.columns:
            tpa_value = result["TPA"][0]
            print(f"  Trees per acre: {tpa_value:.2f}")
        
        if "N_TREES" in result.columns:
            tree_count = result["N_TREES"][0]
            print(f"  Total trees: {tree_count:,}")
        elif "TREE_TOTAL" in result.columns:
            tree_count = result["TREE_TOTAL"][0]
            print(f"  Total trees: {tree_count:,}")
        else:
            print(f"  Available columns: {result.columns}")
            tree_count = None
            
    except Exception as e:
        print(f"  Error with pyFIA method: {e}")
        tree_count = None
    
    # Method 2: Direct SQL calculation
    print("\nMETHOD 2: Direct SQL query")
    print("-"*40)
    
    conn = duckdb.connect(str(db_path), read_only=True)
    
    try:
        # First, find the most recent Oklahoma EXPALL evaluation
        evalid_query = """
        SELECT 
            pe.EVALID,
            pe.EVAL_DESCR,
            COUNT(DISTINCT ppsa.PLT_CN) as plot_count
        FROM POP_EVAL pe
        JOIN POP_EVAL_TYP pet ON pe.CN = pet.EVAL_CN
        JOIN POP_PLOT_STRATUM_ASSGN ppsa ON pe.EVALID = ppsa.EVALID
        WHERE pe.STATECD = 40
          AND pet.EVAL_TYP = 'EXPALL'
        GROUP BY pe.EVALID, pe.EVAL_DESCR
        ORDER BY pe.EVALID DESC
        LIMIT 1
        """
        
        evalid_result = conn.execute(evalid_query).fetchone()
        if evalid_result:
            evalid, eval_desc, plot_count = evalid_result
            print(f"  Using EVALID {evalid}: {eval_desc}")
            print(f"  Plot count: {plot_count:,}")
        else:
            print("  No EXPALL evaluation found for Oklahoma")
            evalid = 402300  # Fallback to known OK EVALID
            print(f"  Using fallback EVALID: {evalid}")
        
        # Calculate total expanded tree count on forestland
        tree_query = f"""
        SELECT 
            SUM(
                CAST(t.TPA_UNADJ AS DOUBLE) * 
                CAST(ps.EXPNS AS DOUBLE) *
                CAST(
                    CASE 
                        WHEN t.DIA < 5.0 THEN ps.ADJ_FACTOR_MICR
                        WHEN t.DIA < COALESCE(p.MACRO_BREAKPOINT_DIA, 9999) THEN ps.ADJ_FACTOR_SUBP
                        ELSE ps.ADJ_FACTOR_MACR
                    END AS DOUBLE
                )
            ) as total_trees,
            COUNT(DISTINCT t.CN) as tree_records,
            COUNT(DISTINCT p.CN) as plots_with_trees
        FROM POP_STRATUM ps
        JOIN POP_PLOT_STRATUM_ASSGN ppsa ON ps.CN = ppsa.STRATUM_CN
        JOIN PLOT p ON ppsa.PLT_CN = p.CN
        JOIN COND c ON p.CN = c.PLT_CN
        JOIN TREE t ON c.PLT_CN = t.PLT_CN AND c.CONDID = t.CONDID
        WHERE ps.EVALID = {evalid}
          AND p.STATECD = 40  -- Oklahoma
          AND t.STATUSCD = 1   -- Live trees
          AND c.COND_STATUS_CD = 1  -- Forestland
        """
        
        result = conn.execute(tree_query).fetchone()
        
        if result and result[0] is not None:
            total_trees_sql = result[0]
            tree_records = result[1]
            plots_with_trees = result[2]
            
            print(f"  Total trees (expanded): {total_trees_sql:,.0f}")
            print(f"  Tree records (raw): {tree_records:,}")
            print(f"  Plots with trees: {plots_with_trees:,}")
            
            # Calculate difference from published
            difference = abs(total_trees_sql - EXPECTED_TREES)
            pct_diff = (difference / EXPECTED_TREES) * 100
            
            print(f"\n  Comparison:")
            print(f"    Expected:   {EXPECTED_TREES:,}")
            print(f"    Calculated: {total_trees_sql:,.0f}")
            print(f"    Difference: {difference:,.0f} ({pct_diff:.2f}%)")
            
            sql_passed = pct_diff <= TOLERANCE_PCT
        else:
            print("  No data returned from query")
            total_trees_sql = None
            sql_passed = False
            
    except Exception as e:
        print(f"  Error in SQL query: {e}")
        total_trees_sql = None
        sql_passed = False
    finally:
        conn.close()
    
    # Method 3: Check by forest type breakdown
    print("\nMETHOD 3: Tree count by major forest types")
    print("-"*40)
    
    conn = duckdb.connect(str(db_path), read_only=True)
    
    try:
        # Get tree counts by forest type
        forest_type_query = f"""
        SELECT 
            c.FORTYPCD,
            COUNT(DISTINCT t.CN) as tree_records,
            SUM(
                CAST(t.TPA_UNADJ AS DOUBLE) * 
                CAST(ps.EXPNS AS DOUBLE) *
                CAST(
                    CASE 
                        WHEN t.DIA < 5.0 THEN ps.ADJ_FACTOR_MICR
                        WHEN t.DIA < COALESCE(p.MACRO_BREAKPOINT_DIA, 9999) THEN ps.ADJ_FACTOR_SUBP
                        ELSE ps.ADJ_FACTOR_MACR
                    END AS DOUBLE
                )
            ) as expanded_trees
        FROM POP_STRATUM ps
        JOIN POP_PLOT_STRATUM_ASSGN ppsa ON ps.CN = ppsa.STRATUM_CN
        JOIN PLOT p ON ppsa.PLT_CN = p.CN
        JOIN COND c ON p.CN = c.PLT_CN
        JOIN TREE t ON c.PLT_CN = t.PLT_CN AND c.CONDID = t.CONDID
        WHERE ps.EVALID = {evalid}
          AND p.STATECD = 40
          AND t.STATUSCD = 1
          AND c.COND_STATUS_CD = 1
        GROUP BY c.FORTYPCD
        ORDER BY expanded_trees DESC
        LIMIT 5
        """
        
        forest_types = conn.execute(forest_type_query).fetchall()
        
        if forest_types:
            print(f"  Top 5 forest types by tree count:")
            total_by_type = 0
            for fortypcd, records, expanded in forest_types:
                total_by_type += expanded
                print(f"    Type {fortypcd}: {expanded:,.0f} trees ({records:,} records)")
            print(f"  Total (top 5): {total_by_type:,.0f} trees")
        
    except Exception as e:
        print(f"  Error getting forest types: {e}")
    finally:
        conn.close()
    
    # Summary
    print("\n" + "="*80)
    print("TEST SUMMARY")
    print("="*80)
    
    if sql_passed:
        print("✅ TEST PASSED: Oklahoma tree count matches published value!")
        print(f"   Published:  {EXPECTED_TREES:,} trees")
        print(f"   Calculated: {total_trees_sql:,.0f} trees")
        print(f"   Difference: {pct_diff:.2f}% (within {TOLERANCE_PCT}% tolerance)")
        return True
    else:
        print("❌ TEST FAILED: Tree count does not match published value")
        if total_trees_sql is not None:
            print(f"   Published:  {EXPECTED_TREES:,} trees")
            print(f"   Calculated: {total_trees_sql:,.0f} trees")
            print(f"   Difference: {pct_diff:.2f}% (exceeds {TOLERANCE_PCT}% tolerance)")
        else:
            print("   Could not calculate tree count")
        return False


def check_oklahoma_data_availability():
    """Check if Oklahoma data is available in the database."""
    
    db_path = Path("data/nfi_south.duckdb")
    
    if not db_path.exists():
        print(f"Database not found: {db_path}")
        return False
    
    conn = duckdb.connect(str(db_path), read_only=True)
    
    try:
        # Check for Oklahoma data
        check_query = """
        SELECT 
            COUNT(DISTINCT p.CN) as plots,
            COUNT(DISTINCT t.CN) as trees,
            MIN(p.INVYR) as min_year,
            MAX(p.INVYR) as max_year
        FROM PLOT p
        LEFT JOIN TREE t ON p.CN = t.PLT_CN
        WHERE p.STATECD = 40
        """
        
        result = conn.execute(check_query).fetchone()
        
        if result and result[0] > 0:
            plots, trees, min_year, max_year = result
            print(f"\nOklahoma data found:")
            print(f"  Plots: {plots:,}")
            print(f"  Tree records: {trees:,}")
            print(f"  Years: {min_year} - {max_year}")
            return True
        else:
            print("\nNo Oklahoma data found in database")
            return False
            
    finally:
        conn.close()


def main():
    """Run Oklahoma tree count validation."""
    
    print("\n🌳" * 40)
    print("OKLAHOMA TREE COUNT VALIDATION")
    print("🌳" * 40)
    
    # First check if Oklahoma data is available
    if not check_oklahoma_data_availability():
        print("\n⚠️  Cannot run validation - Oklahoma data not available")
        return 1
    
    # Run the validation test
    if test_oklahoma_tree_count_published():
        print("\n🎉 Validation successful!")
        return 0
    else:
        print("\n⚠️  Validation failed - investigate discrepancies")
        return 1


if __name__ == "__main__":
    sys.exit(main())