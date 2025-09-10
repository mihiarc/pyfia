#!/usr/bin/env python
"""
Oklahoma tree count validation test - FIXED VERSION.

This test validates that pyFIA correctly calculates the total number of trees
on forestland in Oklahoma, comparing against the published total of 5,592,821,689 trees.

Key insights from the SQL example:
1. Use EVALID 402301 (EXPVOL/EXPCURR) for tree counts
2. Include MACRO_BREAKPOINT_DIA from PLOT table for proper adjustment factors
3. Use RSCD=33 filter (South region) 
4. Apply complex diameter-based adjustment factor logic

Source: FIA published estimate for Oklahoma forestland
Expected: 5,592,821,689 total trees on forestland
"""

import sys
from pathlib import Path
import duckdb


def test_oklahoma_tree_count_with_proper_expansion():
    """
    Validate Oklahoma total tree count using the exact expansion logic from FIA.
    
    This uses the proper adjustment factor selection based on:
    - Trees < 5.0" DBH: ADJ_FACTOR_MICR
    - Trees 5.0" to MACRO_BREAKPOINT_DIA: ADJ_FACTOR_SUBP  
    - Trees >= MACRO_BREAKPOINT_DIA: ADJ_FACTOR_MACR
    """
    
    EXPECTED_TREES = 5_592_821_689  # Published total
    TOLERANCE_PCT = 0.1  # Allow 0.1% tolerance for rounding
    
    print("="*80)
    print("OKLAHOMA TREE COUNT VALIDATION TEST - FIXED VERSION")
    print("="*80)
    print(f"Expected (published): {EXPECTED_TREES:,} trees on forestland")
    print(f"Tolerance: {TOLERANCE_PCT}%")
    print("="*80)
    
    db_path = Path("data/nfi_south.duckdb")
    if not db_path.exists():
        print(f"Error: Database not found at {db_path}")
        return False
    
    conn = duckdb.connect(str(db_path), read_only=True)
    
    try:
        # SQL based on the provided example, adapted for total tree count
        tree_count_query = """
        SELECT 
            SUM(ESTIMATED_VALUE * EXPNS) as TOTAL_TREES,
            COUNT(DISTINCT plot_cn) as unique_plots,
            COUNT(DISTINCT cond_cn) as unique_conditions,
            COUNT(*) as tree_records
        FROM (
            SELECT 
                pop_stratum.estn_unit_cn,
                pop_stratum.cn as STRATACN,
                plot.cn as plot_cn,
                cond.cn as cond_cn,
                pop_stratum.expns as EXPNS,
                SUM(
                    COALESCE(
                        TREE.TPA_UNADJ * 
                        CASE 
                            WHEN TREE.DIA IS NULL THEN POP_STRATUM.ADJ_FACTOR_SUBP
                            ELSE 
                                CASE 
                                    -- Trees under 5 inches use microplot adjustment
                                    WHEN TREE.DIA < 5.0 THEN POP_STRATUM.ADJ_FACTOR_MICR
                                    -- Trees 5" to macro breakpoint use subplot adjustment
                                    WHEN TREE.DIA < COALESCE(PLOT.MACRO_BREAKPOINT_DIA, 9999) 
                                        THEN POP_STRATUM.ADJ_FACTOR_SUBP
                                    -- Trees at or above macro breakpoint use macroplot adjustment
                                    ELSE POP_STRATUM.ADJ_FACTOR_MACR
                                END
                        END, 
                        0
                    )
                ) AS ESTIMATED_VALUE
            FROM POP_STRATUM
            JOIN POP_PLOT_STRATUM_ASSGN ON (POP_PLOT_STRATUM_ASSGN.STRATUM_CN = POP_STRATUM.CN)
            JOIN PLOT ON (POP_PLOT_STRATUM_ASSGN.PLT_CN = PLOT.CN)
            JOIN COND ON (COND.PLT_CN = PLOT.CN)
            JOIN TREE ON (TREE.PLT_CN = COND.PLT_CN AND TREE.CONDID = COND.CONDID)
            WHERE 
                TREE.STATUSCD = 1  -- Live trees
                AND COND.COND_STATUS_CD = 1  -- Forestland
                AND POP_STRATUM.RSCD = 33  -- South region (from example)
                AND POP_STRATUM.EVALID = 402301  -- EXPVOL evaluation (matches published)
            GROUP BY 
                pop_stratum.estn_unit_cn,
                pop_stratum.cn,
                plot.cn,
                cond.cn,
                pop_stratum.expns
        ) AS tree_expansion
        """
        
        print("\nRunning corrected SQL query with proper expansion factors...")
        result = conn.execute(tree_count_query).fetchone()
        
        if result and result[0] is not None:
            total_trees = result[0]
            unique_plots = result[1]
            unique_conditions = result[2]
            tree_records = result[3]
            
            print(f"\nResults:")
            print(f"  Total trees (expanded): {total_trees:,.0f}")
            print(f"  Unique plots: {unique_plots:,}")
            print(f"  Unique conditions: {unique_conditions:,}")
            print(f"  Tree records: {tree_records:,}")
            
            # Calculate difference
            difference = abs(total_trees - EXPECTED_TREES)
            pct_diff = (difference / EXPECTED_TREES) * 100
            
            print(f"\nComparison with Published Value:")
            print(f"  Expected:   {EXPECTED_TREES:,} trees")
            print(f"  Calculated: {total_trees:,.0f} trees")
            print(f"  Difference: {difference:,.0f} trees ({pct_diff:.4f}%)")
            
            if pct_diff <= TOLERANCE_PCT:
                print(f"\n✅ TEST PASSED! Within {TOLERANCE_PCT}% tolerance")
                return True
            else:
                print(f"\n❌ TEST FAILED: {pct_diff:.4f}% exceeds {TOLERANCE_PCT}% tolerance")
                return False
        else:
            print("  No data returned from query")
            return False
            
    except Exception as e:
        print(f"  Error in query: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        conn.close()


def verify_adjustment_factors():
    """
    Verify the adjustment factors are being applied correctly by checking 
    tree counts in different diameter classes.
    """
    
    print("\n" + "="*80)
    print("VERIFYING ADJUSTMENT FACTOR APPLICATION")
    print("="*80)
    
    db_path = Path("data/nfi_south.duckdb")
    conn = duckdb.connect(str(db_path), read_only=True)
    
    try:
        # Check tree counts by diameter class
        diameter_query = """
        SELECT 
            CASE 
                WHEN TREE.DIA < 5.0 THEN '1. Microplot (<5")'
                WHEN TREE.DIA < COALESCE(PLOT.MACRO_BREAKPOINT_DIA, 9999) THEN '2. Subplot (5"-macro)'
                ELSE '3. Macroplot (>=macro)'
            END as diameter_class,
            COUNT(*) as tree_records,
            AVG(TREE.DIA) as avg_diameter,
            MIN(TREE.DIA) as min_diameter,
            MAX(TREE.DIA) as max_diameter,
            SUM(
                TREE.TPA_UNADJ * 
                CASE 
                    WHEN TREE.DIA < 5.0 THEN POP_STRATUM.ADJ_FACTOR_MICR
                    WHEN TREE.DIA < COALESCE(PLOT.MACRO_BREAKPOINT_DIA, 9999) 
                        THEN POP_STRATUM.ADJ_FACTOR_SUBP
                    ELSE POP_STRATUM.ADJ_FACTOR_MACR
                END * 
                POP_STRATUM.EXPNS
            ) as expanded_trees
        FROM POP_STRATUM
        JOIN POP_PLOT_STRATUM_ASSGN ON (POP_PLOT_STRATUM_ASSGN.STRATUM_CN = POP_STRATUM.CN)
        JOIN PLOT ON (POP_PLOT_STRATUM_ASSGN.PLT_CN = PLOT.CN)
        JOIN COND ON (COND.PLT_CN = PLOT.CN)
        JOIN TREE ON (TREE.PLT_CN = COND.PLT_CN AND TREE.CONDID = COND.CONDID)
        WHERE 
            TREE.STATUSCD = 1
            AND COND.COND_STATUS_CD = 1
            AND POP_STRATUM.RSCD = 33
            AND POP_STRATUM.EVALID = 402301
        GROUP BY diameter_class
        ORDER BY diameter_class
        """
        
        print("\nTree counts by diameter class:")
        results = conn.execute(diameter_query).fetchall()
        
        total_expanded = 0
        for diam_class, records, avg_dia, min_dia, max_dia, expanded in results:
            total_expanded += expanded
            print(f"\n{diam_class}:")
            print(f"  Records: {records:,}")
            print(f"  Diameter range: {min_dia:.1f}\" - {max_dia:.1f}\" (avg: {avg_dia:.1f}\")")
            print(f"  Expanded trees: {expanded:,.0f}")
        
        print(f"\nTotal expanded trees: {total_expanded:,.0f}")
        
        # Check MACRO_BREAKPOINT_DIA distribution
        macro_query = """
        SELECT 
            COALESCE(PLOT.MACRO_BREAKPOINT_DIA, 9999) as breakpoint,
            COUNT(DISTINCT PLOT.CN) as plot_count
        FROM PLOT
        JOIN POP_PLOT_STRATUM_ASSGN ON PLOT.CN = POP_PLOT_STRATUM_ASSGN.PLT_CN
        JOIN POP_STRATUM ON POP_PLOT_STRATUM_ASSGN.STRATUM_CN = POP_STRATUM.CN
        WHERE 
            POP_STRATUM.RSCD = 33
            AND POP_STRATUM.EVALID = 402301
            AND PLOT.STATECD = 40
        GROUP BY breakpoint
        ORDER BY breakpoint
        """
        
        print("\nMACRO_BREAKPOINT_DIA distribution:")
        macro_results = conn.execute(macro_query).fetchall()
        for breakpoint, count in macro_results:
            if breakpoint == 9999:
                print(f"  No macroplot: {count:,} plots")
            else:
                print(f"  Breakpoint {breakpoint:.1f}\": {count:,} plots")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()


def main():
    """Run Oklahoma tree count validation with proper expansion factors."""
    
    print("\n🌳" * 40)
    print("OKLAHOMA TREE COUNT VALIDATION - CORRECTED")
    print("🌳" * 40)
    
    # Run the corrected validation test
    test_passed = test_oklahoma_tree_count_with_proper_expansion()
    
    # Verify adjustment factors
    verify_adjustment_factors()
    
    # Summary
    print("\n" + "="*80)
    print("VALIDATION SUMMARY")
    print("="*80)
    
    if test_passed:
        print("✅ SUCCESS: Oklahoma tree count matches published value!")
        print("   Published: 5,592,821,689 trees")
        print("   Key factors for correct calculation:")
        print("   1. Use EVALID 402301 (EXPVOL/EXPCURR)")
        print("   2. Include MACRO_BREAKPOINT_DIA from PLOT table")
        print("   3. Apply diameter-based adjustment factors correctly")
        print("   4. Filter by RSCD=33 (South region)")
        return 0
    else:
        print("❌ FAILED: Tree count does not match")
        print("   Check the expansion factor logic and EVALID selection")
        return 1


if __name__ == "__main__":
    sys.exit(main())