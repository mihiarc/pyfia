#!/usr/bin/env python3
"""
Test the corrected tree count implementation with real FIA data.
Query: "How many loblolly pine trees are there in Texas?"
"""

import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

def test_loblolly_pine_texas():
    """Test the actual loblolly pine count in Texas using our corrected implementation."""
    
    print("🌲 Testing: How many loblolly pine trees are there in Texas?")
    print("=" * 70)
    print("Using corrected tree_count implementation with real FIA data")
    print()
    
    try:
        from pyfia.core import FIA
        from pyfia.estimation.tree import tree_count
        
        # Connect to the FIA database
        print("📊 Connecting to FIA database...")
        fia = FIA("fia.duckdb")
        print("✅ Connected successfully")
        
        # Test the query parameters
        print("\n🔍 Query Parameters:")
        print("   Species: Loblolly pine (SPCD = 131)")
        print("   State: Texas (STATECD = 48)")  
        print("   Tree Type: Live trees")
        print("   Grouping: By species")
        
        # Execute the corrected tree count
        print("\n⚙️  Executing tree count with corrected implementation...")
        
        result = tree_count(
            fia,
            by_species=True,
            tree_domain='SPCD == 131',  # Loblolly pine
            area_domain='STATECD == 48',  # Texas
            tree_type='live',
            totals=True
        )
        
        print("✅ Query executed successfully!")
        
        # Display results
        print("\n📋 Results:")
        print("-" * 50)
        
        if len(result) > 0:
            for row in result.iter_rows(named=True):
                print(f"Species Code (SPCD): {row.get('SPCD', 'N/A')}")
                print(f"Common Name: {row.get('COMMON_NAME', 'N/A')}")
                print(f"Scientific Name: {row.get('SCIENTIFIC_NAME', 'N/A')}")
                print(f"Tree Count: {row.get('TREE_COUNT', 0):,.0f} trees")
                print(f"Number of Plots: {row.get('nPlots', 0):,}")
                print(f"Standard Error: {row.get('SE', 0):,.0f}")
                print(f"SE Percentage: {row.get('SE_PERCENT', 0):.1f}%")
                print()
        else:
            print("❌ No results found - this could indicate:")
            print("   - No loblolly pine trees in Texas in the database")
            print("   - Issue with filtering logic")
            print("   - EVALID or data availability problems")
        
        # Test without species grouping for comparison
        print("\n🔄 Testing total count (without species grouping)...")
        
        total_result = tree_count(
            fia,
            by_species=False,
            tree_domain='SPCD == 131',  # Loblolly pine
            area_domain='STATECD == 48',  # Texas  
            tree_type='live',
            totals=True
        )
        
        if len(total_result) > 0:
            total_row = total_result.row(0, named=True)
            print(f"Total Loblolly Pine Trees in Texas: {total_row.get('TREE_COUNT', 0):,.0f}")
            print(f"Based on {total_row.get('nPlots', 0):,} plots")
        
        return result
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return None

def test_ai_agent_simulation():
    """Simulate how the AI agent would call this."""
    
    print("\n🤖 Simulating AI Agent Call")
    print("=" * 40)
    
    try:
        from pyfia.ai.agent import FIAAgent
        
        print("AI Agent would parse the query and call:")
        print("execute_tree_command('bySpecies treeDomain=\"SPCD == 131\" areaDomain=\"STATECD == 48\" treeType=live')")
        print()
        print("This would internally call:")
        print("tree_count(fia, by_species=True, tree_domain='SPCD == 131', area_domain='STATECD == 48', tree_type='live')")
        print()
        print("✅ AI Agent simulation complete")
        
    except Exception as e:
        print(f"❌ AI Agent simulation error: {e}")

if __name__ == "__main__":
    result = test_loblolly_pine_texas()
    test_ai_agent_simulation()
    
    print("\n" + "=" * 70)
    if result is not None and len(result) > 0:
        print("🎉 SUCCESS: Got actual results from corrected implementation!")
    else:
        print("⚠️  No results - may need to investigate data availability or filtering") 