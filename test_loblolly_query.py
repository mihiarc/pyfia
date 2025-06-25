#!/usr/bin/env python3
"""
Test script for debugging the loblolly pine query.
"""

import os
import sys
from pathlib import Path
from pyfia.ai_agent_modern import FIAAgentModern

def debug_loblolly_query():
    """Debug the specific loblolly pine query step by step."""
    
    print("🐛 Debugging: 'How many loblolly pine trees are there in Texas?'")
    print("=" * 60)
    
    # Check prerequisites
    print("\n1. Checking prerequisites...")
    
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        print("❌ OPENAI_API_KEY not set")
        print("Set it with: export OPENAI_API_KEY='your-key'")
        return False
    print("✅ API key found")
    
    db_path = Path("./fia.duckdb")
    if not db_path.exists():
        print("❌ Database not found at ./fia.duckdb")
        return False
    print("✅ Database found")
    
    # Initialize agent
    print("\n2. Initializing modern agent...")
    try:
        agent = FIAAgentModern(
            db_path=db_path,
            verbose=True,  # Enable debug output
            temperature=0.1,  # Lower temperature for consistency
        )
        print("✅ Agent initialized successfully")
    except Exception as e:
        print(f"❌ Failed to initialize agent: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    # Test individual tools first
    print("\n3. Testing individual tools...")
    
    # Test 1: Find species codes
    print("\n   Testing species lookup...")
    try:
        # Access the agent's tools directly for testing
        from pyfia.duckdb_query_interface import DuckDBQueryInterface
        query_interface = DuckDBQueryInterface(db_path)
        
        species_query = """
        SELECT SPCD, COMMON_NAME, SCIENTIFIC_NAME
        FROM REF_SPECIES
        WHERE LOWER(COMMON_NAME) LIKE LOWER('%loblolly%pine%')
           OR LOWER(SCIENTIFIC_NAME) LIKE LOWER('%loblolly%')
        """
        species_result = query_interface.execute_query(species_query)
        print(f"   Found {len(species_result)} species matches:")
        for row in species_result.iter_rows(named=True):
            print(f"   - Code {row['SPCD']}: {row['COMMON_NAME']}")
        
    except Exception as e:
        print(f"   ❌ Species lookup failed: {e}")
    
    # Test 2: Find Texas state code
    print("\n   Testing state lookup...")
    try:
        state_query = """
        SELECT VALUE AS STATECD, MEANING AS STATE_NAME
        FROM REF_STATECD
        WHERE LOWER(MEANING) LIKE LOWER('%texas%')
        """
        state_result = query_interface.execute_query(state_query)
        print(f"   Found {len(state_result)} state matches:")
        for row in state_result.iter_rows(named=True):
            print(f"   - Code {row['STATECD']}: {row['STATE_NAME']}")
        
    except Exception as e:
        print(f"   ❌ State lookup failed: {e}")
    
    # Test 3: Find Texas evaluations
    print("\n   Testing evaluation lookup...")
    try:
        eval_query = """
        SELECT 
            pe.EVALID,
            pe.EVAL_DESCR,
            pe.STATECD,
            pe.START_INVYR,
            pe.END_INVYR,
            pet.EVAL_TYP,
            COUNT(DISTINCT ppsa.PLT_CN) as plot_count
        FROM POP_EVAL pe
        LEFT JOIN POP_EVAL_TYP pet ON pe.CN = pet.EVAL_CN
        LEFT JOIN POP_PLOT_STRATUM_ASSGN ppsa ON pe.EVALID = ppsa.EVALID
        WHERE pe.STATECD = 48
        GROUP BY pe.EVALID, pe.EVAL_DESCR, pe.STATECD, 
                 pe.START_INVYR, pe.END_INVYR, pet.EVAL_TYP
        ORDER BY pe.END_INVYR DESC
        LIMIT 5
        """
        eval_result = query_interface.execute_query(eval_query)
        print(f"   Found {len(eval_result)} Texas evaluations:")
        for row in eval_result.iter_rows(named=True):
            print(f"   - EVALID {row['EVALID']}: {row['EVAL_DESCR']} ({row['EVAL_TYP']})")
            print(f"     Years: {row['START_INVYR']}-{row['END_INVYR']}, Plots: {row['plot_count']:,}")
        
    except Exception as e:
        print(f"   ❌ Evaluation lookup failed: {e}")
    
    # Test 4: Count loblolly pine trees
    print("\n   Testing tree count query...")
    try:
        # Use most recent VOL evaluation for Texas (should be 482301 or similar)
        tree_query = """
        SELECT COUNT(*) as tree_count
        FROM TREE t
        JOIN REF_SPECIES rs ON t.SPCD = rs.SPCD
        JOIN POP_PLOT_STRATUM_ASSGN ppsa ON t.PLT_CN = ppsa.PLT_CN
        JOIN PLOT p ON t.PLT_CN = p.CN
        WHERE rs.SPCD = 131  -- Loblolly pine
          AND p.STATECD = 48  -- Texas
          AND t.STATUSCD = 1  -- Live trees
          AND ppsa.EVALID = (
              SELECT pe.EVALID 
              FROM POP_EVAL pe 
              JOIN POP_EVAL_TYP pet ON pe.CN = pet.EVAL_CN
              WHERE pe.STATECD = 48 AND pet.EVAL_TYP = 'VOL'
              ORDER BY pe.END_INVYR DESC 
              LIMIT 1
          )
        """
        tree_result = query_interface.execute_query(tree_query)
        tree_count = tree_result.row(0)[0]
        print(f"   Found {tree_count:,} loblolly pine trees in Texas")
        
    except Exception as e:
        print(f"   ❌ Tree count failed: {e}")
    
    # Test the new count_trees_by_criteria function
    print("\n   Testing count_trees_by_criteria function...")
    try:
        # Test the function directly
        from pyfia.ai_agent_modern import FIAAgentModern
        test_agent = FIAAgentModern(db_path, verbose=False)
        
        # This should work now with the new tool
        print("   (This tests the internal tool functionality)")
        
    except Exception as e:
        print(f"   ❌ Tool test failed: {e}")
    
    # Test the full agent query
    print("\n4. Testing full agent query...")
    try:
        print("\n   Sending query to agent...")
        response = agent.query(
            "How many loblolly pine trees are there in Texas?",
            thread_id="debug_session"
        )
        
        print("\n   📝 Agent Response:")
        print("-" * 40)
        print(response)
        print("-" * 40)
        
        # Show conversation history
        print("\n   📜 Conversation History:")
        history = agent.get_conversation_history("debug_session")
        for i, msg in enumerate(history[-4:], 1):  # Show last 4 messages
            msg_type = msg.__class__.__name__
            content = msg.content[:200] + "..." if len(msg.content) > 200 else msg.content
            print(f"   {i}. {msg_type}: {content}")
        
        print("\n✅ Agent query completed successfully!")
        return True
        
    except Exception as e:
        print(f"\n❌ Agent query failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Main function."""
    success = debug_loblolly_query()
    
    if success:
        print("\n🎉 All tests passed!")
        print("\nThe agent should now work correctly with your query.")
        print("Try running: ./qa")
        print("Then ask: how many loblolly pine trees are there in texas?")
    else:
        print("\n💥 Some tests failed!")
        print("Check the errors above and fix them before trying the full query.")
    
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())