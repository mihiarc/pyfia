#!/usr/bin/env python3
"""
Quick test script for the modern AI agent.
"""

import os
from pathlib import Path
from pyfia.ai_agent_modern import FIAAgentModern

def test_modern_agent():
    """Test the modern agent with a simple query."""
    # Check for API key
    if not os.getenv("OPENAI_API_KEY"):
        print("❌ OPENAI_API_KEY not set")
        return
    
    # Find database
    db_path = Path("./fia.duckdb")
    if not db_path.exists():
        print("❌ Database not found at ./fia.duckdb")
        return
    
    print("✅ Initializing modern agent...")
    
    try:
        # Create agent
        agent = FIAAgentModern(
            db_path=db_path,
            verbose=True,
            model_name="gpt-4-turbo-preview"
        )
        
        print("✅ Agent created successfully")
        
        # Test query
        print("\n📊 Testing query: 'How many evaluation types are in the database?'")
        response = agent.query("How many evaluation types are in the database?")
        
        print("\n🤖 Agent response:")
        print(response)
        
        # Test with species query
        print("\n📊 Testing query: 'Find species codes for oak'")
        response = agent.query("Find species codes for oak", thread_id="test_thread")
        
        print("\n🤖 Agent response:")
        print(response)
        
        # Show conversation history
        print("\n📜 Conversation history:")
        history = agent.get_conversation_history("test_thread")
        for i, msg in enumerate(history):
            print(f"{i+1}. {msg.type}: {msg.content[:100]}...")
        
        print("\n✅ All tests passed!")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_modern_agent()