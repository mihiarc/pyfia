# Modern Agent Migration Guide

## Overview

The `FIAAgentModern` class represents a complete rewrite of the pyFIA AI agent using 2025 LangChain patterns. It replaces the complex node-based architecture with a simpler, more maintainable approach.

## Key Improvements

### 1. **Simplified Architecture**
- **Old**: 5-node StateGraph with manual workflow management
- **New**: Single `create_react_agent` call with built-in ReAct pattern

### 2. **Tool Implementation**
- **Old**: Complex Tool objects with closures
- **New**: Simple Python functions with docstrings

### 3. **Memory & Persistence**
- **Old**: No built-in memory
- **New**: Automatic conversation memory with checkpointing

### 4. **Human-in-the-Loop**
- **Old**: Not supported
- **New**: Optional tool approval before execution

## Migration Steps

### For CLI Users

The modern agent is now the default. Just run:
```bash
./qa  # or pyfia-ai
```

To explicitly use the old agents:
```bash
pyfia-ai --agent basic    # Old basic agent
pyfia-ai --agent enhanced  # Old enhanced agent
pyfia-ai --agent modern    # New modern agent (default)
```

### For Python API Users

**Old Pattern:**
```python
from pyfia.ai_agent import FIAAgent, FIAAgentConfig

config = FIAAgentConfig(verbose=True)
agent = FIAAgent(db_path, config, api_key)
response = agent.query("How many oak trees?")
```

**New Pattern:**
```python
from pyfia.ai_agent_modern import FIAAgentModern

agent = FIAAgentModern(
    db_path=db_path,
    api_key=api_key,
    verbose=True,
    checkpoint_dir="/path/to/checkpoints"  # Optional
)
response = agent.query("How many oak trees?", thread_id="session1")
```

## Feature Comparison

| Feature | Old Agent | Modern Agent |
|---------|-----------|--------------|
| Natural language queries | ✅ | ✅ |
| SQL generation | ✅ | ✅ |
| Tool calling | Manual selection | LLM decides |
| Memory | ❌ | ✅ |
| Conversation threads | ❌ | ✅ |
| Human approval | ❌ | ✅ |
| Checkpointing | ❌ | ✅ |
| Architecture | Complex | Simple |

## New Features

### 1. **Conversation Memory**
```python
# Continue previous conversation
response = agent.query("What about pine trees?", thread_id="session1")

# Get conversation history
history = agent.get_conversation_history("session1")
```

### 2. **Human Approval**
```python
agent = FIAAgentModern(
    db_path=db_path,
    enable_human_approval=True  # Require approval for tools
)
```

### 3. **Built-in Tools**
- `execute_fia_query`: Run SQL queries
- `get_database_schema`: Explore tables
- `find_species_codes`: Look up species
- `get_evalid_info`: Find evaluations
- `get_state_codes`: List states
- `calculate_forest_statistics`: Use pyFIA estimators

## Performance Notes

The modern agent:
- Starts faster (no complex graph compilation)
- Uses less memory (streamlined state)
- Provides better error messages
- Supports interruption/resumption

## Compatibility

The old agents remain available for backward compatibility. We recommend migrating to the modern agent for:
- Better performance
- New features
- Simpler debugging
- Future support

## Support

For issues or questions:
- GitHub Issues: https://github.com/anthropics/claude-code/issues
- Documentation: https://docs.anthropic.com/en/docs/claude-code