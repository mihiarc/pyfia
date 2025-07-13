# FIA AI Agent Documentation

## Overview

The FIA AI Agent is a modern natural language interface for querying Forest Inventory Analysis (FIA) databases. Built with LangGraph and 2025 AI best practices, it allows users to interact with complex forest inventory data using plain English questions.

## Features

### 🤖 Natural Language Processing
- **Conversational Interface**: Ask questions in natural language
- **Domain Expertise**: Deep understanding of forest inventory terminology
- **Context Awareness**: Maintains conversation context for follow-up questions
- **Memory Persistence**: Remembers conversation history across sessions

### 🌲 Forest Inventory Expertise
- **FIA Data Structures**: Deep knowledge of PLOT, TREE, COND, and population tables
- **Statistical Awareness**: Proper use of EVALID for population estimates
- **Species Intelligence**: Automatic species code lookup and validation
- **Measurement Units**: Understands forestry units (DBH, height, volume, biomass)

### 🔒 Safety & Validation
- **Query Validation**: All SQL queries are validated before execution
- **Read-Only Access**: No data modification capabilities
- **Result Limits**: Automatic limits to prevent overwhelming outputs
- **Error Handling**: Graceful error recovery with helpful suggestions

### 🎨 Modern User Interface
- **Rich CLI**: Beautiful command-line interface with colors and formatting
- **Progress Indicators**: Real-time feedback during query processing
- **Syntax Highlighting**: SQL queries displayed with proper formatting
- **Interactive Help**: Context-sensitive help and examples

## Installation

### Prerequisites
- Python 3.11 or higher
- OpenAI API key
- FIA database in DuckDB format

### Install pyFIA with AI Agent Support

```bash
# Install with LangChain dependencies
pip install pyfia[langchain]

# Or install all optional dependencies
pip install pyfia[all]
```

### Set OpenAI API Key

```bash
export OPENAI_API_KEY="your-api-key-here"
```

## Quick Start

### Command Line Interface

```bash
# Start the AI assistant
pyfia-ai database.duckdb

# Or use the qa script
./qa
```

### Python API

```python
from pyfia.ai_agent import FIAAgent

# Initialize the agent
agent = FIAAgent("path/to/fia_database.duckdb")

# Ask natural language questions
response = agent.query("How many oak trees are there in California?")
print(response)

# Use conversation memory
response = agent.query("What about pine trees?", thread_id="my_session")
```

## Usage Examples

### Natural Language Queries

```bash
fia-ai> How many live trees are there by species?
fia-ai> What's the total forest area in North Carolina?
fia-ai> Show me plots with high biomass in the Pacific Northwest
fia-ai> Compare oak volume between 2010 and 2020
```

### Follow-up Questions

The agent maintains conversation context:

```bash
fia-ai> How many oak trees are in Texas?
fia-ai> What about pine trees?
fia-ai> Show me the largest diameter trees from those results
```

## CLI Commands

### Database Commands
- `connect <path>` - Connect to FIA database
- `schema [table]` - View database schema
- `evalid [search]` - Show available evaluations

### Analysis Commands
- `concepts [term]` - Explore FIA terminology
- `history` - View query history
- `export <file>` - Export results
- `last [n]` - Show last n results

### Utility Commands
- `clear` - Clear the screen
- `quit` or `exit` - Exit the application

### Query Commands
- Any natural language question about forest data
- Follow-up questions that build on previous queries
- Technical questions about FIA methodology

## Configuration Options

### Agent Configuration

```python
from pyfia.ai_agent import FIAAgent

agent = FIAAgent(
    db_path="/path/to/database.duckdb",
    model_name="gpt-4o",              # OpenAI model
    temperature=0.1,                  # Response creativity (0-1)
    verbose=False,                    # Detailed logging
    enable_human_approval=False,      # Human-in-the-loop
    checkpoint_dir=None               # Conversation persistence directory
)
```

## Technical Architecture

### Modern LangGraph Design

The AI agent uses LangGraph's `create_react_agent` pattern with:

1. **ReAct Pattern**: Reasoning and Acting in interleaved steps
2. **Built-in Memory**: Automatic conversation history management
3. **Tool Selection**: LLM automatically chooses appropriate tools
4. **Error Recovery**: Graceful handling of failed operations
5. **Human-in-the-Loop**: Optional approval for sensitive operations

### Specialized Tools

- **execute_fia_query**: Safe SQL execution with result limits
- **get_database_schema**: Schema information for query planning
- **get_evalid_info**: Evaluation metadata for statistical queries
- **find_species_codes**: Species name to code resolution
- **get_state_codes**: State code lookups
- **count_trees_by_criteria**: Optimized tree counting

### Safety Features

- **Query Validation**: All queries validated before execution
- **Read-Only Access**: Database opened in read-only mode
- **Result Limits**: Automatic limits prevent overwhelming output
- **SQL Injection Prevention**: Parameterized queries and validation
- **Error Recovery**: Graceful handling of invalid queries

## Best Practices

### Writing Effective Queries

1. **Be Specific**: "Show oak volume in California" vs "Show volume"
2. **Include Context**: Mention time periods, geographic scope
3. **Use Forest Terms**: DBH, basal area, forest type, etc.
4. **Ask Follow-ups**: Build on previous queries for deeper analysis

### Statistical Considerations

1. **EVALID Awareness**: Statistical estimates require proper EVALID filtering
2. **Sampling Errors**: Ask about confidence intervals for population estimates
3. **Temporal Consistency**: Use consistent time periods for comparisons
4. **Geographic Scope**: Understand estimation unit boundaries

### Performance Tips

1. **Limit Results**: Use reasonable limits for large datasets
2. **Specific Filters**: Add geographic or temporal filters
3. **Incremental Queries**: Start broad, then narrow down
4. **Cache Results**: Save important query results

## Troubleshooting

### Common Issues

**"No results found"**
- Check if your filters are too restrictive
- Verify species names and codes
- Ensure geographic scope is valid

**"Query validation failed"**
- Rephrase your question more clearly
- Try breaking complex questions into parts
- Check for typos in species names

**"EVALID required for estimates"**
- Use `evalids` command to see available evaluations
- Specify time period or geographic scope
- Ask for help with statistical methodology

### Error Messages

**"OpenAI API key not found"**
```bash
export OPENAI_API_KEY="your-key-here"
```

**"Database not found"**
- Verify the database file path
- Ensure you have read permissions
- Check if the file is a valid DuckDB database

**"LangChain dependencies not available"**
```bash
pip install pyfia[langchain]
```

## Advanced Usage

### Custom Agent Configuration

```python
from pyfia.ai_agent import FIAAgent, FIAAgentConfig

# High-precision configuration
config = FIAAgentConfig(
    model_name="gpt-4o",
    temperature=0.05,  # Very low for consistent results
    result_limit=1000, # Higher limit for detailed analysis
    verbose=True       # Detailed logging
)

agent = FIAAgent("/path/to/database.duckdb", config)
```

### Batch Processing

```python
questions = [
    "How many plots are there by state?",
    "What's the average tree density per acre?",
    "Show volume estimates by forest type",
    "Calculate biomass for the top 5 species"
]

results = []
for question in questions:
    response = agent.query(question)
    results.append({"question": question, "response": response})
```

### Integration with Analysis Workflows

```python
# Get raw data for further analysis
agent = create_fia_agent("/path/to/database.duckdb")

# Use agent to identify interesting patterns
response = agent.query("Find states with highest oak density")

# Extract specific data for detailed analysis
oak_data = agent.query("Show oak tree measurements for top 3 states")

# Continue with statistical analysis in pandas/polars
```

## API Reference

### FIAAgent Class

```python
class FIAAgent:
    def __init__(self, db_path, config=None, api_key=None)
    def query(self, user_input: str) -> str
    def get_available_evaluations(self, state_code=None) -> pl.DataFrame
    def validate_query(self, sql_query: str) -> Dict[str, Any]
```

### FIAAgentConfig Class

```python
@dataclass
class FIAAgentConfig:
    model_name: str = "gpt-4o"
    temperature: float = 0.1
    max_tokens: int = 2000
    result_limit: int = 100
    enable_query_validation: bool = True
    enable_safety_checks: bool = True
    verbose: bool = False
```

### Utility Functions

```python
def create_fia_agent(db_path, api_key=None, **config_kwargs) -> FIAAgent
```

## Contributing

## Best Practices

### Writing Effective Queries

1. **Be Specific**: "Show oak volume in California" vs "Show volume"
2. **Include Context**: Mention time periods, geographic scope
3. **Use Forest Terms**: DBH, basal area, forest type, etc.
4. **Ask Follow-ups**: Build on previous queries for deeper analysis

### Statistical Considerations

1. **EVALID Awareness**: Statistical estimates require proper EVALID filtering
2. **Sampling Errors**: Ask about confidence intervals for population estimates
3. **Temporal Consistency**: Use consistent time periods for comparisons
4. **Geographic Scope**: Understand estimation unit boundaries

### Performance Tips

1. **Limit Results**: Use reasonable limits for large datasets
2. **Specific Filters**: Add geographic or temporal filters
3. **Incremental Queries**: Start broad, then narrow down
4. **Cache Results**: Save important query results

## Troubleshooting

### Common Issues

**"No results found"**
- Check if your filters are too restrictive
- Verify species names and codes
- Ensure geographic scope is valid

**"Query validation failed"**
- Rephrase your question more clearly
- Try breaking complex questions into parts
- Check for typos in species names

**"EVALID required for estimates"**
- Use `evalid` command to see available evaluations
- Specify time period or geographic scope
- Ask for help with statistical methodology

### Error Messages

**"OpenAI API key not found"**
```bash
export OPENAI_API_KEY="your-key-here"
```

**"Database not found"**
- Verify the database file path
- Ensure you have read permissions
- Check if the file is a valid DuckDB database

**"LangChain dependencies not available"**
```bash
pip install pyfia[langchain]
```

## Advanced Usage

### Conversation Memory

```python
from pyfia.ai_agent import FIAAgent

agent = FIAAgent("database.duckdb")

# Use thread IDs for separate conversations
session1 = agent.query("How many oak trees?", thread_id="session1")
session2 = agent.query("Show pine volume", thread_id="session2")

# Continue conversations
followup1 = agent.query("What about maple?", thread_id="session1")

# Get conversation history
history = agent.get_conversation_history("session1")
```

### Batch Processing

```python
questions = [
    "How many plots are there by state?",
    "What's the average tree density per acre?",
    "Show volume estimates by forest type",
    "Calculate biomass for the top 5 species"
]

results = []
for question in questions:
    response = agent.query(question)
    results.append({"question": question, "response": response})
```

### Integration with Analysis Workflows

```python
# Get raw data for further analysis
agent = FIAAgent("database.duckdb")

# Use agent to identify interesting patterns
response = agent.query("Find states with highest oak density")

# Extract specific data for detailed analysis
oak_data = agent.query("Show oak tree measurements for top 3 states")

# Continue with statistical analysis in pandas/polars
```

## API Reference

### FIAAgent Class

```python
class FIAAgent:
    def __init__(
        self,
        db_path: str,
        api_key: Optional[str] = None,
        model_name: str = "gpt-4-turbo-preview",
        temperature: float = 0,
        verbose: bool = False,
        enable_human_approval: bool = False,
        checkpoint_dir: Optional[str] = None
    )

    def query(
        self,
        question: str,
        thread_id: Optional[str] = None,
        config: Optional[Dict] = None
    ) -> str

    def get_conversation_history(self, thread_id: str) -> List[BaseMessage]

    def clear_memory(self, thread_id: Optional[str] = None)
```

### Utility Functions

```python
def create_fia_agent(db_path, api_key=None, **kwargs) -> FIAAgent
```

## Contributing

### Adding New Tools

1. Create tool function with clear docstring in `ai_agent.py`
2. Add to tools list in `_create_agent()` method
3. Test with various query types
4. Update documentation

### Improving Query Understanding

1. Enhance system prompt with domain knowledge
2. Add more example patterns in tool descriptions
3. Improve query validation logic
4. Test with diverse query types

### Extending CLI Features

1. Add new commands to `cli_ai.py`
2. Implement command handlers
3. Update help documentation
4. Test interactive features

## Support

For questions, issues, or contributions:

- **GitHub Issues**: Report bugs and feature requests
- **Documentation**: Check the full pyFIA documentation
- **Examples**: See the `examples/` directory for usage patterns
- **Community**: Join forest inventory analysis discussions

## License

This project is licensed under the MIT License. See the LICENSE file for details.