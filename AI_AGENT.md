# FIA AI Agent Documentation

## Overview

The FIA AI Agent is a cutting-edge natural language interface for querying Forest Inventory Analysis (FIA) databases. Built with LangGraph and modern AI technologies, it allows users to interact with complex forest inventory data using plain English questions.

## Features

### 🤖 Natural Language Processing
- **Conversational Interface**: Ask questions in natural language
- **Domain Expertise**: Deep understanding of forest inventory terminology
- **Context Awareness**: Maintains conversation context for follow-up questions
- **Query Intent Recognition**: Automatically identifies what type of forest data you need

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
# Start the interactive AI agent
fia-ai /path/to/your/fia_database.duckdb

# Use different model
fia-ai --model gpt-4o-mini /path/to/database.duckdb

# Adjust temperature for more/less creative responses
fia-ai --temperature 0.2 /path/to/database.duckdb
```

### Python API

```python
from pyfia.ai_agent import create_fia_agent

# Create agent
agent = create_fia_agent("/path/to/fia_database.duckdb")

# Ask questions
response = agent.query("How many live oak trees are in the database?")
print(response)

# Get available evaluations
evaluations = agent.get_available_evaluations()
print(evaluations)
```

## Usage Examples

### Basic Tree Queries

```
"How many live trees are in the database?"
"What are the top 10 most common tree species?"
"Show me trees with diameter greater than 20 inches"
"Find all oak species in the database"
```

### Forest Area Analysis

```
"What's the total forest area by state?"
"Show forest area by ownership type"
"How much area is in different forest types?"
"Find the largest forest plots"
```

### Volume and Biomass

```
"What's the total volume by species?"
"Show aboveground biomass for hardwood species"
"Calculate volume per acre by forest type"
"Which species has the highest biomass per tree?"
```

### Statistical Queries

```
"Estimate trees per acre by species for California"
"Calculate forest area for the most recent evaluation"
"Show population estimates for oak volume"
"What's the sampling error for pine volume estimates?"
```

### Species-Specific Analysis

```
"Show diameter distribution for white oak"
"Find plots with high pine density"
"What's the average height of Douglas fir trees?"
"Compare growth rates between hardwood and softwood species"
```

## CLI Commands

### Information Commands
- `help` - Show available commands and usage tips
- `schema` - Display database table structure
- `evalids [state_code]` - List available evaluation IDs
- `examples` - Show common query patterns
- `species <name>` - Find species codes by name

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
from pyfia.ai_agent import FIAAgent, FIAAgentConfig

config = FIAAgentConfig(
    model_name="gpt-4o",           # OpenAI model
    temperature=0.1,               # Response creativity (0-1)
    max_tokens=2000,              # Maximum response length
    result_limit=100,             # Query result limit
    enable_query_validation=True,  # Validate SQL before execution
    enable_safety_checks=True,     # Enable safety features
    verbose=False                 # Detailed logging
)

agent = FIAAgent("/path/to/database.duckdb", config)
```

### CLI Options

```bash
fia-ai --help                    # Show all options
fia-ai --model gpt-4o-mini      # Use different model
fia-ai --temperature 0.2        # Adjust creativity
fia-ai --limit 50               # Limit query results
fia-ai --verbose               # Enable detailed output
```

## Technical Architecture

### LangGraph Workflow

The AI agent uses a sophisticated LangGraph workflow:

1. **Query Planner**: Analyzes user input and determines intent
2. **Tool User**: Gathers database schema and contextual information
3. **Query Generator**: Creates safe, validated SQL queries
4. **Query Executor**: Executes queries with safety checks
5. **Response Formatter**: Formats results with explanations

### Specialized Tools

- **execute_fia_query**: Safe SQL execution with result limits
- **get_database_schema**: Schema information for query planning
- **get_evalid_info**: Evaluation metadata for statistical queries
- **find_species_codes**: Species name to code resolution
- **get_estimation_examples**: Common query patterns

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

### Adding New Tools

1. Create tool function in `ai_agent.py`
2. Add to `_create_tools()` method
3. Update workflow if needed
4. Add tests and documentation

### Improving Query Understanding

1. Enhance system prompt with domain knowledge
2. Add more example patterns
3. Improve query validation logic
4. Test with diverse query types

### Extending CLI Features

1. Add new commands to `cli_ai_agent.py`
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