# PyFIA AI Agent User Guide

This comprehensive guide covers all features and capabilities of the PyFIA AI Agent.

## Table of Contents

1. [Getting Started](#getting-started)
2. [Natural Language Queries](#natural-language-queries)
3. [CLI Commands](#cli-commands)
4. [Advanced Features](#advanced-features)
5. [Configuration](#configuration)
6. [Best Practices](#best-practices)

## Getting Started

### Installation Requirements

- Python 3.11 or higher
- OpenAI API key
- FIA database in DuckDB format

### First Run

```bash
# Set your API key
export OPENAI_API_KEY="sk-..."

# Start the AI agent
pyfia-ai /path/to/fia_database.duckdb

# You'll see a welcome message
🌲 Welcome to PyFIA AI Assistant!
Connected to database: /path/to/fia_database.duckdb

Type 'help' for available commands or ask any question about forest inventory data.

fia-ai>
```

## Natural Language Queries

### Basic Questions

The AI agent understands natural language questions about forest inventory data:

```bash
# Tree counts
fia-ai> How many live trees are there?
fia-ai> Count oak trees in California
fia-ai> What's the most common species in Oregon?

# Forest area
fia-ai> Total forest area in the Southeast
fia-ai> Timberland area by ownership type
fia-ai> Non-forest land in Texas

# Volume and biomass
fia-ai> Total volume of merchantable timber
fia-ai> Aboveground biomass by species
fia-ai> Carbon storage in national forests
```

### Complex Queries

The agent handles sophisticated analysis requests:

```bash
# Multi-dimensional analysis
fia-ai> Show volume by species and diameter class for public lands

# Temporal comparisons
fia-ai> Compare forest area between 2010 and 2020 evaluations

# Statistical queries
fia-ai> Calculate average trees per acre with confidence intervals

# Spatial filtering
fia-ai> Find counties with declining oak populations
```

### Follow-up Questions

The agent maintains conversation context:

```bash
fia-ai> How many pine trees are in Georgia?
# Response shows pine tree count...

fia-ai> What about oak trees?
# Agent understands "in Georgia" context

fia-ai> Show me the top 5 species
# Agent knows you mean "top 5 species in Georgia"
```

## CLI Commands

### Database Commands

| Command | Description | Example |
|---------|-------------|---------|
| `connect <path>` | Connect to a different database | `connect /new/path/db.duckdb` |
| `schema [table]` | View database schema | `schema TREE` |
| `tables` | List all available tables | `tables` |
| `evalid [search]` | Show evaluation IDs | `evalid North Carolina` |

### Analysis Commands

| Command | Description | Example |
|---------|-------------|---------|
| `concepts [term]` | Explore FIA concepts | `concepts biomass` |
| `species [search]` | Look up species codes | `species oak` |
| `states` | List state codes | `states` |

### Result Management

| Command | Description | Example |
|---------|-------------|---------|
| `last [n]` | Show last n results | `last 5` |
| `export <file>` | Export results | `export results.csv` |
| `history` | View query history | `history` |
| `save` | Save current session | `save session.json` |

### Utility Commands

| Command | Description | Example |
|---------|-------------|---------|
| `clear` | Clear the screen | `clear` |
| `help [command]` | Get help | `help export` |
| `settings` | View/edit settings | `settings` |
| `quit` or `exit` | Exit the application | `quit` |

## Advanced Features

### Working with EVALIDs

EVALIDs are crucial for statistically valid estimates:

```bash
# View available evaluations
fia-ai> evalid

# Use specific EVALID
fia-ai> Using evalid 372301, what's the total forest area?

# Find latest evaluation
fia-ai> What's the most recent evaluation for North Carolina?
```

### Conversation Memory

The agent remembers your conversation:

```python
# Python API with persistent memory
agent = FIAAgent("database.duckdb", checkpoint_dir="./checkpoints")

# Use thread IDs for separate conversations
response1 = agent.query("Count trees", thread_id="analysis1")
response2 = agent.query("Count trees", thread_id="analysis2")
```

### Export Formats

Export results in various formats:

```bash
# CSV export
fia-ai> export results.csv

# JSON export
fia-ai> export data.json

# Markdown table
fia-ai> export report.md

# Excel (if pandas installed)
fia-ai> export analysis.xlsx
```

### Query Validation

The agent validates queries before execution:

```bash
fia-ai> Show me invalid SQL
❌ Query validation failed: Invalid SQL syntax

fia-ai> Delete all trees  
❌ Safety check failed: Read-only access - no modifications allowed
```

## Configuration

### Agent Settings

Configure the agent behavior:

```python
from pyfia.ai.agent import FIAAgent

agent = FIAAgent(
    db_path="database.duckdb",
    model_name="gpt-4o",           # AI model to use
    temperature=0.1,               # Response consistency (0-1)
    verbose=True,                  # Detailed logging
    result_limit=1000,             # Max rows to return
    enable_human_approval=False    # Human-in-the-loop mode
)
```

### Environment Variables

```bash
# Required
export OPENAI_API_KEY="sk-..."

# Optional
export PYFIA_MODEL="gpt-4o"
export PYFIA_TEMPERATURE="0.1"
export PYFIA_VERBOSE="true"
export PYFIA_RESULT_LIMIT="500"
```

### CLI Configuration File

Create `~/.pyfia/config.json`:

```json
{
  "model": "gpt-4o",
  "temperature": 0.1,
  "result_limit": 1000,
  "export_format": "csv",
  "theme": "forest",
  "auto_save": true
}
```

## Best Practices

### Writing Effective Queries

1. **Be Specific About Location**
   - ❌ "How many trees?"
   - ✅ "How many trees in California?"

2. **Include Time Context**
   - ❌ "Show forest area"
   - ✅ "Show forest area for the 2023 evaluation"

3. **Specify Measurements**
   - ❌ "What's the volume?"
   - ✅ "What's the net merchantable volume in cubic feet?"

4. **Use Proper Terminology**
   - ❌ "Big trees"
   - ✅ "Trees with DBH >= 20 inches"

### Understanding Results

1. **Check the EVALID**: Results are specific to an evaluation
2. **Note Sample Size**: More plots = more reliable estimates
3. **Review Standard Errors**: Lower SE% = higher precision
4. **Consider Scope**: State-level vs county-level estimates

### Performance Tips

1. **Start Broad, Then Narrow**
   ```bash
   fia-ai> How many tree species are there?
   fia-ai> Show me the top 10 by volume
   fia-ai> Focus on pine species only
   ```

2. **Use Filters Wisely**
   - Add geographic filters first
   - Then temporal filters
   - Finally, attribute filters

3. **Leverage Memory**
   - Ask follow-up questions
   - Reference previous results
   - Build complex analyses incrementally

### Common Patterns

#### Species Analysis
```bash
# Basic species composition
fia-ai> What are the dominant species by basal area?

# Species-specific analysis
fia-ai> Show all pine species with their volume estimates

# Rare species
fia-ai> Which species have fewer than 1000 trees statewide?
```

#### Temporal Analysis
```bash
# Change over time
fia-ai> How has forest area changed over the last 20 years?

# Specific period comparison
fia-ai> Compare oak volume between 2010 and 2020 evaluations

# Trends
fia-ai> Show annual mortality rates for the last 5 evaluations
```

#### Spatial Analysis
```bash
# Regional patterns
fia-ai> Which counties have the most forest land?

# Ownership analysis
fia-ai> Compare species diversity between public and private lands

# Ecosystem analysis
fia-ai> Show forest types by ecoregion
```

## Tips and Tricks

### Keyboard Shortcuts
- `↑`/`↓` - Navigate command history
- `Ctrl+C` - Cancel current query
- `Ctrl+D` - Exit (same as `quit`)
- `Ctrl+L` - Clear screen (same as `clear`)

### Hidden Features
- `debug on/off` - Toggle debug mode
- `profile` - Show query performance stats
- `cache clear` - Clear query cache
- `version` - Show version information

### Integration Ideas
1. **Jupyter Notebooks**: Use the Python API for analysis
2. **Automated Reports**: Script regular queries
3. **Data Pipelines**: Export results for further processing
4. **Dashboards**: Feed real-time queries to visualization tools

## Next Steps

- Explore [real-world examples](EXAMPLES.md)
- Learn about [technical architecture](ARCHITECTURE.md)
- Review [troubleshooting guide](TROUBLESHOOTING.md)
- Read the [developer guide](DEVELOPER_GUIDE.md) to extend functionality