# PyFIA AI Agent

Welcome to the PyFIA AI Agent documentation! The AI Agent provides a natural language interface for querying Forest Inventory Analysis (FIA) databases, making complex forest data analysis accessible to everyone.

## 🚀 Quick Start

### Installation

```bash
# Install pyFIA with AI agent support
pip install pyfia[langchain]

# Or install all features
pip install pyfia[all]
```

### Set up OpenAI API Key

```bash
export OPENAI_API_KEY="your-api-key-here"
```

### Basic Usage

=== "Command Line"

    ```bash
    # Start the AI assistant
    pyfia-ai path/to/database.duckdb
    
    # Ask questions in natural language
    fia-ai> How many oak trees are in California?
    fia-ai> What's the total forest area by ownership type?
    fia-ai> Show me biomass trends over the last decade
    ```

=== "Python API"

    ```python
    from pyfia.ai.agent import FIAAgent
    
    # Initialize the agent
    agent = FIAAgent("path/to/database.duckdb")
    
    # Ask questions
    result = agent.query("How many live trees are in Oregon?")
    print(result)
    ```

## 🌟 Key Features

### Natural Language Understanding
- Ask questions in plain English
- No SQL knowledge required
- Intelligent interpretation of forest terminology

### Domain Expertise
- Deep understanding of FIA data structures
- Knows about species codes, forest types, and measurements
- Handles complex statistical queries correctly

### Interactive Experience
- Beautiful terminal interface with Rich formatting
- Conversation memory for follow-up questions
- Export results in multiple formats

### Safety & Validation
- Read-only database access
- Query validation before execution
- Helpful error messages and suggestions

## 📚 What You Can Do

### Basic Queries
- Count trees by species, size, or location
- Calculate forest area by various attributes
- Analyze volume and biomass estimates
- Track mortality and growth rates

### Advanced Analysis
- Compare data across time periods
- Aggregate by custom groupings
- Filter by complex conditions
- Generate statistical summaries

### Examples

```bash
# Species composition
fia-ai> What are the top 10 tree species by volume in the Pacific Northwest?

# Temporal analysis
fia-ai> How has oak forest area changed in Texas between 2010 and 2020?

# Spatial queries
fia-ai> Show me counties with the highest biomass density in Colorado

# Complex filtering
fia-ai> Find all plots with large diameter Douglas fir on public land
```

## 🎯 Next Steps

- **[User Guide](USER_GUIDE.md)** - Comprehensive guide to all features
- **[Examples](EXAMPLES.md)** - Real-world usage scenarios
- **[Troubleshooting](TROUBLESHOOTING.md)** - Common issues and solutions
- **[Developer Guide](DEVELOPER_GUIDE.md)** - Extend and customize the agent

## 💡 Tips for Success

1. **Be Specific**: Include location, time period, and measurement type
2. **Use Forest Terms**: The agent understands DBH, basal area, site index, etc.
3. **Ask Follow-ups**: Build on previous queries for deeper analysis
4. **Check EVALIDs**: Use `evalid` command to see available evaluations

## 🆘 Getting Help

If you encounter issues:

1. Check the [Troubleshooting Guide](TROUBLESHOOTING.md)
2. Review [Common Examples](EXAMPLES.md)
3. Use the `help` command in the CLI
4. Report issues on [GitHub](https://github.com/your-username/pyfia/issues)

---

Ready to explore forest data with AI? Start with `pyfia-ai` and ask your first question!