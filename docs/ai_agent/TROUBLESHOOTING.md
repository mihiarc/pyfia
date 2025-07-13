# AI Agent Troubleshooting Guide

This guide helps resolve common issues with the PyFIA AI Agent.

## Quick Diagnostics

Run this command to check your setup:

```bash
# Check all dependencies and connections
pyfia-ai --diagnose
```

This will verify:
- ✓ Python version (3.11+)
- ✓ Required packages installed
- ✓ OpenAI API key configured
- ✓ Database connection
- ✓ Memory/cache directories

## Common Issues

### 1. OpenAI API Key Issues

#### Problem: "OpenAI API key not found"

**Solution:**
```bash
# Set the API key
export OPENAI_API_KEY="sk-..."

# Or add to your shell profile
echo 'export OPENAI_API_KEY="sk-..."' >> ~/.bashrc
source ~/.bashrc
```

#### Problem: "Invalid API key"

**Check:**
- Key starts with `sk-`
- No extra spaces or quotes
- Key hasn't been revoked

**Test:**
```python
import openai
openai.api_key = "your-key"
openai.models.list()  # Should work
```

### 2. Database Connection Issues

#### Problem: "Database not found"

**Solutions:**

1. Check file path:
```bash
# Verify file exists
ls -la /path/to/database.duckdb

# Use absolute path
pyfia-ai /absolute/path/to/database.duckdb
```

2. Check permissions:
```bash
# Ensure read access
chmod 644 database.duckdb
```

#### Problem: "Cannot open database"

**Possible causes:**
- Database is corrupted
- Wrong DuckDB version
- File is locked by another process

**Fix:**
```python
import duckdb
# Test connection
conn = duckdb.connect("database.duckdb", read_only=True)
conn.execute("SELECT 1").fetchall()
```

### 3. Import and Dependency Issues

#### Problem: "ModuleNotFoundError: langchain"

**Solution:**
```bash
# Install with AI dependencies
pip install pyfia[langchain]

# Or install all
pip install pyfia[all]
```

#### Problem: "ImportError: cannot import name 'create_react_agent'"

**Fix - Update LangGraph:**
```bash
pip install --upgrade langgraph langchain-core
```

### 4. Query Processing Issues

#### Problem: "No results found"

**Common causes and fixes:**

1. **Wrong EVALID**:
```bash
# Find available EVALIDs
fia-ai> evalid

# Use correct EVALID
fia-ai> Using evalid 372301, count trees
```

2. **Too restrictive filters**:
```bash
# Start broad
fia-ai> How many trees total?

# Then narrow down
fia-ai> How many oak trees?
fia-ai> How many large oak trees?
```

3. **Species name issues**:
```bash
# Use partial names
fia-ai> species pine  # Finds all pines

# Or use scientific names
fia-ai> Find Quercus alba
```

#### Problem: "Query timeout"

**Solutions:**

1. Simplify query:
```bash
# Instead of
fia-ai> Show all tree measurements

# Use
fia-ai> Show tree counts by species
```

2. Add limits:
```bash
fia-ai> Show top 100 plots by biomass
```

3. Use specific filters:
```bash
fia-ai> Show data for North Carolina only
```

### 5. Memory and Performance Issues

#### Problem: "Conversation not persisting"

**Check checkpoint directory:**
```bash
# Default location
ls ~/.pyfia/checkpoints/

# Set custom location
pyfia-ai database.duckdb --checkpoint-dir ./my_checkpoints
```

**Fix permissions:**
```bash
mkdir -p ~/.pyfia/checkpoints
chmod 755 ~/.pyfia/checkpoints
```

#### Problem: "Slow responses"

**Optimizations:**

1. **Enable caching**:
```python
agent = FIAAgent("db.duckdb", enable_cache=True)
```

2. **Reduce token usage**:
```python
agent = FIAAgent("db.duckdb", max_tokens=1000)
```

3. **Use faster model**:
```python
agent = FIAAgent("db.duckdb", model_name="gpt-3.5-turbo")
```

### 6. Formatting and Display Issues

#### Problem: "No colors or formatting in terminal"

**Solutions:**

1. **Check terminal support**:
```bash
# Test Rich support
python -c "from rich import print; print('[bold green]Test[/bold green]')"
```

2. **Force color output**:
```bash
export FORCE_COLOR=1
pyfia-ai database.duckdb
```

3. **Disable Rich if needed**:
```python
agent = FIAAgent("db.duckdb", use_rich=False)
```

#### Problem: "Tables not displaying correctly"

**Fix terminal width:**
```bash
# Check terminal size
echo $COLUMNS

# Set minimum width
export COLUMNS=120
```

## Error Messages Reference

### API Errors

| Error | Meaning | Solution |
|-------|---------|----------|
| `RateLimitError` | Too many requests | Wait and retry, or upgrade API plan |
| `InvalidRequestError` | Bad request format | Check query syntax |
| `AuthenticationError` | Invalid API key | Verify key configuration |
| `APIConnectionError` | Network issue | Check internet connection |

### Database Errors

| Error | Meaning | Solution |
|-------|---------|----------|
| `CatalogException` | Table not found | Verify table names with `schema` |
| `BinderException` | Column not found | Check column names |
| `ParserException` | SQL syntax error | Review query syntax |
| `IOException` | Can't read file | Check file path and permissions |

### Agent Errors

| Error | Meaning | Solution |
|-------|---------|----------|
| `ToolExecutionError` | Tool failed | Check tool parameters |
| `ValidationError` | Invalid input | Review input format |
| `MemoryError` | Out of memory | Reduce result size |
| `TimeoutError` | Query too slow | Simplify query |

## Debugging Techniques

### 1. Enable Verbose Mode

```bash
# See detailed processing
pyfia-ai database.duckdb --verbose

# Or in Python
agent = FIAAgent("db.duckdb", verbose=True)
```

### 2. Check Logs

```bash
# View agent logs
tail -f ~/.pyfia/logs/agent.log

# Enable debug logging
export PYFIA_LOG_LEVEL=DEBUG
```

### 3. Test Individual Components

```python
# Test database connection
from pyfia.database.query_interface import DuckDBQueryInterface
qi = DuckDBQueryInterface("database.duckdb")
qi.test_connection()

# Test tools directly
from pyfia.ai.agent import find_species_codes
result = find_species_codes("oak")
print(result)
```

### 4. Trace LangChain Execution

```bash
# Enable LangChain tracing
export LANGCHAIN_TRACING_V2=true
export LANGCHAIN_API_KEY="your-langsmith-key"
```

## Performance Optimization

### Query Optimization

1. **Use EVALIDs**:
```bash
# Slow - scans all data
fia-ai> Count all trees

# Fast - uses EVALID index
fia-ai> Count trees in evalid 372301
```

2. **Filter early**:
```bash
# Efficient
fia-ai> Show pine volume in California

# Inefficient  
fia-ai> Show all volume, then filter for pine in California
```

### Resource Management

1. **Limit memory usage**:
```python
# Set max result size
agent = FIAAgent("db.duckdb", max_result_mb=100)
```

2. **Clear cache periodically**:
```bash
fia-ai> clear cache
```

3. **Close unused connections**:
```python
agent.close()  # When done
```

## Getting Help

### Self-Help Resources

1. **Built-in help**:
```bash
fia-ai> help
fia-ai> help export
```

2. **Examples**:
```bash
fia-ai> show examples
```

3. **Documentation**:
- [User Guide](USER_GUIDE.md)
- [Examples](EXAMPLES.md)
- [Tools Reference](TOOLS_REFERENCE.md)

### Community Support

1. **GitHub Issues**: Report bugs and request features
2. **Discussions**: Ask questions and share tips
3. **Wiki**: Community-contributed guides

### Debug Information

When reporting issues, include:

```bash
# System info
python --version
pip show pyfia

# Error details
pyfia-ai --diagnose > debug_info.txt

# Sample query that fails
echo "Your problematic query"
```

## Recovery Procedures

### Reset Agent State

```bash
# Clear all caches and checkpoints
rm -rf ~/.pyfia/checkpoints/*
rm -rf ~/.pyfia/cache/*

# Restart fresh
pyfia-ai database.duckdb
```

### Reinstall Package

```bash
# Complete reinstall
pip uninstall pyfia
pip install pyfia[all] --force-reinstall
```

### Database Repair

```python
import duckdb

# Verify database integrity
conn = duckdb.connect("database.duckdb")
conn.execute("PRAGMA integrity_check").fetchall()

# Export and reimport if needed
conn.execute("EXPORT DATABASE 'backup' (FORMAT PARQUET)")
```

## Preventive Measures

1. **Regular Updates**:
```bash
pip install --upgrade pyfia[all]
```

2. **Monitor Resources**:
```bash
# Check disk space
df -h ~/.pyfia

# Monitor memory
htop  # While running agent
```

3. **Backup Conversations**:
```bash
# Backup checkpoints
cp -r ~/.pyfia/checkpoints ~/.pyfia/checkpoints.backup
```

4. **Test After Updates**:
```bash
pyfia-ai --diagnose
```

Remember: Most issues can be resolved by checking the basics - API key, database path, and dependencies. When in doubt, start with `pyfia-ai --diagnose`.