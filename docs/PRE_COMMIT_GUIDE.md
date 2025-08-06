# Pre-commit Quick Reference for pyFIA

## 🚀 Essential Commands

| Command | Purpose |
|---------|---------|
| `uv run pyfia-setup-precommit` | Install pre-commit hooks |
| `uv run pre-commit run --all-files` | Run all hooks on all files |
| `uv run pre-commit run ruff` | Run specific hook |
| `git commit --no-verify` | Skip hooks (emergency only!) |
| `SKIP=mypy git commit` | Skip specific hooks |

## 🔧 Hook Categories

### 🧹 Formatting & Cleanup
- **ruff-format**: Auto-formats Python code
- **trailing-whitespace**: Removes trailing spaces
- **end-of-file-fixer**: Adds missing newlines
- **mixed-line-ending**: Converts to LF line endings

### 🔍 Linting & Validation
- **ruff**: Python linting with auto-fixes
- **check-yaml/json/toml**: Syntax validation
- **check-merge-conflict**: Detects merge markers
- **check-case-conflict**: Prevents case conflicts

### 🎯 Type Checking
- **mypy**: Static type checking for Python
- Configured in `pyproject.toml`
- Checks `pyfia/` directory only

### 🛡️ Security
- **bandit**: Security vulnerability scanner
- **detect-secrets**: Prevents credential commits
- **detect-private-key**: Blocks private keys

### 📝 Documentation
- **pydocstyle**: Docstring convention checker
- **check-docstring-first**: Docstring placement
- **prettier**: Markdown/YAML/JSON formatting

## 🔥 Quick Fixes

| Error Type | Fix Command |
|------------|-------------|
| Formatting | `uv run ruff format pyfia/` |
| Linting | `uv run ruff check --fix pyfia/` |
| Type errors | `uv run mypy pyfia/specific_file.py` |
| Large files | Add to `.gitignore` or use Git LFS |
| Docstrings | Add `"""Description."""` at top |

## 🚨 Common Error Patterns

### Ruff Formatting
```
Would reformat: pyfia/core/fia.py
```
**Fix**: `uv run ruff format pyfia/core/fia.py`

### Mypy Type Errors
```
error: Argument 1 to "func" has incompatible type "str"; expected "int"
```
**Fix**: Update type annotations or fix the code

### Bandit Security
```
B101 assert_used
```
**Fix**: Replace `assert` with proper error handling

### Missing Docstring
```
D100 Missing docstring in public module
```
**Fix**: Add module docstring at file top

## ⚙️ Configuration Files

| File | Purpose |
|------|---------|
| `.pre-commit-config.yaml` | Hook definitions and versions |
| `pyproject.toml` | Tool configs (ruff, mypy, pydocstyle) |
| `.bandit` | Security scan exclusions |
| `.secrets.baseline` | Known false positive secrets |

## 🎯 When to Use Pre-commit

1. **ALWAYS before commits** - Catches issues early
2. **After major changes** - Ensures consistency
3. **Before PR submission** - Avoids CI failures
4. **Weekly maintenance** - Run `autoupdate`

## 🛠️ Advanced Usage

### Update All Hooks
```bash
uv run pre-commit autoupdate
uv run pre-commit run --all-files
```

### Debug Specific Hook
```bash
uv run pre-commit run mypy --verbose --all-files
```

### Performance Mode
```bash
# Skip slow hooks during development
SKIP=mypy,bandit uv run pre-commit run --all-files
```

### CI Integration
```yaml
# Already configured in .github/workflows/
name: pre-commit
on: [push, pull_request]
```

## 📋 Hook Reference

| Hook | Auto-fix | Speed | Importance |
|------|----------|-------|------------|
| ruff-format | ✅ | Fast | High |
| ruff | ✅ | Fast | High |
| trailing-whitespace | ✅ | Fast | Medium |
| mypy | ❌ | Slow | High |
| bandit | ❌ | Medium | High |
| detect-secrets | ❌ | Medium | Critical |
| prettier | ✅ | Fast | Low |

## 🔧 Troubleshooting

```bash
# Reset everything
uv run pre-commit clean
uv run pre-commit uninstall
uv run pre-commit install

# Update baseline for secrets
uv run detect-secrets scan --baseline .secrets.baseline

# Check hook versions
uv run pre-commit autoupdate --freeze
```