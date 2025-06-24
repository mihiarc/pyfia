# Pre-commit Hooks Guide for pyFIA

## Overview

Pre-commit hooks ensure code quality by automatically checking and fixing issues before commits. This guide covers our pre-commit setup for pyFIA.

## Quick Start

### Installation

```bash
# Install pre-commit hooks
uv run python scripts/setup_precommit.py
# or
uv run pyfia-setup-precommit
```

### Manual Usage

```bash
# Run all hooks on all files
uv run pre-commit run --all-files

# Run specific hook
uv run pre-commit run mypy

# Update hooks to latest versions
uv run pre-commit autoupdate
```

## Configured Hooks

### 1. **Standard Checks** (pre-commit-hooks)
- `trailing-whitespace`: Remove trailing whitespace
- `end-of-file-fixer`: Ensure files end with newline
- `check-yaml`: Validate YAML syntax
- `check-added-large-files`: Prevent large files (>1MB)
- `check-json`: Validate JSON syntax
- `check-toml`: Validate TOML syntax
- `check-merge-conflict`: Check for merge conflict markers
- `check-case-conflict`: Check for case conflicts
- `check-docstring-first`: Ensure docstring comes first
- `detect-private-key`: Detect private keys
- `mixed-line-ending`: Normalize line endings to LF

### 2. **Python Formatting** (ruff)
- `ruff-format`: Format code according to project style
- `ruff`: Lint code and auto-fix issues

### 3. **Type Checking** (mypy)
- Runs mypy with project configuration
- Checks all files in `pyfia/` directory
- Uses type stubs for dependencies

### 4. **Security** (bandit)
- Scans for common security issues
- Configured via `.bandit` file
- Excludes test files

### 5. **Documentation** (pydocstyle)
- Checks docstring conventions
- Configured in `pyproject.toml`
- Ignores specific rules for project style

### 6. **Secrets Detection** (detect-secrets)
- Prevents accidental commit of secrets
- Uses `.secrets.baseline` for known false positives

### 7. **Markdown/YAML/JSON** (prettier)
- Formats documentation files
- Ensures consistent style

## Handling Hook Failures

### Common Issues and Fixes

1. **Formatting Issues**
   ```bash
   # Auto-fix with ruff
   uv run ruff format pyfia/
   uv run ruff check --fix pyfia/
   ```

2. **Type Errors**
   ```bash
   # Check specific file
   uv run mypy pyfia/specific_file.py
   ```

3. **Large Files**
   - Add to `.gitignore` if appropriate
   - Use Git LFS for necessary large files
   - Compress or split large data files

4. **Security Issues**
   - Review bandit warnings carefully
   - Add false positives to `.bandit` skips
   - Refactor code to avoid security issues

5. **Missing Docstrings**
   ```python
   # Add module docstring at top of file
   """Module description."""
   
   # Add function docstring
   def function():
       """Function description."""
       pass
   ```

## Bypassing Hooks (Emergency Only)

```bash
# Skip all hooks (use sparingly!)
git commit --no-verify -m "Emergency fix"

# Skip specific hooks
SKIP=mypy,bandit git commit -m "Fix with known type issues"
```

## Updating Baseline Files

### Secrets Baseline
```bash
# Update after reviewing new detections
uv run detect-secrets scan --baseline .secrets.baseline
```

## CI/CD Integration

Pre-commit can be integrated with CI:

```yaml
# .github/workflows/pre-commit.yml
name: pre-commit
on: [push, pull_request]
jobs:
  pre-commit:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v3
    - uses: actions/setup-python@v4
    - uses: pre-commit/action@v3.0.0
```

## Best Practices

1. **Run hooks before pushing**: Saves CI time
2. **Keep hooks updated**: Run `pre-commit autoupdate` monthly
3. **Don't skip habitually**: Fix issues properly
4. **Add project-specific hooks**: Customize for your needs
5. **Document exceptions**: Explain any disabled checks

## Troubleshooting

### Hook Installation Issues
```bash
# Reinstall hooks
uv run pre-commit uninstall
uv run pre-commit install
```

### Cache Issues
```bash
# Clear pre-commit cache
uv run pre-commit clean
```

### Slow Performance
```bash
# Run hooks in parallel
uv run pre-commit run --all-files --show-diff-on-failure
```

## Adding New Hooks

Edit `.pre-commit-config.yaml`:
```yaml
- repo: https://github.com/owner/repo
  rev: vX.Y.Z
  hooks:
    - id: hook-id
      args: [--arg1, --arg2]
```

Then update:
```bash
uv run pre-commit autoupdate
uv run pre-commit run --all-files
```