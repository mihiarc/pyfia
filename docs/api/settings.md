# Settings

Configuration management for PyFIA.

## Overview

PyFIA uses a centralized settings system with support for environment variables.

```python
from pyfia import settings

# View current settings
print(settings.database_path)
print(settings.max_threads)

# Modify settings
settings.max_threads = 8
settings.cache_enabled = True
```

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `PYFIA_DATABASE_PATH` | Default database path | `fia.duckdb` |
| `PYFIA_DATABASE_ENGINE` | Database engine | `duckdb` |
| `PYFIA_MAX_THREADS` | Max processing threads | `4` |
| `PYFIA_CACHE_ENABLED` | Enable caching | `true` |
| `PYFIA_LOG_LEVEL` | Logging level | `CRITICAL` |

## Class Reference

::: pyfia.PyFIASettings
    options:
      show_root_heading: true
      show_source: true

## Helper Functions

::: pyfia.get_default_db_path
    options:
      show_root_heading: true

::: pyfia.get_default_engine
    options:
      show_root_heading: true
