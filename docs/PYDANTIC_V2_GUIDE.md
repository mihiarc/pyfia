# Pydantic v2 Quick Reference for pyFIA

## 🔍 Quick Migration Checklist

### ✅ Configuration Changes
```python
# FIND: class Config:
# REPLACE WITH: model_config = {}

# v1 → v2 mapping
"allow_population_by_field_name" → "populate_by_name"
"schema_extra" → "json_schema_extra"
"orm_mode" → "from_attributes"
```

### ✅ Import Updates
```python
# OLD IMPORTS (v1)
from pydantic import validator, root_validator
from pydantic import BaseSettings  # v1

# NEW IMPORTS (v2)
from pydantic import field_validator, model_validator
from pydantic_settings import BaseSettings  # v2
```

### ✅ Method Replacements
| v1 Method | v2 Method |
|-----------|-----------|
| `.dict()` | `.model_dump()` |
| `.json()` | `.model_dump_json()` |
| `.copy()` | `.model_copy()` |
| `.parse_obj()` | `.model_validate()` |
| `.parse_raw()` | `.model_validate_json()` |
| `.schema()` | `.model_json_schema()` |
| `.__fields__` | `.model_fields` |

### ✅ Validator Patterns
```python
# v1 PATTERN
@validator('field_name')
def validate_field(cls, v):
    return v

# v2 PATTERN
@field_validator('field_name')
@classmethod
def validate_field(cls, v):
    return v
```

## 🎯 Common pyFIA Patterns

### Polars DataFrame Models
```python
class DataFrameModel(BaseModel):
    model_config = {"arbitrary_types_allowed": True}

    df: pl.DataFrame
    name: str

    @field_validator('df')
    @classmethod
    def validate_dataframe(cls, v):
        if not isinstance(v, pl.DataFrame):
            raise ValueError("Must be a Polars DataFrame")
        return v
```

### Settings Pattern
```python
from pydantic_settings import BaseSettings

class FIASettings(BaseSettings):
    model_config = {
        "env_prefix": "PYFIA_",
        "env_file": ".env",
        "validate_assignment": True,
    }

    database_path: str = "fia.duckdb"
    api_key: Optional[str] = None
```

### Enum Models
```python
from enum import Enum

class EvalType(str, Enum):
    VOL = "VOL"
    GRM = "GRM"

class EvalModel(BaseModel):
    eval_type: EvalType = EvalType.VOL
    evalid: str = Field(..., pattern=r"^\d{6}$")
```

## 🔧 Search Patterns for Migration

### Find Old Validators
```regex
@validator\(['"]?\w+['"]?\)
```

### Find Old Config Classes
```regex
class\s+Config:
```

### Find Old Methods
```regex
\.dict\(\)|\.json\(\)|\.parse_obj\(|\.parse_raw\(|\.copy\(
```

### Find Old Imports
```regex
from pydantic import.*validator|from pydantic import BaseSettings
```

## ⚡ Performance Tips

1. **Use `model_validate` instead of constructors for untrusted data**
2. **Leverage `exclude_unset=True` for partial updates**
3. **Use `mode='json'` for JSON-compatible output**
4. **Prefer `computed_field` over properties for derived values**

## 🚨 Common Pitfalls

1. **Missing `@classmethod` on validators** - Always required in v2
2. **Using `values` dict in validators** - No longer available, validate individual fields
3. **Forgetting to update imports** - `BaseSettings` moved to `pydantic_settings`
4. **Using `__fields__`** - Replace with `.model_fields`

## 📋 Verification Commands

```bash
# Check for v1 patterns
grep -r "@validator" pyfia/
grep -r "class Config:" pyfia/
grep -r "\.dict()" pyfia/
grep -r "from pydantic import.*BaseSettings" pyfia/

# Run type checking
uv run mypy pyfia/

# Run tests
uv run pytest -xvs
```