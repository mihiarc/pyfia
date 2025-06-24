# Pydantic v2 Migration Guide for pyFIA

## Overview

pyFIA uses Pydantic v2.11+ for data validation. This guide documents our Pydantic v2 patterns and best practices.

## Key Changes from v1 to v2

### 1. Configuration
```python
# OLD (v1)
class Model(BaseModel):
    class Config:
        arbitrary_types_allowed = True
        
# NEW (v2)
class Model(BaseModel):
    model_config = {"arbitrary_types_allowed": True}
```

### 2. Validators
```python
# OLD (v1)
from pydantic import validator

@validator('field_name')
def validate_field(cls, v):
    return v

# NEW (v2)
from pydantic import field_validator

@field_validator('field_name')
@classmethod
def validate_field(cls, v):
    return v
```

### 3. Model Methods
```python
# OLD (v1)
model.dict()
model.json()
Model.parse_obj(data)
Model.parse_raw(json_str)

# NEW (v2)
model.model_dump()
model.model_dump_json()
Model.model_validate(data)
Model.model_validate_json(json_str)
```

## Current Usage in pyFIA

### Models (`models.py`)
- ✅ Using `field_validator` with `@classmethod`
- ✅ Using `model_config` instead of Config class
- ✅ Proper `model_post_init` signature with `__context: Any`

### AI Agent (`ai_agent.py`)
- ✅ Using `model_config` for arbitrary types
- ✅ Using Pydantic v2 BaseModel
- ✅ Proper Field usage

### Best Practices

1. **Type Annotations**
   ```python
   from typing import Optional, List, Dict, Any
   from pydantic import BaseModel, Field
   
   class MyModel(BaseModel):
       name: str = Field(..., description="Name field")
       age: Optional[int] = Field(None, ge=0, le=150)
       tags: List[str] = Field(default_factory=list)
   ```

2. **Validation**
   ```python
   @field_validator('email')
   @classmethod
   def validate_email(cls, v: str) -> str:
       if '@' not in v:
           raise ValueError('Invalid email')
       return v.lower()
   ```

3. **Model Configuration**
   ```python
   class MyModel(BaseModel):
       model_config = {
           "arbitrary_types_allowed": True,
           "validate_assignment": True,
           "extra": "forbid",
           "str_strip_whitespace": True,
       }
   ```

4. **Computed Fields**
   ```python
   from pydantic import computed_field
   
   class MyModel(BaseModel):
       radius: float
       
       @computed_field
       @property
       def area(self) -> float:
           return 3.14159 * self.radius ** 2
   ```

5. **Model Serialization**
   ```python
   # Dump to dict
   data = model.model_dump(exclude_unset=True)
   
   # Dump to JSON
   json_str = model.model_dump_json(indent=2)
   
   # Custom serialization
   data = model.model_dump(
       mode='json',
       exclude={'password'},
       by_alias=True
   )
   ```

## Performance Improvements

Pydantic v2 is significantly faster:
- 5-50x faster validation
- Rust-based core (`pydantic-core`)
- Better memory efficiency

## Common Patterns in pyFIA

### DataFrame Validation
```python
class FIADataFrameWrapper(BaseModel):
    model_config = {"arbitrary_types_allowed": True}
    
    data: pl.DataFrame
    table_name: str
    
    @field_validator("data")
    @classmethod
    def validate_dataframe(cls, v: pl.DataFrame) -> pl.DataFrame:
        if not isinstance(v, pl.DataFrame):
            raise ValueError("data must be a polars DataFrame")
        return v
```

### Enum Validation
```python
from enum import Enum

class EvalType(str, Enum):
    VOL = "VOL"
    GRM = "GRM"
    CHNG = "CHNG"
    
class EvaluationInfo(BaseModel):
    eval_typ: EvalType = EvalType.VOL
```

### Settings Management
```python
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    database_path: str = "fia.duckdb"
    api_key: Optional[str] = None
    
    model_config = {
        "env_prefix": "PYFIA_",
        "env_file": ".env",
        "env_file_encoding": "utf-8",
    }
```

## Testing with Pydantic

```python
import pytest
from pydantic import ValidationError

def test_model_validation():
    # Valid data
    model = MyModel(name="test", age=25)
    assert model.name == "test"
    
    # Invalid data
    with pytest.raises(ValidationError) as exc_info:
        MyModel(name="", age=200)
    
    errors = exc_info.value.errors()
    assert len(errors) == 2
```

## Resources

- [Pydantic v2 Documentation](https://docs.pydantic.dev/latest/)
- [Migration Guide](https://docs.pydantic.dev/latest/migration/)
- [Performance Comparison](https://docs.pydantic.dev/latest/benchmarks/)
- [Pydantic Settings](https://docs.pydantic.dev/latest/concepts/pydantic_settings/)