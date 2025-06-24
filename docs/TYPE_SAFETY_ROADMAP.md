# Type Safety Roadmap for pyFIA

## Overview

This document outlines our gradual adoption plan for comprehensive type safety in pyFIA using both mypy and ty (alpha).

## Current Status

- **mypy**: Configured with gradual strictness, per-module overrides
- **ty**: Alpha version configured for modern type checking
- **py.typed**: Marker file added for PEP 561 compliance

## Type Checking Strategy

### Phase 1: Foundation (Current)
- [x] Configure both mypy and ty with lenient settings
- [x] Add py.typed marker for type hint support
- [x] Create unified type checking script
- [x] Enable strict typing for core modules (config, models, constants)

### Phase 2: Core Module Types (Next)
- [ ] Add complete type annotations to `core.py`
- [ ] Type annotate all estimation modules (`area.py`, `biomass.py`, etc.)
- [ ] Add return type annotations to all public functions
- [ ] Create type stubs for complex types

### Phase 3: Gradual Strictness
- [ ] Enable `disallow_untyped_defs` module by module
- [ ] Add Protocol types for estimator interfaces
- [ ] Type annotate all CLI modules
- [ ] Enable stricter mypy settings incrementally

### Phase 4: Advanced Types
- [ ] Add generic types where appropriate
- [ ] Create custom type guards
- [ ] Add overloaded function signatures
- [ ] Enable `disallow_any_generics`

### Phase 5: Full Type Safety
- [ ] Enable strict mode in mypy
- [ ] Achieve zero type errors in both checkers
- [ ] Add runtime type validation with beartype/typeguard
- [ ] Migrate fully to ty when it reaches stable

## Usage

Run both type checkers:
```bash
uv run python scripts/typecheck.py
# or
uv run pyfia-typecheck
```

Run individually:
```bash
uv run mypy pyfia/
uv run ty check pyfia/
```

## Per-Module Status

| Module | Type Coverage | mypy Strict | ty Compliant | Notes |
|--------|--------------|-------------|--------------|-------|
| config.py | Partial | ✅ | 🔄 | Return types added |
| models.py | Good | ✅ | 🔄 | Pydantic provides types |
| constants.py | Full | ✅ | 🔄 | Simple module |
| core.py | Partial | ❌ | ❌ | Needs work |
| area.py | Minimal | ❌ | ❌ | Complex calculations |
| cli*.py | Minimal | ❌ | ❌ | Many dynamic calls |

## Best Practices

1. **New Code**: Always add type hints
2. **Refactoring**: Add types when touching code
3. **Complex Types**: Use type aliases for clarity
4. **Documentation**: Types are documentation
5. **Gradual**: Don't break working code for types

## Common Patterns

### Type Aliases
```python
from typing import TypeAlias
EVALIDType: TypeAlias = int
PlotDataFrame: TypeAlias = pl.DataFrame
```

### Protocols
```python
from typing import Protocol
class Estimator(Protocol):
    def estimate(self, db: FIA, **kwargs) -> pl.DataFrame: ...
```

### Overloads
```python
from typing import overload
@overload
def process(data: str) -> str: ...
@overload
def process(data: int) -> int: ...
```

## Resources

- [mypy documentation](https://mypy.readthedocs.io/)
- [ty documentation](https://github.com/inakleinbottle/ty)
- [PEP 484 - Type Hints](https://peps.python.org/pep-0484/)
- [Python Type Checking Guide](https://realpython.com/python-type-checking/)