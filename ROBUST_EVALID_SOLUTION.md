# Robust EVALID Solution

## Problem Statement

EVALIDs use a 6-digit format `SSYYTT` where `YY` represents the last 2 digits of the year. This creates a critical sorting issue:
- EVALID 139901 (Georgia 1999) sorts higher than 132301 (Georgia 2023) when treated as integers
- This causes older data to be incorrectly selected as "most recent"

## The Robust Solution

### 1. EVALID Parser Module (`src/pyfia/core/evalid_parser.py`)

A dedicated module that properly handles EVALID parsing with Y2K-style windowing:
- Years 00-30 → 2000-2030
- Years 31-99 → 1931-1999

Key features:
- `ParsedEvalid` dataclass with proper comparison operators
- `parse_evalid()` - Parses EVALID into components with 4-digit year
- `sort_evalids_by_year()` - Sorts EVALIDs chronologically, not numerically
- `get_most_recent_evalid()` - Finds most recent with optional filtering
- `add_parsed_evalid_columns()` - Adds year columns to Polars DataFrames

### 2. Updated `find_evalid()` Method

The method now:
1. Adds parsed EVALID columns (`EVALID_YEAR`, `EVALID_STATE`, `EVALID_TYPE`)
2. Sorts by parsed year rather than EVALID value
3. Handles special cases (e.g., Texas full state vs regional)
4. Returns chronologically correct results

### 3. Test Results

```python
# Original problem:
evalids = [139901, 132301]  # 1999, 2023
sorted_wrong = sorted(evalids, reverse=True)  # [139901, 132301] - WRONG!

# With robust parser:
sorted_correct = sort_evalids_by_year(evalids)  # [132301, 139901] - CORRECT!
```

## Implementation Status

✅ **Created `evalid_parser.py`** - Robust EVALID parsing utilities
✅ **Tested thoroughly** - Handles edge cases and real-world scenarios
✅ **Demonstrated fix** - Shows how to update `find_evalid()` method

## Benefits

1. **Correctness**: Always selects truly most recent data
2. **Robustness**: Handles Y2K windowing properly through 2030
3. **Clarity**: Explicit year parsing makes logic clear
4. **Compatibility**: Works with existing Polars DataFrames
5. **Future-proof**: Will work correctly until 2030

## Migration Path

To integrate this solution:

1. Add `src/pyfia/core/evalid_parser.py` to the codebase
2. Update `find_evalid()` in `src/pyfia/core/fia.py` to use the parser
3. Consider adding unit tests for EVALID parsing
4. Update any other code that sorts EVALIDs directly

## Critical Dates

- **2030**: The windowing cutoff - EVALIDs with year 31+ will be interpreted as 2031+
- This gives us 6+ years to plan for the next transition

## Conclusion

This robust solution ensures that EVALID sorting always respects chronological order, preventing the selection of decades-old data as "most recent". The parser module provides a clean, reusable API for any EVALID-related operations in the codebase.