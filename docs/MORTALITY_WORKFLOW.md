# Mortality Estimation Workflow

This document describes the mortality estimation workflow in pyFIA and how it integrates with the AI agent.

## Overview

Mortality estimation uses FIA's Growth/Removal/Mortality (GRM) methodology to calculate annual tree mortality rates. Unlike volume or area estimates, mortality requires special evaluation types and remeasurement data.

## AI Agent Integration

### Agent Tool Function

The AI agent includes an `execute_mortality_command` tool that:

1. **Automatically selects GRM EVALIDs** - Uses `get_recommended_evalid` with `"mortality"` analysis type
2. **Parses CLI arguments** - Converts natural language to function parameters
3. **Handles state filtering** - Extracts state codes from area_domain for EVALID selection
4. **Enhanced formatting** - Uses the result formatter for comprehensive output

### Example Agent Queries

```text
"What is the annual tree mortality rate in North Carolina?"
```

This will automatically:
- Parse "North Carolina" → STATECD = 37
- Select GRM EVALID 372303 (NC 2023 mortality evaluation)
- Calculate mortality rate using `mortality()` function
- Format results with statistical context

```text
"Calculate growing stock mortality for loblolly pine in Texas"
```

This will:
- Parse "Texas" → STATECD = 48
- Filter to loblolly pine (SPCD = 131)
- Use growing stock tree class
- Return merchantable timber mortality only

## Core Function Integration

The agent's `execute_mortality_command` calls the core `mortality()` function with these key features:

### Automatic EVALID Selection

```python
# Agent automatically handles this
evalid, explanation = get_recommended_evalid(
    query_interface, state_code, "mortality"
)
if evalid:
    fia.evalid = evalid  # Sets GRM evaluation
```

### Parameter Mapping

Agent CLI args → Core function parameters:

- `treeClass=growing_stock` → `tree_class="growing_stock"`
- `landType=forest` → `land_type="forest"`
- `bySpecies` → `by_species=True`
- `totals` → `totals=True` (always enabled for population estimates)

### Tree Class Support

- **All trees** (`tree_class="all"`): All live trees regardless of merchantability
- **Growing stock** (`tree_class="growing_stock"`): Merchantable timber only

## Expected Results

Based on documented EVALIDator queries:

### North Carolina (EVALID 372303)
- **Annual mortality rate**: ~0.080 trees/acre/year
- **Total annual mortality**: ~1,485,000 trees/year
- **Forest area**: ~18,560,000 acres
- **Plot count**: ~5,673 plots

### Colorado (EVALID 82003)
- **Merchantable volume mortality**: ~9.7 million cu.ft./year
- **Growing stock focus**: Only trees with VOLCFNET > 0

## Real Integration Test

The `test_north_carolina_mortality_real_validation.py` test validates:

1. **Database requirements** - GRM tables and mortality components exist
2. **EVALID selection** - Returns valid GRM evaluation for North Carolina
3. **Accuracy verification** - Results match expected values within tolerance
4. **Tree class comparison** - Growing stock < all trees mortality

### Test Execution

```bash
pytest tests/test_north_carolina_mortality_real_validation.py::TestNorthCarolinaMortalityRealData::test_north_carolina_mortality_rate_is_correct -v
```

## Technical Implementation

### Key Components

1. **`src/pyfia/estimation/mortality.py`** - Core mortality estimation function
2. **`src/pyfia/ai/agent.py`** - Agent tool integration
3. **`src/pyfia/ai/result_formatter.py`** - Enhanced output formatting
4. **`src/pyfia/filters/evalid.py`** - EVALID selection logic

### Mortality-Specific Features

- **GRM requirement**: Must use Growth/Removal/Mortality evaluations
- **Component filtering**: Uses `COMPONENT LIKE 'MORTALITY%'`
- **Annual rates**: Pre-calculated in TREE_GRM_COMPONENT tables
- **Tree basis adjustment**: Proper factors based on subplot design

### Error Handling

The agent handles common mortality estimation errors:

- **Missing GRM evaluation**: Returns clear error message
- **No mortality data**: Indicates insufficient remeasurement data
- **Invalid tree class**: Validates parameter values

## Query Examples

### Basic Mortality Rate
```text
"Annual tree mortality rate for North Carolina forest land"
```

### Species-Specific
```text
"Loblolly pine mortality in Texas by tree class"
```

### Growing Stock Focus
```text
"Merchantable timber mortality in Colorado"
```

### Comparison Queries
```text
"Compare all trees vs growing stock mortality in Georgia"
```

## Validation and Testing

All mortality queries should be validated against:

1. **EVALIDator queries** - Reference SQL from FIA documentation
2. **Expected ranges** - ~0.05-0.15 trees/acre/year typical for most forests
3. **Statistical quality** - CV < 30% for reliable estimates
4. **Plot counts** - Sufficient sample size (>1000 plots ideal)

The integration test provides the validation framework for ensuring accuracy and reliability of mortality estimates through the AI agent interface.