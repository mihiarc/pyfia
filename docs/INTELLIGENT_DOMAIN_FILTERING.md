# Intelligent Domain Filtering in pyFIA

The enhanced domain filtering system in pyFIA provides intelligent defaults and transparent assumption tracking to improve user experience and AI agent communication.

## Overview

The `domain.py` module now serves three key purposes:

1. **Set reasonable defaults** when users don't specify filters
2. **Track filtering assumptions** for transparent communication
3. **Provide context-aware suggestions** for different analysis types

## Key Features

### 1. Intelligent Defaults

The system can automatically select appropriate defaults based on:
- Natural language context from user queries
- Analysis type (volume, biomass, mortality, area, etc.)
- Domain expertise encoded in the system

```python
from pyfia.filters.domain import get_intelligent_defaults

# Context-aware defaults
defaults = get_intelligent_defaults(
    query_context="How many live trees are in Minnesota?",
    analysis_type="tree_count"
)
# Returns: {"tree_type": "live", "land_type": "forest"}

# Analysis-specific defaults
defaults = get_intelligent_defaults(
    query_context="Volume analysis",
    analysis_type="volume"
)
# Returns: {"tree_type": "gs", "land_type": "forest"}
```

### 2. Assumption Tracking

All filtering operations can track what assumptions are made:

```python
from pyfia.filters.domain import apply_standard_filters

# Apply filters with assumption tracking
filtered_trees, filtered_conds, assumptions = apply_standard_filters(
    tree_df, cond_df,
    tree_type="live",
    land_type="timber",
    track_assumptions=True
)

# Get human-readable explanation
explanation = assumptions.to_explanation()
print(explanation)
```

Output example:
```
• Including only live trees
• Including only timberland (productive, unreserved forest)

Key assumptions:
• Live trees defined as STATUSCD == 1
• Timberland defined as:
  - Forest land (COND_STATUS_CD == 1)
  - Productive sites (SITECLCD in productive classes)
  - Not reserved (RESERVCD == 0)
```

### 3. Auto Mode

Use `"auto"` for tree_type or land_type to enable intelligent selection:

```python
# Let the system choose appropriate filters
filtered_trees, filtered_conds, assumptions = apply_standard_filters(
    tree_df, cond_df,
    tree_type="auto",          # Will be chosen based on context
    land_type="auto",          # Will be chosen based on context
    query_context="Mortality from fire damage",
    track_assumptions=True
)
```

The system will:
- Detect "mortality" → set tree_type to "dead"
- Default land_type to "forest"
- Track that these defaults were applied

### 4. Domain Suggestions

Get suggestions for common filters by analysis type:

```python
from pyfia.filters.domain import suggest_common_domains

suggestions = suggest_common_domains("volume")
print(suggestions)
```

Output:
```python
{
    "tree_types": ["gs", "live_gs"],
    "land_types": ["timber", "forest"],
    "tree_domains": ["DIA >= 5.0", "DIA >= 9.0", "HT > 4.5"],
    "area_domains": []
}
```

## AI Agent Integration

The AI agent now uses these features for transparent communication:

### New Tools

1. **`explain_domain_filters`** - Explains filtering assumptions
2. **`suggest_domain_options`** - Suggests filters for analysis types

### Enhanced Workflow

When users ask questions, the agent:

1. Determines appropriate defaults based on context
2. Applies filters using the domain system
3. **Always explains assumptions** using `explain_domain_filters`
4. Provides results with full transparency

Example agent response:
```
Based on your query about "live trees in Minnesota", I'm using these filters:

• Including only live trees
• Including all forest land

Key assumptions:
• Live trees defined as STATUSCD == 1
• Forest land defined as COND_STATUS_CD == 1

Results: 2.1 billion live trees on 16.7 million acres of forest land.

If you'd like to adjust these filters (e.g., include only timberland, or add
diameter restrictions), please let me know!
```

## Implementation Details

### FilterAssumptions Class

Tracks what assumptions were made during filtering:

```python
@dataclass
class FilterAssumptions:
    tree_type: str
    land_type: str
    tree_domain: Optional[str]
    area_domain: Optional[str]
    assumptions_made: List[str]      # Technical assumptions
    defaults_applied: List[str]      # What defaults were used
```

### Intelligence Rules

The system uses these rules for intelligent defaults:

#### Tree Type Intelligence
- "live", "living", "alive" → `tree_type="live"`
- "dead", "mortality", "died" → `tree_type="dead"`
- "growing stock", "merchantable", "commercial" → `tree_type="gs"`

#### Land Type Intelligence
- "timber", "commercial", "productive" → `land_type="timber"`
- "all land", "any land" → `land_type="all"`

#### Analysis Type Rules
- **Volume analysis**: Default to growing stock trees (`"gs"`)
- **Mortality analysis**: Default to dead trees (`"dead"`)
- **Biomass analysis**: Default to live trees (`"live"`)

### Transparent Filter Definitions

All filters include clear explanations:

- **Live trees**: "STATUSCD == 1"
- **Dead trees**: "STATUSCD == 2"
- **Growing stock**: "STATUSCD == 1, TREECLCD == 2, AGENTCD < 30"
- **Forest land**: "COND_STATUS_CD == 1"
- **Timberland**: "Forest + productive + unreserved"

## Best Practices

### For API Users

1. **Use assumption tracking** in production code:
```python
_, _, assumptions = apply_standard_filters(..., track_assumptions=True)
if assumptions:
    logger.info(f"Filter assumptions: {assumptions.to_explanation()}")
```

2. **Leverage auto mode** for user-facing applications:
```python
apply_tree_filters(df, tree_type="auto", query_context=user_query)
```

3. **Provide domain suggestions** to help users:
```python
suggestions = suggest_common_domains(analysis_type)
# Show suggestions to user
```

### For AI Agent Development

1. **Always explain assumptions** after performing analysis
2. **Use context** from user queries to set intelligent defaults
3. **Suggest alternatives** when appropriate
4. **Make it easy for users to adjust** filters

## Example Use Cases

### 1. Beginner User Query
```
User: "How many trees are in Texas?"

System:
- Detects general question → tree_type="all", land_type="forest"
- Explains: "Including all tree types on forest land"
- Suggests: "Would you like to filter by live trees only, or include specific diameter classes?"
```

### 2. Expert User Query
```
User: "Board foot volume of loblolly pine on private timberland"

System:
- Detects "board foot" → tree_type="gs", tree_domain="DIA >= 9.0"
- Detects "timberland" → land_type="timber"
- Detects "private" → area_domain="OWNGRPCD == 40"
- Explains all assumptions clearly
```

### 3. AI Research Assistant
```
User: "I'm studying fire mortality patterns"

Agent:
- Suggests mortality-specific filters
- Explains different mortality agent codes
- Provides domain suggestions for fire-specific analysis
- Tracks all assumptions for reproducible research
```

## Migration Guide

### From Legacy Code

Old approach:
```python
# Manual filter application
tree_df = tree_df.filter(pl.col("STATUSCD") == 1)  # Magic number
cond_df = cond_df.filter(pl.col("COND_STATUS_CD") == 1)  # Not documented
```

New approach:
```python
# Transparent, documented filtering
filtered_trees, filtered_conds, assumptions = apply_standard_filters(
    tree_df, cond_df,
    tree_type="live",     # Clear intent
    land_type="forest",   # Clear intent
    track_assumptions=True
)

# Document what was done
print(assumptions.to_explanation())
```

### Updating Existing Functions

Add assumption tracking to existing estimation functions:

```python
def my_estimation_function(..., track_assumptions=False):
    # Apply domain filters with tracking
    filtered_data, assumptions = apply_tree_filters(
        data, tree_type, tree_domain,
        track_assumptions=track_assumptions
    )

    # ... estimation logic ...

    if track_assumptions and assumptions:
        result.attrs['assumptions'] = assumptions.to_explanation()

    return result
```

## Future Enhancements

Planned improvements include:

1. **Machine learning** for better default prediction
2. **User preference learning** - remember user's typical filter choices
3. **Advanced context parsing** - understand more complex queries
4. **Filter validation** - warn about unusual filter combinations
5. **Performance hints** - suggest more efficient filter approaches

This system represents a significant step toward making FIA data analysis more accessible while maintaining scientific rigor and transparency.