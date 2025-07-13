# Intelligent Domain Filtering for AI Agent

The PyFIA AI Agent leverages intelligent domain filtering to provide accurate, context-aware results while maintaining transparency about filtering assumptions.

## Overview

When you ask the AI agent a question about forest data, it automatically:
1. **Detects context** from your natural language query
2. **Applies appropriate filters** based on the question type
3. **Explains all assumptions** made during filtering
4. **Suggests alternatives** when relevant

## How It Works

### Context Detection

The AI agent analyzes your query to understand intent:

```bash
# Query: "How many live trees are in California?"
# Detected: tree_type="live", location="California"

# Query: "Show mortality from fire damage"
# Detected: tree_type="dead", cause="fire"

# Query: "Calculate merchantable volume"
# Detected: tree_type="gs" (growing stock)
```

### Automatic Filtering

Based on context, appropriate filters are applied:

| Query Context | Tree Type | Land Type | Additional Filters |
|--------------|-----------|-----------|-------------------|
| "live trees" | live | forest | STATUSCD == 1 |
| "mortality" | dead | forest | STATUSCD == 2 |
| "timber volume" | gs | timber | Growing stock criteria |
| "all trees" | all | forest | No status filter |
| "biomass" | live | forest | Live trees only |

### Transparent Communication

The agent always explains what filters were applied:

```
Based on your query about "live oak trees", I'm applying these filters:

✓ Including only live trees (STATUSCD == 1)
✓ Including all forest land (COND_STATUS_CD == 1)
✓ Filtering for oak species (Quercus genus)

If you'd like different filters (e.g., only timberland or specific size classes), 
please let me know!
```

## AI Agent Features

### Smart Defaults

The agent uses intelligent defaults based on analysis type:

```python
# Volume analysis → Growing stock trees
"What's the volume?" → tree_type="gs", land_type="timber"

# Area analysis → All trees  
"Forest area?" → tree_type="all", land_type="forest"

# Mortality analysis → Dead trees
"Annual mortality?" → tree_type="dead", land_type="forest"
```

### Filter Suggestions

When appropriate, the agent suggests relevant filters:

```
For your volume analysis, you might also consider:
• Diameter limits (e.g., DBH >= 5.0 inches)
• Specific forest types
• Ownership categories (public vs private)
• Regional boundaries
```

### Assumption Tracking

All filtering assumptions are tracked and can be reviewed:

```bash
fia-ai> explain last query filters

Filters applied in previous query:
• Tree type: Growing stock (commercial species, sound condition)
• Land type: Timberland (productive, unreserved forest)
• Geographic: California (STATECD == 6)
• Time period: EVALID 62021 (2021 evaluation)

Technical details:
• STATUSCD == 1 (live trees)
• TREECLCD == 2 (growing stock classification)
• AGENTCD < 30 (no significant damage)
• SITECLCD in [1,2,3,4,5] (productive sites)
• RESERVCD == 0 (not reserved)
```

## Common Patterns

### Basic Queries

```bash
# Simple count - uses intelligent defaults
fia-ai> How many trees are there?
→ Applies: tree_type="all", land_type="forest"

# Species-specific - maintains context
fia-ai> How many pine trees?
→ Applies: Previous filters + species filter

# Clear intent - specific filters
fia-ai> Count dead oak trees
→ Applies: tree_type="dead", species="oak"
```

### Advanced Filtering

```bash
# Multiple criteria
fia-ai> Large diameter pine on public timberland
→ Applies: tree_type="live", land_type="timber", 
          DIA >= 20.0, ownership="public", species="pine"

# Exclusions
fia-ai> All trees except damaged ones
→ Applies: tree_type="live", AGENTCD < 30

# Complex domains
fia-ai> Merchantable sawtimber volume
→ Applies: tree_type="gs", DIA >= 9.0, 
          height/quality thresholds
```

## Customizing Filters

### Override Defaults

You can explicitly specify filters:

```bash
# Override default land type
fia-ai> Count all trees including non-forest land
→ Uses: land_type="all" instead of default "forest"

# Specify exact criteria
fia-ai> Trees with STATUSCD=1 and DIA between 10 and 20
→ Uses: Your exact specifications
```

### Clear Filters

Reset to no filtering:

```bash
fia-ai> Show raw tree count with no filters
→ Returns unfiltered data

fia-ai> Clear all domain filters
→ Resets to defaults
```

## Filter Reference

### Tree Types

| Type | Description | Filter Applied |
|------|-------------|----------------|
| `live` | Living trees | STATUSCD == 1 |
| `dead` | Dead trees | STATUSCD == 2 |
| `gs` | Growing stock | Live + commercial + sound |
| `all` | All trees | No status filter |

### Land Types

| Type | Description | Filter Applied |
|------|-------------|----------------|
| `forest` | Forest land | COND_STATUS_CD == 1 |
| `timber` | Timberland | Forest + productive + unreserved |
| `all` | All conditions | No condition filter |

### Common Domains

| Domain | Description | Example |
|--------|-------------|---------|
| Size | Diameter limits | DIA >= 5.0 |
| Height | Height limits | HT > 4.5 |
| Species | Species codes | SPCD IN (202, 316) |
| Damage | Agent codes | AGENTCD < 30 |
| Owner | Ownership | OWNGRPCD == 40 |

## Best Practices

### Be Specific When Needed

```bash
# Vague - agent will use defaults
fia-ai> What's the volume?

# Specific - agent uses your criteria  
fia-ai> What's the net merchantable volume of live pine sawtimber on private land?
```

### Use Natural Language

```bash
# Technical - works but not necessary
fia-ai> Select trees where STATUSCD=1 and DIA>=10

# Natural - agent understands
fia-ai> Show me live trees at least 10 inches in diameter
```

### Verify Assumptions

```bash
# After any query, check filters
fia-ai> What filters did you use?

# Get detailed explanation
fia-ai> Explain the growing stock definition
```

## Examples

### Research Scenario

```bash
fia-ai> I'm studying carbon storage in old growth forests

Agent: I'll help you analyze carbon storage in old growth forests. 
I'll apply these filters:
• Large, old trees (DBH >= 20 inches)
• Live trees only (for active carbon storage)
• Forest land only

Would you like to:
1. Set a specific diameter threshold?
2. Include dead wood carbon?
3. Focus on specific forest types?
```

### Management Query

```bash
fia-ai> Show harvestable timber volume on our private lands

Agent: For harvestable timber analysis, I'm using:
• Growing stock trees (merchantable species)
• Minimum DBH of 9.0 inches (sawtimber)
• Private ownership (OWNGRPCD == 40)
• Timberland only (productive, unreserved)

Results show 1,234 million cubic feet of merchantable volume.
```

### Ecological Study

```bash
fia-ai> Analyze tree mortality from insects

Agent: Analyzing insect-caused mortality with these filters:
• Dead trees (STATUSCD == 2)
• Insect damage agents (AGENTCD 10000-19999)
• Forest land only
• Latest evaluation period

Found 45,000 trees killed by insects, primarily:
- Mountain pine beetle (12,000)
- Emerald ash borer (8,500)
- Southern pine beetle (6,200)
```

## Technical Details

For developers, the domain filtering system:

1. **Parses natural language** to extract filter intent
2. **Maps to FIA domains** using knowledge base
3. **Validates combinations** for consistency
4. **Tracks all decisions** for transparency
5. **Generates explanations** in user-friendly terms

See the [Developer Guide](DEVELOPER_GUIDE.md) for implementation details.