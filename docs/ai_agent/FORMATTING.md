# Enhanced Result Formatting with Rich

## Overview

The enhanced result formatting system provides comprehensive, user-friendly presentation of FIA analysis results with proper statistical context, confidence intervals, and reliability assessments. The system now uses the **Rich Python library** to create beautiful, professional terminal output with colors, tables, panels, and advanced styling.

## Features

### 🎯 **Statistical Rigor**
- **Confidence Intervals**: Automatic calculation of 95% (or configurable) confidence intervals
- **Reliability Assessment**: Color-coded reliability ratings based on standard error percentages
- **Proper Statistical Context**: Clear explanation of what estimates represent

### 📊 **Visual Enhancement with Rich**
- **Rich Panels**: Beautiful bordered panels for organizing information sections
- **Professional Tables**: Styled tables with proper alignment, colors, and formatting
- **Emoji Indicators**: Optional visual cues for different types of information
- **Color Coding**: Syntax highlighting and color-coded reliability indicators
- **Responsive Layout**: Automatically adapts to terminal width
- **Number Formatting**: Proper comma-separated formatting for large numbers

### 🔬 **Scientific Context**
- **Methodology Notes**: Explanation of FIA EVALIDator methodology
- **Sample Information**: Plot counts and sampling context
- **Interpretation Guidance**: Help users understand what results mean

## Usage Examples

### Basic Tree Count Query

**Input:** "How many live oak trees are in Texas?"

**Enhanced Output:**
```
🌳 **FIA Tree Count Analysis Results**
==================================================

📊 **Query Summary:**
   • Tree Status: Live trees
   • Land Type: Forest land
   • Tree Filter: SPCD == 802
   • Area Filter: STATECD == 48
   • Evaluation ID: 482201
   • Analysis Date: 2025-01-27 14:30

📈 **Population Estimate:**

🔢 **Total Trees:** 45,234,567

📊 **Statistical Precision:**
   • Standard Error: ±2,261,728 trees (5.0%)
   • 95% Confidence Interval: 40,801,584 - 49,667,550
   • Reliability: 🟢 Excellent (Very reliable estimate)

📍 **Sample Information:**
   • Field Plots Used: 1,247
   • Average Trees per Plot: 36.3

🔬 **Methodology Notes:**
   • Population estimates use FIA EVALIDator methodology
   • Expansion factors account for plot sampling design
   • Standard errors reflect sampling uncertainty
   • 95% confidence intervals assume normal distribution

💡 **Interpretation Guide:**
   • Population estimates represent total trees across the area
   • Standard errors indicate precision of estimates
   • Lower SE% = more precise estimate
   • Confidence intervals show plausible range of true values
   • Reliability ratings help assess estimate quality
```

### Grouped Results (By Species)

**Input:** "Show me tree counts by species in North Carolina"

**Enhanced Output:**
```
🌳 **FIA Tree Count Analysis Results**
==================================================

📊 **Query Summary:**
   • Tree Status: Live trees
   • Land Type: Forest land
   • Evaluation ID: 372301
   • Analysis Date: 2025-01-27 14:30

📈 **Detailed Results:**

**1. loblolly pine** (*Pinus taeda*)
   Species Code: 131
   🔢 **Population Estimate:** 2,112,569,195 trees
   📊 **Standard Error:** ±105,628,460 trees (5.0%)
   🎯 **95% Confidence Interval:** 1,905,538,813 - 2,319,599,577 trees
   🟢 **Reliability:** Excellent (≤5%)
   📍 **Sample Size:** 3,500 plots

**2. red maple** (*Acer rubrum*)
   Species Code: 316
   🔢 **Population Estimate:** 1,933,632,940 trees
   📊 **Standard Error:** ±96,681,647 trees (5.0%)
   🎯 **95% Confidence Interval:** 1,744,137,692 - 2,123,128,188 trees
   🟢 **Reliability:** Excellent (≤5%)
   📍 **Sample Size:** 3,500 plots

📋 **Summary Statistics:**
   • Total Entries: 2
   • Combined Population: 4,046,202,135 trees
   • Plot Sample Size: 3,500 plots
```

## Reliability Assessment

The system automatically assesses estimate reliability based on standard error percentages:

| Rating | SE Range | Indicator | Interpretation |
|--------|----------|-----------|----------------|
| 🟢 Excellent | ≤5% | Green | Very reliable estimate |
| 🟡 Good | 5-10% | Yellow | Reliable estimate |
| 🟠 Fair | 10-20% | Orange | Moderately reliable estimate |
| 🔴 Poor | >20% | Red | Use with caution |

## Configuration Options

### Formatter Styles with Rich

```python
from pyfia.ai.result_formatter import create_result_formatter

# Enhanced style with Rich (default) - beautiful panels, tables, and colors
formatter = create_result_formatter("enhanced", use_rich=True)

# Simple style with Rich - clean panels without emojis
formatter = create_result_formatter("simple", use_rich=True)

# Scientific style with Rich - 99% confidence intervals, professional styling
formatter = create_result_formatter("scientific", use_rich=True)

# Fallback to plain text (when Rich unavailable or disabled)
formatter = create_result_formatter("enhanced", use_rich=False)
```

### Rich-Specific Features

```python
# Control terminal width for Rich output
formatter = create_result_formatter("enhanced", console_width=120)

# Rich formatting automatically provides:
# - Colored panels with borders
# - Professional tables with alignment
# - Responsive layout
# - Syntax highlighting
# - Visual hierarchy
```

### Custom Configuration

```python
from pyfia.ai.result_formatter import FIAResultFormatter

# Custom confidence level with Rich
formatter = FIAResultFormatter(
    confidence_level=0.90,  # 90% CI
    use_rich=True,
    console_width=100
)

# Disable emojis but keep Rich styling
formatter = FIAResultFormatter(
    include_emojis=False,
    use_rich=True
)

# Scientific configuration with Rich
formatter = FIAResultFormatter(
    include_emojis=False,
    confidence_level=0.99,
    use_rich=True
)
```

## Rich Output Examples

### Single Result with Rich Panels

When Rich is enabled, single results are displayed in beautiful bordered panels:

```
╭─ 🌳 Query Summary ─────────────────────────────────────────────╮
│                                                                │
│  Tree Status: Live trees                                       │
│  Land Type: Forest land                                        │
│  Area Filter: STATECD == 37                                    │
│  Evaluation ID: 372301                                         │
│  Analysis Date: 2025-06-25 18:43                               │
│                                                                │
╰────────────────────────────────────────────────────────────────╯

╭─ 📊 Population Analysis Results ───────────────────────────────╮
│                                                                │
│  Population Estimate: 2,112,569,195 trees                     │
│                                                                │
│  Statistical Precision:                                        │
│  • Standard Error: ±105,628,460 trees (5.0%)                  │
│  • 95% Confidence Interval: 1,905,537,413 - 2,319,600,977     │
│  • Reliability: 🟢 Excellent (Very reliable estimate)          │
│                                                                │
│  Sample Information:                                           │
│  • Field Plots Used: 3,500                                     │
│  • Average Trees per Plot: 603,591.2                           │
│                                                                │
╰────────────────────────────────────────────────────────────────╯
```

### Grouped Results with Rich Tables

Multiple species are displayed in professional tables:

```
                      🌳 Tree Population Analysis
┏━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━┓
┃ Species               ┃    Population ┃ Standard Error ┃ Reliability  ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━┩
│ loblolly pine         │ 2,112,569,195 │   ±105,628,460 │ 🟢 Excellent │
│ (Pinus taeda)         │               │         (5.0%) │              │
│ red maple             │ 1,933,632,940 │    ±96,681,647 │ 🟢 Excellent │
│ (Acer rubrum)         │               │         (5.0%) │              │
└───────────────────────┴───────────────┴────────────────┴──────────────┘
```

## Integration with AI Agent

The enhanced formatting is automatically used by the AI agent when available:

```python
from pyfia.ai.agent import FIAAgent

agent = FIAAgent("path/to/database.duckdb")
response = agent.query("How many pine trees are in Oregon?")
# Automatically uses enhanced formatting
```

### Fallback Behavior

If the enhanced formatter is not available, the system gracefully falls back to simple formatting:

```
Tree Count Results:

Species: loblolly pine (Pinus taeda)
Total Population: 2,112,569,195 trees
Standard Error: 105,628,460
Standard Error %: 5.0%

(Statistically valid population estimate using FIA methodology)
```

## Technical Implementation

### Key Components

1. **FIAResultFormatter**: Main formatter class with configurable options
2. **Reliability Assessment**: Automatic quality evaluation based on SE%
3. **Confidence Intervals**: Statistical calculation with proper bounds
4. **Visual Enhancement**: Emoji mapping and structured layout
5. **Fallback System**: Graceful degradation when formatter unavailable

### Statistical Calculations

- **Confidence Intervals**: Uses appropriate z-scores (1.96 for 95%, 2.576 for 99%)
- **Lower Bound Constraint**: Ensures confidence intervals don't go below zero
- **Reliability Thresholds**: Based on FIA accuracy standards

### Error Handling

- **Import Safety**: Graceful handling when dependencies unavailable
- **Data Validation**: Checks for required columns and valid values
- **NaN Handling**: Proper treatment of missing statistical values

## Best Practices

### For Users

1. **Pay attention to reliability ratings** - Green is best, red requires caution
2. **Use confidence intervals** - They show the uncertainty in estimates
3. **Consider sample size** - More plots generally mean better estimates
4. **Understand methodology** - Read the methodology notes for context

### For Developers

1. **Always provide fallback** - Simple formatting should always work
2. **Validate input data** - Check for required columns before formatting
3. **Handle edge cases** - Zero estimates, missing SE values, etc.
4. **Test different scenarios** - Single results, grouped results, edge cases

## Future Enhancements

### Planned Features

- **Comparison Formatting**: Side-by-side comparisons of different estimates
- **Trend Analysis**: Formatting for temporal comparisons
- **Export Options**: PDF, CSV, and other format exports
- **Interactive Elements**: Expandable sections and drill-down capabilities

### Visualization Integration

- **Chart Generation**: Automatic creation of bar charts and plots
- **Map Integration**: Geographic visualization of spatial results
- **Statistical Plots**: Confidence interval visualizations

## Examples for Different Estimate Types

### Volume Estimates
```
📊 Volume Analysis Results
• Net Volume: 2,659.03 cu ft/acre (±39.89, 1.5% SE)
• Gross Volume: 2,692.80 cu ft/acre (±40.39, 1.5% SE)
• 🟢 Excellent reliability for both estimates
```

### Biomass Estimates
```
🌿 Biomass Analysis Results
• Aboveground: 69.7 tons/acre (±1.05, 1.5% SE)
• Carbon Content: 32.8 tons/acre (±0.49, 1.5% SE)
• 🟢 Excellent reliability - suitable for carbon accounting
```

### Area Estimates
```
🗺️ Forest Area Analysis
• Total Forest: 18,592,940 acres (±117,647, 0.63% SE)
• Timberland: 17,854,302 acres (±125,181, 0.70% SE)
• 🟢 Excellent reliability - meets FIA accuracy standards
```

This enhanced formatting system transforms raw statistical output into user-friendly, scientifically rigorous presentations that help users understand both the results and their reliability.