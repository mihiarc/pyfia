# Remeasurement Panels

Create t1/t2 linked panel datasets from FIA remeasurement data for harvest analysis, growth tracking, and change detection.

## Overview

The `panel()` function creates linked datasets where each row represents a measurement pair:

- **t1 (time 1)**: Previous measurement
- **t2 (time 2)**: Current measurement

This panel data is essential for:

- Harvest probability modeling
- Forest change detection
- Growth and mortality analysis
- Land use transition studies

```python
import pyfia

db = pyfia.FIA("data/nc.duckdb")
db.clip_by_state("NC")

# Condition-level panel for harvest analysis
cond_panel = pyfia.panel(db, level="condition", land_type="timber")
print(f"Harvest rate: {cond_panel['HARVEST'].mean():.1%}")

# Tree-level panel for mortality/cut analysis
tree_panel = pyfia.panel(db, level="tree")
print(tree_panel.group_by("TREE_FATE").len())
```

## Function Reference

::: pyfia.panel
    options:
      show_root_heading: true
      show_source: true

## Panel Levels

### Condition-Level (`level="condition"`)

Each row represents a forest condition measured at two time points. Best for:

- Area-based harvest probability models
- Land use change analysis
- Stand-level attribute tracking

**Key columns:**

| Column | Description |
|--------|-------------|
| `PLT_CN` | Current plot control number |
| `PREV_PLT_CN` | Previous plot control number |
| `CONDID` | Condition identifier |
| `INVYR` | Current inventory year |
| `REMPER` | Remeasurement period (years) |
| `HARVEST` | Harvest indicator (1=detected, 0=no) |
| `t1_*` / `t2_*` | Attributes at time 1 and time 2 |

### Tree-Level (`level="tree"`)

Each row represents an individual tree measured at two time points. Best for:

- Individual tree fate analysis
- Species-specific harvest patterns
- Mortality vs. harvest separation

**Key columns:**

| Column | Description |
|--------|-------------|
| `TRE_CN` | Current tree control number |
| `PREV_TRE_CN` | Previous tree control number |
| `TREE_FATE` | Tree fate classification |
| `t1_*` / `t2_*` | Tree attributes at time 1 and time 2 |

**Tree Fate Values:**

| Fate | Description | GRM Component |
|------|-------------|---------------|
| `survivor` | Live at t1 and t2 | SURVIVOR |
| `mortality` | Live at t1, dead at t2 (natural causes) | MORTALITY1, MORTALITY2 |
| `cut` | Live at t1, removed/harvested at t2 | CUT1, CUT2 |
| `diversion` | Removed due to land use change | DIVERSION1, DIVERSION2 |
| `ingrowth` | New tree crossing 5" DBH threshold | INGROWTH |
| `other` | Unrecognized component type | - |

## Harvest Detection

### Condition-Level Detection

Harvest is detected using TRTCD (treatment code) fields:

- **TRTCD = 10**: Cutting (harvest)
- **TRTCD = 20**: Site preparation (implies prior harvest)

```python
# Get harvested conditions
harvested = pyfia.panel(db, level="condition", harvest_only=True)
print(f"Harvested conditions: {len(harvested)}")
```

### Tree-Level Detection (GRM-Based)

Tree-level panels use FIA's GRM (Growth-Removal-Mortality) tables for authoritative fate classification. The `TREE_GRM_COMPONENT` table provides pre-computed classifications that align with FIA's official estimation methodology:

```python
# Tree fate comes directly from GRM COMPONENT classification
tree_panel = pyfia.panel(db, level="tree")
cut_trees = tree_panel.filter(pl.col("TREE_FATE") == "cut")

# Get all removals (cut + diversion)
removals = pyfia.panel(db, level="tree", harvest_only=True)
```

This ensures consistency with the `removals()` function and EVALIDator results.

## Examples

### Basic Harvest Analysis

```python
import pyfia
import polars as pl

db = pyfia.FIA("data/nc.duckdb")
db.clip_by_state("NC")

# Condition-level harvest rates
panel = pyfia.panel(db, level="condition", land_type="forest")

# Overall harvest rate
harvest_rate = panel["HARVEST"].mean()
remper = panel["REMPER"].mean()
annual_rate = 1 - (1 - harvest_rate) ** (1 / remper)

print(f"Period harvest rate: {harvest_rate:.1%}")
print(f"Annualized rate: {annual_rate:.2%}/year")
```

### Harvest by Ownership

```python
panel = pyfia.panel(db, level="condition")

harvest_by_owner = (
    panel
    .group_by("t2_OWNGRPCD")
    .agg([
        pl.len().alias("n_conditions"),
        pl.col("HARVEST").mean().alias("harvest_rate")
    ])
    .sort("t2_OWNGRPCD")
)

# OWNGRPCD: 10=USFS, 20=Other Fed, 30=State/Local, 40=Private
print(harvest_by_owner)
```

### Tree-Level Cut Analysis

```python
# Get all trees with fate information
tree_panel = pyfia.panel(db, level="tree", tree_type="all")

# Tree fate distribution
fate_dist = tree_panel.group_by("TREE_FATE").len()
print(fate_dist)

# Cut trees only
cut_trees = pyfia.panel(db, level="tree", harvest_only=True)

# Top species cut
species_cut = (
    cut_trees
    .group_by("t1_SPCD")
    .agg([
        pl.len().alias("n_trees"),
        pl.col("t1_DIA").mean().alias("avg_dia")
    ])
    .sort("n_trees", descending=True)
    .head(10)
)
print(species_cut)
```

### Filtering Options

```python
# Timberland only (productive, unreserved forest)
timber_panel = pyfia.panel(db, level="condition", land_type="timber")

# Private land only
private_panel = pyfia.panel(
    db,
    level="condition",
    area_domain="OWNGRPCD == 40"
)

# Specific remeasurement period range
panel_5_10yr = pyfia.panel(
    db,
    level="condition",
    min_remper=5,
    max_remper=10
)

# Loblolly pine only (tree-level)
loblolly_panel = pyfia.panel(
    db,
    level="tree",
    tree_domain="SPCD == 131"
)
```

### Multi-Period Chain Analysis

```python
# Get panel with chain expansion (default)
panel = pyfia.panel(db, level="condition", expand_chains=True)

# Count plots by number of measurement periods
chain_lengths = (
    panel
    .group_by("PLT_CN")
    .len()
    .group_by("len")
    .len()
    .rename({"len": "periods", "len_right": "n_plots"})
    .sort("periods")
)
print("Plots by chain length:")
print(chain_lengths)
```

## Expansion to Per-Acre Estimates

Tree-level panels can be expanded to produce per-acre estimates comparable to `removals()`. This applies the same three-layer expansion methodology:

1. **TPA_UNADJ**: Base trees-per-acre from plot design
2. **ADJ_FACTOR**: Plot-type adjustment (subplot/microplot/macroplot)
3. **EXPNS**: Stratum expansion to total acres

### Basic Expansion

```python
# Get per-acre removals estimates (comparable to removals())
expanded = pyfia.panel(
    db,
    level="tree",
    harvest_only=True,
    expand=True,
    measure="tpa"  # Trees per acre
)
print(f"Total removals: {expanded['PANEL_TOTAL'][0]:,.0f} trees/year")
print(f"Per acre: {expanded['PANEL_ACRE'][0]:,.1f} trees/acre/year")
```

### Expansion by Species

```python
# Removals by species
by_species = pyfia.panel(
    db,
    level="tree",
    harvest_only=True,
    expand=True,
    measure="volume",
    grp_by=["SPCD"]
)
print(by_species.sort("PANEL_TOTAL", descending=True).head(10))
```

### Expansion by Tree Fate

```python
# Compare survivor, mortality, cut volumes
by_fate = pyfia.panel(
    db,
    level="tree",
    expand=True,
    measure="volume",
    by_fate=True  # Groups by TREE_FATE automatically
)
print(by_fate)
```

### Expansion Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `expand` | bool | False | Apply expansion factors for per-acre estimates |
| `measure` | str | "tpa" | Measure to expand: "tpa" or "volume" |
| `grp_by` | list | None | Grouping columns (e.g., ["SPCD"]) |
| `by_fate` | bool | False | Include TREE_FATE in grouping |

**Note:** `expand=True` requires `level="tree"` and returns aggregated estimates instead of tree-level data.

### Harvest Transition Analysis

```python
import polars as pl

panel = pyfia.panel(db, level="condition")

# Find plots with multiple periods
multi_period = panel.filter(
    pl.col("PLT_CN").is_in(
        panel.group_by("PLT_CN").len().filter(pl.col("len") > 1)["PLT_CN"]
    )
).sort(["PLT_CN", "INVYR"])

# Calculate harvest transitions
transitions = (
    multi_period
    .with_columns([
        pl.col("HARVEST").shift(1).over("PLT_CN").alias("PREV_HARVEST")
    ])
    .filter(pl.col("PREV_HARVEST").is_not_null())
    .group_by(["PREV_HARVEST", "HARVEST"])
    .len()
)
print("Harvest transitions:")
print(transitions)
```

## Technical Notes

### Inventory Year Filter (`min_invyr`)

FIA transitioned from periodic to annual inventory methodology around 1999-2000. By default, `min_invyr=2000` ensures only post-transition data is used, providing:

- Consistent methodology across the panel
- Annual rather than periodic measurements
- Better tree tracking via PREV_TRE_CN

Set `min_invyr=0` to include historical data if needed.

### Chain Expansion (`expand_chains`)

When `expand_chains=True` (default), plots with multiple remeasurements (t1→t2→t3) generate all consecutive pairs:

- (t1, t2) and (t2, t3)

This maximizes data utilization for transition modeling.

### Data Sources

- **PLOT table**: `PREV_PLT_CN`, `REMPER` for plot linking
- **COND table**: `TRTCD1-3` for harvest detection
- **TREE table**: `PREV_TRE_CN`, `STATUSCD` for tree tracking

## See Also

- [`area_change()`](area_change.md) - Estimate forest area change with variance
- [`mortality()`](mortality.md) - Annual tree mortality estimation
- [`removals()`](removals.md) - Timber removals estimation
- [`growth()`](growth.md) - Annual forest growth estimation
