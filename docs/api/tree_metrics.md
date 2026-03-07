# Tree Metrics

Compute TPA-weighted descriptive statistics at the condition or group level.

## Overview

The `tree_metrics()` function calculates sample-level tree metrics such as quadratic mean diameter (QMD), mean height, and species composition. Unlike population-level estimators (`volume()`, `tpa()`, etc.), these are **descriptive statistics** that do not use expansion factors or variance estimation.

This is useful for characterizing stand structure, linking plot-level attributes to external models, or computing derived metrics not available in the standard FIA tables.

```python
import pyfia

db = pyfia.FIA("georgia.duckdb")
db.clip_by_state("GA")

# QMD and mean height by forest type
result = pyfia.tree_metrics(db, metrics=["qmd", "mean_height"], grp_by="FORTYPCD")
```

## Function Reference

::: pyfia.tree_metrics
    options:
      show_root_heading: true
      show_source: true

## Available Metrics

| Metric | Output Column | Description |
|--------|---------------|-------------|
| `"qmd"` | `QMD` | Quadratic mean diameter (TPA-weighted) |
| `"mean_dia"` | `MEAN_DIA` | Arithmetic mean diameter (TPA-weighted) |
| `"mean_height"` | `MEAN_HT` | Mean tree height (TPA-weighted, nulls excluded) |
| `"softwood_prop"` | `SOFTWOOD_PROP` | Softwood proportion of bole biomass (SPCD < 300) |
| `"sawtimber_prop"` | `SAWTIMBER_PROP` | Proportion of TPA above sawtimber diameter threshold |
| `"max_dia"` | `MAX_DIA` | Maximum tree diameter in group |
| `"stocking"` | `STOCKING` | Rough stocking index |

All results also include `N_PLOTS` and `N_TREES` diagnostic counts.

## Examples

### QMD by Forest Type

```python
result = pyfia.tree_metrics(db, metrics=["qmd"], grp_by="FORTYPCD")
result = pyfia.join_forest_type_names(result, db)
print(result.sort("QMD", descending=True).head(10))
```

### Stand Structure Profile

Compute multiple metrics at once for a comprehensive stand description:

```python
result = pyfia.tree_metrics(
    db,
    metrics=["qmd", "mean_height", "softwood_prop", "sawtimber_prop", "stocking"],
    grp_by="FORTYPCD",
)
```

### Condition-Level Metrics for External Models

Get per-plot-condition metrics with additional COND attributes passed through:

```python
result = pyfia.tree_metrics(
    db,
    metrics=["qmd", "mean_height", "softwood_prop", "sawtimber_prop"],
    grp_by=["PLT_CN", "CONDID", "STDAGE", "FORTYPCD"],
    land_type="timber",
    tree_domain="DIA >= 1.0",
    include_cond_attrs=["SLOPE", "SICOND"],
)
```

Each row represents a single plot-condition, useful for linking to harvest probability models or growth simulators.

### Large Trees Only

```python
result = pyfia.tree_metrics(
    db,
    metrics=["qmd", "mean_dia", "max_dia"],
    tree_domain="DIA >= 12.0",
    grp_by="FORTYPCD",
)
```

### Sawtimber with Custom Threshold

The default sawtimber threshold is 9.0 inches. Override it for hardwood-specific analysis:

```python
result = pyfia.tree_metrics(
    db,
    metrics=["sawtimber_prop"],
    sawtimber_threshold=11.0,
    grp_by="FORTYPCD",
)
```

## Comparison with Other Estimators

| Feature | `tree_metrics()` | `tpa()`, `volume()`, etc. |
|---------|-------------------|---------------------------|
| Estimate type | Sample-level descriptive | Population-level statistical |
| Expansion factors | No | Yes |
| Variance / SE | No | Yes |
| Confidence intervals | No | Yes |
| Use case | Stand characterization, model inputs | Area/volume/biomass totals and per-acre rates |
