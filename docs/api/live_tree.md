# Live Tree Carbon Estimation

Estimate live tree carbon using the NSVB biomass framework with species-specific carbon fractions.

## Overview

The `live_tree()` function recomputes above-ground live tree biomass from scratch using the National Scale Volume and Biomass (NSVB) framework of Westfall et al. (2023, GTR-WO-104) and converts to carbon via species-specific S10a carbon fractions. This produces carbon estimates that align with the EPA NGHGI LULUCF live tree pool and match FIADB's pre-computed `CARBON_AG` column for NSVB-era inventories (September 2023 onward).

```python
import pyfia

db = pyfia.FIA("georgia.duckdb")
db.clip_by_state("GA")
db.clip_most_recent(eval_type="VOL")

# Above-ground live tree carbon
result = pyfia.live_tree(db, pool="ag")

# Total carbon (AG + BG bridge)
total = pyfia.live_tree(db, pool="total")
```

## Function Reference

::: pyfia.live_tree
    options:
      show_root_heading: true
      show_source: true

## Carbon Pools

| Pool | Description | Method |
|------|-------------|--------|
| `"ag"` | Above-ground (default) | NSVB pipeline: stem wood + bark + branches, harmonized to total AGB, then multiplied by species-specific S10a carbon fractions |
| `"bg"` | Below-ground (coarse roots) | Bridge to FIADB `TREE.CARBON_BG` (Phase 1 shortcut; native NSVB root model planned) |
| `"total"` | AG + BG | NSVB above-ground + FIADB below-ground bridge |

## How It Differs from `biomass()`

| | `live_tree()` | `biomass()` |
|---|---|---|
| **Biomass source** | Recomputed from scratch via NSVB equations | Reads FIADB pre-computed `DRYBIO_*` columns |
| **Carbon fraction** | Species-specific S10a (0.40-0.55) | Flat 0.47 multiplier |
| **Coefficient lookup** | 3-level precedence (DIVISION, species, Jenkins) | N/A (pre-computed) |
| **Cull adjustment** | NSVB cull formula with DECAYCD=3 density prop | Built into FIADB values |
| **Transparency** | Full recompute, auditable | Black-box FIADB values |

For NSVB-era inventories (2024+), both should agree closely. `live_tree()` is the preferred path for carbon accounting work that needs methodological transparency.

## Technical Notes

The NSVB pipeline predicts five biomass components per tree:

1. Stem inside-bark wood volume (S1a) x wood density x 62.4 = gross wood weight
2. Stem bark biomass (S6a)
3. Branch biomass (S7a)
4. Total above-ground biomass (S8a) - directly predicted

The component sum is harmonized proportionally to the directly-predicted total AGB. Cull-reduced wood uses the Harmon et al. (2011) DECAYCD=3 density proportion (0.54 hardwood, 0.92 softwood). Carbon = harmonized AGB x species-specific S10a fraction.

The optional `PLOTGEOM.ECOSUBCD` join activates Level 2 of the NSVB coefficient precedence (SPCD + Bailey DIVISION), closing a ~3% growing-stock biomass bias present in the species-level-only fallback. When `PLOTGEOM` is missing from older databases, the estimator falls back gracefully with a one-shot log warning.

## Examples

### Above-Ground Carbon Per Acre

```python
result = pyfia.live_tree(db, pool="ag")
print(f"Carbon: {result['CARBON_ACRE'][0]:.2f} tons/acre")
```

### Carbon by Species

```python
result = pyfia.live_tree(db, pool="ag", by_species=True)
result = pyfia.join_species_names(result, db)
print(result.sort("CARBON_ACRE", descending=True).head(10))
```

### Carbon by Ownership Group

```python
result = pyfia.live_tree(
    db,
    pool="total",
    grp_by="OWNGRPCD",
    totals=True,
    variance=True,
)
# OWNGRPCD: 10=National Forest, 20=Other Federal,
#           30=State/Local, 40=Private
print(result)
```

### Large Tree Carbon by Forest Type

```python
result = pyfia.live_tree(
    db,
    pool="ag",
    grp_by="FORTYPCD",
    tree_domain="DIA >= 20.0",
)
result = pyfia.join_forest_type_names(result, db)
print(result)
```

### Carbon on Timberland with Standard Errors

```python
result = pyfia.live_tree(
    db,
    pool="ag",
    land_type="timber",
    variance=True,
)
print(f"Carbon: {result['CARBON_ACRE'][0]:.2f} +/- "
      f"{result['CARBON_ACRE_SE'][0]:.2f} tons/acre")
```
