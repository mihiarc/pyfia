# Standing Dead Carbon Estimation

Estimate standing dead tree carbon using the NSVB biomass framework with decay-class reductions and dead-tree carbon fractions.

## Overview

The `standing_dead()` function recomputes above-ground standing dead tree biomass from scratch using the National Scale Volume and Biomass (NSVB) framework and applies the FIADB `REF_TREE_DECAY_PROP` density and structural-loss reductions by decay class. The reduced biomass is converted to carbon via S10b dead-tree carbon fractions (hardwood/softwood x DECAYCD). This produces carbon estimates that align with the EPA NGHGI LULUCF standing dead pool.

```python
import pyfia

db = pyfia.FIA("georgia.duckdb")
db.clip_by_state("GA")
db.clip_most_recent(eval_type="VOL")

# Above-ground standing dead carbon
result = pyfia.standing_dead(db, pool="ag")

# Standing dead carbon by decay class
by_decay = pyfia.standing_dead(db, pool="ag", grp_by="DECAYCD")
```

## Function Reference

::: pyfia.standing_dead
    options:
      show_root_heading: true
      show_source: true

## Carbon Pools

| Pool | Description | Method |
|------|-------------|--------|
| `"ag"` | Above-ground (default) | NSVB pipeline with REF_TREE_DECAY_PROP reductions + S10b dead carbon fractions |
| `"bg"` | Below-ground (coarse roots) | Bridge to FIADB `TREE.CARBON_BG` (same as live tree) |
| `"total"` | AG + BG | NSVB dead above-ground + FIADB below-ground bridge |

## Decay-Class Reductions

Standing dead trees lose mass through decomposition. The FIADB `REF_TREE_DECAY_PROP` table provides three multiplicative reduction factors applied to each gross NSVB component by hardwood/softwood classification and decay class (1-5):

| Factor | Applied to | Description |
|--------|-----------|-------------|
| `DENSITY_PROP` | Stem wood | Fraction of wood biomass remaining after density loss |
| `BARK_LOSS_PROP` | Stem bark | Fraction of bark biomass remaining |
| `BRANCH_LOSS_PROP` | Branches | Fraction of branch biomass remaining |

### Reduction Factors by Decay Class

| Decay Class | Description | HW Density | HW Bark | HW Branch | SW Density | SW Bark | SW Branch |
|:-----------:|-------------|:----------:|:-------:|:---------:|:----------:|:-------:|:---------:|
| 1 | All limbs present, intact | 0.99 | 1.00 | 1.00 | 0.97 | 1.00 | 1.00 |
| 2 | Few limbs, no fine branches | 0.80 | 0.80 | 0.50 | 1.00 | 0.80 | 0.50 |
| 3 | Limb stubs only, top broken | 0.54 | 0.50 | 0.10 | 0.92 | 0.50 | 0.10 |
| 4 | Few stubs, top broken | 0.43 | 0.20 | 0.00 | 0.55 | 0.20 | 0.00 |
| 5 | No limbs, <20% bark | 0.43 | 0.00 | 0.00 | 0.55 | 0.00 | 0.00 |

Per FIADB User Guide v9.1 Appendix K, `TREE.CULL` is **not** applied to standing dead tree biomass. The decay reductions above are the only mass adjustments.

## Dead Carbon Fractions (S10b)

Unlike live trees (which use per-species S10a fractions), dead tree carbon fractions come from S10b and vary only by hardwood/softwood and decay class:

| Decay Class | Hardwood | Softwood |
|:-----------:|:--------:|:--------:|
| 1 | 0.470 | 0.501 |
| 2 | 0.473 | 0.504 |
| 3 | 0.481 | 0.506 |
| 4 | 0.480 | 0.520 |
| 5 | 0.472 | 0.527 |

## Population Filter

The standing-dead population is automatically filtered as:

- `STATUSCD = 2` (dead tree)
- `STANDING_DEAD_CD = 1` (standing, not downed)
- `DECAYCD IS NOT NULL` (required for the decay-proportion lookup)
- `DIA >= 1.0` (NSVB floor)

Trees with `STANDING_DEAD_CD = 0` (downed dead) belong to the down dead wood pool and are excluded.

## Technical Notes

**Known limitation: broken-top corrections are not yet implemented.** Approximately 75% of standing dead trees have broken tops (`ACTUALHT < HT`). The full FIADB pipeline applies crown-proportion and volume-ratio adjustments for these trees using `REF_TREE_STND_DEAD_CR_PROP`. The current implementation uses the intact `HT`, which systematically over-estimates biomass for broken-top snags. This is a known gap — the validation gate's ratchet thresholds accommodate it and will tighten when broken-top handling lands.

## Examples

### Standing Dead Carbon Per Acre

```python
result = pyfia.standing_dead(db, pool="ag")
print(f"SD Carbon: {result['CARBON_ACRE'][0]:.2f} tons/acre")
```

### Carbon by Decay Class

```python
result = pyfia.standing_dead(
    db,
    pool="ag",
    grp_by="DECAYCD",
)
for row in result.iter_rows(named=True):
    print(f"Decay {row['DECAYCD']}: {row['CARBON_ACRE']:.3f} tons/acre")
```

### Carbon by Species

```python
result = pyfia.standing_dead(db, pool="ag", by_species=True)
result = pyfia.join_species_names(result, db)
print(result.sort("CARBON_ACRE", descending=True).head(10))
```

### Large Snag Carbon by Ownership

```python
result = pyfia.standing_dead(
    db,
    pool="ag",
    grp_by="OWNGRPCD",
    tree_domain="DIA >= 20.0",
    totals=True,
)
print(result)
```

### Total Standing Dead Carbon with Variance

```python
result = pyfia.standing_dead(
    db,
    pool="total",
    land_type="forest",
    variance=True,
    totals=True,
)
print(f"SD Carbon: {result['CARBON_TOTAL'][0]:,.0f} +/- "
      f"{result['CARBON_TOTAL_SE'][0]:,.0f} tons")
```
