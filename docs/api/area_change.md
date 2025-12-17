# Area Change Estimation

Estimate annual changes in forest land area using remeasured plots.

## Overview

The `area_change()` function calculates net and gross changes in forest land area by tracking land use transitions between measurement periods. This uses the `SUBP_COND_CHNG_MTRX` table which records subplot condition changes.

```python
import pyfia

db = pyfia.FIA("georgia.duckdb")
db.clip_most_recent()

# Net annual area change
net_change = pyfia.area_change(db)

# Gross forest loss by ownership
loss_by_owner = pyfia.area_change(db, change_type="gross_loss", grp_by="OWNGRPCD")
```

## Function Reference

::: pyfia.area_change
    options:
      show_root_heading: true
      show_source: true

## Change Types

| Type | Description |
|------|-------------|
| `"net"` | Net change = (gains from non-forest) - (losses to non-forest) |
| `"gross_gain"` | Area converted from non-forest to forest |
| `"gross_loss"` | Area converted from forest to non-forest |

## Technical Notes

### Data Requirements

Area change estimation requires remeasured plots:

- `SUBP_COND_CHNG_MTRX` table tracks subplot-level condition changes
- `COND` table provides current and previous condition status
- `PLOT` table provides `REMPER` (remeasurement period)
- Only plots measured at two time points contribute to estimates

### Condition Status Codes

| COND_STATUS_CD | Description |
|----------------|-------------|
| 1 | Forest land |
| 2 | Non-forest land |
| 3 | Non-census water |
| 4 | Census water |
| 5 | Denied access |

### Transition Logic

- **Gain**: Previous `COND_STATUS_CD != 1`, Current `COND_STATUS_CD == 1`
- **Loss**: Previous `COND_STATUS_CD == 1`, Current `COND_STATUS_CD != 1`
- **Net**: Gains minus losses

### Annualization

By default, results are annualized by dividing by `REMPER` (remeasurement period, typically 5-7 years). Set `annual=False` to get total change over the measurement period.

## Examples

### Net Annual Forest Area Change

```python
result = pyfia.area_change(db, land_type="forest")
net = result["AREA_CHANGE_TOTAL"][0]
print(f"Annual Net Change: {net:+,.0f} acres/year")
# Positive = net gain, Negative = net loss
```

### Gross Gain and Loss

```python
# Area gained (non-forest to forest)
gain = pyfia.area_change(db, change_type="gross_gain")
print(f"Annual Gain: {gain['AREA_CHANGE_TOTAL'][0]:,.0f} acres/year")

# Area lost (forest to non-forest)
loss = pyfia.area_change(db, change_type="gross_loss")
print(f"Annual Loss: {loss['AREA_CHANGE_TOTAL'][0]:,.0f} acres/year")

# Verify: net = gain - loss
net = pyfia.area_change(db, change_type="net")
assert abs(net["AREA_CHANGE_TOTAL"][0] - (gain["AREA_CHANGE_TOTAL"][0] - loss["AREA_CHANGE_TOTAL"][0])) < 1
```

### Total Change (Non-Annualized)

```python
# Total change over remeasurement period (not per year)
result = pyfia.area_change(db, annual=False)
print(f"Total Change: {result['AREA_CHANGE_TOTAL'][0]:+,.0f} acres")
```

### Area Change by Ownership

```python
result = pyfia.area_change(
    db,
    change_type="gross_loss",
    grp_by="OWNGRPCD"
)
# OWNGRPCD: 10=Forest Service, 20=Other Federal, 30=State/Local, 40=Private
print(result)
```

### Area Change by Forest Type

```python
result = pyfia.area_change(
    db,
    grp_by="FORTYPCD"
)
result = pyfia.join_forest_type_names(result, db)
print(result.sort("AREA_CHANGE_TOTAL").head(10))  # Top losers
```

### With Variance Estimation

```python
result = pyfia.area_change(
    db,
    variance=True
)
print(f"Net Change: {result['AREA_CHANGE_TOTAL'][0]:+,.0f} acres/year")
print(f"SE: {result['SE'][0]:,.0f}")
```

## Comparison with EVALIDator

EVALIDator provides area change estimates through the following `snum` codes:

| snum | Description |
|------|-------------|
| 127 | Forest land area change (remeasured conditions, both measurements) |
| 128 | Forest land area change (at least one measurement is forest) |
| 136 | Annual forest land area change (both measurements forest) |
| 137 | Annual forest land area change (either measurement forest) |

!!! note "EVALIDator Interpretation"
    EVALIDator's "area change" estimates (snum 136, 137) report the **total area** meeting the criteria on remeasured plots, not the net transition. The difference between snum 137 (either) and snum 136 (both) represents the total transition area.

    pyFIA's `area_change()` calculates the **net transition** (gains minus losses), which is typically what users want for trend analysis.

## References

- Bechtold & Patterson (2005), "The Enhanced Forest Inventory and Analysis Program - National Sampling Design and Estimation Procedures", Chapter 4
- FIA Database User Guide, SUBP_COND_CHNG_MTRX table documentation
