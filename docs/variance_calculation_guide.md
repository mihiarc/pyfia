# FIA Variance Calculation Guide

## Understanding FIA Variance Estimation for pyFIA

This guide explains the proper implementation of variance calculations for FIA (Forest Inventory and Analysis) estimates, based on lessons learned from implementing mortality variance calculations that match EVALIDator results.

## Table of Contents
1. [Key Concepts](#key-concepts)
2. [The FIA Variance Formula](#the-fia-variance-formula)
3. [Implementation Steps](#implementation-steps)
4. [Common Pitfalls](#common-pitfalls)
5. [Validation Against EVALIDator](#validation-against-evalidator)
6. [Code Examples](#code-examples)

## Key Concepts

### EXPNS: The FIA Expansion Factor

The most critical concept for understanding FIA variance calculations is the **EXPNS** (expansion factor). Unlike standard survey weights, EXPNS is defined as:

```
EXPNS = total_acres_in_stratum / number_of_plots_in_stratum
```

**Key insight**: EXPNS already incorporates the inverse of sample size (1/n_h) in its definition. This is fundamentally different from standard stratified sampling weights.

### Domain Estimation

FIA estimates are typically **domain estimates** - we're estimating totals or means for a subset (domain) of the population, such as:
- Forestland only (not all land)
- Specific tree species
- Mortality trees only
- Trees above certain diameter thresholds

### Total vs Mean Estimation

- **Total estimation**: Estimating the total value across all acres (e.g., total volume in cubic feet)
- **Mean/per-acre estimation**: Estimating the average value per acre (e.g., volume per acre)

## The FIA Variance Formula

### For Domain Total Estimation

The correct variance formula for FIA domain totals is:

```
V(Ŷ_D) = Σ_h [w_h² × s²_yh × n_h]
```

Where:
- `w_h` = EXPNS for stratum h (expansion factor in acres/plot)
- `s²_yh` = sample variance of the attribute in stratum h
- `n_h` = number of sampled plots in stratum h
- The sum is over all strata (h)

### Why Multiply by n_h?

This seems counterintuitive compared to standard survey formulas, but is correct because:

1. EXPNS = total_acres_h / n_h (already contains 1/n_h)
2. We're estimating a total, not a mean
3. The mathematical derivation:
   ```
   V(total) = V(Σ_i y_i × EXPNS_i)
            = Σ_h [EXPNS_h² × n_h × Var(y_i)]
            = Σ_h [w_h² × s²_yh × n_h]
   ```

### For Per-Acre Estimates

To get variance for per-acre estimates:
```
V(per_acre) = V(total) / (total_area)²
SE(per_acre) = SE(total) / total_area
```

Where total_area = Σ_h (EXPNS_h × n_h)

## Implementation Steps

### Step 1: Aggregate to Plot Level

Include ALL plots in the evaluation, with zero values for plots without the attribute:

```python
# Get all plots in the evaluation
all_plots = get_all_plots_for_evalid()

# Get attribute values for plots that have them
plots_with_attribute = calculate_plot_values()

# Join, filling missing with zeros
all_plots_with_values = all_plots.join(
    plots_with_attribute,
    on="PLT_CN",
    how="left"
).fill_null(0.0)
```

### Step 2: Calculate Stratum Statistics

Group by stratum and calculate statistics:

```python
strat_stats = all_plots_with_values.group_by("STRATUM_CN").agg([
    pl.count("PLT_CN").alias("n_h"),           # Number of plots
    pl.mean("plot_value").alias("ybar_h"),     # Mean value
    pl.var("plot_value", ddof=1).alias("s2_yh"), # Sample variance
    pl.first("EXPNS").alias("w_h")             # Expansion factor
])
```

### Step 3: Handle Single-Plot Strata

Set variance to zero for strata with only one plot:

```python
strat_stats = strat_stats.with_columns([
    pl.when(pl.col("s2_yh").is_null() | (pl.col("n_h") == 1))
    .then(0.0)
    .otherwise(pl.col("s2_yh"))
    .alias("s2_yh")
])
```

### Step 4: Calculate Variance Components

Apply the variance formula:

```python
variance_components = strat_stats.with_columns([
    (pl.col("w_h").cast(pl.Float64) ** 2 *
     pl.col("s2_yh") *
     pl.col("n_h")).alias("v_h")
])
```

### Step 5: Sum Components and Calculate SE

```python
total_variance = variance_components["v_h"].sum()
se_total = total_variance ** 0.5

# For per-acre SE
total_area = (strat_stats["w_h"] * strat_stats["n_h"]).sum()
se_per_acre = se_total / total_area
```

## Common Pitfalls

### ❌ Pitfall 1: Using Standard Survey Formulas

**Wrong**: Applying textbook stratified sampling variance formulas that divide by n_h
```python
# WRONG for FIA!
variance = sum(w_h**2 * s2_h / n_h)  # This is for means, not FIA totals
```

**Right**: Using FIA-specific formula that multiplies by n_h
```python
# CORRECT for FIA
variance = sum(w_h**2 * s2_h * n_h)  # Correct for FIA domain totals
```

### ❌ Pitfall 2: Excluding Zero-Value Plots

**Wrong**: Only including plots with the attribute of interest
```python
# WRONG - biases variance
plots_with_mortality = data.filter(mortality > 0)
```

**Right**: Including all plots, with zeros for plots without the attribute
```python
# CORRECT - unbiased variance
all_plots = all_plots.join(mortality_plots, how="left").fill_null(0)
```

### ❌ Pitfall 3: Confusing Population and Sample Counts

**Wrong**: Using N_h (population plots) when you have n_h (sample plots)
```python
# WRONG if N_h is not available
variance = N_h**2 * s2_h / n_h  # Need to know N_h
```

**Right**: Using EXPNS which encapsulates the N_h/n_h relationship
```python
# CORRECT - EXPNS contains the necessary information
variance = EXPNS**2 * s2_h * n_h
```

### ❌ Pitfall 4: Not Handling Adjustment Factors

For tree-level estimates, remember to apply adjustment factors BEFORE variance calculation:

```python
# Apply adjustment factors based on tree size
adjusted_value = value * adjustment_factor
# THEN calculate variance on adjusted values
```

## Validation Against EVALIDator

### Expected Accuracy

When properly implemented, variance calculations should be within:
- **0.5% of EVALIDator SE%** for large, well-sampled domains
- **1-2% of EVALIDator SE%** for smaller domains
- Higher differences may occur for rare attributes or small sample sizes

### Common Sources of Differences

1. **Rounding**: Intermediate calculation rounding
2. **Adjustment factors**: Slight differences in how factors are applied
3. **Finite Population Correction**: EVALIDator may apply FPC in some cases
4. **Edge cases**: Different handling of single-plot strata or missing values

### Validation Checklist

- [ ] Total estimate matches EVALIDator (validates core calculation)
- [ ] SE% is within 1% of EVALIDator value
- [ ] Plot counts match EVALIDator reports
- [ ] Results are consistent across different groupings

## Code Examples

### Complete Mortality Variance Implementation

```python
def calculate_variance(self, results: pl.DataFrame) -> pl.DataFrame:
    """Calculate variance for FIA estimates using stratified sampling formulas."""

    # Get stratification data
    strat_data = self._get_stratification_data()

    # Load and process data
    data = self.load_data()
    if data is None:
        return results.with_columns([
            pl.lit(0.0).alias("SE_TOTAL"),
            pl.lit(0.0).alias("SE_ACRE")
        ])

    # Apply filters and calculate values
    data = self.apply_filters(data)
    data = self.calculate_values(data)

    # Join with stratification
    data_with_strat = data.join(strat_data, on="PLT_CN", how="inner")

    # Apply adjustment factors
    data_with_strat = apply_adjustment_factors(data_with_strat)

    # Get all plots (including zeros)
    all_plots = strat_data.select("PLT_CN", "STRATUM_CN", "EXPNS").unique()

    # Aggregate to plot level
    plot_values = data_with_strat.group_by(["PLT_CN", "STRATUM_CN", "EXPNS"]).agg([
        pl.sum("adjusted_value").alias("plot_value")
    ])

    # Join to include all plots with zeros
    all_plots_values = all_plots.join(
        plot_values.select(["PLT_CN", "plot_value"]),
        on="PLT_CN",
        how="left"
    ).with_columns([
        pl.col("plot_value").fill_null(0.0)
    ])

    # Calculate stratum statistics
    strat_stats = all_plots_values.group_by("STRATUM_CN").agg([
        pl.count("PLT_CN").alias("n_h"),
        pl.mean("plot_value").alias("ybar_h"),
        pl.var("plot_value", ddof=1).alias("s2_yh"),
        pl.first("EXPNS").alias("w_h")
    ])

    # Handle single-plot strata
    strat_stats = strat_stats.with_columns([
        pl.when(pl.col("s2_yh").is_null() | (pl.col("n_h") == 1))
        .then(0.0)
        .otherwise(pl.col("s2_yh"))
        .alias("s2_yh")
    ])

    # Calculate variance components
    variance_components = strat_stats.with_columns([
        (pl.col("w_h").cast(pl.Float64) ** 2 *
         pl.col("s2_yh") *
         pl.col("n_h")).alias("v_h")
    ])

    # Sum variance components
    total_variance = variance_components.collect()["v_h"].sum()
    se_total = (total_variance ** 0.5) if total_variance > 0 else 0.0

    # Calculate per-acre SE
    total_area = (strat_stats["w_h"] * strat_stats["n_h"]).sum()
    se_acre = se_total / total_area if total_area > 0 else 0.0

    # Update results
    results = results.with_columns([
        pl.lit(se_total).alias("SE_TOTAL"),
        pl.lit(se_acre).alias("SE_ACRE")
    ])

    # Add SE% if totals exist
    if "TOTAL" in results.columns:
        results = results.with_columns([
            pl.when(pl.col("TOTAL") > 0)
            .then(pl.col("SE_TOTAL") / pl.col("TOTAL") * 100)
            .otherwise(None)
            .alias("SE_PERCENT")
        ])

    return results
```

### Testing Variance Calculations

```python
def test_variance_against_evalidator():
    """Test that variance calculations match EVALIDator."""

    # Run estimation
    result = estimate_with_variance(db)

    # Calculate SE%
    se_percent = result["SE_TOTAL"][0] / result["TOTAL"][0] * 100

    # Compare with EVALIDator
    evalidator_se_percent = 5.527  # From EVALIDator report
    difference = abs(se_percent - evalidator_se_percent)

    assert difference < 0.5, f"SE% differs by {difference:.2f}% from EVALIDator"

    print(f"SE%: {se_percent:.3f}% (EVALIDator: {evalidator_se_percent}%)")
    print(f"Difference: {difference:.3f}%")
```

## References

1. **Bechtold, W.A., and Patterson, P.L. (Editors). 2005.** The Enhanced Forest Inventory
   and Analysis Program - National Sampling Design and Estimation Procedures.
   Gen. Tech. Rep. SRS-80. Asheville, NC: U.S. Department of Agriculture, Forest Service,
   Southern Research Station. 85 p. https://doi.org/10.2737/SRS-GTR-80

   **Key Sections for Variance Calculations:**
   | Section/Equation | Page | Description |
   |------------------|------|-------------|
   | Eq. 4.1 | 47 | Domain indicator function (Φ_hid) for condition attributes |
   | Eq. 4.2 | 49 | Adjustment factor (1/p_mh) for non-sampled plots |
   | Eq. 4.8 | 53 | Tree attribute estimation (y_hid) |
   | Section 3.4.3 | 40-42 | Nonsampled Plots and Plot Replacement |
   | Section 4.2 | 55-60 | Post-stratified estimation and EXPNS expansion factor |

2. **Scott, C.T.; Bechtold, W.A.; Reams, G.A.; Smith, W.D.; Westfall, J.A.;
   Hansen, M.H.; Moisen, G.G. 2005.** Sample-based estimators used by the
   Forest Inventory and Analysis national information management system.
   Gen. Tech. Rep. SRS-80, Chapter 4, pp. 53-77.

3. **Westfall, J.A.; Patterson, P.L.; Coulston, J.W. 2011.** Post-stratified
   estimation: Within-strata and total sample size recommendations.
   Canadian Journal of Forest Research, 41(5): 1130-1139.

4. **USDA Forest Service. 2018.** Population Estimation User Guide (Edition: November 2018).
   https://research.fs.usda.gov/sites/default/files/2024-05/wo-nov2018_ug_population_estimation.pdf

5. FIA Database User Guide, Version 9.1 - Section on Statistical Estimation

## Summary

The key to correct FIA variance calculation is understanding that:
1. **EXPNS is not a standard survey weight** - it's an area-based expansion factor
2. **Multiply by n_h for domain totals** - this is correct for FIA's methodology
3. **Include all plots with zeros** - essential for unbiased variance
4. **Validate against EVALIDator** - the gold standard for FIA estimates

When these principles are followed, variance calculations will match EVALIDator results within acceptable tolerances (typically <1% difference in SE%).