# Variance Implementation Analysis and Proposal

## Executive Summary

This document analyzes the variance calculation implementations in pyFIA's `area()` and `volume()` functions, and proposes improvements to align `volume()` with proper FIA statistical methodology.

## Current State

### area() Function - Proper Implementation ✅

The `area()` function (lines 382-541 in `area.py`) implements statistically valid variance calculation following Bechtold & Patterson (2005):

**Key Features:**
- Preserves plot-condition level data for variance calculation
- Implements domain indicator approach for subset estimation
- Uses proper stratified variance formula: V(Ŷ_D) = Σ_h [w_h² × s²_yDh × n_h]
- Handles grouped estimates with separate variance calculations
- Correctly accounts for domain indicators (0/1 values)

**Implementation Details:**
```python
# Line 353: Store plot-condition data
self.plot_condition_data = data_with_strat.select(cols_to_select).collect()

# Lines 485-517: Proper variance calculation
variance_components = strata_stats.with_columns([
    (pl.col("w_h").cast(pl.Float64) ** 2 * pl.col("s2_yh") * pl.col("n_h")).alias("v_h")
])
```

### volume() Function - Placeholder Implementation ⚠️

The `volume()` function (lines 154-181 in `volume.py`) uses a simplified placeholder:

**Current Issues:**
- Fixed 12% coefficient of variation for all estimates
- No data preservation for variance calculation
- No stratification consideration
- Same CV applied uniformly regardless of sample size or grouping

**Current Code:**
```python
# Lines 160-162: Simplified placeholder
results = results.with_columns([
    (pl.col("VOLUME_ACRE") * 0.12).alias("VOLUME_ACRE_SE"),
    (pl.col("VOLUME_TOTAL") * 0.12).alias("VOLUME_TOTAL_SE"),
])
```

## Variance Formula Differences

### Area Estimation (Domain Total)

For area, we're estimating a domain total:
- Formula: V(Ŷ_D) = Σ_h [w_h² × s²_yDh × n_h]
- Where y_i is the domain indicator (0 or 1) times condition proportion
- Variance components are summed across strata

### Volume Estimation (Ratio-of-Means)

For volume, we're estimating a ratio (volume/area):
- Formula: V(R̂) ≈ (1/X̄²) × [V(Ŷ) + R̂² × V(X̄) - 2R̂ × Cov(Ŷ,X̄)]
- Where Y is total volume, X is total area, R is the ratio
- More complex due to covariance between numerator and denominator

## Proposed Implementation for volume()

### Phase 1: Data Preservation

```python
def __init__(self, db, config):
    super().__init__(db, config)
    self.plot_tree_data = None  # Store for variance calculation

def aggregate_results(self, data: pl.LazyFrame) -> pl.DataFrame:
    # ... existing code ...

    # Before two-stage aggregation, preserve data
    cols_to_preserve = [
        "PLT_CN", "CONDID", "STRATUM_CN", "EXPNS",
        "VOLUME_ADJ", "ADJ_FACTOR", "CONDPROP_UNADJ"
    ]

    # Add grouping columns
    if self.config.get("grp_by"):
        grp_by = self.config["grp_by"]
        if isinstance(grp_by, str):
            grp_by = [grp_by]
        cols_to_preserve.extend(grp_by)

    # Preserve data before aggregation
    self.plot_tree_data = data_with_strat.select(cols_to_preserve).collect()
    self.group_cols = group_cols  # Store for variance calculation

    # Continue with existing aggregation...
```

### Phase 2: Variance Calculation

```python
def calculate_variance(self, results: pl.DataFrame) -> pl.DataFrame:
    """Calculate variance using proper ratio estimation formula."""

    if self.plot_tree_data is None:
        # Fallback to conservative estimate
        import warnings
        warnings.warn(
            "Plot-tree data not available for proper variance calculation. "
            "Using placeholder 12% CV."
        )
        return self._apply_placeholder_variance(results)

    # Step 1: Aggregate to plot-condition level
    plot_cond_data = self.plot_tree_data.group_by(
        ["PLT_CN", "CONDID", "STRATUM_CN", "EXPNS", "CONDPROP_UNADJ"]
    ).agg([
        pl.sum("VOLUME_ADJ").alias("y_ic"),  # Volume per condition
    ])

    # Step 2: Aggregate to plot level
    plot_data = plot_cond_data.group_by(
        ["PLT_CN", "STRATUM_CN", "EXPNS"]
    ).agg([
        pl.sum("y_ic").alias("y_i"),  # Total volume per plot
        pl.sum("CONDPROP_UNADJ").alias("x_i")  # Total area per plot
    ])

    # Step 3: Calculate variance for groups or overall
    if self.group_cols:
        variance_results = []
        for group_vals in results.iter_rows():
            # Filter data for this group
            # ... (group filtering logic)
            var_stats = self._calculate_ratio_variance(group_plot_data)
            variance_results.append(var_stats)

        # Join back to results
        var_df = pl.DataFrame(variance_results)
        results = results.join(var_df, on=self.group_cols, how="left")
    else:
        var_stats = self._calculate_ratio_variance(plot_data)
        results = results.with_columns([
            pl.lit(var_stats["se_acre"]).alias("VOLUME_ACRE_SE"),
            pl.lit(var_stats["se_total"]).alias("VOLUME_TOTAL_SE"),
        ])

    return results

def _calculate_ratio_variance(self, plot_data: pl.DataFrame) -> dict:
    """Calculate variance for ratio-of-means estimator."""

    # Get stratum statistics
    strata_stats = plot_data.group_by(["STRATUM_CN"]).agg([
        pl.count("PLT_CN").alias("n_h"),
        pl.mean("y_i").alias("ybar_h"),  # Mean volume
        pl.mean("x_i").alias("xbar_h"),  # Mean area
        pl.var("y_i", ddof=1).alias("s2_yh"),  # Volume variance
        pl.var("x_i", ddof=1).alias("s2_xh"),  # Area variance
        # Covariance between volume and area
        ((pl.col("y_i") - pl.mean("y_i")) *
         (pl.col("x_i") - pl.mean("x_i"))).sum() / (pl.count() - 1)
        .alias("s_yxh"),
        pl.first("EXPNS").cast(pl.Float64).alias("w_h")
    ])

    # Calculate ratio estimate
    total_y = (strata_stats["ybar_h"] * strata_stats["w_h"] * strata_stats["n_h"]).sum()
    total_x = (strata_stats["xbar_h"] * strata_stats["w_h"] * strata_stats["n_h"]).sum()
    ratio = total_y / total_x if total_x > 0 else 0

    # Calculate variance components for ratio estimator
    # V(R) ≈ (1/X̄²) × Σ_h w_h² × [s²_yh + R² × s²_xh - 2R × s_yxh] × n_h
    variance_components = strata_stats.with_columns([
        (pl.col("w_h") ** 2 *
         (pl.col("s2_yh") +
          ratio ** 2 * pl.col("s2_xh") -
          2 * ratio * pl.col("s_yxh")) *
         pl.col("n_h")
        ).alias("v_h")
    ])

    # Sum variance components
    total_variance = variance_components["v_h"].sum()
    if total_variance is None or total_variance < 0:
        total_variance = 0.0

    # Calculate standard errors
    se_total = total_variance ** 0.5
    se_acre = se_total / total_x if total_x > 0 else 0

    return {
        "variance": total_variance,
        "se_total": se_total,
        "se_acre": se_acre,
        "ratio": ratio
    }
```

## Implementation Timeline

### Phase 1: Foundation (Immediate)
1. Add data preservation structure to VolumeEstimator
2. Implement basic plot-level aggregation
3. Add fallback warnings for missing data

### Phase 2: Core Variance (Week 1)
1. Implement _calculate_ratio_variance method
2. Add stratified variance calculation
3. Handle grouped estimates

### Phase 3: Testing & Validation (Week 2)
1. Compare with FIA EVALIDator results
2. Test edge cases (single strata, small samples)
3. Validate CV ranges for different metrics

### Phase 4: Extension to Other Estimators
1. Apply similar pattern to biomass.py
2. Extend to tpa.py
3. Update mortality.py and growth.py

## Testing Strategy

### Unit Tests
```python
def test_volume_variance_calculation():
    """Test that volume variance is properly calculated."""
    db = FIA("test_data.duckdb")
    db.clip_by_state(13)  # Georgia

    # Get volume with variance
    results = volume(db, variance=False)

    # Check variance is calculated
    assert "VOLCFNET_ACRE_SE" in results.columns
    assert results["VOLCFNET_ACRE_SE"][0] > 0

    # Check CV is reasonable (typically 5-20% for state-level)
    cv = 100 * results["VOLCFNET_ACRE_SE"][0] / results["VOLCFNET_ACRE"][0]
    assert 5 <= cv <= 20

def test_volume_variance_by_groups():
    """Test variance calculation with grouping."""
    db = FIA("test_data.duckdb")
    results = volume(db, grp_by="OWNGRPCD")

    # Each group should have different SE
    se_values = results["VOLCFNET_ACRE_SE"].unique()
    assert len(se_values) > 1  # Not all the same
```

### Integration Tests
- Compare with published FIA estimates
- Validate against EVALIDator web tool
- Check consistency across different groupings

## Expected Benefits

1. **Statistical Validity**: Proper variance estimates following FIA methodology
2. **Accurate Confidence Intervals**: Users can calculate valid CIs for estimates
3. **Domain-Specific Variance**: Different CVs for different species/conditions
4. **Sample Size Awareness**: Smaller samples correctly show higher variance
5. **Publication Ready**: Results match official FIA publications

## Migration Path

1. **Backward Compatibility**: Keep placeholder as fallback
2. **Opt-in Period**: Add `use_proper_variance=True` parameter initially
3. **Documentation**: Clear examples showing difference
4. **Deprecation**: Remove placeholder after validation period

## References

- Bechtold, W.A. and Patterson, P.L., 2005. The enhanced forest inventory and analysis program - national sampling design and estimation procedures. Gen. Tech. Rep. SRS-80.
- Scott, C.T., Bechtold, W.A., Reams, G.A., Smith, W.D., Westfall, J.A., Hansen, M.H. and Moisen, G.G., 2005. Sample-based estimators used by the forest inventory and analysis national information management system.
- FIA Database User Guide: https://www.fia.fs.usda.gov/library/database-documentation/