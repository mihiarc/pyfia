# Variance Calculation Fix Implementation Plan

## Problem Summary

The current pyFIA implementation has **critically flawed variance calculations** as identified in DATA_SCIENCE_REVIEW.md. Testing with Georgia FIA data shows:
- Area estimates are accurate (within 2% of EVALIDator)
- Standard errors are 2x higher than they should be (1.20% vs 0.563%)
- All estimators use placeholder CV values instead of proper statistical calculations

## Root Causes Identified

### 1. Wrong Statistical Model
- **Current**: Using ratio-of-means estimator inappropriately
- **Should be**: Using total estimation for area (not a ratio)
- **Impact**: Introduces unnecessary covariance terms and complexity

### 2. Incorrect Data Aggregation Level
- **Current**: Aggregating to plot level, losing condition-level detail
- **Should be**: Maintain plot-condition level for variance calculation
- **Impact**: Inflates variance by not properly accounting for within-plot variation

### 3. Incorrect Auxiliary Variable
- **Current**: Using `AREA_USED = 1.0` constant as denominator
- **Should be**: For area estimation, no separate denominator needed
- **Impact**: Creates artificial ratio that doesn't represent the actual estimation

### 4. Missing Hierarchical Structure
- **Current**: Using STRATUM_CN directly
- **Should be**: Respect ESTN_UNIT/STRATUM hierarchy
- **Impact**: Doesn't properly account for multi-stage sampling design

## Implementation Plan

### Phase 1: Fix Area Estimator Variance (Priority: CRITICAL)

#### Step 1.1: Modify AreaEstimator class
```python
# In src/pyfia/estimation/estimators/area.py

class AreaEstimator(BaseEstimator):
    def __init__(self, db, config):
        super().__init__(db, config)
        self.plot_condition_data = None  # Store for variance calc

    def aggregate_results(self, data: pl.LazyFrame) -> pl.DataFrame:
        """Modified to preserve plot-condition level data."""
        # Get stratification
        strat_data = self._get_stratification_data()

        # Join and apply adjustments
        data_with_strat = data.join(strat_data, on="PLT_CN", how="inner")
        data_with_strat = apply_area_adjustment_factors(
            data_with_strat,
            prop_basis_col="PROP_BASIS",
            output_col="ADJ_FACTOR_AREA"
        )

        # CRITICAL: Store plot-condition level data
        self.plot_condition_data = data_with_strat.select([
            "PLT_CN", "CONDID", "ESTN_UNIT", "STRATUM_CN",
            "CONDPROP_UNADJ", "ADJ_FACTOR_AREA", "EXPNS"
        ]).collect()

        # Continue with normal aggregation for estimates
        # ... existing aggregation code ...
```

#### Step 1.2: Implement Correct Variance Calculation
```python
def calculate_variance(self, results: pl.DataFrame) -> pl.DataFrame:
    """Calculate variance using proper total estimation formula."""

    if self.plot_condition_data is None:
        # Fallback if no data available
        return results.with_columns([
            (pl.col("AREA_TOTAL") * 0.05).alias("AREA_SE")
        ])

    # Step 1: Calculate condition-level areas
    cond_data = self.plot_condition_data.with_columns([
        (pl.col("CONDPROP_UNADJ") *
         pl.col("ADJ_FACTOR_AREA")).alias("h_ic")
    ])

    # Step 2: Aggregate to plot level
    plot_data = cond_data.group_by(
        ["PLT_CN", "ESTN_UNIT", "STRATUM_CN", "EXPNS"]
    ).agg([
        pl.sum("h_ic").alias("y_i")  # Total adjusted area per plot
    ])

    # Step 3: Calculate stratum statistics
    strata_stats = plot_data.group_by(["ESTN_UNIT", "STRATUM_CN"]).agg([
        pl.count("PLT_CN").alias("n_h"),
        pl.mean("y_i").alias("ybar_h"),
        pl.var("y_i", ddof=1).alias("s2_yh"),
        pl.first("EXPNS").alias("w_h")
    ])

    # Step 4: Calculate variance components
    # V(Ŷ) = Σ_h [N_h² × (1 - f_h) × s²_yh / n_h]
    # Where f_h = n_h/N_h (sampling fraction)
    # For large N_h, (1 - f_h) ≈ 1

    variance_components = strata_stats.with_columns([
        (pl.col("w_h") ** 2 *
         pl.col("s2_yh") / pl.col("n_h")).alias("v_h")
    ])

    # Step 5: Sum variance components
    total_variance = variance_components["v_h"].sum()
    se_total = total_variance ** 0.5

    # Step 6: Calculate SE as percentage
    total_area = results["AREA_TOTAL"][0] if "AREA_TOTAL" in results.columns else results["AREA"][0]
    se_percent = 100 * se_total / total_area if total_area > 0 else 0

    # Update results
    return results.with_columns([
        pl.lit(se_total).alias("AREA_SE"),
        pl.lit(se_percent).alias("AREA_SE_PERCENT"),
        pl.lit(total_variance).alias("AREA_VARIANCE")
    ])
```

### Phase 2: Fix Ratio Estimation for Other Metrics

For metrics like volume/acre, biomass/acre, we DO need ratio estimation:

```python
def calculate_ratio_variance(
    plot_data: pl.DataFrame,
    response_col: str,  # e.g., "VOLUME"
    area_col: str = "AREA"
) -> dict:
    """Proper ratio-of-means variance for per-acre estimates."""

    # Calculate stratum statistics
    strata_stats = plot_data.group_by(["ESTN_UNIT", "STRATUM_CN"]).agg([
        pl.count("PLT_CN").alias("n_h"),
        pl.mean(response_col).alias("ybar_h"),
        pl.mean(area_col).alias("xbar_h"),
        pl.var(response_col, ddof=1).alias("s2_yh"),
        pl.var(area_col, ddof=1).alias("s2_xh"),
        # Covariance
        ((pl.col(response_col) - pl.mean(response_col)) *
         (pl.col(area_col) - pl.mean(area_col))).sum().alias("sum_yx")
    ]).with_columns([
        (pl.col("sum_yx") / (pl.col("n_h") - 1)).alias("s_yxh")
    ])

    # Calculate totals
    Y_total = (strata_stats["ybar_h"] * strata_stats["w_h"]).sum()
    X_total = (strata_stats["xbar_h"] * strata_stats["w_h"]).sum()
    R = Y_total / X_total  # Ratio estimate

    # Variance of ratio
    variance_components = strata_stats.with_columns([
        (pl.col("w_h") ** 2 / pl.col("n_h") *
         (pl.col("s2_yh") +
          R ** 2 * pl.col("s2_xh") -
          2 * R * pl.col("s_yxh"))).alias("v_h")
    ])

    total_variance = variance_components["v_h"].sum()
    se_ratio = (total_variance / X_total ** 2) ** 0.5

    return {
        "estimate": R,
        "variance": total_variance,
        "se": se_ratio,
        "se_percent": 100 * se_ratio / R if R > 0 else 0
    }
```

### Phase 3: Update All Estimators

#### Priority Order:
1. **area.py** - Most critical, simplest case
2. **volume.py** - Needs ratio estimation
3. **biomass.py** - Similar to volume
4. **tpa.py** - Simple total estimation
5. **mortality.py** - Complex GRM tables
6. **growth.py** - Complex GRM tables

### Phase 4: Add Comprehensive Testing

```python
# tests/test_variance_validation.py

def test_area_variance_against_published():
    """Validate variance calculation against EVALIDator."""
    db = FIA("data/georgia.duckdb")
    db.clip_most_recent(eval_type="ALL")

    results = area(db, land_type="forest")

    # Published values
    published_se_percent = 0.563

    # Should be within 10% of published
    assert abs(results["AREA_SE_PERCENT"][0] - published_se_percent) < 0.1

def test_variance_properties():
    """Test statistical properties of variance estimator."""
    # 1. Variance should increase with smaller sample size
    # 2. Variance should decrease with stratification
    # 3. SE% should be approximately 1/sqrt(n) for simple random sampling
    pass
```

### Phase 5: Add Finite Population Correction

```python
def apply_fpc(variance: float, n_h: int, N_h: int) -> float:
    """Apply finite population correction when sampling fraction is large."""
    if N_h > 0 and n_h / N_h > 0.05:
        fpc = (N_h - n_h) / N_h
        return variance * fpc
    return variance
```

## Success Criteria

1. **Area SE matches EVALIDator**: Within 10% of published values
2. **Grouped estimates have different SEs**: Each group has appropriate SE
3. **Tests pass**: All variance validation tests pass
4. **Performance maintained**: No significant slowdown

## Timeline

- **Week 1**: Implement Phase 1 (Area estimator fix)
- **Week 2**: Implement Phase 2-3 (Other estimators)
- **Week 3**: Testing and validation
- **Week 4**: Documentation and final review

## Notes from DATA_SCIENCE_REVIEW.md

The review correctly identifies (Section 1.1) that the current implementation uses placeholder values. The recommended solution in the review is close but needs adjustments:

1. The review suggests ratio-of-means for everything, but area estimation should use total estimation
2. The review's code example doesn't preserve plot-condition level data properly
3. The finite population correction (Section 1.2) is important but secondary to getting the basic formula right

## References

- Bechtold, W.A. and Patterson, P.L. (2005). The Enhanced Forest Inventory and Analysis Program
- FIA Database User Guide: Section on Variance Estimation
- EVALIDator documentation on sampling error calculations