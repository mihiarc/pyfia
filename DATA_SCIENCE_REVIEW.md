# pyFIA Data Science and Statistical Review

## Executive Summary

This comprehensive review of the pyFIA package identifies several critical issues with statistical methodology, numerical accuracy, and data processing efficiency. While the codebase shows good architectural simplification (85% LOC reduction), **the variance calculation implementation is critically incomplete**, using placeholder values instead of proper FIA statistical methods. Additionally, there are opportunities for significant performance improvements through better Polars usage and vectorization.

## 1. Statistical Methodology Issues

### 1.1 Critical: Placeholder Variance Calculations

**Issue**: The variance calculation in all estimators uses a hardcoded 10-15% coefficient of variation instead of implementing proper Bechtold & Patterson (2005) methodology.

```python
# Current implementation in volume.py, lines 159-164
def calculate_variance(self, results: pl.DataFrame) -> pl.DataFrame:
    # Simplified variance calculation
    # Conservative estimate: 10-15% CV is typical for volume estimates
    results = results.with_columns([
        (pl.col("VOLUME_ACRE") * 0.12).alias("VOLUME_ACRE_SE"),
        (pl.col("VOLUME_TOTAL") * 0.12).alias("VOLUME_TOTAL_SE"),
    ])
```

**Impact**: This renders all standard errors and confidence intervals meaningless for scientific analysis.

**Recommendation**: Implement the actual ratio-of-means variance calculation from `statistics.py`:

```python
def calculate_variance(self, results: pl.DataFrame) -> pl.DataFrame:
    """Calculate variance using proper FIA methodology."""
    # Get stratification data with plot-level values
    strat_data = self._get_stratification_data().collect()

    # Join with tree data to get plot-level aggregates
    plot_data = self.data.group_by(["PLT_CN", "STRATUM_CN"]).agg([
        pl.sum("VOLUME_ADJ").alias("y_i"),  # Response variable
        pl.sum("CONDPROP_UNADJ").alias("a_i"),  # Area variable
        pl.first("EXPNS").alias("w_h")
    ]).collect()

    # Calculate stratum-level statistics
    strata_stats = plot_data.group_by("STRATUM_CN").agg([
        pl.count("PLT_CN").alias("n_h"),
        pl.mean("y_i").alias("ybar_h"),
        pl.mean("a_i").alias("abar_h"),
        pl.std("y_i", ddof=1).alias("s_y_h"),
        pl.std("a_i", ddof=1).alias("s_a_h"),
        # Covariance calculation
        ((pl.col("y_i") - pl.mean("y_i")) *
         (pl.col("a_i") - pl.mean("a_i"))).mean().alias("s_ya_h"),
        pl.first("w_h").alias("w_h")
    ])

    # Apply ratio-of-means variance formula
    Y_total = (strata_stats["ybar_h"] * strata_stats["w_h"]).sum()
    A_total = (strata_stats["abar_h"] * strata_stats["w_h"]).sum()
    R = Y_total / A_total if A_total > 0 else 0

    # Variance components
    var_components = strata_stats.with_columns([
        ((pl.col("w_h") ** 2) * (1 - 1/pl.col("n_h")) / pl.col("n_h") *
         (pl.col("s_y_h") ** 2 +
          (R ** 2) * pl.col("s_a_h") ** 2 -
          2 * R * pl.col("s_ya_h"))).alias("var_h")
    ])

    total_variance = var_components["var_h"].sum()
    se = (total_variance / (A_total ** 2)) ** 0.5 if A_total > 0 else 0

    return results.with_columns([
        pl.lit(se).alias("VOLUME_ACRE_SE"),
        pl.lit(se * A_total).alias("VOLUME_TOTAL_SE")
    ])
```

### 1.2 Missing Finite Population Correction

**Issue**: The variance calculations don't apply finite population correction when sampling fraction is large.

**Recommendation**: Apply FPC when n/N > 0.05:

```python
def apply_fpc(variance: float, n_sampled: int, n_total: int) -> float:
    """Apply finite population correction."""
    if n_total > 0 and n_sampled / n_total > 0.05:
        fpc = (n_total - n_sampled) / n_total
        return variance * fpc
    return variance
```

### 1.3 Two-Stage Aggregation Implementation

**Positive**: The `_apply_two_stage_aggregation` method correctly implements FIA's critical two-stage methodology, preventing the 26x underestimation bug. This is well-tested and documented.

**Suggestion**: Consider extracting the covariance calculation into a separate method for reusability:

```python
def calculate_stratum_covariance(
    plot_data: pl.DataFrame,
    response_col: str,
    area_col: str,
    stratum_col: str
) -> pl.DataFrame:
    """Calculate within-stratum covariance for ratio estimator."""
    return plot_data.group_by(stratum_col).agg([
        ((pl.col(response_col) - pl.mean(response_col)) *
         (pl.col(area_col) - pl.mean(area_col))).mean().alias("cov_ya")
    ])
```

## 2. Data Processing and Efficiency Issues

### 2.1 Inefficient LazyFrame Usage

**Issue**: Frequent `collect()` calls break lazy evaluation benefits:

```python
# Current pattern in base.py, line 236
data_df = data.collect()  # Materializes entire dataset
# Apply filters...
return data_df.lazy()  # Convert back to lazy
```

**Recommendation**: Keep operations lazy using Polars expressions:

```python
def apply_filters(self, data: pl.LazyFrame) -> pl.LazyFrame:
    """Apply domain filtering without materializing."""
    # Tree domain filter using lazy expression
    if self.config.get("tree_domain"):
        filter_expr = parse_domain_to_expr(self.config["tree_domain"])
        data = data.filter(filter_expr)

    # Land type filter
    land_type = self.config.get("land_type", "forest")
    if land_type == "forest":
        data = data.filter(pl.col("COND_STATUS_CD") == 1)
    elif land_type == "timber":
        data = data.filter(
            (pl.col("COND_STATUS_CD") == 1) &
            (pl.col("SITECLCD").is_in([1, 2, 3, 4, 5, 6])) &
            (pl.col("RESERVCD") == 0)
        )

    return data  # Stays lazy throughout
```

### 2.2 Missing Vectorization Opportunities

**Issue**: Tree expansion factors calculated row-by-row instead of vectorized:

**Recommendation**: Use Polars' `when-then-otherwise` for vectorized operations:

```python
# Vectorized adjustment factor calculation
def get_vectorized_adjustment(df: pl.LazyFrame) -> pl.LazyFrame:
    return df.with_columns([
        pl.when(pl.col("DIA").is_null())
            .then(pl.col("ADJ_FACTOR_SUBP"))
        .when(pl.col("DIA") < 5.0)
            .then(pl.col("ADJ_FACTOR_MICR"))
        .when(pl.col("DIA") < pl.col("MACRO_BREAKPOINT_DIA").fill_null(9999))
            .then(pl.col("ADJ_FACTOR_SUBP"))
        .otherwise(pl.col("ADJ_FACTOR_MACR"))
        .alias("ADJ_FACTOR")
    ])
```

### 2.3 Inefficient Schema Checking

**Issue**: Multiple `collect_schema()` calls for column validation:

```python
# Current pattern - calls collect_schema multiple times
available_cols = data_with_strat.collect_schema().names()  # Line 440
# ... later ...
available_cols = data.collect_schema().names()  # Line 131
```

**Recommendation**: Cache schema once per operation:

```python
class BaseEstimator:
    def __init__(self, db, config):
        # ... existing code ...
        self._schema_cache = {}

    def get_schema(self, df: pl.LazyFrame, cache_key: str) -> list:
        """Get schema with caching."""
        if cache_key not in self._schema_cache:
            self._schema_cache[cache_key] = df.collect_schema().names()
        return self._schema_cache[cache_key]
```

## 3. Numerical Accuracy Issues

### 3.1 Floating-Point Precision Loss

**Issue**: Direct floating-point operations without precision control:

```python
# Potential precision loss
(pl.col("VOLUME_ACRE") * 0.12).alias("VOLUME_ACRE_SE")
```

**Recommendation**: Use precise decimal calculations for financial/scientific accuracy:

```python
from decimal import Decimal, getcontext

def calculate_precise_variance(estimate: float, cv: float) -> float:
    """Calculate variance with high precision."""
    getcontext().prec = 28  # Set precision
    d_estimate = Decimal(str(estimate))
    d_cv = Decimal(str(cv))
    variance = (d_estimate * d_cv) ** 2
    return float(variance)
```

### 3.2 Division by Zero Handling

**Positive**: The code properly handles division by zero in ratio calculations:

```python
pl.when(pl.col("AREA_TOTAL") > 0)
    .then(pl.col(f"{metric_name}_NUM") / pl.col("AREA_TOTAL"))
    .otherwise(0.0)
```

**Suggestion**: Consider using NaN instead of 0.0 for undefined ratios to distinguish from true zeros:

```python
.otherwise(float('nan'))  # More statistically appropriate
```

### 3.3 Missing Bounds Checking

**Issue**: No validation of extreme values that could indicate data issues:

**Recommendation**: Add statistical outlier detection:

```python
def validate_estimates(df: pl.DataFrame, metric: str) -> pl.DataFrame:
    """Flag statistical outliers using IQR method."""
    q1 = df[metric].quantile(0.25)
    q3 = df[metric].quantile(0.75)
    iqr = q3 - q1

    return df.with_columns([
        ((pl.col(metric) < (q1 - 3 * iqr)) |
         (pl.col(metric) > (q3 + 3 * iqr))).alias(f"{metric}_OUTLIER")
    ])
```

## 4. Testing and Validation Issues

### 4.1 Limited Property-Based Testing

**Issue**: Property-based tests are incomplete and don't test statistical invariants:

**Recommendation**: Add comprehensive property tests:

```python
from hypothesis import given, strategies as st
import numpy as np

@given(
    n_plots=st.integers(min_value=10, max_value=1000),
    cv_true=st.floats(min_value=0.05, max_value=0.5)
)
def test_variance_estimation_consistency(n_plots, cv_true):
    """Test that variance estimation is consistent with known CV."""
    # Generate synthetic data with known variance
    true_mean = 1000.0
    true_sd = true_mean * cv_true

    # Simulate plot values
    plot_values = np.random.normal(true_mean, true_sd, n_plots)
    plot_values = np.maximum(plot_values, 0)  # Ensure non-negative

    # Create DataFrame
    df = pl.DataFrame({
        "PLT_CN": [f"P{i}" for i in range(n_plots)],
        "VALUE": plot_values,
        "EXPNS": np.ones(n_plots) * 1000.0
    })

    # Calculate variance using FIA method
    result = calculate_ratio_of_means_variance(
        df, "VALUE", area_col="EXPNS"
    )

    estimated_cv = result["SE"][0] / result["ESTIMATE"][0]

    # Should be within reasonable bounds (accounting for sampling error)
    assert abs(estimated_cv - cv_true) < 0.1
```

### 4.2 Missing Edge Case Testing

**Issue**: No tests for edge cases like empty strata, single plot estimates, or missing data.

**Recommendation**: Add comprehensive edge case tests:

```python
def test_single_plot_variance():
    """Test variance calculation with single plot (should be undefined)."""
    df = pl.DataFrame({
        "PLT_CN": ["P1"],
        "VALUE": [100.0],
        "AREA": [1.0]
    })

    result = calculate_ratio_of_means_variance(df, "VALUE", "AREA")
    assert result["SE"][0] == 0 or np.isnan(result["SE"][0])

def test_empty_stratum():
    """Test handling of empty strata."""
    df = pl.DataFrame({
        "PLT_CN": [],
        "VALUE": [],
        "STRATUM": []
    })

    result = calculate_ratio_of_means_variance(df, "VALUE")
    assert result["ESTIMATE"][0] == 0
```

## 5. Data Science Best Practices

### 5.1 Missing Data Documentation

**Issue**: No systematic handling or documentation of missing data patterns:

**Recommendation**: Add missing data profiling:

```python
def profile_missing_data(df: pl.DataFrame) -> pl.DataFrame:
    """Profile missing data patterns."""
    missing_stats = []
    for col in df.columns:
        missing_stats.append({
            "column": col,
            "missing_count": df[col].null_count(),
            "missing_pct": df[col].null_count() / len(df) * 100,
            "dtype": str(df[col].dtype)
        })

    return pl.DataFrame(missing_stats).sort("missing_pct", descending=True)
```

### 5.2 Lack of Data Validation Pipeline

**Issue**: No systematic validation of input data assumptions.

**Recommendation**: Create validation pipeline:

```python
class DataValidator:
    """Validate FIA data assumptions."""

    @staticmethod
    def validate_tree_data(df: pl.DataFrame) -> dict:
        """Validate tree data meets FIA specifications."""
        issues = []

        # Check diameter ranges
        if (df["DIA"] < 0).any():
            issues.append("Negative diameter values found")

        # Check TPA_UNADJ ranges
        if (df["TPA_UNADJ"] <= 0).any():
            issues.append("Invalid TPA_UNADJ values")

        # Check required relationships
        live_dead = df.group_by("STATUSCD").count()
        if not set(live_dead["STATUSCD"].to_list()).issubset({1, 2}):
            issues.append("Invalid STATUSCD values")

        return {
            "valid": len(issues) == 0,
            "issues": issues,
            "n_trees": len(df),
            "n_plots": df["PLT_CN"].n_unique()
        }
```

## 6. Performance Optimization Opportunities

### 6.1 Query Optimization

**Issue**: Multiple passes over data for aggregation.

**Recommendation**: Combine operations in single pass:

```python
# Instead of multiple aggregations
def optimized_aggregation(df: pl.LazyFrame) -> pl.DataFrame:
    """Single-pass aggregation for multiple metrics."""
    return df.group_by(["PLT_CN", "CONDID"]).agg([
        # Volume metrics
        (pl.col("VOLCFNET") * pl.col("TPA_UNADJ")).sum().alias("VOL_NET"),
        (pl.col("VOLCFGRS") * pl.col("TPA_UNADJ")).sum().alias("VOL_GROSS"),

        # Biomass metrics
        (pl.col("DRYBIO_AG") * pl.col("TPA_UNADJ")).sum().alias("BIO_AG"),

        # Tree counts
        pl.col("TPA_UNADJ").sum().alias("TPA_TOTAL"),

        # Statistics for variance
        pl.count().alias("N_TREES")
    ]).collect()
```

### 6.2 Memory Optimization

**Recommendation**: Use appropriate data types to reduce memory:

```python
def optimize_dtypes(df: pl.LazyFrame) -> pl.LazyFrame:
    """Optimize data types for memory efficiency."""
    return df.with_columns([
        # Use smaller integer types where appropriate
        pl.col("STATUSCD").cast(pl.Int8),
        pl.col("SPCD").cast(pl.Int16),
        pl.col("STATECD").cast(pl.Int8),

        # Use Float32 for measurements with limited precision
        pl.col("DIA").cast(pl.Float32),
        pl.col("HT").cast(pl.Float32)
    ])
```

## 7. Critical Recommendations

### Immediate Actions Required:

1. **Replace placeholder variance calculations** with proper implementation from `statistics.py`
2. **Add comprehensive variance validation tests** against known FIA estimates
3. **Implement proper error propagation** for grouped estimates

### High Priority Improvements:

1. **Optimize LazyFrame usage** to avoid unnecessary materialization
2. **Add data validation pipeline** for input assumptions
3. **Implement proper missing data handling** with documentation

### Medium Priority Enhancements:

1. **Add property-based testing** for statistical invariants
2. **Implement outlier detection** for quality control
3. **Optimize memory usage** with appropriate dtypes

### Code Quality Improvements:

1. **Extract common statistical calculations** into reusable functions
2. **Add comprehensive edge case testing**
3. **Document statistical assumptions** in docstrings

## 8. Positive Aspects

The codebase has several strengths:

1. **Correct two-stage aggregation** implementation prevents critical estimation errors
2. **Clean architecture** after 85% code reduction
3. **Good separation of concerns** between estimation and data loading
4. **Proper handling of nested plot design** in tree expansion
5. **Well-tested critical paths** for basic estimation

## Conclusion

While the pyFIA package has a solid architectural foundation and correctly implements the critical two-stage aggregation methodology, **it cannot be considered production-ready for scientific use** due to the placeholder variance calculations. The immediate priority should be implementing proper variance estimation following Bechtold & Patterson (2005). Once this is addressed, the package would benefit from the performance optimizations and data validation improvements outlined above.

The codebase shows good software engineering practices with its simplified architecture, but needs more rigor in its statistical implementation to meet the standards expected for FIA data analysis.