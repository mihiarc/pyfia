# Property-Based Testing Guide for pyFIA

## Overview

Property-based testing with Hypothesis helps us verify that our code satisfies certain properties across a wide range of inputs, not just specific test cases.

## What is Property-Based Testing?

Instead of writing:
```python
def test_specific_case():
    assert calculate_area([1, 2, 3]) == 6
```

We write:
```python
@given(values=st.lists(st.floats(min_value=0)))
def test_area_always_positive(values):
    assert calculate_area(values) >= 0
```

Hypothesis generates hundreds of test cases automatically!

## Key Properties We Test

### 1. **Mathematical Invariants**
- Variance is always non-negative
- CV = (SE / Estimate) × 100
- Proportions sum to ≤ 1
- Ratios preserve ordering

### 2. **Domain Constraints**
- Forest area ≤ Total area
- Tree counts are non-negative
- DBH values are positive
- Plot counts match expected ranges

### 3. **Statistical Properties**
- Estimates are unbiased
- Variance formulas are correct
- Stratification reduces variance
- Confidence intervals contain true values

### 4. **Data Integrity**
- Joins don't increase row counts
- Filters reduce or maintain counts
- Grouping preserves totals
- Missing data is handled correctly

## Running Property Tests

### Basic Usage
```bash
# Run all property tests
uv run pytest tests/test_property_based.py -v

# Run with more examples (slower but more thorough)
uv run pytest tests/test_property_based.py --hypothesis-profile=ci

# Run specific test
uv run pytest tests/test_property_based.py::TestEstimationProperties::test_variance_non_negative -v
```

### Hypothesis Profiles
- `dev`: 10 examples (fast, for development)
- `ci`: 100 examples (for continuous integration)
- `nightly`: 1000 examples (thorough testing)

### Debugging Failures
When a test fails, Hypothesis provides:
1. The minimal failing example
2. Steps to reproduce
3. Shrunk input that still fails

Example:
```
Falsifying example: test_variance_non_negative(
    n_plots=1,
    values=[0.0],
)
```

## Writing New Property Tests

### 1. Identify Properties
Ask: "What should always be true?"
- Output constraints (non-negative, bounded)
- Relationships (X ≤ Y, sum = total)
- Invariants (formulas, conservation laws)

### 2. Create Custom Strategies
```python
@st.composite
def plot_data_strategy(draw):
    """Generate realistic plot data."""
    n_plots = draw(st.integers(min_value=1, max_value=100))
    return pl.DataFrame({
        "PLT_CN": [f"P{i:04d}" for i in range(n_plots)],
        "INVYR": draw(st.lists(
            st.integers(2010, 2025),
            min_size=n_plots,
            max_size=n_plots
        ))
    })
```

### 3. Write Property Tests
```python
@given(data=plot_data_strategy())
def test_property(data):
    result = process_data(data)
    # Assert property holds
    assert property_check(result)
```

### 4. Handle Edge Cases
```python
@given(values=st.lists(st.floats()))
def test_with_edge_cases(values):
    assume(len(values) > 0)  # Skip empty lists
    assume(not any(math.isnan(v) for v in values))  # Skip NaN

    result = calculate(values)
    assert result >= 0
```

## Common Patterns

### Testing Numerical Stability
```python
@given(
    small=st.floats(min_value=1e-10, max_value=1e-5),
    large=st.floats(min_value=1e5, max_value=1e10)
)
def test_numerical_stability(small, large):
    # Should handle extreme values
    result = calculate_ratio(large, small)
    assert not math.isnan(result)
    assert not math.isinf(result)
```

### Testing Transformations
```python
@given(df=dataframe_strategy())
def test_transformation_preserves_property(df):
    original_sum = df["value"].sum()
    transformed = apply_transformation(df)
    # Transformation should preserve sum
    assert abs(transformed["value"].sum() - original_sum) < 1e-10
```

### Testing Estimators
```python
@given(
    true_value=st.floats(min_value=0, max_value=1000),
    n_samples=st.integers(min_value=10, max_value=1000)
)
def test_estimator_unbiased(true_value, n_samples):
    estimates = []
    for _ in range(100):
        sample = generate_sample(true_value, n_samples)
        estimates.append(calculate_estimate(sample))

    # Mean of estimates should be close to true value
    assert abs(np.mean(estimates) - true_value) < true_value * 0.1
```

## Best Practices

1. **Start Simple**: Test obvious properties first
2. **Use Realistic Data**: Create domain-specific strategies
3. **Test Relationships**: Not just individual values
4. **Consider Performance**: Use `@settings(deadline=...)` for slow tests
5. **Document Properties**: Explain why property should hold

## Integration with CI/CD

```yaml
# .github/workflows/test.yml
- name: Run property tests
  run: |
    uv run pytest tests/test_property_based.py \
      --hypothesis-profile=ci \
      --hypothesis-show-statistics
```

## Resources

- [Hypothesis Documentation](https://hypothesis.readthedocs.io/)
- [Property-Based Testing Guide](https://hypothesis.works/articles/what-is-property-based-testing/)
- [Hypothesis Examples](https://github.com/HypothesisWorks/hypothesis/tree/master/hypothesis-python/examples)