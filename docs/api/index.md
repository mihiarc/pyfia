# API Reference

Complete reference documentation for PyFIA's public API.

## Core Classes

| Class | Description |
|-------|-------------|
| [`FIA`](fia.md) | Main database class for working with FIA data |
| [`FIADataReader`](data_reader.md) | Low-level data reading utilities |
| [`PyFIASettings`](settings.md) | Configuration management |

## Estimation Functions

All estimation functions follow a consistent pattern:

```python
result = estimator(db, **options)  # Returns pl.DataFrame
```

| Function | Description |
|----------|-------------|
| [`area()`](area.md) | Estimate forest area |
| [`area_change()`](area_change.md) | Annual forest area change |
| [`volume()`](volume.md) | Estimate tree volume |
| [`tpa()`](tpa.md) | Trees per acre and basal area |
| [`biomass()`](biomass.md) | Tree biomass and carbon |
| [`mortality()`](mortality.md) | Annual tree mortality |
| [`growth()`](growth.md) | Annual tree growth |
| [`removals()`](removals.md) | Timber removals |

## Data Download

| Function | Description |
|----------|-------------|
| [`download()`](downloader.md) | Download FIA data from DataMart |
| [`DataMartClient`](downloader.md) | Low-level HTTP client for downloads |
| [`DownloadCache`](downloader.md) | Cache management for downloads |

## Utility Functions

| Function | Description |
|----------|-------------|
| [`join_species_names()`](reference_tables.md#join_species_names) | Add species names to results |
| [`join_forest_type_names()`](reference_tables.md#join_forest_type_names) | Add forest type names |
| [`join_state_names()`](reference_tables.md#join_state_names) | Add state names |

## Validation

| Class/Function | Description |
|----------------|-------------|
| [`EVALIDatorClient`](evalidator.md) | Client for USFS EVALIDator API |
| [`validate_pyfia_estimate()`](evalidator.md#validate_pyfia_estimate) | Compare estimates |

## Return Value Schema

All estimation functions return a `polars.DataFrame` with these columns:

| Column | Type | Description |
|--------|------|-------------|
| `estimate` | Float | Point estimate |
| `variance` | Float | Estimated variance |
| `se` | Float | Standard error |
| `cv` | Float | Coefficient of variation (%) |
| `ci_lower` | Float | Lower 95% confidence bound |
| `ci_upper` | Float | Upper 95% confidence bound |
| `n_plots` | Int | Number of plots in domain |

Plus any grouping columns specified via `grp_by`.
