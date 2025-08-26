# pyFIA Phase 4: High-Level Pipeline API

## Overview

The Phase 4 high-level API provides multiple ways to create and use estimation pipelines, from simple quick-start functions to advanced builders with full customization. This document describes the four main approaches for creating pipelines.

## 1. Quick Start Functions

The simplest way to create pipelines, matching the existing pyFIA API:

```python
from pyfia.estimation.pipeline import create_volume_pipeline

# Direct replacement for volume() function
pipeline = create_volume_pipeline(
    by_species=True,
    tree_domain="DIA >= 10.0",
    variance=True
)

# Execute with database
result = pipeline.execute(db)
```

### Available Quick Start Functions

- `create_volume_pipeline()` - Volume estimation
- `create_biomass_pipeline()` - Biomass estimation  
- `create_tpa_pipeline()` - Trees per acre
- `create_area_pipeline()` - Area estimation
- `create_growth_pipeline()` - Growth estimation
- `create_mortality_pipeline()` - Mortality estimation

### Convenience Functions

```python
# Quick calculations with common patterns
result = quick_volume(db, species_code=131, min_dbh=10.0)
carbon = quick_carbon_assessment(db, by_ownership=True)
inventory = quick_forest_inventory(db, metrics=["volume", "biomass", "tpa"])
```

## 2. Pipeline Builders (Fluent API)

Builders provide a fluent API for constructing pipelines with method chaining:

```python
from pyfia.estimation.pipeline import VolumeEstimationBuilder

builder = VolumeEstimationBuilder()
pipeline = (
    builder
    .with_species_grouping()
    .with_tree_domain("DIA >= 10.0")
    .with_parallel_execution()
    .skip_step("validate_output")
    .build()
)
```

### Builder Methods

#### Configuration Methods
- `.with_species_grouping()` - Enable species grouping
- `.with_size_class_grouping()` - Enable size class grouping
- `.with_grouping(columns)` - Add custom grouping columns
- `.with_tree_domain(filter)` - Add tree-level filter
- `.with_area_domain(filter)` - Add area-level filter
- `.with_plot_domain(filter)` - Add plot-level filter
- `.with_totals()` - Enable totals calculation
- `.without_variance()` - Disable variance calculation
- `.with_temporal_method(method)` - Set temporal method

#### Execution Methods
- `.with_parallel_execution()` - Use parallel execution
- `.with_adaptive_execution()` - Use adaptive execution
- `.without_caching()` - Disable caching
- `.with_debug()` - Enable debug mode

#### Customization Methods
- `.add_custom_step(step)` - Add custom processing step
- `.override_step(name, step)` - Replace default step
- `.skip_step(name)` - Skip a default step
- `.with_config(**kwargs)` - Add arbitrary configuration

### Estimation-Specific Builders

Each estimation type has specialized methods:

```python
# Biomass builder
BiomassEstimationBuilder()
    .with_component("total")  # Specify component
    .for_carbon_assessment()  # Carbon-specific setup

# Area builder  
AreaEstimationBuilder()
    .with_land_type_filter("forest")
    .by_ownership()

# Mortality builder
MortalityEstimationBuilder()
    .with_mortality_type("biomass")
    .by_disturbance_cause()
```

## 3. Pipeline Templates

Pre-configured templates for common use cases:

```python
from pyfia.estimation.pipeline import get_template, list_templates

# Get and use a template
template = get_template("volume_by_species")
pipeline = template.create_pipeline(
    tree_domain="DIA >= 15.0"  # Override defaults
)

# List available templates
templates = list_templates(category=TemplateCategory.SPECIES)
```

### Template Categories

#### Basic Templates
- `basic_volume` - Simple volume estimation
- `basic_biomass` - Simple biomass estimation
- `basic_tpa` - Simple TPA estimation
- `basic_area` - Simple area estimation

#### Species Templates
- `volume_by_species` - Volume grouped by species
- `biomass_by_species` - Biomass grouped by species
- `tpa_by_species` - TPA grouped by species
- `mortality_by_species` - Mortality grouped by species

#### Ownership Templates
- `volume_by_ownership` - Volume by ownership class
- `area_by_ownership` - Area by ownership class

#### Temporal Templates
- `annual_volume_trend` - Volume trends over time
- `growth_trend` - Growth estimation over time
- `mortality_trend` - Mortality trends

#### Custom Domain Templates
- `large_tree_volume` - Volume of large trees (DBH >= 20")
- `hardwood_biomass` - Biomass of hardwood species
- `pine_mortality` - Mortality in pine species

#### Advanced Templates
- `comprehensive_forest_inventory` - Complete inventory
- `carbon_assessment` - Carbon stock assessment
- `disturbance_impact` - Disturbance impact analysis

### Template Customization

```python
from pyfia.estimation.pipeline import TemplateCustomizer

# Customize an existing template
template = get_template("basic_volume")
custom = TemplateCustomizer.add_custom_domain(
    template,
    tree_domain="SPGRPCD == 2",  # Hardwoods only
    area_domain="OWNGRPCD == 10"  # National forest
)
```

## 4. Pipeline Factory

The factory provides centralized pipeline creation with validation and optimization:

```python
from pyfia.estimation.pipeline import EstimationPipelineFactory

# Create by type
pipeline = EstimationPipelineFactory.create_pipeline(
    "volume",
    by_species=True,
    tree_domain="DIA >= 10.0"
)

# Create from configuration
config = PipelineConfig(
    estimation_type=EstimationType.BIOMASS,
    by_species=True,
    module_config={"component": "total"}
)
pipeline = EstimationPipelineFactory.create_from_config(config)

# Auto-detect type from parameters
pipeline = EstimationPipelineFactory.auto_detect_pipeline({
    "component": "aboveground",  # Indicates biomass
    "by_species": True
})
```

### Configuration Management

```python
# Save configuration
config.to_json("pipeline_config.json")

# Load and create from saved config
pipeline = EstimationPipelineFactory.create_from_config("pipeline_config.json")

# Validate configuration
issues = EstimationPipelineFactory.validate_pipeline_config(config)

# Optimize configuration
optimized = PipelineOptimizer.optimize_config(config)
suggestions = PipelineOptimizer.suggest_optimizations(config)
```

## Migration from Existing API

For users migrating from the traditional pyFIA API:

```python
# Old API
from pyfia import volume
result = volume(db, bySpecies=True, treeDomain="DIA >= 10.0")

# New Pipeline API - Direct replacement
pipeline = create_volume_pipeline(
    by_species=True,
    tree_domain="DIA >= 10.0"
)
result = pipeline.execute(db)

# Migration helper
pipeline = migrate_to_pipeline(
    "volume",
    bySpecies=True,  # Old parameter names work
    treeDomain="DIA >= 10.0"
)
```

## Choosing the Right Approach

### Use Quick Start Functions When:
- You need a direct replacement for existing code
- You want the simplest API
- Default behavior is acceptable
- You're prototyping or exploring data

### Use Builders When:
- You need fine-grained control
- You want to customize multiple aspects
- You're building reusable pipeline configurations
- You need to add custom steps

### Use Templates When:
- You have standard analysis patterns
- You want consistent configurations across projects
- You're implementing organizational standards
- You need pre-validated configurations

### Use Factory When:
- You're building dynamic pipelines
- You need configuration management
- You want validation and optimization
- You're integrating with other systems

## Performance Optimization

The pipeline system automatically optimizes execution:

```python
# Automatic optimization based on configuration
pipeline = VolumeEstimationBuilder()
    .with_species_grouping()
    .with_size_class_grouping()
    .with_grouping(["OWNGRPCD"])  # Multiple groupings
    .build()
# Automatically uses parallel execution

# Get optimization suggestions
suggestions = PipelineOptimizer.suggest_optimizations(config)

# Apply optimizations
optimized = PipelineOptimizer.optimize_config(config)
```

## Advanced Features

### Custom Steps

```python
from pyfia.estimation.pipeline import CustomStep

def custom_calculation(data: pl.LazyFrame, context: dict) -> pl.LazyFrame:
    return data.with_columns([
        (pl.col("VOLUME") * 0.5).alias("HALF_VOLUME")
    ])

custom_step = CustomStep(
    func=custom_calculation,
    step_id="half_volume",
    description="Calculate half volume"
)

builder.add_custom_step(custom_step)
```

### Conditional Execution

```python
from pyfia.estimation.pipeline import ConditionalStep

variance_step = CalculateVarianceStep()
conditional = ConditionalStep(
    condition=lambda config: config.variance,
    step=variance_step
)
```

### Pipeline Composition

```python
# Combine multiple pipelines
volume_pipeline = create_volume_pipeline(by_species=True)
biomass_pipeline = create_biomass_pipeline(by_species=True)

# Run in sequence
volume_result = volume_pipeline.execute(db)
biomass_result = biomass_pipeline.execute(db)

# Or create a composite pipeline
composite = EstimationPipeline()
composite.add_steps(volume_pipeline.steps)
composite.add_steps(biomass_pipeline.steps)
```

## Best Practices

1. **Start Simple**: Use quick start functions for initial development
2. **Graduate to Builders**: Move to builders when you need customization
3. **Create Templates**: Standardize common patterns as templates
4. **Validate Configurations**: Always validate before production use
5. **Monitor Performance**: Use pipeline monitoring for optimization
6. **Test Thoroughly**: Use pipeline testing utilities for validation
7. **Document Custom Steps**: Clearly document any custom processing

## Summary

The Phase 4 high-level API provides a complete toolkit for creating FIA estimation pipelines:

- **Quick Start**: Simple functions matching existing API
- **Builders**: Fluent API for detailed configuration
- **Templates**: Pre-configured patterns for common use cases
- **Factory**: Centralized creation with validation and optimization

This flexible architecture supports everything from simple one-line pipeline creation to complex, highly customized estimation workflows, while maintaining backward compatibility and ensuring statistical accuracy.