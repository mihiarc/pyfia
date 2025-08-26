"""
Example usage of pyFIA Phase 4 Pipeline Framework.

This module demonstrates the various ways to create and use pipelines
with the new builders, templates, and factory system.
"""

import polars as pl
from pathlib import Path

from pyfia import FIA
from pyfia.estimation.pipeline import (
    # Builders
    VolumeEstimationBuilder,
    BiomassEstimationBuilder,
    
    # Templates
    get_template,
    list_templates,
    TemplateCategory,
    
    # Quick start
    create_volume_pipeline,
    create_biomass_pipeline,
    quick_volume,
    quick_carbon_assessment,
    quick_forest_inventory,
    migrate_to_pipeline,
    
    # Factory
    EstimationPipelineFactory,
    PipelineConfig,
    EstimationType,
    auto_detect_pipeline,
    
    # Core
    ExecutionMode,
)


def example_basic_builder_usage():
    """Example: Using pipeline builders with fluent API."""
    print("=== Basic Builder Usage ===\n")
    
    # Create a volume estimation pipeline using fluent API
    builder = VolumeEstimationBuilder()
    
    pipeline = (
        builder
        .with_species_grouping()
        .with_size_class_grouping()
        .with_tree_domain("DIA >= 10.0 AND STATUSCD == 1")
        .with_totals()
        .with_parallel_execution()
        .build()
    )
    
    print(f"Created pipeline: {pipeline.pipeline_id}")
    print(f"Execution mode: {pipeline.execution_mode}")
    print(f"Number of steps: {len(pipeline.steps)}")
    
    # Alternative: Build with parameters
    pipeline2 = VolumeEstimationBuilder().build(
        by_species=True,
        tree_domain="DIA >= 20.0",
        variance=True,
        totals=True
    )
    
    return pipeline, pipeline2


def example_advanced_builder_customization():
    """Example: Advanced builder customization."""
    print("\n=== Advanced Builder Customization ===\n")
    
    # Create biomass builder with custom steps
    builder = BiomassEstimationBuilder()
    
    # Customize the pipeline
    pipeline = (
        builder
        .with_component("total")  # Total biomass
        .for_carbon_assessment()   # Configure for carbon
        .with_species_grouping()
        .with_grouping(["OWNGRPCD", "FORTYPCD"])  # Additional grouping
        .skip_step("validate_output")  # Skip validation
        .with_debug()  # Enable debug mode
        .build()
    )
    
    print(f"Pipeline configured for carbon assessment")
    print(f"Debug mode: {pipeline.debug}")
    
    # Add custom step after building
    from pyfia.estimation.pipeline import CustomStep
    
    def carbon_conversion(data: pl.LazyFrame, context: dict) -> pl.LazyFrame:
        """Convert biomass to carbon (multiply by 0.5)."""
        return data.with_columns([
            (pl.col("BIOMASS_ESTIMATE") * 0.5).alias("CARBON_ESTIMATE")
        ])
    
    custom_step = CustomStep(
        func=carbon_conversion,
        step_id="carbon_conversion",
        description="Convert biomass to carbon"
    )
    
    builder.add_custom_step(custom_step)
    
    return pipeline


def example_template_usage():
    """Example: Using pre-configured templates."""
    print("\n=== Template Usage ===\n")
    
    # List available templates
    print("Available templates by category:\n")
    
    for category in TemplateCategory:
        templates = list_templates(category=category)
        print(f"{category.value.upper()}:")
        for template in templates:
            print(f"  - {template.name}: {template.description}")
    
    print("\n")
    
    # Use a specific template
    template = get_template("volume_by_species")
    pipeline = template.create_pipeline(
        tree_domain="DIA >= 15.0",  # Override default
        variance=True
    )
    
    print(f"Created pipeline from template: {template.name}")
    print(f"Template category: {template.category.value}")
    
    # Use advanced template
    carbon_template = get_template("carbon_assessment")
    carbon_pipeline = carbon_template.create_pipeline()
    
    print(f"Created carbon assessment pipeline")
    
    return pipeline, carbon_pipeline


def example_quick_start_functions():
    """Example: Quick start functions for easy migration."""
    print("\n=== Quick Start Functions ===\n")
    
    # Direct replacement for existing API
    pipeline = create_volume_pipeline(
        by_species=True,
        tree_domain="DIA >= 10.0",
        variance=True,
        totals=True
    )
    
    print("Created volume pipeline with quick start function")
    
    # With database context
    # db = FIA("path/to/fia.db")
    # result = create_volume_pipeline(db, by_species=True).execute()
    
    # Quick convenience functions (assuming db is available)
    # result = quick_volume(db, species_code=131, min_dbh=10.0)
    # carbon = quick_carbon_assessment(db, by_ownership=True)
    
    # Comprehensive inventory
    # inventory = quick_forest_inventory(
    #     db,
    #     metrics=["volume", "biomass", "tpa", "area"],
    #     by_species=True
    # )
    
    return pipeline


def example_factory_usage():
    """Example: Using the pipeline factory."""
    print("\n=== Pipeline Factory ===\n")
    
    # Create pipeline by type
    pipeline = EstimationPipelineFactory.create_pipeline(
        "volume",
        by_species=True,
        tree_domain="SPGRPCD == 1"  # Softwoods
    )
    
    print("Created pipeline using factory")
    
    # Create from configuration object
    config = PipelineConfig(
        estimation_type=EstimationType.BIOMASS,
        by_species=True,
        by_size_class=True,
        module_config={"component": "aboveground"},
        execution_mode=ExecutionMode.PARALLEL
    )
    
    pipeline2 = EstimationPipelineFactory.create_from_config(config)
    
    print(f"Created pipeline from config: {config.estimation_type.value}")
    
    # Save configuration for reuse
    config_path = Path("pipeline_config.json")
    config.to_json(config_path)
    print(f"Saved configuration to {config_path}")
    
    # Load and create from saved config
    pipeline3 = EstimationPipelineFactory.create_from_config(config_path)
    
    # Auto-detect pipeline type
    params = {
        "component": "total",  # Indicates biomass
        "by_species": True,
        "variance": True
    }
    
    auto_pipeline = EstimationPipelineFactory.auto_detect_pipeline(params)
    print(f"Auto-detected pipeline type: biomass")
    
    # Validate configuration
    issues = EstimationPipelineFactory.validate_pipeline_config(config)
    if not issues:
        print("Configuration is valid")
    
    # Clean up
    if config_path.exists():
        config_path.unlink()
    
    return pipeline, pipeline2, auto_pipeline


def example_migration_from_old_api():
    """Example: Migrating from old API to pipeline."""
    print("\n=== Migration from Old API ===\n")
    
    # Old API style (for comparison)
    # from pyfia import volume
    # result = volume(db, bySpecies=True, treeDomain="DIA >= 10.0")
    
    # New pipeline API - direct replacement
    pipeline = migrate_to_pipeline(
        "volume",
        bySpecies=True,
        treeDomain="DIA >= 10.0",
        variance=True
    )
    
    print("Migrated old API call to pipeline")
    print(f"Pipeline has {len(pipeline.steps)} steps")
    
    # The pipeline can now be customized further
    pipeline.skip_step("validate_output")
    
    return pipeline


def example_optimization_suggestions():
    """Example: Getting optimization suggestions."""
    print("\n=== Optimization Suggestions ===\n")
    
    from pyfia.estimation.pipeline.factory import PipelineOptimizer
    
    # Create a configuration
    config = {
        "estimation_type": "volume",
        "by_species": True,
        "by_size_class": True,
        "grp_by": ["OWNGRPCD", "FORTYPCD"],
        "execution_mode": "SEQUENTIAL",
        "enable_caching": False
    }
    
    # Get optimization suggestions
    suggestions = PipelineOptimizer.suggest_optimizations(config)
    
    print("Optimization suggestions:")
    for suggestion in suggestions:
        print(f"  - {suggestion}")
    
    # Apply optimizations
    optimized_config = PipelineOptimizer.optimize_config(config)
    
    print(f"\nOptimized configuration:")
    print(f"  Execution mode: {optimized_config.execution_mode}")
    print(f"  Caching enabled: {optimized_config.enable_caching}")
    
    return optimized_config


def example_complete_workflow():
    """Example: Complete workflow from data to results."""
    print("\n=== Complete Workflow Example ===\n")
    
    # This would work with a real database
    # db = FIA("/path/to/fia.duckdb")
    # db.clip_by_state(37)  # North Carolina
    
    # Method 1: Builder with fluent API
    builder = VolumeEstimationBuilder()
    pipeline = (
        builder
        .with_species_grouping()
        .with_tree_domain("DIA >= 10.0 AND STATUSCD == 1")
        .with_totals()
        .with_parallel_execution()
        .build()
    )
    
    # Method 2: Template
    template_pipeline = get_template("volume_by_species").create_pipeline(
        tree_domain="DIA >= 10.0 AND STATUSCD == 1"
    )
    
    # Method 3: Quick start
    quick_pipeline = create_volume_pipeline(
        by_species=True,
        tree_domain="DIA >= 10.0 AND STATUSCD == 1",
        totals=True,
        execution_mode=ExecutionMode.PARALLEL
    )
    
    # Method 4: Factory with auto-detection
    auto_pipeline = auto_detect_pipeline({
        "by_species": True,
        "tree_domain": "DIA >= 10.0 AND STATUSCD == 1",
        "totals": True,
        "variance": True
    })
    
    # All methods produce equivalent pipelines
    print("Created 4 equivalent pipelines using different methods")
    
    # Execute pipeline (would work with real database)
    # result = pipeline.execute(db)
    # print(f"Results shape: {result.shape}")
    # print(f"Columns: {result.columns}")
    
    return pipeline, template_pipeline, quick_pipeline, auto_pipeline


def main():
    """Run all examples."""
    print("=" * 60)
    print("pyFIA Phase 4 Pipeline Framework Examples")
    print("=" * 60)
    
    # Run examples
    example_basic_builder_usage()
    example_advanced_builder_customization()
    example_template_usage()
    example_quick_start_functions()
    example_factory_usage()
    example_migration_from_old_api()
    example_optimization_suggestions()
    example_complete_workflow()
    
    print("\n" + "=" * 60)
    print("Examples completed successfully!")
    print("=" * 60)


if __name__ == "__main__":
    main()