"""
Quick start functions for creating estimation pipelines.

This module provides simple functions that match the existing pyFIA API
while leveraging the pipeline framework. These functions offer drop-in
replacements for the traditional estimation functions.
"""

from typing import Optional, List, Union, Any
import polars as pl

from ...core import FIA
from ..config import EstimatorConfig
from .core import EstimationPipeline, ExecutionMode
from .builders import (
    VolumeEstimationBuilder,
    BiomassEstimationBuilder,
    TPAEstimationBuilder,
    AreaEstimationBuilder,
    GrowthEstimationBuilder,
    MortalityEstimationBuilder,
)
from .templates import get_template


def create_volume_pipeline(
    db: Optional[FIA] = None,
    by_species: bool = False,
    by_size_class: bool = False,
    grp_by: Optional[List[str]] = None,
    tree_domain: Optional[str] = None,
    area_domain: Optional[str] = None,
    plot_domain: Optional[str] = None,
    totals: bool = False,
    variance: bool = True,
    temporal_method: str = "TI",
    execution_mode: ExecutionMode = ExecutionMode.SEQUENTIAL,
    enable_caching: bool = True,
    **kwargs
) -> EstimationPipeline:
    """
    Create a volume estimation pipeline.
    
    This function provides a quick way to create volume pipelines that
    match the existing `volume()` function API.
    
    Parameters
    ----------
    db : Optional[FIA]
        FIA database connection (can be provided at execution)
    by_species : bool
        Whether to group by species
    by_size_class : bool
        Whether to group by size class
    grp_by : Optional[List[str]]
        Additional grouping columns
    tree_domain : Optional[str]
        Tree-level domain filter
    area_domain : Optional[str]
        Area-level domain filter
    plot_domain : Optional[str]
        Plot-level domain filter
    totals : bool
        Whether to calculate totals
    variance : bool
        Whether to calculate variance
    temporal_method : str
        Temporal estimation method
    execution_mode : ExecutionMode
        Pipeline execution mode
    enable_caching : bool
        Whether to enable caching
    **kwargs
        Additional configuration parameters
        
    Returns
    -------
    EstimationPipeline
        Configured volume estimation pipeline
        
    Examples
    --------
    >>> # Create a simple volume pipeline
    >>> pipeline = create_volume_pipeline(by_species=True)
    >>> result = pipeline.execute(db)
    
    >>> # Create with custom domain
    >>> pipeline = create_volume_pipeline(
    ...     tree_domain="DIA >= 10.0",
    ...     by_species=True
    ... )
    
    >>> # Direct execution with database
    >>> result = create_volume_pipeline(db, by_species=True).execute()
    """
    config = {
        "by_species": by_species,
        "by_size_class": by_size_class,
        "grp_by": grp_by,
        "tree_domain": tree_domain,
        "area_domain": area_domain,
        "plot_domain": plot_domain,
        "totals": totals,
        "variance": variance,
        "temporal_method": temporal_method,
        **kwargs
    }
    
    builder = VolumeEstimationBuilder(
        execution_mode=execution_mode,
        enable_caching=enable_caching
    )
    
    pipeline = builder.build(**config)
    
    # If database provided, set it as initial context
    if db:
        pipeline.context["database"] = db
    
    return pipeline


def create_biomass_pipeline(
    db: Optional[FIA] = None,
    component: str = "aboveground",
    by_species: bool = False,
    by_size_class: bool = False,
    grp_by: Optional[List[str]] = None,
    tree_domain: Optional[str] = None,
    area_domain: Optional[str] = None,
    plot_domain: Optional[str] = None,
    totals: bool = False,
    variance: bool = True,
    temporal_method: str = "TI",
    execution_mode: ExecutionMode = ExecutionMode.SEQUENTIAL,
    enable_caching: bool = True,
    **kwargs
) -> EstimationPipeline:
    """
    Create a biomass estimation pipeline.
    
    This function provides a quick way to create biomass pipelines that
    match the existing `biomass()` function API.
    
    Parameters
    ----------
    db : Optional[FIA]
        FIA database connection (can be provided at execution)
    component : str
        Biomass component (aboveground, belowground, total)
    by_species : bool
        Whether to group by species
    by_size_class : bool
        Whether to group by size class
    grp_by : Optional[List[str]]
        Additional grouping columns
    tree_domain : Optional[str]
        Tree-level domain filter
    area_domain : Optional[str]
        Area-level domain filter
    plot_domain : Optional[str]
        Plot-level domain filter
    totals : bool
        Whether to calculate totals
    variance : bool
        Whether to calculate variance
    temporal_method : str
        Temporal estimation method
    execution_mode : ExecutionMode
        Pipeline execution mode
    enable_caching : bool
        Whether to enable caching
    **kwargs
        Additional configuration parameters
        
    Returns
    -------
    EstimationPipeline
        Configured biomass estimation pipeline
        
    Examples
    --------
    >>> # Create a biomass pipeline for aboveground component
    >>> pipeline = create_biomass_pipeline(
    ...     component="aboveground",
    ...     by_species=True
    ... )
    
    >>> # Total biomass with custom domain
    >>> pipeline = create_biomass_pipeline(
    ...     component="total",
    ...     tree_domain="SPGRPCD == 2"  # Hardwoods
    ... )
    """
    config = {
        "module_config": {"component": component},
        "by_species": by_species,
        "by_size_class": by_size_class,
        "grp_by": grp_by,
        "tree_domain": tree_domain,
        "area_domain": area_domain,
        "plot_domain": plot_domain,
        "totals": totals,
        "variance": variance,
        "temporal_method": temporal_method,
        **kwargs
    }
    
    builder = BiomassEstimationBuilder(
        execution_mode=execution_mode,
        enable_caching=enable_caching
    )
    
    pipeline = builder.build(**config)
    
    if db:
        pipeline.context["database"] = db
    
    return pipeline


def create_tpa_pipeline(
    db: Optional[FIA] = None,
    by_species: bool = False,
    by_size_class: bool = False,
    grp_by: Optional[List[str]] = None,
    tree_domain: Optional[str] = None,
    area_domain: Optional[str] = None,
    plot_domain: Optional[str] = None,
    totals: bool = False,
    variance: bool = True,
    temporal_method: str = "TI",
    execution_mode: ExecutionMode = ExecutionMode.SEQUENTIAL,
    enable_caching: bool = True,
    **kwargs
) -> EstimationPipeline:
    """
    Create a trees per acre (TPA) estimation pipeline.
    
    This function provides a quick way to create TPA pipelines that
    match the existing `tpa()` function API.
    
    Parameters
    ----------
    db : Optional[FIA]
        FIA database connection (can be provided at execution)
    by_species : bool
        Whether to group by species
    by_size_class : bool
        Whether to group by size class
    grp_by : Optional[List[str]]
        Additional grouping columns
    tree_domain : Optional[str]
        Tree-level domain filter
    area_domain : Optional[str]
        Area-level domain filter
    plot_domain : Optional[str]
        Plot-level domain filter
    totals : bool
        Whether to calculate totals
    variance : bool
        Whether to calculate variance
    temporal_method : str
        Temporal estimation method
    execution_mode : ExecutionMode
        Pipeline execution mode
    enable_caching : bool
        Whether to enable caching
    **kwargs
        Additional configuration parameters
        
    Returns
    -------
    EstimationPipeline
        Configured TPA estimation pipeline
        
    Examples
    --------
    >>> # Create a TPA pipeline grouped by species
    >>> pipeline = create_tpa_pipeline(by_species=True)
    
    >>> # TPA for large trees only
    >>> pipeline = create_tpa_pipeline(
    ...     tree_domain="DIA >= 20.0",
    ...     by_size_class=True
    ... )
    """
    config = {
        "by_species": by_species,
        "by_size_class": by_size_class,
        "grp_by": grp_by,
        "tree_domain": tree_domain,
        "area_domain": area_domain,
        "plot_domain": plot_domain,
        "totals": totals,
        "variance": variance,
        "temporal_method": temporal_method,
        **kwargs
    }
    
    builder = TPAEstimationBuilder(
        execution_mode=execution_mode,
        enable_caching=enable_caching
    )
    
    pipeline = builder.build(**config)
    
    if db:
        pipeline.context["database"] = db
    
    return pipeline


def create_area_pipeline(
    db: Optional[FIA] = None,
    land_type: str = "forest",
    grp_by: Optional[List[str]] = None,
    area_domain: Optional[str] = None,
    plot_domain: Optional[str] = None,
    totals: bool = False,
    variance: bool = True,
    temporal_method: str = "TI",
    execution_mode: ExecutionMode = ExecutionMode.SEQUENTIAL,
    enable_caching: bool = True,
    **kwargs
) -> EstimationPipeline:
    """
    Create an area estimation pipeline.
    
    This function provides a quick way to create area pipelines that
    match the existing `area()` function API.
    
    Parameters
    ----------
    db : Optional[FIA]
        FIA database connection (can be provided at execution)
    land_type : str
        Land type to estimate (forest, timber, etc.)
    grp_by : Optional[List[str]]
        Grouping columns
    area_domain : Optional[str]
        Area-level domain filter
    plot_domain : Optional[str]
        Plot-level domain filter
    totals : bool
        Whether to calculate totals
    variance : bool
        Whether to calculate variance
    temporal_method : str
        Temporal estimation method
    execution_mode : ExecutionMode
        Pipeline execution mode
    enable_caching : bool
        Whether to enable caching
    **kwargs
        Additional configuration parameters
        
    Returns
    -------
    EstimationPipeline
        Configured area estimation pipeline
        
    Examples
    --------
    >>> # Create area pipeline for forest land
    >>> pipeline = create_area_pipeline(land_type="forest")
    
    >>> # Area by ownership
    >>> pipeline = create_area_pipeline(
    ...     land_type="timber",
    ...     grp_by=["OWNGRPCD"]
    ... )
    """
    config = {
        "land_type": land_type,
        "grp_by": grp_by,
        "area_domain": area_domain,
        "plot_domain": plot_domain,
        "totals": totals,
        "variance": variance,
        "temporal_method": temporal_method,
        **kwargs
    }
    
    builder = AreaEstimationBuilder(
        execution_mode=execution_mode,
        enable_caching=enable_caching
    )
    
    pipeline = builder.build(**config)
    
    if db:
        pipeline.context["database"] = db
    
    return pipeline


def create_growth_pipeline(
    db: Optional[FIA] = None,
    growth_type: str = "net",
    by_species: bool = False,
    by_size_class: bool = False,
    grp_by: Optional[List[str]] = None,
    tree_domain: Optional[str] = None,
    area_domain: Optional[str] = None,
    plot_domain: Optional[str] = None,
    totals: bool = False,
    variance: bool = True,
    temporal_method: str = "annual",
    execution_mode: ExecutionMode = ExecutionMode.SEQUENTIAL,
    enable_caching: bool = True,
    **kwargs
) -> EstimationPipeline:
    """
    Create a growth estimation pipeline.
    
    This function provides a quick way to create growth pipelines that
    match the existing `growth()` function API.
    
    Parameters
    ----------
    db : Optional[FIA]
        FIA database connection (can be provided at execution)
    growth_type : str
        Type of growth (net, gross)
    by_species : bool
        Whether to group by species
    by_size_class : bool
        Whether to group by size class
    grp_by : Optional[List[str]]
        Additional grouping columns
    tree_domain : Optional[str]
        Tree-level domain filter
    area_domain : Optional[str]
        Area-level domain filter
    plot_domain : Optional[str]
        Plot-level domain filter
    totals : bool
        Whether to calculate totals
    variance : bool
        Whether to calculate variance
    temporal_method : str
        Temporal estimation method (default: annual)
    execution_mode : ExecutionMode
        Pipeline execution mode
    enable_caching : bool
        Whether to enable caching
    **kwargs
        Additional configuration parameters
        
    Returns
    -------
    EstimationPipeline
        Configured growth estimation pipeline
        
    Examples
    --------
    >>> # Create net growth pipeline
    >>> pipeline = create_growth_pipeline(
    ...     growth_type="net",
    ...     by_species=True
    ... )
    
    >>> # Gross growth with temporal analysis
    >>> pipeline = create_growth_pipeline(
    ...     growth_type="gross",
    ...     temporal_method="annual",
    ...     grp_by=["INVYR"]
    ... )
    """
    config = {
        "module_config": {"growth_type": growth_type},
        "by_species": by_species,
        "by_size_class": by_size_class,
        "grp_by": grp_by,
        "tree_domain": tree_domain,
        "area_domain": area_domain,
        "plot_domain": plot_domain,
        "totals": totals,
        "variance": variance,
        "temporal_method": temporal_method,
        **kwargs
    }
    
    builder = GrowthEstimationBuilder(
        execution_mode=execution_mode,
        enable_caching=enable_caching
    )
    
    pipeline = builder.build(**config)
    
    if db:
        pipeline.context["database"] = db
    
    return pipeline


def create_mortality_pipeline(
    db: Optional[FIA] = None,
    mortality_type: str = "volume",
    by_species: bool = False,
    by_size_class: bool = False,
    grp_by: Optional[List[str]] = None,
    tree_domain: Optional[str] = None,
    area_domain: Optional[str] = None,
    plot_domain: Optional[str] = None,
    totals: bool = False,
    variance: bool = True,
    temporal_method: str = "TI",
    execution_mode: ExecutionMode = ExecutionMode.SEQUENTIAL,
    enable_caching: bool = True,
    **kwargs
) -> EstimationPipeline:
    """
    Create a mortality estimation pipeline.
    
    This function provides a quick way to create mortality pipelines that
    match the existing `mortality()` function API.
    
    Parameters
    ----------
    db : Optional[FIA]
        FIA database connection (can be provided at execution)
    mortality_type : str
        Type of mortality metric (volume, biomass, tpa)
    by_species : bool
        Whether to group by species
    by_size_class : bool
        Whether to group by size class
    grp_by : Optional[List[str]]
        Additional grouping columns
    tree_domain : Optional[str]
        Tree-level domain filter
    area_domain : Optional[str]
        Area-level domain filter
    plot_domain : Optional[str]
        Plot-level domain filter
    totals : bool
        Whether to calculate totals
    variance : bool
        Whether to calculate variance
    temporal_method : str
        Temporal estimation method
    execution_mode : ExecutionMode
        Pipeline execution mode
    enable_caching : bool
        Whether to enable caching
    **kwargs
        Additional configuration parameters
        
    Returns
    -------
    EstimationPipeline
        Configured mortality estimation pipeline
        
    Examples
    --------
    >>> # Create volume mortality pipeline
    >>> pipeline = create_mortality_pipeline(
    ...     mortality_type="volume",
    ...     by_species=True
    ... )
    
    >>> # Biomass mortality by cause
    >>> pipeline = create_mortality_pipeline(
    ...     mortality_type="biomass",
    ...     grp_by=["AGENTCD"]
    ... )
    """
    config = {
        "mortality_type": mortality_type,
        "by_species": by_species,
        "by_size_class": by_size_class,
        "grp_by": grp_by,
        "tree_domain": tree_domain,
        "area_domain": area_domain,
        "plot_domain": plot_domain,
        "totals": totals,
        "variance": variance,
        "temporal_method": temporal_method,
        **kwargs
    }
    
    builder = MortalityEstimationBuilder(
        execution_mode=execution_mode,
        enable_caching=enable_caching
    )
    
    pipeline = builder.build(**config)
    
    if db:
        pipeline.context["database"] = db
    
    return pipeline


# === Template-based Quick Start Functions ===

def create_pipeline_from_template(
    template_name: str,
    db: Optional[FIA] = None,
    **kwargs
) -> EstimationPipeline:
    """
    Create a pipeline from a pre-defined template.
    
    Parameters
    ----------
    template_name : str
        Name of the template to use
    db : Optional[FIA]
        FIA database connection
    **kwargs
        Additional configuration parameters
        
    Returns
    -------
    EstimationPipeline
        Configured pipeline from template
        
    Examples
    --------
    >>> # Use basic volume template
    >>> pipeline = create_pipeline_from_template("basic_volume")
    
    >>> # Use species grouping template with custom domain
    >>> pipeline = create_pipeline_from_template(
    ...     "volume_by_species",
    ...     tree_domain="DIA >= 15.0"
    ... )
    """
    template = get_template(template_name)
    pipeline = template.create_pipeline(**kwargs)
    
    if db:
        pipeline.context["database"] = db
    
    return pipeline


# === Convenience Functions for Common Patterns ===

def quick_volume(
    db: FIA,
    species_code: Optional[int] = None,
    min_dbh: Optional[float] = None,
    **kwargs
) -> pl.DataFrame:
    """
    Quick volume estimation with common filters.
    
    Parameters
    ----------
    db : FIA
        FIA database connection
    species_code : Optional[int]
        Species code to filter
    min_dbh : Optional[float]
        Minimum DBH threshold
    **kwargs
        Additional parameters
        
    Returns
    -------
    pl.DataFrame
        Volume estimation results
        
    Examples
    --------
    >>> # Volume for loblolly pine >= 10" DBH
    >>> result = quick_volume(db, species_code=131, min_dbh=10.0)
    """
    tree_domain = []
    
    if species_code:
        tree_domain.append(f"SPCD == {species_code}")
    
    if min_dbh:
        tree_domain.append(f"DIA >= {min_dbh}")
    
    tree_domain_str = " AND ".join(tree_domain) if tree_domain else None
    
    pipeline = create_volume_pipeline(
        tree_domain=tree_domain_str,
        by_species=(species_code is None),  # Group by species if not filtering
        **kwargs
    )
    
    return pipeline.execute(db)


def quick_biomass(
    db: FIA,
    species_group: Optional[str] = None,
    component: str = "aboveground",
    **kwargs
) -> pl.DataFrame:
    """
    Quick biomass estimation with common filters.
    
    Parameters
    ----------
    db : FIA
        FIA database connection
    species_group : Optional[str]
        Species group (hardwood, softwood)
    component : str
        Biomass component
    **kwargs
        Additional parameters
        
    Returns
    -------
    pl.DataFrame
        Biomass estimation results
        
    Examples
    --------
    >>> # Aboveground biomass for hardwoods
    >>> result = quick_biomass(db, species_group="hardwood")
    """
    tree_domain = None
    
    if species_group:
        if species_group.lower() == "hardwood":
            tree_domain = "SPGRPCD == 2"
        elif species_group.lower() == "softwood":
            tree_domain = "SPGRPCD == 1"
    
    pipeline = create_biomass_pipeline(
        tree_domain=tree_domain,
        component=component,
        by_species=True,
        **kwargs
    )
    
    return pipeline.execute(db)


def quick_carbon_assessment(
    db: FIA,
    by_ownership: bool = True,
    **kwargs
) -> pl.DataFrame:
    """
    Quick carbon stock assessment.
    
    Parameters
    ----------
    db : FIA
        FIA database connection
    by_ownership : bool
        Whether to group by ownership
    **kwargs
        Additional parameters
        
    Returns
    -------
    pl.DataFrame
        Carbon assessment results
        
    Examples
    --------
    >>> # Carbon stock by ownership class
    >>> result = quick_carbon_assessment(db, by_ownership=True)
    """
    grp_by = ["OWNGRPCD"] if by_ownership else None
    
    # Carbon = total biomass * 0.5 (standard conversion)
    pipeline = create_biomass_pipeline(
        component="total",
        grp_by=grp_by,
        variance=True,
        totals=True,
        **kwargs
    )
    
    # Execute and convert to carbon
    result = pipeline.execute(db)
    
    # Convert biomass to carbon (multiply by 0.5)
    value_cols = [col for col in result.columns if "ESTIMATE" in col or "TOTAL" in col]
    for col in value_cols:
        result = result.with_columns(
            (pl.col(col) * 0.5).alias(col.replace("BIOMASS", "CARBON"))
        )
    
    return result


def quick_forest_inventory(
    db: FIA,
    metrics: List[str] = ["volume", "biomass", "tpa"],
    by_species: bool = True,
    **kwargs
) -> dict:
    """
    Quick comprehensive forest inventory.
    
    Parameters
    ----------
    db : FIA
        FIA database connection
    metrics : List[str]
        Metrics to calculate
    by_species : bool
        Whether to group by species
    **kwargs
        Additional parameters
        
    Returns
    -------
    dict
        Dictionary of results by metric
        
    Examples
    --------
    >>> # Complete inventory with all metrics
    >>> results = quick_forest_inventory(
    ...     db,
    ...     metrics=["volume", "biomass", "tpa", "area"]
    ... )
    """
    results = {}
    
    if "volume" in metrics:
        pipeline = create_volume_pipeline(by_species=by_species, **kwargs)
        results["volume"] = pipeline.execute(db)
    
    if "biomass" in metrics:
        pipeline = create_biomass_pipeline(by_species=by_species, **kwargs)
        results["biomass"] = pipeline.execute(db)
    
    if "tpa" in metrics:
        pipeline = create_tpa_pipeline(by_species=by_species, **kwargs)
        results["tpa"] = pipeline.execute(db)
    
    if "area" in metrics:
        pipeline = create_area_pipeline(**kwargs)
        results["area"] = pipeline.execute(db)
    
    if "growth" in metrics:
        pipeline = create_growth_pipeline(by_species=by_species, **kwargs)
        results["growth"] = pipeline.execute(db)
    
    if "mortality" in metrics:
        pipeline = create_mortality_pipeline(by_species=by_species, **kwargs)
        results["mortality"] = pipeline.execute(db)
    
    return results


# === Migration Helpers ===

def migrate_to_pipeline(
    function_name: str,
    *args,
    **kwargs
) -> EstimationPipeline:
    """
    Migrate from traditional function to pipeline.
    
    This helper function maps traditional pyFIA function calls to
    their pipeline equivalents.
    
    Parameters
    ----------
    function_name : str
        Name of traditional function (volume, biomass, etc.)
    *args
        Positional arguments
    **kwargs
        Keyword arguments
        
    Returns
    -------
    EstimationPipeline
        Equivalent pipeline
        
    Examples
    --------
    >>> # Migrate volume() call to pipeline
    >>> pipeline = migrate_to_pipeline(
    ...     "volume",
    ...     db,
    ...     bySpecies=True,
    ...     treeDomain="DIA >= 10.0"
    ... )
    """
    # Map traditional parameter names to new names
    param_mapping = {
        "bySpecies": "by_species",
        "bySizeClass": "by_size_class",
        "grpBy": "grp_by",
        "treeDomain": "tree_domain",
        "areaDomain": "area_domain",
        "plotDomain": "plot_domain",
        "landType": "land_type",
        "temporalMethod": "temporal_method",
    }
    
    # Remap parameters
    remapped_kwargs = {}
    for old_key, new_key in param_mapping.items():
        if old_key in kwargs:
            remapped_kwargs[new_key] = kwargs.pop(old_key)
    
    # Add remaining kwargs
    remapped_kwargs.update(kwargs)
    
    # Extract database if provided as first argument
    db = args[0] if args else remapped_kwargs.pop("db", None)
    
    # Create appropriate pipeline
    if function_name == "volume":
        return create_volume_pipeline(db, **remapped_kwargs)
    elif function_name == "biomass":
        return create_biomass_pipeline(db, **remapped_kwargs)
    elif function_name == "tpa":
        return create_tpa_pipeline(db, **remapped_kwargs)
    elif function_name == "area":
        return create_area_pipeline(db, **remapped_kwargs)
    elif function_name == "growth":
        return create_growth_pipeline(db, **remapped_kwargs)
    elif function_name == "mortality":
        return create_mortality_pipeline(db, **remapped_kwargs)
    else:
        raise ValueError(f"Unknown function: {function_name}")