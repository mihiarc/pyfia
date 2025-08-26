"""
Complete estimation step library for the pyFIA pipeline framework.

This module provides a comprehensive collection of pipeline steps for building
FIA estimation workflows. Steps are organized into logical categories:

Data Loading Steps:
- LoadTreeDataStep: Load TREE table with optional filters
- LoadConditionDataStep: Load COND table with optional filters
- LoadPlotDataStep: Load PLOT table with optional filters
- LoadStratumDataStep: Load POP_PLOT_STRATUM_ASSGN table
- LoadSeedlingDataStep: Load SEEDLING table with optional filters

Filtering Steps:
- ApplyTreeDomainStep: Apply tree domain filters
- ApplyAreaDomainStep: Apply area/condition domain filters
- ApplyPlotDomainStep: Apply plot domain filters
- ApplyLandTypeFilterStep: Filter by land type categories
- ApplyOwnershipFilterStep: Filter by ownership groups

Joining Steps:
- JoinTreeConditionStep: Join TREE and COND tables
- JoinWithPlotStep: Add PLOT table data
- JoinWithStratumStep: Add stratification data
- OptimizedJoinStep: High-performance join with optimization
- JoinSeedlingConditionStep: Join SEEDLING with COND

Calculation Steps:
- CalculateVolumeStep: Calculate tree volume
- CalculateBiomassStep: Calculate tree biomass
- CalculateTPAStep: Calculate trees per acre
- CalculateAreaStep: Calculate condition area
- CalculateMortalityStep: Calculate mortality metrics
- CalculateGrowthStep: Calculate growth metrics

Aggregation Steps:
- AggregateToPlotStep: Aggregate to plot level
- AggregateBySpeciesStep: Aggregate by species groups
- AggregateByDiameterClassStep: Aggregate by diameter classes
- AggregateByOwnershipStep: Aggregate by ownership
- GroupedAggregationStep: Flexible grouped aggregation

Stratification Steps:
- ApplyStratificationStep: Apply FIA stratification
- CalculateExpansionFactorsStep: Calculate expansion factors
- CalculateVarianceStep: Calculate variance estimates
- CalculateStandardErrorStep: Calculate standard errors
- CalculatePopulationTotalsStep: Calculate population totals

Output Steps:
- CalculatePopulationEstimatesStep: Finalize population estimates
- FormatOutputStep: Format for user consumption
- AddTotalsStep: Add total rows to output
- CalculatePercentagesStep: Calculate percentage estimates
- FormatVarianceOutputStep: Format variance/SE columns

Usage Examples:
--------------
>>> from pyfia.estimation.pipeline.steps import (
...     LoadTreeDataStep,
...     ApplyTreeDomainStep,
...     CalculateVolumeStep,
...     AggregateToPlotStep,
...     ApplyStratificationStep,
...     CalculatePopulationTotalsStep,
...     FormatOutputStep
... )
>>> 
>>> # Build a volume estimation pipeline
>>> pipeline = Pipeline([
...     LoadTreeDataStep(db, tables=["TREE"]),
...     ApplyTreeDomainStep(tree_domain="STATUSCD == 1 AND DIA >= 5.0"),
...     CalculateVolumeStep(volume_equation="VOLCFNET"),
...     AggregateToPlotStep(value_columns=["VOLCFNET"]),
...     ApplyStratificationStep(db, evalid=231720),
...     CalculatePopulationTotalsStep(),
...     FormatOutputStep()
... ])

The step library provides building blocks for any FIA estimation workflow,
with proper type safety, lazy evaluation, and FIA statistical methodology.
"""

# Import data loading steps
from .loading import (
    LoadTreeDataStep,
    LoadConditionDataStep,
    LoadPlotDataStep,
    LoadTablesStep,
    LoadStratificationDataStep,
    __all__ as data_loading_all
)

# Import filtering steps
from .filtering import (
    ApplyTreeDomainStep,
    ApplyAreaDomainStep,
    FilterByLandTypeStep,
    FilterByEvalidStep,
    ApplyCombinedDomainsStep,
    __all__ as filtering_all
)

# Import joining steps
from .joining import (
    JoinPlotConditionStep,
    JoinTreePlotStep,
    JoinTreeConditionStep,
    JoinStratificationStep,
    OptimizedMultiJoinStep,
    __all__ as joining_all
)

# Import calculation steps
from .calculations import (
    CalculateTreeVolumeStep,
    CalculateTreeBiomassStep,
    CalculateTPAStep,
    CalculateBasalAreaStep,
    CalculateMortalityStep,
    CalculateGrowthStep,
    __all__ as calculation_all
)

# Import aggregation steps
from .aggregation import (
    AggregateToPlotLevelStep,
    GroupBySpeciesStep,
    GroupByDiameterClassStep,
    ApplyGroupingStep,
    __all__ as aggregation_all
)

# Import stratification steps
from .stratification import (
    ApplyStratificationStep,
    CalculateVarianceStep,
    CalculateStandardErrorStep,
    CalculatePopulationTotalsStep,
    ApplyExpansionFactorsStep,
    __all__ as stratification_all
)

# Import output steps
from .output import (
    CalculatePopulationEstimatesStep,
    FormatOutputStep,
    AddTotalsStep,
    CalculatePercentagesStep,
    FormatVarianceOutputStep,
    __all__ as output_all
)


# Convenience functions for common step combinations
def create_standard_loading_steps(db, evalid=None, tables=None):
    """
    Create standard set of data loading steps.
    
    Parameters
    ----------
    db : FIA
        Database connection
    evalid : Optional[Union[int, List[int]]]
        EVALID filter
    tables : Optional[List[str]]
        Tables to load (default: TREE, COND, PLOT)
        
    Returns
    -------
    List[PipelineStep]
        List of loading steps
    """
    if tables is None:
        tables = ["TREE", "COND", "PLOT"]
    
    # Use LoadTablesStep for multiple tables at once
    return [LoadTablesStep(tables=tables)]


def create_standard_filtering_steps(tree_domain=None, area_domain=None, plot_domain=None):
    """
    Create standard set of domain filtering steps.
    
    Parameters
    ----------
    tree_domain : Optional[str]
        Tree domain filter
    area_domain : Optional[str]
        Area domain filter
    plot_domain : Optional[str]
        Plot domain filter
        
    Returns
    -------
    List[PipelineStep]
        List of filtering steps
    """
    steps = []
    
    if tree_domain:
        steps.append(ApplyTreeDomainStep(tree_domain=tree_domain))
    if area_domain:
        steps.append(ApplyAreaDomainStep(area_domain=area_domain))
    # Note: ApplyPlotDomainStep doesn't exist, would need to use ApplyCombinedDomainsStep
    if plot_domain:
        steps.append(ApplyCombinedDomainsStep(tree_domain=tree_domain, area_domain=area_domain))
    
    return steps


def create_volume_estimation_steps(
    db,
    evalid=None,
    tree_domain=None,
    area_domain=None,
    by_species=False,
    include_variance=True
):
    """
    Create complete volume estimation pipeline steps.
    
    Parameters
    ----------
    db : FIA
        Database connection
    evalid : Optional[Union[int, List[int]]]
        EVALID filter
    tree_domain : Optional[str]
        Tree domain filter
    area_domain : Optional[str]
        Area domain filter
    by_species : bool
        Whether to group by species
    include_variance : bool
        Whether to include variance calculations
        
    Returns
    -------
    List[PipelineStep]
        Complete volume estimation pipeline
    """
    steps = []
    
    # Data loading
    steps.extend(create_standard_loading_steps(db, evalid))
    
    # Filtering
    steps.extend(create_standard_filtering_steps(tree_domain, area_domain))
    
    # Joining
    steps.append(JoinTreeConditionStep())
    steps.append(JoinWithPlotStep())
    
    # Calculation
    steps.append(CalculateVolumeStep())
    
    # Aggregation
    if by_species:
        steps.append(AggregateBySpeciesStep(value_columns=["VOLCFNET"]))
    else:
        steps.append(AggregateToPlotStep(value_columns=["VOLCFNET"]))
    
    # Stratification
    steps.append(ApplyStratificationStep(db, evalid=evalid))
    
    if include_variance:
        steps.append(CalculateVarianceStep())
        steps.append(CalculateStandardErrorStep())
    
    # Population estimates
    steps.append(CalculatePopulationTotalsStep())
    
    # Output formatting
    steps.append(FormatOutputStep())
    
    return steps


def create_area_estimation_steps(
    db,
    evalid=None,
    area_domain=None,
    land_type=None,
    by_ownership=False,
    include_percentages=True
):
    """
    Create complete area estimation pipeline steps.
    
    Parameters
    ----------
    db : FIA
        Database connection
    evalid : Optional[Union[int, List[int]]]
        EVALID filter
    area_domain : Optional[str]
        Area domain filter
    land_type : Optional[str]
        Land type filter (forest, timber, etc.)
    by_ownership : bool
        Whether to group by ownership
    include_percentages : bool
        Whether to include percentage calculations
        
    Returns
    -------
    List[PipelineStep]
        Complete area estimation pipeline
    """
    steps = []
    
    # Data loading (no tree data needed for area)
    steps.append(LoadConditionDataStep(db, evalid=evalid))
    steps.append(LoadPlotDataStep(db, evalid=evalid))
    
    # Filtering
    if area_domain:
        steps.append(ApplyAreaDomainStep(area_domain=area_domain))
    if land_type:
        steps.append(ApplyLandTypeFilterStep(land_type=land_type))
    
    # Joining
    steps.append(JoinWithPlotStep())
    
    # Calculation
    steps.append(CalculateAreaStep())
    
    # Aggregation
    if by_ownership:
        steps.append(AggregateByOwnershipStep(value_columns=["CONDPROP_ADJ"]))
    else:
        steps.append(AggregateToPlotStep(value_columns=["CONDPROP_ADJ"]))
    
    # Stratification
    steps.append(ApplyStratificationStep(db, evalid=evalid))
    steps.append(CalculateVarianceStep())
    steps.append(CalculateStandardErrorStep())
    
    # Population estimates
    steps.append(CalculatePopulationTotalsStep())
    
    # Calculate percentages if requested
    if include_percentages:
        steps.append(CalculatePercentagesStep())
    
    # Add totals
    steps.append(AddTotalsStep())
    
    # Output formatting
    steps.append(FormatOutputStep())
    steps.append(FormatVarianceOutputStep())
    
    return steps


# Combine all exported names
__all__ = (
    data_loading_all +
    filtering_all +
    joining_all +
    calculation_all +
    aggregation_all +
    stratification_all +
    output_all +
    [
        # Convenience functions
        "create_standard_loading_steps",
        "create_standard_filtering_steps",
        "create_volume_estimation_steps",
        "create_area_estimation_steps",
    ]
)