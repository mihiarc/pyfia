"""
Consolidated filtering functions for FIA data analysis.

This module contains all tree and area filtering logic used across
different FIA estimators to reduce code duplication and ensure
consistent filtering methodology. It also provides intelligent defaults
and tracks filtering assumptions for transparent AI agent communication.
"""

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import polars as pl

from .domain_parser import DomainExpressionParser
from .common import apply_tree_filters as apply_tree_filters_base, apply_area_filters as apply_area_filters_base
from ..constants.constants import (
    LandStatus,
    ReserveStatus,
    SiteClass,
    TreeClass,
    TreeStatus,
)


@dataclass
class FilterAssumptions:
    """Track filtering assumptions for transparent communication."""
    tree_type: str
    land_type: str
    tree_domain: Optional[str]
    area_domain: Optional[str]
    assumptions_made: List[str]
    defaults_applied: List[str]

    def to_explanation(self) -> str:
        """Convert assumptions to human-readable explanation."""
        explanation = []

        # Core filter explanations
        if self.tree_type == "all":
            explanation.append("• Including all tree types (live and dead)")
        elif self.tree_type == "live":
            explanation.append("• Including only live trees")
        elif self.tree_type == "dead":
            explanation.append("• Including only dead trees")
        elif self.tree_type == "gs":
            explanation.append("• Including only growing stock trees (live, sound, commercial species)")
        elif self.tree_type == "live_gs":
            explanation.append("• Including only live growing stock trees")
        elif self.tree_type == "dead_gs":
            explanation.append("• Including only dead growing stock trees")

        if self.land_type == "forest":
            explanation.append("• Including all forest land")
        elif self.land_type == "timber":
            explanation.append("• Including only timberland (productive, unreserved forest)")
        elif self.land_type == "all":
            explanation.append("• Including all land types")

        # Custom domains
        if self.tree_domain:
            explanation.append(f"• Tree filter: {self.tree_domain}")
        if self.area_domain:
            explanation.append(f"• Area filter: {self.area_domain}")

        # Defaults that were applied
        if self.defaults_applied:
            explanation.append("\nDefaults applied:")
            for default in self.defaults_applied:
                explanation.append(f"• {default}")

        # Additional assumptions
        if self.assumptions_made:
            explanation.append("\nKey assumptions:")
            for assumption in self.assumptions_made:
                explanation.append(f"• {assumption}")

        return "\n".join(explanation)


def get_intelligent_defaults(
    query_context: Optional[str] = None,
    analysis_type: str = "general",
) -> Dict[str, str]:
    """
    Get intelligent defaults based on query context and analysis type.

    Parameters
    ----------
    query_context : str, optional
        Natural language context about the query
    analysis_type : str, default "general"
        Type of analysis: "area", "volume", "biomass", "mortality", "growth", "general"

    Returns
    -------
    dict
        Dictionary with intelligent defaults for tree_type, land_type
    """
    defaults = {"tree_type": "all", "land_type": "forest"}

    # Context-aware defaults
    if query_context:
        context_lower = query_context.lower()

        # Tree type intelligence
        if any(word in context_lower for word in ["live", "living", "alive"]):
            defaults["tree_type"] = "live"
        elif any(word in context_lower for word in ["dead", "mortality", "died"]):
            defaults["tree_type"] = "dead"
        elif any(word in context_lower for word in ["growing stock", "merchantable", "commercial"]):
            defaults["tree_type"] = "gs"

        # Land type intelligence
        if any(word in context_lower for word in ["timber", "commercial", "productive"]):
            defaults["land_type"] = "timber"
        elif any(word in context_lower for word in ["all land", "any land"]):
            defaults["land_type"] = "all"

    # Analysis-specific defaults
    if analysis_type == "volume":
        # Volume typically focuses on merchantable trees
        if defaults["tree_type"] == "all":
            defaults["tree_type"] = "gs"
    elif analysis_type == "mortality":
        # Mortality analysis often wants dead trees specifically
        if defaults["tree_type"] == "all":
            defaults["tree_type"] = "dead"
    elif analysis_type == "biomass":
        # Biomass often focuses on live trees
        if defaults["tree_type"] == "all":
            defaults["tree_type"] = "live"

    return defaults


def apply_tree_filters(
    tree_df: pl.DataFrame,
    tree_type: str = "all",
    tree_domain: Optional[str] = None,
    query_context: Optional[str] = None,
    track_assumptions: bool = False,
) -> Tuple[pl.DataFrame, Optional[FilterAssumptions]]:
    """
    Apply tree type and domain filters following FIA methodology.

    Parameters
    ----------
    tree_df : pl.DataFrame
        Tree dataframe to filter
    tree_type : str, default "all"
        Type of trees to include:
        - "all": All trees
        - "live": Live trees only (STATUSCD == TreeStatus.LIVE)
        - "dead": Dead trees only (STATUSCD == TreeStatus.DEAD)
        - "gs": Growing stock trees (live, sound, commercial species)
        - "live_gs": Live growing stock
        - "dead_gs": Dead growing stock
        - "auto": Use intelligent defaults based on context
    tree_domain : str, optional
        Additional filter expression (e.g., "DIA >= 5")
    query_context : str, optional
        Natural language context for intelligent defaults
    track_assumptions : bool, default False
        Whether to track and return filtering assumptions

    Returns
    -------
    pl.DataFrame or tuple
        Filtered tree dataframe, optionally with FilterAssumptions
    """
    assumptions_made = []
    defaults_applied = []

    # Apply intelligent defaults if requested
    original_tree_type = tree_type
    if tree_type == "auto":
        intelligent_defaults = get_intelligent_defaults(query_context, "general")
        tree_type = intelligent_defaults["tree_type"]
        defaults_applied.append(f"Tree type defaulted to '{tree_type}' based on context")

    # Map enhanced tree types to common function compatible types
    # Handle the enhanced types (live_gs, dead_gs) that common function doesn't support
    if tree_type in ["live_gs", "dead_gs"]:
        # For these enhanced types, we need custom logic
        if tree_type == "live_gs":
            tree_df = tree_df.filter(
                (pl.col("STATUSCD") == TreeStatus.LIVE)
                & (pl.col("TREECLCD") == TreeClass.GROWING_STOCK)
                & (pl.col("AGENTCD") < 30)
            )
            assumptions_made.extend([
                "Live growing stock: STATUSCD == 1, TREECLCD == 2, AGENTCD < 30"
            ])
        elif tree_type == "dead_gs":
            tree_df = tree_df.filter(
                (pl.col("STATUSCD") == TreeStatus.DEAD)
                & (pl.col("TREECLCD") == TreeClass.GROWING_STOCK)
                & (pl.col("AGENTCD") < 30)
            )
            assumptions_made.extend([
                "Dead growing stock: STATUSCD == 2, TREECLCD == 2, AGENTCD < 30"
            ])
        
        # Still apply domain filter if provided
        if tree_domain:
            tree_df = DomainExpressionParser.apply_to_dataframe(tree_df, tree_domain, "tree")
            assumptions_made.append(f"Custom tree filter applied: {tree_domain}")
    else:
        # Use common function for standard tree types (live, dead, gs, all)
        tree_df = apply_tree_filters_base(
            tree_df=tree_df,
            tree_type=tree_type,
            tree_domain=tree_domain,
            require_volume=False,
            require_diameter_thresholds=False
        )
        
        # Add assumptions based on what the common function did
        if tree_type == "live":
            assumptions_made.append("Live trees defined as STATUSCD == 1")
        elif tree_type == "dead":
            assumptions_made.append("Dead trees defined as STATUSCD == 2") 
        elif tree_type == "gs":
            assumptions_made.extend([
                "Growing stock trees (includes live and dead with STATUSCD filtering)"
            ])
        elif tree_type == "all":
            assumptions_made.append("Including all tree types (no STATUSCD filter)")
        
        if tree_domain:
            assumptions_made.append(f"Custom tree filter applied: {tree_domain}")

    if track_assumptions:
        filter_assumptions = FilterAssumptions(
            tree_type=tree_type,
            land_type="",  # Will be filled by area filters
            tree_domain=tree_domain,
            area_domain=None,  # Will be filled by area filters
            assumptions_made=assumptions_made,
            defaults_applied=defaults_applied,
        )
        return tree_df, filter_assumptions

    return tree_df, None


def apply_area_filters(
    cond_df: pl.DataFrame,
    land_type: str = "forest",
    area_domain: Optional[str] = None,
    query_context: Optional[str] = None,
    assumptions: Optional[FilterAssumptions] = None,
    track_assumptions: bool = False,
) -> Tuple[pl.DataFrame, Optional[FilterAssumptions]]:
    """
    Apply land type and area domain filters.

    Parameters
    ----------
    cond_df : pl.DataFrame
        Condition dataframe to filter
    land_type : str, default "forest"
        Type of land to include:
        - "forest": Forest land (COND_STATUS_CD == LandStatus.FOREST)
        - "timber": Timberland (forest + productive + unreserved)
        - "all": All conditions
        - "auto": Use intelligent defaults based on context
    area_domain : str, optional
        Additional filter expression (e.g., "OWNGRPCD == 10")
    query_context : str, optional
        Natural language context for intelligent defaults
    assumptions : FilterAssumptions, optional
        Existing assumptions object to update
    track_assumptions : bool, default False
        Whether to track and return filtering assumptions

    Returns
    -------
    pl.DataFrame or tuple
        Filtered condition dataframe, optionally with FilterAssumptions
    """
    if assumptions is None:
        assumptions = FilterAssumptions(
            tree_type="",
            land_type="",
            tree_domain=None,
            area_domain=area_domain,
            assumptions_made=[],
            defaults_applied=[],
        )


    # Apply intelligent defaults if requested
    if land_type == "auto":
        intelligent_defaults = get_intelligent_defaults(query_context, "area")
        land_type = intelligent_defaults["land_type"]
        assumptions.defaults_applied.append(f"Land type defaulted to '{land_type}' based on context")

    # Use common function for area filtering, but keep assumption tracking
    cond_df = apply_area_filters_base(
        cond_df=cond_df,
        land_type=land_type, 
        area_domain=area_domain,
        area_estimation_mode=False
    )
    
    # Add assumptions based on what the common function did
    if land_type == "forest":
        assumptions.assumptions_made.append("Forest land defined as COND_STATUS_CD == 1")
    elif land_type == "timber":
        assumptions.assumptions_made.extend([
            "Timberland defined as:",
            "  - Forest land (COND_STATUS_CD == 1)",
            "  - Productive sites (SITECLCD in productive classes)",
            "  - Not reserved (RESERVCD == 0)"
        ])
    elif land_type == "all":
        assumptions.assumptions_made.append("Including all land types (no COND_STATUS_CD filter)")
    
    if area_domain:
        assumptions.assumptions_made.append(f"Custom area filter applied: {area_domain}")

    # Update assumptions object
    assumptions.land_type = land_type
    assumptions.area_domain = area_domain

    if track_assumptions:
        return cond_df, assumptions

    return cond_df, None




def apply_growing_stock_filter(
    tree_df: pl.DataFrame,
    gs_type: str = "standard",
) -> pl.DataFrame:
    """
    Apply growing stock filters per FIA definitions.

    Parameters
    ----------
    tree_df : pl.DataFrame
        Tree dataframe to filter
    gs_type : str, default "standard"
        Type of growing stock definition:
        - "standard": Standard GS definition
        - "merchantable": For merchantable volume
        - "board_foot": For board foot volume

    Returns
    -------
    pl.DataFrame
        Filtered tree dataframe
    """
    # Base growing stock filter - check for column existence
    gs_filter = (
        (pl.col("STATUSCD") == TreeStatus.LIVE)  # Live
        & (pl.col("TREECLCD") == TreeClass.GROWING_STOCK)  # Growing stock class
    )
    
    # Add AGENTCD filter only if column exists
    if "AGENTCD" in tree_df.columns:
        gs_filter = gs_filter & (pl.col("AGENTCD") < 30)  # No severe damage

    if gs_type == "merchantable":
        # Additional filters for merchantable volume
        gs_filter = gs_filter & (pl.col("DIA") >= 5.0)
    elif gs_type == "board_foot":
        # Board foot requires larger diameter
        gs_filter = gs_filter & (pl.col("DIA") >= 9.0)
    elif gs_type != "standard":
        raise ValueError(f"Invalid gs_type: {gs_type}")

    return tree_df.filter(gs_filter)


def apply_mortality_filters(
    tree_df: pl.DataFrame,
    tree_class: str = "all",
) -> pl.DataFrame:
    """
    Apply mortality-specific filters.

    Parameters
    ----------
    tree_df : pl.DataFrame
        Tree dataframe to filter
    tree_class : str, default "all"
        Tree classification:
        - "all": All mortality trees
        - "growing_stock": Only growing stock mortality

    Returns
    -------
    pl.DataFrame
        Filtered tree dataframe
    """
    # Base mortality filter - trees with mortality component
    mort_df = tree_df.filter(pl.col("COMPONENT").str.contains("MORTALITY"))

    if tree_class == "growing_stock":
        # Apply growing stock filters to mortality trees
        mort_df = mort_df.filter(
            (pl.col("TREECLCD") == TreeClass.GROWING_STOCK)  # Growing stock class
            & (pl.col("AGENTCD") < 30)  # No severe damage at time of death
        )
    elif tree_class != "all":
        raise ValueError(f"Invalid tree_class: {tree_class}")

    return mort_df


def apply_standard_filters(
    tree_df: pl.DataFrame,
    cond_df: pl.DataFrame,
    tree_type: str = "all",
    land_type: str = "forest",
    tree_domain: Optional[str] = None,
    area_domain: Optional[str] = None,
    query_context: Optional[str] = None,
    track_assumptions: bool = False,
) -> Tuple[pl.DataFrame, pl.DataFrame, Optional[FilterAssumptions]]:
    """
    Apply standard tree and area filters together with intelligent defaults.

    This is a convenience function that applies both tree and area
    filters in one call, returning both filtered dataframes and assumptions.

    Parameters
    ----------
    tree_df : pl.DataFrame
        Tree dataframe to filter
    cond_df : pl.DataFrame
        Condition dataframe to filter
    tree_type : str, default "all"
        Type of trees to include (or "auto" for intelligent defaults)
    land_type : str, default "forest"
        Type of land to include (or "auto" for intelligent defaults)
    tree_domain : str, optional
        Additional tree filter expression
    area_domain : str, optional
        Additional area filter expression
    query_context : str, optional
        Natural language context for intelligent defaults
    track_assumptions : bool, default False
        Whether to track and return filtering assumptions

    Returns
    -------
    tuple[pl.DataFrame, pl.DataFrame, Optional[FilterAssumptions]]
        Filtered (tree_df, cond_df, assumptions) tuple
    """
    # Apply tree filters with assumption tracking
    if track_assumptions:
        filtered_trees, assumptions = apply_tree_filters(
            tree_df, tree_type, tree_domain, query_context, track_assumptions=True
        )

        # Apply area filters with existing assumptions
        filtered_conds, assumptions = apply_area_filters(
            cond_df, land_type, area_domain, query_context, assumptions, track_assumptions=True
        )

        return filtered_trees, filtered_conds, assumptions
    else:
        # Simple mode without assumption tracking
        filtered_trees, _ = apply_tree_filters(tree_df, tree_type, tree_domain, query_context)
        filtered_conds, _ = apply_area_filters(cond_df, land_type, area_domain, query_context)

        return filtered_trees, filtered_conds, None


def get_size_class_expr() -> pl.Expr:
    """
    Get Polars expression for FIA standard size classes.

    Returns
    -------
    pl.Expr
        Expression that creates 'sizeClass' column based on DIA
    """
    return (
        pl.when(pl.col("DIA") < 10.0).then(pl.lit("Small"))
        .when(pl.col("DIA") < 18.0).then(pl.lit("Medium"))
        .otherwise(pl.lit("Large"))
        .alias("SIZE_CLASS")
    )


def create_domain_explanation(
    tree_type: str = "all",
    land_type: str = "forest",
    tree_domain: Optional[str] = None,
    area_domain: Optional[str] = None,
    query_context: Optional[str] = None,
) -> str:
    """
    Create human-readable explanation of domain filters for agent communication.

    Parameters
    ----------
    tree_type : str
        Tree type filter being applied
    land_type : str
        Land type filter being applied
    tree_domain : str, optional
        Custom tree domain filter
    area_domain : str, optional
        Custom area domain filter
    query_context : str, optional
        Original query context for reference

    Returns
    -------
    str
        Human-readable explanation of all filters and assumptions
    """
    # Apply filters to get assumptions without actually filtering data
    dummy_tree_df = pl.DataFrame({"STATUSCD": [1], "TREECLCD": [2], "AGENTCD": [10]})
    dummy_cond_df = pl.DataFrame({"COND_STATUS_CD": [1], "SITECLCD": [1], "RESERVCD": [0]})

    try:
        _, _, assumptions = apply_standard_filters(
            dummy_tree_df, dummy_cond_df,
            tree_type, land_type, tree_domain, area_domain,
            query_context, track_assumptions=True
        )

        if assumptions:
            explanation = assumptions.to_explanation()
            if query_context:
                explanation = f"Query: \"{query_context}\"\n\n{explanation}"
            return explanation
        else:
            return "Standard FIA filters applied (no special assumptions tracked)"

    except Exception as e:
        return f"Error generating explanation: {str(e)}"


def suggest_common_domains(analysis_type: str = "general") -> Dict[str, List[str]]:
    """
    Suggest common domain filters for different analysis types.

    Parameters
    ----------
    analysis_type : str
        Type of analysis to suggest domains for

    Returns
    -------
    dict
        Dictionary with suggested tree_domains and area_domains
    """
    suggestions = {
        "tree_domains": [],
        "area_domains": [],
        "tree_types": [],
        "land_types": []
    }

    if analysis_type == "volume":
        suggestions["tree_domains"] = [
            "DIA >= 5.0",  # Merchantable diameter
            "DIA >= 9.0",  # Board foot diameter
            "HT > 4.5",    # Merchantable height
        ]
        suggestions["tree_types"] = ["gs", "live_gs"]
        suggestions["land_types"] = ["timber", "forest"]

    elif analysis_type == "biomass":
        suggestions["tree_domains"] = [
            "DIA >= 1.0",  # Include small trees
            "STATUSCD == 1",  # Live trees only
        ]
        suggestions["area_domains"] = [
            "OWNGRPCD == 10",  # Public land
            "OWNGRPCD == 40",  # Private land
        ]
        suggestions["tree_types"] = ["live", "all"]

    elif analysis_type == "mortality":
        suggestions["tree_domains"] = [
            "AGENTCD >= 30",  # Severe damage
            "AGENTCD == 40",  # Disease
            "AGENTCD == 50",  # Fire
        ]
        suggestions["tree_types"] = ["dead", "dead_gs"]

    elif analysis_type == "area":
        suggestions["area_domains"] = [
            "FORTYPCD < 400",  # Forest types
            "OWNGRPCD == 10",  # Public ownership
            "DSTRBCD1 == 0",   # No disturbance
        ]
        suggestions["land_types"] = ["forest", "timber"]

    return suggestions


def validate_filters(
    tree_type: str = "all",
    land_type: str = "forest",
    gs_type: str = "standard",
) -> None:
    """
    Validate filter type parameters.

    Parameters
    ----------
    tree_type : str
        Tree type to validate
    land_type : str
        Land type to validate
    gs_type : str
        Growing stock type to validate

    Raises
    ------
    ValueError
        If any filter type is invalid
    """
    valid_tree_types = {"all", "live", "dead", "gs", "live_gs", "dead_gs", "auto"}
    valid_land_types = {"all", "forest", "timber", "auto"}
    valid_gs_types = {"standard", "merchantable", "board_foot"}

    if tree_type not in valid_tree_types:
        raise ValueError(
            f"Invalid tree_type: {tree_type}. "
            f"Valid options: {', '.join(valid_tree_types)}"
        )

    if land_type not in valid_land_types:
        raise ValueError(
            f"Invalid land_type: {land_type}. "
            f"Valid options: {', '.join(valid_land_types)}"
        )

    if gs_type not in valid_gs_types:
        raise ValueError(
            f"Invalid gs_type: {gs_type}. "
            f"Valid options: {', '.join(valid_gs_types)}"
        )
