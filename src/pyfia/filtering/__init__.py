"""
Data filtering and processing utilities for pyFIA.

This module provides all filtering functionality including:
- Tree and area filtering functions
- Domain expression parsing
- Domain indicator calculation
- Grouping and classification utilities
- Adjustment factor calculations
"""

# Core parsing functionality
from .core.parser import DomainExpressionParser

# Tree filtering
from .tree.filters import apply_tree_filters

# Area filtering
from .area.filters import apply_area_filters

# Domain indicators
from .indicators.calculator import DomainIndicatorCalculator
from .indicators.land_types import LandTypeClassifier, LandTypeStrategy

# Utility functions
from .utils.grouping import setup_grouping_columns
from .utils.grouping_functions import (
    create_size_class_expr,
    get_size_class_bounds,
    add_species_info,
    add_ownership_group_name,
    add_forest_type_group,
)
from .utils.adjustment import (
    apply_adjustment_factors,
    apply_tree_adjustment_factors,
)
from .utils.classification import (
    assign_tree_basis,
    assign_size_class,
    assign_forest_type_group,
    assign_species_group,
)
from .utils.validation import (
    ColumnValidator,
    validate_columns,
    check_columns,
    ensure_columns,
)

__all__ = [
    # Core
    "DomainExpressionParser",
    
    # Filtering functions
    "apply_tree_filters",
    "apply_area_filters",
    
    # Domain indicators
    "DomainIndicatorCalculator",
    "LandTypeClassifier",
    "LandTypeStrategy",
    
    # Grouping
    "setup_grouping_columns",
    "create_size_class_expr",
    "get_size_class_bounds",
    "add_species_info",
    "add_ownership_group_name",
    "add_forest_type_group",
    
    # Adjustment
    "apply_adjustment_factors",
    "apply_tree_adjustment_factors",
    
    # Classification
    "assign_tree_basis",
    "assign_size_class",
    "assign_forest_type_group",
    "assign_species_group",
    
    # Validation
    "ColumnValidator",
    "validate_columns",
    "check_columns",
    "ensure_columns",
]