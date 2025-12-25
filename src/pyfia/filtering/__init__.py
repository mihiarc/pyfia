"""
Data filtering and processing utilities for pyFIA.

This module provides all filtering functionality including:
- Tree and area filtering functions
- Domain expression parsing
- Domain indicator calculation
- Grouping and classification utilities
"""

# Core parsing functionality
# Area filtering
from .area.filters import apply_area_filters
from .core.parser import DomainExpressionParser

# Plot filtering
from .plot.filters import apply_plot_filters

# Domain indicators
from .indicators.land_types import (
    LandTypeCategory,
    add_land_type_categories,
    classify_land_types,
    get_land_domain_indicator,
)

# Tree filtering
from .tree.filters import apply_tree_filters
from .utils.classification import (
    assign_forest_type_group,
    assign_size_class,
    assign_species_group,
    assign_tree_basis,
)

# Utility functions
from .utils.grouping_functions import (
    add_forest_type_group,
    add_ownership_group_name,
    add_species_info,
    create_size_class_expr,
    get_size_class_bounds,
    setup_grouping_columns,
)
from .utils.validation import (
    ColumnValidator,
    check_columns,
    ensure_columns,
    validate_columns,
)

__all__ = [
    # Core
    "DomainExpressionParser",
    # Filtering functions
    "apply_tree_filters",
    "apply_area_filters",
    "apply_plot_filters",
    # Domain indicators
    "classify_land_types",
    "get_land_domain_indicator",
    "add_land_type_categories",
    "LandTypeCategory",
    # Grouping
    "setup_grouping_columns",
    "create_size_class_expr",
    "get_size_class_bounds",
    "add_species_info",
    "add_ownership_group_name",
    "add_forest_type_group",
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
